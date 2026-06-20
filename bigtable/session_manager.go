// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package bigtable

import (
	"context"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	btpb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
	btransport "cloud.google.com/go/bigtable/internal/transport"
	"go.opentelemetry.io/otel/metric"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

// managedPool bundles a session pool with its configuration-listener unregister
// thunk so the listener can be detached when the pool is closed.
type managedPool struct {
	pool       *btransport.SessionPoolImpl
	unregister func()
}

// SessionManager manages dynamic table-specific session pools for vRPC operations.
type SessionManager struct {
	mu                sync.Mutex
	enableSessionPool bool
	metricsEnabled    bool
	disableRetryInfo  bool
	featureFlagsMD    metadata.MD
	diverter          *btransport.Diverter
	configManager     *btransport.ClientConfigurationManager
	backgroundCtx     context.Context
	sessionPools      map[string]*managedPool
	// nextPoolID is the monotonic counter used to mint unique pool names
	// like "OpenTablePool-3". Distinct from the keyed sessionPools map
	// (which uses the table+permission key for dedup); the pool name is
	// purely the human-facing identifier surfaced in the debug UI.
	nextPoolID  atomic.Uint64
	minSessions int
	maxSessions int
	channelPool managedChannelPool
}

// NewSessionManager creates a new SessionManager.
func NewSessionManager(
	enableSessionPool bool,
	metricsEnabled bool,
	disableRetryInfo bool,
	featureFlagsMD metadata.MD,
	diverter *btransport.Diverter,
	configManager *btransport.ClientConfigurationManager,
	backgroundCtx context.Context,
	minSessions int,
	maxSessions int,
	meterProvider metric.MeterProvider,
	channelPool managedChannelPool,
) *SessionManager {
	if metricsEnabled && meterProvider != nil {
		_ = btransport.InitializeSessionMetrics(meterProvider)
	}
	return &SessionManager{
		enableSessionPool: enableSessionPool,
		metricsEnabled:    metricsEnabled,
		disableRetryInfo:  disableRetryInfo,
		featureFlagsMD:    featureFlagsMD,
		diverter:          diverter,
		configManager:     configManager,
		backgroundCtx:     backgroundCtx,
		sessionPools:      make(map[string]*managedPool),
		minSessions:       minSessions,
		maxSessions:       maxSessions,
		channelPool:       channelPool,
	}
}

// Close closes all session pools managed by the SessionManager.
func (m *SessionManager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, mp := range m.sessionPools {
		if mp.unregister != nil {
			mp.unregister()
		}
		mp.pool.Close()
	}
	var err error
	if m.channelPool.pool != nil {
		err = m.channelPool.Close()
	}
	return err
}

// GetOrCreateSessionTable initializes or retrieves session-based transport pools for a table/view.
func (m *SessionManager) GetOrCreateSessionTable(
	resourceName string,
	classic *Table,
	sessionDesc *btransport.SessionDescriptor,
	readStreamFactory func(ctx context.Context) (btransport.Stream, error),
	writeStreamFactory func(ctx context.Context) (btransport.Stream, error),
	readPayload proto.Message,
	writePayload proto.Message,
	readVRpcDesc btransport.VRpcDescriptor,
	writeVRpcDesc btransport.VRpcDescriptor,
	keyPrefix string,
) TableAPI {
	if !m.enableSessionPool {
		return &tableImpl{*classic}
	}

	flags := &btpb.FeatureFlags{
		RoutingCookie:            true,
		ReverseScans:             true,
		LastScannedRowResponses:  true,
		ClientSideMetricsEnabled: m.metricsEnabled,
		RetryInfo:                !m.disableRetryInfo,
		TrafficDirectorEnabled:   true,
		DirectAccessRequested:    true,
		SessionsCompatible:       true,
		PeerInfo:                 true,
	}

	readKey := fmt.Sprintf("%s:read", keyPrefix)
	readPool, err := m.createPoolForPayload(resourceName, sessionDesc, readStreamFactory, readPayload, flags, readKey)
	if err != nil {
		fmt.Printf(">>> SessionManager: failed to create read pool for key=%s: %v; falling back to classic <<<\n", readKey, err)
		return &tableImpl{*classic}
	}

	writeKey := fmt.Sprintf("%s:write", keyPrefix)
	writePool, err := m.createPoolForPayload(resourceName, sessionDesc, writeStreamFactory, writePayload, flags, writeKey)
	if err != nil {
		fmt.Printf(">>> SessionManager: failed to create write pool for key=%s: %v; falling back to classic <<<\n", writeKey, err)
		return &tableImpl{*classic}
	}

	if readPool != nil && m.diverter != nil {
		sessionTable := NewSessionTable(classic.table, classic, readPool, writePool, readVRpcDesc, writeVRpcDesc)
		return NewTableShim(&tableImpl{*classic}, sessionTable, m.diverter)
	}

	return &tableImpl{*classic}
}

func (m *SessionManager) createPoolForPayload(
	resourceName string,
	sessionDesc *btransport.SessionDescriptor,
	streamFactory func(ctx context.Context) (btransport.Stream, error),
	payload proto.Message,
	flags *btpb.FeatureFlags,
	key string,
) (*btransport.SessionPoolImpl, error) {
	if payload == nil {
		return nil, nil
	}

	payloadBytes, err := proto.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("proto.Marshal session payload: %w", err)
	}
	handshake := &btpb.OpenSessionRequest{
		ProtocolVersion: 1,
		Payload:         payloadBytes,
		Flags:           flags,
	}

	metaMap := sessionDesc.MetadataFn(payload)
	keys := make([]string, 0, len(metaMap))
	for k := range metaMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	sessionMetadata := make([]string, 0, len(keys))
	for _, k := range keys {
		sessionMetadata = append(sessionMetadata, fmt.Sprintf("%s=%s", k, url.QueryEscape(metaMap[k])))
	}
	paramsVal := strings.Join(sessionMetadata, "&")

	md := metadata.Join(metadata.Pairs(
		resourcePrefixHeader, resourceName,
		requestParamsHeader, paramsVal,
	), m.featureFlagsMD)

	min := 10
	if m.minSessions > 0 {
		min = m.minSessions
	}
	max := 100
	if m.maxSessions > 0 {
		max = m.maxSessions
	}
	return m.GetOrCreateSessionPool(key, min, max, streamFactory, handshake, md, sessionDesc.Type), nil
}

// GetOrCreateSessionPool gets or creates a session pool for a specific key.
func (m *SessionManager) GetOrCreateSessionPool(
	key string,
	min, max int,
	streamFactory func(ctx context.Context) (btransport.Stream, error),
	openSessionRequest *btpb.OpenSessionRequest,
	md metadata.MD,
	sessionType btransport.SessionType,
) *btransport.SessionPoolImpl {
	fmt.Printf(">>> getOrCreateSessionPool: key=%s, min=%d, max=%d <<<\n", key, min, max)

	m.mu.Lock()
	mp, ok := m.sessionPools[key]
	if ok {
		m.mu.Unlock()
		return mp.pool
	}

	// Mint a unique, human-readable pool name from the proto type the
	// session is opened with plus a monotonic ID. The map key (`key`,
	// e.g. "table:foo:read") stays the dedup key so repeat OpenTable
	// calls return the same pool; the surfaced pool name is what shows
	// up in logs and any debug UI. Permission hint comes from the key's
	// :read / :write suffix when present.
	id := m.nextPoolID.Add(1)
	permission := ""
	if strings.HasSuffix(key, ":read") {
		permission = "READ"
	} else if strings.HasSuffix(key, ":write") {
		permission = "WRITE"
	}
	poolName := fmt.Sprintf("%sPool-%d", sessionType.ProtoName(), id)
	if permission != "" {
		poolName += " [" + permission + "]"
	}

	pool := btransport.NewSessionPoolImpl(poolName, min, max, streamFactory, openSessionRequest, md, sessionType)
	mp = &managedPool{pool: pool}
	m.sessionPools[key] = mp
	configManager := m.configManager
	backgroundCtx := m.backgroundCtx
	m.mu.Unlock()

	// Register the configuration listener and remember the unregister thunk so
	// Close() can detach the listener before tearing down the pool.
	if configManager != nil {
		unregister := configManager.AddSessionPoolListener(func(config *btpb.SessionClientConfiguration_SessionPoolConfiguration) {
			pool.UpdateConfig(config)
		})
		m.mu.Lock()
		// Re-check the map entry: Close() may have removed it while we were
		// registering. If so, immediately detach the listener.
		if cur, stillThere := m.sessionPools[key]; stillThere && cur == mp {
			mp.unregister = unregister
			m.mu.Unlock()
		} else {
			m.mu.Unlock()
			unregister()
		}
	}

	// StartHeartbeat spawns its own goroutine so it does not block. PerformScaling
	// is synchronous and dials/handshakes new sessions, so it MUST run outside
	// m.mu to avoid blocking concurrent GetOrCreateSessionPool calls for other
	// keys.
	pool.StartHeartbeat(backgroundCtx, 1*time.Second)
	pool.PerformScaling(backgroundCtx)
	return pool
}
