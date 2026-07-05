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
	nextPoolID        atomic.Uint64
	minSessions       int
	maxSessions       int
	channelPool       managedChannelPool
}

// NewSessionManager creates a new SessionManager. When
// `enableSessionPool` is true, it owns and drives the
// ClientConfigurationManager — polling GetClientConfiguration over the
// session channel pool (`sessionClient`), starting it against
// `backgroundCtx`, and wiring the SessionLoad → Diverter feedback
// loop. When `enableSessionPool` is false, no config polling happens
// (there'd be nothing to configure); callers of ConfigManager get nil
// back.
//
// `sessionClient` MUST have been built against the same channel pool
// passed as `channelPool` — session pool sessions and config polls
// then share transport, matching the planned NewSessionClient design.
func NewSessionManager(
	enableSessionPool bool,
	metricsEnabled bool,
	disableRetryInfo bool,
	featureFlagsMD metadata.MD,
	diverter *btransport.Diverter,
	sessionClient btpb.BigtableClient,
	instanceName string,
	appProfile string,
	configMD metadata.MD,
	backgroundCtx context.Context,
	minSessions int,
	maxSessions int,
	meterProvider metric.MeterProvider,
	channelPool managedChannelPool,
) *SessionManager {
	if metricsEnabled && meterProvider != nil {
		_ = btransport.InitializeSessionMetrics(meterProvider)
	}
	m := &SessionManager{
		enableSessionPool: enableSessionPool,
		metricsEnabled:    metricsEnabled,
		disableRetryInfo:  disableRetryInfo,
		featureFlagsMD:    featureFlagsMD,
		diverter:          diverter,
		backgroundCtx:     backgroundCtx,
		sessionPools:      make(map[string]*managedPool),
		minSessions:       minSessions,
		maxSessions:       maxSessions,
		channelPool:       channelPool,
	}
	// Only spin up config polling when a session pool exists to consume
	// its updates. `sessionClient == nil` is a belt-and-braces guard: we
	// require both a truthy flag AND a real stub so a caller that
	// mis-wires won't panic on a nil poll.
	if enableSessionPool && sessionClient != nil {
		m.configManager = btransport.NewClientConfigurationManager(
			sessionClient, instanceName, appProfile, configMD, nil,
		)
		m.configManager.Start(backgroundCtx)
		// SessionLoad drives the classic/session traffic split via the
		// Diverter. The listener used to live in NewClient; moved here
		// because the config manager it consumes now lives here too.
		m.configManager.AddSessionLoadListener(func(load float64) {
			diverter.SetSessionLoad(load)
		})
	}
	return m
}

// ConfigManager returns the internal ClientConfigurationManager for
// external debug consumers (ConfigDebug). Nil when session pool is
// disabled — callers must handle that.
func (m *SessionManager) ConfigManager() *btransport.ClientConfigurationManager {
	return m.configManager
}

// channelChannelPool returns the SessionManager's underlying
// BigtableChannelPool if it has one, or nil. Used by Client.ChannelDebug to
// surface session-pool channel stats without leaking managedChannelPool to
// the public API.
func (m *SessionManager) channelChannelPool() *btransport.BigtableChannelPool {
	if m.channelPool.pool == nil {
		return nil
	}
	bp, _ := m.channelPool.pool.(*btransport.BigtableChannelPool)
	return bp
}

// ManagerSnapshot returns a snapshot of every pool the SessionManager
// currently owns, ordered by pool key for stable rendering. The pool lock is
// held only long enough to copy out the (key, *pool) pairs; the per-pool
// snapshots are taken lock-free with respect to m.mu.
func (m *SessionManager) ManagerSnapshot() []btransport.PoolSnapshot {
	m.mu.Lock()
	type entry struct {
		key  string
		pool *btransport.SessionPoolImpl
	}
	entries := make([]entry, 0, len(m.sessionPools))
	for k, mp := range m.sessionPools {
		entries = append(entries, entry{key: k, pool: mp.pool})
	}
	m.mu.Unlock()

	sort.Slice(entries, func(i, j int) bool { return entries[i].key < entries[j].key })

	out := make([]btransport.PoolSnapshot, 0, len(entries))
	for _, e := range entries {
		if e.pool == nil {
			continue
		}
		out = append(out, e.pool.PoolSnapshot())
	}
	return out
}

// LoadBalancingSnapshots returns one per-pool picker + pick-history
// snapshot for the loadz debug page. Ordered by pool key for stable
// rendering. Same lock discipline as ManagerSnapshot — hold m.mu only
// long enough to copy the (key, *pool) pairs; the per-pool
// LoadBalancingSnapshot has its own inner locks.
func (m *SessionManager) LoadBalancingSnapshots() []btransport.LoadBalancingSnapshot {
	m.mu.Lock()
	type entry struct {
		key  string
		pool *btransport.SessionPoolImpl
	}
	entries := make([]entry, 0, len(m.sessionPools))
	for k, mp := range m.sessionPools {
		entries = append(entries, entry{key: k, pool: mp.pool})
	}
	m.mu.Unlock()

	sort.Slice(entries, func(i, j int) bool { return entries[i].key < entries[j].key })

	out := make([]btransport.LoadBalancingSnapshot, 0, len(entries))
	for _, e := range entries {
		if e.pool == nil {
			continue
		}
		out = append(out, e.pool.LoadBalancingSnapshot())
	}
	return out
}

// Close tears down everything the SessionManager owns, in the order
// that keeps late callbacks from firing against half-dead pools:
//
//  1. Stop config polling (configManager.Close) — blocks until any
//     in-flight poll and its listener callbacks return. After this no
//     UpdateConfig fires, so the SessionPool teardown below cannot
//     race a late server-driven config update.
//  2. Close every session pool (per-pool listeners already detached).
//  3. Close the underlying channel pool.
func (m *SessionManager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.configManager != nil {
		m.configManager.Close()
	}
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

// GetOrCreateSessionTable initializes session-based transport for a
// table/view. The read and write pools are NOT opened here — they open
// lazily on the first ReadRow / Apply so read-only tables never pay
// for a write pool that goes unused.
//
// The classic *Table is captured for the fallback path (used when
// session pooling is disabled, when a lazy pool open fails, or by
// non-session methods like ReadRows / SampleRowKeys).
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
	if !m.enableSessionPool || m.diverter == nil {
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

	openRead := m.buildLazyOpener(resourceName, sessionDesc, readStreamFactory, readPayload, flags, fmt.Sprintf("%s:read", keyPrefix))
	openWrite := m.buildLazyOpener(resourceName, sessionDesc, writeStreamFactory, writePayload, flags, fmt.Sprintf("%s:write", keyPrefix))

	sessionTable := NewSessionTable(classic.table, classic, openRead, openWrite, readVRpcDesc, writeVRpcDesc)
	return NewTableShim(&tableImpl{*classic}, sessionTable, m.diverter)
}

// buildLazyOpener returns a closure that, on first invocation, creates
// (or reuses via GetOrCreateSessionPool's keyed cache) the pool for the
// given payload/key. Returns nil when payload is nil — that's the
// materialized-view write side, which should map to
// "no session write support" (Apply falls back to classic-error).
//
// The Invoker return type widens *SessionPoolImpl to an interface so
// SessionTable's lazyPool doesn't leak the concrete transport type.
func (m *SessionManager) buildLazyOpener(
	resourceName string,
	sessionDesc *btransport.SessionDescriptor,
	streamFactory func(ctx context.Context) (btransport.Stream, error),
	payload proto.Message,
	flags *btpb.FeatureFlags,
	key string,
) func() (Invoker, error) {
	if payload == nil {
		return nil
	}
	return func() (Invoker, error) {
		pool, err := m.createPoolForPayload(resourceName, sessionDesc, streamFactory, payload, flags, key)
		if err != nil {
			return nil, err
		}
		if pool == nil {
			return nil, nil
		}
		return pool, nil
	}
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
	shortName := ""
	if sessionDesc.ShortNameFn != nil {
		shortName = sessionDesc.ShortNameFn(payload)
	}
	return m.GetOrCreateSessionPool(key, min, max, streamFactory, handshake, md, sessionDesc.Type, shortName), nil
}

// GetOrCreateSessionPool gets or creates a session pool for a specific key.
// shortName is an optional resource identifier (e.g. table leaf name) that
// gets embedded in the pool's display name so operators can distinguish
// pools of the same proto type at a glance.
func (m *SessionManager) GetOrCreateSessionPool(
	key string,
	min, max int,
	streamFactory func(ctx context.Context) (btransport.Stream, error),
	openSessionRequest *btpb.OpenSessionRequest,
	md metadata.MD,
	sessionType btransport.SessionType,
	shortName string,
) *btransport.SessionPoolImpl {
	m.mu.Lock()
	mp, ok := m.sessionPools[key]
	if ok {
		m.mu.Unlock()
		return mp.pool
	}

	// Mint a unique, human-readable pool name from the proto type +
	// monotonic ID + (optional) resource short name + permission hint.
	// The map key stays the dedup key so repeat OpenTable calls return
	// the same pool; the pool's surfaced name is what the debug UI shows.
	//
	//   OpenTablePool-1 (sushanb) [READ]
	//   OpenAuthorizedViewPool-3 (sushanb/myview) [WRITE]
	id := m.nextPoolID.Add(1)
	permission := ""
	if strings.HasSuffix(key, ":read") {
		permission = "READ"
	} else if strings.HasSuffix(key, ":write") {
		permission = "WRITE"
	}
	poolName := fmt.Sprintf("%sPool-%d", sessionType.ProtoName(), id)
	if shortName != "" {
		poolName += " (" + shortName + ")"
	}
	if permission != "" {
		poolName += " [" + permission + "]"
	}

	pool := btransport.NewSessionPoolImpl(poolName, min, max, streamFactory, openSessionRequest, md, sessionType)
	pool.SetPoolID(id)
	pool.SetPoolShortName(shortName)
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
	// AFE-handle GC runs on its own slow cadence (java-parity 10 min) —
	// kept off the 1-sec heartbeat so sl.mu held during the map walk
	// never contends with serving-path Checkouts.
	pool.StartAfePrune(backgroundCtx)
	pool.PerformScaling(backgroundCtx)
	return pool
}
