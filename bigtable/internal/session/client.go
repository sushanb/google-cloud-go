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

package session

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

// Standard gRPC routing headers — duplicated from the bigtable package
// constants (package boundary means we can't import them). Keep the
// values in sync with bigtable/doc.go.
const (
	resourcePrefixHeader = "google-cloud-resource-prefix"
	requestParamsHeader  = "x-goog-request-params"
)

// Default pool sizing — same as SessionManager's fallback (10/100).
const (
	defaultMinSessions = 10
	defaultMaxSessions = 100
)

// ChannelPool is the narrow surface sessionClient needs from the
// managed channel pool it owns. Satisfied by
// *btransport.BigtableChannelPool. Interfaced so tests can swap in a
// fake pool without wiring a real gRPC transport.
type ChannelPool interface {
	Close() error
}

// Config bundles the settings sessionClient needs at construction
// time. Kept as a struct rather than a long positional constructor.
type Config struct {
	// Project / Instance / AppProfile identify the target resource
	// and get baked into resource-name composition + request-params.
	Project    string
	Instance   string
	AppProfile string

	// FeatureFlagsMD is merged into per-pool routing metadata.
	FeatureFlagsMD metadata.MD

	// ConfigMD is the metadata attached to the ClientConfigurationManager's
	// GetClientConfiguration polls — instance-scoped headers.
	ConfigMD metadata.MD

	// MetricsEnabled / DisableRetryInfo mirror the SessionManager
	// booleans of the same name; both propagate into FeatureFlags on
	// every OpenSessionRequest.
	MetricsEnabled   bool
	DisableRetryInfo bool

	// MinSessions / MaxSessions are per-pool bounds. Zero uses
	// defaults (10/100).
	MinSessions int
	MaxSessions int

	// SessionLoadListener is invoked whenever the server-driven
	// ClientConfigurationManager reports a new session-load ratio. The
	// bigtable Client wires this to Diverter.SetSessionLoad so the
	// classic/session traffic split follows the server's directive.
	SessionLoadListener func(load float64)

	// BackgroundCtx is the parent context passed to per-pool
	// StartHeartbeat / StartAfePrune / PerformScaling loops. Cancelled
	// by Client teardown so all background loops unwind.
	BackgroundCtx context.Context
}

// managedPool bundles a pool with its config-listener unregister
// thunk so the listener can be detached before pool teardown.
type managedPool struct {
	pool       *btransport.SessionPoolImpl
	unregister func()
}

// sessionClient is the internal implementation of the SessionClient
// interface. Owns the channel pool + gRPC stub + configuration
// manager, and vends per-resource SessionTableApi instances.
type sessionClient struct {
	cfg           Config
	channelPool   ChannelPool
	stub          btpb.BigtableClient
	meterProvider metric.MeterProvider
	configManager *btransport.ClientConfigurationManager

	poolsMu    sync.Mutex
	pools      map[string]*managedPool
	nextPoolID atomic.Uint64
}

// NewSessionClient constructs a sessionClient. The channel pool is
// OWNED — closing sessionClient closes the pool. The stub is expected
// to have been built against the same channel pool.
func NewSessionClient(channelPool ChannelPool, stub btpb.BigtableClient, meterProvider metric.MeterProvider, cfg Config) SessionClient {
	if cfg.MetricsEnabled && meterProvider != nil {
		_ = btransport.InitializeSessionMetrics(meterProvider)
	}
	sc := &sessionClient{
		cfg:           cfg,
		channelPool:   channelPool,
		stub:          stub,
		meterProvider: meterProvider,
		pools:         make(map[string]*managedPool),
	}
	if stub != nil {
		sc.configManager = btransport.NewClientConfigurationManager(
			stub, sc.fullInstanceName(), cfg.AppProfile, cfg.ConfigMD, nil,
		)
		sc.configManager.Start(cfg.BackgroundCtx)
		if cfg.SessionLoadListener != nil {
			sc.configManager.AddSessionLoadListener(cfg.SessionLoadListener)
		}
	}
	return sc
}

func (sc *sessionClient) MeterProvider() metric.MeterProvider {
	return sc.meterProvider
}

// OpenSessionTable returns a SessionTableApi for a standard table.
func (sc *sessionClient) OpenSessionTable(tableName string) SessionTableApi {
	fullName := sc.fullTableName(tableName)
	openRead := sc.buildLazyOpener(
		fullName,
		btransport.TABLE_SESSION,
		func(ctx context.Context) (btransport.Stream, error) { return sc.stub.OpenTable(ctx) },
		&btpb.OpenTableRequest{
			TableName:    fullName,
			AppProfileId: sc.cfg.AppProfile,
			Permission:   btpb.OpenTableRequest_PERMISSION_READ,
		},
		fmt.Sprintf("table:%s:read", tableName),
	)
	openWrite := sc.buildLazyOpener(
		fullName,
		btransport.TABLE_SESSION,
		func(ctx context.Context) (btransport.Stream, error) { return sc.stub.OpenTable(ctx) },
		&btpb.OpenTableRequest{
			TableName:    fullName,
			AppProfileId: sc.cfg.AppProfile,
			Permission:   btpb.OpenTableRequest_PERMISSION_WRITE,
		},
		fmt.Sprintf("table:%s:write", tableName),
	)
	return newSessionTable(fullName, openRead, openWrite, btransport.READ_ROW, btransport.MUTATE_ROW, sc.perResourceMetadata(fullName, "table_name", fullName))
}

// OpenAuthorizedView returns a SessionTableApi for an authorized view.
func (sc *sessionClient) OpenAuthorizedView(table, view string) SessionTableApi {
	fullName := sc.fullAuthorizedViewName(table, view)
	openRead := sc.buildLazyOpener(
		fullName,
		btransport.AUTHORIZED_VIEW_SESSION,
		func(ctx context.Context) (btransport.Stream, error) { return sc.stub.OpenAuthorizedView(ctx) },
		&btpb.OpenAuthorizedViewRequest{
			AuthorizedViewName: fullName,
			AppProfileId:       sc.cfg.AppProfile,
			Permission:         btpb.OpenAuthorizedViewRequest_PERMISSION_READ,
		},
		fmt.Sprintf("av:%s:%s:read", table, view),
	)
	openWrite := sc.buildLazyOpener(
		fullName,
		btransport.AUTHORIZED_VIEW_SESSION,
		func(ctx context.Context) (btransport.Stream, error) { return sc.stub.OpenAuthorizedView(ctx) },
		&btpb.OpenAuthorizedViewRequest{
			AuthorizedViewName: fullName,
			AppProfileId:       sc.cfg.AppProfile,
			Permission:         btpb.OpenAuthorizedViewRequest_PERMISSION_WRITE,
		},
		fmt.Sprintf("av:%s:%s:write", table, view),
	)
	return newSessionTable(fullName, openRead, openWrite, btransport.READ_ROW_AUTH_VIEW, btransport.MUTATE_ROW_AUTH_VIEW, sc.perResourceMetadata(fullName, "authorized_view_name", fullName))
}

// OpenMaterializedView returns a read-only SessionTableApi for a
// materialized view. Write pool closure stays nil so MutateRow
// errors cleanly.
func (sc *sessionClient) OpenMaterializedView(view string) SessionTableApi {
	fullName := sc.fullMaterializedViewName(view)
	openRead := sc.buildLazyOpener(
		fullName,
		btransport.MATERIALIZED_VIEW_SESSION,
		func(ctx context.Context) (btransport.Stream, error) { return sc.stub.OpenMaterializedView(ctx) },
		&btpb.OpenMaterializedViewRequest{
			MaterializedViewName: fullName,
			AppProfileId:         sc.cfg.AppProfile,
		},
		fmt.Sprintf("mv:%s:read", view),
	)
	return newSessionTable(fullName, openRead, nil, btransport.READ_ROW_MAT_VIEW, nil, sc.perResourceMetadata(fullName, "materialized_view_name", fullName))
}

// Close tears down in the 3-phase order that keeps late callbacks
// from firing against half-dead pools:
//  1. Stop config polling — no more UpdateConfig can fire after.
//  2. Close every session pool (per-pool listeners already detached).
//  3. Close the underlying channel pool.
func (sc *sessionClient) Close() error {
	sc.poolsMu.Lock()
	defer sc.poolsMu.Unlock()
	if sc.configManager != nil {
		sc.configManager.Close()
	}
	for _, mp := range sc.pools {
		if mp.unregister != nil {
			mp.unregister()
		}
		mp.pool.Close()
	}
	var err error
	if sc.channelPool != nil {
		err = sc.channelPool.Close()
	}
	return err
}

// buildLazyOpener returns a closure that, on first invocation,
// creates (or reuses via the keyed cache) the pool for the given
// payload/key. Returns nil when payload is nil (materialized-view
// write side).
func (sc *sessionClient) buildLazyOpener(
	resourceName string,
	sessionDesc *btransport.SessionDescriptor,
	streamFactory func(ctx context.Context) (btransport.Stream, error),
	payload proto.Message,
	key string,
) func() (Invoker, error) {
	if payload == nil {
		return nil
	}
	return func() (Invoker, error) {
		pool, err := sc.createPoolForPayload(resourceName, sessionDesc, streamFactory, payload, key)
		if err != nil {
			return nil, err
		}
		if pool == nil {
			return nil, nil
		}
		return pool, nil
	}
}

// createPoolForPayload marshals the resource-typed OpenXxxRequest
// into the transport-level OpenSessionRequest envelope, builds routing
// metadata via the descriptor's MetadataFn, and delegates to
// getOrCreatePool for cache-hit-or-construct.
func (sc *sessionClient) createPoolForPayload(
	resourceName string,
	sessionDesc *btransport.SessionDescriptor,
	streamFactory func(ctx context.Context) (btransport.Stream, error),
	payload proto.Message,
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
		Flags:           sc.featureFlags(),
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
	), sc.cfg.FeatureFlagsMD)

	min := sc.cfg.MinSessions
	if min <= 0 {
		min = defaultMinSessions
	}
	max := sc.cfg.MaxSessions
	if max <= 0 {
		max = defaultMaxSessions
	}
	shortName := ""
	if sessionDesc.ShortNameFn != nil {
		shortName = sessionDesc.ShortNameFn(payload)
	}
	return sc.getOrCreatePool(key, min, max, streamFactory, handshake, md, sessionDesc.Type, shortName), nil
}

// getOrCreatePool ports SessionManager.GetOrCreateSessionPool:
// dedups on key, mints a display name, constructs the pool, wires
// the config listener + background loops.
func (sc *sessionClient) getOrCreatePool(
	key string,
	min, max int,
	streamFactory func(ctx context.Context) (btransport.Stream, error),
	openSessionRequest *btpb.OpenSessionRequest,
	md metadata.MD,
	sessionType btransport.SessionType,
	shortName string,
) *btransport.SessionPoolImpl {
	sc.poolsMu.Lock()
	if mp, ok := sc.pools[key]; ok {
		sc.poolsMu.Unlock()
		return mp.pool
	}
	id := sc.nextPoolID.Add(1)
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
	mp := &managedPool{pool: pool}
	sc.pools[key] = mp
	configManager := sc.configManager
	backgroundCtx := sc.cfg.BackgroundCtx
	sc.poolsMu.Unlock()

	if configManager != nil {
		unregister := configManager.AddSessionPoolListener(func(config *btpb.SessionClientConfiguration_SessionPoolConfiguration) {
			pool.UpdateConfig(config)
		})
		sc.poolsMu.Lock()
		if cur, stillThere := sc.pools[key]; stillThere && cur == mp {
			mp.unregister = unregister
			sc.poolsMu.Unlock()
		} else {
			sc.poolsMu.Unlock()
			unregister()
		}
	}

	pool.StartHeartbeat(backgroundCtx, 1*time.Second)
	pool.StartAfePrune(backgroundCtx)
	pool.PerformScaling(backgroundCtx)
	return pool
}

// featureFlags builds the OpenSessionRequest.Flags proto from
// sessionClient config — matches SessionManager.GetOrCreateSessionTable's
// flags construction at session_manager.go:252.
func (sc *sessionClient) featureFlags() *btpb.FeatureFlags {
	return &btpb.FeatureFlags{
		RoutingCookie:            true,
		ReverseScans:             true,
		LastScannedRowResponses:  true,
		ClientSideMetricsEnabled: sc.cfg.MetricsEnabled,
		RetryInfo:                !sc.cfg.DisableRetryInfo,
		TrafficDirectorEnabled:   true,
		DirectAccessRequested:    true,
		SessionsCompatible:       true,
		PeerInfo:                 true,
	}
}

// perResourceMetadata builds the per-vRPC context metadata: the pair
// carried on the outgoing gRPC call for the underlying bidi stream.
// Header shape matches classic Table.md (bigtable/open.go:32-35).
func (sc *sessionClient) perResourceMetadata(fullResourceName, paramKey, paramVal string) metadata.MD {
	return metadata.Join(metadata.Pairs(
		resourcePrefixHeader, fullResourceName,
		requestParamsHeader, fmt.Sprintf("%s=%s&app_profile_id=%s", paramKey, url.QueryEscape(paramVal), url.QueryEscape(sc.cfg.AppProfile)),
	), sc.cfg.FeatureFlagsMD)
}

// Resource-name composition — duplicated from bigtable.Client to
// avoid an import cycle. Keep in sync with client.go's
// fullTableName / fullAuthorizedViewName / fullMaterializedViewName /
// fullInstanceName helpers.

func (sc *sessionClient) fullTableName(table string) string {
	return fmt.Sprintf("projects/%s/instances/%s/tables/%s", sc.cfg.Project, sc.cfg.Instance, table)
}

func (sc *sessionClient) fullAuthorizedViewName(table, view string) string {
	return fmt.Sprintf("projects/%s/instances/%s/tables/%s/authorizedViews/%s", sc.cfg.Project, sc.cfg.Instance, table, view)
}

func (sc *sessionClient) fullMaterializedViewName(view string) string {
	return fmt.Sprintf("projects/%s/instances/%s/materializedViews/%s", sc.cfg.Project, sc.cfg.Instance, view)
}

func (sc *sessionClient) fullInstanceName() string {
	return fmt.Sprintf("projects/%s/instances/%s", sc.cfg.Project, sc.cfg.Instance)
}

// Debug accessors exposed for the bigtable/session_debug.go providers.
// Kept off the SessionClient interface — consumers who need them
// type-assert to *sessionClient.

// ConfigManager returns the internal ClientConfigurationManager for
// configz. Nil when no stub was provided.
func (sc *sessionClient) ConfigManager() *btransport.ClientConfigurationManager {
	return sc.configManager
}

// PoolSnapshots returns one PoolSnapshot per owned pool, ordered by
// pool key for stable rendering. Same lock discipline as
// SessionManager.ManagerSnapshot.
func (sc *sessionClient) PoolSnapshots() []btransport.PoolSnapshot {
	sc.poolsMu.Lock()
	type entry struct {
		key  string
		pool *btransport.SessionPoolImpl
	}
	entries := make([]entry, 0, len(sc.pools))
	for k, mp := range sc.pools {
		entries = append(entries, entry{key: k, pool: mp.pool})
	}
	sc.poolsMu.Unlock()
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

// LoadBalancingSnapshots returns per-pool picker + pick-history
// snapshots for loadz. Ordered by pool key.
func (sc *sessionClient) LoadBalancingSnapshots() []btransport.LoadBalancingSnapshot {
	sc.poolsMu.Lock()
	type entry struct {
		key  string
		pool *btransport.SessionPoolImpl
	}
	entries := make([]entry, 0, len(sc.pools))
	for k, mp := range sc.pools {
		entries = append(entries, entry{key: k, pool: mp.pool})
	}
	sc.poolsMu.Unlock()
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

// ChannelPool returns the *btransport.BigtableChannelPool the
// sessionClient was constructed with, if any. Used by channelz to
// surface session-pool channel stats without leaking the interface
// through the public SessionClient API.
func (sc *sessionClient) ChannelPool() *btransport.BigtableChannelPool {
	if sc.channelPool == nil {
		return nil
	}
	bp, _ := sc.channelPool.(*btransport.BigtableChannelPool)
	return bp
}
