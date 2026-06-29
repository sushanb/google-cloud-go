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
	"sync/atomic"
	"time"

	btpb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
	btransport "cloud.google.com/go/bigtable/internal/transport"
	"go.opentelemetry.io/otel/metric"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

const (
	resourcePrefixHeader = "google-cloud-resource-prefix"
	requestParamsHeader  = "x-goog-request-params"

	defaultMinSessions = 10
	defaultMaxSessions = 100
)

// sessionClient is the private SessionClient implementation. It holds one gRPC
// connection (placeholder for a future per-AFE pool) and uses it to construct
// session pools on demand for each SessionTableApi it vends. It also owns the
// per-instance ClientConfigurationManager built by NewSessionClient.
//
// Lifted from bigtable.SessionManager: pool naming with monotonic ID,
// ClientConfigurationManager listener registration. Metrics integration is
// deferred to Phase 4 — meterProvider is a no-op and dispatch code skips the
// per-attempt tracer work.
type sessionClient struct {
	project, instance, appProfile string

	conn           *grpc.ClientConn
	btClient       btpb.BigtableClient
	featureFlagsMD metadata.MD
	meterProvider  metric.MeterProvider

	// configManager pushes pool sizing / channel selection configuration and
	// SessionLoad updates from the server. Owned by this sessionClient —
	// Close closes it before tearing down the gRPC connection.
	configManager *btransport.ClientConfigurationManager

	// nextPoolID mints unique pool IDs for human-readable pool names, mirroring
	// bigtable.SessionManager.nextPoolID.
	nextPoolID atomic.Uint64
}

// managedPool bundles a SessionPoolImpl with its CCM-listener unregister thunk
// so the listener is detached before the pool is closed.
type managedPool struct {
	pool       *btransport.SessionPoolImpl
	unregister func() // nil when no CCM listener was registered
}

func (m *managedPool) close() error {
	if m == nil {
		return nil
	}
	if m.unregister != nil {
		m.unregister()
	}
	return m.pool.Close()
}

// NewSessionTable constructs a fresh SessionTableApi for tableName. Each call
// stands up new read and write session pools — there is no caching here.
// Callers are responsible for caching SessionTableApi instances per table.
//
// Pools are opened lazily per permission — the read pool is created on the
// first ReadRow call, the write pool on the first MutateRow call. This mirrors
// Java's ReadRowShimInner / MutateRowShim split, where a read-only or
// write-only workload never materializes the unused pool.
func (c *sessionClient) NewSessionTable(tableName string) SessionTableApi {
	fullTableName := fmt.Sprintf("projects/%s/instances/%s/tables/%s", c.project, c.instance, tableName)
	instanceName := fmt.Sprintf("projects/%s/instances/%s", c.project, c.instance)

	md := metadata.Join(metadata.Pairs(
		resourcePrefixHeader, instanceName,
		requestParamsHeader, urlParams(map[string]string{"table_name": fullTableName}),
	), c.featureFlagsMD)

	return &sessionTable{
		tableName:     fullTableName,
		md:            md,
		readPoolFn:    func() *managedPool { return c.makePool(fullTableName, btpb.OpenTableRequest_PERMISSION_READ) },
		writePoolFn:   func() *managedPool { return c.makePool(fullTableName, btpb.OpenTableRequest_PERMISSION_WRITE) },
		readVRpcDesc:  btransport.READ_ROW,
		writeVRpcDesc: btransport.MUTATE_ROW,
	}
}

// MeterProvider returns the OTel meter provider derived from the (future)
// injected metrics factory. Phase 1 always returns a no-op provider.
func (c *sessionClient) MeterProvider() metric.MeterProvider {
	return c.meterProvider
}

// OnSessionLoad registers a listener for server-driven SessionLoad updates.
// Returns a no-op unregister thunk if no configManager is wired.
func (c *sessionClient) OnSessionLoad(listener func(load float64)) func() {
	if c.configManager == nil {
		return func() {}
	}
	return c.configManager.AddSessionLoadListener(listener)
}

// Close closes the embedded ClientConfigurationManager first — its barrier
// blocks until any in-flight poll's listener callbacks have returned, so no
// SessionPool.UpdateConfig fires against a pool that is about to tear down —
// then closes the underlying gRPC connection.
func (c *sessionClient) Close() error {
	if c.configManager != nil {
		c.configManager.Close()
	}
	if c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

// makePool constructs a SessionPoolImpl for a single (table, permission) pair
// and wires up CCM listener registration.
//
// The pool's stream factory closes over c.btClient.OpenTable, so all pools
// share the same underlying connection.
//
// Lifted from bigtable.SessionManager.GetOrCreateSessionPool, minus the
// per-key dedup map (caching is the caller's responsibility — see
// SESSION_CLIENT_REFACTOR.md).
func (c *sessionClient) makePool(fullTableName string, permission btpb.OpenTableRequest_Permission) *managedPool {
	streamFactory := func(ctx context.Context) (btransport.Stream, error) {
		return c.btClient.OpenTable(ctx)
	}

	openReq := &btpb.OpenTableRequest{
		TableName:    fullTableName,
		AppProfileId: c.appProfile,
		Permission:   permission,
	}
	payload, err := proto.Marshal(openReq)
	if err != nil {
		// Marshalling a struct of well-known fields cannot fail in practice;
		// fall back to an empty payload rather than panic, so PerformScaling's
		// handshake will surface the error if it ever does fail.
		payload = nil
	}

	// Feature flags mirror bigtable.SessionManager — ClientSideMetricsEnabled
	// is off in Phase 1; Phase 4 will toggle it based on the injected metrics
	// factory.
	flags := &btpb.FeatureFlags{
		RoutingCookie:            true,
		ReverseScans:             true,
		LastScannedRowResponses:  true,
		ClientSideMetricsEnabled: false,
		RetryInfo:                true,
		TrafficDirectorEnabled:   true,
		DirectAccessRequested:    true,
		SessionsCompatible:       true,
		PeerInfo:                 true,
	}
	handshake := &btpb.OpenSessionRequest{
		ProtocolVersion: 1,
		Payload:         payload,
		Flags:           flags,
	}

	// Session-open metadata mirrors TABLE_SESSION.MetadataFn from
	// internal/transport/session_descriptors.go.
	sessionParams := urlParams(map[string]string{
		"open_session.payload.table_name":     fullTableName,
		"open_session.payload.app_profile_id": c.appProfile,
		"open_session.payload.permission":     permission.String(),
	})
	poolMD := metadata.Join(
		metadata.Pairs(requestParamsHeader, sessionParams),
		c.featureFlagsMD,
	)

	// Pool naming with monotonic ID mirrors bigtable.SessionManager so
	// SessionPoolImpl-generated session names (which embed poolID + shortName)
	// stay consistent with the classic path. shortName is the table leaf.
	id := c.nextPoolID.Add(1)
	shortName := tableShortName(fullTableName)
	permStr := "READ"
	if permission == btpb.OpenTableRequest_PERMISSION_WRITE {
		permStr = "WRITE"
	}
	poolName := fmt.Sprintf("%sPool-%d (%s) [%s]",
		btransport.SessionTypeTable.ProtoName(), id, shortName, permStr)

	pool := btransport.NewSessionPoolImpl(
		poolName,
		defaultMinSessions, defaultMaxSessions,
		streamFactory, handshake, poolMD,
		btransport.SessionTypeTable,
	)
	pool.SetPoolID(id)
	pool.SetPoolShortName(shortName)

	mp := &managedPool{pool: pool}

	// Register the CCM listener and remember the unregister thunk so Close()
	// can detach it before tearing down the pool.
	if c.configManager != nil {
		mp.unregister = c.configManager.AddSessionPoolListener(func(config *btpb.SessionClientConfiguration_SessionPoolConfiguration) {
			pool.UpdateConfig(config)
		})
	}

	// StartHeartbeat spawns its own goroutine; PerformScaling is synchronous
	// and dials/handshakes the initial sessions.
	pool.StartHeartbeat(context.Background(), 1*time.Second)
	pool.PerformScaling(context.Background())
	return mp
}

// tableShortName extracts the leaf segment of a fully-qualified table resource
// name ("projects/P/instances/I/tables/T" → "T"). Returns the full string if
// it does not have the expected shape — best-effort cosmetic helper.
func tableShortName(fullTableName string) string {
	if i := strings.LastIndex(fullTableName, "/"); i >= 0 {
		return fullTableName[i+1:]
	}
	return fullTableName
}

// urlParams URL-encodes a map into a sorted &-joined query string.
func urlParams(m map[string]string) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s=%s", k, url.QueryEscape(m[k])))
	}
	return strings.Join(parts, "&")
}
