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
	"errors"
	"fmt"
	"sync"
	"time"

	btpb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
	btopt "cloud.google.com/go/bigtable/internal/option"
	btransport "cloud.google.com/go/bigtable/internal/transport"
)

// lazyPool wraps an Invoker (typically *btransport.SessionPoolImpl) that
// is opened on first use. Callers invoke get(); the first winner runs
// the open closure (synchronously — dial + handshake happens here) and
// stores the result; subsequent callers see the stored pool with no
// work.
//
// The stored type is Invoker rather than the concrete pool so tests can
// substitute a fake that only implements the .Invoke method.
//
// Failed opens are NOT cached: the next caller retries. This matters
// because pool creation can fail transiently (proto.Marshal is the only
// obvious source today, but future descriptor variants may add more).
// A permanent error-cache would leave the SessionTable stuck in
// classic-fallback for the process lifetime.
//
// A nil *lazyPool or one with a nil open closure returns (nil, nil) —
// "no session support, use fallback." Used for the write side of
// materialized views (read-only) and for the SessionManager-disabled
// case.
type lazyPool struct {
	mu   sync.Mutex
	pool Invoker
	open func() (Invoker, error)
}

// get returns the underlying pool, opening it on first call. Concurrent
// callers block until the open completes. See type doc for error /
// nil-lazyPool semantics.
func (l *lazyPool) get() (Invoker, error) {
	if l == nil || l.open == nil {
		return nil, nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.pool != nil {
		return l.pool, nil
	}
	p, err := l.open()
	if err != nil {
		return nil, err
	}
	l.pool = p
	return p, nil
}

// opened reports whether the pool has been opened yet — for tests and
// for the sessionz debug UI which wants to render "read pool: not yet
// opened" versus a live pool.
func (l *lazyPool) opened() bool {
	if l == nil {
		return false
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.pool != nil
}

// Invoker is the narrow surface SessionTable needs from a session pool:
// the ability to dispatch a single virtual RPC and surface the full
// InvokeResult (response, cluster info, server-side Stats, and the local
// SentAt timestamp). It is satisfied by *btransport.SessionPoolImpl; the
// interface exists so tests can substitute a fake implementation without
// standing up a real pool.
//
// Note: this interface intentionally requires Invoke (not the
// back-compat Invoke wrapper) so SessionTable can populate
// per-attempt clientBlockingLatency (from SentAt - attemptStart) and
// serverLatency (from Stats.BackendLatency).
type Invoker interface {
	Invoke(ctx context.Context, desc btransport.VRpcDescriptor, req interface{}) (btransport.InvokeResult, error)
}

// SessionTable implements TableAPI by routing calls via virtual RPCs through dedicated session pools.
// The pools are opened lazily on first ReadRow / Apply — read-only tables
// never pay for a write pool that they'll never use, and constructing a
// SessionTable no longer dials sessions upfront.
type SessionTable struct {
	tableName     string
	classic       *Table
	readPool      *lazyPool
	writePool     *lazyPool
	readVRpcDesc  btransport.VRpcDescriptor
	writeVRpcDesc btransport.VRpcDescriptor
}

// NewSessionTable creates a SessionTable whose read and write pools open
// on first use. openRead / openWrite are the closures that construct the
// underlying SessionPoolImpl (typically calling into SessionManager's
// keyed cache so pool sharing across tables is preserved). Either
// closure may be nil to indicate "no session pool for this op" —
// materialized views use nil for openWrite. A nil closure causes the
// corresponding call to fall back to the classic *Table.
func NewSessionTable(
	tableName string,
	classic *Table,
	openRead func() (Invoker, error),
	openWrite func() (Invoker, error),
	readVRpcDesc btransport.VRpcDescriptor,
	writeVRpcDesc btransport.VRpcDescriptor,
) *SessionTable {
	return &SessionTable{
		tableName:     tableName,
		classic:       classic,
		readPool:      &lazyPool{open: openRead},
		writePool:     &lazyPool{open: openWrite},
		readVRpcDesc:  readVRpcDesc,
		writeVRpcDesc: writeVRpcDesc,
	}
}

type sessionMetricsTracer struct{}

func (sessionMetricsTracer) OnAttemptStart(ctx context.Context) {
	if mt := metricsTracerFromContext(ctx); mt != nil {
		mt.recordAttemptStart()
	}
}

func (sessionMetricsTracer) OnAttemptComplete(ctx context.Context, err error) {
	if mt := metricsTracerFromContext(ctx); mt != nil {
		mt.recordAttemptCompletion(nil, nil, err)
	}
}

// ReadRow reads a single row via vRPC. Opens the read pool on first
// call — see lazyPool. Falls back to the classic *Table when there is
// no session support for this table or when the pool open fails
// transiently (next call retries the open).
func (t *SessionTable) ReadRow(ctx context.Context, row string, opts ...ReadOption) (rowVal Row, err error) {
	readPool, poolErr := t.readPool.get()
	if poolErr != nil {
		btopt.Debugf(nil, "SessionTable.ReadRow: readPool open failed: %v; falling back to classic", poolErr)
		return t.classic.ReadRow(ctx, row, opts...)
	}
	if readPool == nil {
		return t.classic.ReadRow(ctx, row, opts...)
	}

	ctx = mergeOutgoingMetadata(ctx, t.classic.md)

	mt := t.classic.newBuiltinMetricsTracer(ctx, false)
	defer mt.recordOperationCompletion()
	mt.setMethod("ReadRows")
	ctx = contextWithMetricsTracer(ctx, mt)

	defer func() {
		statusCode, _ := convertToGrpcStatusErr(err)
		mt.setCurrOpStatus(statusCode)
	}()

	req := &btpb.ReadRowsRequest{
		TableName: t.tableName,
		Rows: &btpb.RowSet{
			RowKeys: [][]byte{[]byte(row)},
		},
	}
	settings := makeReadSettings(req, 0)
	for _, opt := range opts {
		opt.set(&settings)
	}

	retryInterceptor := btransport.RetryingVRpc(btransport.RetryingOptions{
		MaxAttempts:       10, // Up to 10 attempts (initial attempt + 9 retries)
		InitialBackoff:    10 * time.Millisecond,
		MaxBackoff:        32 * time.Second,
		BackoffMultiplier: 1.5,
		Tracer:            sessionMetricsTracer{},
		Idempotent:        true, // reads are always idempotent
	})

	args := btransport.ReadRowArgs{
		RowKey: row,
		Filter: req.Filter,
	}

	baseHandler := func(attemptCtx context.Context, request interface{}) (interface{}, error) {
		result, err := readPool.Invoke(attemptCtx, t.readVRpcDesc, request)
		if mt := metricsTracerFromContext(attemptCtx); mt != nil {
			if result.ClusterInfo != nil {
				mt.currOp.currAttempt.setClusterID(result.ClusterInfo.ClusterId)
				mt.currOp.currAttempt.setZoneID(result.ClusterInfo.ZoneId)
			}
			// Stamp client-blocking latency as (SentAt - attemptStart). The
			// gRPC stats handler never fires for vRPC frames, so without
			// this assignment clientBlockingLatency would stay at 0 on
			// the session path.
			if !result.SentAt.IsZero() && !mt.currOp.currAttempt.startTime.IsZero() {
				mt.currOp.currAttempt.clientBlockingLatency = convertToMs(result.SentAt.Sub(mt.currOp.currAttempt.startTime))
			}
			// Pull server-reported backend latency out of the Stats
			// payload when the server populated it on the success frame.
			if result.Stats != nil && result.Stats.BackendLatency != nil {
				mt.currOp.currAttempt.setServerLatency(convertToMs(result.Stats.GetBackendLatency().AsDuration()))
			}
			// Stamp transport labels for attempt_latencies2 from the
			// serving session's peer info. Classic (unary) RPCs get
			// these via extractPeerInfo in recordAttemptCompletion; the
			// session path has no per-attempt header so we source them
			// from the bound Session's parsed PeerInfo instead.
			if result.PeerInfo != nil {
				mt.currOp.currAttempt.transportType = btransport.TransportTypeName(result.PeerInfo.GetTransportType())
				mt.currOp.currAttempt.transportRegion = result.PeerInfo.GetApplicationFrontendRegion()
				mt.currOp.currAttempt.transportZone = result.PeerInfo.GetApplicationFrontendZone()
				mt.currOp.currAttempt.transportSubZone = result.PeerInfo.GetApplicationFrontendSubzone()
			}
		}
		if err != nil {
			return nil, err
		}
		return result.Response, nil
	}

	// Seed vRPC metadata so RetryingVRpc's WithAttempt(ctx, n) actually
	// increments the per-attempt counter that Invoke reads via
	// VRpcAttempt(ctx). Without this seed, WithAttempt is a no-op and every
	// retry wire-frame carries AttemptNumber=1.
	ctx = btransport.WithVRpcMetadata(ctx, t.readVRpcDesc.Method(), 0)
	chained := btransport.ChainInterceptors(retryInterceptor)
	res, err := chained(ctx, args, baseHandler)
	if err != nil {
		return nil, fmt.Errorf("failed to execute ReadRow vRPC: %w", err)
	}

	readResp, ok := res.(*btpb.SessionReadRowResponse)
	if !ok {
		return nil, fmt.Errorf("unexpected response type from vRPC: %T", res)
	}

	if readResp.Stats != nil && settings.fullReadStatsFunc != nil {
		stats := makeFullReadStats(readResp.Stats)
		settings.fullReadStatsFunc(&stats)
	}

	return protoRowToRow(readResp.GetRow()), nil
}

// Apply applies a single mutation via vRPC. Opens the write pool on
// first call — see lazyPool. Conditional mutations always fall through
// to classic. Returns an error if there is no session write support
// (writePool closure was nil) — matches the classic-side "no writes on
// this resource" behaviour.
func (t *SessionTable) Apply(ctx context.Context, row string, m *Mutation, opts ...ApplyOption) (err error) {
	if m.isConditional {
		return t.classic.Apply(ctx, row, m, opts...)
	}
	writePool, poolErr := t.writePool.get()
	if poolErr != nil {
		btopt.Debugf(nil, "SessionTable.Apply: writePool open failed: %v; falling back to classic", poolErr)
		return t.classic.Apply(ctx, row, m, opts...)
	}
	if writePool == nil {
		return errors.New("bigtable: write operations not supported on this resource")
	}

	ctx = mergeOutgoingMetadata(ctx, t.classic.md)

	mt := t.classic.newBuiltinMetricsTracer(ctx, false)
	defer mt.recordOperationCompletion()
	mt.setMethod("MutateRow")
	ctx = contextWithMetricsTracer(ctx, mt)

	defer func() {
		statusCode, _ := convertToGrpcStatusErr(err)
		mt.setCurrOpStatus(statusCode)
	}()

	// Non-idempotent mutations (SetCell with ServerTime) cannot retry on
	// TransportFailure — a retry of an already-applied mutation would create
	// duplicate cells with different server-assigned timestamps. Uncommitted
	// attempts (ErrSessionNotActive from a Closing session, encode failure)
	// still retry regardless of Idempotent, so short-circuited attempts
	// that never reached the server are covered automatically.
	retryInterceptor := btransport.RetryingVRpc(btransport.RetryingOptions{
		MaxAttempts:       10,
		InitialBackoff:    10 * time.Millisecond,
		MaxBackoff:        32 * time.Second,
		BackoffMultiplier: 1.5,
		Tracer:            sessionMetricsTracer{},
		Idempotent:        mutationsAreRetryable(m.ops),
	})

	args := btransport.MutateRowArgs{
		RowKey:    row,
		Mutations: m.ops,
	}

	baseHandler := func(attemptCtx context.Context, request interface{}) (interface{}, error) {
		result, err := writePool.Invoke(attemptCtx, t.writeVRpcDesc, request)
		if mt := metricsTracerFromContext(attemptCtx); mt != nil {
			if result.ClusterInfo != nil {
				mt.currOp.currAttempt.setClusterID(result.ClusterInfo.ClusterId)
				mt.currOp.currAttempt.setZoneID(result.ClusterInfo.ZoneId)
			}
			// Stamp client-blocking latency as (SentAt - attemptStart). The
			// gRPC stats handler never fires for vRPC frames, so without
			// this assignment clientBlockingLatency would stay at 0 on
			// the session path.
			if !result.SentAt.IsZero() && !mt.currOp.currAttempt.startTime.IsZero() {
				mt.currOp.currAttempt.clientBlockingLatency = convertToMs(result.SentAt.Sub(mt.currOp.currAttempt.startTime))
			}
			// Pull server-reported backend latency out of the Stats
			// payload when the server populated it on the success frame.
			if result.Stats != nil && result.Stats.BackendLatency != nil {
				mt.currOp.currAttempt.setServerLatency(convertToMs(result.Stats.GetBackendLatency().AsDuration()))
			}
			// Stamp transport labels for attempt_latencies2 from the
			// serving session's peer info. Classic (unary) RPCs get
			// these via extractPeerInfo in recordAttemptCompletion; the
			// session path has no per-attempt header so we source them
			// from the bound Session's parsed PeerInfo instead.
			if result.PeerInfo != nil {
				mt.currOp.currAttempt.transportType = btransport.TransportTypeName(result.PeerInfo.GetTransportType())
				mt.currOp.currAttempt.transportRegion = result.PeerInfo.GetApplicationFrontendRegion()
				mt.currOp.currAttempt.transportZone = result.PeerInfo.GetApplicationFrontendZone()
				mt.currOp.currAttempt.transportSubZone = result.PeerInfo.GetApplicationFrontendSubzone()
			}
		}
		if err != nil {
			return nil, err
		}
		return result.Response, nil
	}

	// Seed vRPC metadata so RetryingVRpc's WithAttempt(ctx, n) actually
	// increments the per-attempt counter that Invoke reads via
	// VRpcAttempt(ctx). Without this seed, WithAttempt is a no-op and every
	// retry wire-frame carries AttemptNumber=1.
	ctx = btransport.WithVRpcMetadata(ctx, t.writeVRpcDesc.Method(), 0)
	chained := btransport.ChainInterceptors(retryInterceptor)
	_, err = chained(ctx, args, baseHandler)
	if err != nil {
		return fmt.Errorf("failed to execute MutateRow vRPC: %w", err)
	}

	return nil
}

// ReadRows delegates to classic TableAPI.
func (t *SessionTable) ReadRows(ctx context.Context, arg RowSet, f func(Row) bool, opts ...ReadOption) error {
	return t.classic.ReadRows(ctx, arg, f, opts...)
}

// SampleRowKeys delegates to classic TableAPI.
func (t *SessionTable) SampleRowKeys(ctx context.Context) ([]string, error) {
	return t.classic.SampleRowKeys(ctx)
}

// ApplyBulk delegates to classic TableAPI.
func (t *SessionTable) ApplyBulk(ctx context.Context, rowKeys []string, muts []*Mutation, opts ...ApplyOption) ([]error, error) {
	return t.classic.ApplyBulk(ctx, rowKeys, muts, opts...)
}

// ApplyReadModifyWrite delegates to classic TableAPI.
func (t *SessionTable) ApplyReadModifyWrite(ctx context.Context, row string, m *ReadModifyWrite) (Row, error) {
	return t.classic.ApplyReadModifyWrite(ctx, row, m)
}

func protoRowToRow(pr *btpb.Row) Row {
	if pr == nil {
		return nil
	}
	rowMap := make(Row)
	rowKey := string(pr.Key)
	for _, fam := range pr.Families {
		familyName := fam.Name
		for _, col := range fam.Columns {
			columnName := familyName + ":" + string(col.Qualifier)
			var items []ReadItem
			for _, cell := range col.Cells {
				items = append(items, ReadItem{
					Row:       rowKey,
					Column:    columnName,
					Timestamp: Timestamp(cell.TimestampMicros),
					Value:     cell.Value,
					Labels:    cell.Labels,
				})
			}
			if len(items) > 0 {
				rowMap[familyName] = append(rowMap[familyName], items...)
			}
		}
	}
	return rowMap
}
