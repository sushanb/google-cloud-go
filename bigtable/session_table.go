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
	"time"

	btpb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
	btransport "cloud.google.com/go/bigtable/internal/transport"
	"google.golang.org/grpc/metadata"
)

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

// SessionTable routes ReadRow and Apply calls via virtual RPCs through
// dedicated session pools. It has no dependency on the classic *Table or
// *Client — callers supply request metadata and an optional metrics factory
// directly, enabling construction without a full bigtable.Client (e.g. from
// the accelerator).
type SessionTable struct {
	tableName      string
	md             metadata.MD
	metricsFactory func(ctx context.Context, isStreaming bool) *builtinMetricsTracer // nil = noop
	readPool       Invoker
	writePool      Invoker
	readVRpcDesc   btransport.VRpcDescriptor
	writeVRpcDesc  btransport.VRpcDescriptor
}

// NewSessionTable creates a SessionTable.
//
// md is the outgoing gRPC metadata to attach to every vRPC request (resource
// prefix header, feature flags, etc.). metricsFactory may be nil, in which
// case a no-op tracer is used and no metrics are emitted.
func NewSessionTable(
	tableName string,
	md metadata.MD,
	metricsFactory func(ctx context.Context, isStreaming bool) *builtinMetricsTracer,
	readPool *btransport.SessionPoolImpl,
	writePool *btransport.SessionPoolImpl,
	readVRpcDesc btransport.VRpcDescriptor,
	writeVRpcDesc btransport.VRpcDescriptor,
) *SessionTable {
	st := &SessionTable{
		tableName:      tableName,
		md:             md,
		metricsFactory: metricsFactory,
		readVRpcDesc:   readVRpcDesc,
		writeVRpcDesc:  writeVRpcDesc,
	}
	// Avoid storing a typed-nil *SessionPoolImpl in the interface field: the
	// nil-pool checks in ReadRow/Apply use a plain `pool == nil` comparison,
	// which is false for a typed-nil-wrapped-in-interface value.
	if readPool != nil {
		st.readPool = readPool
	}
	if writePool != nil {
		st.writePool = writePool
	}
	return st
}

// newTracer returns a metrics tracer for a single operation. If metricsFactory
// is nil a zero-value (no-op) tracer is returned — all builtinMetricsTracer
// recording methods gate on builtInEnabled, so the zero value is safe.
func (t *SessionTable) newTracer(ctx context.Context, isStreaming bool) *builtinMetricsTracer {
	if t.metricsFactory == nil {
		return &builtinMetricsTracer{}
	}
	return t.metricsFactory(ctx, isStreaming)
}

type sessionMetricsListener struct{}

func (l sessionMetricsListener) OnAttemptStart(ctx context.Context) {
	if mt := metricsTracerFromContext(ctx); mt != nil {
		mt.recordAttemptStart()
	}
}

func (l sessionMetricsListener) OnAttemptComplete(ctx context.Context, err error) {
	if mt := metricsTracerFromContext(ctx); mt != nil {
		mt.recordAttemptCompletion(nil, nil, err)
	}
}

// ReadRow reads a single row via vRPC and returns the result as a bigtable.Row.
// Returns (nil, nil) for a missing row. Returns an error if the read pool is
// not available — callers (TableShim) should route to the classic path instead.
func (t *SessionTable) ReadRow(ctx context.Context, row string, opts ...ReadOption) (rowVal Row, err error) {
	pr, err := t.ReadRowProto(ctx, row, nil)
	if err != nil {
		return nil, err
	}
	return protoRowToRow(pr), nil
}

// ReadRowProto reads a single row via vRPC and returns the raw *btpb.Row proto,
// bypassing the bigtable.Row conversion. Returns (nil, nil) for a missing row.
func (t *SessionTable) ReadRowProto(ctx context.Context, row string, filter *btpb.RowFilter) (pr *btpb.Row, err error) {
	if t.readPool == nil {
		return nil, errors.New("bigtable: read pool not available")
	}

	ctx = mergeOutgoingMetadata(ctx, t.md)

	mt := t.newTracer(ctx, false)
	defer mt.recordOperationCompletion()
	mt.setMethod("ReadRows")
	ctx = contextWithMetricsTracer(ctx, mt)

	defer func() {
		statusCode, _ := convertToGrpcStatusErr(err)
		mt.setCurrOpStatus(statusCode)
	}()

	// Apply any ReadOptions that set a filter.
	if filter == nil {
		req := &btpb.ReadRowsRequest{
			TableName: t.tableName,
			Rows:      &btpb.RowSet{RowKeys: [][]byte{[]byte(row)}},
		}
		settings := makeReadSettings(req, 0)
		filter = req.Filter
		_ = settings
	}

	retryInterceptor := btransport.RetryingVRpc(btransport.RetryingOptions{
		MaxAttempts:       10,
		InitialBackoff:    10 * time.Millisecond,
		MaxBackoff:        32 * time.Second,
		BackoffMultiplier: 1.5,
		Listener:          sessionMetricsListener{},
	})

	args := btransport.ReadRowArgs{
		RowKey: row,
		Filter: filter,
	}

	baseHandler := func(attemptCtx context.Context, request interface{}) (interface{}, error) {
		result, err := t.readPool.Invoke(attemptCtx, t.readVRpcDesc, request)
		if mt := metricsTracerFromContext(attemptCtx); mt != nil {
			if result.ClusterInfo != nil {
				mt.currOp.currAttempt.setClusterID(result.ClusterInfo.ClusterId)
				mt.currOp.currAttempt.setZoneID(result.ClusterInfo.ZoneId)
			}
			if !result.SentAt.IsZero() && !mt.currOp.currAttempt.startTime.IsZero() {
				mt.currOp.currAttempt.clientBlockingLatency = convertToMs(result.SentAt.Sub(mt.currOp.currAttempt.startTime))
			}
			if result.Stats != nil && result.Stats.BackendLatency != nil {
				mt.currOp.currAttempt.setServerLatency(convertToMs(result.Stats.GetBackendLatency().AsDuration()))
			}
		}
		if err != nil {
			return nil, err
		}
		return result.Response, nil
	}

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
	return readResp.GetRow(), nil
}

// Apply applies a single mutation via vRPC. Conditional mutations must be
// routed to the classic path by the caller (TableShim) before reaching here.
func (t *SessionTable) Apply(ctx context.Context, row string, m *Mutation, opts ...ApplyOption) (err error) {
	if t.writePool == nil {
		return errors.New("bigtable: write operations not supported on this resource")
	}

	ctx = mergeOutgoingMetadata(ctx, t.md)

	mt := t.newTracer(ctx, false)
	defer mt.recordOperationCompletion()
	mt.setMethod("MutateRow")
	ctx = contextWithMetricsTracer(ctx, mt)

	defer func() {
		statusCode, _ := convertToGrpcStatusErr(err)
		mt.setCurrOpStatus(statusCode)
	}()

	// Non-idempotent mutations (SetCell with ServerTime) cannot blindly retry —
	// a retry of an already-applied mutation would create duplicate cells with
	// different server-assigned timestamps. They CAN safely retry on errors
	// that prove the request never reached the server: ErrSessionNotActive
	// (Invoke short-circuits before Send) and ErrUnavailableGoAway (the server
	// explicitly reports the rpc id as not-admitted).
	var shouldRetry func(error) bool
	if !mutationsAreRetryable(m.ops) {
		shouldRetry = func(err error) bool {
			return errors.Is(err, btransport.ErrSessionNotActive) ||
				errors.Is(err, btransport.ErrUnavailableGoAway)
		}
	}

	retryInterceptor := btransport.RetryingVRpc(btransport.RetryingOptions{
		MaxAttempts:       10,
		InitialBackoff:    10 * time.Millisecond,
		MaxBackoff:        32 * time.Second,
		BackoffMultiplier: 1.5,
		Listener:          sessionMetricsListener{},
		ShouldRetry:       shouldRetry,
	})

	args := btransport.MutateRowArgs{
		RowKey:    row,
		Mutations: m.ops,
	}

	baseHandler := func(attemptCtx context.Context, request interface{}) (interface{}, error) {
		result, err := t.writePool.Invoke(attemptCtx, t.writeVRpcDesc, request)
		if mt := metricsTracerFromContext(attemptCtx); mt != nil {
			if result.ClusterInfo != nil {
				mt.currOp.currAttempt.setClusterID(result.ClusterInfo.ClusterId)
				mt.currOp.currAttempt.setZoneID(result.ClusterInfo.ZoneId)
			}
			if !result.SentAt.IsZero() && !mt.currOp.currAttempt.startTime.IsZero() {
				mt.currOp.currAttempt.clientBlockingLatency = convertToMs(result.SentAt.Sub(mt.currOp.currAttempt.startTime))
			}
			if result.Stats != nil && result.Stats.BackendLatency != nil {
				mt.currOp.currAttempt.setServerLatency(convertToMs(result.Stats.GetBackendLatency().AsDuration()))
			}
		}
		if err != nil {
			return nil, err
		}
		return result.Response, nil
	}

	ctx = btransport.WithVRpcMetadata(ctx, t.writeVRpcDesc.Method(), 0)
	chained := btransport.ChainInterceptors(retryInterceptor)
	_, err = chained(ctx, args, baseHandler)
	if err != nil {
		return fmt.Errorf("failed to execute MutateRow vRPC: %w", err)
	}

	return nil
}

// MutateRowProto applies proto mutations directly without bigtable.Mutation boxing.
// ops are passed directly to the vRPC layer.
func (t *SessionTable) MutateRowProto(ctx context.Context, row string, mutations []*btpb.Mutation) error {
	return t.Apply(ctx, row, &Mutation{ops: mutations})
}

// ReadRows, SampleRowKeys, ApplyBulk, and ApplyReadModifyWrite are always
// routed to the classic path by TableShim and will never be called on a
// SessionTable. They exist only to satisfy the TableAPI interface.

func (t *SessionTable) ReadRows(ctx context.Context, arg RowSet, f func(Row) bool, opts ...ReadOption) error {
	return errors.New("bigtable: ReadRows not supported on session table")
}

func (t *SessionTable) SampleRowKeys(ctx context.Context) ([]string, error) {
	return nil, errors.New("bigtable: SampleRowKeys not supported on session table")
}

func (t *SessionTable) ApplyBulk(ctx context.Context, rowKeys []string, muts []*Mutation, opts ...ApplyOption) ([]error, error) {
	return nil, errors.New("bigtable: ApplyBulk not supported on session table")
}

func (t *SessionTable) ApplyReadModifyWrite(ctx context.Context, row string, m *ReadModifyWrite) (Row, error) {
	return nil, errors.New("bigtable: ApplyReadModifyWrite not supported on session table")
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
