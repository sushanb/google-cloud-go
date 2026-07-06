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
	"errors"
	"fmt"
	"time"

	btpb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
	btransport "cloud.google.com/go/bigtable/internal/transport"
	"google.golang.org/grpc/metadata"
)

// ErrWriteNotSupported is returned by MutateRow when the resource has
// no write pool — e.g. materialized views, which are read-only.
var ErrWriteNotSupported = errors.New("bigtable/session: write operations not supported on this resource")

// sessionTable implements SessionTableApi. Read and write session
// pools open lazily on first call (see lazyPool). No classic
// fallback — callers that want fallback wrap sessionTable in
// bigtable.TableShim.
type sessionTable struct {
	tableName     string
	readPool      *lazyPool
	writePool     *lazyPool
	readVRpcDesc  btransport.VRpcDescriptor
	writeVRpcDesc btransport.VRpcDescriptor
	md            metadata.MD
}

// newSessionTable is the internal constructor. Callers (sessionClient)
// build the lazyPool open closures + supply the vRPC descriptors and
// resource-scoped metadata.
func newSessionTable(
	tableName string,
	openRead func() (Invoker, error),
	openWrite func() (Invoker, error),
	readVRpcDesc btransport.VRpcDescriptor,
	writeVRpcDesc btransport.VRpcDescriptor,
	md metadata.MD,
) *sessionTable {
	return &sessionTable{
		tableName:     tableName,
		readPool:      &lazyPool{open: openRead},
		writePool:     &lazyPool{open: openWrite},
		readVRpcDesc:  readVRpcDesc,
		writeVRpcDesc: writeVRpcDesc,
		md:            md,
	}
}

// ReadRow dispatches a proto-native ReadRow through a lazily-opened
// READ session pool. Wraps the invoke in RetryingVRpc (idempotent =
// true — reads are always retryable).
func (t *sessionTable) ReadRow(ctx context.Context, req *btpb.SessionReadRowRequest) (*btpb.SessionReadRowResponse, error) {
	if req == nil {
		return nil, errors.New("bigtable/session: SessionReadRowRequest is nil")
	}
	readPool, poolErr := t.readPool.get()
	if poolErr != nil {
		return nil, fmt.Errorf("session read pool open: %w", poolErr)
	}
	if readPool == nil {
		return nil, ErrWriteNotSupported // reused for "no read support" — unreachable in practice since every resource has a read side
	}

	ctx = attachOutgoingMetadata(ctx, t.md)

	retryInterceptor := btransport.RetryingVRpc(btransport.RetryingOptions{
		MaxAttempts:       10,
		InitialBackoff:    10 * time.Millisecond,
		MaxBackoff:        32 * time.Second,
		BackoffMultiplier: 1.5,
		Idempotent:        true,
	})

	args := btransport.ReadRowArgs{
		RowKey: string(req.GetKey()),
		Filter: req.GetFilter(),
	}
	baseHandler := func(attemptCtx context.Context, request interface{}) (interface{}, error) {
		result, err := readPool.Invoke(attemptCtx, t.readVRpcDesc, request)
		if err != nil {
			return nil, err
		}
		return result.Response, nil
	}

	ctx = btransport.WithVRpcMetadata(ctx, t.readVRpcDesc.Method(), 0)
	chained := btransport.ChainInterceptors(retryInterceptor)
	res, err := chained(ctx, args, baseHandler)
	if err != nil {
		return nil, fmt.Errorf("session ReadRow vRPC: %w", err)
	}
	resp, ok := res.(*btpb.SessionReadRowResponse)
	if !ok {
		return nil, fmt.Errorf("session ReadRow: unexpected response type %T", res)
	}
	return resp, nil
}

// MutateRow dispatches a proto-native MutateRow through a lazily-
// opened WRITE session pool. Errors with ErrWriteNotSupported when
// the resource has no write pool (materialized views).
//
// Idempotency is computed from the mutation shape: a SetCell with
// ServerTime is non-idempotent (retrying would create duplicate cells
// with different server timestamps).
func (t *sessionTable) MutateRow(ctx context.Context, req *btpb.SessionMutateRowRequest) (*btpb.SessionMutateRowResponse, error) {
	if req == nil {
		return nil, errors.New("bigtable/session: SessionMutateRowRequest is nil")
	}
	writePool, poolErr := t.writePool.get()
	if poolErr != nil {
		return nil, fmt.Errorf("session write pool open: %w", poolErr)
	}
	if writePool == nil {
		return nil, ErrWriteNotSupported
	}

	ctx = attachOutgoingMetadata(ctx, t.md)

	retryInterceptor := btransport.RetryingVRpc(btransport.RetryingOptions{
		MaxAttempts:       10,
		InitialBackoff:    10 * time.Millisecond,
		MaxBackoff:        32 * time.Second,
		BackoffMultiplier: 1.5,
		Idempotent:        mutationsAreRetryable(req.GetMutations()),
	})

	args := btransport.MutateRowArgs{
		RowKey:    string(req.GetKey()),
		Mutations: req.GetMutations(),
	}
	baseHandler := func(attemptCtx context.Context, request interface{}) (interface{}, error) {
		result, err := writePool.Invoke(attemptCtx, t.writeVRpcDesc, request)
		if err != nil {
			return nil, err
		}
		return result.Response, nil
	}

	ctx = btransport.WithVRpcMetadata(ctx, t.writeVRpcDesc.Method(), 0)
	chained := btransport.ChainInterceptors(retryInterceptor)
	res, err := chained(ctx, args, baseHandler)
	if err != nil {
		return nil, fmt.Errorf("session MutateRow vRPC: %w", err)
	}
	resp, ok := res.(*btpb.SessionMutateRowResponse)
	if !ok {
		// Some vRPC descriptors return nil-typed responses on success;
		// synthesize an empty MutateRow response for callers.
		if res == nil {
			return &btpb.SessionMutateRowResponse{}, nil
		}
		return nil, fmt.Errorf("session MutateRow: unexpected response type %T", res)
	}
	return resp, nil
}

// Close is a no-op today — the underlying pools are shared across
// resources via sessionClient.pools and torn down by sessionClient.Close.
// Retained on the interface so callers get a symmetric Open/Close
// pattern and future implementations can add per-resource teardown.
func (t *sessionTable) Close() error {
	return nil
}

// attachOutgoingMetadata is the internal-session equivalent of
// bigtable.mergeOutgoingMetadata: joins the resource-scoped headers
// onto the outgoing gRPC context so any downstream inspection
// (interceptor, tracer) sees them.
func attachOutgoingMetadata(ctx context.Context, md metadata.MD) context.Context {
	if md == nil {
		return ctx
	}
	if existing, ok := metadata.FromOutgoingContext(ctx); ok {
		return metadata.NewOutgoingContext(ctx, metadata.Join(existing, md))
	}
	return metadata.NewOutgoingContext(ctx, md)
}

// mutationsAreRetryable mirrors bigtable.mutationsAreRetryable. A
// mutation is idempotent iff every SetCell carries an explicit
// timestamp (not ServerTime = -1). Duplicated here to avoid an
// import cycle; keep in sync with the classic-side helper.
func mutationsAreRetryable(muts []*btpb.Mutation) bool {
	const serverTime int64 = -1
	for _, mut := range muts {
		if setCell := mut.GetSetCell(); setCell != nil && setCell.TimestampMicros == serverTime {
			return false
		}
	}
	return true
}
