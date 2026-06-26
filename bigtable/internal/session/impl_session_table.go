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
	"sync"
	"time"

	btpb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
	btransport "cloud.google.com/go/bigtable/internal/transport"
	"google.golang.org/grpc/metadata"
)

// sessionTable is the private SessionTableApi implementation. It holds at most
// one read pool (PERMISSION_READ) and one write pool (PERMISSION_WRITE),
// opened lazily on first use of the corresponding method — a read-only or
// write-only workload never materializes the unused pool. This mirrors Java's
// ReadRowShimInner / MutateRowShim separation.
//
// Pools are wrapped in managedPool so the CCM-listener unregister thunk runs
// before pool.Close(); see impl_session_client.go.
type sessionTable struct {
	tableName     string
	md            metadata.MD
	readVRpcDesc  btransport.VRpcDescriptor
	writeVRpcDesc btransport.VRpcDescriptor

	// readPoolFn / writePoolFn materialize the corresponding pool on demand.
	// Captured at NewSessionTable time so sessionTable does not need to hold a
	// reference back to sessionClient.
	readPoolFn  func() *managedPool
	writePoolFn func() *managedPool

	readOnce  sync.Once
	readPool  *managedPool
	writeOnce sync.Once
	writePool *managedPool
}

// readPoolHandle returns the read pool, materializing it on first call.
// Subsequent calls return the same pool. Returns nil if readPoolFn is nil.
func (t *sessionTable) readPoolHandle() *managedPool {
	if t.readPoolFn == nil {
		return nil
	}
	t.readOnce.Do(func() { t.readPool = t.readPoolFn() })
	return t.readPool
}

// writePoolHandle returns the write pool, materializing it on first call.
func (t *sessionTable) writePoolHandle() *managedPool {
	if t.writePoolFn == nil {
		return nil
	}
	t.writeOnce.Do(func() { t.writePool = t.writePoolFn() })
	return t.writePool
}

// ReadRow dispatches a SessionReadRowRequest over the read pool, opening the
// pool on first call.
func (t *sessionTable) ReadRow(ctx context.Context, req *btpb.SessionReadRowRequest) (*btpb.SessionReadRowResponse, error) {
	if req == nil {
		return nil, errors.New("session: nil ReadRow request")
	}
	pool := t.readPoolHandle()
	if pool == nil || pool.pool == nil {
		return nil, errors.New("session: read pool not available")
	}

	ctx = mergeOutgoingMetadata(ctx, t.md)

	retryInterceptor := btransport.RetryingVRpc(btransport.RetryingOptions{
		MaxAttempts:       10,
		InitialBackoff:    10 * time.Millisecond,
		MaxBackoff:        32 * time.Second,
		BackoffMultiplier: 1.5,
	})

	args := btransport.ReadRowArgs{
		RowKey: string(req.GetKey()),
		Filter: req.GetFilter(),
	}

	baseHandler := func(attemptCtx context.Context, request interface{}) (interface{}, error) {
		result, err := pool.pool.Invoke(attemptCtx, t.readVRpcDesc, request)
		if err != nil {
			return nil, err
		}
		return result.Response, nil
	}

	// Seed vRPC metadata so RetryingVRpc's WithAttempt(ctx, n) actually
	// increments the per-attempt counter. Without this, every retry wire-frame
	// would carry AttemptNumber=1.
	ctx = btransport.WithVRpcMetadata(ctx, t.readVRpcDesc.Method(), 0)
	chained := btransport.ChainInterceptors(retryInterceptor)
	res, err := chained(ctx, args, baseHandler)
	if err != nil {
		return nil, fmt.Errorf("session: ReadRow vRPC: %w", err)
	}

	resp, ok := res.(*btpb.SessionReadRowResponse)
	if !ok {
		return nil, fmt.Errorf("session: unexpected ReadRow response type: %T", res)
	}
	return resp, nil
}

// MutateRow dispatches a SessionMutateRowRequest over the write pool, opening
// the pool on first call.
func (t *sessionTable) MutateRow(ctx context.Context, req *btpb.SessionMutateRowRequest) (*btpb.SessionMutateRowResponse, error) {
	if req == nil {
		return nil, errors.New("session: nil MutateRow request")
	}
	pool := t.writePoolHandle()
	if pool == nil || pool.pool == nil {
		return nil, errors.New("session: write pool not available")
	}

	ctx = mergeOutgoingMetadata(ctx, t.md)

	// Non-idempotent mutations (SetCell with ServerTime) cannot retry. The
	// retry layer will surface the first attempt's error.
	maxAttempts := int32(10)
	if !mutationsAreRetryable(req.GetMutations()) {
		maxAttempts = 1
	}

	retryInterceptor := btransport.RetryingVRpc(btransport.RetryingOptions{
		MaxAttempts:       maxAttempts,
		InitialBackoff:    10 * time.Millisecond,
		MaxBackoff:        32 * time.Second,
		BackoffMultiplier: 1.5,
	})

	args := btransport.MutateRowArgs{
		RowKey:    string(req.GetKey()),
		Mutations: req.GetMutations(),
	}

	baseHandler := func(attemptCtx context.Context, request interface{}) (interface{}, error) {
		result, err := pool.pool.Invoke(attemptCtx, t.writeVRpcDesc, request)
		if err != nil {
			return nil, err
		}
		return result.Response, nil
	}

	ctx = btransport.WithVRpcMetadata(ctx, t.writeVRpcDesc.Method(), 0)
	chained := btransport.ChainInterceptors(retryInterceptor)
	if _, err := chained(ctx, args, baseHandler); err != nil {
		return nil, fmt.Errorf("session: MutateRow vRPC: %w", err)
	}
	return &btpb.SessionMutateRowResponse{}, nil
}

// Close releases whichever pools have been materialized, detaching any CCM
// listeners they registered before tearing down the underlying SessionPoolImpl.
// Pools that were never opened (because no read / write ever happened) are
// untouched.
func (t *sessionTable) Close() error {
	var firstErr error
	if t.readPool != nil {
		if err := t.readPool.close(); err != nil {
			firstErr = err
		}
	}
	if t.writePool != nil {
		if err := t.writePool.close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// mergeOutgoingMetadata attaches md to ctx's outgoing gRPC metadata, merging
// with any metadata already present.
func mergeOutgoingMetadata(ctx context.Context, md metadata.MD) context.Context {
	if existing, ok := metadata.FromOutgoingContext(ctx); ok {
		md = metadata.Join(existing, md)
	}
	return metadata.NewOutgoingContext(ctx, md)
}
