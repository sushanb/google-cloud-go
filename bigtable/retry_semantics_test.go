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

// EXPERIMENT-BRANCH FILE — probe/mutation-retry-semantics. Promoted to
// feat/bigtable-sessionz-debug only when the coverage it adds is
// non-redundant with existing tests.
//
// Scope after de-duplication: end-to-end MutateRow retry via the
// classic path, wired through a mockserver that injects Unavailable
// on the first N attempts. This is the only layer that lacked a
// deterministic retry test — session-side retry mechanics are already
// covered in-tree at three lower layers:
//
//   internal/transport/vrpc_test.go
//     TestRetryingVRpc_TransportFailureIdempotent
//     TestRetryingVRpc_TransportFailureNonIdempotentNoRetry
//     TestRetryingVRpc_UncommittedAlwaysRetries
//     TestRetryingVRpc_ServerResultNotRetriedByDefault (5 codes)
//     TestRetryingVRpc_ServerDeadlineExceededNoRetryByDefault
//     TestRetryingVRpc_MaxAttemptsExceeded / HonorServerRetryDelay / ...
//
//   internal/session/table_test.go
//     TestSessionTableMutateRow_IdempotencyFlowsToRetry
//     TestSessionTableMutateRow_CtxDoneStopsAtOneAttempt
//
//   internal/transport/session_lifecycle_test.go
//     TestHeartBeatLoop_ForceClosesOnMissedHeartbeat
//
// The missed-heartbeat critical case sushanb asked about is composed
// from two of the above:
//   - HeartBeatLoop → ForceClose is proven at the transport layer.
//   - ForceClose tags in-flight vRPCs as StateTransportFailure at
//     session_vrpc.go:420 (cancelActiveRPCs).
//   - The retry interceptor then treats StateTransportFailure per
//     Idempotent gate — TestRetryingVRpc_TransportFailure{Idempotent,
//     NonIdempotentNoRetry} pin that behavior.
// End-to-end sandbox proof of that chain still needs a mockserver
// that speaks OpenSession bidi; deferred as follow-up.

package bigtable

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	btpb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
	"cloud.google.com/go/bigtable/internal/mockserver"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// faultyMutateRow returns a MutateRowFn that fails the first failN
// attempts with the given code, then succeeds. attemptCount is
// incremented on every call so tests can assert exact attempts.
func faultyMutateRow(attemptCount *int64, failN int, code codes.Code) func(context.Context, *btpb.MutateRowRequest) (*btpb.MutateRowResponse, error) {
	return func(_ context.Context, _ *btpb.MutateRowRequest) (*btpb.MutateRowResponse, error) {
		n := atomic.AddInt64(attemptCount, 1)
		if int(n) <= failN {
			return nil, status.Error(code, "injected fault")
		}
		return &btpb.MutateRowResponse{}, nil
	}
}

// newTestClient wires a real bigtable.Client at the mockserver with
// unauthenticated insecure creds. Installs a no-op PingAndWarm
// handler on srv so the connection pool factory prime step succeeds.
func newTestClient(t *testing.T, srv *mockserver.Server) *Client {
	t.Helper()
	if srv.PingAndWarmFn == nil {
		srv.PingAndWarmFn = func(context.Context, *btpb.PingAndWarmRequest) (*btpb.PingAndWarmResponse, error) {
			return &btpb.PingAndWarmResponse{}, nil
		}
	}
	ctx := context.Background()
	client, err := NewClient(ctx, "proj", "inst",
		option.WithEndpoint(srv.Addr),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		option.WithoutAuthentication())
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	return client
}

// TestRetrySemantics_Classic_Idempotent asserts that a mutation with
// only explicit-timestamp cells retries through the standard
// {Unavailable, DeadlineExceeded, Aborted} classic retry option
// (bigtable.go: mutationsAreRetryable → t.c.retryOption). No test in
// tree drives this end-to-end today; TestMutationsAreRetryable at
// bigtable_test.go tests only the predicate.
func TestRetrySemantics_Classic_Idempotent(t *testing.T) {
	srv, err := mockserver.NewServer("localhost:0")
	if err != nil {
		t.Fatalf("mockserver: %v", err)
	}
	defer srv.Close()

	var attempts int64
	srv.MutateRowFn = faultyMutateRow(&attempts, 2, codes.Unavailable)

	client := newTestClient(t, srv)
	defer client.Close()
	tbl := client.OpenTable("t")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	mut := NewMutation()
	mut.Set("cf", "q", Time(time.Unix(0, 1_000_000)), []byte("v")) // explicit ts → idempotent
	err = tbl.Apply(ctx, "k", mut)
	if err != nil {
		t.Fatalf("Apply: got err=%v, want nil (should have retried past 2 faults)", err)
	}
	if got := atomic.LoadInt64(&attempts); got != 3 {
		t.Errorf("attempts=%d, want 3 (2 injected fails + 1 success)", got)
	}
}

// TestRetrySemantics_Classic_NonIdempotent asserts that a mutation
// with any ServerTime cell does NOT attach the retry option, so a
// transient Unavailable propagates to the caller after the first
// attempt.
func TestRetrySemantics_Classic_NonIdempotent(t *testing.T) {
	srv, err := mockserver.NewServer("localhost:0")
	if err != nil {
		t.Fatalf("mockserver: %v", err)
	}
	defer srv.Close()

	var attempts int64
	srv.MutateRowFn = faultyMutateRow(&attempts, 2, codes.Unavailable)

	client := newTestClient(t, srv)
	defer client.Close()
	tbl := client.OpenTable("t")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	mut := NewMutation()
	mut.Set("cf", "q", ServerTime, []byte("v")) // ServerTime → non-idempotent
	err = tbl.Apply(ctx, "k", mut)
	if err == nil {
		t.Fatalf("Apply: got err=nil, want Unavailable (non-idempotent should not retry)")
	}
	if got := status.Code(err); got != codes.Unavailable {
		t.Errorf("err code=%s, want Unavailable", got)
	}
	if got := atomic.LoadInt64(&attempts); got != 1 {
		t.Errorf("attempts=%d, want 1 (non-idempotent must not retry)", got)
	}
}

// TestRetrySemantics_Classic_ServerResultRetries documents the classic
// behavior for reference: a bare server-returned Aborted is retried on
// classic (via clientOnlyRetry). This is a KNOWN divergence from the
// session path — session's shouldRetryDefault classifies any bare
// error as StateServerResult (never retry). See
// internal/transport/vrpc_test.go::TestRetryingVRpc_ServerResultNotRetriedByDefault
// for the session-side counter-assertion.
func TestRetrySemantics_Classic_ServerResultRetries(t *testing.T) {
	srv, err := mockserver.NewServer("localhost:0")
	if err != nil {
		t.Fatalf("mockserver: %v", err)
	}
	defer srv.Close()

	// Server returns Aborted (in the classic idempotent retry set).
	var attempts int64
	srv.MutateRowFn = faultyMutateRow(&attempts, 1, codes.Aborted)

	client := newTestClient(t, srv)
	defer client.Close()
	tbl := client.OpenTable("t")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	mut := NewMutation()
	mut.Set("cf", "q", Time(time.Unix(0, 1_000_000)), []byte("v"))
	err = tbl.Apply(ctx, "k", mut)
	if err != nil {
		t.Fatalf("Apply: got err=%v, want nil", err)
	}
	if got := atomic.LoadInt64(&attempts); got < 2 {
		t.Errorf("attempts=%d, want >=2 (classic retries Aborted for idempotent)", got)
	}
}
