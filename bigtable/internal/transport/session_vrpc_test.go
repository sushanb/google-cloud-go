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

package internal

import (
	"context"
	"testing"
	"time"

	spb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
)

// runVRpcCapture drives an ExecuteVRpc call to completion with a stub
// VirtualRpcResponse, then returns the *VirtualRpcRequest that was sent on the
// wire. All inspection of the deadline / metadata plumbing is done against
// this snapshot.
func runVRpcCapture(t *testing.T, ctx context.Context) *spb.VirtualRpcRequest {
	t.Helper()
	s, stream := makeActive(t, SessionHooks{})
	desc := newRoundTripDesc()

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _, _ = s.ExecuteVRpc(ctx, desc, "hello")
	}()

	waitFor(t, time.Second, func() bool { return len(stream.snapshotSent()) > 0 }, "Send called")
	sent := stream.snapshotSent()[0].GetVirtualRpc()
	if sent == nil {
		t.Fatal("sent frame was not a VirtualRpcRequest")
	}

	// Deliver a benign success so ExecuteVRpc returns and the goroutine exits.
	s.handleVRPCResponse(&spb.VirtualRpcResponse{
		RpcId:   sent.RpcId,
		Payload: []byte("ok"),
	})
	<-done
	return sent
}

func TestExecuteVRpc_DeadlineCarriedInRequest(t *testing.T) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(500*time.Millisecond))
	defer cancel()

	sent := runVRpcCapture(t, ctx)
	if sent.Deadline == nil {
		t.Fatal("VirtualRpc.Deadline = nil, want non-nil when ctx carries a deadline")
	}
	d := sent.Deadline.AsDuration()
	if d <= 0 || d > 500*time.Millisecond {
		t.Errorf("VirtualRpc.Deadline = %v, want in (0, 500ms]", d)
	}
	// A negative or absurdly small value would imply we lost most of the
	// budget to test setup; sanity-bound it.
	if d < time.Microsecond {
		t.Errorf("VirtualRpc.Deadline = %v, suspiciously small", d)
	}
}

func TestExecuteVRpc_NoDeadlineWhenCtxHasNone(t *testing.T) {
	sent := runVRpcCapture(t, context.Background())
	if sent.Deadline != nil {
		t.Errorf("VirtualRpc.Deadline = %v, want nil when ctx has no deadline", sent.Deadline.AsDuration())
	}
}

func TestExecuteVRpc_AttemptNumberFromCtx(t *testing.T) {
	// WithAttempt is a no-op unless WithVRpcMetadata has seeded the context
	// with a vrpcMetadata struct first — that's how the retrying interceptor
	// uses it in production (the retry loop seeds attempt=1 on entry, then
	// updates via WithAttempt on each retry).
	ctx := WithVRpcMetadata(context.Background(), "ReadRow", 1)
	ctx = WithAttempt(ctx, 7)
	sent := runVRpcCapture(t, ctx)
	if sent.Metadata == nil {
		t.Fatal("VirtualRpc.Metadata = nil")
	}
	if got := sent.Metadata.AttemptNumber; got != 7 {
		t.Errorf("AttemptNumber = %d, want 7", got)
	}
}

func TestExecuteVRpc_AttemptNumberDefault(t *testing.T) {
	sent := runVRpcCapture(t, context.Background())
	if sent.Metadata == nil {
		t.Fatal("VirtualRpc.Metadata = nil")
	}
	if got := sent.Metadata.AttemptNumber; got != 1 {
		t.Errorf("AttemptNumber = %d, want 1 (calls without WithAttempt should default to first attempt)", got)
	}
}

func TestExecuteVRpc_AttemptStartIsRecent(t *testing.T) {
	before := time.Now()
	sent := runVRpcCapture(t, context.Background())
	after := time.Now()

	if sent.Metadata == nil || sent.Metadata.AttemptStart == nil {
		t.Fatal("VirtualRpc.Metadata.AttemptStart = nil")
	}
	got := sent.Metadata.AttemptStart.AsTime()
	// Allow a small slop so wall-clock skew between captures doesn't flake.
	const slop = 2 * time.Second
	if got.Before(before.Add(-slop)) || got.After(after.Add(slop)) {
		t.Errorf("AttemptStart = %v, want within [%v, %v] (slop %v)", got, before, after, slop)
	}
}
