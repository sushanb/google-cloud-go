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
	"fmt"
	"testing"
	"time"

	spb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	rpcstatus "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
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

// --- ExecuteVRpcEx: Stats, SentAt, RetryInfo plumbing ------------------------

// execVRpcEx drives an ExecuteVRpcEx call to completion by waiting for the
// outbound frame, calling deliver(sent) to produce the server response, and
// returning the populated ExecuteResult + err. It mirrors runVRpcCapture but
// returns the full ExecuteResult so tests can assert on Stats / SentAt /
// ClusterInfo without the back-compat triple stripping them.
func execVRpcEx(t *testing.T, ctx context.Context, deliver func(s *Session, sent *spb.VirtualRpcRequest)) (ExecuteResult, error) {
	t.Helper()
	s, stream := makeActive(t, SessionHooks{})
	desc := newRoundTripDesc()

	type outcome struct {
		res ExecuteResult
		err error
	}
	resCh := make(chan outcome, 1)
	go func() {
		r, err := s.ExecuteVRpcEx(ctx, desc, "hello")
		resCh <- outcome{res: r, err: err}
	}()

	waitFor(t, time.Second, func() bool { return len(stream.snapshotSent()) > 0 }, "Send called")
	sent := stream.snapshotSent()[0].GetVirtualRpc()
	if sent == nil {
		t.Fatal("sent frame was not a VirtualRpcRequest")
	}
	deliver(s, sent)
	select {
	case out := <-resCh:
		return out.res, out.err
	case <-time.After(time.Second):
		t.Fatal("ExecuteVRpcEx did not return after delivering response")
		return ExecuteResult{}, nil
	}
}

func TestExecuteVRpcEx_StatsExtractedFromResponse(t *testing.T) {
	const wantBackend = 123 * time.Millisecond

	before := time.Now()
	res, err := execVRpcEx(t, context.Background(), func(s *Session, sent *spb.VirtualRpcRequest) {
		s.handleVRPCResponse(&spb.VirtualRpcResponse{
			RpcId:   sent.RpcId,
			Payload: []byte("ok"),
			Stats: &spb.SessionRequestStats{
				BackendLatency: durationpb.New(wantBackend),
			},
		})
	})
	if err != nil {
		t.Fatalf("ExecuteVRpcEx: unexpected err %v", err)
	}
	if res.Stats == nil {
		t.Fatal("ExecuteResult.Stats = nil, want non-nil (server-supplied Stats must be propagated)")
	}
	if got := res.Stats.GetBackendLatency().AsDuration(); got != wantBackend {
		t.Errorf("ExecuteResult.Stats.BackendLatency = %v, want %v", got, wantBackend)
	}
	// Sanity bookend on SentAt.
	if res.SentAt.Before(before) {
		t.Errorf("SentAt %v < before %v", res.SentAt, before)
	}
}

func TestExecuteVRpcEx_StatsNilWhenServerOmits(t *testing.T) {
	res, err := execVRpcEx(t, context.Background(), func(s *Session, sent *spb.VirtualRpcRequest) {
		s.handleVRPCResponse(&spb.VirtualRpcResponse{
			RpcId:   sent.RpcId,
			Payload: []byte("ok"),
			// Stats intentionally nil.
		})
	})
	if err != nil {
		t.Fatalf("ExecuteVRpcEx: unexpected err %v", err)
	}
	if res.Stats != nil {
		t.Errorf("ExecuteResult.Stats = %v, want nil when server omits Stats", res.Stats)
	}
}

func TestExecuteVRpcEx_SentAtIsNonZero(t *testing.T) {
	before := time.Now()
	res, err := execVRpcEx(t, context.Background(), func(s *Session, sent *spb.VirtualRpcRequest) {
		s.handleVRPCResponse(&spb.VirtualRpcResponse{
			RpcId:   sent.RpcId,
			Payload: []byte("ok"),
		})
	})
	after := time.Now()
	if err != nil {
		t.Fatalf("ExecuteVRpcEx: unexpected err %v", err)
	}
	if res.SentAt.IsZero() {
		t.Fatal("SentAt is zero after a successful send; want non-zero")
	}
	// SentAt must land in the [before, after] window (with a small slop to
	// tolerate clock skew between samples).
	const slop = 2 * time.Second
	if res.SentAt.Before(before.Add(-slop)) || res.SentAt.After(after.Add(slop)) {
		t.Errorf("SentAt = %v, want within [%v, %v]", res.SentAt, before, after)
	}
}

func TestExecuteVRpcEx_SentAtIsSetEvenOnSendFailure(t *testing.T) {
	// session_vrpc.go captures SentAt *before* calling Send. That is the
	// contract callers rely on for client-blocking-latency math — we record
	// when we attempted to hand the frame off, not whether the handoff
	// succeeded. This test pins that contract: a Send-failure must still
	// return a non-zero SentAt.
	stream := newFakeStream()
	stream.sendFn = func(req *spb.SessionRequest) error {
		return fmt.Errorf("network down")
	}
	s := newTestSession(t, stream, SessionHooks{})
	s.mu.Lock()
	s.state = StateActive
	s.mu.Unlock()

	before := time.Now()
	res, err := s.ExecuteVRpcEx(context.Background(), newRoundTripDesc(), "hello")
	after := time.Now()
	if err == nil {
		t.Fatal("ExecuteVRpcEx: expected error from failing Send, got nil")
	}
	if res.SentAt.IsZero() {
		t.Errorf("SentAt is zero after Send failure; want non-zero (session_vrpc.go captures SentAt before calling Send)")
	}
	const slop = 2 * time.Second
	if res.SentAt.Before(before.Add(-slop)) || res.SentAt.After(after.Add(slop)) {
		t.Errorf("SentAt = %v on Send-failure path; want within [%v, %v]", res.SentAt, before, after)
	}
}

func TestExecuteVRpcEx_ClusterInfoOnErrorPath(t *testing.T) {
	res, err := execVRpcEx(t, context.Background(), func(s *Session, sent *spb.VirtualRpcRequest) {
		s.handleVRPCErrorResponse(&spb.ErrorResponse{
			RpcId:       sent.RpcId,
			Status:      &rpcstatus.Status{Code: int32(codes.FailedPrecondition), Message: "boom"},
			ClusterInfo: &spb.ClusterInformation{ClusterId: "c1", ZoneId: "z1"},
		})
	})
	if err == nil {
		t.Fatal("ExecuteVRpcEx returned nil err on ErrorResponse path; want non-nil")
	}
	if res.ClusterInfo == nil {
		t.Fatal("ExecuteResult.ClusterInfo = nil on error path; want it preserved from ErrorResponse.ClusterInfo")
	}
	if got := res.ClusterInfo.ClusterId; got != "c1" {
		t.Errorf("ClusterInfo.ClusterId = %q, want %q", got, "c1")
	}
	if got := res.ClusterInfo.ZoneId; got != "z1" {
		t.Errorf("ClusterInfo.ZoneId = %q, want %q", got, "z1")
	}
}

func TestExecuteVRpcEx_RetryInfoPackedIntoStatus(t *testing.T) {
	// Marquee test for the RetryInfo plumbing in handleVRPCErrorResponse:
	// the server-supplied RetryInfo on ErrorResponse must be packed into the
	// status details so downstream consumers can recover it via
	// status.FromError(err).Details(). RetryingVRpc reads it from there.
	const wantDelay = 250 * time.Millisecond

	_, err := execVRpcEx(t, context.Background(), func(s *Session, sent *spb.VirtualRpcRequest) {
		s.handleVRPCErrorResponse(&spb.ErrorResponse{
			RpcId:     sent.RpcId,
			Status:    &rpcstatus.Status{Code: int32(codes.Unavailable), Message: "boom"},
			RetryInfo: &errdetails.RetryInfo{RetryDelay: durationpb.New(wantDelay)},
		})
	})
	if err == nil {
		t.Fatal("ExecuteVRpcEx returned nil err on ErrorResponse path; want non-nil")
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("status.FromError(%v) returned ok=false; want a gRPC status error", err)
	}
	if st.Code() != codes.Unavailable {
		t.Errorf("status.Code = %v, want Unavailable", st.Code())
	}
	var found *errdetails.RetryInfo
	for _, d := range st.Details() {
		if ri, ok := d.(*errdetails.RetryInfo); ok {
			found = ri
			break
		}
	}
	if found == nil {
		t.Fatalf("RetryInfo not found in status.Details (%d details total); the handleVRPCErrorResponse plumbing failed to attach it", len(st.Details()))
	}
	if got := found.GetRetryDelay().AsDuration(); got != wantDelay {
		t.Errorf("RetryInfo.RetryDelay = %v, want %v", got, wantDelay)
	}
}

func TestExecuteVRpc_BackCompatWrapperReturnsTriple(t *testing.T) {
	// The back-compat wrapper must still return (resp, clusterInfo, err)
	// drawn from the same ExecuteResult that ExecuteVRpcEx returns.
	s, stream := makeActive(t, SessionHooks{})
	desc := newRoundTripDesc()

	type outcome struct {
		resp    interface{}
		cluster *spb.ClusterInformation
		err     error
	}
	resCh := make(chan outcome, 1)
	go func() {
		r, c, err := s.ExecuteVRpc(context.Background(), desc, "hello")
		resCh <- outcome{resp: r, cluster: c, err: err}
	}()

	waitFor(t, time.Second, func() bool { return len(stream.snapshotSent()) > 0 }, "Send called")
	sent := stream.snapshotSent()[0].GetVirtualRpc()
	s.handleVRPCResponse(&spb.VirtualRpcResponse{
		RpcId:       sent.RpcId,
		Payload:     []byte("world"),
		ClusterInfo: &spb.ClusterInformation{ClusterId: "cwrap"},
		Stats:       &spb.SessionRequestStats{BackendLatency: durationpb.New(time.Millisecond)},
	})

	var out outcome
	select {
	case out = <-resCh:
	case <-time.After(time.Second):
		t.Fatal("ExecuteVRpc did not return")
	}
	if out.err != nil {
		t.Fatalf("ExecuteVRpc err = %v, want nil", out.err)
	}
	if got, _ := out.resp.(string); got != "world" {
		t.Errorf("resp = %v, want %q", out.resp, "world")
	}
	if out.cluster == nil || out.cluster.ClusterId != "cwrap" {
		t.Errorf("clusterInfo = %v, want ClusterId=cwrap", out.cluster)
	}
}

