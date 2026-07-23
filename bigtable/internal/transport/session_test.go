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
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	spb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
	rpcstatus "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// --- shared test fixtures (used by both session_test.go and
// session_lifecycle_test.go) ---------------------------------------------------

// fakeStream implements Stream and exposes channels so tests can drive both
// sides of the conversation.
type fakeStream struct {
	sentMu    sync.Mutex
	sent      []*spb.SessionRequest
	recv      chan recvOp
	hdr       metadata.MD
	hdrErr    error
	sendFn    func(*spb.SessionRequest) error
	closeOnce sync.Once
}

type recvOp struct {
	resp *spb.SessionResponse
	err  error
}

func newFakeStream() *fakeStream {
	return &fakeStream{
		recv: make(chan recvOp, 32),
		hdr:  metadata.MD{},
	}
}

// Close unblocks Recv() by closing the recv channel. Idempotent so cleanup
// and explicit test teardown don't collide.
func (f *fakeStream) Close() {
	f.closeOnce.Do(func() { close(f.recv) })
}

func (f *fakeStream) Send(req *spb.SessionRequest) error {
	if f.sendFn != nil {
		if err := f.sendFn(req); err != nil {
			return err
		}
	}
	f.sentMu.Lock()
	f.sent = append(f.sent, req)
	f.sentMu.Unlock()
	return nil
}

func (f *fakeStream) Recv() (*spb.SessionResponse, error) {
	op, ok := <-f.recv
	if !ok {
		return nil, fmt.Errorf("stream closed")
	}
	return op.resp, op.err
}

func (f *fakeStream) Header() (metadata.MD, error) {
	return f.hdr, f.hdrErr
}

func (f *fakeStream) Context() context.Context {
	return context.Background()
}

func (f *fakeStream) snapshotSent() []*spb.SessionRequest {
	f.sentMu.Lock()
	defer f.sentMu.Unlock()
	out := make([]*spb.SessionRequest, len(f.sent))
	copy(out, f.sent)
	return out
}

// hookCounts captures lifecycle callbacks via a SessionHooks value.
type hookCounts struct {
	mu          sync.Mutex
	startCount  int
	activeCount int
	closeCount  int
	closeErr    error
}

func (c *hookCounts) hooks() SessionHooks {
	return SessionHooks{
		OnStart: func(context.Context) {
			c.mu.Lock()
			defer c.mu.Unlock()
			c.startCount++
		},
		OnActive: func(*Session) {
			c.mu.Lock()
			defer c.mu.Unlock()
			c.activeCount++
		},
		OnClose: func(_ *Session, err error) {
			c.mu.Lock()
			defer c.mu.Unlock()
			c.closeCount++
			c.closeErr = err
		},
	}
}

func (c *hookCounts) counts() (start, active, closed int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.startCount, c.activeCount, c.closeCount
}

// fakeDesc is a minimal VRpcDescriptor for Invoke tests.
type fakeDesc struct {
	method string
	enc    func(req interface{}) ([]byte, error)
	dec    func(buf []byte) (interface{}, error)
}

func (f *fakeDesc) Method() string                         { return f.method }
func (f *fakeDesc) Encode(req interface{}) ([]byte, error) { return f.enc(req) }
func (f *fakeDesc) Decode(buf []byte) (interface{}, error) { return f.dec(buf) }

func newTestSession(t *testing.T, stream Stream, hooks SessionHooks) *Session {
	t.Helper()
	return NewSession("test-session", stream, hooks, SessionTypeTable)
}

// waitFor polls cond every 5ms up to timeout, failing the test if cond never
// becomes true.
func waitFor(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out after %v waiting for: %s", timeout, msg)
}

// makeActive constructs a session and forces it into StateReady without going
// through the handshake.
func makeActive(t *testing.T, hooks SessionHooks) (*Session, *fakeStream) {
	t.Helper()
	stream := newFakeStream()
	s := newTestSession(t, stream, hooks)
	s.state.Store(int32(StateReady))
	return s, stream
}

// --- pure value tests --------------------------------------------------------

func TestMultiPlexingLimit(t *testing.T) {
	if multiPlexingLimit != 1 {
		t.Errorf("multiPlexingLimit = %d, want 1 (multiplexing unsupported)", multiPlexingLimit)
	}
}

func TestState_String(t *testing.T) {
	tests := []struct {
		s    State
		want string
	}{
		{StateNew, "New"},
		{StateStarting, "Starting"},
		{StateReady, "Ready"},
		{StateClosing, "Closing"},
		{StateWaitServerClose, "WaitServerClose"},
		{StateClosed, "Closed"},
		{State(99), "Unknown"},
	}
	for _, tt := range tests {
		if got := tt.s.String(); got != tt.want {
			t.Errorf("State(%d).String() = %q, want %q", int(tt.s), got, tt.want)
		}
	}
}

func TestNewSession_Defaults(t *testing.T) {
	stream := newFakeStream()
	s := NewSession("log", stream, SessionHooks{}, SessionTypeAuthorizedView)

	if got := s.State(); got != StateNew {
		t.Errorf("initial state = %v, want StateNew", got)
	}
	if got := s.LogName(); got != "log" {
		t.Errorf("LogName = %q, want %q", got, "log")
	}
	if got := s.sessionType; got != SessionTypeAuthorizedView {
		t.Errorf("sessionType = %v, want SessionTypeAuthorizedView", got)
	}
	if s.activeVRPC() != nil {
		t.Error("activeRPC slot should start nil")
	}
	if s.quiescent == nil {
		t.Error("quiescent channel not initialized")
	}
	if got := time.Duration(s.heartbeatIntervalNano.Load()); got != defaultHeartbeatInterval {
		t.Errorf("heartbeatInterval = %v, want %v", got, defaultHeartbeatInterval)
	}
	if s.PeerInfo() != nil {
		t.Error("PeerInfo should start nil")
	}
	if s.RefreshConfig() != nil {
		t.Error("RefreshConfig should start nil")
	}
	if s.HasOkRpcs() || s.HasErrorRpcs() {
		t.Error("HasOkRpcs/HasErrorRpcs should start false")
	}
}

func TestUnavailable_WrapsCauseAndStatus(t *testing.T) {
	err := unavailable(ErrUnavailableHeartBeatMissed, "heartbeat dead for %s", "test")

	if !errors.Is(err, ErrUnavailableHeartBeatMissed) {
		t.Error("errors.Is should match ErrUnavailableHeartBeatMissed")
	}
	if errors.Is(err, ErrUnavailableGoAway) {
		t.Error("errors.Is should not match unrelated sentinel")
	}
	if code := status.Code(err); code != codes.Unavailable {
		t.Errorf("status.Code = %v, want Unavailable", code)
	}
	if msg := err.Error(); msg == "" {
		t.Error("error string should be non-empty")
	}

	// nil cause should not crash and should still be Unavailable.
	err = unavailable(nil, "no cause")
	if code := status.Code(err); code != codes.Unavailable {
		t.Errorf("nil-cause: status.Code = %v, want Unavailable", code)
	}
	if errors.Is(err, ErrUnavailableHeartBeatMissed) {
		t.Error("nil-cause: errors.Is should not match any sentinel")
	}
}

// --- vRPC dispatch tests (handleVRPCResponse / handleVRPCErrorResponse) ------

func TestHandleVRPCResponse_RoutesByRpcID(t *testing.T) {
	s, _ := makeActive(t, SessionHooks{})

	rpc := &vrpcImpl{id: 7, method: "ReadRow", resultChan: make(chan vrpcResult, 1)}
	s.setSlotForTest(rpc)

	resp := &spb.VirtualRpcResponse{RpcId: 7, Payload: []byte("p")}
	s.handleVRPCResponse(resp)

	select {
	case res := <-rpc.resultChan:
		if res.resp != resp {
			t.Errorf("got resp %p, want %p", res.resp, resp)
		}
	default:
		t.Fatal("no result delivered")
	}
	if !s.HasOkRpcs() {
		t.Error("HasOkRpcs should be true after successful response")
	}
	// Unknown rpc_id is dropped silently.
	s.handleVRPCResponse(&spb.VirtualRpcResponse{RpcId: 999})
}

func TestHandleVRPCErrorResponse_RoutesByRpcID(t *testing.T) {
	s, _ := makeActive(t, SessionHooks{})

	rpc := &vrpcImpl{id: 3, method: "ReadRow", resultChan: make(chan vrpcResult, 1)}
	s.setSlotForTest(rpc)

	errResp := &spb.ErrorResponse{
		RpcId:  3,
		Status: &rpcstatus.Status{Code: int32(codes.FailedPrecondition), Message: "boom"},
	}
	s.handleVRPCErrorResponse(errResp)

	select {
	case res := <-rpc.resultChan:
		if res.errResp == nil {
			t.Fatal("expected errResp result")
		}
		if got := codes.Code(res.errResp.Status.GetCode()); got != codes.FailedPrecondition {
			t.Errorf("status code = %v, want FailedPrecondition", got)
		}
	default:
		t.Fatal("no result delivered")
	}
	if !s.HasErrorRpcs() {
		t.Error("HasErrorRpcs should be true after error response")
	}
}

// --- Invoke tests ------------------------------------------------------------

func newRoundTripDesc() *fakeDesc {
	return &fakeDesc{
		method: "RoundTrip",
		enc: func(req interface{}) ([]byte, error) {
			return []byte(fmt.Sprintf("req:%v", req)), nil
		},
		dec: func(buf []byte) (interface{}, error) {
			return string(buf), nil
		},
	}
}

func TestInvoke_RejectsWhenNotActive(t *testing.T) {
	stream := newFakeStream()
	s := newTestSession(t, stream, SessionHooks{}) // state = New
	_, err := s.Invoke(context.Background(), newRoundTripDesc(), "hello")
	if !errors.Is(err, ErrSessionNotActive) {
		t.Errorf("err = %v, want ErrSessionNotActive in chain", err)
	}
	if code := status.Code(err); code != codes.Unavailable {
		t.Errorf("status.Code = %v, want Unavailable", code)
	}
}

func TestInvoke_HappyPath(t *testing.T) {
	s, stream := makeActive(t, SessionHooks{})
	desc := newRoundTripDesc()

	done := make(chan struct{})
	var res InvokeResult
	var execErr error
	go func() {
		defer close(done)
		res, execErr = s.Invoke(context.Background(), desc, "hello")
	}()

	waitFor(t, time.Second, func() bool { return len(stream.snapshotSent()) > 0 }, "Send called")

	sent := stream.snapshotSent()[0].GetVirtualRpc()
	if sent == nil {
		t.Fatal("sent frame is not a VirtualRpcRequest")
	}
	if string(sent.Payload) != "req:hello" {
		t.Errorf("encoded payload = %q, want %q", sent.Payload, "req:hello")
	}

	s.handleVRPCResponse(&spb.VirtualRpcResponse{
		RpcId:       sent.RpcId,
		Payload:     []byte("world"),
		ClusterInfo: &spb.ClusterInformation{ClusterId: "c1"},
	})

	<-done
	if execErr != nil {
		t.Fatalf("Invoke error: %v", execErr)
	}
	if got := res.Response.(string); got != "world" {
		t.Errorf("resp = %q, want %q", got, "world")
	}
	if res.ClusterInfo == nil || res.ClusterInfo.ClusterId != "c1" {
		t.Errorf("clusterInfo = %v, want ClusterId=c1", res.ClusterInfo)
	}
}

func TestInvoke_ContextCancel(t *testing.T) {
	s, _ := makeActive(t, SessionHooks{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := s.Invoke(ctx, newRoundTripDesc(), "hello")
	if !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, want context.Canceled", err)
	}
}

func TestInvoke_SendFailureCleansUpMap(t *testing.T) {
	stream := newFakeStream()
	stream.sendFn = func(req *spb.SessionRequest) error {
		return fmt.Errorf("network down")
	}
	s := newTestSession(t, stream, SessionHooks{})
	s.state.Store(int32(StateReady))

	_, err := s.Invoke(context.Background(), newRoundTripDesc(), "hello")
	if err == nil {
		t.Fatal("expected error from failed Send")
	}
	if s.activeVRPC() != nil {
		t.Error("activeRPC slot should be cleared by defer on Send failure")
	}
}

// TestInvoke_ConcurrentSecondFailsWithSlotBusy: SESSION_SPEC #2 —
// single-in-flight-vRPC invariant guarded by claimSlot under slotMu (Java
// SessionImpl.startRpc L423 parity). A caller entering Invoke while the slot
// is still claimed MUST get ErrSessionNotActive tagged StateUncommitted with
// a "busy" diagnostic so the retry oracle steers to another session.
func TestInvoke_ConcurrentSecondFailsWithSlotBusy(t *testing.T) {
	s, _ := makeActive(t, SessionHooks{})

	// Pin the slot with a placeholder in-flight vRPC.
	s.setSlotForTest(&vrpcImpl{id: 999, resultChan: make(chan vrpcResult, 1)})

	_, err := s.Invoke(context.Background(), newRoundTripDesc(), "req")
	if err == nil {
		t.Fatal("expected error when Invoke enters with activeRPC already claimed")
	}
	if !errors.Is(err, ErrSessionNotActive) {
		t.Errorf("err = %v, want wrapping ErrSessionNotActive", err)
	}
	if got := ClassifyErr(err).State; got != StateUncommitted {
		t.Errorf("AttemptState = %v, want StateUncommitted (attempt never reached wire → retryable)", got)
	}
	if !strings.Contains(err.Error(), "busy") {
		t.Errorf("err message = %q, want to name the session-busy condition for operator grep", err.Error())
	}
}

// --- AfeID -------------------------------------------------------------------

func TestSessionAfeID(t *testing.T) {
	for _, tc := range []struct {
		name     string
		peerInfo *spb.PeerInfo
		want     afeID
	}{
		{"nil-peer-info", nil, 0},
		{"empty-afe-id", &spb.PeerInfo{}, 0},
		{"set-afe-id", &spb.PeerInfo{ApplicationFrontendId: 4242}, 4242},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestSession(t, newFakeStream(), SessionHooks{})
			if tc.peerInfo != nil {
				s.peerInfo.Store(tc.peerInfo)
			}
			if got := s.AfeID(); got != tc.want {
				t.Errorf("AfeID() = %d, want %d", got, tc.want)
			}
		})
	}
}
