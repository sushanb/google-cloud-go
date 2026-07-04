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
	"testing"

	spb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
	rpcstatus "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
)

func TestSession_CountsOkAndErrorRpcs(t *testing.T) {
	s, _ := makeActive(t, SessionHooks{})

	for _, id := range []int64{1, 2, 3} {
		rpc := &vrpcImpl{id: id, method: "ReadRow", resultChan: make(chan vrpcResult, 1)}
		s.activeRPC.Store(rpc)
		s.handleVRPCResponse(&spb.VirtualRpcResponse{RpcId: id, Payload: []byte("p")})
		s.activeRPC.Store(nil)
	}
	for _, id := range []int64{10, 11} {
		rpc := &vrpcImpl{id: id, method: "ReadRow", resultChan: make(chan vrpcResult, 1)}
		s.activeRPC.Store(rpc)
		s.handleVRPCErrorResponse(&spb.ErrorResponse{
			RpcId:  id,
			Status: &rpcstatus.Status{Code: int32(codes.Unavailable), Message: "boom"},
		})
		s.activeRPC.Store(nil)
	}

	if got := s.OkRpcs(); got != 3 {
		t.Errorf("OkRpcs = %d, want 3", got)
	}
	if got := s.ErrorRpcs(); got != 2 {
		t.Errorf("ErrorRpcs = %d, want 2", got)
	}
	if !s.HasOkRpcs() || !s.HasErrorRpcs() {
		t.Error("HasOkRpcs/HasErrorRpcs should be true once counters are non-zero")
	}

	// Unknown rpc_id is dropped silently and must not bump either counter.
	s.handleVRPCResponse(&spb.VirtualRpcResponse{RpcId: 999})
	s.handleVRPCErrorResponse(&spb.ErrorResponse{
		RpcId:  999,
		Status: &rpcstatus.Status{Code: int32(codes.Unavailable), Message: "ghost"},
	})
	if got := s.OkRpcs(); got != 3 {
		t.Errorf("OkRpcs after dropped frame = %d, want 3", got)
	}
	if got := s.ErrorRpcs(); got != 2 {
		t.Errorf("ErrorRpcs after dropped frame = %d, want 2", got)
	}
}

func TestSession_Snapshot_BasicFields(t *testing.T) {
	s, _ := makeActive(t, SessionHooks{})
	s.peerInfo.Store(&spb.PeerInfo{
		GoogleFrontendId:           42,
		ApplicationFrontendId:      99,
		ApplicationFrontendRegion:  "us-central1",
		ApplicationFrontendSubzone: "us-central1-b1",
		TransportType:              spb.PeerInfo_TRANSPORT_TYPE_SESSION_DIRECT_ACCESS,
	})
	s.okRpcs.Store(5)
	s.errorRpcs.Store(1)

	snap := s.Snapshot()
	if snap.State != "Ready" {
		t.Errorf("State = %q, want Active", snap.State)
	}
	if snap.LogName != "test-session" {
		t.Errorf("LogName = %q, want test-session", snap.LogName)
	}
	if snap.OkRpcs != 5 || snap.ErrorRpcs != 1 {
		t.Errorf("Ok/Err = %d/%d, want 5/1", snap.OkRpcs, snap.ErrorRpcs)
	}
	if snap.Peer.GoogleFrontendID != 42 {
		t.Errorf("Peer.GoogleFrontendID = %d, want 42", snap.Peer.GoogleFrontendID)
	}
	if snap.Peer.ApplicationFrontendRegion != "us-central1" {
		t.Errorf("Peer.ApplicationFrontendRegion = %q, want us-central1", snap.Peer.ApplicationFrontendRegion)
	}
	if snap.Peer.TransportType != "TRANSPORT_TYPE_SESSION_DIRECT_ACCESS" {
		t.Errorf("Peer.TransportType = %q, want TRANSPORT_TYPE_SESSION_DIRECT_ACCESS", snap.Peer.TransportType)
	}
	if snap.SessionType != "table" {
		t.Errorf("SessionType = %q, want table", snap.SessionType)
	}
}

func TestSession_Snapshot_NilPeer(t *testing.T) {
	s, _ := makeActive(t, SessionHooks{})
	snap := s.Snapshot()
	// Nil peer is reported as the zero PeerInfoSnapshot so the UI can render
	// a dash; we deliberately do NOT synthesize "TRANSPORT_TYPE_UNKNOWN" here.
	if snap.Peer.TransportType != "" {
		t.Errorf("nil peer TransportType = %q, want empty string", snap.Peer.TransportType)
	}
	if snap.Peer.GoogleFrontendID != 0 {
		t.Errorf("nil peer GFE = %d, want 0", snap.Peer.GoogleFrontendID)
	}
}

func TestSessionHandle_Snapshot(t *testing.T) {
	s, _ := makeActive(t, SessionHooks{})
	h := NewSessionHandle(s)
	h.IncOutstanding()
	h.IncOutstanding()

	snap := h.Snapshot()
	if snap.Outstanding != 2 {
		t.Errorf("Outstanding = %d, want 2", snap.Outstanding)
	}
}

func TestPoolSnapshot_AggregatesSessions(t *testing.T) {
	pool := NewSessionPoolImpl("test:read", 1, 5, nil, nil, nil, SessionTypeTable)

	// Two active sessions, one with traffic.
	for i := 0; i < 2; i++ {
		stream := newFakeStream()
		s := NewSession("s", stream, SessionHooks{}, SessionTypeTable)
		s.state.Store(int32(StateReady))
		sh := NewSessionHandle(s)
		pool.sessions = append(pool.sessions, sh)
	}
	pool.sessions[0].IncOutstanding()
	pool.sessions[0].session.okRpcs.Store(10)
	pool.sessions[1].session.errorRpcs.Store(3)

	snap := pool.PoolSnapshot()
	if snap.Name != "test:read" {
		t.Errorf("Name = %q, want test:read", snap.Name)
	}
	if snap.TotalSessions != 2 || snap.ReadyCount != 2 {
		t.Errorf("Total/Ready = %d/%d, want 2/2", snap.TotalSessions, snap.ReadyCount)
	}
	if snap.InUseCount != 1 || snap.PendingCount != 1 {
		t.Errorf("InUse/Pending = %d/%d, want 1/1", snap.InUseCount, snap.PendingCount)
	}
	if snap.PickerType == "" {
		t.Errorf("PickerType empty; expected a reflected type name")
	}
	if len(snap.Sessions) != 2 {
		t.Fatalf("Sessions len = %d, want 2", len(snap.Sessions))
	}
	if snap.Sessions[0].OkRpcs != 10 || snap.Sessions[1].ErrorRpcs != 3 {
		t.Errorf("counts not propagated: %+v", snap.Sessions)
	}
}
