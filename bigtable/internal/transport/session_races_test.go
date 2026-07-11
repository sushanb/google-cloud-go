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

// Race-condition reproducers derived from the state-check bug scan.
//
// Convention: each test asserts the DESIRED behavior. Tests FAIL today
// on the pre-fix code and PASS once the corresponding fix lands. Each
// test is deterministic (channels for ordering, no time.Sleep on the
// critical path) so it behaves the same under `-race -count=1000`.

import (
	"context"
	"errors"
	"testing"
	"time"

	spb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
)

// TestRace_R2_InvokeSendsOnSessionRacingToClose reproduces the CRITICAL
// window between Invoke's state check at session_vrpc.go:87 and the CAS
// at :107. In that window, Encode runs — enough time for a concurrent
// handleGoAway → spawned s.Close to advance state through Closing to
// WaitServerClose. Without the fix, the CAS still succeeds (it only
// checks activeRPC) and Send fires on a session mid-close. On a real
// backend the server may commit the mutation before it sees our
// CloseSession, and then the response is dropped by handleVRPCResponse's
// defensive state whitelist — the caller sees Unavailable on a mutation
// that succeeded. Silent client/server divergence for non-idempotent
// Apply.
//
// The fix lives in session_lifecycle.go's SendVRpc (state re-check under
// sendMu, serialized with Close's Send(CloseSessionRequest)) plus the
// call-site translation in session_vrpc.go's Invoke (ErrSessionNotActive
// → StateUncommitted tag so RetryingVRpc silently re-picks).
//
// The test uses a paused Encode to open the window deterministically.
// A sendFn hook on fakeStream signals when (if) Send actually fires, so
// we can distinguish the two outcomes without polling:
//
//	pre-fix: Send fires; sendSignal channel receives; assertion FAILS.
//	post-fix: SendVRpc rejects; Invoke returns ErrSessionNotActive
//	          tagged StateUncommitted; sendSignal never receives.

func TestRace_R2_InvokeSendsOnSessionRacingToClose(t *testing.T) {
	stream := newFakeStream()
	t.Cleanup(stream.Close)

	// Signal when Send actually writes to the stream. Buffered so the
	// send goroutine doesn't block waiting for us to observe.
	sendSignal := make(chan struct{}, 1)
	stream.sendFn = func(*spb.SessionRequest) error {
		select {
		case sendSignal <- struct{}{}:
		default:
		}
		return nil
	}

	s := newTestSession(t, stream, SessionHooks{})
	s.state.Store(int32(StateReady))

	encodeEntered := make(chan struct{})
	encodeRelease := make(chan struct{})
	desc := &fakeDesc{
		method: "MutateRow",
		enc: func(interface{}) ([]byte, error) {
			close(encodeEntered)
			<-encodeRelease
			return []byte("mutate"), nil
		},
		dec: func([]byte) (interface{}, error) { return nil, nil },
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	invokeErr := make(chan error, 1)
	go func() {
		_, err := s.Invoke(ctx, desc, "mutate")
		invokeErr <- err
	}()

	<-encodeEntered
	// The R2 window. Invoke's line-87 state check already passed.
	// Simulate what handleGoAway → spawned s.Close achieves in
	// production: state advances all the way to WaitServerClose while
	// Encode is running. When Encode returns, Invoke's CAS at :107
	// succeeds (state is not re-checked there), and — pre-fix — Send
	// fires unconditionally.
	s.state.Store(int32(StateWaitServerClose))
	close(encodeRelease)

	// Wait for one of three outcomes:
	//   1. Send fires → pre-fix behavior, R2 present. Fail.
	//   2. Invoke returns with ErrSessionNotActive → post-fix behavior. Pass.
	//   3. Timeout → test setup wrong.
	select {
	case <-sendSignal:
		t.Errorf("R2 REPRODUCED: Send fired on session in state %v; desired: no Send after state left Ready",
			s.State())
		// Unblock Invoke, which is now waiting on resultChan.
		s.ForceClose(&spb.CloseSessionRequest{
			Reason:      spb.CloseSessionRequest_CLOSE_SESSION_REASON_ERROR,
			Description: "test cleanup",
		})
		select {
		case <-invokeErr:
		case <-time.After(2 * time.Second):
			t.Fatal("Invoke did not return after ForceClose")
		}
	case err := <-invokeErr:
		// R2 fix in effect. Verify the error surface: retry-transparent
		// (StateUncommitted) and wraps ErrSessionNotActive.
		if err == nil {
			t.Fatalf("Invoke returned nil err; expected ErrSessionNotActive")
		}
		if !errors.Is(err, ErrSessionNotActive) {
			t.Errorf("Invoke err = %v; want wrap of ErrSessionNotActive", err)
		}
		if got := ClassifyErr(err).State; got != StateUncommitted {
			t.Errorf("Invoke err state tag = %v; want StateUncommitted so RetryingVRpc silently re-picks",
				got)
		}
		// Sanity: no frame should have been sent.
		if len(stream.snapshotSent()) != 0 {
			t.Errorf("stream.sent = %d frame(s); want 0 (SendVRpc must reject before write)",
				len(stream.snapshotSent()))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("neither Send nor Invoke return within 2s")
	}
}

// TestRace_R1a_HandleGoAway_ChecksOutClosingSession exercises the R1a
// structural guarantee introduced by syncCtx: handleGoAway and Invoke's
// slot-claim are BOTH scheduled on the same per-session syncCtx, so a
// GOAWAY-observing Invoke either
//
//   - runs BEFORE handleGoAway (claims slot; the vRPC gets its response
//     during the drain grace period — Java parity), OR
//   - runs AFTER handleGoAway (observes StateClosing under ExecuteSync
//     and rejects with ErrSessionNotActive tagged StateUncommitted —
//     RetryingVRpc silently re-picks a fresh session).
//
// The interleaving "state==Ready observed → CAS succeeds → wire send
// fires while state has moved to Closing" is structurally impossible
// under the POC: the state read and CAS live inside the same
// ExecuteSync body, and the only migrated transition path
// (handleGoAway) queues on the same serializer.
//
// Test shape: fire a GOAWAY-triggering handleGoAway callback, then
// immediately call Invoke and assert the reject. On BASELINE without
// the syncCtx wiring, handleGoAway transitions state synchronously
// too (via transitionTo in the readLoop), so this test also passes on
// baseline — R1a's meaningful win is structural (removes reliance on
// SendVRpc's sendMu-scoped re-check), not observational. The test
// nails the STRUCTURAL post-condition: after handleGoAway returns,
// every subsequent Invoke on the same session rejects, and no vRPC
// frame reaches the wire.
func TestRace_R1a_HandleGoAway_ChecksOutClosingSession(t *testing.T) {
	stream := newFakeStream()
	t.Cleanup(stream.Close)

	// Signal (via a buffered chan) if a vRPC frame ever hits Send.
	// Filter for VirtualRpc payload only — CloseSession also flows through
	// Send (from handleGoAway's spawned Close goroutine) and would
	// otherwise trip the assertion.
	sendSignal := make(chan struct{}, 1)
	stream.sendFn = func(req *spb.SessionRequest) error {
		if _, ok := req.GetPayload().(*spb.SessionRequest_VirtualRpc); ok {
			select {
			case sendSignal <- struct{}{}:
			default:
			}
		}
		return nil
	}

	s := newTestSession(t, stream, SessionHooks{})
	// Reap the per-session syncCtx runner even though the test never
	// drives notifyClosed (fakeStream never EOFs). Without this the
	// runner goroutine leaks and shows up under -count=100 -race.
	t.Cleanup(func() { s.syncC.Shutdown() })
	s.state.Store(int32(StateReady))

	// Fire handleGoAway. Under the POC wiring this dispatches the state
	// transition through the syncCtx via ExecuteSync — by the time it
	// returns, state has advanced to Closing.
	s.handleGoAway(&spb.GoAwayResponse{
		Reason:      "test",
		Description: "R1a: server graceful drain",
	})

	// State should be non-Ready. Under -count=100 the spawned Close
	// goroutine has occasionally already advanced Closing→WaitServerClose
	// by the time we observe — either is fine; the R1a claim is "no longer
	// eligible for fresh vRPCs," not a specific terminal-bound state.
	if got := s.State(); got == StateReady {
		t.Fatalf("post-GOAWAY state = %v; want any non-Ready state", got)
	}

	desc := &fakeDesc{
		method: "MutateRow",
		enc:    func(interface{}) ([]byte, error) { return []byte("mutate"), nil },
		dec:    func([]byte) (interface{}, error) { return nil, nil },
	}
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	_, err := s.Invoke(ctx, desc, "mutate")
	if err == nil {
		t.Fatal("Invoke returned nil err after handleGoAway; want ErrSessionNotActive")
	}
	if !errors.Is(err, ErrSessionNotActive) {
		t.Errorf("Invoke err = %v; want wrap of ErrSessionNotActive", err)
	}
	if got := ClassifyErr(err).State; got != StateUncommitted {
		t.Errorf("Invoke err state tag = %v; want StateUncommitted so RetryingVRpc silently re-picks", got)
	}
	// R1a structural post-condition: no vRPC wire send occurred.
	// (snapshotSent counts every frame including handleGoAway's
	// CloseSessionRequest, so filter via sendSignal which only fires on
	// VirtualRpc payloads.)
	select {
	case <-sendSignal:
		t.Error("vRPC Send fired on a session post-handleGoAway; want no vRPC wire traffic")
	default:
	}
	var vrpcFrames int
	for _, f := range stream.snapshotSent() {
		if _, ok := f.GetPayload().(*spb.SessionRequest_VirtualRpc); ok {
			vrpcFrames++
		}
	}
	if vrpcFrames != 0 {
		t.Errorf("stream vRPC frames = %d; want 0", vrpcFrames)
	}
}
