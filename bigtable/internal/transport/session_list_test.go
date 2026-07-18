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
	"time"

	spb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
)

// makeHandleWithAfe creates a SessionHandle whose Session reports the given
// AfeID. Skips the handshake — sets PeerInfo directly.
func makeHandleWithAfe(t *testing.T, id afeID) *SessionHandle {
	t.Helper()
	s := newTestSession(t, newFakeStream(), SessionHooks{})
	s.peerInfo.Store(&spb.PeerInfo{ApplicationFrontendId: int64(id)})
	return NewSessionHandle(s, time.Time{})
}

func TestSessionList_OnSessionStarted_BucketsByAfe(t *testing.T) {
	sl := newSessionList()

	h1a := makeHandleWithAfe(t, 1)
	h1b := makeHandleWithAfe(t, 1)
	h2 := makeHandleWithAfe(t, 2)

	sl.OnSessionStarted(h1a)
	sl.OnSessionStarted(h1b)
	sl.OnSessionStarted(h2)

	if got := len(sl.afeHandles); got != 2 {
		t.Errorf("len(afeHandles) = %d, want 2", got)
	}
	if got := len(sl.afesWithReady); got != 2 {
		t.Errorf("len(afesWithReady) = %d, want 2", got)
	}
	if got := sl.afeHandles[1].refCount; got != 2 {
		t.Errorf("afe(1).refCount = %d, want 2", got)
	}
	if got := len(sl.afeHandles[1].sessions); got != 2 {
		t.Errorf("afe(1).sessions len = %d, want 2", got)
	}
	if got := sl.afeHandles[2].refCount; got != 1 {
		t.Errorf("afe(2).refCount = %d, want 1", got)
	}
}

func TestSessionList_OnSessionStarted_Idempotent(t *testing.T) {
	sl := newSessionList()
	h := makeHandleWithAfe(t, 7)

	sl.OnSessionStarted(h)
	sl.OnSessionStarted(h) // duplicate

	if got := sl.afeHandles[7].refCount; got != 1 {
		t.Errorf("refCount = %d, want 1 (dup register must be a no-op)", got)
	}
}

func TestSessionList_Checkout_DrainsAndRefills(t *testing.T) {
	sl := newSessionList()
	h1 := makeHandleWithAfe(t, 1)
	h2 := makeHandleWithAfe(t, 1)
	sl.OnSessionStarted(h1)
	sl.OnSessionStarted(h2)

	afe := sl.afeHandles[1]

	// First checkout: queue still has one → AFE stays in ready list.
	got1 := sl.Checkout(afe)
	if got1 == nil {
		t.Fatal("Checkout returned nil")
	}
	if len(sl.afesWithReady) != 1 {
		t.Errorf("afesWithReady len = %d, want 1 after first checkout", len(sl.afesWithReady))
	}

	// Second checkout: queue emptied → AFE drops from ready list.
	got2 := sl.Checkout(afe)
	if got2 == nil {
		t.Fatal("second Checkout returned nil")
	}
	if len(sl.afesWithReady) != 0 {
		t.Errorf("afesWithReady len = %d, want 0 after drain", len(sl.afesWithReady))
	}
	if got1 == got2 {
		t.Error("Checkout returned the same handle twice")
	}

	// Release the first: AFE re-enters ready list.
	sl.ReleaseToPool(got1)
	if len(sl.afesWithReady) != 1 {
		t.Errorf("afesWithReady len = %d, want 1 after release", len(sl.afesWithReady))
	}
	if len(afe.sessions) != 1 {
		t.Errorf("afe.sessions len = %d, want 1 after release", len(afe.sessions))
	}
}

func TestSessionList_Checkout_EmptyAfeReturnsNil(t *testing.T) {
	sl := newSessionList()
	h := makeHandleWithAfe(t, 1)
	sl.OnSessionStarted(h)
	afe := sl.afeHandles[1]
	sl.Checkout(afe)
	if got := sl.Checkout(afe); got != nil {
		t.Errorf("Checkout on empty AFE = %v, want nil", got)
	}
}

// TestPool_CheckoutSkipsSessionWithUndrainedSlot pins the Java-parity
// checkout gate: sessionList.Checkout MUST NOT hand out a session
// whose activeVRPC slot is still claimed (caller-abandoned RPC waiting
// for its server response to drain). Skipped busy sessions are
// re-enqueued at the tail so they remain eligible once their slot
// drains via handleVRPCResponse / handleVRPCErrorResponse.
//
// This is the pool-side complement to the slotMu refactor: without it,
// Invoke's defer re-adds a session with a still-claimed slot back to
// the idle queue, the next Checkout hands it out, and claimSlot fails
// with "session busy: prior vRPC has not drained on the wire" — the
// log line that surfaced in the sandbox and motivated v2.
//
// Java parity: SessionList.java's AfeHandle.tryAcquire skips sessions
// with currentRpc != null at the same layer.
func TestPool_CheckoutSkipsSessionWithUndrainedSlot(t *testing.T) {
	sl := newSessionList()
	h1 := makeHandleWithAfe(t, 1)
	h2 := makeHandleWithAfe(t, 1)
	sl.OnSessionStarted(h1)
	sl.OnSessionStarted(h2)
	afe := sl.afeHandles[1]

	// Pin h1's slot with a placeholder vRPC — simulates a caller-
	// abandoned RPC whose server response has not yet drained.
	h1.session.setSlotForTest(&vrpcImpl{id: 42, resultChan: make(chan vrpcResult, 1)})

	// Checkout MUST skip h1 and return h2. The AFE stays in the ready
	// set because h1 is still in the queue (just at the tail now).
	got := sl.Checkout(afe)
	if got == nil {
		t.Fatal("Checkout returned nil; want h2 (the idle-slot session)")
	}
	if got != h2 {
		t.Fatalf("Checkout returned %p, want h2 %p (should skip busy h1)", got, h2)
	}
	if len(afe.sessions) != 1 || afe.sessions[0] != h1 {
		t.Errorf("afe.sessions = %v, want [h1] (busy session re-enqueued at tail)", afe.sessions)
	}
	if len(sl.afesWithReady) != 1 {
		t.Errorf("afesWithReady len = %d, want 1 (AFE stays ready — queue still non-empty)", len(sl.afesWithReady))
	}

	// While h1's slot stays claimed, Checkout again on this AFE with
	// only h1 in the queue must return nil (all-busy case).
	if got := sl.Checkout(afe); got != nil {
		t.Errorf("Checkout with only busy h1 = %p, want nil (all-busy)", got)
	}
	// h1 must still be in the queue after the all-busy sweep — it stays
	// eligible for the next drain-triggered retry.
	if len(afe.sessions) != 1 || afe.sessions[0] != h1 {
		t.Errorf("afe.sessions after all-busy Checkout = %v, want [h1] (busy session must NOT be orphaned)", afe.sessions)
	}

	// Drain h1's slot (simulates handleVRPCResponse's drainSlot success)
	// and confirm Checkout now returns it, with the AFE dropping from
	// the ready set as the queue empties.
	h1.session.setSlotForTest(nil)
	if got := sl.Checkout(afe); got != h1 {
		t.Fatalf("Checkout after draining h1 = %p, want h1 %p", got, h1)
	}
	if len(sl.afesWithReady) != 0 {
		t.Errorf("afesWithReady len = %d, want 0 (AFE queue emptied on last checkout)", len(sl.afesWithReady))
	}
}

func TestSessionList_OnSessionClosing_RemovesIdleFromQueue(t *testing.T) {
	sl := newSessionList()
	h1 := makeHandleWithAfe(t, 1)
	h2 := makeHandleWithAfe(t, 1)
	sl.OnSessionStarted(h1)
	sl.OnSessionStarted(h2)

	sl.OnSessionClosing(h1)

	afe := sl.afeHandles[1]
	if len(afe.sessions) != 1 {
		t.Errorf("afe.sessions len = %d, want 1 after closing h1", len(afe.sessions))
	}
	if afe.refCount != 2 {
		t.Errorf("refCount = %d, want 2 (Closing does not decrement)", afe.refCount)
	}
}

func TestSessionList_OnSessionClosed_DecrementsRefcount(t *testing.T) {
	sl := newSessionList()
	h1 := makeHandleWithAfe(t, 1)
	h2 := makeHandleWithAfe(t, 1)
	sl.OnSessionStarted(h1)
	sl.OnSessionStarted(h2)

	sl.OnSessionClosing(h1)
	sl.OnSessionClosed(h1)

	afe := sl.afeHandles[1]
	if afe.refCount != 1 {
		t.Errorf("refCount = %d, want 1 after closing h1", afe.refCount)
	}
	if _, present := sl.handleToAfe[h1]; present {
		t.Error("handleToAfe still references closed handle h1")
	}
}

func TestSessionList_OnSessionClosed_LastSessionDropsFromReady(t *testing.T) {
	sl := newSessionList()
	h := makeHandleWithAfe(t, 5)
	sl.OnSessionStarted(h)

	// Skip Closing → straight to Closed (force-close path).
	sl.OnSessionClosed(h)

	if len(sl.afesWithReady) != 0 {
		t.Errorf("afesWithReady len = %d, want 0 (last session on AFE closed)", len(sl.afesWithReady))
	}
	// Bucket stays until Prune GCs it.
	if _, ok := sl.afeHandles[5]; !ok {
		t.Error("afeHandles missing bucket 5 (should stay until Prune)")
	}
}

func TestSessionList_RecordVRpcOutcome_SkipsNonOK(t *testing.T) {
	sl := newSessionList()
	h := makeHandleWithAfe(t, 1)
	sl.OnSessionStarted(h)

	// Non-OK response with a very fast (fake) latency must NOT update
	// EWMA — Java parity, so a fast-failing AFE can't look fastest.
	// afeHandle constructs its e2eEwma via NewPeakEwmaSeeded(afeE2eEwmaSeed),
	// so the untouched value is afeE2eEwmaSeed (1ms), not zero.
	sl.RecordVRpcOutcome(h, 1*time.Nanosecond, 0, false)
	if got, want := sl.afeHandles[1].e2eEwma.Value(), float64(afeE2eEwmaSeed); got != want {
		t.Errorf("e2eEwma = %g, want %g (seed unchanged) after non-OK record", got, want)
	}

	// OK response updates.
	sl.RecordVRpcOutcome(h, 5*time.Millisecond, 1*time.Millisecond, true)
	afe := sl.afeHandles[1]
	if afe.e2eEwma.Value() <= 0 {
		t.Errorf("e2eEwma = %g, want > 0 after OK record", afe.e2eEwma.Value())
	}
	if afe.transportEwma.Value() <= 0 {
		t.Errorf("transportEwma = %g, want > 0 after OK record", afe.transportEwma.Value())
	}
}

func TestSessionList_ReadyAfes_Snapshot(t *testing.T) {
	sl := newSessionList()
	h1a := makeHandleWithAfe(t, 1)
	h1b := makeHandleWithAfe(t, 1)
	h2 := makeHandleWithAfe(t, 2)
	sl.OnSessionStarted(h1a)
	sl.OnSessionStarted(h1b)
	sl.OnSessionStarted(h2)

	// Simulate one in-flight on AFE 1.
	sl.Checkout(sl.afeHandles[1])

	snaps := sl.ReadyAfes()
	if len(snaps) != 2 {
		t.Fatalf("snaps len = %d, want 2", len(snaps))
	}

	byID := map[afeID]afeSnapshot{}
	for _, s := range snaps {
		byID[s.Handle.ID()] = s
	}
	if got := byID[1]; got.IdleCount != 1 || got.NumOutstanding != 1 {
		t.Errorf("AFE 1: idle=%d inflight=%d, want 1/1", got.IdleCount, got.NumOutstanding)
	}
	if got := byID[2]; got.IdleCount != 1 || got.NumOutstanding != 0 {
		t.Errorf("AFE 2: idle=%d inflight=%d, want 1/0", got.IdleCount, got.NumOutstanding)
	}
}

func TestSessionList_Prune(t *testing.T) {
	sl := newSessionList()
	h1 := makeHandleWithAfe(t, 1) // will be closed and go stale
	h2 := makeHandleWithAfe(t, 2) // stays live

	sl.OnSessionStarted(h1)
	sl.OnSessionStarted(h2)
	sl.OnSessionClosed(h1)

	// Force AFE 1's lastConnected into the past.
	sl.afeHandles[1].lastConnected = time.Now().Add(-2 * afePruneMaxIdle)

	sl.Prune(time.Now())

	if _, ok := sl.afeHandles[1]; ok {
		t.Error("AFE 1 should have been pruned (refCount=0, aged)")
	}
	if _, ok := sl.afeHandles[2]; !ok {
		t.Error("AFE 2 should not be pruned (still live)")
	}
}

func TestSessionList_Snapshot(t *testing.T) {
	sl := newSessionList()

	h5 := makeHandleWithAfe(t, 5)
	h1a := makeHandleWithAfe(t, 1)
	h1b := makeHandleWithAfe(t, 1)
	sl.OnSessionStarted(h5)
	sl.OnSessionStarted(h1a)
	sl.OnSessionStarted(h1b)

	// Mark one AFE-1 session as in-flight (idle=1, refCount=2).
	sl.Checkout(sl.afeHandles[1])

	rows := sl.Snapshot()
	if len(rows) != 2 {
		t.Fatalf("Snapshot rows = %d, want 2", len(rows))
	}
	// Deterministic sort by ID ascending → AFE 1 first, AFE 5 second.
	if rows[0].ID != 1 || rows[0].RefCount != 2 || rows[0].IdleCount != 1 {
		t.Errorf("rows[0] = %+v, want ID=1 RefCount=2 IdleCount=1", rows[0])
	}
	if rows[1].ID != 5 || rows[1].RefCount != 1 || rows[1].IdleCount != 1 {
		t.Errorf("rows[1] = %+v, want ID=5 RefCount=1 IdleCount=1", rows[1])
	}
	if rows[0].LastConnected.IsZero() {
		t.Error("LastConnected should be populated after OnSessionStarted")
	}
}

func TestSessionList_Prune_KeepsRecentlyActiveEmpty(t *testing.T) {
	sl := newSessionList()
	h := makeHandleWithAfe(t, 1)
	sl.OnSessionStarted(h)
	sl.OnSessionClosed(h)
	// Do NOT age lastConnected — it should still be within the window.

	sl.Prune(time.Now())

	if _, ok := sl.afeHandles[1]; !ok {
		t.Error("AFE 1 should be kept (empty but within age window)")
	}
}
