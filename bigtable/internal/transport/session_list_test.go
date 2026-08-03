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
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	spb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
)

// newSessionHandle mints a SessionHandle wrapping session. The createdAt
// stamp feeds the pool's lifetime histogram; pass time.Now() from
// OnActive-style paths or the zero time when the test doesn't care.
//
// Test-only helper — production code (session_pool_scaling.go's
// createSession) mints handles directly with a struct literal so the
// hot path doesn't take a function-call frame. Shared across the
// package's _test.go files rather than duplicated per file.
func newSessionHandle(session *Session, createdAt time.Time) *SessionHandle {
	return &SessionHandle{session: session, createdAt: createdAt}
}

// makeHandleWithAfe creates a SessionHandle whose Session reports the given
// AfeID. Skips the handshake — sets PeerInfo directly.
func makeHandleWithAfe(t *testing.T, id AfeID) *SessionHandle {
	t.Helper()
	s := newTestSession(t, newFakeStream(), SessionHooks{})
	s.peerInfo.Store(&spb.PeerInfo{ApplicationFrontendId: int64(id)})
	return newSessionHandle(s, time.Time{})
}

// verifyInvariantsLocked re-derives sessionList's bookkeeping from
// scratch and returns a description of the first invariant violation
// found (empty string == all invariants hold). Test-only. Caller must
// hold sl.mu.
//
// Checks state invariants I1–I5 documented in session_list.go:65-72.
// I6 is temporal (refCount decrements only in OnSessionClosed) and
// cannot be verified from a snapshot alone — indirectly guarded via
// the sequence of assertions in individual tests.
func (sl *sessionList) verifyInvariantsLocked() string {
	// I2 + implicit I1: readyCount equals count of registered handles
	// with inExpectedCount==true. Any sh with inExpectedCount==true not
	// present in handleToAfe would corrupt this walk — but we're
	// iterating handleToAfe, so the "sh missing from map" case is
	// covered by the count mismatch.
	derived := 0
	for sh := range sl.handleToAfe {
		if sh.inExpectedCount {
			derived++
		}
	}
	if derived != sl.readyCount {
		return fmt.Sprintf("I2: readyCount=%d, want %d (from handleToAfe walk)",
			sl.readyCount, derived)
	}

	// I4: refCount per AFE matches the number of handleToAfe entries
	// pointing at it.
	pointerCounts := map[*afeHandle]int{}
	for _, afe := range sl.handleToAfe {
		pointerCounts[afe]++
	}
	for id, afe := range sl.afeHandles {
		if pointerCounts[afe] != afe.refCount {
			return fmt.Sprintf("I4: afe=%d refCount=%d, want %d (derived)",
				id, afe.refCount, pointerCounts[afe])
		}
	}
	// Also: no orphaned handleToAfe entry pointing at an AFE not in
	// sl.afeHandles.
	for sh, afe := range sl.handleToAfe {
		if sl.afeHandles[afe.id] != afe {
			return fmt.Sprintf("I4: sh=%p points at orphan afe=%d", sh, afe.id)
		}
	}

	// I5: every sh in afe.sessions maps back to that afe with
	// inExpectedCount==true.
	for id, afe := range sl.afeHandles {
		for i, sh := range afe.sessions {
			if got := sl.handleToAfe[sh]; got != afe {
				return fmt.Sprintf("I5: afe=%d sessions[%d] maps to afe=%v, want %v",
					id, i, got, afe)
			}
			if !sh.inExpectedCount {
				return fmt.Sprintf("I5: afe=%d sessions[%d] has inExpectedCount=false",
					id, i)
			}
		}
	}

	// I3: afesWithReady == { afe : len(sessions) > 0 } as a set (no dupes).
	seen := map[*afeHandle]bool{}
	for _, afe := range sl.afesWithReady {
		if seen[afe] {
			return fmt.Sprintf("I3: afe=%d appears twice in afesWithReady", afe.id)
		}
		seen[afe] = true
	}
	for id, afe := range sl.afeHandles {
		hasSessions := len(afe.sessions) > 0
		if hasSessions && !seen[afe] {
			return fmt.Sprintf("I3: afe=%d has %d sessions but missing from afesWithReady",
				id, len(afe.sessions))
		}
		if !hasSessions && seen[afe] {
			return fmt.Sprintf("I3: afe=%d has 0 sessions but present in afesWithReady", id)
		}
	}

	return ""
}

// checkInvariants runs verifyInvariantsLocked under sl.mu and fails
// the test on violation. Call at the end of every mutating test so
// bookkeeping drift is caught even when the test's explicit assertions
// wouldn't notice.
func checkInvariants(t *testing.T, sl *sessionList) {
	t.Helper()
	sl.mu.Lock()
	defer sl.mu.Unlock()
	if msg := sl.verifyInvariantsLocked(); msg != "" {
		t.Errorf("sessionList invariant violation: %s", msg)
	}
}

func TestSessionList_OnSessionStarted_BucketsByAfe(t *testing.T) {
	sl := newSessionList()

	h1a := makeHandleWithAfe(t, 1)
	h1b := makeHandleWithAfe(t, 1)
	h2 := makeHandleWithAfe(t, 2)

	sl.OnSessionStarted(h1a)
	sl.OnSessionStarted(h1b)
	sl.OnSessionStarted(h2)

	if got, want := sl.ReadyCount(), 3; got != want {
		t.Errorf("ReadyCount() = %d, want %d", got, want)
	}
	snaps := sl.ReadyAfes()
	if got, want := len(snaps), 2; got != want {
		t.Errorf("len(ReadyAfes()) = %d, want %d", got, want)
	}
	byID := map[AfeID]AfeSnapshot{}
	for _, s := range snaps {
		byID[s.ID] = s
	}
	if got := byID[1]; got.IdleCount != 2 {
		t.Errorf("AFE 1 IdleCount = %d, want 2", got.IdleCount)
	}
	if got := byID[2]; got.IdleCount != 1 {
		t.Errorf("AFE 2 IdleCount = %d, want 1", got.IdleCount)
	}
	checkInvariants(t, sl)
}

func TestSessionList_OnSessionStarted_Idempotent(t *testing.T) {
	sl := newSessionList()
	h := makeHandleWithAfe(t, 7)

	sl.OnSessionStarted(h)
	sl.OnSessionStarted(h) // duplicate

	if got, want := sl.ReadyCount(), 1; got != want {
		t.Errorf("ReadyCount() = %d, want %d (dup register must be a no-op)", got, want)
	}
	checkInvariants(t, sl)
}

func TestSessionList_Checkout_DrainsAndRefills(t *testing.T) {
	sl := newSessionList()
	h1 := makeHandleWithAfe(t, 1)
	h2 := makeHandleWithAfe(t, 1)
	sl.OnSessionStarted(h1)
	sl.OnSessionStarted(h2)

	// First checkout: queue still has one → AFE stays in ready list.
	got1 := sl.Checkout(AfeID(1))
	if got1 == nil {
		t.Fatal("Checkout returned nil")
	}
	if got, want := len(sl.ReadyAfes()), 1; got != want {
		t.Errorf("len(ReadyAfes()) = %d, want %d after first checkout", got, want)
	}

	// Second checkout: queue emptied → AFE drops from ready list.
	got2 := sl.Checkout(AfeID(1))
	if got2 == nil {
		t.Fatal("second Checkout returned nil")
	}
	if got, want := len(sl.ReadyAfes()), 0; got != want {
		t.Errorf("len(ReadyAfes()) = %d, want %d after drain", got, want)
	}
	if got1 == got2 {
		t.Error("Checkout returned the same handle twice")
	}

	// Release the first: AFE re-enters ready list.
	sl.ReleaseToPool(got1)
	if got, want := len(sl.ReadyAfes()), 1; got != want {
		t.Errorf("len(ReadyAfes()) = %d, want %d after release", got, want)
	}
	checkInvariants(t, sl)
}

func TestSessionList_Checkout_EmptyAfeReturnsNil(t *testing.T) {
	sl := newSessionList()
	h := makeHandleWithAfe(t, 1)
	sl.OnSessionStarted(h)
	sl.Checkout(AfeID(1))
	if got := sl.Checkout(AfeID(1)); got != nil {
		t.Errorf("Checkout on empty AFE = %v, want nil", got)
	}
	checkInvariants(t, sl)
}

func TestSessionList_Checkout_UnknownAfeReturnsNil(t *testing.T) {
	sl := newSessionList()
	if got := sl.Checkout(AfeID(999)); got != nil {
		t.Errorf("Checkout on unknown AFE = %v, want nil", got)
	}
	checkInvariants(t, sl)
}

func TestSessionList_OnSessionClosing_RemovesIdleFromQueue(t *testing.T) {
	sl := newSessionList()
	h1 := makeHandleWithAfe(t, 1)
	h2 := makeHandleWithAfe(t, 1)
	sl.OnSessionStarted(h1)
	sl.OnSessionStarted(h2)

	sl.OnSessionClosing(h1)

	// I6: refCount is NOT decremented on Closing — slot stays warm.
	if got, want := sl.afeHandles[1].refCount, 2; got != want {
		t.Errorf("refCount = %d, want %d (Closing does not decrement, I6)", got, want)
	}
	// I2: readyCount drops (h1 out of expected set).
	if got, want := sl.ReadyCount(), 1; got != want {
		t.Errorf("ReadyCount() = %d, want %d", got, want)
	}
	// I5: h1 removed from idle queue (only h2 pickable).
	snaps := sl.ReadyAfes()
	if len(snaps) != 1 || snaps[0].IdleCount != 1 {
		t.Errorf("ReadyAfes() = %+v, want single AFE with IdleCount=1", snaps)
	}
	checkInvariants(t, sl)
}

func TestSessionList_OnSessionClosed_DecrementsRefcount(t *testing.T) {
	sl := newSessionList()
	h1 := makeHandleWithAfe(t, 1)
	h2 := makeHandleWithAfe(t, 1)
	sl.OnSessionStarted(h1)
	sl.OnSessionStarted(h2)

	sl.OnSessionClosing(h1)
	sl.OnSessionClosed(h1)

	if got, want := sl.afeHandles[1].refCount, 1; got != want {
		t.Errorf("refCount = %d, want %d after Closing+Closed h1", got, want)
	}
	if got, want := sl.ReadyCount(), 1; got != want {
		t.Errorf("ReadyCount() = %d, want %d", got, want)
	}
	checkInvariants(t, sl)
}

func TestSessionList_OnSessionClosed_LastSessionDropsFromReady(t *testing.T) {
	sl := newSessionList()
	h := makeHandleWithAfe(t, 5)
	sl.OnSessionStarted(h)

	// Skip Closing → straight to Closed (force-close path).
	sl.OnSessionClosed(h)

	if got, want := len(sl.ReadyAfes()), 0; got != want {
		t.Errorf("len(ReadyAfes()) = %d, want %d (last session closed)", got, want)
	}
	// Bucket stays until Prune GCs it — verified via Snapshot (public).
	rows := sl.Snapshot()
	if len(rows) != 1 || rows[0].ID != 5 {
		t.Errorf("Snapshot() = %+v, want single row for AFE 5", rows)
	}
	checkInvariants(t, sl)
}

// TestSessionList_AllHandles_ReflectsMembership pins that AllHandles()
// returns every handle currently in handleToAfe — every state EXCEPT
// NotRegistered and Closed. Sessionz's per-session UI iterates this
// slice, so an implementation slip that drops in-flight or Closing
// handles would silently blank rows in the debug view.
func TestSessionList_AllHandles_ReflectsMembership(t *testing.T) {
	sl := newSessionList()
	h1 := makeHandleWithAfe(t, 1) // will stay Idle
	h2 := makeHandleWithAfe(t, 2) // will close
	h3 := makeHandleWithAfe(t, 1) // will move to Closing (not yet Closed)
	sl.OnSessionStarted(h1)
	sl.OnSessionStarted(h2)
	sl.OnSessionStarted(h3)

	// Move h3 into Closing but NOT Closed — AllHandles should still see it.
	sl.OnSessionClosing(h3)

	// Fully close h2 — AllHandles must drop it.
	sl.OnSessionClosing(h2)
	sl.OnSessionClosed(h2)

	got := sl.AllHandles()
	if len(got) != 2 {
		t.Fatalf("AllHandles() len = %d, want 2 (h1 Idle + h3 Closing; h2 fully closed)", len(got))
	}
	seen := map[*SessionHandle]bool{}
	for _, sh := range got {
		seen[sh] = true
	}
	if !seen[h1] {
		t.Error("AllHandles() missing h1 (Idle)")
	}
	if !seen[h3] {
		t.Error("AllHandles() missing h3 (Closing — refCount kept warm per I6)")
	}
	if seen[h2] {
		t.Error("AllHandles() included h2 which is Closed")
	}
	checkInvariants(t, sl)
}

func TestSessionList_RecordVRpcOutcome_SkipsNonOK(t *testing.T) {
	sl := newSessionList()
	h := makeHandleWithAfe(t, 1)
	sl.OnSessionStarted(h)

	// Non-OK response with a very fast (fake) latency must NOT update
	// EWMA — OK-gated, so a fast-failing AFE can't look fastest.
	// afeHandle constructs its e2eEwma via NewPeakEwmaSeeded(afeE2eEwmaSeed),
	// so the untouched value is afeE2eEwmaSeed (1ms), not zero.
	sl.RecordVRpcOutcome(h, 1*time.Nanosecond, 0, false)
	snap := sl.Snapshot()[0]
	if got, want := snap.E2eEwma, time.Duration(afeE2eEwmaSeed); got != want {
		t.Errorf("E2eEwma = %v, want %v (seed unchanged) after non-OK record", got, want)
	}

	// OK response updates both cost trackers.
	sl.RecordVRpcOutcome(h, 5*time.Millisecond, 1*time.Millisecond, true)
	snap = sl.Snapshot()[0]
	if snap.E2eEwma <= 0 {
		t.Errorf("E2eEwma = %v, want > 0 after OK record", snap.E2eEwma)
	}
	if snap.TransportEwma <= 0 {
		t.Errorf("TransportEwma = %v, want > 0 after OK record", snap.TransportEwma)
	}
	checkInvariants(t, sl)
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
	sl.Checkout(AfeID(1))

	snaps := sl.ReadyAfes()
	if len(snaps) != 2 {
		t.Fatalf("snaps len = %d, want 2", len(snaps))
	}
	byID := map[AfeID]AfeSnapshot{}
	for _, s := range snaps {
		byID[s.ID] = s
	}
	if got := byID[1]; got.IdleCount != 1 || got.NumOutstanding != 1 {
		t.Errorf("AFE 1: idle=%d inflight=%d, want 1/1", got.IdleCount, got.NumOutstanding)
	}
	if got := byID[2]; got.IdleCount != 1 || got.NumOutstanding != 0 {
		t.Errorf("AFE 2: idle=%d inflight=%d, want 1/0", got.IdleCount, got.NumOutstanding)
	}
	checkInvariants(t, sl)
}

func TestSessionList_Prune(t *testing.T) {
	sl := newSessionList()
	h1 := makeHandleWithAfe(t, 1) // will be closed and go stale
	h2 := makeHandleWithAfe(t, 2) // stays live

	sl.OnSessionStarted(h1)
	sl.OnSessionStarted(h2)
	sl.OnSessionClosed(h1)

	// Force AFE 1's lastConnected into the past. This is the ONE
	// unavoidable white-box touch — Prune's input is time-based and
	// there's no public knob to move an AFE's clock backward.
	sl.mu.Lock()
	sl.afeHandles[1].lastConnected = time.Now().Add(-2 * afePruneMaxIdle)
	sl.mu.Unlock()

	sl.Prune(time.Now())

	rows := sl.Snapshot()
	ids := map[AfeID]bool{}
	for _, r := range rows {
		ids[r.ID] = true
	}
	if ids[1] {
		t.Error("AFE 1 should have been pruned (refCount=0, aged)")
	}
	if !ids[2] {
		t.Error("AFE 2 should not be pruned (still live)")
	}
	checkInvariants(t, sl)
}

func TestSessionList_Prune_KeepsRecentlyActiveEmpty(t *testing.T) {
	sl := newSessionList()
	h := makeHandleWithAfe(t, 1)
	sl.OnSessionStarted(h)
	sl.OnSessionClosed(h)
	// Do NOT age lastConnected — it should still be within the window.

	sl.Prune(time.Now())

	rows := sl.Snapshot()
	if len(rows) != 1 || rows[0].ID != 1 {
		t.Errorf("Snapshot() = %+v, want single row for AFE 1 (empty but within age window)", rows)
	}
	checkInvariants(t, sl)
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
	sl.Checkout(AfeID(1))

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
	checkInvariants(t, sl)
}

// TestSessionList_ReleaseToPool_AfterClosing_NoOp pins the guard that
// prevents the WaitServerClose retry-storm documented in the
// project_bigtable_release_to_pool_bug memory: a session drained by
// OnSessionClosing must not re-enter the idle queue when a late
// ReleaseToPool arrives, or the next Checkout will hand back a dying
// session and every retry will hit "session is not active."
//
// Load-bearing: removing the `!sh.inExpectedCount` guard on
// session_list.go:211 must make this test fail. If it doesn't, the
// guard's contract is under-covered.
func TestSessionList_ReleaseToPool_AfterClosing_NoOp(t *testing.T) {
	sl := newSessionList()
	h := makeHandleWithAfe(t, 1)
	sl.OnSessionStarted(h)

	// InFlight → Closing (mid-flight OnSessionClosing race).
	sl.Checkout(AfeID(1))
	sl.OnSessionClosing(h)

	// Pin the mechanism directly: OnSessionClosing MUST flip
	// inExpectedCount, and that flag is what ReleaseToPool consults
	// to no-op. If a future refactor moves the guard to a different
	// mechanism that ALSO happens to leave the queue empty, the
	// outcome-only assertions below would still pass — this pins the
	// flag so a swap fails loudly here first.
	if h.inExpectedCount {
		t.Fatal("OnSessionClosing must clear inExpectedCount — the I5/I2 guard that makes ReleaseToPool a no-op after Closing")
	}

	// Late ReleaseToPool arriving after Closing must be a no-op.
	sl.ReleaseToPool(h)

	// AFE must not appear in the ready set: the only session on it is
	// draining and its idle queue must stay empty.
	if got := sl.ReadyAfes(); len(got) != 0 {
		t.Errorf("ReadyAfes() = %+v, want empty (drained session must not re-enqueue)", got)
	}
	// Also: a follow-up Checkout must return nil, not hand out the
	// drained handle.
	if got := sl.Checkout(AfeID(1)); got != nil {
		t.Errorf("Checkout after Closing+ReleaseToPool = %v, want nil", got)
	}
	checkInvariants(t, sl)
}

// TestSessionList_RecordVRpcOutcome_AfterClose pins the "post-close call
// is a silent no-op" behavior of RecordVRpcOutcome: after full teardown
// (OnSessionClosing → OnSessionClosed) the handle is out of
// handleToAfe, so the map lookup returns nil, the method bails, and no
// PeakEwma seed is disturbed.
//
// This test does NOT exercise the documented detach-mid-update race
// (map lookup returns non-nil afe → sl.mu dropped → close+prune fires
// → the deferred PeakEwma.Update lands on an orphan struct). Reaching
// that window requires a concurrent driver or an injectable pause
// between session_list.go's map read and PeakEwma update; a
// single-goroutine test can't produce it.
func TestSessionList_RecordVRpcOutcome_AfterClose(t *testing.T) {
	sl := newSessionList()
	h := makeHandleWithAfe(t, 1)
	sl.OnSessionStarted(h)

	// Seed the AFE's PeakEwma with a known value so we can assert the
	// post-close call did NOT mutate it.
	sl.RecordVRpcOutcome(h, 5*time.Millisecond, 1*time.Millisecond, true)
	afe := sl.afeHandles[AfeID(1)]
	seededE2e := afe.e2eEwma.Value()
	seededTransport := afe.transportEwma.Value()

	// Full teardown: OnSessionClosing → OnSessionClosed detaches the
	// handle from handleToAfe entirely.
	sl.OnSessionClosing(h)
	sl.OnSessionClosed(h)

	// Post-close RecordVRpcOutcome MUST be a silent no-op.
	sl.RecordVRpcOutcome(h, 500*time.Millisecond, 100*time.Millisecond, true)

	// Bucket may have been pruned already, but if it survives the
	// EWMAs must be unchanged (the update we just made was dropped).
	if afe, ok := sl.afeHandles[AfeID(1)]; ok {
		if afe.e2eEwma.Value() != seededE2e {
			t.Errorf("e2eEwma mutated post-close: got %v, want seeded %v", afe.e2eEwma.Value(), seededE2e)
		}
		if afe.transportEwma.Value() != seededTransport {
			t.Errorf("transportEwma mutated post-close: got %v, want seeded %v", afe.transportEwma.Value(), seededTransport)
		}
	}
	checkInvariants(t, sl)
}

// TestSessionList_AfeIDZero_Fallback verifies that sessions whose
// AfeID is 0 (server didn't populate the peer-info header) all land
// in the same bucket, count toward ReadyCount, and appear in
// Snapshot — documented behavior at session_list.go:133-136.
func TestSessionList_AfeIDZero_Fallback(t *testing.T) {
	sl := newSessionList()

	h1 := makeHandleWithAfe(t, 0)
	h2 := makeHandleWithAfe(t, 0)
	sl.OnSessionStarted(h1)
	sl.OnSessionStarted(h2)

	if got, want := sl.ReadyCount(), 2; got != want {
		t.Errorf("ReadyCount() = %d, want %d", got, want)
	}
	snaps := sl.ReadyAfes()
	if len(snaps) != 1 || snaps[0].ID != 0 || snaps[0].IdleCount != 2 {
		t.Errorf("ReadyAfes() = %+v, want single ID=0 bucket with IdleCount=2", snaps)
	}
	rows := sl.Snapshot()
	if len(rows) != 1 || rows[0].ID != 0 {
		t.Errorf("Snapshot() = %+v, want single row for AFE 0", rows)
	}
	checkInvariants(t, sl)
}

// TestSessionList_OnSessionClosing_Idempotent covers the guarantee
// that dropMembershipLocked is safe to call multiple times — pool
// teardown (session_pool_lifecycle.go) relies on it. Two consecutive
// OnSessionClosing calls must land readyCount in the same place as
// one.
func TestSessionList_OnSessionClosing_Idempotent(t *testing.T) {
	sl := newSessionList()
	h := makeHandleWithAfe(t, 1)
	sl.OnSessionStarted(h)

	sl.OnSessionClosing(h)
	sl.OnSessionClosing(h) // duplicate

	if got, want := sl.ReadyCount(), 0; got != want {
		t.Errorf("ReadyCount() = %d, want %d after double-Closing", got, want)
	}
	if got, want := sl.afeHandles[1].refCount, 1; got != want {
		t.Errorf("refCount = %d, want %d (Closing does not decrement, I6)", got, want)
	}
	checkInvariants(t, sl)
}

// TestSessionList_OnSessionClosed_Idempotent covers the double-Closed
// case. handleToAfe delete on the first call means the second finds
// afe==nil and short-circuits — must not double-decrement refCount or
// trip the refcount-underflow assertion.
func TestSessionList_OnSessionClosed_Idempotent(t *testing.T) {
	sl := newSessionList()
	h1 := makeHandleWithAfe(t, 1)
	h2 := makeHandleWithAfe(t, 1)
	sl.OnSessionStarted(h1)
	sl.OnSessionStarted(h2)

	sl.OnSessionClosed(h1)
	sl.OnSessionClosed(h1) // duplicate

	if got, want := sl.afeHandles[1].refCount, 1; got != want {
		t.Errorf("refCount = %d, want %d after double-Closed h1", got, want)
	}
	checkInvariants(t, sl)
}

// TestSessionList_OnSessionClosing_ThenClosed_HappyPath and
// TestSessionList_OnSessionClosed_SkipsClosing are already covered
// above. This one covers the reversed order — Closed fires first (a
// force-close path) and Closing arrives late; the late Closing must
// be a no-op because handleToAfe no longer references the handle.
func TestSessionList_OnSessionClosed_ThenClosing_LateClosingNoOp(t *testing.T) {
	sl := newSessionList()
	h1 := makeHandleWithAfe(t, 1)
	h2 := makeHandleWithAfe(t, 1)
	sl.OnSessionStarted(h1)
	sl.OnSessionStarted(h2)

	sl.OnSessionClosed(h1)  // force-close path
	sl.OnSessionClosing(h1) // late Closing arriving after Closed

	if got, want := sl.afeHandles[1].refCount, 1; got != want {
		t.Errorf("refCount = %d, want %d after Closed-then-late-Closing", got, want)
	}
	if got, want := sl.ReadyCount(), 1; got != want {
		t.Errorf("ReadyCount() = %d, want %d", got, want)
	}
	checkInvariants(t, sl)
}

// withQuarantineTuning shrinks the quarantine threshold and duration
// for the enclosing test so the trip / half-open / recovery paths can
// be exercised inside a -short go-test budget. Restores the production
// values via t.Cleanup. Not safe under t.Parallel() — the current
// session_list_test.go tests don't parallelize, matching the concurrent
// stress-test's shared-state pattern.
func withQuarantineTuning(t *testing.T, threshold int, duration time.Duration) {
	t.Helper()
	oldT, oldD := afeQuarantineFailureThreshold, afeQuarantineDuration
	afeQuarantineFailureThreshold = threshold
	afeQuarantineDuration = duration
	t.Cleanup(func() {
		afeQuarantineFailureThreshold = oldT
		afeQuarantineDuration = oldD
	})
}

// failN feeds n non-OK vRPC outcomes with a stub 1-ns latency to sh.
// Setup helper for the quarantine tests, all of which do the same loop.
func failN(sl *sessionList, sh *SessionHandle, n int) {
	for i := 0; i < n; i++ {
		sl.RecordVRpcOutcome(sh, 1*time.Nanosecond, 0, false)
	}
}

// snapshotByID returns sl.Snapshot() indexed by AfeID. Used by tests
// that add a healthy neighbor AFE (to keep pool-wide suppression at
// bay) and need to inspect just the tripped bucket.
func snapshotByID(sl *sessionList) map[AfeID]AfeSnapshotRow {
	rows := sl.Snapshot()
	out := make(map[AfeID]AfeSnapshotRow, len(rows))
	for _, r := range rows {
		out[r.ID] = r
	}
	return out
}

// readyByID mirrors snapshotByID for ReadyAfes.
func readyByID(sl *sessionList) map[AfeID]AfeSnapshot {
	rows := sl.ReadyAfes()
	out := make(map[AfeID]AfeSnapshot, len(rows))
	for _, r := range rows {
		out[r.ID] = r
	}
	return out
}

func TestSessionList_Quarantine_CountsFailures_TripsAtThreshold(t *testing.T) {
	withQuarantineTuning(t, 3, 50*time.Millisecond)
	sl := newSessionList()
	// Two AFEs so the pool-wide 70% suppression guard doesn't fire when
	// one gets quarantined (1/2 = 50% < 70%).
	h := makeHandleWithAfe(t, 1)
	sl.OnSessionStarted(h)
	sl.OnSessionStarted(makeHandleWithAfe(t, 2))

	// Feed threshold-1 failures — counter climbs, no trip yet.
	for i := 0; i < 2; i++ {
		sl.RecordVRpcOutcome(h, 1*time.Nanosecond, 0, false)
	}
	if got := snapshotByID(sl)[1]; got.ConsecFailures != 2 {
		t.Errorf("after 2 failures: ConsecFailures = %d, want 2", got.ConsecFailures)
	}
	if got := snapshotByID(sl)[1]; !got.QuarantinedUntil.IsZero() {
		t.Errorf("after 2 failures: QuarantinedUntil = %v, want zero (below threshold)", got.QuarantinedUntil)
	}
	if _, ok := readyByID(sl)[1]; !ok {
		t.Error("after 2 failures: AFE 1 missing from ReadyAfes (should still be eligible)")
	}

	// The threshold-th failure trips.
	sl.RecordVRpcOutcome(h, 1*time.Nanosecond, 0, false)
	row := snapshotByID(sl)[1]
	if row.ConsecFailures != 3 {
		t.Errorf("post-trip ConsecFailures = %d, want 3", row.ConsecFailures)
	}
	if row.QuarantinedUntil.IsZero() {
		t.Errorf("post-trip QuarantinedUntil = zero, want non-zero")
	}
	if _, ok := readyByID(sl)[1]; ok {
		t.Error("post-trip AFE 1 still in ReadyAfes (should be excluded)")
	}
	if _, ok := readyByID(sl)[2]; !ok {
		t.Error("post-trip AFE 2 missing from ReadyAfes (should be unaffected)")
	}
	checkInvariants(t, sl)
}

func TestSessionList_Quarantine_ResetsOnOK_BelowThreshold(t *testing.T) {
	withQuarantineTuning(t, 5, 50*time.Millisecond)
	sl := newSessionList()
	h := makeHandleWithAfe(t, 1)
	sl.OnSessionStarted(h)

	for i := 0; i < 4; i++ {
		sl.RecordVRpcOutcome(h, 1*time.Nanosecond, 0, false)
	}
	sl.RecordVRpcOutcome(h, 5*time.Millisecond, 1*time.Millisecond, true)

	row := sl.Snapshot()[0]
	if row.ConsecFailures != 0 {
		t.Errorf("ConsecFailures = %d, want 0 (reset on OK)", row.ConsecFailures)
	}
	if !row.QuarantinedUntil.IsZero() {
		t.Errorf("QuarantinedUntil = %v, want zero", row.QuarantinedUntil)
	}
	// One more failure — counter starts fresh at 1, still eligible.
	sl.RecordVRpcOutcome(h, 1*time.Nanosecond, 0, false)
	if got := sl.Snapshot()[0].ConsecFailures; got != 1 {
		t.Errorf("post-reset failure: ConsecFailures = %d, want 1", got)
	}
	checkInvariants(t, sl)
}

func TestSessionList_Quarantine_ExcludedFromReadyAfes(t *testing.T) {
	withQuarantineTuning(t, 2, 50*time.Millisecond)
	sl := newSessionList()
	hA := makeHandleWithAfe(t, 1)
	hB := makeHandleWithAfe(t, 2)
	sl.OnSessionStarted(hA)
	sl.OnSessionStarted(hB)

	// Trip AFE 1 only.
	for i := 0; i < 2; i++ {
		sl.RecordVRpcOutcome(hA, 1*time.Nanosecond, 0, false)
	}

	ready := sl.ReadyAfes()
	if len(ready) != 1 {
		t.Fatalf("ReadyAfes len = %d, want 1 (AFE 1 quarantined, AFE 2 healthy)", len(ready))
	}
	if ready[0].ID != AfeID(2) {
		t.Errorf("ReadyAfes[0].ID = %d, want 2", ready[0].ID)
	}
	checkInvariants(t, sl)
}

func TestSessionList_Quarantine_WindowExpires_HalfOpenReadyAfes(t *testing.T) {
	withQuarantineTuning(t, 2, 30*time.Millisecond)
	sl := newSessionList()
	h := makeHandleWithAfe(t, 1)
	sl.OnSessionStarted(h)
	// Healthy neighbor to keep pool-wide suppression from firing.
	sl.OnSessionStarted(makeHandleWithAfe(t, 2))

	for i := 0; i < 2; i++ {
		sl.RecordVRpcOutcome(h, 1*time.Nanosecond, 0, false)
	}
	if _, ok := readyByID(sl)[1]; ok {
		t.Fatal("pre-expiry AFE 1 still in ReadyAfes (should be quarantined)")
	}

	// Wait past the quarantine window; ReadyAfes should re-include the
	// AFE without any explicit "reset" call — the filter is a live
	// now-vs-until comparison.
	time.Sleep(60 * time.Millisecond)
	if _, ok := readyByID(sl)[1]; !ok {
		t.Error("post-expiry AFE 1 missing from ReadyAfes (should be half-open)")
	}
	checkInvariants(t, sl)
}

func TestSessionList_Quarantine_HalfOpen_OKClears(t *testing.T) {
	withQuarantineTuning(t, 2, 30*time.Millisecond)
	sl := newSessionList()
	h := makeHandleWithAfe(t, 1)
	sl.OnSessionStarted(h)

	for i := 0; i < 2; i++ {
		sl.RecordVRpcOutcome(h, 1*time.Nanosecond, 0, false)
	}
	time.Sleep(60 * time.Millisecond)

	// Probe OK — should zero counter AND clear window explicitly.
	sl.RecordVRpcOutcome(h, 5*time.Millisecond, 1*time.Millisecond, true)
	row := sl.Snapshot()[0]
	if row.ConsecFailures != 0 {
		t.Errorf("post-probe-OK ConsecFailures = %d, want 0", row.ConsecFailures)
	}
	if !row.QuarantinedUntil.IsZero() {
		t.Errorf("post-probe-OK QuarantinedUntil = %v, want zero", row.QuarantinedUntil)
	}
	checkInvariants(t, sl)
}

// TestSessionList_Quarantine_HalfOpen_ProbeFailResetsCounter_ReTripsOnBurst
// pins the reset-on-lazy-expiry contract. A single failure right after
// the window has elapsed must NOT re-quarantine — it starts a fresh
// counter at 1. Re-tripping requires another full threshold-worth of
// failures, i.e. a real burst rather than one straggler.
func TestSessionList_Quarantine_HalfOpen_ProbeFailResetsCounter_ReTripsOnBurst(t *testing.T) {
	withQuarantineTuning(t, 2, 30*time.Millisecond)
	sl := newSessionList()
	h := makeHandleWithAfe(t, 1)
	sl.OnSessionStarted(h)
	// Healthy neighbor to keep pool-wide suppression from firing.
	sl.OnSessionStarted(makeHandleWithAfe(t, 2))

	failN(sl, h, 2)
	firstWindow := snapshotByID(sl)[1].QuarantinedUntil
	time.Sleep(60 * time.Millisecond)

	// Single post-window failure: lazy-expiry resets the counter (to 0),
	// then this failure bumps it to 1. Below threshold → NOT re-tripped.
	failN(sl, h, 1)
	row := snapshotByID(sl)[1]
	if !row.QuarantinedUntil.IsZero() {
		t.Errorf("single post-window failure re-quarantined: QuarantinedUntil = %v, want zero (counter should have reset)", row.QuarantinedUntil)
	}
	if row.ConsecFailures != 1 {
		t.Errorf("post-lazy-expiry ConsecFailures = %d, want 1", row.ConsecFailures)
	}

	// Now the second failure crosses threshold and re-quarantines with
	// a fresh window (which must be later than the first).
	failN(sl, h, 1)
	row = snapshotByID(sl)[1]
	if row.QuarantinedUntil.IsZero() {
		t.Fatal("second post-window failure did not re-trip")
	}
	if !row.QuarantinedUntil.After(firstWindow) {
		t.Errorf("re-tripped window %v not after first window %v", row.QuarantinedUntil, firstWindow)
	}
	if _, ok := readyByID(sl)[1]; ok {
		t.Error("post-re-trip AFE 1 still in ReadyAfes (should be re-excluded)")
	}
	checkInvariants(t, sl)
}

// TestSessionList_Quarantine_MidWindowOKClears pins the "any OK during
// an active window recovers" branch of recordOutcomeLocked (as
// distinct from the post-window half-open probe). This is what a
// mid-window in-flight vRPC completing OK looks like, or an OK landing
// on an AFE that pool-wide suppression re-exposed to the picker.
func TestSessionList_Quarantine_MidWindowOKClears(t *testing.T) {
	// Long window so we're firmly inside it when the OK arrives — no
	// sleep between the trip and the OK.
	withQuarantineTuning(t, 2, 500*time.Millisecond)
	sl := newSessionList()
	h := makeHandleWithAfe(t, 1)
	sl.OnSessionStarted(h)
	sl.OnSessionStarted(makeHandleWithAfe(t, 2)) // neighbor for suppression math

	failN(sl, h, 2)
	if snapshotByID(sl)[1].QuarantinedUntil.IsZero() {
		t.Fatal("setup: AFE 1 should be quarantined after threshold failures")
	}

	// OK arrives mid-window; both counter and window clear.
	sl.RecordVRpcOutcome(h, 5*time.Millisecond, 1*time.Millisecond, true)
	row := snapshotByID(sl)[1]
	if !row.QuarantinedUntil.IsZero() {
		t.Errorf("mid-window OK: QuarantinedUntil = %v, want zero", row.QuarantinedUntil)
	}
	if row.ConsecFailures != 0 {
		t.Errorf("mid-window OK: ConsecFailures = %d, want 0", row.ConsecFailures)
	}
	if _, ok := readyByID(sl)[1]; !ok {
		t.Error("mid-window OK: AFE 1 missing from ReadyAfes (should be back)")
	}
	checkInvariants(t, sl)
}

func TestSessionList_Quarantine_TripOnceWhileInWindow(t *testing.T) {
	withQuarantineTuning(t, 2, 500*time.Millisecond)
	sl := newSessionList()
	h := makeHandleWithAfe(t, 1)
	sl.OnSessionStarted(h)

	for i := 0; i < 2; i++ {
		sl.RecordVRpcOutcome(h, 1*time.Nanosecond, 0, false)
	}
	firstWindow := sl.Snapshot()[0].QuarantinedUntil
	// Additional failures during the window: counter grows, window
	// does NOT shift. Refreshing the window on every failure would
	// mean a persistent outage never re-enters half-open.
	time.Sleep(5 * time.Millisecond)
	for i := 0; i < 20; i++ {
		sl.RecordVRpcOutcome(h, 1*time.Nanosecond, 0, false)
	}
	row := sl.Snapshot()[0]
	if !row.QuarantinedUntil.Equal(firstWindow) {
		t.Errorf("in-window window shifted: got %v, want %v", row.QuarantinedUntil, firstWindow)
	}
	if row.ConsecFailures < 22 {
		t.Errorf("in-window ConsecFailures = %d, want >= 22 (counter keeps growing)", row.ConsecFailures)
	}
	checkInvariants(t, sl)
}

func TestSessionList_Quarantine_PoolWideSuppression(t *testing.T) {
	withQuarantineTuning(t, 2, 500*time.Millisecond)
	sl := newSessionList()
	hA := makeHandleWithAfe(t, 1)
	hB := makeHandleWithAfe(t, 2)
	hC := makeHandleWithAfe(t, 3)
	sl.OnSessionStarted(hA)
	sl.OnSessionStarted(hB)
	sl.OnSessionStarted(hC)

	// Trip all three (100% ≥ 70% suppression threshold).
	for _, h := range []*SessionHandle{hA, hB, hC} {
		failN(sl, h, 2)
	}

	ready := sl.ReadyAfes()
	if len(ready) != 3 {
		t.Fatalf("all-quarantined ReadyAfes len = %d, want 3 (suppression should surface every ready AFE)", len(ready))
	}
	// The picker-facing snapshot deliberately omits QuarantinedUntil,
	// so verify the underlying quarantine state via Snapshot() —
	// suppression must not have cleared it.
	snaps := snapshotByID(sl)
	for _, id := range []AfeID{1, 2, 3} {
		if snaps[id].QuarantinedUntil.IsZero() {
			t.Errorf("AFE %d: QuarantinedUntil = zero after suppression, want the trip window intact", id)
		}
	}
	checkInvariants(t, sl)
}

// TestSessionList_Quarantine_SuppressionTogglesOnRecovery exercises
// the interesting boundary of the pool-wide guard: 3 AFEs all
// quarantined → suppression fires → one recovers via OK → 2/3 = 66% <
// 70% → suppression lifts → the two still-quarantined AFEs are back
// to being filtered, so the picker sees only the recovered one.
func TestSessionList_Quarantine_SuppressionTogglesOnRecovery(t *testing.T) {
	withQuarantineTuning(t, 2, 500*time.Millisecond)
	sl := newSessionList()
	hA := makeHandleWithAfe(t, 1)
	hB := makeHandleWithAfe(t, 2)
	hC := makeHandleWithAfe(t, 3)
	sl.OnSessionStarted(hA)
	sl.OnSessionStarted(hB)
	sl.OnSessionStarted(hC)

	for _, h := range []*SessionHandle{hA, hB, hC} {
		failN(sl, h, 2)
	}
	if got := len(sl.ReadyAfes()); got != 3 {
		t.Fatalf("setup: all-quarantined ReadyAfes len = %d, want 3 (suppression)", got)
	}

	// One AFE recovers via an OK. Now 2/3 = 66% < 70% → suppression
	// no longer fires, and the two still-quarantined AFEs are excluded.
	sl.RecordVRpcOutcome(hB, 5*time.Millisecond, 1*time.Millisecond, true)

	ready := readyByID(sl)
	if len(ready) != 1 {
		t.Fatalf("post-recovery ReadyAfes len = %d, want 1 (suppression should have lifted)", len(ready))
	}
	if _, ok := ready[2]; !ok {
		t.Errorf("post-recovery ReadyAfes = %+v, want AFE 2 (the recovered one)", ready)
	}
	checkInvariants(t, sl)
}

func TestSessionList_Quarantine_SnapshotFieldsPopulated(t *testing.T) {
	withQuarantineTuning(t, 2, 500*time.Millisecond)
	sl := newSessionList()
	hA := makeHandleWithAfe(t, 1) // will trip
	hB := makeHandleWithAfe(t, 2) // stays healthy
	sl.OnSessionStarted(hA)
	sl.OnSessionStarted(hB)

	for i := 0; i < 2; i++ {
		sl.RecordVRpcOutcome(hA, 1*time.Nanosecond, 0, false)
	}
	sl.RecordVRpcOutcome(hB, 5*time.Millisecond, 1*time.Millisecond, true)

	rows := sl.Snapshot()
	byID := map[AfeID]AfeSnapshotRow{}
	for _, r := range rows {
		byID[r.ID] = r
	}
	if a := byID[1]; a.ConsecFailures != 2 || a.QuarantinedUntil.IsZero() {
		t.Errorf("AFE 1 row = %+v, want ConsecFailures=2 and QuarantinedUntil non-zero", a)
	}
	if b := byID[2]; b.ConsecFailures != 0 || !b.QuarantinedUntil.IsZero() {
		t.Errorf("AFE 2 row = %+v, want ConsecFailures=0 and QuarantinedUntil zero", b)
	}
	checkInvariants(t, sl)
}

// TestSessionList_ConcurrentOps_MaintainsInvariants stress-tests the
// mutex discipline. N goroutines drive random OnSessionStarted /
// Checkout / ReleaseToPool / OnSessionClosing / OnSessionClosed on a
// shared handle pool; at quiesce the invariant walker must return
// clean. -race amplifies the coverage. Short-mode-friendly (100 ops
// per worker, ~10ms wall clock on modern hardware).
func TestSessionList_ConcurrentOps_MaintainsInvariants(t *testing.T) {
	const (
		workers  = 8
		opsEach  = 200
		numAfes  = 4
		poolSize = 32
	)

	sl := newSessionList()

	// Pre-build the handle pool so goroutines don't race on
	// makeHandleWithAfe (which is not thread-safe).
	handles := make([]*SessionHandle, poolSize)
	for i := range handles {
		handles[i] = makeHandleWithAfe(t, AfeID(1+i%numAfes))
	}

	// Track per-handle state (unregistered / registered / closed) with
	// a small stateMu so workers don't double-register the same handle
	// (which would be a legitimate no-op but skews the invariant walk).
	type handleState int
	const (
		unregistered handleState = iota
		registered
		closed
	)
	states := make([]handleState, poolSize)
	var stateMu sync.Mutex

	// Deterministic per-worker seed so failures reproduce AND workers
	// don't serialize on a shared rand mutex — the whole point of a
	// concurrency stress test is that the workers actually race.
	const baseSeed = uint64(0x5e551011115700ff)

	var wg sync.WaitGroup
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func(worker int) {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(baseSeed+uint64(worker), 0xdeadbeefcafebabe))
			randHandle := func() int { return rng.IntN(poolSize) }
			randAfe := func() AfeID { return AfeID(1 + rng.IntN(numAfes)) }
			for i := 0; i < opsEach; i++ {
				switch rng.IntN(6) {
				case 0: // OnSessionStarted (once per handle)
					idx := randHandle()
					stateMu.Lock()
					if states[idx] == unregistered {
						states[idx] = registered
						stateMu.Unlock()
						sl.OnSessionStarted(handles[idx])
					} else {
						stateMu.Unlock()
					}
				case 1: // Checkout (may return nil legitimately)
					sl.Checkout(randAfe())
				case 2: // ReleaseToPool (only safe on registered handles)
					idx := randHandle()
					stateMu.Lock()
					if states[idx] == registered {
						stateMu.Unlock()
						sl.ReleaseToPool(handles[idx])
					} else {
						stateMu.Unlock()
					}
				case 3: // OnSessionClosing
					idx := randHandle()
					stateMu.Lock()
					if states[idx] == registered {
						stateMu.Unlock()
						sl.OnSessionClosing(handles[idx])
					} else {
						stateMu.Unlock()
					}
				case 4: // OnSessionClosed (transition registered→closed)
					idx := randHandle()
					stateMu.Lock()
					if states[idx] == registered {
						states[idx] = closed
						stateMu.Unlock()
						sl.OnSessionClosed(handles[idx])
					} else {
						stateMu.Unlock()
					}
				case 5: // RecordVRpcOutcome — safe on any handle (lookup gates it)
					// and OK-gate mixed to exercise both branches. Racing
					// with case 4's OnSessionClosed exercises the
					// documented detach-then-update path.
					idx := randHandle()
					e2e := time.Duration(1+rng.IntN(50)) * time.Millisecond
					backend := time.Duration(1+rng.IntN(20)) * time.Millisecond
					sl.RecordVRpcOutcome(handles[idx], e2e, backend, rng.IntN(2) == 0)
				}
			}
		}(w)
	}
	wg.Wait()

	checkInvariants(t, sl)
}
