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

// Quarantine sub-suite for sessionList: trip / half-open / recovery /
// pool-wide suppression paths of afeHandle.recordOutcomeLocked +
// sessionList.ReadyAfes filtering. Split from session_list_test.go
// after that file crossed the 1000-line guideline; helpers live here
// alongside their sole callers.

package internal

import (
	"testing"
	"time"
)

// failN feeds n non-OK vRPC outcomes with a stub 1-ns latency to sh.
// Setup helper for the quarantine tests, all of which drive the same
// "burst until trip" loop.
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
	resetDebugTagCountsForTest()
	sl := newSessionListWith(3, 50*time.Millisecond)
	// Two AFEs so the pool-wide 70% suppression guard doesn't fire when
	// one gets quarantined (1/2 = 50% < 70%).
	h := makeHandleWithAfe(t, 1)
	sl.OnSessionStarted(h)
	sl.OnSessionStarted(makeHandleWithAfe(t, 2))

	// Feed threshold-1 failures — counter climbs, no trip yet.
	failN(sl, h, 2)
	pre := snapshotByID(sl)[1]
	if pre.ConsecFailures != 2 {
		t.Errorf("after 2 failures: ConsecFailures = %d, want 2", pre.ConsecFailures)
	}
	if !pre.QuarantinedUntil.IsZero() {
		t.Errorf("after 2 failures: QuarantinedUntil = %v, want zero (below threshold)", pre.QuarantinedUntil)
	}
	if _, ok := readyByID(sl)[1]; !ok {
		t.Error("after 2 failures: AFE 1 missing from ReadyAfes (should still be eligible)")
	}
	if got := snapshotDebugTagCounts()[tagSessionListAfeQuarantineTripped]; got != 0 {
		t.Errorf("pre-trip tripped count = %d, want 0", got)
	}

	// The threshold-th failure trips.
	failN(sl, h, 1)
	post := snapshotByID(sl)[1]
	if post.ConsecFailures != 3 {
		t.Errorf("post-trip ConsecFailures = %d, want 3", post.ConsecFailures)
	}
	if post.QuarantinedUntil.IsZero() {
		t.Errorf("post-trip QuarantinedUntil = zero, want non-zero")
	}
	if _, ok := readyByID(sl)[1]; ok {
		t.Error("post-trip AFE 1 still in ReadyAfes (should be excluded)")
	}
	if _, ok := readyByID(sl)[2]; !ok {
		t.Error("post-trip AFE 2 missing from ReadyAfes (should be unaffected)")
	}
	if got := snapshotDebugTagCounts()[tagSessionListAfeQuarantineTripped]; got != 1 {
		t.Errorf("post-trip tripped count = %d, want 1", got)
	}
	checkInvariants(t, sl)
}

func TestSessionList_Quarantine_ResetsOnOK_BelowThreshold(t *testing.T) {
	sl := newSessionListWith(5, 50*time.Millisecond)
	h := makeHandleWithAfe(t, 1)
	sl.OnSessionStarted(h)

	failN(sl, h, 4)
	sl.RecordVRpcOutcome(h, 5*time.Millisecond, 1*time.Millisecond, true)

	row := snapshotByID(sl)[1]
	if row.ConsecFailures != 0 {
		t.Errorf("ConsecFailures = %d, want 0 (reset on OK)", row.ConsecFailures)
	}
	if !row.QuarantinedUntil.IsZero() {
		t.Errorf("QuarantinedUntil = %v, want zero", row.QuarantinedUntil)
	}
	// One more failure — counter starts fresh at 1, still eligible.
	failN(sl, h, 1)
	if got := snapshotByID(sl)[1].ConsecFailures; got != 1 {
		t.Errorf("post-reset failure: ConsecFailures = %d, want 1", got)
	}
	checkInvariants(t, sl)
}

func TestSessionList_Quarantine_ExcludedFromReadyAfes(t *testing.T) {
	sl := newSessionListWith(2, 50*time.Millisecond)
	hA := makeHandleWithAfe(t, 1)
	hB := makeHandleWithAfe(t, 2)
	sl.OnSessionStarted(hA)
	sl.OnSessionStarted(hB)

	failN(sl, hA, 2)

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
	sl := newSessionListWith(2, 30*time.Millisecond)
	h := makeHandleWithAfe(t, 1)
	sl.OnSessionStarted(h)
	// Healthy neighbor to keep pool-wide suppression from firing.
	sl.OnSessionStarted(makeHandleWithAfe(t, 2))

	failN(sl, h, 2)
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
	resetDebugTagCountsForTest()
	sl := newSessionListWith(2, 30*time.Millisecond)
	h := makeHandleWithAfe(t, 1)
	sl.OnSessionStarted(h)

	failN(sl, h, 2)
	time.Sleep(60 * time.Millisecond)

	// Post-window probe OK — should zero counter, clear window, and
	// fire the probeOK tag (not the inWindow tag).
	sl.RecordVRpcOutcome(h, 5*time.Millisecond, 1*time.Millisecond, true)
	row := snapshotByID(sl)[1]
	if row.ConsecFailures != 0 {
		t.Errorf("post-probe-OK ConsecFailures = %d, want 0", row.ConsecFailures)
	}
	if !row.QuarantinedUntil.IsZero() {
		t.Errorf("post-probe-OK QuarantinedUntil = %v, want zero", row.QuarantinedUntil)
	}
	counts := snapshotDebugTagCounts()
	if got := counts[tagSessionListAfeQuarantineProbeOK]; got != 1 {
		t.Errorf("probeOK count = %d, want 1", got)
	}
	if got := counts[tagSessionListAfeQuarantineInWindowOK]; got != 0 {
		t.Errorf("inWindowOK count = %d, want 0 (this was a post-window probe)", got)
	}
	checkInvariants(t, sl)
}

// TestSessionList_Quarantine_HalfOpen_ProbeFailResetsCounter_ReTripsOnBurst
// pins the reset-on-lazy-expiry contract. A single failure right after
// the window has elapsed must NOT re-quarantine — it starts a fresh
// counter at 1. Re-tripping requires another full threshold-worth of
// failures, i.e. a real burst rather than one straggler.
func TestSessionList_Quarantine_HalfOpen_ProbeFailResetsCounter_ReTripsOnBurst(t *testing.T) {
	sl := newSessionListWith(2, 30*time.Millisecond)
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
	afterProbe := snapshotByID(sl)[1]
	if !afterProbe.QuarantinedUntil.IsZero() {
		t.Errorf("single post-window failure re-quarantined: QuarantinedUntil = %v, want zero (counter should have reset)", afterProbe.QuarantinedUntil)
	}
	if afterProbe.ConsecFailures != 1 {
		t.Errorf("post-lazy-expiry ConsecFailures = %d, want 1", afterProbe.ConsecFailures)
	}

	// Now the second failure crosses threshold and re-quarantines with
	// a fresh window (which must be later than the first).
	failN(sl, h, 1)
	afterBurst := snapshotByID(sl)[1]
	if afterBurst.QuarantinedUntil.IsZero() {
		t.Fatal("second post-window failure did not re-trip")
	}
	if !afterBurst.QuarantinedUntil.After(firstWindow) {
		t.Errorf("re-tripped window %v not after first window %v", afterBurst.QuarantinedUntil, firstWindow)
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
	resetDebugTagCountsForTest()
	// Long window so we're firmly inside it when the OK arrives — no
	// sleep between the trip and the OK.
	sl := newSessionListWith(2, 500*time.Millisecond)
	h := makeHandleWithAfe(t, 1)
	sl.OnSessionStarted(h)
	sl.OnSessionStarted(makeHandleWithAfe(t, 2)) // neighbor for suppression math

	failN(sl, h, 2)
	if snapshotByID(sl)[1].QuarantinedUntil.IsZero() {
		t.Fatal("setup: AFE 1 should be quarantined after threshold failures")
	}

	// OK arrives mid-window; both counter and window clear, and the
	// inWindowOK tag fires (not probeOK).
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
	counts := snapshotDebugTagCounts()
	if got := counts[tagSessionListAfeQuarantineInWindowOK]; got != 1 {
		t.Errorf("inWindowOK count = %d, want 1", got)
	}
	if got := counts[tagSessionListAfeQuarantineProbeOK]; got != 0 {
		t.Errorf("probeOK count = %d, want 0 (this was mid-window, not post-window)", got)
	}
	checkInvariants(t, sl)
}

func TestSessionList_Quarantine_TripOnceWhileInWindow(t *testing.T) {
	sl := newSessionListWith(2, 500*time.Millisecond)
	h := makeHandleWithAfe(t, 1)
	sl.OnSessionStarted(h)

	failN(sl, h, 2)
	firstWindow := snapshotByID(sl)[1].QuarantinedUntil
	// Additional failures during the window: counter grows, window
	// does NOT shift. Refreshing the window on every failure would
	// mean a persistent outage never re-enters half-open.
	time.Sleep(5 * time.Millisecond)
	failN(sl, h, 20)
	row := snapshotByID(sl)[1]
	if !row.QuarantinedUntil.Equal(firstWindow) {
		t.Errorf("in-window window shifted: got %v, want %v", row.QuarantinedUntil, firstWindow)
	}
	if row.ConsecFailures < 22 {
		t.Errorf("in-window ConsecFailures = %d, want >= 22 (counter keeps growing)", row.ConsecFailures)
	}
	checkInvariants(t, sl)
}

func TestSessionList_Quarantine_PoolWideSuppression(t *testing.T) {
	sl := newSessionListWith(2, 500*time.Millisecond)
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
	sl := newSessionListWith(2, 500*time.Millisecond)
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

// TestSessionList_Quarantine_DenominatorBoundary pins the integer-math
// threshold in ReadyAfes: 7/10 = exactly 70% → suppresses; 7/11 = 63%
// → doesn't. Protects against a future `>=` → `>` typo that would
// silently shift the guard by one AFE.
func TestSessionList_Quarantine_DenominatorBoundary(t *testing.T) {
	// Helper closure — builds a fresh sessionList with N ready AFEs
	// and quarantines the first `bad` of them.
	build := func(t *testing.T, ready, bad int) *sessionList {
		t.Helper()
		sl := newSessionListWith(1, time.Minute) // threshold=1 → single failure trips
		for i := 1; i <= ready; i++ {
			h := makeHandleWithAfe(t, AfeID(i))
			sl.OnSessionStarted(h)
			if i <= bad {
				failN(sl, h, 1)
			}
		}
		return sl
	}

	// Exactly 70% → suppresses; every ready AFE surfaces.
	slSuppress := build(t, 10, 7)
	if got := len(slSuppress.ReadyAfes()); got != 10 {
		t.Errorf("7/10 (70%%) suppression: ReadyAfes len = %d, want 10", got)
	}
	checkInvariants(t, slSuppress)

	// Just below 70% (63%) → filter fires; only the 4 healthy AFEs
	// surface.
	slFilter := build(t, 11, 7)
	ready := slFilter.ReadyAfes()
	if len(ready) != 4 {
		t.Errorf("7/11 (63%%) filter: ReadyAfes len = %d, want 4 (only healthy AFEs)", len(ready))
	}
	for _, s := range ready {
		if s.ID <= 7 {
			t.Errorf("filtered result includes quarantined AFE %d", s.ID)
		}
	}
	checkInvariants(t, slFilter)
}

func TestSessionList_Quarantine_SnapshotFieldsPopulated(t *testing.T) {
	sl := newSessionListWith(2, 500*time.Millisecond)
	hA := makeHandleWithAfe(t, 1) // will trip
	hB := makeHandleWithAfe(t, 2) // stays healthy
	sl.OnSessionStarted(hA)
	sl.OnSessionStarted(hB)

	failN(sl, hA, 2)
	sl.RecordVRpcOutcome(hB, 5*time.Millisecond, 1*time.Millisecond, true)

	byID := snapshotByID(sl)
	if a := byID[1]; a.ConsecFailures != 2 || a.QuarantinedUntil.IsZero() {
		t.Errorf("AFE 1 row = %+v, want ConsecFailures=2 and QuarantinedUntil non-zero", a)
	}
	if b := byID[2]; b.ConsecFailures != 0 || !b.QuarantinedUntil.IsZero() {
		t.Errorf("AFE 2 row = %+v, want ConsecFailures=0 and QuarantinedUntil zero", b)
	}
	checkInvariants(t, sl)
}
