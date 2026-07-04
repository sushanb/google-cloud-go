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
)

// --- close-reason ledger ---------------------------------------------------

func TestBumpCloseReason_Increments(t *testing.T) {
	p := newTestPool(t, 1, 10)
	p.bumpCloseReason("GoAway")
	p.bumpCloseReason("GoAway")
	p.bumpCloseReason("MissedHeartbeat")
	got := p.snapshotCloseReasons()
	if got["GoAway"] != 2 {
		t.Errorf("GoAway count = %d, want 2", got["GoAway"])
	}
	if got["MissedHeartbeat"] != 1 {
		t.Errorf("MissedHeartbeat count = %d, want 1", got["MissedHeartbeat"])
	}
}

func TestBumpCloseReason_EmptyLabelBecomesUnspecified(t *testing.T) {
	p := newTestPool(t, 1, 10)
	p.bumpCloseReason("")
	if got := p.snapshotCloseReasons()["Unspecified"]; got != 1 {
		t.Errorf("Unspecified count = %d, want 1 (empty label must fold here)", got)
	}
}

func TestRecordSessionClose_OnceFlag(t *testing.T) {
	p := newTestPool(t, 1, 10)
	sh := injectActiveSession(t, p, "s1", time.Now())
	s := sh.session

	// Multiple calls dedupe via s.poolCloseRecorded.
	p.recordSessionClose(s, "First")
	p.recordSessionClose(s, "SecondShouldNotFire")
	p.recordSessionClose(s, "ThirdShouldNotFire")

	if got := p.sessionsClosed.Load(); got != 1 {
		t.Errorf("sessionsClosed = %d, want 1 (once-flag violated)", got)
	}
	reasons := p.snapshotCloseReasons()
	if reasons["First"] != 1 {
		t.Errorf("First count = %d, want 1", reasons["First"])
	}
	if _, ok := reasons["SecondShouldNotFire"]; ok {
		t.Errorf("second-call fallback leaked into ledger: %+v", reasons)
	}
}

func TestRecordSessionClose_UsesSessionReasonOverFallback(t *testing.T) {
	p := newTestPool(t, 1, 10)
	sh := injectActiveSession(t, p, "s1", time.Now())
	sh.session.setCloseReason("GoAway")

	p.recordSessionClose(sh.session, "IgnoredFallback")
	if got := p.snapshotCloseReasons()["GoAway"]; got != 1 {
		t.Errorf("GoAway count = %d, want 1 (session's own reason should win)", got)
	}
	if _, ok := p.snapshotCloseReasons()["IgnoredFallback"]; ok {
		t.Errorf("fallback leaked despite session reason set: %+v", p.snapshotCloseReasons())
	}
}

func TestRecordSessionClose_NilSessionIsNoOp(t *testing.T) {
	p := newTestPool(t, 1, 10)
	p.recordSessionClose(nil, "Anything")
	if got := p.sessionsClosed.Load(); got != 0 {
		t.Errorf("sessionsClosed = %d, want 0 for nil session", got)
	}
}

func TestBumpStartingClose_UsesFailedToStart(t *testing.T) {
	p := newTestPool(t, 1, 10)
	sh := injectActiveSession(t, p, "s1", time.Now())
	p.bumpStartingClose(sh.session)
	if got := p.snapshotCloseReasons()["FailedToStart"]; got != 1 {
		t.Errorf("FailedToStart count = %d, want 1", got)
	}
}

// --- OnActive / OnClose ---------------------------------------------------

func TestOnActive_DuplicateSessionSkipped(t *testing.T) {
	p := newTestPool(t, 1, 10)
	sh := injectActiveSession(t, p, "s1", time.Now())
	before := len(p.sessions)
	// Firing OnActive for a session already in p.sessions must not
	// double-register.
	p.OnActive(sh.session)
	if got := len(p.sessions); got != before {
		t.Errorf("p.sessions len = %d, want %d (duplicate register)", got, before)
	}
}

func TestOnActive_SignalsFree(t *testing.T) {
	p := newTestPool(t, 1, 10)
	// Drain any pre-existing signal.
	select {
	case <-p.freeSignal:
	default:
	}
	// Craft a fresh session and fire OnActive — must post to freeSignal.
	stream := newFakeStream()
	s := NewSession("s-fresh", stream, SessionHooks{
		OnStart:  p.OnStart,
		OnActive: p.OnActive,
		OnClose:  p.OnClose,
	}, SessionTypeTable)
	s.state.Store(int32(StateReady))
	p.OnActive(s)
	select {
	case <-p.freeSignal:
		// expected
	case <-time.After(100 * time.Millisecond):
		t.Error("OnActive did not signal freeSignal within 100ms")
	}
}

func TestOnClose_StartingSessionBumpsFailedToStart(t *testing.T) {
	p := newTestPool(t, 1, 10)
	// Session in startingSessions but not in p.sessions.
	stream := newFakeStream()
	s := NewSession("s-starting", stream, SessionHooks{OnClose: p.OnClose}, SessionTypeTable)
	p.mu.Lock()
	p.startingSessions[s] = true
	p.mu.Unlock()

	p.OnClose(s, nil)

	p.mu.Lock()
	_, stillThere := p.startingSessions[s]
	p.mu.Unlock()
	if stillThere {
		t.Error("session still in startingSessions after OnClose")
	}
	if got := p.snapshotCloseReasons()["FailedToStart"]; got != 1 {
		t.Errorf("FailedToStart count = %d, want 1", got)
	}
}

func TestOnClose_ActiveSessionRemovedAndRecorded(t *testing.T) {
	p := newTestPool(t, 1, 10)
	sh := injectActiveSession(t, p, "s1", time.Now().Add(-time.Second))

	p.OnClose(sh.session, nil)

	p.mu.Lock()
	found := false
	for _, cur := range p.sessions {
		if cur == sh {
			found = true
			break
		}
	}
	p.mu.Unlock()
	if found {
		t.Error("session still in p.sessions after OnClose")
	}
	if got := p.sessionsClosed.Load(); got != 1 {
		t.Errorf("sessionsClosed = %d, want 1", got)
	}
	if got := len(p.snapshotLifetimes()); got != 1 {
		t.Errorf("lifetimes len = %d, want 1 (createdAt was set → recordLifetime should fire)", got)
	}
}

func TestOnClose_IdxNotFoundStillRecords(t *testing.T) {
	p := newTestPool(t, 1, 10)
	// A session neither in p.sessions nor startingSessions — the "already
	// proactively removed" path (CheckoutSession dead-detect or
	// Pool.Close removed it). recordSessionClose should still fire so a
	// hypothetical missed record elsewhere doesn't silently leak the
	// count.
	stream := newFakeStream()
	s := NewSession("s-ghost", stream, SessionHooks{}, SessionTypeTable)

	p.OnClose(s, nil)
	if got := p.sessionsClosed.Load(); got != 1 {
		t.Errorf("sessionsClosed = %d, want 1", got)
	}
}

// --- sweepStuckSessions ---------------------------------------------------

func TestSweepStuckSessions_LeavesFreshAlone(t *testing.T) {
	p := newTestPool(t, 1, 10)
	sh := injectActiveSession(t, p, "s1", time.Now())
	// Transition into WaitServerClose but keep the last-state-change
	// stamp fresh — should be skipped.
	sh.session.state.Store(int32(StateWaitServerClose))
	sh.session.lastStateChangeNano.Store(time.Now().UnixNano())

	p.sweepStuckSessions()

	if State(sh.session.state.Load()) != StateWaitServerClose {
		t.Errorf("fresh WaitServerClose session was swept; state = %v", State(sh.session.state.Load()))
	}
}

func TestSweepStuckSessions_ForceClosesStale(t *testing.T) {
	p := newTestPool(t, 1, 10)
	sh := injectActiveSession(t, p, "s1", time.Now())
	// Fake a session that's been in WaitServerClose past the grace window.
	sh.session.state.Store(int32(StateWaitServerClose))
	stale := time.Now().Add(-waitServerCloseGrace - time.Second).UnixNano()
	sh.session.lastStateChangeNano.Store(stale)

	p.sweepStuckSessions()

	if State(sh.session.state.Load()) != StateClosed {
		t.Errorf("stale WaitServerClose session state = %v, want Closed (ForceClose should have fired)",
			State(sh.session.state.Load()))
	}
}

// --- OnStart no-op --------------------------------------------------------

func TestOnStart_NoOp(t *testing.T) {
	p := newTestPool(t, 1, 10)
	// OnStart takes a ctx and does nothing. Just verify it doesn't
	// panic and doesn't touch any counters.
	before := p.sessionsOpened.Load()
	p.OnStart(nil)
	if got := p.sessionsOpened.Load(); got != before {
		t.Errorf("OnStart mutated sessionsOpened: %d → %d", before, got)
	}
}
