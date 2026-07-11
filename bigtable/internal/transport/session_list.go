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
	"sync"
	"time"
)

// AFE-grouping constants — chosen to match Java's SessionList / AfeHandle.
const (
	// afePeakEwmaTau is the PeakEwma decay time constant used for both the
	// per-AFE transport and e2e latency trackers. Same tau (10s) already
	// used by per-session ewma in picker.go — keep them aligned so the
	// forthcoming LeastLatencyPicker behaves comparably to today's
	// PeakEwmaPicker on the same workloads.
	afePeakEwmaTau = 10 * time.Second

	// afeTransportEwmaSeed and afeE2eEwmaSeed seed the per-AFE latency
	// trackers so a brand-new AFE has non-zero cost immediately (avoids
	// LeastLatencyPicker pinning traffic to the newest AFE for the first
	// few samples). Java's SessionList.java uses 500µs / 1ms for the
	// same reason.
	afeTransportEwmaSeed = 500 * time.Microsecond
	afeE2eEwmaSeed       = 1 * time.Millisecond

	// afePruneMaxIdle is the age at which an AfeHandle with refCount==0
	// becomes eligible for GC by sessionList.Prune. Java uses 10 min in
	// SessionList.prune (its "recently used" retention window).
	afePruneMaxIdle = 10 * time.Minute
)

// State model
// -----------
// A registered SessionHandle transitions through:
//
//   NotRegistered → Idle → InFlight → Closing → Closed
//                        ↺ ReleaseToPool
//
//   NotRegistered  no entry in handleToAfe.
//   Idle           handleToAfe[sh] = afe, sh in afe.sessions, inExpectedCount=true.
//   InFlight       handleToAfe[sh] = afe, sh NOT in afe.sessions, inExpectedCount=true.
//   Closing        handleToAfe[sh] = afe, sh NOT in afe.sessions, inExpectedCount=false.
//   Closed         handleToAfe has no entry for sh.
//
// Transitions:
//   NotRegistered → Idle       OnSessionStarted
//   Idle          → InFlight   Checkout
//   InFlight      → Idle       ReleaseToPool     (only if inExpectedCount)
//   {Idle,InFlight} → Closing  OnSessionClosing
//   {Idle,InFlight,Closing} → Closed  OnSessionClosed
//
// Invariants (all guarded by sl.mu):
//
//   I1  sh.inExpectedCount == true  ⇒  handleToAfe[sh] != nil
//   I2  readyCount == count of sh in handleToAfe with inExpectedCount==true
//   I3  afesWithReady == { afe : len(afe.sessions) > 0 }, as a set, order irrelevant
//   I4  afe.refCount == count of sh in handleToAfe pointing at afe (idle + inFlight + closing)
//   I5  sh in afe.sessions  ⇒  handleToAfe[sh] == afe  and  inExpectedCount==true
//   I6  refCount is decremented ONLY on OnSessionClosed (Closing keeps the slot warm)
//
// Lock order: sl.mu ONLY. Never take pool.mu while holding sl.mu.

// afeHandle is the per-AFE bucket in sessionList. Owns the FIFO idle
// queue, the refCount (idle + inFlight + closing — see I4/I6), and the
// two per-AFE PeakEwma trackers that LeastLatencyPicker consumes.
//
// All fields are guarded by the enclosing sessionList's mu.
type afeHandle struct {
	id            afeID
	sessions      []*SessionHandle // idle queue, FIFO
	refCount      int              // idle + inFlight + closing (I4/I6)
	lastConnected time.Time        // stamped on OnSessionStarted; drives Prune
	transportEwma *PeakEwma        // updated on OK vRPCs only (Java parity)
	e2eEwma       *PeakEwma        // updated on OK vRPCs only (Java parity)
}

// ID returns the AFE identifier for this bucket.
func (a *afeHandle) ID() afeID { return a.id }

// afeSnapshot is an immutable view of an afeHandle sufficient for a picker
// to score / pick without needing to hold sessionList.mu on the hot path.
type afeSnapshot struct {
	Handle         *afeHandle
	IdleCount      int
	NumOutstanding int     // refCount − IdleCount, ≥ 0
	TransportCost  float64 // 0 if never updated (no OK vRPCs yet)
	E2eCost        float64 // 0 if never updated
}

// sessionList groups sessions by the AFE they landed on. Mirrors Java's
// SessionList (java-bigtable/.../session/SessionList.java) but with a
// dedicated mutex rather than piggy-backing on the pool's monitor.
//
// See the "State model" block above for the handle state machine and
// the six invariants (I1–I6) every method preserves. Lock ordering with
// the pool: sl.mu is always innermost — pool.mu is never taken while
// holding sl.mu (SESSION_COMPONENT_SPEC §B8).
type sessionList struct {
	mu            sync.Mutex
	afeHandles    map[afeID]*afeHandle
	afesWithReady []*afeHandle // subset with len(sessions) > 0 (I3)
	handleToAfe   map[*SessionHandle]*afeHandle
	// readyCount == number of registered handles still in the pool's
	// expected set. Direct O(1) replacement for the old
	// len(SessionPoolImpl.sessions) that gated scale-up. See I2.
	readyCount int
}

// newSessionList returns an empty sessionList ready for use.
func newSessionList() *sessionList {
	return &sessionList{
		afeHandles:  make(map[afeID]*afeHandle),
		handleToAfe: make(map[*SessionHandle]*afeHandle),
	}
}

// OnSessionStarted registers a newly-Active session into its AFE bucket
// (NotRegistered → Idle). Must be called after PeerInfo is populated —
// handleOpenSession guarantees this synchronously before hooks.onActive
// fires. A session whose AfeID() is 0 (server didn't send the peer-info
// header) lands in the AfeID=0 bucket — it still gets picked but never
// counts toward AFE-fanout.
//
// A duplicate register is a silent no-op. This isn't defensive; sessions
// are one-shot and OnActive fires exactly once per handle. The early
// return asserts that invariant.
func (sl *sessionList) OnSessionStarted(sh *SessionHandle) {
	if sh == nil {
		return
	}
	id := sh.session.AfeID()
	sl.mu.Lock()
	defer sl.mu.Unlock()
	if _, dup := sl.handleToAfe[sh]; dup {
		return
	}
	afe := sl.afeHandles[id]
	if afe == nil {
		afe = &afeHandle{
			id:            id,
			transportEwma: NewPeakEwma(afePeakEwmaTau),
			e2eEwma:       NewPeakEwma(afePeakEwmaTau),
		}
		sl.afeHandles[id] = afe
	}
	if afe.enqueueLocked(sh) { // wasEmpty → afe enters the ready set (I3)
		sl.afesWithReady = append(sl.afesWithReady, afe)
	}
	afe.refCount++ // I4
	afe.lastConnected = time.Now()
	sl.handleToAfe[sh] = afe
	sh.inExpectedCount = true
	sl.readyCount++ // I2
}

// Checkout dequeues one idle session from afe (Idle → InFlight). Returns
// nil when afe is nil or its queue is empty — a legitimate race with a
// concurrent Checkout or an OnSessionClosing that just drained the last
// idle handle. Callers should have picked from ReadyAfes() first.
func (sl *sessionList) Checkout(afe *afeHandle) *SessionHandle {
	if afe == nil {
		return nil
	}
	sl.mu.Lock()
	defer sl.mu.Unlock()
	sh, nowEmpty := afe.dequeueLocked()
	if sh == nil {
		return nil
	}
	if nowEmpty { // afe leaves the ready set (I3)
		sl.removeFromReadyLocked(afe)
	}
	return sh
}

// ReleaseToPool re-appends sh to its AFE's idle queue (InFlight → Idle),
// making it pickable again.
//
// NO-OP when the handle has already left the pool's expected set — i.e.
// OnSessionClosing fired concurrently while the RPC was in flight
// (GOAWAY, graceful Close, or ForceClose). Enforcing this is invariant
// I5: only Ready/InFlight handles may appear in afe.sessions. Omitting
// the guard causes the WaitServerClose retry storm documented in the
// project_bigtable_release_to_pool_bug memory — a drained session gets
// re-picked and every next attempt hits "session is not active".
func (sl *sessionList) ReleaseToPool(sh *SessionHandle) {
	if sh == nil {
		return
	}
	sl.mu.Lock()
	defer sl.mu.Unlock()
	if !sh.inExpectedCount { // I5 enforcement — see doc.
		return
	}
	afe := sl.handleToAfe[sh]
	if afe == nil {
		return
	}
	if afe.enqueueLocked(sh) { // wasEmpty → afe re-enters the ready set (I3)
		sl.afesWithReady = append(sl.afesWithReady, afe)
	}
}

// OnSessionClosing transitions {Idle, InFlight} → Closing. Drops the
// handle from the pool's expected set (I2) and, if it was idle, removes
// it from its AFE's queue (I3/I5). refCount is intentionally NOT
// decremented — I6 keeps the slot warm until OnSessionClosed so the
// sizer sees stable capacity across the close handshake.
func (sl *sessionList) OnSessionClosing(sh *SessionHandle) {
	if sh == nil {
		return
	}
	sl.mu.Lock()
	defer sl.mu.Unlock()
	sl.dropMembershipLocked(sh) // I2
	afe := sl.handleToAfe[sh]
	if afe == nil {
		return
	}
	if removed, nowEmpty := afe.removeIfPresentLocked(sh); removed && nowEmpty {
		sl.removeFromReadyLocked(afe) // I3
	}
}

// OnSessionClosed transitions {Idle, InFlight, Closing} → Closed.
// Decrements the AFE's refCount (I4/I6), purges handleToAfe, and
// removes the handle from the idle queue if it was still present
// (force-close paths that skip OnSessionClosing). The AFE bucket itself
// stays until Prune GCs it after afePruneMaxIdle so brief
// connect/disconnect churn doesn't churn the map.
func (sl *sessionList) OnSessionClosed(sh *SessionHandle) {
	if sh == nil {
		return
	}
	sl.mu.Lock()
	defer sl.mu.Unlock()
	sl.dropMembershipLocked(sh) // I2 (idempotent — Closing may have fired)
	afe := sl.handleToAfe[sh]
	if afe == nil {
		return
	}
	delete(sl.handleToAfe, sh)
	if afe.refCount > 0 {
		afe.refCount-- // I4/I6
	}
	if removed, nowEmpty := afe.removeIfPresentLocked(sh); removed && nowEmpty {
		sl.removeFromReadyLocked(afe) // I3
	}
}

// RecordVRpcOutcome updates the AFE's PeakEwma trackers with an OK
// response's e2e and backend latencies. Non-OK results are dropped —
// Java parity, SessionList.java:181-187 — so a fast-failing AFE never
// looks fastest to LeastLatencyPicker. transportEwma tracks e2e −
// backend; e2eEwma tracks e2e directly.
func (sl *sessionList) RecordVRpcOutcome(sh *SessionHandle, e2e, backend time.Duration, ok bool) {
	if !ok || sh == nil {
		return
	}
	sl.mu.Lock()
	afe := sl.handleToAfe[sh]
	sl.mu.Unlock()
	if afe == nil {
		return
	}
	if e2e > 0 {
		afe.e2eEwma.Update(e2e)
	}
	if transport := e2e - backend; transport > 0 {
		afe.transportEwma.Update(transport)
	}
}

// ReadyAfes returns an immutable snapshot of AFEs with at least one
// idle session, plus the per-AFE fields a picker needs to score them
// (idle/inflight counts and current EWMA costs). Membership matches
// invariant I3 — len(afe.sessions) > 0. The picker can iterate this
// slice without holding sessionList.mu.
func (sl *sessionList) ReadyAfes() []afeSnapshot {
	sl.mu.Lock()
	defer sl.mu.Unlock()
	if len(sl.afesWithReady) == 0 {
		return nil
	}
	out := make([]afeSnapshot, 0, len(sl.afesWithReady))
	for _, afe := range sl.afesWithReady {
		idle := len(afe.sessions)
		inflight := afe.refCount - idle
		if inflight < 0 {
			inflight = 0
		}
		out = append(out, afeSnapshot{
			Handle:         afe,
			IdleCount:      idle,
			NumOutstanding: inflight,
			TransportCost:  afe.transportEwma.Value(),
			E2eCost:        afe.e2eEwma.Value(),
		})
	}
	return out
}

// AllHandles returns a snapshot of every registered SessionHandle —
// every state EXCEPT NotRegistered and Closed. Order is not stable
// (map iteration); callers that need order should sort by
// sh.createdAt. Cheap: single lock acquisition, no per-entry work
// beyond the slice grow.
func (sl *sessionList) AllHandles() []*SessionHandle {
	sl.mu.Lock()
	defer sl.mu.Unlock()
	if len(sl.handleToAfe) == 0 {
		return nil
	}
	out := make([]*SessionHandle, 0, len(sl.handleToAfe))
	for sh := range sl.handleToAfe {
		out = append(out, sh)
	}
	return out
}

// ReadyCount returns the number of handles still in the pool's
// expected set — the "count that gates scale-up." See I2. O(1).
func (sl *sessionList) ReadyCount() int {
	sl.mu.Lock()
	defer sl.mu.Unlock()
	return sl.readyCount
}

// Snapshot returns a stable, human-readable view of every AFE bucket
// currently tracked, sorted by ID for deterministic rendering. Cheap
// (single lock acquisition) — safe to call from the sessionz / afez
// snapshot paths.
func (sl *sessionList) Snapshot() []AfeSnapshotRow {
	sl.mu.Lock()
	defer sl.mu.Unlock()
	if len(sl.afeHandles) == 0 {
		return nil
	}
	rows := make([]AfeSnapshotRow, 0, len(sl.afeHandles))
	for _, afe := range sl.afeHandles {
		idle := len(afe.sessions)
		rows = append(rows, AfeSnapshotRow{
			ID:            int64(afe.id),
			RefCount:      afe.refCount,
			IdleCount:     idle,
			TransportEwma: time.Duration(afe.transportEwma.Value()),
			E2eEwma:       time.Duration(afe.e2eEwma.Value()),
			LastConnected: afe.lastConnected,
		})
	}
	sortAfeRowsByID(rows)
	return rows
}

// Prune deletes AfeHandles that have been empty (refCount == 0) since
// before `now.Sub(afePruneMaxIdle)`. AFEs with any live session (idle
// or in-flight) are never pruned. Java parity: SessionList.prune runs
// on a 10 min cadence.
func (sl *sessionList) Prune(now time.Time) {
	sl.mu.Lock()
	defer sl.mu.Unlock()
	cutoff := now.Add(-afePruneMaxIdle)
	for id, afe := range sl.afeHandles {
		if afe.refCount > 0 {
			continue
		}
		if !afe.lastConnected.IsZero() && afe.lastConnected.After(cutoff) {
			continue
		}
		// I3: an empty-refCount AFE should never be in afesWithReady
		// (queue is empty), but drop it here so a slipped invariant
		// doesn't dangle a stale pointer past the delete.
		sl.removeFromReadyLocked(afe)
		delete(sl.afeHandles, id)
	}
}

// dropMembershipLocked flips sh.inExpectedCount to false and decrements
// readyCount if the flag was true. Idempotent — a second call is a
// no-op. Both OnSessionClosing and OnSessionClosed call this so a
// handle leaves the "member" set exactly once (I2), regardless of which
// path runs first or whether both run.
func (sl *sessionList) dropMembershipLocked(sh *SessionHandle) {
	if sh.inExpectedCount {
		sh.inExpectedCount = false
		sl.readyCount--
	}
}

// removeFromReadyLocked removes afe from sl.afesWithReady in O(N).
// Safe to call when afe is not present (no-op). Caller must hold sl.mu.
func (sl *sessionList) removeFromReadyLocked(afe *afeHandle) {
	for i, a := range sl.afesWithReady {
		if a == afe {
			sl.afesWithReady = append(sl.afesWithReady[:i], sl.afesWithReady[i+1:]...)
			return
		}
	}
}

// enqueueLocked appends sh to the idle FIFO. Returns whether the queue
// was empty BEFORE the append — callers use this to toggle afesWithReady
// membership (I3). Caller must hold sl.mu.
func (a *afeHandle) enqueueLocked(sh *SessionHandle) (wasEmpty bool) {
	wasEmpty = len(a.sessions) == 0
	a.sessions = append(a.sessions, sh)
	return
}

// dequeueLocked pops the FIFO head. Returns nil,false if empty.
// nowEmpty reports whether the queue is empty AFTER the pop — callers
// use this to toggle afesWithReady membership (I3). Caller must hold
// sl.mu.
func (a *afeHandle) dequeueLocked() (sh *SessionHandle, nowEmpty bool) {
	if len(a.sessions) == 0 {
		return nil, false
	}
	sh = a.sessions[0]
	a.sessions = a.sessions[1:]
	nowEmpty = len(a.sessions) == 0
	return
}

// removeIfPresentLocked removes sh from the idle queue if present.
// removed=false when sh isn't in the queue (already checked out or
// never enqueued). nowEmpty is meaningful only when removed=true.
// Caller must hold sl.mu.
func (a *afeHandle) removeIfPresentLocked(sh *SessionHandle) (removed, nowEmpty bool) {
	for i, h := range a.sessions {
		if h == sh {
			a.sessions = append(a.sessions[:i], a.sessions[i+1:]...)
			return true, len(a.sessions) == 0
		}
	}
	return false, false
}

// sortAfeRowsByID sorts an AFE snapshot slice by ID ascending. Used to
// keep the sessionz / afez rendering deterministic across snapshots
// (map iteration order is randomised in Go).
func sortAfeRowsByID(rows []AfeSnapshotRow) {
	for i := 1; i < len(rows); i++ {
		for j := i; j > 0 && rows[j-1].ID > rows[j].ID; j-- {
			rows[j-1], rows[j] = rows[j], rows[j-1]
		}
	}
}
