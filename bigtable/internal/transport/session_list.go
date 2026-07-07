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

// afeHandle is the per-AFE bucket in sessionList. It owns the FIFO queue
// of currently-idle sessions on this AFE, the refCount (idle+inUse), and
// the two per-AFE PeakEwma trackers that LeastLatencyPicker consumes.
//
// All fields are guarded by the enclosing sessionList's mu.
type afeHandle struct {
	id            afeID
	sessions      []*SessionHandle // idle queue, FIFO
	refCount      int              // idle + inUse
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
// Contract for callers holding the pool's own mutex: NEVER take pool.mu
// while holding sessionList.mu. sessionList methods are self-contained
// (only touch sessionList state) so this ordering is easy to preserve.
//
// The list is unwired in this commit — callers land in a follow-up.
type sessionList struct {
	mu            sync.Mutex
	afeHandles    map[afeID]*afeHandle
	afesWithReady []*afeHandle              // subset with len(sessions) > 0
	handleToAfe   map[*SessionHandle]*afeHandle
	// readyCount is the count of handles currently marked
	// inExpectedCount — i.e. registered via OnSessionStarted and not yet
	// dropped via OnSessionClosing/OnSessionClosed. Direct replacement
	// for the old len(SessionPoolImpl.sessions) that gated scale-up.
	// Guarded by mu.
	readyCount int
}

// newSessionList returns an empty sessionList ready for use.
func newSessionList() *sessionList {
	return &sessionList{
		afeHandles:  make(map[afeID]*afeHandle),
		handleToAfe: make(map[*SessionHandle]*afeHandle),
	}
}

// OnSessionStarted registers a newly-Active session in its AFE bucket.
// Must be called after PeerInfo is populated (step 1 guarantees this
// synchronously in handleOpenSession, before hooks.onActive fires). A
// session whose AfeID() is 0 (server didn't send the peer-info header)
// is placed in the AfeID=0 bucket — it still gets picked, but never
// counts toward AFE-fanout.
func (sl *sessionList) OnSessionStarted(sh *SessionHandle) {
	if sh == nil {
		return
	}
	id := sh.session.AfeID()
	sl.mu.Lock()
	defer sl.mu.Unlock()
	if _, dup := sl.handleToAfe[sh]; dup {
		return // idempotent: same session registered twice
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
	wasEmpty := len(afe.sessions) == 0
	afe.sessions = append(afe.sessions, sh)
	afe.refCount++
	afe.lastConnected = time.Now()
	if wasEmpty {
		sl.afesWithReady = append(sl.afesWithReady, afe)
	}
	sl.handleToAfe[sh] = afe
	sh.inExpectedCount = true
	sl.readyCount++
}

// Checkout dequeues one idle session from the given AFE. Returns nil if
// the AFE has no idle sessions (defensive — callers should have picked
// from ReadyAfes). Removes the AFE from afesWithReady when its queue
// empties.
func (sl *sessionList) Checkout(afe *afeHandle) *SessionHandle {
	if afe == nil {
		return nil
	}
	sl.mu.Lock()
	defer sl.mu.Unlock()
	if len(afe.sessions) == 0 {
		return nil
	}
	sh := afe.sessions[0]
	afe.sessions = afe.sessions[1:]
	if len(afe.sessions) == 0 {
		sl.removeFromReadyLocked(afe)
	}
	return sh
}

// ReleaseToPool returns a previously checked-out session to its AFE's
// idle queue. Re-adds the AFE to afesWithReady if the queue was empty.
// Idempotent-tolerant: a session not registered here (or on the wrong
// AFE) is silently ignored.
func (sl *sessionList) ReleaseToPool(sh *SessionHandle) {
	if sh == nil {
		return
	}
	sl.mu.Lock()
	defer sl.mu.Unlock()
	afe := sl.handleToAfe[sh]
	if afe == nil {
		return
	}
	wasEmpty := len(afe.sessions) == 0
	afe.sessions = append(afe.sessions, sh)
	if wasEmpty {
		sl.afesWithReady = append(sl.afesWithReady, afe)
	}
}

// OnSessionClosing removes the session from its AFE's idle queue if it
// was idle. refCount is not decremented until OnSessionClosed — a
// closing session that is currently in-flight still counts.
func (sl *sessionList) OnSessionClosing(sh *SessionHandle) {
	if sh == nil {
		return
	}
	sl.mu.Lock()
	defer sl.mu.Unlock()
	if sh.inExpectedCount {
		sh.inExpectedCount = false
		sl.readyCount--
	}
	afe := sl.handleToAfe[sh]
	if afe == nil {
		return
	}
	if removed := removeFromQueueLocked(afe, sh); removed && len(afe.sessions) == 0 {
		sl.removeFromReadyLocked(afe)
	}
}

// OnSessionClosed decrements the AFE's refCount and drops the handle
// from handleToAfe. The AFE bucket itself is not removed here — Prune
// GCs empty buckets after afePruneMaxIdle so brief connect/disconnect
// churn doesn't churn the map.
func (sl *sessionList) OnSessionClosed(sh *SessionHandle) {
	if sh == nil {
		return
	}
	sl.mu.Lock()
	defer sl.mu.Unlock()
	if sh.inExpectedCount {
		sh.inExpectedCount = false
		sl.readyCount--
	}
	afe := sl.handleToAfe[sh]
	if afe == nil {
		return
	}
	delete(sl.handleToAfe, sh)
	if afe.refCount > 0 {
		afe.refCount--
	}
	// If the session was still idle in the queue at close time, remove
	// it. OnSessionClosing normally runs first, but a caller may skip it
	// on force-close paths.
	if removed := removeFromQueueLocked(afe, sh); removed && len(afe.sessions) == 0 {
		sl.removeFromReadyLocked(afe)
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

// ReadyAfes returns an immutable snapshot of AFEs with at least one idle
// session, along with the per-AFE fields a picker needs to score them
// (idle/inflight counts and current EWMA costs). The picker can iterate
// this slice without holding sessionList.mu.
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

// AllHandles returns a snapshot of every SessionHandle currently
// registered — Ready sessions AND sessions past OnSessionClosing but
// still awaiting OnSessionClosed. Order is not stable (map iteration).
// Callers that need order should sort by sh.createdAt after receiving.
// Cheap: single lock acquisition, no per-entry work beyond the slice
// grow.
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

// ReadyCount returns the count of handles currently marked
// inExpectedCount — the "count that gates scale-up." Equivalent to the
// old len(SessionPoolImpl.sessions) after OnSessionClosing had removed
// dying sessions. O(1).
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
		// Defensive: an empty-refCount AFE should never be in
		// afesWithReady (queue is empty), but if invariants ever slip,
		// drop it here rather than dangling a stale pointer.
		sl.removeFromReadyLocked(afe)
		delete(sl.afeHandles, id)
	}
}

// removeFromReadyLocked removes afe from sl.afesWithReady in O(N).
// Caller must hold sl.mu. Safe to call when afe is not present (no-op).
func (sl *sessionList) removeFromReadyLocked(afe *afeHandle) {
	for i, a := range sl.afesWithReady {
		if a == afe {
			sl.afesWithReady = append(sl.afesWithReady[:i], sl.afesWithReady[i+1:]...)
			return
		}
	}
}

// removeFromQueueLocked removes sh from afe.sessions in O(N). Caller
// must hold sl.mu. Returns whether a removal happened.
func removeFromQueueLocked(afe *afeHandle, sh *SessionHandle) bool {
	for i, h := range afe.sessions {
		if h == sh {
			afe.sessions = append(afe.sessions[:i], afe.sessions[i+1:]...)
			return true
		}
	}
	return false
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
