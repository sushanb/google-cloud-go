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

// Session lifecycle glue for SessionPoolImpl: session-hooks callbacks
// (OnStart/OnActive/OnClose), Close/teardown, the per-session close-
// reason ledger, and the background maintenance ticker (heartbeat) that
// drives uptime sampling and stuck-session sweeping.

package internal

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	spb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
	btopt "cloud.google.com/go/bigtable/internal/option"
)

// waitServerCloseGrace bounds how long a session may sit in
// StateWaitServerClose before the pool force-closes it. The server should
// EOF the stream promptly after acknowledging CloseSession; if it doesn't,
// this gives us a deterministic teardown so OnClose fires and counters move.
const waitServerCloseGrace = 30 * time.Second

// sampleActiveUptimes snapshots the currently-active session list under
// the pool lock and records each session's current age into the
// session.uptime histogram. Sampling happens without the pool lock so
// tracer work never blocks CheckoutSession / OnClose.
func (p *SessionPoolImpl) sampleActiveUptimes(ctx context.Context) {
	handles := p.allHandles()
	for _, sh := range handles {
		if sh == nil || sh.session == nil {
			continue
		}
		if State(sh.session.state.Load()) != StateReady {
			continue
		}
		sh.session.SampleUptime(ctx)
	}
}

// sweepStuckSessions scans the pool for sessions parked in
// StateWaitServerClose beyond waitServerCloseGrace and force-closes them.
// Runs from PerformScaling at the heartbeat cadence; takes p.mu only long
// enough to snapshot the (handle, last-state-change) tuples then issues
// ForceClose calls outside the lock.
func (p *SessionPoolImpl) sweepStuckSessions() {
	type victim struct {
		sess     *Session
		stuckFor time.Duration
	}
	var victims []victim

	for _, sh := range p.allHandles() {
		if sh == nil || sh.session == nil {
			continue
		}
		stuck := State(sh.session.state.Load()) == StateWaitServerClose
		since := time.Since(time.Unix(0, sh.session.lastStateChangeNano.Load()))
		if stuck && since > waitServerCloseGrace {
			victims = append(victims, victim{sess: sh.session, stuckFor: since})
		}
	}

	for _, v := range victims {
		// One tag per swept victim — the count IS the "stuck sessions
		// per minute" gauge. Server that responsibly EOFed the stream
		// after our CloseSession never triggers this.
		recordDebugTag(tagSessionPoolStuckSessionSwept)
		btopt.Debugf(nil, "POOL %s sweepStuckSessions: force-closing %s stuck in WaitServerClose for %v",
			p.poolName, v.sess.LogName(), v.stuckFor.Round(time.Second))
		v.sess.ForceClose(&spb.CloseSessionRequest{
			Reason:      spb.CloseSessionRequest_CLOSE_SESSION_REASON_ERROR,
			Description: "stuck in WaitServerClose past grace",
		})
	}
}

// bumpCloseReason atomically increments the close-reason counter; the map
// is keyed by label so the set of reasons can grow without struct churn.
func (p *SessionPoolImpl) bumpCloseReason(label string) {
	if label == "" {
		label = "Unspecified"
	}
	c, _ := p.m.closesByReason.LoadOrStore(label, new(atomic.Int64))
	c.(*atomic.Int64).Add(1)
}

// recordSessionClose marks a session as retired exactly once and bumps
// sessionsClosed + the close-reason histogram. Called from every removal
// site (OnClose, CheckoutSession's dead-detect, Pool.Close) so the
// counter reflects pool-side retirements promptly even when the
// underlying session's hooks.OnClose hasn't fired yet (e.g. the server
// hasn't EOFed the stream). The once-flag lives on the Session so it
// dedupes across paths.
//
// fallbackReason is used only when the session itself hasn't recorded a
// reason yet — e.g. CheckoutSession found a session in StateClosed via
// a race and needs to attribute the retirement.
func (p *SessionPoolImpl) recordSessionClose(s *Session, fallbackReason string) {
	if s == nil {
		return
	}
	if !s.poolCloseRecorded.CompareAndSwap(false, true) {
		return
	}
	reason := s.CloseReason()
	if reason == "" {
		reason = fallbackReason
	}
	p.m.sessionsClosed.Add(1)
	p.bumpCloseReason(reason)
}

// bumpStartingClose is the recordSessionClose variant for sessions that
// died before reaching active state — they're held in startingSessions
// and never got a poolHandle stamp, so OnClose's starting-branch is the
// only signal we get. Wraps the same once-flag for consistency.
func (p *SessionPoolImpl) bumpStartingClose(s *Session) {
	p.recordSessionClose(s, "FailedToStart")
}

// snapshotCloseReasons returns the per-reason counts as a flat map.
func (p *SessionPoolImpl) snapshotCloseReasons() map[string]int64 {
	out := map[string]int64{}
	p.m.closesByReason.Range(func(k, v interface{}) bool {
		out[k.(string)] = v.(*atomic.Int64).Load()
		return true
	})
	return out
}

// Close gracefully closes all active sessions in the pool, bounded by a 30s
// timeout. Sessions are closed concurrently; Close blocks until every
// per-session graceful Close returns (or the bounded ctx fires). Only after
// the WaitGroup completes do we cancel poolCtx, which tears down any
// remaining session goroutines (readLoop/heartBeatLoop) via Session.Start's
// ctx supervisor.
func (p *SessionPoolImpl) Close() error {
	// Phase 1: take a snapshot under lock and mark the pool closed so no new
	// sessions are admitted while we drain.
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	p.mu.Unlock()

	// Snapshot AFTER marking closed so any OnActive races have either
	// (a) already added to sl, in which case we see them, or (b) will
	// see p.closed and route straight to ForceClose without registering.
	snapshot := p.allHandles()

	// Record the closes up-front (with PoolClose as the fallback reason)
	// so the debug counters reflect retirement immediately, even though the
	// actual graceful Close on each session is still in flight. Also drop
	// the handles from the AFE-aware sessionList so a concurrent picker
	// racing with teardown never returns a retired session.
	for _, sh := range snapshot {
		if sh != nil && sh.session != nil {
			if !sh.createdAt.IsZero() {
				p.recordLifetime(time.Since(sh.createdAt))
			}
			p.recordSessionClose(sh.session, "PoolClose")
		}
		p.removeSession(sh)
	}

	// Phase 2: kick off graceful Close for every session with a bounded ctx
	// that is independent of poolCtx — so Session.Close can attempt to drain
	// in-flight RPCs without being immediately killed by poolCancel below.
	closeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	for _, sh := range snapshot {
		if sh.session == nil {
			continue
		}
		wg.Add(1)
		go func(s *Session) {
			defer wg.Done()
			s.Close(closeCtx, &spb.CloseSessionRequest{
				Reason:      spb.CloseSessionRequest_CLOSE_SESSION_REASON_USER,
				Description: "graceful pool teardown",
			})
		}(sh.session)
	}

	// Phase 3: wait for all graceful closes to finish (or for closeCtx to
	// fire — Session.Close itself selects on its ctx and ForceCloses on
	// expiry, so the WaitGroup will unblock either way).
	wg.Wait()
	if closeCtx.Err() != nil {
		// Wait unblocked because the 30s bound expired — at least one
		// session's graceful drain didn't complete and got ForceClosed
		// as a result. Meaningfully different signal from a clean pool
		// teardown, so it gets its own tag.
		recordDebugTag(tagSessionPoolDrainTimeout)
	}

	// Phase 4: cancel poolCtx to bring down any lingering session goroutines
	// (readLoop/heartBeatLoop supervisors) that were started from this pool.
	if p.poolCancel != nil {
		p.poolCancel()
	}
	return nil
}

// OnStart is a no-op callback for session start.
func (p *SessionPoolImpl) OnStart(ctx context.Context) {}

// OnActive is triggered when a background session finishes its open session req and becomes active.
// The session is wrapped inside a SessionHandle and registered into the ready sessions list!
func (p *SessionPoolImpl) OnActive(s *Session) {
	// Callers hold this method's body under p.mu for the full duration.
	// Keep new work here allocation-only / atomic-only — anything that
	// blocks would deadlock the read loop and heartbeat scheduler that
	// contend for p.mu. See SessionHooks doc: "hooks must not block."
	p.mu.Lock()
	defer p.mu.Unlock()

	delete(p.startingSessions, s)

	if p.closed {
		// Dispatch to a goroutine so this method returns and releases
		// p.mu before the OnClose callback chain fires. ForceClose →
		// notifyClosed → hooks.onClose == p.OnClose, and p.OnClose
		// re-acquires p.mu — synchronous would deadlock on the
		// non-reentrant mutex. Race window: Close() sets p.closed=true
		// then releases p.mu; a session already in flight through
		// OpenSession can land here up to ~30s later.
		go s.ForceClose(&spb.CloseSessionRequest{
			Reason:      spb.CloseSessionRequest_CLOSE_SESSION_REASON_ERROR,
			Description: "pool closed before session became active",
		})
		return
	}

	// Dup-check via the once-set poolHandle. If OnActive already ran for
	// this session, poolHandle is non-nil and we exit idempotently.
	if s.poolHandle.Load() != nil {
		return
	}

	sh := NewSessionHandle(s, time.Now())
	s.poolHandle.Store(sh)
	p.m.sessionsOpened.Add(1)

	// Register the newly-Active session in its AFE bucket. PeerInfo is
	// guaranteed populated at this point — handleOpenSession parses it
	// synchronously before firing onActive (see session_lifecycle.go).
	p.registerActive(sh)

	// New session is immediately idle. Post a wake-up so a waiting
	// worker can grab it without waiting out the 50ms safety timer.
	p.signalFree()
}

// OnClosing is fired by the Session at the FIRST transition out of Ready
// (handleGoAway, Close, ForceClose, handleClose — whichever wins). Java
// parity: onSessionClosing. Removes the session from the pool's
// operational structures immediately so:
//   - the picker's AFE idle queue no longer sees it,
//   - it no longer counts toward the scale-up gate,
//   - PerformScaling gets a chance to replace it right away,
//
// even though the actual close may take up to waitServerCloseGrace to
// complete. This is what lets CheckoutSession skip the per-miss dead-
// sweep — dying sessions leave the pool's accounting the instant they
// start dying, not at end-of-teardown.
func (p *SessionPoolImpl) OnClosing(s *Session) {
	// Still-starting sessions leave the pool via OnClose's
	// bumpStartingClose path — they were never promoted to Active, so
	// s.poolHandle was never stored.
	p.mu.Lock()
	_, starting := p.startingSessions[s]
	p.mu.Unlock()
	if starting {
		return
	}
	removed := s.poolHandle.Load()
	if removed == nil {
		return
	}
	if !removed.createdAt.IsZero() {
		p.recordLifetime(time.Since(removed.createdAt))
	}

	// Remove from the AFE idle queue too. sessionList keeps refCount
	// alive (in-flight vRPCs still complete via the session) until
	// OnClose fires and drops the handle entirely. This also decrements
	// sl.readyCount, freeing a slot in the scale-up budget.
	p.markClosing(removed)

	if p.readyCount() < p.maxSessions {
		go p.PerformScaling(p.poolCtx)
	}
}

// OnClose fires at the end of teardown — the stream has actually
// closed. By this point OnClosing has already dropped the session
// from sl.readyCount and the picker's idle queue. This callback
// finalizes: drop the AFE handle from sl (refCount → 0, out of
// handleToAfe) and record the close in the reason ledger. The
// *Session → *SessionHandle back-ref is Session.poolHandle, set
// once in OnActive.
func (p *SessionPoolImpl) OnClose(s *Session, err error) {
	p.mu.Lock()
	if _, starting := p.startingSessions[s]; starting {
		delete(p.startingSessions, s)
		p.mu.Unlock()
		// A session that was never promoted to active still counts toward
		// the close ledger — bumpStartingClose is the once-flag path.
		p.bumpStartingClose(s)
		// A start that never reached Active but failed abnormally still
		// contributes to the consecutive-failure signal. createSession
		// already routes the OpenSession error through budget.Release
		// (which applies the creation penalty); this counter is the
		// separate "did any session make progress" signal Java tracks.
		p.noteAbnormalCloseIfAny(s)
		return
	}
	p.mu.Unlock()

	if handle := s.poolHandle.Load(); handle != nil {
		p.removeSession(handle)
	}
	// recordSessionClose is once-guarded via s.poolCloseRecorded — safe
	// to call even when OnClosing already invoked it.
	p.recordSessionClose(s, "")
	p.noteAbnormalCloseIfAny(s)
}

// noteAbnormalCloseIfAny bumps the consecutive-failure counter when
// the session's final close reason classifies as abnormal (same gate
// the debug tracer uses for tagSessionAbnormalClose). When the counter
// crosses consecutiveFailureThreshold, every parked waiter is woken
// with ErrConsecutiveFailures and the counter is reset. Java parity:
// SessionPoolImpl.handleSessionClose lines 572-586.
func (p *SessionPoolImpl) noteAbnormalCloseIfAny(s *Session) {
	if !isAbnormalCloseReason(s.CloseReason()) {
		return
	}
	n := p.consecutiveFailures.Add(1)
	threshold := p.consecutiveFailureThreshold.Load()
	if threshold <= 0 || n < threshold {
		return
	}
	// Reset before draining so a concurrent OnClose crossing the
	// threshold again doesn't double-drain the queue. CAS guards
	// against two goroutines both seeing n == threshold and both
	// draining. The loser exits after the CAS fails; the winner
	// drains and logs.
	if !p.consecutiveFailures.CompareAndSwap(n, 0) {
		return
	}
	woken := p.drainWaitersWithErr(ErrConsecutiveFailures)
	if woken > 0 {
		recordDebugTag(tagSessionPoolConsecutiveFailuresTripped)
	}
}

// StartHeartbeat begins the background scaling watchdog evaluation loop.
func (p *SessionPoolImpl) StartHeartbeat(ctx context.Context, interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				p.PerformScaling(ctx)
			}
		}
	}()
}

// StartAfePrune begins the background AFE-handle GC loop. Runs on its
// own afePruneMaxIdle cadence (java-parity: SessionPoolImpl in Java uses
// the same SESSION_LIST_PRUNE_INTERVAL for both the horizon and the
// scheduling tick) — deliberately OFF the 1-sec heartbeat so the sl.mu
// held during the map walk can't contend with serving-path Checkouts
// even under pathological AFE-count growth.
func (p *SessionPoolImpl) StartAfePrune(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(afePruneMaxIdle)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				p.pruneAfes(time.Now())
			}
		}
	}()
}
