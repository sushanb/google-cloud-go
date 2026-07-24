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
	"runtime/debug"
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
	handles := p.sl.AllHandles()
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
// Runs from Tick at the heartbeat cadence; takes p.mu only long
// enough to snapshot the (handle, last-state-change) tuples then issues
// ForceClose calls outside the lock.
func (p *SessionPoolImpl) sweepStuckSessions() {
	type victim struct {
		sess     *Session
		stuckFor time.Duration
	}
	var victims []victim

	for _, sh := range p.sl.AllHandles() {
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
// and never fired onActive, so onClose's starting-branch is the only
// signal we get. Wraps the same once-flag for consistency.
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
	snapshot := p.sl.AllHandles()

	// Record the closes up-front (with PoolClose as the fallback reason)
	// so the debug counters reflect retirement immediately, even though the
	// actual graceful Close on each session is still in flight. Also drop
	// the handles from the AFE-aware sessionList so a concurrent picker
	// racing with teardown never returns a retired session.
	//
	// Flip closingRecorded / closeRecorded on each handle after recording
	// so the callback chain fired by Phase-2 s.Close (notifyClosing →
	// p.onClosing, closeOnce → p.onClose) short-circuits on the CAS
	// guard. Without this, onClosing would re-run recordLifetime for every
	// session and onClose would call sl.OnSessionClosed a second time —
	// sessionList tolerates it (idempotent), but the lifetimes ring gets
	// 2× the correct entries. Replaces the prior poolHandle.Store(nil)
	// trick, expressing the dedup as an actual dedup flag.
	for _, sh := range snapshot {
		if sh != nil && sh.session != nil {
			if !sh.createdAt.IsZero() {
				p.recordLifetime(time.Since(sh.createdAt))
			}
			p.recordSessionClose(sh.session, "PoolClose")
			sh.closingRecorded.Store(true)
			sh.closeRecorded.Store(true)
		}
		p.sl.OnSessionClosed(sh)
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

// onActive is triggered when a background session finishes its open
// session req and becomes active. The SessionHandle was already minted
// by createSession — this method just publishes it into sl and clears
// the starting-set entry.
//
// The activated CAS makes this safe to invoke twice on the same handle;
// tests exercise the guard (production wires each closure exactly once
// per Session lifetime).
func (p *SessionPoolImpl) onActive(sh *SessionHandle) {
	if !sh.activated.CompareAndSwap(false, true) {
		return
	}
	// Callers hold this method's body under p.mu for the full duration
	// of the map/registration work. Keep new work here allocation-only
	// / atomic-only — anything that blocks would deadlock the read
	// loop and heartbeat scheduler that contend for p.mu. See
	// SessionHooks doc: "hooks must not block."
	p.mu.Lock()
	defer p.mu.Unlock()

	delete(p.startingSessions, sh)

	if p.closed {
		// Dispatch to a goroutine so this method returns and releases
		// p.mu before the onClose callback chain fires. ForceClose →
		// notifyClosed → hooks.onClose fires p.onClose, and p.onClose
		// re-acquires p.mu — synchronous would deadlock on the
		// non-reentrant mutex. Race window: Close() sets p.closed=true
		// then releases p.mu; a session already in flight through
		// OpenSession can land here up to ~30s later.
		go sh.session.ForceClose(&spb.CloseSessionRequest{
			Reason:      spb.CloseSessionRequest_CLOSE_SESSION_REASON_ERROR,
			Description: "pool closed before session became active",
		})
		return
	}

	p.m.sessionsOpened.Add(1)

	// Register the newly-Active session in its AFE bucket. PeerInfo is
	// guaranteed populated at this point — handleOpenSession parses it
	// synchronously before firing onActive (see session_lifecycle.go).
	p.sl.OnSessionStarted(sh)

	// New session is immediately idle. Post a wake-up so a waiting
	// worker can grab it without waiting out the 50ms safety timer.
	p.signalFree()
}

// onClosing is fired by the Session at the FIRST transition out of Ready
// (handleGoAway, Close, ForceClose, handleClose — whichever wins).
// Removes the session from the pool's
// operational structures immediately so:
//   - the picker's AFE idle queue no longer sees it,
//   - it no longer counts toward the scale-up gate,
//   - Tick gets a chance to replace it right away,
//
// even though the actual close may take up to waitServerCloseGrace to
// complete. This is what lets CheckoutSession skip the per-miss dead-
// sweep — dying sessions leave the pool's accounting the instant they
// start dying, not at end-of-teardown.
func (p *SessionPoolImpl) onClosing(sh *SessionHandle) {
	// Still-starting sessions leave the pool via onClose's
	// bumpStartingClose path — they were never promoted to Active, so
	// there's nothing to remove from sl.
	p.mu.Lock()
	_, starting := p.startingSessions[sh]
	p.mu.Unlock()
	if starting {
		return
	}
	if !sh.closingRecorded.CompareAndSwap(false, true) {
		return // Pool.Close Phase-1 already recorded.
	}
	if !sh.createdAt.IsZero() {
		p.recordLifetime(time.Since(sh.createdAt))
	}

	// Remove from the AFE idle queue too. sessionList keeps refCount
	// alive (in-flight vRPCs still complete via the session) until
	// onClose fires and drops the handle entirely. This also decrements
	// sl.readyCount, freeing a slot in the scale-up budget.
	p.sl.OnSessionClosing(sh)

	if p.sl.ReadyCount() < p.maxSessions {
		go p.tickOnce(p.poolCtx)
	}
}

// onClose fires at the end of teardown — the stream has actually
// closed. By this point onClosing has already dropped the session
// from sl.readyCount and the picker's idle queue. This callback
// finalizes: drop the AFE handle from sl (refCount → 0, out of
// handleToAfe) and record the close in the reason ledger. Pool.Close's
// Phase-1 flips sh.closeRecorded before Phase-2's s.Close runs, so a
// pool-driven close short-circuits here instead of double-firing
// sl.OnSessionClosed.
func (p *SessionPoolImpl) onClose(sh *SessionHandle, err error) {
	p.mu.Lock()
	if _, starting := p.startingSessions[sh]; starting {
		delete(p.startingSessions, sh)
		p.mu.Unlock()
		// A session that was never promoted to active still counts toward
		// the close ledger — bumpStartingClose is the once-flag path.
		p.bumpStartingClose(sh.session)
		// A start that never reached Active but failed abnormally still
		// contributes to the consecutive-failure signal. createSession
		// already routes the OpenSession error through budget.Release
		// (which applies the creation penalty); this counter is the
		// separate "did any session make progress" signal.
		p.noteAbnormalCloseIfAny(sh.session)
		return
	}
	p.mu.Unlock()

	if !sh.closeRecorded.CompareAndSwap(false, true) {
		// Pool.Close Phase-1 already recorded. bumpAbnormalClose still
		// runs on the s.poolCloseRecorded once-flag path.
		p.noteAbnormalCloseIfAny(sh.session)
		return
	}
	p.sl.OnSessionClosed(sh)
	// recordSessionClose is once-guarded via s.poolCloseRecorded — safe
	// to call even when onClosing already invoked it.
	p.recordSessionClose(sh.session, "")
	p.noteAbnormalCloseIfAny(sh.session)
}

// noteAbnormalCloseIfAny bumps the consecutive-failure counter when
// the session's final close reason classifies as abnormal (same gate
// the debug tracer uses for tagSessionAbnormalClose). When the counter
// crosses consecutiveFailureThreshold, every parked waiter is woken
// with ErrConsecutiveFailures and the counter is reset. Design note:
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

// tickInterval is the cadence for the periodic Tick watchdog. Fixed
// (not configurable) — server-driven scaling changes take effect on
// the next tick, so a shorter cadence means faster reaction to server
// config, and a longer one means less CPU/mu contention. 1 s balances
// the two.
const tickInterval = 1 * time.Second

// Start brings the pool up: an immediate Tick to seed min-sessions
// before the caller's next Checkout, plus the two background loops
// (periodic Tick watchdog + AFE prune). Encapsulates what would
// otherwise be three separate caller lines whose order matters (seed
// must precede the loop so callers don't wait a full interval on
// cold-start). Idempotent per pool — startOnce protects against a
// double-call spawning duplicate background goroutines.
func (p *SessionPoolImpl) Start(ctx context.Context) {
	p.startOnce.Do(func() {
		p.tickOnce(ctx)
		p.startTickLoop(ctx)
		p.startAfePruneLoop(ctx)
	})
}

// startTickLoop runs Tick every tickInterval until ctx cancels. Each
// iteration goes through tickOnce so a panic inside Tick doesn't
// silently kill the watchdog goroutine.
func (p *SessionPoolImpl) startTickLoop(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(tickInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				p.tickOnce(ctx)
			}
		}
	}()
}

// tickOnce runs one Tick with panic recovery. The single entry point
// for every Tick invocation — the periodic loop, the pre-start seed in
// Start, and the three empty-pool kick sites (CheckoutSession's
// one-shot kick, waiter-park kick, onClosing replace-slot kick). A
// panic on any of those paths would otherwise kill a bare goroutine
// or unwind up to the caller; recovering here keeps the pool alive
// and captures the stack for post-mortem.
func (p *SessionPoolImpl) tickOnce(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			btopt.Debugf(nil, "POOL %s Tick panic recovered: %v\n%s", p.poolName, r, debug.Stack())
		}
	}()
	p.Tick(ctx)
}

// startAfePruneLoop runs sl.Prune on afePruneMaxIdle cadence until ctx
// cancels — deliberately OFF the tickInterval so the sl.mu held during
// the map walk can't contend with serving-path Checkouts even under
// pathological AFE-count growth. Wrapped in defer-recover per iteration
// so a bad prune body can't kill the loop.
func (p *SessionPoolImpl) startAfePruneLoop(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(afePruneMaxIdle)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				p.pruneOnce()
			}
		}
	}()
}

// pruneOnce runs one sl.Prune with panic recovery per iteration.
func (p *SessionPoolImpl) pruneOnce() {
	defer func() {
		if r := recover(); r != nil {
			btopt.Debugf(nil, "POOL %s AFE prune panic recovered: %v\n%s", p.poolName, r, debug.Stack())
		}
	}()
	p.sl.Prune(time.Now())
}
