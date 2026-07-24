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

// Scaling for SessionPoolImpl: the Tick driver, the
// createSession worker, the scaling-history ring buffer that records
// their decisions, the human-readable reason helper, and the
// deadline-stripping context wrapper used for long-lived dials.
//
// Scale-up is the only actioned branch: the sizer's negative deltas are
// advisory, and the pool shrinks passively via OnClose's replace-on-
// death gate (the same asymmetric
// design).

package internal

import (
	"context"
	"fmt"
	"log"
	"math/rand/v2"
	"sync/atomic"
	"time"

	btopt "cloud.google.com/go/bigtable/internal/option"
	"google.golang.org/grpc/metadata"
)

// ScalingEvent is one row in a pool's scaling history.
//
// Before is the pool's session count when the sizer decided to act.
// Requested is the delta the sizer asked for (>0 scale up, <0 scale down).
// Launched is the per-action outcome: createSession calls that returned nil
// for scale-up (sessions handshaking but not yet active), or sessions
// actually pruned for scale-down. Always sign-matches Requested.
//
// For scale-up the actual pool growth lags this event — handshakes complete
// asynchronously via OnActive. Use the pool's live size for "what's
// available right now"; use Launched + Requested to understand "what the
// sizer tried to do."
type ScalingEvent struct {
	At        time.Time
	Before    int
	Requested int
	Launched  int
	Reason    string

	// Decision is the full sizer trace that produced Requested — every
	// input, every intermediate, plus the branch taken
	// ("scale-up" | "scale-down" | "suppressed" | "dead-band").
	// Lets the operator answer "why did the sizer pick THIS?" without
	// re-running the math. Suppressed events (Requested == 0,
	// Decision.WouldDelta != 0) are also recorded so cooldown activity
	// is visible.
	Decision ScaleDecision
}

// maxScalingHistory caps the per-pool ring buffer length. Picked so that at
// the default 1-second heartbeat interval the buffer covers the last ~16
// minutes of activity — long enough to see a full provisioning episode but
// short enough to stay tiny.
const maxScalingHistory = 1024

// recordScaling appends an event to the ring buffer, dropping the oldest
// entry when full.
func (p *SessionPoolImpl) recordScaling(ev ScalingEvent) {
	p.m.scalingHistoryMu.Lock()
	defer p.m.scalingHistoryMu.Unlock()
	if len(p.m.scalingHistory) >= maxScalingHistory {
		copy(p.m.scalingHistory, p.m.scalingHistory[1:])
		p.m.scalingHistory = p.m.scalingHistory[:len(p.m.scalingHistory)-1]
	}
	p.m.scalingHistory = append(p.m.scalingHistory, ev)
}

// snapshotScalingHistory returns a copy of the ring buffer, oldest first.
func (p *SessionPoolImpl) snapshotScalingHistory() []ScalingEvent {
	p.m.scalingHistoryMu.Lock()
	defer p.m.scalingHistoryMu.Unlock()
	out := make([]ScalingEvent, len(p.m.scalingHistory))
	copy(out, p.m.scalingHistory)
	return out
}

// Tick is the heartbeat/checkout-triggered driver that samples
// operational state, prunes stuck sessions, and — if the sizer asks for
// growth — launches new-session goroutines. Scale-down deltas are logged
// but not actioned (passive shrink via OnClose).
func (p *SessionPoolImpl) Tick(ctx context.Context) {
	// Sample a time-series point on every heartbeat so the sparkline ring
	// buffer fills at the heartbeat cadence regardless of whether scaling
	// actually fires below.
	p.recordTimeSeries()
	// Sample each active session's current age into session.uptime so the
	// histogram reflects the distribution of live session ages, not just
	// per-close lifetimes (that's session.durations).
	p.sampleActiveUptimes(ctx)
	// Sweep for sessions stuck in WaitServerClose past the grace window —
	// happens when a server sent GoAway / accepted CloseSession but never
	// followed up with a stream EOF. ForceClose drives them to Closed so
	// OnClose fires and the pool retires them.
	p.sweepStuckSessions()
	// AFE prune runs on its own timer (see startAfePruneLoop) at
	// afePruneMaxIdle cadence — kept OFF the 1-sec
	// heartbeat so the sl.mu it holds during map-walk can't contend with
	// serving-path Checkouts even under pathological AFE-count growth.

	p.mu.Lock()
	if p.closed || p.scalingInProgress {
		p.mu.Unlock()
		return
	}
	p.scalingInProgress = true
	p.mu.Unlock()

	defer func() {
		p.mu.Lock()
		p.scalingInProgress = false
		p.mu.Unlock()
	}()

	decision := p.sizer.Decide()
	stats := &PoolStats{
		ReadyCount:    decision.ReadyCount,
		StartingCount: decision.StartingCount,
		InUseCount:    decision.InUseCount,
		PendingCount:  decision.PendingCount,
	}
	delta := decision.Delta

	currentSessions := p.sl.ReadyCount()

	// Only scale-up is actioned. Scale-down deltas are advisory — the
	// pool shrinks passively via OnClose's replace-on-death gate (see
	// same asymmetric PoolSizer design). This
	// removes the periodic prune that produced the burst-then-lull
	// oscillation on wave-shaped workloads.
	if delta <= 0 {
		return
	}

	reason := scalingReason(stats, delta)
	// Record the scaling decision immediately — Requested = delta.
	// The per-session outcomes (dial + handshake success) can't be
	// waited on here without blocking every Tick caller (including
	// Start's pre-start seed) on the slowest session's handshake.
	// Instead: each session that becomes Ready fires OnActive →
	// p.signalFree(), which wakes any parked CheckoutSession waiter.
	// Failures bump tagSessionPoolCreateFailed for observability.
	p.recordScaling(ScalingEvent{
		At:        time.Now(),
		Before:    currentSessions,
		Requested: delta,
		Reason:    reason,
		Decision:  decision,
	})

	// Reserve pendingStarts + register spawns BEFORE launching the
	// fire-and-forget goroutines. Held under p.mu with a re-check of
	// p.closed so Close's Phase 1 (which sets p.closed under the same
	// lock) synchronizes-with all subsequent p.spawns.Wait: if Close has
	// already fired, we skip both the reservation and the spawn — no
	// new Add can happen concurrent with Close's Wait, avoiding the
	// classic WaitGroup Add/Wait race.
	//
	// pendingStarts guards the sizer double-count (see field doc);
	// spawns guards teardown so no createSession goroutine outlives
	// Close, including error paths that touch package-level metric
	// state (recordDebugTag).
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.pendingStarts += delta
	p.spawns.Add(delta)
	p.mu.Unlock()

	// Scale up: fire and forget. Sessions signal readiness via
	// OnActive → signalFree; no wait-group needed for readiness.
	for i := 0; i < delta; i++ {
		go func() {
			defer p.spawns.Done()
			if err := p.createSession(ctx); err != nil {
				// A scale-up attempt failed — the pool wanted `delta`
				// more sessions and got fewer. Every failure is a tag
				// so the count IS the "scale-ups we lost" gauge.
				recordDebugTag(tagSessionPoolCreateFailed)
				btopt.Debugf(nil, "POOL %s Tick createSession failed: %v", p.poolName, err)
			}
		}()
	}
}

func (p *SessionPoolImpl) createSession(ctx context.Context) error {
	// Use the pool-scoped ctx (not the per-request ctx) for the long-lived
	// dial, budget acquisition, and Session.Start. The wrapper strips any
	// deadline (a Bidi stream must not inherit a user-set timeout) but
	// preserves cancellation so pool teardown propagates through.
	dialCtx := noDeadlineButCancellableContext{Context: p.poolCtx}

	// This goroutine owns one pendingStarts reservation minted by Tick.
	// On any failure before the transfer point below it releases the
	// reservation; on success the transfer point consumes it atomically
	// with the startingSessions insert (so a concurrent Decide() sees
	// exactly one "in-flight" credit — never zero, never two).
	reserved := true
	defer func() {
		if reserved {
			p.mu.Lock()
			p.pendingStarts--
			p.mu.Unlock()
		}
	}()

	// Acquire a token from the concurrency governor budget before dialing!
	if err := p.budget.Acquire(dialCtx); err != nil {
		return fmt.Errorf("failed to acquire session creation budget: %w", err)
	}

	success := false
	budgetReleased := false
	defer func() {
		// Only release here if the success path hasn't already done so —
		// success path releases early so budget isn't held for the
		// Session's lifetime.
		if !budgetReleased {
			p.budget.Release(success)
		}
	}()

	// Inject the pre-computed target metadata headers context-safely E2E!
	dialCtxOut := metadata.NewOutgoingContext(dialCtx, p.metadata)
	// Pre-allocate a channel-pick hint so the underlying gRPC channel pool
	// can publish which connEntry it placed this session's stream on.
	// Defaults to -1 (no hint received).
	var pickedChannel atomic.Int32
	pickedChannel.Store(-1)
	stream, err := p.streamFactory(ChannelPickHintInto(dialCtxOut, &pickedChannel))
	if err != nil {
		return err
	}

	// Determine session name and check limits briefly under lock
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return fmt.Errorf("session pool is closed")
	}
	if p.sl.ReadyCount() >= p.maxSessions {
		p.mu.Unlock()
		return fmt.Errorf("session pool limit reached")
	}
	// Read the pool's permission axis directly — set once at
	// construction via SetPoolIdentity. "session" is the fallback for
	// PermissionUnknown (test setups that skip SetPoolIdentity).
	role := p.perm.role()
	// Session log names must be globally unique within the client so the
	// channelz → sessionz reverse link is unambiguous, and self-describing
	// so the name itself tells you what the session opens. Format:
	//
	//   {ProtoName}{poolID}-{shortName}-{role}-{uniqueHex}
	//
	// e.g. OpenTable1-sushanb-read-a3f2e891, OpenTable3-users-write-7c0d54a2.
	//
	// The trailing segment is a random 32-bit hex id rather than a
	// monotonic counter, so a pool that churns sessions for years can't
	// overflow a uint64. Collision odds with N live sessions in the
	// 2^32 space are ≈ N² / 2^33; well under 1 in 8k at N = 1000.
	//
	// Falls back to ProtoName-role-hex when no SessionManager IDs are
	// assigned (test setups).
	hexID := fmt.Sprintf("%08x", rand.Uint32())
	var sessionName string
	switch {
	case p.poolID > 0 && p.poolShortName != "":
		sessionName = fmt.Sprintf("%s%d-%s-%s-%s", p.sessionType.ProtoName(), p.poolID, p.poolShortName, role, hexID)
	case p.poolID > 0:
		sessionName = fmt.Sprintf("%s%d-%s-%s", p.sessionType.ProtoName(), p.poolID, role, hexID)
	default:
		sessionName = fmt.Sprintf("%s-%s-%s", p.sessionType.ProtoName(), role, hexID)
	}
	// nextSessionID is still bumped so any caller that relies on the
	// monotonic count for stats stays correct; the value just isn't part
	// of the name any more.
	atomic.AddUint64(&p.nextSessionID, 1)
	p.mu.Unlock()

	// Mint the SessionHandle BEFORE NewSession so the per-session hook
	// closures can capture it directly — no Session→SessionHandle
	// back-ref needed. Design:
	// per-session listeners at construction time with the handle in
	// scope. sh.session and sh.createdAt are backfilled two statements
	// down; the closures don't fire until Session.Start runs.
	sh := &SessionHandle{}
	hooks := SessionHooks{
		OnStart:  p.onStart,
		OnActive: func(_ *Session) { p.onActive(sh) },
		// Fires from every successful drainSlot; captures sh so it
		// doesn't need to walk back through Session.
		OnSlotDrained: func() {
			p.sl.ReleaseToPool(sh)
			p.signalFree()
		},
		OnClosing: func(_ *Session) { p.onClosing(sh) },
		OnClose:   func(_ *Session, err error) { p.onClose(sh, err) },
	}
	s := NewSession(sessionName, stream, hooks, p.sessionType,
		WithSessionPoolName(p.poolName), WithSessionLogger(log.Default()))
	if hint := pickedChannel.Load(); hint >= 0 {
		s.setChannelIndex(hint)
	}

	// Backfill the handle now that the Session exists.
	sh.session = s
	sh.createdAt = time.Now()

	// Transfer the pendingStarts reservation into startingSessions
	// under a single lock so a concurrent Decide() never sees a
	// gap (both fields drop to 0) or a double (both count us).
	p.mu.Lock()
	p.pendingStarts--
	p.startingSessions[sh] = struct{}{}
	p.mu.Unlock()
	reserved = false

	if err := s.Start(dialCtx, p.openSessionRequest); err != nil {
		p.mu.Lock()
		delete(p.startingSessions, sh)
		p.mu.Unlock()
		btopt.Debugf(nil, "POOL %p createSession Start failed for %s: %v", p, sessionName, err)
		return fmt.Errorf("failed to start session: %w", err)
	}

	success = true

	// Free the budget slot NOW so the next scale-up isn't blocked by the
	// session's lifetime — normally the deferred Release would fire
	// only after we return, which is after WaitGoroutines below (i.e.
	// after the session dies).
	p.budget.Release(true)
	budgetReleased = true

	// Block until Session's readLoop + heartBeatLoop have fully unwound
	// (through their notifyClosed → recordClose / recordDebugTag
	// callback chains). Keeps this createSession goroutine — tracked by
	// p.spawns — alive for the entire session lifetime so pool.Close's
	// Phase 5 (p.spawns.Wait) waits for every session goroutine to exit
	// before returning. Without this, a session that was removed from
	// sl by its own onClose (fired synchronously from readLoop) is
	// missed by any sl-based snapshot, and its readLoop leaks past
	// Close to race the next test's InitializeSessionMetrics.
	s.WaitGoroutines()
	return nil
}

// scalingReason summarizes why the sizer requested a scale delta given the
// pool's current stats. Pure helper — no side effects — so the snapshot
// reader gets the same text the operator would derive from the log.
func scalingReason(stats *PoolStats, delta int) string {
	if delta > 0 {
		switch {
		case stats == nil:
			return "scale up (no stats)"
		case stats.PendingCount > 0:
			return fmt.Sprintf("pending=%d", stats.PendingCount)
		case stats.InUseCount > 0 && stats.ReadyCount-stats.InUseCount <= 0:
			return fmt.Sprintf("ready=%d in_use=%d (headroom exhausted)", stats.ReadyCount, stats.InUseCount)
		default:
			return fmt.Sprintf("ready=%d in_use=%d (load>headroom)", stats.ReadyCount, stats.InUseCount)
		}
	}
	if stats == nil {
		return "scale down (no stats)"
	}
	return fmt.Sprintf("scale down: ready=%d in_use=%d", stats.ReadyCount, stats.InUseCount)
}

// noDeadlineButCancellableContext wraps a parent context to strip any
// deadline (so a long-lived Bidi stream does not inherit a per-request
// timeout) while preserving cancellation, error propagation, and value
// lookups from the parent. Built on top of the pool-scoped poolCtx so that
// pool teardown via poolCancel() unblocks anything dialing, waiting on the
// session-creation budget, or running in Session.Start's loops.
type noDeadlineButCancellableContext struct {
	context.Context
}

func (noDeadlineButCancellableContext) Deadline() (deadline time.Time, ok bool) {
	return time.Time{}, false
}
