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

// Scaling for SessionPoolImpl: the PerformScaling driver, the
// createSession worker, the scaling-history ring buffer that records
// their decisions, the human-readable reason helper, and the
// deadline-stripping context wrapper used for long-lived dials.
//
// Scale-up is the only actioned branch: the sizer's negative deltas are
// advisory, and the pool shrinks passively via OnClose's replace-on-
// death gate (see java-bigtable's PoolSizer for the same asymmetric
// design).

package internal

import (
	"context"
	"fmt"
	"log"
	"math/rand/v2"
	"strings"
	"sync"
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

// PerformScaling is the heartbeat/checkout-triggered driver that samples
// operational state, prunes stuck sessions, and — if the sizer asks for
// growth — launches new-session goroutines. Scale-down deltas are logged
// but not actioned (passive shrink via OnClose).
func (p *SessionPoolImpl) PerformScaling(ctx context.Context) {
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
	// AFE prune runs on its own timer (see StartHeartbeat) at
	// afePruneMaxIdle cadence for java-parity — kept OFF the 1-sec
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

	currentSessions := p.readyCount()

	// Only scale-up is actioned. Scale-down deltas are advisory — the
	// pool shrinks passively via OnClose's replace-on-death gate (see
	// java-bigtable's PoolSizer for the same asymmetric design). This
	// removes the periodic prune that produced the burst-then-lull
	// oscillation on wave-shaped workloads.
	if delta <= 0 {
		return
	}

	reason := scalingReason(stats, delta)
	var launched atomic.Int64
	defer func() {
		actual := int(launched.Load())
		p.recordScaling(ScalingEvent{
			At:        time.Now(),
			Before:    currentSessions,
			Requested: delta,
			Launched:  actual,
			Reason:    reason,
			Decision:  decision,
		})
	}()

	// Scale up: provision new sessions asynchronously and wait for
	// completion to release the gate.
	var wg sync.WaitGroup
	for i := 0; i < delta; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := p.createSession(ctx); err != nil {
				// A scale-up attempt failed — the pool wanted `delta`
				// more sessions and got fewer. Every failure is a tag
				// so the count IS the "scale-ups we lost" gauge.
				recordDebugTag(tagSessionPoolCreateFailed)
				btopt.Debugf(nil, "POOL %s PerformScaling createSession failed: %v", p.poolName, err)
			} else {
				launched.Add(1)
			}
		}()
	}
	wg.Wait()
}

func (p *SessionPoolImpl) createSession(ctx context.Context) error {
	// Use the pool-scoped ctx (not the per-request ctx) for the long-lived
	// dial, budget acquisition, and Session.Start. The wrapper strips any
	// deadline (a Bidi stream must not inherit a user-set timeout) but
	// preserves cancellation so pool teardown propagates through.
	dialCtx := noDeadlineButCancellableContext{Context: p.poolCtx}

	// Acquire a token from the concurrency governor budget before dialing!
	if err := p.budget.Acquire(dialCtx); err != nil {
		return fmt.Errorf("failed to acquire session creation budget: %w", err)
	}

	success := false
	defer func() {
		p.budget.Release(success) // Release budget registering success/failure penalty token!
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
	if p.readyCount() >= p.maxSessions {
		p.mu.Unlock()
		return fmt.Errorf("session pool limit reached")
	}
	// Derive a short role hint for the session log name from whatever
	// permission marker the pool's name carries (the SessionManager adds
	// "[READ]" / "[WRITE]" suffixes). Falls back to a generic "s".
	role := "s"
	switch {
	case strings.Contains(p.poolName, "[READ]"):
		role = "read"
	case strings.Contains(p.poolName, "[WRITE]"):
		role = "write"
	}
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

	// Create and start new session wrapper passing pool pointer as the lifecycle hooks target.
	s := NewSession(sessionName, stream, SessionHooks{
		OnStart:   p.OnStart,
		OnActive:  p.OnActive,
		OnClosing: p.OnClosing,
		OnClose:   p.OnClose,
	}, p.sessionType, WithSessionPoolName(p.poolName), WithSessionLogger(log.Default()))
	if hint := int(pickedChannel.Load()); hint >= 0 {
		s.SetChannelIndex(hint)
	}

	p.mu.Lock()
	p.startingSessions[s] = true
	p.mu.Unlock()

	if err := s.Start(dialCtx, p.openSessionRequest); err != nil {
		p.mu.Lock()
		delete(p.startingSessions, s)
		p.mu.Unlock()
		btopt.Debugf(nil, "POOL %p createSession Start failed for %s: %v", p, sessionName, err)
		return fmt.Errorf("failed to start session: %w", err)
	}

	success = true
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
