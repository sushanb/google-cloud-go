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

// SessionPoolImpl core: the struct definition, constructor, the
// CheckoutSession / Invoke hot path, Stats, UpdateConfig, and the
// tiny name-plumbing setters. All observability ring buffers live in
// session_pool_debug.go; session-hooks callbacks + Close + heartbeat
// scheduler live in session_pool_lifecycle.go; scaling driver +
// createSession + scaling-history ring live in session_pool_scaling.go.

package internal

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	spb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
	btopt "cloud.google.com/go/bigtable/internal/option"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// ErrNoSessionsAvailable is returned by CheckoutSession when a caller
// parked in the waiter queue is unblocked by ctx cancellation or
// deadline before a session becomes free. The returned error also
// wraps ctx.Err(), so errors.Is(err, context.DeadlineExceeded) and
// errors.Is(err, ErrNoSessionsAvailable) both hold — callers can
// distinguish "pool exhaustion timed us out" from "user's ctx fired
// mid-RPC" via the sentinel while retry code continues to key on the
// ctx cause. The pool emits
// Status.DEADLINE_EXCEEDED with description "Deadline exceeded
// waiting for session".
var ErrNoSessionsAvailable = errors.New("bigtable: no sessions available")

// ErrConsecutiveFailures is returned by CheckoutSession when the pool
// has tripped its consecutive-session-failure circuit breaker (fired
// when consecutive abnormal session closes reach
// ConsecutiveSessionFailureThreshold).
// All parked waiters at trip time are woken with this sentinel so
// callers surface the failure to the user instead of continuing to
// block on a pool whose backend is repeatedly rejecting OpenSession.
var ErrConsecutiveFailures = errors.New("bigtable: session pool tripped consecutive-failure threshold")

// waiter is one parked CheckoutSession caller. ready is closed by
// signalFree when this waiter is selected to wake, or by removeWaiter
// when ctx cancellation pulls the waiter out of the queue.
// close-exactly-once is guarded by the waitersMu / w.elem invariant:
// the waiter is only in the queue while w.elem != nil, and both wake
// paths hold waitersMu when they nil w.elem out.
type waiter struct {
	ready chan struct{}
	elem  *list.Element // non-nil while enqueued; nil after dequeue
	// err is set by the wake path (under waitersMu, before close(ready))
	// when the waiter should fail with a specific error instead of
	// looping back to re-pick. Today only the consecutive-failure trip
	// path uses this (sets ErrConsecutiveFailures on every waiter it
	// drains). A normal signalFree leaves err nil so CheckoutSession
	// continues its retry loop.
	err error
}

// SessionPoolImpl implements a thread-safe session pool.
type SessionPoolImpl struct {
	mu     sync.Mutex
	sizer  *PoolSizer
	picker AfePicker
	// sl is the AFE-aware bucketing structure. It owns the idle-session
	// queues per AFE and the per-AFE PeakEwma trackers the picker
	// consumes. sl is the sole store of active SessionHandles now — no
	// flat mirror. sl has its own lock (finer than p.mu).
	//
	// Lock ordering: sl methods never call back into SessionPoolImpl
	// (no pool reference held), so the "never take p.mu while holding
	// sl.mu" rule is preserved by construction — not by a comment.
	// Production call sites reach directly through p.sl.X (Checkout,
	// ReadyAfes, ReadyCount, AllHandles, OnSessionStarted/Closing/
	// Closed, ReleaseToPool, Prune, Snapshot). The one sl-adjacent
	// method that earns a pool-level home is noteVRpcOutcome — it
	// forwards to sl.RecordVRpcOutcome AND resets the pool's
	// consecutive-failure counter on OK.
	sl     *sessionList
	budget SessionThrottler
	// startingSessions holds handles dialed via createSession that have
	// not yet reached onActive. Cleared in onActive (promotion) or in
	// onClose (failed start → bumpStartingClose). Registered active
	// sessions live in sl (sessionList) — the pool no longer carries a
	// separate flat slice. Keyed by *SessionHandle rather than *Session
	// because the hook closures capture sh; the pool never needs a
	// *Session-only lookup path.
	startingSessions map[*SessionHandle]struct{}
	// pendingStarts counts fire-and-forget goroutines spawned by Tick
	// that haven't yet reached streamFactory success (so they aren't in
	// startingSessions yet). Without this, back-to-back Ticks fired
	// within the streamFactory window each see StartingCount=0 and
	// re-request the same delta — a pending=2 waiter would drive 5×5=25
	// spawns instead of 5. Bumped in Tick before spawning; consumed by
	// createSession's transfer into startingSessions on success, or by
	// the goroutine's defer on any failure before transfer.
	pendingStarts int
	// spawns tracks every goroutine the pool fires and forgets — today
	// only Tick's createSession workers. Close waits on it so no
	// pool-spawned goroutine outlives Close. Session-owned goroutines
	// (readLoop, heartBeatLoop) are tracked separately on Session via
	// Session.loops.
	spawns sync.WaitGroup
	closed            bool
	scalingInProgress bool
	minSessions        int
	maxSessions        int
	streamFactory      func(ctx context.Context) (Stream, error)
	openSessionRequest *spb.OpenSessionRequest // Target specific stream handshake template
	metadata           metadata.MD             // Pre-computed call metadata headers
	nextSessionID      uint64                  // Monotonically increasing counter for unique session naming
	sessionType        SessionType
	poolName           string
	// perm is the read/write axis, required at ctor via PoolIdentity.
	// Consumed by session log name minting; typed rather than parsed
	// back out of poolName's "[READ]"/"[WRITE]" suffix.
	perm Permission
	// startOnce guards Start so a second call is a no-op, not a
	// duplicate spawn of the tick + prune goroutines.
	startOnce sync.Once
	// tickPending debounces tickOnce so a burst of empty-pool kicks
	// from CheckoutSession (or a periodic-loop tick landing while a
	// kicked tick is still running) coalesces to at most one active
	// Tick body at a time. CAS false→true at entry; Store false at
	// exit via defer.
	tickPending atomic.Bool
	// poolID is the SessionManager-assigned unique pool number baked
	// into every session log name so the channelz → sessionz reverse
	// link stays unambiguous across pools.
	poolID uint64

	// waitersCount is the live count of callers parked inside
	// CheckoutSession waiting for an idle session at the pool boundary.
	// This is the "pending vRPCs" signal the sizer needs — same as
	// it via a dedicated Sized input. Before this field, Stats() was
	// (mis)reporting sum(outstanding) as PendingCount, which equaled
	// InUseCount and made the sizer oscillate.
	waitersCount atomic.Int32

	// consecutiveFailures counts session closes classified as abnormal
	// by isAbnormalCloseReason since the last successful vRPC. Reset to
	// 0 on any ok vRPC outcome (hot path — hence atomic, not p.mu).
	// When it crosses consecutiveFailureThreshold the pool trips: every
	// parked waiter is woken with ErrConsecutiveFailures and the
	// counter is reset so the next round of traffic gets a fresh
	// window. Analogous to SessionPoolImpl.consecutiveFailures /
	// getMaxConsecutiveFailures / popClosableRpcs.
	consecutiveFailures        atomic.Int32
	consecutiveFailureThreshold atomic.Int32

	// m owns every observability-only field — counters, ring buffers,
	// histograms. Extracted into a sub-struct so this definition shows
	// the pool's operational shape (sessions, sizer, hooks) without
	// wading through 100+ lines of bookkeeping. See poolMetrics in
	// session_pool_debug.go for the breakdown; every accessor spells
	// out its intent via p.m.<field>.
	m poolMetrics

	// waiters is a FIFO queue of CheckoutSession callers parked because
	// no idle session was available at pick time. Design
	// (see PendingVRpc/ArrayDeque in SessionPoolImpl.java) — each free-
	// session event wakes exactly one waiter, in insertion order. Fair
	// under contention (no random-select unfairness like a shared chan
	// gives). Every wake is delivered (no cap-1 collapse), so the old
	// 50ms polling hedge is gone.
	//
	// Cancellation removes the waiter from the queue on the way out, so
	// a cancelled caller doesn't hold a wake-up token that could
	// otherwise get dropped.
	waitersMu sync.Mutex
	waiters   *list.List // *waiter

	// poolCtx is a cancellable context scoped to the lifetime of the pool. It
	// is passed (wrapped to strip deadlines but preserve cancellation) to the
	// underlying streamFactory, budget.Acquire, and Session.Start so that
	// pool teardown propagates into session loops and unblocks any goroutine
	// waiting on the budget semaphore.
	poolCtx    context.Context
	poolCancel context.CancelFunc
}

// Permission is the pool's read/write axis, plumbed as typed data so the
// session-name minter doesn't have to parse it back out of the poolName
// display string. Required at construction — the ctor panics on
// PermissionUnknown to catch mis-wired callers early.
type Permission uint8

const (
	// PermissionUnknown is the zero-value; the ctor rejects it so a
	// forgotten identity field can't silently produce misleading log
	// names in prod.
	PermissionUnknown Permission = iota
	PermissionRead
	PermissionWrite
)

// role returns the short label embedded in session log names. Panics
// on PermissionUnknown — the ctor guarantees perm is Read or Write.
func (p Permission) role() string {
	switch p {
	case PermissionRead:
		return "read"
	case PermissionWrite:
		return "write"
	default:
		panic(fmt.Sprintf("session pool: Permission.role() called on %d — SessionPoolImpl was constructed with PermissionUnknown", p))
	}
}

// PoolIdentity bundles the identifying fields passed to
// NewSessionPoolImpl.
type PoolIdentity struct {
	// ID is the SessionManager-assigned unique pool number. Baked into
	// the session log name for the channelz → sessionz reverse link.
	ID uint64
	// Perm is the pool's read/write axis.
	Perm Permission
}

// noteVRpcOutcome forwards the vRPC outcome to the AFE's PeakEwma
// trackers (OK-gated) AND, on OK, resets the pool's
// consecutive-failure counter — SessionPoolImpl.java:488 does the
// equivalent on any successful session-close path; we key on vRPC
// success instead because that's the strongest "backend is healthy"
// signal we have. This is the one sl-adjacent method that earns a
// pool-level home; every other sl operation is a direct p.sl.X call.
func (p *SessionPoolImpl) noteVRpcOutcome(sh *SessionHandle, e2e, backend time.Duration, ok bool) {
	p.sl.RecordVRpcOutcome(sh, e2e, backend, ok)
	if ok {
		p.consecutiveFailures.Store(0)
	}
}

// NewSessionPoolImpl creates a new SessionPoolImpl. Panics if
// identity.Perm is PermissionUnknown — every pool must be either
// read or write, and a forgotten identity field silently producing
// "…-session-…" log names in prod is worse than a construction-time
// panic.
func NewSessionPoolImpl(identity PoolIdentity, poolName string, min, max int, streamFactory func(ctx context.Context) (Stream, error), openSessionRequest *spb.OpenSessionRequest, md metadata.MD, sessionType SessionType) *SessionPoolImpl {
	poolCtx, poolCancel := context.WithCancel(context.Background())
	pool := &SessionPoolImpl{
		poolName:           poolName,
		poolID:             identity.ID,
		perm:               identity.Perm,
		minSessions:        min,
		maxSessions:        max,
		streamFactory:      streamFactory,
		openSessionRequest: openSessionRequest,
		metadata:           md,
		startingSessions:   make(map[*SessionHandle]struct{}),
		sessionType:        sessionType,
		waiters:            list.New(),
		poolCtx:            poolCtx,
		poolCancel:         poolCancel,
		sl:                 newSessionList(),
	}
	pool.m.afePickCounts = make(map[afeID]int64)

	fetcher := func() *PoolStats {
		return pool.Stats()
	}
	pool.sizer = NewPoolSizer(fetcher, min, max, 0.10)
	// Bootstrap picker with the "no config yet" fallback (matches the
	// LeastInFlight, K=defaultAfeRandomSubsetSize). Every real caller
	// wires the pool through ClientConfigurationManager.AddSessionPoolListener,
	// which fires UpdateConfig synchronously on registration — so the
	// hardcoded default only ever runs in test setups that skip that
	// wiring. Single source of truth for the LBO → picker mapping lives
	// in pickerFromLoadBalancing.
	pool.picker = pickerFromLoadBalancing(nil)
	// Bootstrap budget with the "no config yet" fallback. Same
	// UpdateConfig-on-registration path as the picker replaces this
	// with the server-driven ceiling (default 50) and penalty
	// (default 60s) before the pool serves real traffic.
	pool.budget = NewAdaptiveSessionThrottler(10, 10*time.Second)
	// Bootstrap consecutive-failure threshold. Default is 10.
	// UpdateConfig overwrites this with the server-driven value on
	// the same registration path.
	pool.consecutiveFailureThreshold.Store(10)

	return pool
}

// CheckoutSession returns a session ready to serve one vRPC. With
// multiPlexingLimit=1 the pool only hands out a session whose
// outstanding count is 0. If every session is busy, the caller parks
// on p.freeSignal until DecOutstanding wakes them. Queueing lives at
// the pool boundary (first freed session goes to the first waiter),
// which superseded an earlier per-session semaphore scheme where random
// picks stacked on busy sessions even when idle ones existed.
func (p *SessionPoolImpl) CheckoutSession(ctx context.Context) (*SessionHandle, error) {
	// One-shot kick if the pool is empty. Cheap check; Tick
	// gates on its own in-progress flag so a duplicate goroutine here
	// exits immediately.
	p.mu.Lock()
	closed := p.closed
	p.mu.Unlock()
	if !closed && p.sl.ReadyCount() == 0 {
		p.spawnTickOnce(p.poolCtx)
	}

	for {
		// Snapshot pool state under the lock. We take p.mu only long
		// enough to (a) check closed and (b) grab a stable picker
		// reference. Everything expensive — the picker call, the
		// sessionList checkout, the ring-buffer record — happens
		// OUTSIDE the lock so concurrent CheckoutSession callers can
		// run in parallel. UpdateConfig writes p.picker under p.mu so
		// this snapshot is a consistent picker instance for the whole
		// pick attempt.
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			return nil, errors.New("session pool is closed")
		}
		picker := p.picker
		p.mu.Unlock()

		// Fast path: two-tier pick — picker chooses an AFE from the
		// ready snapshot, then sessionList dequeues one idle session
		// from that AFE. sessionList uses its own lock; ordering rule
		// (never take p.mu while holding sl.mu) holds trivially since
		// we've released p.mu.
		ready := p.sl.ReadyAfes()
		pickerName := picker.Name()
		afeID, picked, decision := picker.PickAfe(ready)
		p.recordPickDecision(decision, pickerName)
		if picked {
			if idle := p.sl.Checkout(afeID); idle != nil {
				idle.IncOutstanding()
				atomic.AddInt64(&idle.picks, 1)
				return idle, nil
			}
			// Picker said this AFE had a ready session but by the time
			// we tried to check one out it was gone — a lost race
			// against another CheckoutSession or an OnClosing eviction.
			// Legitimate under concurrency; the counter tells us how
			// often it's actually hurting throughput.
			recordDebugTag(tagSessionPoolPickLostRace)
		}

		// Slow path: picker returned nil. No dead-sweep
		// needed. Dying sessions leave sl.readyCount the instant they
		// transition out of Ready via OnClosing (fired from Session's
		// notifyClosing at handleGoAway / Close / ForceClose /
		// handleClose). So the maxSessions gate here already reflects
		// only live-or-starting sessions; a miss just means all live
		// sessions are busy.
		if p.sl.ReadyCount() < p.maxSessions {
			p.spawnTickOnce(p.poolCtx)
		}

		// Park in the FIFO waiter queue. Each free-session event
		// wakes exactly one waiter (queue head). No polling timer —
		// the queue can't miss a wake-up. Bracket with waitersCount
		// so the sizer (via Stats()) sees real queue depth.
		w := &waiter{ready: make(chan struct{})}
		p.waitersMu.Lock()
		w.elem = p.waiters.PushBack(w)
		p.waitersMu.Unlock()

		p.waitersCount.Add(1)
		select {
		case <-ctx.Done():
			p.waitersCount.Add(-1)
			// Remove from queue so a subsequent free-session wake
			// doesn't burn on a caller that's already given up.
			p.removeWaiter(w)
			return nil, fmt.Errorf("%w: %w", ErrNoSessionsAvailable, ctx.Err())
		case <-w.ready:
			p.waitersCount.Add(-1)
			// A poisoned wake (drainWaitersWithErr) sets w.err before
			// closing w.ready. Normal signalFree leaves it nil so the
			// caller loops back to re-pick.
			if w.err != nil {
				return nil, w.err
			}
			// Woken by signalFree. Loop back to re-pick.
		}
	}
}

// removeWaiter pulls w out of the waiter queue if still present. Safe
// to call from the ctx-cancel path even when signalFree has already
// removed the waiter (checks w.elem — nil means already dequeued by
// signalFree, which will have closed w.ready).
func (p *SessionPoolImpl) removeWaiter(w *waiter) {
	p.waitersMu.Lock()
	if w.elem != nil {
		p.waiters.Remove(w.elem)
		w.elem = nil
	}
	p.waitersMu.Unlock()
}

// signalFree wakes exactly one parked CheckoutSession waiter — the
// FIFO queue head. No-op when the queue is empty. Called from
// OnActive (new session became ready) and from the drain-driven
// SessionHandle.onSlotDrained callback (installed at OnActive; fires
// from every drainSlot success in Session, per SESSION_SPEC.md #2).
// Never blocks: the wake channel is dedicated per-waiter, so there's
// no cap-1 collapse to worry about.
func (p *SessionPoolImpl) signalFree() {
	p.waitersMu.Lock()
	if e := p.waiters.Front(); e != nil {
		w := e.Value.(*waiter)
		p.waiters.Remove(e)
		w.elem = nil
		close(w.ready)
	}
	p.waitersMu.Unlock()
}

// drainWaitersWithErr wakes every parked CheckoutSession caller with
// the given error. Same as SessionPoolImpl.popClosableRpcs, which
// drains the pool-level pendingRpcs deque and fails each with a
// consecutive-failures status. Returns the number of waiters woken so
// the caller can log / meter the blast radius. Safe to call with the
// queue empty (returns 0).
func (p *SessionPoolImpl) drainWaitersWithErr(err error) int {
	p.waitersMu.Lock()
	defer p.waitersMu.Unlock()
	n := 0
	for {
		e := p.waiters.Front()
		if e == nil {
			return n
		}
		w := e.Value.(*waiter)
		p.waiters.Remove(e)
		w.elem = nil
		w.err = err
		close(w.ready)
		n++
	}
}

// Stats returns the current operational statistics of the session pool.
func (p *SessionPoolImpl) Stats() *PoolStats {
	p.mu.Lock()
	defer p.mu.Unlock()

	ready := 0
	inUse := 0
	for _, sh := range p.sl.AllHandles() {
		if sh.session.State() == StateReady {
			ready++
		}
		if atomic.LoadInt64(&sh.outstanding) > 0 {
			inUse++
		}
	}

	return &PoolStats{
		ReadyCount: ready,
		InUseCount: inUse,
		// StartingCount = sessions past streamFactory (in startingSessions)
		// PLUS goroutines Tick has spawned but that haven't yet reached
		// the transfer point. Both count as "in-flight scale-up capacity"
		// so the sizer doesn't request duplicate delta in a burst.
		StartingCount: len(p.startingSessions) + p.pendingStarts,
		// PendingCount is the true pool-boundary queue depth —
		// callers parked inside CheckoutSession waiting on
		// freeSignal. The pending-rpc count is the input
		// to the sizer.
		PendingCount: int(p.waitersCount.Load()),
	}
}

// UpdateConfig dynamically adjusts the pool size constraints and budget governor limits at runtime.
func (p *SessionPoolImpl) UpdateConfig(config *spb.SessionClientConfiguration_SessionPoolConfiguration) {
	p.m.listenerFires.Add(1)
	p.mu.Lock()
	p.minSessions = int(config.MinSessionCount)
	p.maxSessions = int(config.MaxSessionCount)

	if config.LoadBalancingOptions != nil {
		p.picker = pickerFromLoadBalancing(config.LoadBalancingOptions)
	}
	p.mu.Unlock()

	// Dynamically update sizer thresholds E2E!
	p.sizer.UpdateConfig(config)

	// Budget: the throttler was bootstrapped with a placeholder in
	// NewSessionPoolImpl. Every real caller registers with
	// ClientConfigurationManager which fires UpdateConfig synchronously,
	// so this is where the server-driven ceiling + penalty actually
	// take effect. Similar to SessionCreationBudget.updateConfig
	// (SessionCreationBudget.java:129).
	if budget := int(config.GetNewSessionCreationBudget()); budget > 0 {
		penalty := config.GetNewSessionCreationPenalty().AsDuration()
		p.budget.UpdateConfig(budget, penalty)
	}

	// Consecutive-failure threshold:
	// SessionPoolImpl.getMaxConsecutiveFailures. Server-driven cap on
	// how many back-to-back abnormal session closes the pool tolerates
	// before failing all parked waiters. Zero/negative means "no
	// change" — the bootstrap default (10) stays in force.
	if thr := config.GetConsecutiveSessionFailureThreshold(); thr > 0 {
		p.consecutiveFailureThreshold.Store(thr)
	}
}

// pickerFromLoadBalancing builds an AfePicker from server-driven
// LoadBalancingOptions. A nil lbo returns the default
// (LeastInFlight with K=defaultAfeRandomSubsetSize) so bootstrap
// paths (NewSessionPoolImpl before UpdateConfig fires, unit tests
// that skip the config wiring) get a working picker.
//
// Single source of truth for the LBO → picker mapping — the previous
// implementation duplicated this switch between NewSessionPoolImpl
// and UpdateConfig, and drifted: the LeastInFlight branch of
// UpdateConfig ignored its own RandomSubsetSize field even though the
// PeakEwma branch honored it. That inconsistency is fixed here — every
// picker that takes a K-choice size reads the corresponding
// RandomSubsetSize from the oneof, with the shared default fallback
// when the server omits it (value ≤ 0).
func pickerFromLoadBalancing(lbo *spb.LoadBalancingOptions) AfePicker {
	if lbo == nil {
		return NewLeastInFlightAfePicker(defaultAfeRandomSubsetSize)
	}
	switch opt := lbo.LoadBalancingStrategy.(type) {
	case *spb.LoadBalancingOptions_Random_:
		// SimplePicker: uniform random over ready AFEs, then
		// dequeue any idle session in that AFE. No K knob.
		return NewSimpleAfePicker()
	case *spb.LoadBalancingOptions_LeastInFlight_:
		k := defaultAfeRandomSubsetSize
		if opt.LeastInFlight != nil && opt.LeastInFlight.RandomSubsetSize > 0 {
			k = int(opt.LeastInFlight.RandomSubsetSize)
		}
		return NewLeastInFlightAfePicker(k)
	case *spb.LoadBalancingOptions_PeakEwma_:
		// PeakEwma maps to the LeastLatencyPicker.
		k := defaultAfeRandomSubsetSize
		if opt.PeakEwma != nil && opt.PeakEwma.RandomSubsetSize > 0 {
			k = int(opt.PeakEwma.RandomSubsetSize)
		}
		return NewLeastLatencyAfePicker(k)
	default:
		return NewLeastInFlightAfePicker(defaultAfeRandomSubsetSize)
	}
}

// Invoke checks out a session from the pool, executes a single virtual RPC
// on it, and returns the full InvokeResult (response, cluster info,
// server-reported Stats, local SentAt timestamp). Outstanding-count
// bookkeeping is managed automatically. RetryInfo from server errors is
// plumbed through the returned error via gRPC status details so the retry
// interceptor can honor it.
func (p *SessionPoolImpl) Invoke(ctx context.Context, desc VRpcDescriptor, req interface{}) (InvokeResult, error) {
	checkoutStart := time.Now()
	sh, err := p.CheckoutSession(ctx)
	if err != nil {
		// Pool-exhaustion incidents are the exact case an operator
		// opens sessionz to debug. Without recording here, calls that
		// parked in CheckoutSession until ctx.Done fired never reach
		// the success-path recorder below — the slow-vRPC table and
		// the pool-wide latency histogram both silently drop them.
		p.recordCheckoutFailure(checkoutStart, desc, err)
		return InvokeResult{}, err
	}
	poolWait := time.Since(checkoutStart)
	// Use checkoutStart as the wall-clock anchor so the recorded
	// Latency includes the pool queue wait — that's the user-visible
	// time. Without it the pool-queue fix would silently hide the
	// wait from the slow-vRPC log.
	start := checkoutStart
	// Track invokeErr / backendDur / latency across the defer so the
	// per-AFE PeakEwma update sees the actual outcome. Under the
	// slot lifecycle (v3), drainSlot success is the sole "session free"
	// signal — the session's response handler fires the OnSlotDrained
	// hook (installed at createSession) which does the AFE-queue
	// re-enqueue AND the Checkout waiter wake. So this defer no longer
	// touches sl.ReleaseToPool or signalFree; it stays only for the
	// per-caller in-flight counter and the outcome-known EWMA update.
	// One consequence: the wake fires from the response handler BEFORE
	// this defer runs, so a picker on another goroutine may see the
	// pre-update EWMAs for this AFE for one tick. Accepted — EWMAs lag
	// by definition, and the same one-tick window ships in v2 for the
	// ctx.Done'd-then-drained path.
	var (
		invokeErr  error
		backendDur time.Duration
		latency    time.Duration
	)
	defer func() {
		sh.DecOutstanding()
		p.noteVRpcOutcome(sh, latency, backendDur, invokeErr == nil)
	}()

	var result InvokeResult
	result, invokeErr = sh.session.Invoke(ctx, desc, req)
	// Bind the serving session's PeerInfo to the result so callers can
	// stamp per-attempt transport labels (attempt_latencies2) without
	// reaching back through the pool. The PeerInfo pointer is set once at
	// session-open and never mutated, so this is a shared read.
	result.PeerInfo = sh.session.PeerInfo()
	latency = time.Since(start)
	// Feed the pool-level histograms so the debug UI's TotalLatency and
	// BackendLatency "N=…" grow over the pool's lifetime instead of
	// being capped at (active sessions × 256) by per-session ring
	// buffers. BackendLatency only records when the server populated
	// Stats — client-observed TotalLatency records for every call.
	p.m.totalLatencyHist.record(latency)
	if result.Stats != nil && result.Stats.BackendLatency != nil {
		backendDur = result.Stats.BackendLatency.AsDuration()
		p.m.backendLatencyHist.record(backendDur)
	}
	// ClientTransportLatency: (stream Send→Recv) − backend =
	// wire + AFE + client-decode overhead outside server processing.
	// Skip samples missing either half (pre-Recv error, no server Stats)
	// or with a non-positive delta (clock skew, backend > stream) so
	// the p50 isn't dragged toward 0. Compute once, share with the
	// pool histogram, the slow-event row, and the exported OTel metric.
	var transportOverhead time.Duration
	if result.TransportLatency > 0 && backendDur > 0 {
		if d := result.TransportLatency - backendDur; d > 0 {
			transportOverhead = d
			p.m.transportLatencyHist.record(d)
			sh.session.RecordTransportOverhead(ctx, desc.Method(), d)
		}
	}
	if latency > p.slowThreshold() {
		ev := SlowVRpcEvent{
			At:               start,
			Method:           desc.Method(),
			Latency:          latency,
			Session:          sh.session.LogName(),
			Success:          invokeErr == nil,
			PoolWait:         poolWait,
			BackendLatency:   backendDur,
			TransportLatency: transportOverhead,
			RpcIDOnSession:   result.RpcIDOnSession,
		}
		ev.SessionAge = start.Sub(sh.session.StartedAt())
		// Capture the session's PeerInfo so cohort patterns (e.g. every
		// Unavailable failure on AFE X) are visible directly in the
		// slow-vRPC table instead of requiring a per-session cross-ref.
		ev.Peer = peerInfoToSnapshot(sh.session.peerInfo.Load())
		ev.RemoteAddr = sh.session.RemoteAddr()
		if invokeErr != nil {
			// Standard library context errors don't implement GRPCStatus(),
			// so status.Code falls through to Unknown — which mislabels
			// deadline-fire rows in the slow-vRPC table. Classify them
			// explicitly so the operator sees the real reason.
			switch {
			case errors.Is(invokeErr, context.DeadlineExceeded):
				ev.ErrCode = "DeadlineExceeded"
			case errors.Is(invokeErr, context.Canceled):
				ev.ErrCode = "Canceled"
			default:
				ev.ErrCode = status.Code(invokeErr).String()
			}
			btopt.Debugf(nil, "POOL %s slow vRPC failed method=%s session=%s rpc_id=%d code=%s latency=%v session_age=%v backend=%v raw_err=%v",
				p.poolName, ev.Method, ev.Session, ev.RpcIDOnSession, ev.ErrCode, ev.Latency, ev.SessionAge, ev.BackendLatency, invokeErr)
		}
		p.recordSlowVRpc(ev)
	}
	return result, invokeErr
}
