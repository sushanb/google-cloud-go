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
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	spb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
	"google.golang.org/grpc/metadata"
)

// SessionPoolImpl implements a thread-safe session pool.
type SessionPoolImpl struct {
	mu                 sync.Mutex
	sizer              *PoolSizer
	picker             Picker
	budget             SessionThrottler
	sessions           []*SessionHandle
	sessionCreatedAt   map[*SessionHandle]time.Time // Tracks when each SessionHandle was added to p.sessions
	startingSessions   map[*Session]bool
	closed             bool
	scalingInProgress  bool
	minSessions        int
	maxSessions        int
	streamFactory      func(ctx context.Context) (Stream, error)
	openSessionRequest *spb.OpenSessionRequest // Target specific stream handshake template
	metadata           metadata.MD             // Pre-computed call metadata headers
	nextSessionID      uint64                  // Monotonically increasing counter for unique session naming
	sessionType        SessionType
	poolName           string
	// poolID is the SessionManager-assigned unique pool number used as a
	// disambiguator when minting session log names — without it,
	// `session-read-5` would collide across pools because each pool
	// numbers its sessions from 1. 0 means "not assigned" (test setups).
	poolID uint64
	// poolShortName is the resource leaf (e.g. "sushanb" for an
	// OpenTable pool) baked into the session log name so the name
	// identifies WHAT the session opens, not just which pool. Empty
	// when no short name was provided.
	poolShortName string

	// freeSignal is the pool-level "a session became idle" wake-up
	// channel. CheckoutSession parks here when every active session has
	// outstanding > 0; DecOutstanding does a non-blocking send when it
	// brings a session back to outstanding == 0; OnActive does the same
	// when a freshly-handshaked session enters the ready set. Buffer 1
	// so at most one wake-up is in flight; further waiters fall through
	// to the timer fallback and re-scan.
	freeSignal chan struct{}

	// waitersCount is the live count of callers parked inside
	// CheckoutSession waiting for an idle session at the pool boundary.
	// This is the "pending vRPCs" signal the sizer needs. Before this
	// field, Stats() was (mis)reporting sum(outstanding) as
	// PendingCount, which equaled InUseCount with multiPlexingLimit=1
	// and made the sizer oscillate.
	waitersCount atomic.Int32

	// poolCtx is a cancellable context scoped to the lifetime of the pool. It
	// is passed (wrapped to strip deadlines but preserve cancellation) to the
	// underlying streamFactory, budget.Acquire, and Session.Start so that
	// pool teardown propagates into session loops and unblocks any goroutine
	// waiting on the budget semaphore.
	poolCtx    context.Context
	poolCancel context.CancelFunc
}

// SetPoolID stamps the pool with a SessionManager-assigned unique ID.
// Used when minting session log names ("OpenTable3-read-5") so names
// stay unique across pools. Call before any session is created.
func (p *SessionPoolImpl) SetPoolID(id uint64) {
	p.poolID = id
}

// SetPoolShortName stamps the pool with a resource short name (e.g.
// "sushanb") so session log names identify what they're opening
// ("OpenTable3-sushanb-read-5"). Slashes are flattened to underscores
// so the name stays URL-safe.
func (p *SessionPoolImpl) SetPoolShortName(name string) {
	p.poolShortName = strings.ReplaceAll(name, "/", "_")
}

// NewSessionPoolImpl creates a new SessionPoolImpl.
func NewSessionPoolImpl(poolName string, min, max int, streamFactory func(ctx context.Context) (Stream, error), openSessionRequest *spb.OpenSessionRequest, md metadata.MD, sessionType SessionType) *SessionPoolImpl {
	poolCtx, poolCancel := context.WithCancel(context.Background())
	pool := &SessionPoolImpl{
		poolName:           poolName,
		minSessions:        min,
		maxSessions:        max,
		streamFactory:      streamFactory,
		openSessionRequest: openSessionRequest,
		metadata:           md,
		startingSessions:   make(map[*Session]bool),
		sessionCreatedAt:   make(map[*SessionHandle]time.Time),
		sessionType:        sessionType,
		freeSignal:         make(chan struct{}, 1),
		poolCtx:            poolCtx,
		poolCancel:         poolCancel,
	}

	fetcher := func() *PoolStats {
		return pool.Stats()
	}
	pool.sizer = NewPoolSizer(fetcher, min, max, 0.10)
	pool.picker = NewLeastInFlightPicker(pool.sessions)
	pool.budget = NewAdaptiveSessionThrottler(10, 10*time.Second)

	return pool
}

// CheckoutSession returns a session ready to serve one vRPC. With
// multiPlexingLimit=1, "ready" means the session is StateActive AND its
// outstanding count is 0. This is the head-of-line-blocking fix:
// instead of returning a busy session and letting the worker queue on
// vrpcSem inside the session, we queue at the pool boundary so a single
// idle session can serve the next waiter immediately. DecOutstanding
// posts to p.freeSignal when a session returns to idle, OnActive does
// the same for freshly-added sessions, and the ctx-aware select unblocks
// the waiter without polling.
//
// Dead-session cleanup (session left StateActive while sitting in the
// pool) still happens here — we scan, drop the bad handle, and trigger
// a scale-up.
func (p *SessionPoolImpl) CheckoutSession(ctx context.Context) (*SessionHandle, error) {
	// Short-circuit obvious "no sessions yet" — kick off scaling so the
	// first caller doesn't burn the full timer waiting on PerformScaling's
	// 1Hz heartbeat to fire.
	p.mu.Lock()
	if !p.closed && len(p.sessions) == 0 {
		fmt.Printf(">>> POOL %s: no sessions yet, kicking PerformScaling <<<\n", p.poolName)
		go p.PerformScaling(ctx)
	}
	p.mu.Unlock()

	for {
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			return nil, errors.New("session pool is closed")
		}

		// Dead-sweep first so the picker scans only live handles. The
		// inline scan that lived here previously had a "first idle
		// wins" break that combined with cap-1 freeSignal to starve
		// later-activated cohorts (their Picks stayed 0 indefinitely
		// while PendingCount held above 0). Routing selection through
		// p.picker (which now applies a lastActivity tie-break) fixes
		// that.
		var dead []*SessionHandle
		for _, sh := range p.sessions {
			if sh == nil || sh.session == nil {
				continue
			}
			if sh.session.State() != StateActive {
				dead = append(dead, sh)
			}
		}
		// Remove any dead handles we found, then re-init the picker so
		// it stops trying to hand them out for tie-breaking.
		if len(dead) > 0 {
			pruneDead(p, dead)
		}

		if idle := p.picker.PickSession(); idle != nil {
			idle.IncOutstanding()
			p.mu.Unlock()
			return idle, nil
		}

		// No idle session. Trigger scale-up if we're under max and the
		// sizer hasn't already started one — better to ask too often
		// than to leave a worker waiting on a sleeping pool.
		if len(p.sessions) < p.maxSessions {
			go p.PerformScaling(ctx)
		}
		p.mu.Unlock()

		// Park on the wake-up signal. DecOutstanding posts here when a
		// session returns to outstanding == 0 and OnActive posts when a
		// fresh session lands. The short timer is a safety net — if we
		// missed a wake-up (rare; the cap-1 buffer can drop concurrent
		// posts), we'll re-scan after 50ms regardless.
		//
		// Bracket the wait with waitersCount so the sizer (via Stats())
		// sees the real queue depth at the pool boundary.
		p.waitersCount.Add(1)
		select {
		case <-ctx.Done():
			p.waitersCount.Add(-1)
			return nil, fmt.Errorf("no active sessions available: %w", ctx.Err())
		case <-p.freeSignal:
		case <-time.After(50 * time.Millisecond):
		}
		p.waitersCount.Add(-1)
	}
}

// pruneDead removes the given handles from p.sessions (caller holds
// p.mu). Triggers a background PerformScaling so the pool fills the
// gap. Separate helper so CheckoutSession stays readable.
func pruneDead(p *SessionPoolImpl, dead []*SessionHandle) {
	for _, victim := range dead {
		for i, sh := range p.sessions {
			if sh == victim {
				delete(p.sessionCreatedAt, victim)
				p.sessions = append(p.sessions[:i], p.sessions[i+1:]...)
				break
			}
		}
	}
	p.picker = NewLeastInFlightPicker(p.sessions)
	go p.PerformScaling(context.Background())
}

// signalFree posts to p.freeSignal without blocking. The cap-1 buffer
// is an oversimplification on purpose: any time a free session exists
// or appears, at least one waiter wakes up and re-scans. Concurrent
// signals collapse into one, which is fine because the woken waiter
// re-checks everything under the lock anyway.
func (p *SessionPoolImpl) signalFree() {
	select {
	case p.freeSignal <- struct{}{}:
	default:
	}
}

// Stats returns the current operational statistics of the session pool.
func (p *SessionPoolImpl) Stats() *PoolStats {
	p.mu.Lock()
	defer p.mu.Unlock()

	ready := 0
	inUse := 0
	for _, sh := range p.sessions {
		if sh.session.State() == StateActive {
			ready++
		}
		if atomic.LoadInt64(&sh.outstanding) > 0 {
			inUse++
		}
	}
	// PendingCount is the true pool-boundary queue depth (callers
	// parked inside CheckoutSession waiting on freeSignal). The
	// previous implementation summed outstanding across sessions,
	// which with multiPlexingLimit=1 equaled InUseCount and made the
	// sizer oscillate.
	totalOutstanding := int(p.waitersCount.Load())

	return &PoolStats{
		ReadyCount:    ready,
		InUseCount:    inUse,
		StartingCount: len(p.startingSessions),
		PendingCount:  totalOutstanding,
	}
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
	snapshot := p.sessions
	p.sessions = nil
	p.sessionCreatedAt = make(map[*SessionHandle]time.Time)
	p.mu.Unlock()

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
	p.mu.Lock()
	defer p.mu.Unlock()

	delete(p.startingSessions, s)

	if p.closed {
		s.ForceClose(&spb.CloseSessionRequest{
			Reason:      spb.CloseSessionRequest_CLOSE_SESSION_REASON_ERROR,
			Description: "pool closed before session became active",
		})
		return
	}

	// Ensure we do not duplicate register the same session!
	for _, sh := range p.sessions {
		if sh.session == s {
			return
		}
	}

	sh := NewSessionHandle(s)
	// Wire DecOutstanding's "I'm now idle" notifier to the pool's wake-up
	// channel so workers parked in CheckoutSession on a full pool can be
	// unblocked the moment any session returns to outstanding == 0.
	sh.SetFreeSignal(p.freeSignal)
	p.sessions = append(p.sessions, sh)
	p.sessionCreatedAt[sh] = time.Now()

	// Re-initialize picker with updated sessions list
	p.picker = NewLeastInFlightPicker(p.sessions)

	// New session is immediately idle (outstanding == 0). Post a wake-up
	// so any worker parked on "no idle session" can grab it without
	// waiting out the 50ms safety timer.
	p.signalFree()
}

// OnClose removes the closed session from the active sessions list and updates the picker.
func (p *SessionPoolImpl) OnClose(s *Session, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if _, starting := p.startingSessions[s]; starting {
		delete(p.startingSessions, s)
		return
	}

	idx := -1
	for i, sh := range p.sessions {
		if sh.session == s {
			idx = i
			break
		}
	}

	if idx != -1 {
		// Remove session handle from slice
		removed := p.sessions[idx]
		delete(p.sessionCreatedAt, removed)
		p.sessions = append(p.sessions[:idx], p.sessions[idx+1:]...)
		// Re-initialize picker with updated active sessions
		p.picker = NewLeastInFlightPicker(p.sessions)
		// Trigger scale up evaluation asynchronously immediately!
		go p.PerformScaling(context.Background())
	}
}

// UpdateConfig dynamically adjusts the pool size constraints and budget governor limits at runtime.
func (p *SessionPoolImpl) UpdateConfig(config *spb.SessionClientConfiguration_SessionPoolConfiguration) {
	p.mu.Lock()
	p.minSessions = int(config.MinSessionCount)
	p.maxSessions = int(config.MaxSessionCount)
	fmt.Printf(">>> SessionPool %p UpdateConfig: minSessions=%d, maxSessions=%d <<<\n", p, p.minSessions, p.maxSessions)

	if config.LoadBalancingOptions != nil {
		lbo := config.LoadBalancingOptions
		switch opt := lbo.LoadBalancingStrategy.(type) {
		case *spb.LoadBalancingOptions_Random_:
			p.picker = NewRandomPicker(p.sessions)
		case *spb.LoadBalancingOptions_LeastInFlight_:
			p.picker = NewLeastInFlightPicker(p.sessions)
		case *spb.LoadBalancingOptions_PeakEwma_:
			subsetSize := 2
			if opt.PeakEwma != nil {
				subsetSize = int(opt.PeakEwma.RandomSubsetSize)
			}
			p.picker = NewPeakEwmaPicker(p.sessions, subsetSize)
		}
	}
	p.mu.Unlock()

	// Dynamically update sizer thresholds E2E!
	p.sizer.UpdateConfig(config)
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

func (p *SessionPoolImpl) PerformScaling(ctx context.Context) {
	// Sweep for sessions stuck in WaitServerClose past the grace window —
	// happens when the server accepted CloseSession but never followed up
	// with a stream EOF. ForceClose drives them to Closed so OnClose fires
	// and the pool retires them.
	p.sweepStuckSessions()

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

	stats := p.Stats()
	fmt.Printf(">>> POOL %s STATS: Ready=%d, Starting=%d, InUse=%d, PendingOutstanding=%d <<<\n",
		p.poolName, stats.ReadyCount, stats.StartingCount, stats.InUseCount, stats.PendingCount)

	delta := p.sizer.GetScaleDelta()
	if delta == 0 {
		return
	}

	p.mu.Lock()
	currentSessions := len(p.sessions)
	startingSessions := len(p.startingSessions)
	p.mu.Unlock()

	fmt.Printf(">>> POOL %s PerformScaling starting evaluation: delta=%d, current_sessions=%d, starting_sessions=%d <<<\n", p.poolName, delta, currentSessions, startingSessions)

	if delta > 0 {
		// Scale up: provision new sessions asynchronously and wait for completion to release the gate
		var wg sync.WaitGroup
		for i := 0; i < delta; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := p.createSession(ctx); err != nil {
					fmt.Printf(">>> POOL %s PerformScaling createSession failed: %v <<<\n", p.poolName, err)
				} else {
					fmt.Printf(">>> POOL %s PerformScaling successfully provisioned a new session <<<\n", p.poolName)
				}
			}()
		}
		wg.Wait()
	} else {
		// Scale down: prune idle sessions gracefully
		fmt.Printf(">>> POOL %s PerformScaling pruning %d idle sessions <<<\n", p.poolName, -delta)
		p.pruneSessions(-delta)
	}
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
	stream, err := p.streamFactory(dialCtxOut)
	if err != nil {
		return err
	}

	// Determine session name and check limits briefly under lock
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return fmt.Errorf("session pool is closed")
	}
	if len(p.sessions) >= p.maxSessions {
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
	// Session log names must be globally unique within the client and
	// self-describing. Format:
	//
	//   {ProtoName}{poolID}-{shortName}-{role}-{uniqueHex}
	//
	// e.g. OpenTable1-sushanb-read-a3f2e891.
	//
	// The trailing segment is a random 32-bit hex id rather than a
	// monotonic counter so a long-lived pool that churns sessions for
	// years can't overflow. Collision odds with N live sessions in the
	// 2^32 space are ≈ N²/2^33; under 1 in 8k at N = 1000.
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
	// nextSessionID is still bumped so any caller relying on the
	// monotonic count for stats stays correct; it's just not in the name.
	atomic.AddUint64(&p.nextSessionID, 1)
	p.mu.Unlock()

	// Create and start new session wrapper passing pool pointer as the lifecycle listener!
	s := NewSession(sessionName, stream, SessionHooks{
		OnStart:  p.OnStart,
		OnActive: p.OnActive,
		OnClose:  p.OnClose,
	}, p.sessionType)

	p.mu.Lock()
	p.startingSessions[s] = true
	p.mu.Unlock()

	if err := s.Start(dialCtx, p.openSessionRequest); err != nil {
		p.mu.Lock()
		delete(p.startingSessions, s)
		p.mu.Unlock()
		fmt.Printf(">>> POOL %p createSession Start failed for %s: %v <<<\n", p, sessionName, err)
		return fmt.Errorf("failed to start session: %w", err)
	}

	success = true
	return nil
}

func (p *SessionPoolImpl) pruneSessions(count int) {
	// Phase 1: under lock, select prune candidates and remove them from
	// p.sessions immediately so concurrent CheckoutSession callers don't
	// pick them. Skip sessions younger than 5s so we don't churn through
	// newly-minted sessions before they have a chance to absorb load.
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}

	now := time.Now()
	const minSessionAge = 5 * time.Second

	pruned := 0
	var toClose []*Session
	var active []*SessionHandle
	for _, sh := range p.sessions {
		createdAt, ok := p.sessionCreatedAt[sh]
		tooYoung := !ok || now.Sub(createdAt) < minSessionAge
		if pruned < count && atomic.LoadInt64(&sh.outstanding) == 0 && !tooYoung {
			if sh.session != nil {
				toClose = append(toClose, sh.session)
			}
			delete(p.sessionCreatedAt, sh)
			pruned++
		} else {
			active = append(active, sh)
		}
	}

	p.sessions = active
	p.picker = NewLeastInFlightPicker(p.sessions)
	p.mu.Unlock()

	if len(toClose) == 0 {
		return
	}

	// Phase 2: spawn graceful Close for every pruned session with a bounded
	// 5s timeout, then wait so this call doesn't return until the closes
	// finish (or the bounded ctx fires).
	closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	for _, s := range toClose {
		wg.Add(1)
		go func(s *Session) {
			defer wg.Done()
			s.Close(closeCtx, &spb.CloseSessionRequest{
				Reason:      spb.CloseSessionRequest_CLOSE_SESSION_REASON_DOWNSIZE,
				Description: "prune session downsize",
			})
		}(s)
	}
	wg.Wait()
}

// waitServerCloseGrace bounds how long a session may sit in
// StateWaitServerClose before the pool force-closes it. The server should
// EOF the stream promptly after acknowledging CloseSession; if it doesn't,
// this gives us a deterministic teardown so OnClose fires and the pool
// retires the session instead of leaking it.
const waitServerCloseGrace = 30 * time.Second

// sweepStuckSessions scans the pool for sessions parked in
// StateWaitServerClose beyond waitServerCloseGrace and force-closes them.
// Runs from PerformScaling at the heartbeat cadence; takes p.mu only long
// enough to snapshot the victim list, then issues ForceClose calls outside
// the lock.
func (p *SessionPoolImpl) sweepStuckSessions() {
	type victim struct {
		sess     *Session
		stuckFor time.Duration
	}
	var victims []victim

	p.mu.Lock()
	for _, sh := range p.sessions {
		if sh == nil || sh.session == nil {
			continue
		}
		sh.session.mu.Lock()
		stuck := sh.session.state == StateWaitServerClose
		since := time.Since(sh.session.lastStateChange)
		sh.session.mu.Unlock()
		if stuck && since > waitServerCloseGrace {
			victims = append(victims, victim{sess: sh.session, stuckFor: since})
		}
	}
	p.mu.Unlock()

	for _, v := range victims {
		fmt.Printf(">>> POOL %s sweepStuckSessions: force-closing %s stuck in WaitServerClose for %v <<<\n",
			p.poolName, v.sess.LogName(), v.stuckFor.Round(time.Second))
		v.sess.ForceClose(&spb.CloseSessionRequest{
			Reason:      spb.CloseSessionRequest_CLOSE_SESSION_REASON_ERROR,
			Description: "stuck in WaitServerClose past grace",
		})
	}
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

// Invoke checks out a session from the pool, executes a single virtual RPC
// on it, and returns the full InvokeResult (response, cluster info,
// server-reported Stats, local SentAt timestamp). Outstanding-count
// bookkeeping is managed automatically. RetryInfo from server errors is
// plumbed through the returned error via gRPC status details so the retry
// interceptor can honor it.
func (p *SessionPoolImpl) Invoke(ctx context.Context, desc VRpcDescriptor, req interface{}) (InvokeResult, error) {
	sh, err := p.CheckoutSession(ctx)
	if err != nil {
		return InvokeResult{}, err
	}
	start := time.Now()
	defer func() {
		sh.DecOutstanding(time.Since(start))
	}()

	return sh.session.Invoke(ctx, desc, req)
}
