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
	"strings"
	"sync"
	"sync/atomic"
	"time"

	spb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
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

	// Lifecycle counters surfaced via PoolSnapshot. All bumped lock-free.
	sessionsOpened atomic.Int64
	sessionsClosed atomic.Int64
	listenerFires  atomic.Int64
	closesByReason sync.Map // close-reason label → *atomic.Int64

	// scalingHistory is a ring buffer of the last N scaling decisions made
	// by PerformScaling. Guarded by scalingHistoryMu so the snapshot reader
	// gets a consistent slice copy without dropping events.
	scalingHistoryMu sync.Mutex
	scalingHistory   []ScalingEvent

	// slowVRpcs is a ring buffer of the last N vRPCs that exceeded the
	// slow-call threshold. Lets operators answer "which call was slow?"
	// without trawling logs.
	slowVRpcsMu       sync.Mutex
	slowVRpcs         []SlowVRpcEvent
	slowVRpcThreshold time.Duration

	// timeSeriesMu / timeSeries is a 1-Hz ring buffer of pool-level
	// observations driven by PerformScaling. Powers inline SVG sparklines
	// in the sessionz UI. Capped at maxTimeSeries.
	timeSeriesMu      sync.Mutex
	timeSeries        []TimeSeriesSample
	tsLastOkRpcs      int64
	tsLastErrorRpcs   int64

	// poolCtx is a cancellable context scoped to the lifetime of the pool. It
	// is passed (wrapped to strip deadlines but preserve cancellation) to the
	// underlying streamFactory, budget.Acquire, and Session.Start so that
	// pool teardown propagates into session loops and unblocks any goroutine
	// waiting on the budget semaphore.
	poolCtx    context.Context
	poolCancel context.CancelFunc
}

// ScalingEvent is one row in a pool's scaling history.
type ScalingEvent struct {
	At        time.Time
	FromCount int
	ToCount   int
	Delta     int
	Reason    string
}

// SlowVRpcEvent is one row in the pool's slow-vRPC log.
type SlowVRpcEvent struct {
	At       time.Time
	Method   string
	Latency  time.Duration
	Session  string // log name of the session that handled the call
	Success  bool
	ErrCode  string // grpc status code on failure, empty on success
}

const (
	maxSlowVRpcs         = 100
	defaultSlowThreshold = 1 * time.Second
	// maxTimeSeries caps the per-pool sparkline ring. At the 1-Hz heartbeat
	// sampling rate this covers the most recent 5 minutes — long enough to
	// span a typical scaling event without ballooning memory.
	maxTimeSeries = 300
)

// TimeSeriesSample is one point in a pool's sparkline ring buffer.
// All counters are deltas since the previous sample so the chart shows
// rate-per-second rather than running totals.
type TimeSeriesSample struct {
	At         time.Time
	Sessions   int
	OkPerSec   float64
	ErrPerSec  float64
	InUse      int
	Pending    int
}

func (p *SessionPoolImpl) recordTimeSeries() {
	p.mu.Lock()
	totalSessions := len(p.sessions)
	inUse, pending := 0, 0
	var okTotal, errTotal int64
	for _, sh := range p.sessions {
		if sh == nil || sh.session == nil {
			continue
		}
		ob := atomic.LoadInt64(&sh.outstanding)
		if ob > 0 {
			inUse++
			pending += int(ob)
		}
		okTotal += sh.session.okRpcs.Load()
		errTotal += sh.session.errorRpcs.Load()
	}
	p.mu.Unlock()

	now := time.Now()
	p.timeSeriesMu.Lock()
	defer p.timeSeriesMu.Unlock()

	var okRate, errRate float64
	if len(p.timeSeries) > 0 {
		prev := p.timeSeries[len(p.timeSeries)-1]
		dt := now.Sub(prev.At).Seconds()
		if dt > 0 {
			okRate = float64(okTotal-p.tsLastOkRpcs) / dt
			errRate = float64(errTotal-p.tsLastErrorRpcs) / dt
		}
	}
	p.tsLastOkRpcs = okTotal
	p.tsLastErrorRpcs = errTotal

	sample := TimeSeriesSample{
		At:        now,
		Sessions:  totalSessions,
		OkPerSec:  okRate,
		ErrPerSec: errRate,
		InUse:     inUse,
		Pending:   pending,
	}
	if len(p.timeSeries) >= maxTimeSeries {
		copy(p.timeSeries, p.timeSeries[1:])
		p.timeSeries = p.timeSeries[:len(p.timeSeries)-1]
	}
	p.timeSeries = append(p.timeSeries, sample)
}

func (p *SessionPoolImpl) snapshotTimeSeries() []TimeSeriesSample {
	p.timeSeriesMu.Lock()
	defer p.timeSeriesMu.Unlock()
	out := make([]TimeSeriesSample, len(p.timeSeries))
	copy(out, p.timeSeries)
	return out
}

// recordSlowVRpc appends to the slow-vRPC ring buffer. Only called from
// SessionPoolImpl.Invoke after a call exceeds the threshold.
func (p *SessionPoolImpl) recordSlowVRpc(ev SlowVRpcEvent) {
	p.slowVRpcsMu.Lock()
	defer p.slowVRpcsMu.Unlock()
	if len(p.slowVRpcs) >= maxSlowVRpcs {
		copy(p.slowVRpcs, p.slowVRpcs[1:])
		p.slowVRpcs = p.slowVRpcs[:len(p.slowVRpcs)-1]
	}
	p.slowVRpcs = append(p.slowVRpcs, ev)
}

func (p *SessionPoolImpl) snapshotSlowVRpcs() []SlowVRpcEvent {
	p.slowVRpcsMu.Lock()
	defer p.slowVRpcsMu.Unlock()
	out := make([]SlowVRpcEvent, len(p.slowVRpcs))
	copy(out, p.slowVRpcs)
	return out
}

func (p *SessionPoolImpl) slowThreshold() time.Duration {
	if p.slowVRpcThreshold > 0 {
		return p.slowVRpcThreshold
	}
	return defaultSlowThreshold
}

// maxScalingHistory caps the per-pool ring buffer length. Picked so that at
// the default 1-second heartbeat interval the buffer covers the last ~16
// minutes of activity — long enough to see a full provisioning episode but
// short enough to stay tiny.
const maxScalingHistory = 1024

// recordScaling appends an event to the ring buffer, dropping the oldest
// entry when full.
func (p *SessionPoolImpl) recordScaling(ev ScalingEvent) {
	p.scalingHistoryMu.Lock()
	defer p.scalingHistoryMu.Unlock()
	if len(p.scalingHistory) >= maxScalingHistory {
		copy(p.scalingHistory, p.scalingHistory[1:])
		p.scalingHistory = p.scalingHistory[:len(p.scalingHistory)-1]
	}
	p.scalingHistory = append(p.scalingHistory, ev)
}

// snapshotScalingHistory returns a copy of the ring buffer, oldest first.
func (p *SessionPoolImpl) snapshotScalingHistory() []ScalingEvent {
	p.scalingHistoryMu.Lock()
	defer p.scalingHistoryMu.Unlock()
	out := make([]ScalingEvent, len(p.scalingHistory))
	copy(out, p.scalingHistory)
	return out
}

// bumpCloseReason atomically increments the close-reason counter; the map
// is keyed by label so the set of reasons can grow without struct churn.
func (p *SessionPoolImpl) bumpCloseReason(label string) {
	if label == "" {
		label = "Unspecified"
	}
	c, _ := p.closesByReason.LoadOrStore(label, new(atomic.Int64))
	c.(*atomic.Int64).Add(1)
}

// recordSessionClose marks a session as retired exactly once and bumps
// sessionsClosed + the close-reason histogram. Called from every removal
// site (OnClose, CheckoutSession's dead-detect, pruneSessions, Pool.Close)
// so the counter reflects pool-side retirements promptly even when the
// underlying session's hooks.OnClose hasn't fired yet (e.g. the server
// hasn't EOFed the stream). The once-flag lives on the Session so it
// dedupes across paths.
//
// fallbackReason is used only when the session itself hasn't recorded a
// reason yet — for example, pruneSessions hasn't sent CloseSession yet,
// or CheckoutSession found a session in StateClosed via a race.
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
	p.sessionsClosed.Add(1)
	p.bumpCloseReason(reason)
}

// bumpStartingClose is the recordSessionClose variant for sessions that
// died before reaching active state — they're held in startingSessions, not
// p.sessions, so OnClose's idx-not-found branch is the only signal we get.
// Wraps the same once-flag for consistency.
func (p *SessionPoolImpl) bumpStartingClose(s *Session) {
	p.recordSessionClose(s, "FailedToStart")
}

// snapshotCloseReasons returns the per-reason counts as a flat map.
func (p *SessionPoolImpl) snapshotCloseReasons() map[string]int64 {
	out := map[string]int64{}
	p.closesByReason.Range(func(k, v interface{}) bool {
		out[k.(string)] = v.(*atomic.Int64).Load()
		return true
	})
	return out
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
		poolCtx:            poolCtx,
		poolCancel:         poolCancel,
	}

	fetcher := func() *PoolStats {
		return pool.Stats()
	}
	pool.sizer = NewPoolSizer(fetcher, min, max, 0.10)
	pool.picker = NewRandomPicker(pool.sessions)
	pool.budget = NewAdaptiveSessionThrottler(10, 10*time.Second)

	return pool
}

// CheckoutSession retrieves a session from the pool for routing requests.
func (p *SessionPoolImpl) CheckoutSession(ctx context.Context) (*SessionHandle, error) {
	// Triggers scaling immediately if we might be short of sessions
	p.mu.Lock()
	if !p.closed && len(p.sessions) == 0 {
		fmt.Printf(">>> POOL %s: all sessions busy, trying to create new session <<<\n", p.poolName)
		go p.PerformScaling(ctx)
	}
	p.mu.Unlock()

	ticker := time.NewTicker(15 * time.Millisecond)
	defer ticker.Stop()

	for {
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			return nil, errors.New("session pool is closed")
		}

		sh := p.picker.PickSession()
		if sh != nil {
			if sh.session.State() == StateActive {
				sh.IncOutstanding()
				atomic.AddInt64(&sh.picks, 1)
				p.mu.Unlock()
				return sh, nil
			}
			// Session is not active anymore. Remove it immediately from pool sessions
			idx := -1
			for i, sHandle := range p.sessions {
				if sHandle == sh {
					idx = i
					break
				}
			}
			if idx != -1 {
				p.recordSessionClose(sh.session, "DeadOnPick")
				delete(p.sessionCreatedAt, p.sessions[idx])
				p.sessions = append(p.sessions[:idx], p.sessions[idx+1:]...)
				p.picker = NewRandomPicker(p.sessions)
			}
			// Trigger scale up immediately to replace the dead session
			go p.PerformScaling(ctx)
		}

		p.mu.Unlock()

		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("no active sessions available: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

// Stats returns the current operational statistics of the session pool.
func (p *SessionPoolImpl) Stats() *PoolStats {
	p.mu.Lock()
	defer p.mu.Unlock()

	ready := 0
	inUse := 0
	totalOutstanding := 0
	for _, sh := range p.sessions {
		if sh.session.State() == StateActive {
			ready++
		}
		outstanding := atomic.LoadInt64(&sh.outstanding)
		if outstanding > 0 {
			inUse++
			totalOutstanding += int(outstanding)
		}
	}

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

	// Record the closes up-front (with PoolClose as the fallback reason)
	// so the debug counters reflect retirement immediately, even though the
	// actual graceful Close on each session is still in flight.
	for _, sh := range snapshot {
		if sh != nil && sh.session != nil {
			p.recordSessionClose(sh.session, "PoolClose")
		}
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
	p.sessions = append(p.sessions, sh)
	p.sessionCreatedAt[sh] = time.Now()
	p.sessionsOpened.Add(1)

	// Re-initialize picker with updated sessions list
	p.picker = NewRandomPicker(p.sessions)
}

// OnClose removes the closed session from the active sessions list and updates the picker.
func (p *SessionPoolImpl) OnClose(s *Session, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if _, starting := p.startingSessions[s]; starting {
		delete(p.startingSessions, s)
		// A session that was never promoted to active still counts toward
		// the close ledger — use a synthetic handle for the once-flag so
		// duplicate OnClose calls don't double-count.
		p.bumpStartingClose(s)
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
		removed := p.sessions[idx]
		p.recordSessionClose(s, "")
		delete(p.sessionCreatedAt, removed)
		p.sessions = append(p.sessions[:idx], p.sessions[idx+1:]...)
		// Re-initialize picker with updated active sessions
		p.picker = NewRandomPicker(p.sessions)
		// Trigger scale up evaluation asynchronously immediately!
		go p.PerformScaling(context.Background())
		return
	}
	// idx == -1: handle was already removed by a proactive path
	// (CheckoutSession dead-detect, pruneSessions, Pool.Close). That path
	// already recorded the close; this is a no-op thanks to the once-flag,
	// but we still call it so a path that ever forgets to record doesn't
	// silently leak counts.
	p.recordSessionClose(s, "")
}

// UpdateConfig dynamically adjusts the pool size constraints and budget governor limits at runtime.
func (p *SessionPoolImpl) UpdateConfig(config *spb.SessionClientConfiguration_SessionPoolConfiguration) {
	p.listenerFires.Add(1)
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
			p.picker = NewRoundRobinPicker(p.sessions)
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
	// Sample a time-series point on every heartbeat so the sparkline ring
	// buffer fills at the heartbeat cadence regardless of whether scaling
	// actually fires below.
	p.recordTimeSeries()

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

	reason := scalingReason(stats, delta)
	defer func() {
		p.mu.Lock()
		newCount := len(p.sessions)
		p.mu.Unlock()
		p.recordScaling(ScalingEvent{
			At:        time.Now(),
			FromCount: currentSessions,
			ToCount:   newCount,
			Delta:     delta,
			Reason:    reason,
		})
	}()

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
	if len(p.sessions) >= p.maxSessions {
		p.mu.Unlock()
		return fmt.Errorf("session pool limit reached")
	}
	id := atomic.AddUint64(&p.nextSessionID, 1)
	role := "session"
	if strings.HasSuffix(p.poolName, ":read") {
		role = "read"
	} else if strings.HasSuffix(p.poolName, ":write") {
		role = "write"
	}
	sessionName := fmt.Sprintf("session-%s-%d", role, id)
	p.mu.Unlock()

	// Create and start new session wrapper passing pool pointer as the lifecycle listener!
	s := NewSession(sessionName, stream, SessionHooks{
		OnStart:  p.OnStart,
		OnActive: p.OnActive,
		OnClose:  p.OnClose,
	}, p.sessionType)
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
				p.recordSessionClose(sh.session, "Downsize")
				toClose = append(toClose, sh.session)
			}
			delete(p.sessionCreatedAt, sh)
			pruned++
		} else {
			active = append(active, sh)
		}
	}

	p.sessions = active
	p.picker = NewRandomPicker(p.sessions)
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

	result, invokeErr := sh.session.Invoke(ctx, desc, req)
	latency := time.Since(start)
	if latency > p.slowThreshold() {
		ev := SlowVRpcEvent{
			At:      start,
			Method:  desc.Method(),
			Latency: latency,
			Session: sh.session.LogName(),
			Success: invokeErr == nil,
		}
		if invokeErr != nil {
			ev.ErrCode = status.Code(invokeErr).String()
		}
		p.recordSlowVRpc(ev)
	}
	return result, invokeErr
}
