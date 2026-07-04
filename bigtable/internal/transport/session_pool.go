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
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	spb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
	btopt "cloud.google.com/go/bigtable/internal/option"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// SessionPoolImpl implements a thread-safe session pool.
type SessionPoolImpl struct {
	mu     sync.Mutex
	sizer  *PoolSizer
	picker AfePicker
	// sl is the AFE-aware bucketing structure. It owns the idle-session
	// queues per AFE and the per-AFE PeakEwma trackers the picker
	// consumes. Kept in sync with the flat p.sessions slice below —
	// OnActive registers into both; OnClose / prune remove from both.
	// sl has its own lock (finer than p.mu); ordering rule: never take
	// p.mu while holding sl.mu.
	sl     *sessionList
	budget SessionThrottler
	// sessions is the flat cross-index used by snapshot / teardown /
	// dead-sweep read paths. AFE-aware picking goes through sl above.
	// Both are kept in sync in OnActive / OnClose / prune*.
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

	// Lifecycle counters surfaced via PoolSnapshot. All bumped lock-free.
	sessionsOpened atomic.Int64
	sessionsClosed atomic.Int64
	listenerFires  atomic.Int64
	closesByReason sync.Map // close-reason label → *atomic.Int64

	// waitersCount is the live count of callers parked inside
	// CheckoutSession waiting for an idle session at the pool boundary.
	// This is the "pending vRPCs" signal the sizer needs — Java tracks
	// it via a dedicated Sized input. Before this field, Stats() was
	// (mis)reporting sum(outstanding) as PendingCount, which equaled
	// InUseCount and made the sizer oscillate.
	waitersCount atomic.Int32

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
	timeSeriesMu    sync.Mutex
	timeSeries      []TimeSeriesSample
	tsLastOkRpcs    int64
	tsLastErrorRpcs int64

	// lifetimesMu / lifetimes is a ring buffer of the last N completed
	// session lifetimes (from pool admission to recordSessionClose). Lets
	// the debug UI render a churn histogram + p50/p95 lifetime.
	lifetimesMu sync.Mutex
	lifetimes   []time.Duration

	// pickHistoryMu / pickHistory is a circular ring buffer of the last N
	// picker decisions from CheckoutSession. Powers the loadz debug page's
	// "recent pick decisions" table so operators can trace picker
	// reasoning without re-running the pick. pickHistoryHead is the index
	// of the oldest slot (= next write slot once len == maxPickHistory);
	// snapshotPickHistory unwraps oldest-first. Constant-time append —
	// this path runs under every CheckoutSession.
	pickHistoryMu   sync.Mutex
	pickHistory     []PickHistoryEvent
	pickHistoryHead int
	// afePickCounts is a running per-AFE tally of successful picks over
	// the pool's lifetime. loadz consumes it to compute actual-share vs.
	// ideal-share tables. Keyed by afeID; a small map (single-digit AFEs
	// typically).
	afePickCounts map[afeID]int64

	// backendLatencyHist / totalLatencyHist keep pool-wide latency
	// percentiles over the pool's entire lifetime. The per-session ring
	// buffers on Session cap at 256 samples each and are lost when a
	// session is recycled, so the pool's "N=…" in the debug UI would be
	// bounded by (active sessions × 256) if we relied on them. These
	// lock-free log2-bucket histograms record every sample as it flows
	// through Invoke and survive session churn.
	backendLatencyHist   latencyHist
	totalLatencyHist     latencyHist
	transportLatencyHist latencyHist

	// freeSignal is the pool-level "a session became idle" wake-up
	// channel. CheckoutSession parks here when every active session
	// has outstanding > 0; DecOutstanding does a non-blocking send
	// when it brings a session back to outstanding == 0; OnActive
	// does the same when a freshly-handshaked session enters the
	// ready set. Buffer 1 so at most one wake-up is in flight.
	freeSignal chan struct{}

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
// so the name stays URL-safe for the channelz→sessionz anchor link.
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
		sl:                 newSessionList(),
		afePickCounts:      make(map[afeID]int64),
	}

	fetcher := func() *PoolStats {
		return pool.Stats()
	}
	pool.sizer = NewPoolSizer(fetcher, min, max, 0.10)
	// Default to LeastInFlight (Java-parity default) with K-choice-2.
	// UpdateConfig can switch to Simple / LeastLatency via the server's
	// LoadBalancingOptions.
	pool.picker = NewLeastInFlightAfePicker(defaultAfeRandomSubsetSize)
	pool.budget = NewAdaptiveSessionThrottler(10, 10*time.Second)

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
	p.mu.Lock()
	if !p.closed && len(p.sessions) == 0 {
		go p.PerformScaling(ctx)
	}
	p.mu.Unlock()

	for {
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			return nil, errors.New("session pool is closed")
		}

		// Fast path: two-tier pick — pool.picker chooses an AFE from the
		// ready snapshot, then sessionList dequeues one idle session
		// from that AFE. sessionList's queue only contains idle
		// sessions, so we don't scan busy ones. The pool-lock we hold
		// covers p.sessions + p.closed; sessionList uses its own inner
		// lock (ordering: never take p.mu while holding sl.mu — here we
		// call sl AFTER establishing !p.closed but the sl calls don't
		// acquire p.mu, so ordering is trivially satisfied).
		ready := p.sl.ReadyAfes()
		pickerName := p.picker.Name()
		afe, decision := p.picker.PickAfe(ready)
		p.recordPickDecision(decision, pickerName)
		if afe != nil {
			if idle := p.sl.Checkout(afe); idle != nil {
				idle.IncOutstanding()
				atomic.AddInt64(&idle.picks, 1)
				p.mu.Unlock()
				return idle, nil
			}
		}

		// Slow path: picker returned nil. Sweep any dead handles now
		// so subsequent scans stay cheap, then trigger scale-up if
		// under max. Sweeping only when the picker misses trades a
		// slightly stale p.sessions view (a session that turned dead
		// between checkouts stays in the slice until the next miss)
		// for skipping the sweep on every successful checkout.
		var dead []*SessionHandle
		for _, sh := range p.sessions {
			if sh == nil || sh.session == nil {
				continue
			}
			if sh.session.State() != StateReady {
				dead = append(dead, sh)
			}
		}
		if len(dead) > 0 {
			p.pruneDeadLocked(dead)
		}
		if len(p.sessions) < p.maxSessions {
			go p.PerformScaling(ctx)
		}
		p.mu.Unlock()

		// Park on the wake-up channel. DecOutstanding posts when any
		// session returns to outstanding == 0; OnActive posts when a
		// fresh session lands. The 50ms timer is a safety net in case
		// a wake-up was dropped (cap-1 buffer; concurrent posts
		// collapse). ctx.Done unblocks immediately.
		//
		// Bracket the wait with waitersCount so the sizer (via Stats())
		// sees the real queue depth at the pool boundary. Java tracks
		// the same signal through its Sized pendingRpcs input.
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

// pruneDeadLocked removes the given handles from p.sessions (caller
// holds p.mu) AND from the AFE-aware sessionList. Records each close +
// lifetime and triggers a scale-up to fill the gap. Separate so
// CheckoutSession stays readable.
func (p *SessionPoolImpl) pruneDeadLocked(dead []*SessionHandle) {
	for _, victim := range dead {
		for i, sh := range p.sessions {
			if sh == victim {
				if created, ok := p.sessionCreatedAt[victim]; ok {
					p.recordLifetime(time.Since(created))
				}
				p.recordSessionClose(victim.session, "DeadOnPick")
				delete(p.sessionCreatedAt, victim)
				p.sessions = append(p.sessions[:i], p.sessions[i+1:]...)
				break
			}
		}
		p.sl.OnSessionClosed(victim)
	}
	go p.PerformScaling(context.Background())
}

// signalFree posts to p.freeSignal without blocking. Cap-1 buffer
// collapses concurrent signals; that's fine — the woken waiter
// re-scans everything under the lock.
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
		if sh.session.State() == StateReady {
			ready++
		}
		if atomic.LoadInt64(&sh.outstanding) > 0 {
			inUse++
		}
	}

	return &PoolStats{
		ReadyCount:    ready,
		InUseCount:    inUse,
		StartingCount: len(p.startingSessions),
		// PendingCount is the true pool-boundary queue depth —
		// callers parked inside CheckoutSession waiting on
		// freeSignal. Matches Java's pendingRpcs.getSize() input
		// to the sizer.
		PendingCount: int(p.waitersCount.Load()),
	}
}

// UpdateConfig dynamically adjusts the pool size constraints and budget governor limits at runtime.
func (p *SessionPoolImpl) UpdateConfig(config *spb.SessionClientConfiguration_SessionPoolConfiguration) {
	p.listenerFires.Add(1)
	p.mu.Lock()
	p.minSessions = int(config.MinSessionCount)
	p.maxSessions = int(config.MaxSessionCount)

	if config.LoadBalancingOptions != nil {
		lbo := config.LoadBalancingOptions
		switch opt := lbo.LoadBalancingStrategy.(type) {
		case *spb.LoadBalancingOptions_Random_:
			// Java's SimplePicker: uniform random over ready AFEs, then
			// dequeue any idle session in that AFE.
			p.picker = NewSimpleAfePicker()
		case *spb.LoadBalancingOptions_LeastInFlight_:
			p.picker = NewLeastInFlightAfePicker(defaultAfeRandomSubsetSize)
		case *spb.LoadBalancingOptions_PeakEwma_:
			// LoadBalancingOptions_PeakEwma maps to the per-AFE
			// least-latency picker (Java's LeastLatencyPicker); its
			// RandomSubsetSize field caps the K-choice draw.
			subsetSize := defaultAfeRandomSubsetSize
			if opt.PeakEwma != nil && opt.PeakEwma.RandomSubsetSize > 0 {
				subsetSize = int(opt.PeakEwma.RandomSubsetSize)
			}
			p.picker = NewLeastLatencyAfePicker(subsetSize)
		}
	}
	p.mu.Unlock()

	// Dynamically update sizer thresholds E2E!
	p.sizer.UpdateConfig(config)
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
	// per-AFE PeakEwma update sees the actual outcome. The pool wakes
	// a waiter centrally here (rather than from DecOutstanding) so a
	// released session is guaranteed back in its AFE queue before the
	// wake fires.
	var (
		invokeErr  error
		backendDur time.Duration
		latency    time.Duration
	)
	defer func() {
		sh.DecOutstanding()
		p.sl.ReleaseToPool(sh)
		p.sl.RecordVRpcOutcome(sh, latency, backendDur, invokeErr == nil)
		p.signalFree()
	}()

	var result InvokeResult
	result, invokeErr = sh.session.Invoke(ctx, desc, req)
	latency = time.Since(start)
	// Feed the pool-level histograms so the debug UI's TotalLatency and
	// BackendLatency "N=…" grow over the pool's lifetime instead of
	// being capped at (active sessions × 256) by per-session ring
	// buffers. BackendLatency only records when the server populated
	// Stats — client-observed TotalLatency records for every call.
	p.totalLatencyHist.record(latency)
	if result.Stats != nil && result.Stats.BackendLatency != nil {
		backendDur = result.Stats.BackendLatency.AsDuration()
		p.backendLatencyHist.record(backendDur)
	}
	// Java-parity ClientTransportLatency: (stream Send→Recv) − backend =
	// wire + AFE + client-decode overhead outside server processing.
	// Skip samples missing either half (pre-Recv error, no server Stats)
	// or with a non-positive delta (clock skew, backend > stream) so
	// the p50 isn't dragged toward 0. Compute once, share with the
	// pool histogram, the slow-event row, and the exported OTel metric.
	var transportOverhead time.Duration
	if result.TransportLatency > 0 && backendDur > 0 {
		if d := result.TransportLatency - backendDur; d > 0 {
			transportOverhead = d
			p.transportLatencyHist.record(d)
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
		if openedAt := sh.session.OpenedAt(); !openedAt.IsZero() {
			ev.SessionAge = start.Sub(openedAt)
		}
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
