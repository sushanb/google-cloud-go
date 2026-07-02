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
	"math/bits"
	"math/rand/v2"
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
	timeSeriesMu      sync.Mutex
	timeSeries        []TimeSeriesSample
	tsLastOkRpcs      int64
	tsLastErrorRpcs   int64

	// lifetimesMu / lifetimes is a ring buffer of the last N completed
	// session lifetimes (from pool admission to recordSessionClose). Lets
	// the debug UI render a churn histogram + p50/p95 lifetime.
	lifetimesMu sync.Mutex
	lifetimes   []time.Duration

	// backendLatencyHist / totalLatencyHist keep pool-wide latency
	// percentiles over the pool's entire lifetime. The per-session ring
	// buffers on Session cap at 256 samples each and are lost when a
	// session is recycled, so the pool's "N=…" in the debug UI would be
	// bounded by (active sessions × 256) if we relied on them. These
	// lock-free log2-bucket histograms record every sample as it flows
	// through Invoke and survive session churn.
	backendLatencyHist latencyHist
	totalLatencyHist   latencyHist

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

	// Deprecated: kept temporarily for JSON back-compat. FromCount mirrors
	// Before; ToCount and Delta are no longer populated meaningfully.
	FromCount int
	ToCount   int
	Delta     int
}

// SlowVRpcEvent is one row in the pool's slow-vRPC log.
type SlowVRpcEvent struct {
	At      time.Time
	Method  string
	Latency time.Duration
	Session string // log name of the session that handled the call
	Success bool
	ErrCode string // grpc status code on failure, empty on success
	// PoolWait is how long the caller spent inside CheckoutSession
	// waiting for an idle session. After the pool-queue fix this is
	// where saturation queue wait lives — SemWait should now be near
	// zero because the pool only hands out idle sessions.
	PoolWait time.Duration
	// SemWait is how long the call spent blocked in vrpcSem.Acquire
	// — i.e. queue wait for the session's single in-flight slot.
	// Should be ~0 with the pool-queue fix; non-zero only when a
	// session was picked but immediately got another concurrent
	// caller (rare with pool-level checkout).
	SemWait time.Duration
	// BackendLatency is the server-reported processing time
	// (SessionRequestStats.BackendLatency); zero if not present.
	BackendLatency time.Duration
	// TransportLatency is the time from vRPC frame Send to response
	// Recv on the stream — network RTT + server queue + Backend.
	// Zero if the call errored before a Recv event (context
	// cancellation, pre-Send failure).
	TransportLatency time.Duration
	// RpcIDOnSession is the per-session 1-indexed RPC id; very small
	// values mean this was a freshly-opened session.
	RpcIDOnSession int64
	// SessionAge is time since the session entered StateActive — zero
	// if the session never reached Active (rare error path).
	SessionAge time.Duration
	// Peer captures the AFE/GFE the session was bound to at the time
	// of the call. Empty when the bidi-stream header hasn't been
	// parsed yet (very young sessions). Lets the sessionz UI surface
	// "Unavailable cohort tied to one AFE" without manually cross-
	// referencing every per-session Peer block.
	Peer PeerInfoSnapshot
}

const (
	maxSlowVRpcs         = 100
	defaultSlowThreshold = 1 * time.Second
	// maxTimeSeries caps the per-pool sparkline ring. At the 1-Hz heartbeat
	// sampling rate this covers the most recent 5 minutes — long enough to
	// span a typical scaling event without ballooning memory.
	maxTimeSeries = 300
	// maxLifetimes caps the per-pool lifetime ring. Enough to render a
	// stable histogram for a busy pool without ballooning memory.
	maxLifetimes = 512
)

// LifetimeBuckets are the time ranges the sessionz UI shows session
// lifetimes against, smallest-first. Sized to span "churn" (sub-minute)
// through "long-lived" (hours).
var LifetimeBuckets = []struct {
	Label string
	Max   time.Duration
}{
	{"<10s", 10 * time.Second},
	{"<1m", time.Minute},
	{"<5m", 5 * time.Minute},
	{"<30m", 30 * time.Minute},
	{"<1h", time.Hour},
	{"<6h", 6 * time.Hour},
	{"<24h", 24 * time.Hour},
	{"≥24h", time.Duration(1<<62 - 1)},
}

// recordLifetime appends a completed session lifetime to the ring buffer.
// Called from each pool removal site that has a known createdAt for the
// session being retired.
func (p *SessionPoolImpl) recordLifetime(d time.Duration) {
	if d <= 0 {
		return
	}
	p.lifetimesMu.Lock()
	defer p.lifetimesMu.Unlock()
	if len(p.lifetimes) >= maxLifetimes {
		copy(p.lifetimes, p.lifetimes[1:])
		p.lifetimes = p.lifetimes[:len(p.lifetimes)-1]
	}
	p.lifetimes = append(p.lifetimes, d)
}

func (p *SessionPoolImpl) snapshotLifetimes() []time.Duration {
	p.lifetimesMu.Lock()
	out := make([]time.Duration, len(p.lifetimes))
	copy(out, p.lifetimes)
	p.lifetimesMu.Unlock()
	return out
}

// latencyHistBuckets is the log2-bucket count of latencyHist. Bucket i
// covers durations in [2^i, 2^(i+1)) nanoseconds, so 40 buckets span
// ~1ns .. ~1000s — plenty of head- and tail-room for RPC latencies.
const latencyHistBuckets = 40

// latencyHist is a lock-free log2-bucket histogram used to keep pool-wide
// latency percentiles for the entire lifetime of the pool. Constant
// memory (40 × uint64 per histogram); each record is a single atomic add.
//
// Percentiles are exact at bucket granularity and interpolated linearly
// within a bucket — that gives ≤2× worst-case error at the tail, which is
// good enough for a debug UI (server-side histograms have similar
// resolution). Preferred over per-session ring buffers because sessions
// churn (GoAway / StreamEnd / scale-down); a pool-level histogram
// survives churn and reflects every sample the pool has ever seen.
type latencyHist struct {
	buckets [latencyHistBuckets]atomic.Uint64
}

// record adds one observation. Non-positive durations are ignored so
// callers don't need to pre-filter zero-latency error paths.
func (h *latencyHist) record(d time.Duration) {
	if d <= 0 {
		return
	}
	// floor(log2(ns)); bits.Len64(x) is one more than that.
	ns := uint64(d)
	b := bits.Len64(ns) - 1
	if b < 0 {
		b = 0
	}
	if b >= latencyHistBuckets {
		b = latencyHistBuckets - 1
	}
	h.buckets[b].Add(1)
}

// snapshot returns p50/p95/p99 and the total sample count. Reads are
// lock-free but a concurrent record() may land between per-bucket loads;
// the resulting skew is bounded by the concurrent write rate over the
// snapshot window and is not meaningful for a debug UI. n is uint64 so
// billion-plus counts don't overflow int on 32-bit platforms.
func (h *latencyHist) snapshot() (p50, p95, p99 time.Duration, n uint64) {
	var counts [latencyHistBuckets]uint64
	for i := range h.buckets {
		counts[i] = h.buckets[i].Load()
		n += counts[i]
	}
	if n == 0 {
		return
	}
	p50 = interpLatencyPercentile(counts[:], n, 50)
	p95 = interpLatencyPercentile(counts[:], n, 95)
	p99 = interpLatencyPercentile(counts[:], n, 99)
	return
}

// interpLatencyPercentile walks the bucket counts and linearly
// interpolates the target position inside the containing bucket. Caller
// guarantees n > 0.
func interpLatencyPercentile(counts []uint64, n uint64, pct float64) time.Duration {
	target := uint64(float64(n) * pct / 100)
	if target == 0 {
		target = 1
	}
	var cum uint64
	for i, c := range counts {
		if c == 0 {
			continue
		}
		if cum+c >= target {
			lo := uint64(1) << i
			hi := uint64(1) << (i + 1)
			frac := float64(target-cum) / float64(c)
			return time.Duration(lo + uint64(float64(hi-lo)*frac))
		}
		cum += c
	}
	// Only reachable on numerical edge cases (target rounded above
	// total); fall back to the last non-empty bucket's upper bound.
	for i := len(counts) - 1; i >= 0; i-- {
		if counts[i] > 0 {
			return time.Duration(uint64(1) << (i + 1))
		}
	}
	return 0
}

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
	inUse := 0
	var okTotal, errTotal int64
	for _, sh := range p.sessions {
		if sh == nil || sh.session == nil {
			continue
		}
		if atomic.LoadInt64(&sh.outstanding) > 0 {
			inUse++
		}
		okTotal += sh.session.okRpcs.Load()
		errTotal += sh.session.errorRpcs.Load()
	}
	p.mu.Unlock()
	// Pending = pool-boundary queue depth (waiters parked in
	// CheckoutSession), same source as Stats().PendingCount. The previous
	// implementation summed outstanding across sessions, which with
	// multiPlexingLimit=1 just equaled inUse and made the UI lie.
	pending := int(p.waitersCount.Load())

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

// waitServerCloseGrace bounds how long a session may sit in
// StateWaitServerClose before the pool force-closes it. The server should
// EOF the stream promptly after acknowledging CloseSession; if it doesn't,
// this gives us a deterministic teardown so OnClose fires and counters move.
const waitServerCloseGrace = 30 * time.Second

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

	p.mu.Lock()
	for _, sh := range p.sessions {
		if sh == nil || sh.session == nil {
			continue
		}
		stuck := State(sh.session.state.Load()) == StateWaitServerClose
		since := time.Since(time.Unix(0, sh.session.lastStateChangeNano.Load()))
		if stuck && since > waitServerCloseGrace {
			victims = append(victims, victim{sess: sh.session, stuckFor: since})
		}
	}
	p.mu.Unlock()

	for _, v := range victims {
		btopt.Debugf(nil, "POOL %s sweepStuckSessions: force-closing %s stuck in WaitServerClose for %v",
			p.poolName, v.sess.LogName(), v.stuckFor.Round(time.Second))
		v.sess.ForceClose(&spb.CloseSessionRequest{
			Reason:      spb.CloseSessionRequest_CLOSE_SESSION_REASON_ERROR,
			Description: "stuck in WaitServerClose past grace",
		})
	}
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
// multiPlexingLimit=1 the pool only hands out a session whose
// outstanding count is 0. If every session is busy, the caller parks
// on p.freeSignal until DecOutstanding wakes them — moving the queue
// from inside the session's vrpcSem (artificial HOL blocking — random
// picks stack on busy sessions even when idle ones exist) to the pool
// boundary where the first freed session goes to the first waiter.
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

		// Fast path: picker already filters non-Active sessions
		// (LeastInFlightPicker.PickSession does the check), so most
		// checkouts skip the O(N) dead-sweep entirely. The picker also
		// applies a lastActivity tie-break so later-activated cohorts
		// aren't starved by the "first idle wins" pattern that used to
		// live inline here.
		if idle := p.picker.PickSession(); idle != nil {
			idle.IncOutstanding()
			atomic.AddInt64(&idle.picks, 1)
			p.mu.Unlock()
			return idle, nil
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
			if sh.session.State() != StateActive {
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
// holds p.mu). Records each close + lifetime, re-inits the picker, and
// triggers a scale-up to fill the gap. Separate so CheckoutSession
// stays readable.
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
	}
	p.picker = NewLeastInFlightPicker(p.sessions)
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
		if sh.session.State() == StateActive {
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
	// Capture per-handle createdAt so we can record lifetimes after the map
	// is reset below.
	createdAts := make(map[*SessionHandle]time.Time, len(snapshot))
	for _, sh := range snapshot {
		if t, ok := p.sessionCreatedAt[sh]; ok {
			createdAts[sh] = t
		}
	}
	p.sessions = nil
	p.sessionCreatedAt = make(map[*SessionHandle]time.Time)
	p.mu.Unlock()

	// Record the closes up-front (with PoolClose as the fallback reason)
	// so the debug counters reflect retirement immediately, even though the
	// actual graceful Close on each session is still in flight.
	for _, sh := range snapshot {
		if sh != nil && sh.session != nil {
			if t, ok := createdAts[sh]; ok {
				p.recordLifetime(time.Since(t))
			}
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
	// Wire DecOutstanding's "I'm now idle" notifier to the pool's
	// wake-up channel so CheckoutSession waiters get unblocked the
	// moment any session returns to outstanding == 0.
	sh.SetFreeSignal(p.freeSignal)
	p.sessions = append(p.sessions, sh)
	p.sessionCreatedAt[sh] = time.Now()
	p.sessionsOpened.Add(1)

	// Re-initialize picker with updated sessions list
	p.picker = NewLeastInFlightPicker(p.sessions)

	// New session is immediately idle. Post a wake-up so a waiting
	// worker can grab it without waiting out the 50ms safety timer.
	p.signalFree()
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
		if created, ok := p.sessionCreatedAt[removed]; ok {
			p.recordLifetime(time.Since(created))
		}
		p.recordSessionClose(s, "")
		delete(p.sessionCreatedAt, removed)
		p.sessions = append(p.sessions[:idx], p.sessions[idx+1:]...)
		// Re-initialize picker with updated active sessions
		p.picker = NewLeastInFlightPicker(p.sessions)
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
	// Sample a time-series point on every heartbeat so the sparkline ring
	// buffer fills at the heartbeat cadence regardless of whether scaling
	// actually fires below.
	p.recordTimeSeries()
	// Sweep for sessions stuck in WaitServerClose past the grace window —
	// happens when a server sent GoAway / accepted CloseSession but never
	// followed up with a stream EOF. ForceClose drives them to Closed so
	// OnClose fires and the pool retires them.
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

	decision := p.sizer.Decide()
	stats := &PoolStats{
		ReadyCount:    decision.ReadyCount,
		StartingCount: decision.StartingCount,
		InUseCount:    decision.InUseCount,
		PendingCount:  decision.PendingCount,
	}
	delta := decision.Delta

	p.mu.Lock()
	currentSessions := len(p.sessions)
	p.mu.Unlock()

	// Record SUPPRESSED scale-downs even though they don't change the
	// pool — otherwise cooldown activity is invisible in the ring
	// buffer and the operator can't see "the sizer wanted to shrink
	// but I held it back".
	if delta == 0 && decision.WouldDelta != 0 {
		p.recordScaling(ScalingEvent{
			At:        time.Now(),
			Before:    currentSessions,
			Requested: 0,
			Launched:  0,
			Reason: fmt.Sprintf("suppressed: would=%d (cooldown %v remaining; desired=%d immediate=%d)",
				decision.WouldDelta, decision.CooldownRemaining,
				decision.DesiredCapacity, decision.ImmediateCapacity),
			Decision:  decision,
			FromCount: currentSessions,
			ToCount:   currentSessions,
			Delta:     0,
		})
		return
	}
	if delta == 0 {
		// dead-band or no-stats — nothing to record.
		return
	}

	reason := scalingReason(stats, delta)
	var launched atomic.Int64
	defer func() {
		actual := int(launched.Load())
		if delta < 0 {
			actual = -actual
		}
		p.recordScaling(ScalingEvent{
			At:        time.Now(),
			Before:    currentSessions,
			Requested: delta,
			Launched:  actual,
			Reason:    reason,
			Decision:  decision,
			// Back-compat: keep the legacy fields populated so JSON
			// consumers that already parse FromCount/ToCount/Delta don't
			// break. ToCount mirrors Before because scale-up genuinely
			// hasn't grown the live pool by the time the defer fires.
			FromCount: currentSessions,
			ToCount:   currentSessions + actual,
			Delta:     delta,
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
					btopt.Debugf(nil, "POOL %s PerformScaling createSession failed: %v", p.poolName, err)
				} else {
					launched.Add(1)
				}
			}()
		}
		wg.Wait()
	} else {
		// Scale down: prune idle sessions gracefully
		launched.Store(int64(p.pruneSessions(-delta)))
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
		btopt.Debugf(nil, "POOL %p createSession Start failed for %s: %v", p, sessionName, err)
		return fmt.Errorf("failed to start session: %w", err)
	}

	success = true
	return nil
}

// pruneSessions removes up to count idle sessions from the pool and kicks
// off graceful close on each. Returns the number actually pruned so the
// scaling-history event can report what happened.
func (p *SessionPoolImpl) pruneSessions(count int) int {
	// Phase 1: under lock, select prune candidates and remove them from
	// p.sessions immediately so concurrent CheckoutSession callers don't
	// pick them. Skip sessions younger than 5s so we don't churn through
	// newly-minted sessions before they have a chance to absorb load.
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return 0
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
				if ok {
					p.recordLifetime(now.Sub(createdAt))
				}
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
	p.picker = NewLeastInFlightPicker(p.sessions)
	p.mu.Unlock()

	if len(toClose) == 0 {
		return 0
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
	return pruned
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
	checkoutStart := time.Now()
	sh, err := p.CheckoutSession(ctx)
	if err != nil {
		return InvokeResult{}, err
	}
	poolWait := time.Since(checkoutStart)
	// Use checkoutStart as the wall-clock anchor so the recorded
	// Latency includes the pool queue wait — that's the user-visible
	// time. Without it the pool-queue fix would silently hide the
	// wait from the slow-vRPC log.
	start := checkoutStart
	defer func() {
		sh.DecOutstanding(time.Since(start))
	}()

	result, invokeErr := sh.session.Invoke(ctx, desc, req)
	latency := time.Since(start)
	// Feed the pool-level histograms so the debug UI's TotalLatency and
	// BackendLatency "N=…" grow over the pool's lifetime instead of
	// being capped at (active sessions × 256) by per-session ring
	// buffers. BackendLatency only records when the server populated
	// Stats — client-observed TotalLatency records for every call.
	p.totalLatencyHist.record(latency)
	if result.Stats != nil && result.Stats.BackendLatency != nil {
		p.backendLatencyHist.record(result.Stats.BackendLatency.AsDuration())
	}
	if latency > p.slowThreshold() {
		ev := SlowVRpcEvent{
			At:               start,
			Method:           desc.Method(),
			Latency:          latency,
			Session:          sh.session.LogName(),
			Success:          invokeErr == nil,
			PoolWait:         poolWait,
			SemWait:          result.SemWait,
			TransportLatency: result.TransportLatency,
			RpcIDOnSession:   result.RpcIDOnSession,
		}
		if result.Stats != nil && result.Stats.BackendLatency != nil {
			ev.BackendLatency = result.Stats.BackendLatency.AsDuration()
		}
		if openedAt := sh.session.OpenedAt(); !openedAt.IsZero() {
			ev.SessionAge = start.Sub(openedAt)
		}
		// Capture the session's PeerInfo so cohort patterns (e.g. every
		// Unavailable failure on AFE X) are visible directly in the
		// slow-vRPC table instead of requiring a per-session cross-ref.
		ev.Peer = peerInfoToSnapshot(sh.session.peerInfo.Load())
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
