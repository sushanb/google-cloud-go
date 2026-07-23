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

// Debug / observability ring buffers and histograms attached to
// SessionPoolImpl. Everything in this file exists to feed sessionz /
// loadz / metric exporters — none of the pool's operational logic
// depends on it. Isolated so a reviewer reading the hot path in
// session_pool.go doesn't have to page past 500 lines of bookkeeping.

package internal

import (
	"context"
	"errors"
	"math/bits"
	"sync"
	"sync/atomic"
	"time"

	btopt "cloud.google.com/go/bigtable/internal/option"
	"google.golang.org/grpc/status"
)

// poolMetrics owns every observability-only field that used to hang
// directly off SessionPoolImpl — counters, ring buffers, and
// histograms. Extracted so a reader opening SessionPoolImpl sees the
// operational shape (sessions, sizer, hooks) without wading through
// 100+ lines of bookkeeping. All methods that mutate these fields live
// in session_pool_debug.go / _lifecycle.go / _scaling.go and access
// via p.m.<field>. The struct is embedded by value on the pool (single
// pointer chase is fine on the hot path — Go inlines it).
type poolMetrics struct {
	// Lifecycle counters, lock-free.
	sessionsOpened atomic.Int64
	sessionsClosed atomic.Int64
	listenerFires  atomic.Int64
	closesByReason sync.Map // close-reason label → *atomic.Int64

	// Scaling history ring buffer.
	scalingHistoryMu sync.Mutex
	scalingHistory   []ScalingEvent

	// Slow-vRPC log ring buffer.
	slowVRpcsMu       sync.Mutex
	slowVRpcs         []SlowVRpcEvent
	slowVRpcThreshold time.Duration

	// Time-series sparkline ring buffer + rate-computation state.
	timeSeriesMu    sync.Mutex
	timeSeries      []TimeSeriesSample
	tsLastOkRpcs    int64
	tsLastErrorRpcs int64

	// Session-lifetime ring buffer.
	lifetimesMu sync.Mutex
	lifetimes   []time.Duration

	// Picker-decision ring buffer + per-AFE counters.
	pickHistoryMu   sync.Mutex
	pickHistory     []PickHistoryEvent
	pickHistoryHead int
	afePickCounts   map[afeID]int64

	// Pool-wide latency histograms — lifetime-of-pool, survive session
	// churn (per-session ring buffers cap at 256 samples each).
	backendLatencyHist   latencyHist
	totalLatencyHist     latencyHist
	transportLatencyHist latencyHist
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
	// waiting for an idle session. This is where saturation queue wait
	// lives — the pool only hands out idle sessions, so per-session
	// semaphore wait no longer exists.
	PoolWait time.Duration
	// BackendLatency is the server-reported processing time
	// (SessionRequestStats.BackendLatency); zero if not present.
	BackendLatency time.Duration
	// TransportLatency on SlowVRpcEvent is the delta:
	// (stream Send→Recv) − BackendLatency. Isolates wire + AFE +
	// client-decode overhead outside server processing. Zero when
	// BackendLatency isn't populated (server didn't return Stats) or
	// when the call errored before a Recv event.
	TransportLatency time.Duration
	// RpcIDOnSession is the per-session 1-indexed RPC id; very small
	// values mean this was a freshly-opened session.
	RpcIDOnSession int64
	// SessionAge is time since the session entered StateReady — zero
	// if the session never reached Active (rare error path).
	SessionAge time.Duration
	// Peer captures the AFE/GFE the session was bound to at the time
	// of the call. Empty when the bidi-stream header hasn't been
	// parsed yet (very young sessions). Lets the sessionz UI surface
	// "Unavailable cohort tied to one AFE" without manually cross-
	// referencing every per-session Peer block.
	Peer PeerInfoSnapshot
	// RemoteAddr is the TCP remote (AFE) socket address ("ip:port")
	// captured from gRPC peer info once the stream Header returned.
	// Empty on very young sessions. Sessionz renders this as a link
	// to tcpz filtered by remote, closing the loop between a slow
	// vRPC and the underlying conn's TCP_INFO.
	RemoteAddr string
}

const (
	maxSlowVRpcs         = 100
	defaultSlowThreshold = 10 * time.Millisecond
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
	p.m.lifetimesMu.Lock()
	defer p.m.lifetimesMu.Unlock()
	if len(p.m.lifetimes) >= maxLifetimes {
		copy(p.m.lifetimes, p.m.lifetimes[1:])
		p.m.lifetimes = p.m.lifetimes[:len(p.m.lifetimes)-1]
	}
	p.m.lifetimes = append(p.m.lifetimes, d)
}

func (p *SessionPoolImpl) snapshotLifetimes() []time.Duration {
	p.m.lifetimesMu.Lock()
	out := make([]time.Duration, len(p.m.lifetimes))
	copy(out, p.m.lifetimes)
	p.m.lifetimesMu.Unlock()
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
	At        time.Time
	Sessions  int
	OkPerSec  float64
	ErrPerSec float64
	InUse     int
	Pending   int
}

func (p *SessionPoolImpl) recordTimeSeries() {
	handles := p.sl.AllHandles()
	totalSessions := len(handles)
	inUse := 0
	var okTotal, errTotal int64
	for _, sh := range handles {
		if sh == nil || sh.session == nil {
			continue
		}
		if atomic.LoadInt64(&sh.outstanding) > 0 {
			inUse++
		}
		okTotal += sh.session.okRpcs.Load()
		errTotal += sh.session.errorRpcs.Load()
	}
	// Pending = pool-boundary queue depth (waiters parked in
	// CheckoutSession), same source as Stats().PendingCount. The previous
	// implementation summed outstanding across sessions, which with
	// multiPlexingLimit=1 just equaled inUse and made the UI lie.
	pending := int(p.waitersCount.Load())

	now := time.Now()
	p.m.timeSeriesMu.Lock()
	defer p.m.timeSeriesMu.Unlock()

	var okRate, errRate float64
	if len(p.m.timeSeries) > 0 {
		prev := p.m.timeSeries[len(p.m.timeSeries)-1]
		dt := now.Sub(prev.At).Seconds()
		if dt > 0 {
			okRate = float64(okTotal-p.m.tsLastOkRpcs) / dt
			errRate = float64(errTotal-p.m.tsLastErrorRpcs) / dt
		}
	}
	p.m.tsLastOkRpcs = okTotal
	p.m.tsLastErrorRpcs = errTotal

	sample := TimeSeriesSample{
		At:        now,
		Sessions:  totalSessions,
		OkPerSec:  okRate,
		ErrPerSec: errRate,
		InUse:     inUse,
		Pending:   pending,
	}
	if len(p.m.timeSeries) >= maxTimeSeries {
		copy(p.m.timeSeries, p.m.timeSeries[1:])
		p.m.timeSeries = p.m.timeSeries[:len(p.m.timeSeries)-1]
	}
	p.m.timeSeries = append(p.m.timeSeries, sample)
}

func (p *SessionPoolImpl) snapshotTimeSeries() []TimeSeriesSample {
	p.m.timeSeriesMu.Lock()
	defer p.m.timeSeriesMu.Unlock()
	out := make([]TimeSeriesSample, len(p.m.timeSeries))
	copy(out, p.m.timeSeries)
	return out
}

// recordSlowVRpc appends to the slow-vRPC ring buffer. Only called from
// SessionPoolImpl.Invoke after a call exceeds the threshold.
func (p *SessionPoolImpl) recordSlowVRpc(ev SlowVRpcEvent) {
	p.m.slowVRpcsMu.Lock()
	defer p.m.slowVRpcsMu.Unlock()
	if len(p.m.slowVRpcs) >= maxSlowVRpcs {
		copy(p.m.slowVRpcs, p.m.slowVRpcs[1:])
		p.m.slowVRpcs = p.m.slowVRpcs[:len(p.m.slowVRpcs)-1]
	}
	p.m.slowVRpcs = append(p.m.slowVRpcs, ev)
}

// recordCheckoutFailure feeds the pool-wide latency histogram and, when the
// wait exceeded the slow threshold, appends a slow-vRPC row for a call that
// never got a session. Session / RpcIDOnSession / SessionAge / Peer stay
// zero — an empty Session cell in the sessionz table is the marker for
// "checkout never returned a handle".
func (p *SessionPoolImpl) recordCheckoutFailure(checkoutStart time.Time, desc VRpcDescriptor, err error) {
	poolWait := time.Since(checkoutStart)
	p.m.totalLatencyHist.record(poolWait)
	if poolWait <= p.slowThreshold() {
		return
	}
	ev := SlowVRpcEvent{
		At:       checkoutStart,
		Method:   desc.Method(),
		Latency:  poolWait,
		PoolWait: poolWait,
	}
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		ev.ErrCode = "DeadlineExceeded"
	case errors.Is(err, context.Canceled):
		ev.ErrCode = "Canceled"
	default:
		ev.ErrCode = status.Code(err).String()
	}
	btopt.Debugf(nil, "POOL %s slow checkout failed method=%s pool_wait=%v code=%s raw_err=%v",
		p.poolName, ev.Method, poolWait, ev.ErrCode, err)
	p.recordSlowVRpc(ev)
}

func (p *SessionPoolImpl) snapshotSlowVRpcs() []SlowVRpcEvent {
	p.m.slowVRpcsMu.Lock()
	defer p.m.slowVRpcsMu.Unlock()
	out := make([]SlowVRpcEvent, len(p.m.slowVRpcs))
	copy(out, p.m.slowVRpcs)
	return out
}

func (p *SessionPoolImpl) slowThreshold() time.Duration {
	if p.m.slowVRpcThreshold > 0 {
		return p.m.slowVRpcThreshold
	}
	return defaultSlowThreshold
}

// maxPickHistory caps the pick-decision ring buffer. Sized so that a
// heavily-loaded pool (a few thousand picks/sec) retains a rolling
// window of the last ~1s of decisions — enough to answer "why did the
// picker choose that one?" without unbounded growth.
const maxPickHistory = 500

// PickHistoryEvent is one row in the pool's pick-decision log — the
// per-CheckoutSession outcome of the AfePicker. Populated even for
// no-candidate picks (Reason == "no-candidates") so loadz can show the
// fallback frequency.
type PickHistoryEvent struct {
	At       time.Time
	Decision PickDecision
	// PickerName captures which picker was in force at the time. Useful
	// when UpdateConfig swaps the picker mid-run; the buffer's older
	// entries retain their original picker attribution.
	PickerName string
}

// recordPickDecision appends a picker outcome to the ring buffer AND
// increments the per-AFE pick counter for the winner. pickerName must be
// supplied by the caller (CheckoutSession already holds p.mu when it
// reads p.picker.Name(), so we avoid a second acquisition + re-entrant
// deadlock here). Cheap enough to call from every CheckoutSession
// without gating.
func (p *SessionPoolImpl) recordPickDecision(d PickDecision, pickerName string) {
	ev := PickHistoryEvent{
		At:         time.Now(),
		Decision:   d,
		PickerName: pickerName,
	}
	p.m.pickHistoryMu.Lock()
	// Circular append: grow to cap, then overwrite oldest at head. Constant
	// time. The previous shift-left implementation memmoved ~500 events
	// per CheckoutSession once the buffer filled — a p95 regression at
	// even moderate QPS.
	if len(p.m.pickHistory) < maxPickHistory {
		p.m.pickHistory = append(p.m.pickHistory, ev)
	} else {
		p.m.pickHistory[p.m.pickHistoryHead] = ev
		p.m.pickHistoryHead++
		if p.m.pickHistoryHead == maxPickHistory {
			p.m.pickHistoryHead = 0
		}
	}
	if d.Winner != 0 {
		p.m.afePickCounts[d.Winner]++
	}
	p.m.pickHistoryMu.Unlock()
}

// snapshotPickHistory returns a copy of the pick-decision ring buffer,
// oldest-first / newest-last. Safe to call concurrently with
// recordPickDecision.
func (p *SessionPoolImpl) snapshotPickHistory() []PickHistoryEvent {
	p.m.pickHistoryMu.Lock()
	defer p.m.pickHistoryMu.Unlock()
	out := make([]PickHistoryEvent, len(p.m.pickHistory))
	if len(p.m.pickHistory) < maxPickHistory {
		// Buffer hasn't wrapped yet; slice is already oldest-first.
		copy(out, p.m.pickHistory)
	} else {
		// Full ring: oldest event lives at pickHistoryHead. Copy the tail
		// (head..end) then the head (0..pickHistoryHead).
		n := copy(out, p.m.pickHistory[p.m.pickHistoryHead:])
		copy(out[n:], p.m.pickHistory[:p.m.pickHistoryHead])
	}
	return out
}

// snapshotAfePickCounts returns a copy of the per-AFE cumulative pick
// counter map. Used by loadz to compute actual-share vs. ideal-share.
func (p *SessionPoolImpl) snapshotAfePickCounts() map[afeID]int64 {
	p.m.pickHistoryMu.Lock()
	defer p.m.pickHistoryMu.Unlock()
	out := make(map[afeID]int64, len(p.m.afePickCounts))
	for k, v := range p.m.afePickCounts {
		out[k] = v
	}
	return out
}
