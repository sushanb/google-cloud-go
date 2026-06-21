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
	"math"
	"sync"
	"time"

	spb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
)

// PoolStats defines the snapshot statistics of a session pool.
type PoolStats struct {
	ReadyCount    int
	StartingCount int
	InUseCount    int
	PendingCount  int
}

// StatsFetcher is a function type that retrieves the current PoolStats.
type StatsFetcher func() *PoolStats

// PoolSizer calculates the optimal session pool size based on workload metrics.
//
// Mirrors the Java PoolSizer in google-cloud-java:
//   - effectivePending uses a server-configured divisor
//     (NewSessionQueueLength), not a hardcoded /10.
//   - idle headroom is floored to minIdleSessions so the cushion can't
//     collapse to zero at low load.
//   - the capacity comparison is split: scale UP only when desired
//     exceeds eventual (ready + starting), scale DOWN only when desired
//     is below immediate (ready). The dead band between immediate and
//     eventual prevents pruning sessions whose handshake is still
//     landing — the root of the per-second 5↔8 oscillation we saw.
type PoolSizer struct {
	mu              sync.Mutex
	fetcher         StatsFetcher
	minSessions     int
	maxSessions     int
	headroomPct     float64 // Idle headroom as a fraction of sessions in use (e.g., 0.10 = 10%)
	newSessionQLen  int     // server-driven per-session pending queue length; divides PendingCount
	minIdleSessions int     // floor on the idle cushion so headroom never collapses to 0
	// lastScaleUp is the time we most recently returned a positive delta
	// (i.e. asked to grow the pool). Scale-down is suppressed for
	// downscaleCooldown after this — the just-launched sessions need a
	// chance to land and absorb the next wave before we kill them.
	lastScaleUp time.Time

	// inUseHist is a ring of recent (timestamp, in_use+effectivePending)
	// samples used to compute the peak working set over peakInUseWindow.
	// Scale-DOWN reads the peak instead of the snapshot — without it,
	// a single momentary dip between bursts is enough to prune a pile
	// of sessions that would be needed again 300ms later. Scale-UP
	// continues to read the snapshot so we react fast to genuine
	// shortages. Net: "fast up, slow down" hysteresis.
	inUseHist []inUseSample
}

type inUseSample struct {
	at    time.Time
	value int // InUseCount + EffectivePending — the "active work" the sizer cares about
}

const (
	defaultNewSessionQueueLength = 10
	defaultMinIdleSessions       = 1
	// downscaleCooldown is how long after a scale-up the sizer refuses
	// to scale down. ~3× the typical handshake budget so freshly-grown
	// sessions get to serve at least one wave before being eligible
	// for pruning.
	downscaleCooldown = 5 * time.Second
	// peakInUseWindow is how far back the sizer looks when computing the
	// "peak working set" used for scale-down decisions. Set wide enough
	// to span a typical wave-shaped traffic cycle (LOW + HIGH phases),
	// so transient troughs inside a cycle don't trigger pruning.
	peakInUseWindow = 30 * time.Second
	// maxInUseSamples caps the in-use history ring. At the 1-Hz heartbeat
	// plus event-driven CheckoutSession kicks, ~120 samples is plenty
	// to cover peakInUseWindow even when load is bursty.
	maxInUseSamples = 128
)

// NewPoolSizer creates a new PoolSizer.
func NewPoolSizer(fetcher StatsFetcher, minSessions, maxSessions int, headroomPct float64) *PoolSizer {
	if headroomPct <= 0 {
		headroomPct = 0.10 // Default to 10% headroom
	}
	return &PoolSizer{
		fetcher:         fetcher,
		minSessions:     minSessions,
		maxSessions:     maxSessions,
		headroomPct:     headroomPct,
		newSessionQLen:  defaultNewSessionQueueLength,
		minIdleSessions: defaultMinIdleSessions,
	}
}

// UpdateConfig dynamically adjusts the sizer capacity bounds and headroom cushions at runtime.
func (s *PoolSizer) UpdateConfig(config *spb.SessionClientConfiguration_SessionPoolConfiguration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.minSessions = int(config.MinSessionCount)
	s.maxSessions = int(config.MaxSessionCount)
	s.headroomPct = float64(config.Headroom)
	if nsql := int(config.GetNewSessionQueueLength()); nsql > 0 {
		s.newSessionQLen = nsql
	}
}

// ScaleDecision is the full decision trace from one evaluation of the
// sizer. Every input, every intermediate value, plus the final delta
// and the reason it landed where it did. Surfaced in ScalingEvent so
// operators can answer "WHY did the sizer choose this?" without
// re-running the math by hand.
type ScaleDecision struct {
	// Inputs (copy of PoolStats at the moment of decision)
	ReadyCount    int
	StartingCount int
	InUseCount    int
	PendingCount  int // pool-boundary waiters

	// Sizer config snapshot
	MinSessions     int
	MaxSessions     int
	HeadroomPct     float64
	NewSessionQLen  int
	MinIdleSessions int

	// Intermediates
	EffectivePending  int // ceil(PendingCount / NewSessionQLen)
	SessionsInUse     int // InUseCount + EffectivePending
	IdleHeadroom      int // max(MinIdleSessions, ceil(SessionsInUse * HeadroomPct))
	DesiredRaw        int // SessionsInUse + IdleHeadroom (pre-clamp)
	DesiredCapacity   int // clamped to [MinSessions, MaxSessions] — used for SCALE-UP
	ImmediateCapacity int // ReadyCount
	EventualCapacity  int // ReadyCount + StartingCount

	// Scale-down inputs — derived from peak-over-window, not snapshot.
	// PeakWorkingSet is max(InUseCount + EffectivePending) over
	// peakInUseWindow. DesiredCapacityDown is the corresponding desired
	// pool size. Only used by the scale-down branch — keeps that
	// decision smoothed across wave troughs.
	PeakWorkingSet      int
	DesiredCapacityDown int

	// Final decision
	Delta             int           // what GetScaleDelta returns
	WouldDelta        int           // what Delta would have been without cooldown
	CooldownActive    bool          // true iff downscale was suppressed
	CooldownRemaining time.Duration // time left in the cooldown window
	Branch            string        // "scale-up" | "scale-down" | "suppressed" | "dead-band" | "no-stats"
}

// GetScaleDelta evaluates the current statistics and calculates the required scaling delta.
// Thin wrapper around Decide() that returns only the final delta — kept for callers
// that don't need the trace.
func (s *PoolSizer) GetScaleDelta() int {
	return s.Decide().Delta
}

// Decide computes a full ScaleDecision: every input, every intermediate,
// and the reasoning behind the final delta. Use this in PerformScaling
// so each ScalingEvent in the ring buffer carries the decision's
// provenance. As a side effect, stamps lastScaleUp when Delta > 0 so
// the cooldown applies to subsequent calls.
func (s *PoolSizer) Decide() ScaleDecision {
	s.mu.Lock()
	defer s.mu.Unlock()

	d := ScaleDecision{
		MinSessions:     s.minSessions,
		MaxSessions:     s.maxSessions,
		HeadroomPct:     s.headroomPct,
		NewSessionQLen:  s.newSessionQLen,
		MinIdleSessions: s.minIdleSessions,
	}

	stats := s.fetcher()
	if stats == nil {
		d.Branch = "no-stats"
		return d
	}
	d.ReadyCount = stats.ReadyCount
	d.StartingCount = stats.StartingCount
	d.InUseCount = stats.InUseCount
	d.PendingCount = stats.PendingCount

	// effectivePending = ceil(PendingCount / NewSessionQueueLength)
	// Java: int effectivePending = (int) Math.ceil((double) pendingRpcs.getSize() / pendingVRpcsPerSession);
	divisor := s.newSessionQLen
	if divisor <= 0 {
		divisor = defaultNewSessionQueueLength
	}
	d.EffectivePending = int(math.Ceil(float64(stats.PendingCount) / float64(divisor)))
	d.SessionsInUse = stats.InUseCount + d.EffectivePending

	// Sample the current working set into the sliding-window ring and
	// compute the peak we'll use for scale-DOWN decisions.
	now := time.Now()
	s.recordInUseLocked(d.SessionsInUse, now)
	d.PeakWorkingSet = s.peakInUseLocked(now)

	// Idle headroom as a fraction of in-use, FLOORED so a brief in-use
	// dip can't collapse the cushion to zero.
	d.IdleHeadroom = int(math.Ceil(float64(d.SessionsInUse) * s.headroomPct))
	if d.IdleHeadroom < s.minIdleSessions {
		d.IdleHeadroom = s.minIdleSessions
	}
	d.DesiredRaw = d.SessionsInUse + d.IdleHeadroom

	// Scale-UP desired uses the current snapshot — keeps reaction fast.
	d.DesiredCapacity = clamp(d.DesiredRaw, s.minSessions, s.maxSessions)

	// Scale-DOWN desired uses the windowed peak — keeps reaction slow.
	// Headroom rides on the peak too, so the dead band tracks the
	// actual wave amplitude rather than the trough.
	downIdle := int(math.Ceil(float64(d.PeakWorkingSet) * s.headroomPct))
	if downIdle < s.minIdleSessions {
		downIdle = s.minIdleSessions
	}
	d.DesiredCapacityDown = clamp(d.PeakWorkingSet+downIdle, s.minSessions, s.maxSessions)

	d.ImmediateCapacity = stats.ReadyCount
	d.EventualCapacity = stats.ReadyCount + stats.StartingCount

	if d.DesiredCapacity > d.EventualCapacity {
		d.Delta = d.DesiredCapacity - d.EventualCapacity
		d.WouldDelta = d.Delta
		d.Branch = "scale-up"
		s.lastScaleUp = now
		return d
	}
	if d.DesiredCapacityDown < d.ImmediateCapacity {
		raw := d.DesiredCapacityDown - d.ImmediateCapacity
		d.WouldDelta = raw
		if !s.lastScaleUp.IsZero() {
			elapsed := now.Sub(s.lastScaleUp)
			if elapsed < downscaleCooldown {
				d.CooldownActive = true
				d.CooldownRemaining = downscaleCooldown - elapsed
				d.Delta = 0
				d.Branch = "suppressed"
				return d
			}
		}
		d.Delta = raw
		d.Branch = "scale-down"
		return d
	}
	d.Branch = "dead-band"
	return d
}

// recordInUseLocked appends a sample, drops any older than peakInUseWindow,
// and caps the ring at maxInUseSamples. Caller holds s.mu.
func (s *PoolSizer) recordInUseLocked(value int, now time.Time) {
	cutoff := now.Add(-peakInUseWindow)
	// Drop expired prefix.
	i := 0
	for i < len(s.inUseHist) && s.inUseHist[i].at.Before(cutoff) {
		i++
	}
	if i > 0 {
		s.inUseHist = s.inUseHist[i:]
	}
	s.inUseHist = append(s.inUseHist, inUseSample{at: now, value: value})
	if len(s.inUseHist) > maxInUseSamples {
		s.inUseHist = s.inUseHist[len(s.inUseHist)-maxInUseSamples:]
	}
}

// peakInUseLocked returns max(value) across samples within peakInUseWindow.
// Caller holds s.mu.
func (s *PoolSizer) peakInUseLocked(now time.Time) int {
	cutoff := now.Add(-peakInUseWindow)
	peak := 0
	for _, sm := range s.inUseHist {
		if sm.at.Before(cutoff) {
			continue
		}
		if sm.value > peak {
			peak = sm.value
		}
	}
	return peak
}

func clamp(v, lo, hi int) int {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}
