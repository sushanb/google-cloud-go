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
	// Without this, instantaneous in-use dips between heartbeats let
	// the sizer prune sessions whose handshake is still in flight,
	// driving the per-second 5↔9 oscillation we observed.
	lastScaleUp time.Time
}

const (
	defaultNewSessionQueueLength = 10
	defaultMinIdleSessions       = 1
	// downscaleCooldown is how long after a scale-up the sizer refuses
	// to scale down. ~3× the typical handshake budget so freshly-grown
	// sessions get to serve at least one wave before being eligible
	// for pruning.
	downscaleCooldown = 5 * time.Second
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
	EffectivePending  int     // ceil(PendingCount / NewSessionQLen)
	SessionsInUse     int     // InUseCount + EffectivePending
	IdleHeadroom      int     // max(MinIdleSessions, ceil(SessionsInUse * HeadroomPct))
	DesiredRaw        int     // SessionsInUse + IdleHeadroom (pre-clamp)
	DesiredCapacity   int     // clamped to [MinSessions, MaxSessions]
	ImmediateCapacity int     // ReadyCount
	EventualCapacity  int     // ReadyCount + StartingCount

	// Final decision
	Delta              int           // what GetScaleDelta returns
	WouldDelta         int           // what Delta would have been without cooldown
	CooldownActive     bool          // true iff downscale was suppressed
	CooldownRemaining  time.Duration // time left in the cooldown window
	Branch             string        // "scale-up" | "scale-down" | "suppressed" | "dead-band" | "no-stats"
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

	// Idle headroom as a fraction of in-use, FLOORED so a brief in-use
	// dip can't collapse the cushion to zero.
	d.IdleHeadroom = int(math.Ceil(float64(d.SessionsInUse) * s.headroomPct))
	if d.IdleHeadroom < s.minIdleSessions {
		d.IdleHeadroom = s.minIdleSessions
	}
	d.DesiredRaw = d.SessionsInUse + d.IdleHeadroom

	d.DesiredCapacity = d.DesiredRaw
	if d.DesiredCapacity < s.minSessions {
		d.DesiredCapacity = s.minSessions
	}
	if d.DesiredCapacity > s.maxSessions {
		d.DesiredCapacity = s.maxSessions
	}

	d.ImmediateCapacity = stats.ReadyCount
	d.EventualCapacity = stats.ReadyCount + stats.StartingCount

	if d.DesiredCapacity > d.EventualCapacity {
		d.Delta = d.DesiredCapacity - d.EventualCapacity
		d.WouldDelta = d.Delta
		d.Branch = "scale-up"
		s.lastScaleUp = time.Now()
		return d
	}
	if d.DesiredCapacity < d.ImmediateCapacity {
		raw := d.DesiredCapacity - d.ImmediateCapacity
		d.WouldDelta = raw
		if !s.lastScaleUp.IsZero() {
			elapsed := time.Since(s.lastScaleUp)
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
