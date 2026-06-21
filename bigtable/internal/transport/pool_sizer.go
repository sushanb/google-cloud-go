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
	PendingCount  int // pool-boundary waiters (NOT sum of outstanding)
}

// StatsFetcher is a function type that retrieves the current PoolStats.
type StatsFetcher func() *PoolStats

// PoolSizer calculates the optimal session pool size based on workload metrics.
//
// Mirrors the Java PoolSizer:
//   - effectivePending uses the server-configured NewSessionQueueLength
//     divisor, not a hardcoded /10.
//   - idle headroom is floored to minIdleSessions so the cushion can't
//     collapse to zero at low load.
//   - capacity comparison is split: scale UP only when desired exceeds
//     eventual (ready+starting), scale DOWN only when desired is below
//     immediate (ready). Dead band prevents pruning in-flight handshakes.
//   - scale-DOWN reads a 30s peak-over-window of working set so transient
//     troughs between bursts don't trigger pruning.
//   - scale-DOWN is suppressed for 5s after any scale-up (cooldown).
type PoolSizer struct {
	mu              sync.Mutex
	fetcher         StatsFetcher
	minSessions     int
	maxSessions     int
	headroomPct     float64
	newSessionQLen  int
	minIdleSessions int
	lastScaleUp     time.Time
	inUseHist       []inUseSample
}

type inUseSample struct {
	at    time.Time
	value int
}

const (
	defaultNewSessionQueueLength = 10
	defaultMinIdleSessions       = 1
	downscaleCooldown            = 5 * time.Second
	peakInUseWindow              = 30 * time.Second
	maxInUseSamples              = 128
)

// NewPoolSizer creates a new PoolSizer.
func NewPoolSizer(fetcher StatsFetcher, minSessions, maxSessions int, headroomPct float64) *PoolSizer {
	if headroomPct <= 0 {
		headroomPct = 0.10
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

// GetScaleDelta evaluates the current statistics and calculates the required scaling delta.
func (s *PoolSizer) GetScaleDelta() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	stats := s.fetcher()
	if stats == nil {
		return 0
	}

	divisor := s.newSessionQLen
	if divisor <= 0 {
		divisor = defaultNewSessionQueueLength
	}
	effectivePending := int(math.Ceil(float64(stats.PendingCount) / float64(divisor)))
	sessionsInUse := stats.InUseCount + effectivePending

	now := time.Now()
	s.recordInUseLocked(sessionsInUse, now)
	peak := s.peakInUseLocked(now)

	idle := int(math.Ceil(float64(sessionsInUse) * s.headroomPct))
	if idle < s.minIdleSessions {
		idle = s.minIdleSessions
	}
	desiredUp := clamp(sessionsInUse+idle, s.minSessions, s.maxSessions)

	downIdle := int(math.Ceil(float64(peak) * s.headroomPct))
	if downIdle < s.minIdleSessions {
		downIdle = s.minIdleSessions
	}
	desiredDown := clamp(peak+downIdle, s.minSessions, s.maxSessions)

	immediate := stats.ReadyCount
	eventual := stats.ReadyCount + stats.StartingCount

	if desiredUp > eventual {
		s.lastScaleUp = now
		return desiredUp - eventual
	}
	if desiredDown < immediate {
		if !s.lastScaleUp.IsZero() && now.Sub(s.lastScaleUp) < downscaleCooldown {
			return 0
		}
		return desiredDown - immediate
	}
	return 0
}

// recordInUseLocked appends a sample, drops any older than peakInUseWindow,
// and caps the ring at maxInUseSamples. Caller holds s.mu.
func (s *PoolSizer) recordInUseLocked(value int, now time.Time) {
	cutoff := now.Add(-peakInUseWindow)
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
