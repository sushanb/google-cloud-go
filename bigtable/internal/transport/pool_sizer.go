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
}

const (
	defaultNewSessionQueueLength = 10
	defaultMinIdleSessions       = 1
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

// GetScaleDelta evaluates the current statistics and calculates the required scaling delta
// to maintain the desired headroom cushion and satisfy pending calls.
func (s *PoolSizer) GetScaleDelta() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	stats := s.fetcher()
	if stats == nil {
		return 0
	}

	// effectivePending = ceil(PendingCount / NewSessionQueueLength)
	// Java: int effectivePending = (int) Math.ceil((double) pendingRpcs.getSize() / pendingVRpcsPerSession);
	divisor := s.newSessionQLen
	if divisor <= 0 {
		divisor = defaultNewSessionQueueLength
	}
	effectivePending := int(math.Ceil(float64(stats.PendingCount) / float64(divisor)))
	sessionsInUse := stats.InUseCount + effectivePending

	// Idle headroom as a fraction of in-use, FLOORED so a brief in-use
	// dip can't collapse the cushion to zero (the cause of the per-second
	// shrink-then-grow we saw in scaling history).
	idle := int(math.Ceil(float64(sessionsInUse) * s.headroomPct))
	if idle < s.minIdleSessions {
		idle = s.minIdleSessions
	}
	desiredCapacity := sessionsInUse + idle

	if desiredCapacity < s.minSessions {
		desiredCapacity = s.minSessions
	}
	if desiredCapacity > s.maxSessions {
		desiredCapacity = s.maxSessions
	}

	// Split capacity into immediate (ready right now) and eventual
	// (includes in-flight handshakes). A dead band between the two
	// prevents pruning sessions whose handshake is still landing.
	immediateCapacity := stats.ReadyCount
	eventualCapacity := stats.ReadyCount + stats.StartingCount

	if desiredCapacity > eventualCapacity {
		// Genuine shortage: even counting in-flight starts, not enough.
		return desiredCapacity - eventualCapacity
	}
	if desiredCapacity < immediateCapacity {
		// Truly overprovisioned: more idle right now than desired.
		return desiredCapacity - immediateCapacity
	}
	return 0
}
