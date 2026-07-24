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
	"strings"
	"testing"
	"time"
)

// --- scalingReason ---------------------------------------------------------

func TestScalingReason(t *testing.T) {
	cases := []struct {
		name        string
		stats       *PoolStats
		delta       int
		minSessions int
		want        string // substring the returned message must contain
	}{
		{
			name:  "scale-up nil stats",
			stats: nil,
			delta: 3,
			want:  "no stats",
		},
		{
			name:  "scale-up with pending waiters",
			stats: &PoolStats{PendingCount: 5, InUseCount: 2, ReadyCount: 4},
			delta: 1,
			want:  "pending=5",
		},
		{
			name:        "scale-up below min sessions",
			stats:       &PoolStats{PendingCount: 0, InUseCount: 0, ReadyCount: 4, StartingCount: 0},
			delta:       1,
			minSessions: 5,
			want:        "below min sessions",
		},
		{
			name:  "scale-up headroom exhausted",
			stats: &PoolStats{PendingCount: 0, InUseCount: 4, ReadyCount: 4},
			delta: 1,
			want:  "headroom exhausted",
		},
		{
			name:  "scale-up load exceeds headroom",
			stats: &PoolStats{PendingCount: 0, InUseCount: 3, ReadyCount: 10},
			delta: 1,
			want:  "load>headroom",
		},
		{
			name:  "scale-down nil stats",
			stats: nil,
			delta: -1,
			want:  "no stats",
		},
		{
			name:  "scale-down with counts",
			stats: &PoolStats{ReadyCount: 8, InUseCount: 1},
			delta: -3,
			want:  "scale down",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := scalingReason(tc.stats, tc.delta, tc.minSessions)
			if !strings.Contains(got, tc.want) {
				t.Errorf("scalingReason(%+v, %d, %d) = %q, want substring %q",
					tc.stats, tc.delta, tc.minSessions, got, tc.want)
			}
		})
	}
}

// --- scaling-history ring --------------------------------------------------

func TestRecordScaling_RingCaps(t *testing.T) {
	p := newTestPool(t, 1, 10)
	for i := 0; i < maxScalingHistory+3; i++ {
		p.recordScaling(ScalingEvent{
			At:        time.Now(),
			Before:    i,
			Requested: 1,
			Launched:  1,
			Reason:    "test",
		})
	}
	snap := p.snapshotScalingHistory()
	if len(snap) != maxScalingHistory {
		t.Fatalf("len = %d, want %d", len(snap), maxScalingHistory)
	}
	// Oldest 3 events dropped: snap[0].Before should be 3 (the 4th append).
	if snap[0].Before != 3 {
		t.Errorf("snap[0].Before = %d, want 3", snap[0].Before)
	}
	if snap[len(snap)-1].Before != maxScalingHistory+2 {
		t.Errorf("snap[last].Before = %d, want %d", snap[len(snap)-1].Before, maxScalingHistory+2)
	}
}

func TestSnapshotScalingHistory_ReturnsCopy(t *testing.T) {
	p := newTestPool(t, 1, 10)
	p.recordScaling(ScalingEvent{Before: 1})
	p.recordScaling(ScalingEvent{Before: 2})

	snap := p.snapshotScalingHistory()
	if len(snap) != 2 {
		t.Fatalf("len = %d, want 2", len(snap))
	}
	// Mutate the snapshot; live buffer must be untouched.
	snap[0].Before = 999
	live := p.snapshotScalingHistory()
	if live[0].Before != 1 {
		t.Errorf("live[0].Before = %d, want 1 (snapshot must be an independent copy)", live[0].Before)
	}
}

// --- noDeadlineButCancellableContext ---------------------------------------

func TestNoDeadlineButCancellableContext_StripsDeadline(t *testing.T) {
	parent, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Hour))
	defer cancel()

	wrapped := noDeadlineButCancellableContext{Context: parent}
	if _, ok := wrapped.Deadline(); ok {
		t.Error("Deadline() returned ok=true after stripping — expected zero-value")
	}
}

func TestNoDeadlineButCancellableContext_PreservesCancel(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	wrapped := noDeadlineButCancellableContext{Context: parent}

	select {
	case <-wrapped.Done():
		t.Fatal("wrapped ctx.Done fired prematurely")
	default:
	}

	cancel()
	select {
	case <-wrapped.Done():
		// expected
	case <-time.After(100 * time.Millisecond):
		t.Fatal("wrapped ctx.Done did not fire within 100ms of parent cancel")
	}
	if wrapped.Err() == nil {
		t.Error("wrapped.Err() = nil after cancel; expected non-nil")
	}
}

func TestNoDeadlineButCancellableContext_PreservesValues(t *testing.T) {
	type k struct{}
	parent := context.WithValue(context.Background(), k{}, "sentinel")
	wrapped := noDeadlineButCancellableContext{Context: parent}
	if got := wrapped.Value(k{}); got != "sentinel" {
		t.Errorf("wrapped.Value = %v, want sentinel", got)
	}
}

// --- Tick --------------------------------------------------------

func TestTick_EarlyReturnWhenClosed(t *testing.T) {
	p := newTestPool(t, 1, 10)
	// Mark closed BEFORE calling Tick. The function should
	// return without recording a scaling event.
	p.mu.Lock()
	p.closed = true
	p.mu.Unlock()

	before := len(p.snapshotScalingHistory())
	p.Tick(context.Background())
	after := len(p.snapshotScalingHistory())
	if after != before {
		t.Errorf("scaling history grew despite closed pool: %d → %d", before, after)
	}
}

func TestTick_ScalingInProgressGate(t *testing.T) {
	p := newTestPool(t, 1, 10)
	// Simulate a prior Tick still in progress. The second call
	// must exit immediately without racing.
	p.mu.Lock()
	p.scalingInProgress = true
	p.mu.Unlock()

	before := len(p.snapshotScalingHistory())
	p.Tick(context.Background())
	after := len(p.snapshotScalingHistory())
	if after != before {
		t.Errorf("scaling event recorded despite scalingInProgress=true: %d → %d", before, after)
	}
	// Gate should NOT have been cleared by our early-return path (only
	// the successful path's defer clears it).
	p.mu.Lock()
	stillGated := p.scalingInProgress
	p.mu.Unlock()
	if !stillGated {
		t.Error("scalingInProgress was flipped by an early-return; the defer should only fire for the winning call")
	}
}
