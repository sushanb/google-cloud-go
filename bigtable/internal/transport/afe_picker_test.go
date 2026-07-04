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
	"testing"
)

// makeSnapshot builds an afeSnapshot pointing at a fresh afeHandle with
// the given id. Used only in these tests; production callers get
// snapshots from sessionList.ReadyAfes.
func makeSnapshot(id afeID, inflight int, e2eCost float64) afeSnapshot {
	return afeSnapshot{
		Handle:         &afeHandle{id: id},
		IdleCount:      1,
		NumOutstanding: inflight,
		E2eCost:        e2eCost,
	}
}

func TestSimpleAfePicker_EmptyReturnsNil(t *testing.T) {
	got, dec := NewSimpleAfePicker().PickAfe(nil)
	if got != nil {
		t.Errorf("PickAfe(nil) handle = %v, want nil", got)
	}
	if dec.Reason != "no-candidates" {
		t.Errorf("PickAfe(nil) reason = %q, want no-candidates", dec.Reason)
	}
}

func TestSimpleAfePicker_DistributesRoughly(t *testing.T) {
	snaps := []afeSnapshot{
		makeSnapshot(1, 0, 0),
		makeSnapshot(2, 0, 0),
		makeSnapshot(3, 0, 0),
	}
	counts := map[afeID]int{}
	p := NewSimpleAfePicker()
	const N = 3000
	for i := 0; i < N; i++ {
		h, _ := p.PickAfe(snaps)
		counts[h.ID()]++
	}
	for _, s := range snaps {
		got := counts[s.Handle.ID()]
		// 3-way uniform expected value = 1000; allow ±25% (~750..1250).
		if got < 750 || got > 1250 {
			t.Errorf("AFE %d picked %d/%d times, want ~1000 ±25%%", s.Handle.ID(), got, N)
		}
	}
}

func TestLeastInFlightAfePicker_EmptyReturnsNil(t *testing.T) {
	got, _ := NewLeastInFlightAfePicker(0).PickAfe(nil)
	if got != nil {
		t.Errorf("PickAfe(nil) = %v, want nil", got)
	}
}

// When randomSubsetSize covers the full ready set, the min NumOutstanding
// always wins deterministically.
func TestLeastInFlightAfePicker_PicksMinWhenSubsetCoversAll(t *testing.T) {
	snaps := []afeSnapshot{
		makeSnapshot(1, 5, 0),
		makeSnapshot(2, 1, 0), // min
		makeSnapshot(3, 3, 0),
	}
	p := NewLeastInFlightAfePicker(len(snaps))
	for i := 0; i < 100; i++ {
		h, _ := p.PickAfe(snaps)
		if got := h.ID(); got != 2 {
			t.Fatalf("iter %d: picked AFE %d, want 2", i, got)
		}
	}
}

// K-choice-2 over 4 AFEs where one AFE has drastically lower inflight
// should still pick that AFE the majority of the time.
func TestLeastInFlightAfePicker_KChoicePrefersLowInflight(t *testing.T) {
	snaps := []afeSnapshot{
		makeSnapshot(1, 100, 0),
		makeSnapshot(2, 100, 0),
		makeSnapshot(3, 1, 0), // the good one
		makeSnapshot(4, 100, 0),
	}
	p := NewLeastInFlightAfePicker(2)
	counts := map[afeID]int{}
	const N = 4000
	for i := 0; i < N; i++ {
		h, _ := p.PickAfe(snaps)
		counts[h.ID()]++
	}
	// K=2 draws hit AFE 3 with prob 1 - (3/4)*(2/3) = 1/2; when drawn
	// it always wins. So expected share ≈ 50% ± sampling noise (±5%).
	share := float64(counts[3]) / float64(N)
	if share < 0.4 || share > 0.6 {
		t.Errorf("K=2 share for min-inflight AFE = %.3f, want ~0.5", share)
	}
}

func TestLeastLatencyAfePicker_PicksMinCost(t *testing.T) {
	snaps := []afeSnapshot{
		makeSnapshot(1, 0, 50.0),
		makeSnapshot(2, 0, 5.0), // min cost
		makeSnapshot(3, 0, 30.0),
	}
	p := NewLeastLatencyAfePicker(len(snaps))
	for i := 0; i < 100; i++ {
		h, _ := p.PickAfe(snaps)
		if got := h.ID(); got != 2 {
			t.Fatalf("iter %d: picked AFE %d, want 2 (min E2eCost)", i, got)
		}
	}
}

func TestLeastLatencyAfePicker_EmptyReturnsNil(t *testing.T) {
	got, _ := NewLeastLatencyAfePicker(0).PickAfe(nil)
	if got != nil {
		t.Errorf("PickAfe(nil) = %v, want nil", got)
	}
}

// kChoiceMinCost must not mutate the caller's snapshot slice — pickers
// receive an immutable view from sessionList.ReadyAfes and multiple
// concurrent callers may share it.
func TestKChoiceMinCost_DoesNotMutateInput(t *testing.T) {
	snaps := []afeSnapshot{
		makeSnapshot(1, 0, 0),
		makeSnapshot(2, 0, 0),
		makeSnapshot(3, 0, 0),
	}
	orig := make([]afeSnapshot, len(snaps))
	copy(orig, snaps)

	NewLeastInFlightAfePicker(3).PickAfe(snaps)

	for i := range snaps {
		if snaps[i].Handle.ID() != orig[i].Handle.ID() {
			t.Errorf("snaps[%d] = %d, want unchanged %d", i, snaps[i].Handle.ID(), orig[i].Handle.ID())
		}
	}
}

// TestPickDecision_RecordsCandidatesAndReason verifies the K-choice
// pickers surface every sampled candidate + the winning reason so
// loadz can render the decision trace verbatim.
func TestPickDecision_RecordsCandidatesAndReason(t *testing.T) {
	snaps := []afeSnapshot{
		makeSnapshot(1, 5, 0),
		makeSnapshot(2, 1, 0),
		makeSnapshot(3, 3, 0),
	}
	_, dec := NewLeastInFlightAfePicker(2).PickAfe(snaps)
	if len(dec.Candidates) != 2 {
		t.Fatalf("Candidates len = %d, want 2 (K-choice-2)", len(dec.Candidates))
	}
	if dec.Reason != "min-inflight" {
		t.Errorf("Reason = %q, want min-inflight", dec.Reason)
	}
	if dec.Winner == 0 {
		t.Error("Winner is zero; expected the chosen AFE id")
	}

	// LeastLatency uses e2e cost.
	_, dec2 := NewLeastLatencyAfePicker(len(snaps)).PickAfe(snaps)
	if dec2.Reason != "min-latency" {
		t.Errorf("LeastLatency reason = %q, want min-latency", dec2.Reason)
	}
	if len(dec2.Candidates) != len(snaps) {
		t.Errorf("LeastLatency Candidates len = %d, want %d (K covered all)", len(dec2.Candidates), len(snaps))
	}
}

func TestAfePicker_Names(t *testing.T) {
	for _, tc := range []struct {
		p    AfePicker
		want string
	}{
		{NewSimpleAfePicker(), "simple"},
		{NewLeastInFlightAfePicker(0), "least-inflight"},
		{NewLeastLatencyAfePicker(0), "least-latency"},
	} {
		if got := tc.p.Name(); got != tc.want {
			t.Errorf("%T.Name() = %q, want %q", tc.p, got, tc.want)
		}
	}
}
