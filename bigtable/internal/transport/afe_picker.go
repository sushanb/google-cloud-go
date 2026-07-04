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
	"math/rand/v2"
)

// defaultAfeRandomSubsetSize is the default K for K-choice random draws
// in LeastInFlightAfePicker / LeastLatencyAfePicker when the caller
// doesn't specify one. Matches Java's typical LoadBalancingOptions
// randomSubsetSize=2 for K-choice-two picking.
const defaultAfeRandomSubsetSize = 2

// AfePicker picks one AFE from a snapshot of ready buckets. The pool's
// two-tier CheckoutSession chains this with sessionList.Checkout to
// dequeue a session from the chosen AFE.
//
// Returning nil means "no AFE eligible" — the pool treats that the same
// as len(ready) == 0 (park the caller on freeSignal, kick scale-up).
//
// Introduced ahead of the pool wiring (step 5). Existing consumers keep
// calling Picker.PickSession() from picker.go; those types will be
// deleted when the pool moves to the two-tier flow.
type AfePicker interface {
	PickAfe(ready []afeSnapshot) *afeHandle
	Name() string
}

// SimpleAfePicker chooses an AFE uniformly at random from the ready set.
// Java parity: SimplePicker.
type SimpleAfePicker struct{}

// NewSimpleAfePicker constructs a SimpleAfePicker.
func NewSimpleAfePicker() *SimpleAfePicker { return &SimpleAfePicker{} }

// Name returns "simple".
func (SimpleAfePicker) Name() string { return "simple" }

// PickAfe uniformly-at-random picks one bucket from ready.
func (SimpleAfePicker) PickAfe(ready []afeSnapshot) *afeHandle {
	if len(ready) == 0 {
		return nil
	}
	return ready[rand.IntN(len(ready))].Handle
}

// LeastInFlightAfePicker picks the AFE with the smallest in-flight count.
// Draws K distinct candidates via partial Fisher-Yates over the ready
// snapshot and returns the min-cost one. RandomSubsetSize caps K; when
// it's <=0 or >= len(ready) every candidate is considered. Java parity:
// LeastInFlightPicker (partial Fisher-Yates capped by randomSubsetSize,
// tie-break on first-seen order).
type LeastInFlightAfePicker struct {
	// RandomSubsetSize caps the K-choice draw. 0 or negative means
	// "consider all candidates" (Java parity when
	// LoadBalancingOptions.randomSubsetSize == 0).
	RandomSubsetSize int
}

// NewLeastInFlightAfePicker constructs a LeastInFlightAfePicker.
func NewLeastInFlightAfePicker(randomSubsetSize int) *LeastInFlightAfePicker {
	return &LeastInFlightAfePicker{RandomSubsetSize: randomSubsetSize}
}

// Name returns "least-inflight".
func (LeastInFlightAfePicker) Name() string { return "least-inflight" }

// PickAfe returns the AFE with the fewest NumOutstanding among K
// randomly-drawn ready candidates.
func (p LeastInFlightAfePicker) PickAfe(ready []afeSnapshot) *afeHandle {
	return kChoiceMinCost(ready, p.RandomSubsetSize, func(s afeSnapshot) float64 {
		return float64(s.NumOutstanding)
	})
}

// LeastLatencyAfePicker picks the AFE with the lowest per-AFE e2e
// PeakEwma cost. Same K-choice partial Fisher-Yates as
// LeastInFlightAfePicker. Java parity: LeastLatencyPicker (uses
// AfeHandle.getE2eCost()).
type LeastLatencyAfePicker struct {
	RandomSubsetSize int
}

// NewLeastLatencyAfePicker constructs a LeastLatencyAfePicker.
func NewLeastLatencyAfePicker(randomSubsetSize int) *LeastLatencyAfePicker {
	return &LeastLatencyAfePicker{RandomSubsetSize: randomSubsetSize}
}

// Name returns "least-latency".
func (LeastLatencyAfePicker) Name() string { return "least-latency" }

// PickAfe returns the AFE with the smallest E2eCost among K randomly-
// drawn ready candidates.
func (p LeastLatencyAfePicker) PickAfe(ready []afeSnapshot) *afeHandle {
	return kChoiceMinCost(ready, p.RandomSubsetSize, func(s afeSnapshot) float64 {
		return s.E2eCost
	})
}

// kChoiceMinCost implements Java's partial-Fisher-Yates + min-cost
// selection over a snapshot slice. K is clamped to len(ready); K<=0 is
// treated as the default (defaultAfeRandomSubsetSize) when len(ready)
// >= default, else scan everything.
//
// The algorithm mutates a local copy of ready (swap-to-front) so the
// caller's snapshot is untouched. cost is called at most K times.
func kChoiceMinCost(ready []afeSnapshot, k int, cost func(afeSnapshot) float64) *afeHandle {
	n := len(ready)
	if n == 0 {
		return nil
	}
	if k <= 0 {
		k = defaultAfeRandomSubsetSize
	}
	if k > n {
		k = n
	}
	// Copy so we can swap-to-front without mutating the caller's slice.
	cand := make([]afeSnapshot, n)
	copy(cand, ready)

	var best *afeHandle
	bestCost := -1.0
	for i := 0; i < k; i++ {
		j := i + rand.IntN(n-i)
		picked := cand[j]
		c := cost(picked)
		if bestCost < 0 || c < bestCost {
			bestCost = c
			best = picked.Handle
		}
		cand[i], cand[j] = cand[j], cand[i]
	}
	return best
}
