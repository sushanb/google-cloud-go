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

// PickCandidate is one AFE the picker considered during a K-choice draw,
// with the cost value the picker's decision rule used to score it.
// Cost's interpretation depends on the picker in play: NumOutstanding
// (int as float) for LeastInFlight, e2e PeakEwma nanos for LeastLatency,
// 0 for Simple (which ignores cost).
type PickCandidate struct {
	AfeID afeID
	Cost  float64
}

// PickDecision captures what candidates a picker sampled, which one won,
// and why. Populated on every PickAfe call so operators can trace picker
// reasoning through loadz without re-running the pick.
type PickDecision struct {
	// Candidates is the K sampled AFEs (K == 1 for SimplePicker,
	// otherwise K == min(RandomSubsetSize, len(ready)) via partial
	// Fisher-Yates). Empty when no ready AFE existed.
	Candidates []PickCandidate
	// Winner is the AFE the picker returned. Zero when ready was empty.
	Winner afeID
	// Reason is a short lower-kebab tag identifying the decision rule
	// ("uniform-random" / "min-inflight" / "min-latency" /
	// "no-candidates"). Machine-readable; loadz maps to prose.
	Reason string
}

// AfePicker picks one AFE from a snapshot of ready buckets AND returns
// the decision metadata for loadz. Callers use the *afeHandle; the
// PickDecision is passed to SessionPoolImpl.recordPickDecision.
//
// Returning nil handle means "no AFE eligible" — the pool treats that
// the same as len(ready) == 0 (park the caller on freeSignal, kick
// scale-up).
type AfePicker interface {
	PickAfe(ready []afeSnapshot) (*afeHandle, PickDecision)
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
func (SimpleAfePicker) PickAfe(ready []afeSnapshot) (*afeHandle, PickDecision) {
	if len(ready) == 0 {
		return nil, PickDecision{Reason: "no-candidates"}
	}
	winner := ready[rand.IntN(len(ready))]
	return winner.Handle, PickDecision{
		Candidates: []PickCandidate{{AfeID: winner.Handle.ID(), Cost: 0}},
		Winner:     winner.Handle.ID(),
		Reason:     "uniform-random",
	}
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
func (p LeastInFlightAfePicker) PickAfe(ready []afeSnapshot) (*afeHandle, PickDecision) {
	winner, cands := kChoiceMinCost(ready, p.RandomSubsetSize, func(s afeSnapshot) float64 {
		return float64(s.NumOutstanding)
	})
	return decisionFor(winner, cands, "min-inflight")
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
func (p LeastLatencyAfePicker) PickAfe(ready []afeSnapshot) (*afeHandle, PickDecision) {
	winner, cands := kChoiceMinCost(ready, p.RandomSubsetSize, func(s afeSnapshot) float64 {
		return s.E2eCost
	})
	return decisionFor(winner, cands, "min-latency")
}

// decisionFor packages kChoiceMinCost's return into a PickDecision.
func decisionFor(winner *afeHandle, cands []PickCandidate, reason string) (*afeHandle, PickDecision) {
	if winner == nil {
		return nil, PickDecision{Reason: "no-candidates"}
	}
	return winner, PickDecision{
		Candidates: cands,
		Winner:     winner.ID(),
		Reason:     reason,
	}
}

// kChoiceMinCost implements Java's partial-Fisher-Yates + min-cost
// selection over a snapshot slice. K is clamped to len(ready); K<=0 is
// treated as the default (defaultAfeRandomSubsetSize) when len(ready)
// >= default, else scan everything.
//
// The algorithm mutates ready in place (swap-to-front). Callers must
// pass a throwaway slice — every production call site produces one via
// sessionList.ReadyAfes(), which allocates a fresh copy per call. cost
// is invoked at most K times. Returns the winner plus the list of
// sampled candidates (with their costs) so callers can build a
// PickDecision for loadz.
//
// The previous implementation allocated a defensive copy of ready per
// call; profiling showed it costing ~4µs at the workload's steady-state
// QPS since the picker runs on every CheckoutSession. Removed because
// the caller doesn't need ready preserved.
func kChoiceMinCost(ready []afeSnapshot, k int, cost func(afeSnapshot) float64) (*afeHandle, []PickCandidate) {
	n := len(ready)
	if n == 0 {
		return nil, nil
	}
	if k <= 0 {
		k = defaultAfeRandomSubsetSize
	}
	if k > n {
		k = n
	}

	sampled := make([]PickCandidate, 0, k)
	var best *afeHandle
	bestCost := -1.0
	for i := 0; i < k; i++ {
		j := i + rand.IntN(n-i)
		picked := ready[j]
		c := cost(picked)
		sampled = append(sampled, PickCandidate{AfeID: picked.Handle.ID(), Cost: c})
		if bestCost < 0 || c < bestCost {
			bestCost = c
			best = picked.Handle
		}
		ready[i], ready[j] = ready[j], ready[i]
	}
	return best, sampled
}
