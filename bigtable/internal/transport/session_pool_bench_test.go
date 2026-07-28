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

// Benchmarks for SessionPoolImpl.CheckoutSession hot path + the picker
// ring buffer that lives inside it. These are the paths where the
// pickHistory memmove regression (~24µs p99 jump) showed up in
// production before commit 1bea7c24e1 fixed it. Run with:
//
//   go test ./internal/transport/ -run=^$ -bench=BenchmarkCheckoutSession -benchmem
//   go test ./internal/transport/ -run=^$ -bench=BenchmarkRecordPickDecision -benchmem
//
// Compare across commits with benchstat:
//
//   git checkout main; go test -run=^$ -bench=. -benchmem -count=5 ./internal/transport/ > /tmp/old.txt
//   git checkout my-branch; go test -run=^$ -bench=. -benchmem -count=5 ./internal/transport/ > /tmp/new.txt
//   benchstat /tmp/old.txt /tmp/new.txt

// BenchmarkRecordPickDecision_WarmRing pre-fills the ring past capacity
// and then benchmarks. This is the mode where the old shift-based
// implementation memmoved ~500 events on every call — the exact bug
// that caused the p99 regression. If someone reintroduces an O(N)
// step, this benchmark blows up by 100×+ vs the cold benchmark.
func BenchmarkRecordPickDecision_WarmRing(b *testing.B) {
	p := newTestPool(b, 1, 10)
	d := PickDecision{Reason: "bench", Winner: afeID(1)}

	// Fill to cap so subsequent recordPickDecision calls exercise the
	// overwrite-and-advance path, not append.
	for i := 0; i < maxPickHistory; i++ {
		p.recordPickDecision(d, "bench")
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.recordPickDecision(d, "bench")
	}
}

// BenchmarkKChoiceMinCost measures the picker's per-call allocation
// budget. Regression guard against re-adding the defensive
// make([]afeSnapshot,n) + copy pair that the picker previously did on
// every call — profiling showed it costing ~4µs at steady-state QPS.
// Callers own the input slice; kChoiceMinCost is allowed to mutate it.
// The []PickCandidate result is retained by PickDecision (goes into
// pickHistory), so 1 alloc/op is the expected floor.
func BenchmarkKChoiceMinCost(b *testing.B) {
	// 8 ready AFEs — kChoiceMinCost samples snap.ID; no *afeHandle needed.
	template := make([]afeSnapshot, 8)
	for i := range template {
		template[i] = afeSnapshot{ID: afeID(i + 1)}
	}
	ready := make([]afeSnapshot, 8)
	cost := func(s afeSnapshot) float64 { return float64(s.NumOutstanding) }

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Callers pass a throwaway slice per pick; also required because
		// kChoiceMinCost mutates ready in place via swap-to-front.
		copy(ready, template)
		_, _, _ = kChoiceMinCost(ready, defaultAfeRandomSubsetSize, true, cost)
	}
}

// BenchmarkSnapshotPickHistory_Full measures the loadz-read path with
// a saturated ring. Snapshots run at each debug-page render, so a
// regression here surfaces as UI-tab-open latency rather than production
// vRPC latency — still worth guarding.
func BenchmarkSnapshotPickHistory_Full(b *testing.B) {
	p := newTestPool(b, 1, 10)
	d := PickDecision{Reason: "bench", Winner: afeID(1)}
	for i := 0; i < maxPickHistory; i++ {
		p.recordPickDecision(d, "bench")
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = p.snapshotPickHistory()
	}
}
