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

package session

import (
	"math/bits"
	"sync"
	"sync/atomic"
	"time"
)

// dispatchLatencyHist mirrors transport.latencyHist (unexported there):
// a lock-free log2-bucket histogram used to summarize dispatch-level
// timings without pulling any allocations onto the hot path.
type dispatchLatencyHist struct {
	buckets [40]atomic.Uint64
}

func (h *dispatchLatencyHist) record(d time.Duration) {
	if d <= 0 {
		return
	}
	ns := uint64(d)
	b := bits.Len64(ns) - 1
	if b < 0 {
		b = 0
	}
	if b >= len(h.buckets) {
		b = len(h.buckets) - 1
	}
	h.buckets[b].Add(1)
}

func (h *dispatchLatencyHist) snapshot() (p50, p95, p99 time.Duration, n uint64) {
	var counts [40]uint64
	for i := range h.buckets {
		counts[i] = h.buckets[i].Load()
		n += counts[i]
	}
	if n == 0 {
		return
	}
	p50 = interpDispatchLatencyPercentile(counts[:], n, 50)
	p95 = interpDispatchLatencyPercentile(counts[:], n, 95)
	p99 = interpDispatchLatencyPercentile(counts[:], n, 99)
	return
}

func interpDispatchLatencyPercentile(counts []uint64, n uint64, pct float64) time.Duration {
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
	for i := len(counts) - 1; i >= 0; i-- {
		if counts[i] > 0 {
			return time.Duration(uint64(1) << (i + 1))
		}
	}
	return 0
}

// dispatchMethodMetrics carries per-method dispatch timings. Same shape
// per method so DispatchTimings() can render them in a uniform table.
type dispatchMethodMetrics struct {
	totalHist   dispatchLatencyHist // full dispatch() wall-clock
	poolGetHist dispatchLatencyHist // spec.pool.get() (first-call lazy open)
	chainedHist dispatchLatencyHist // retry-loop total (chained(...))
	calls       atomic.Int64
	poolGetMiss atomic.Int64 // pool.get returned nil, method never invoked
}

// dispatchMetrics is per-sessionClient. Keyed by method label so we
// don't fabricate an enum for methods that come and go. Reads on the
// hot path go through methodMetrics with mu-write only on first insert
// for a given method.
type dispatchMetrics struct {
	mu      sync.RWMutex
	methods map[string]*dispatchMethodMetrics
}

func newDispatchMetrics() *dispatchMetrics {
	return &dispatchMetrics{methods: make(map[string]*dispatchMethodMetrics)}
}

// forMethod returns (allocating on first-use) the per-method metrics
// bucket. Two-phase lock: RLock fast-path, Lock on miss with a re-check.
func (m *dispatchMetrics) forMethod(method string) *dispatchMethodMetrics {
	if m == nil {
		return nil
	}
	m.mu.RLock()
	mm, ok := m.methods[method]
	m.mu.RUnlock()
	if ok {
		return mm
	}
	m.mu.Lock()
	if mm, ok = m.methods[method]; !ok {
		mm = &dispatchMethodMetrics{}
		m.methods[method] = mm
	}
	m.mu.Unlock()
	return mm
}

// DispatchMethodTimings is the per-method row of the DispatchTimings
// snapshot. Populated for every method dispatch has ever been called
// with on this Client.
type DispatchMethodTimings struct {
	Method       string
	Calls        int64
	PoolGetMiss  int64
	TotalP50     time.Duration
	TotalP95     time.Duration
	TotalP99     time.Duration
	TotalN       uint64
	PoolGetP50   time.Duration
	PoolGetP95   time.Duration
	PoolGetP99   time.Duration
	PoolGetN     uint64
	ChainedP50   time.Duration
	ChainedP95   time.Duration
	ChainedP99   time.Duration
	ChainedN     uint64
}

// snapshot returns one DispatchMethodTimings per known method,
// ordered by method label for stable UI rendering.
func (m *dispatchMetrics) snapshot() []DispatchMethodTimings {
	if m == nil {
		return nil
	}
	m.mu.RLock()
	names := make([]string, 0, len(m.methods))
	rows := make(map[string]*dispatchMethodMetrics, len(m.methods))
	for k, v := range m.methods {
		names = append(names, k)
		rows[k] = v
	}
	m.mu.RUnlock()
	sortStrings(names)
	out := make([]DispatchMethodTimings, 0, len(names))
	for _, name := range names {
		mm := rows[name]
		tp50, tp95, tp99, tn := mm.totalHist.snapshot()
		gp50, gp95, gp99, gn := mm.poolGetHist.snapshot()
		cp50, cp95, cp99, cn := mm.chainedHist.snapshot()
		out = append(out, DispatchMethodTimings{
			Method:      name,
			Calls:       mm.calls.Load(),
			PoolGetMiss: mm.poolGetMiss.Load(),
			TotalP50:    tp50, TotalP95: tp95, TotalP99: tp99, TotalN: tn,
			PoolGetP50: gp50, PoolGetP95: gp95, PoolGetP99: gp99, PoolGetN: gn,
			ChainedP50: cp50, ChainedP95: cp95, ChainedP99: cp99, ChainedN: cn,
		})
	}
	return out
}

// sortStrings is a tiny insertion sort; the method set stays small (a
// handful of RPC labels) so avoiding the sort package keeps this file
// dependency-free relative to the rest of the session package.
func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j-1] > s[j]; j-- {
			s[j-1], s[j] = s[j], s[j-1]
		}
	}
}
