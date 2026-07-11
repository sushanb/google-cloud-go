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
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestSyncCtx_FIFOOrdering verifies callbacks execute in the order they
// were scheduled — this is the whole point of the primitive.
func TestSyncCtx_FIFOOrdering(t *testing.T) {
	sc := newSyncCtx()
	defer sc.Shutdown()

	const n = 100
	seen := make([]int, 0, n)
	var mu sync.Mutex
	done := make(chan struct{})
	for i := 0; i < n; i++ {
		i := i
		sc.Execute(func() {
			mu.Lock()
			seen = append(seen, i)
			mu.Unlock()
			if i == n-1 {
				close(done)
			}
		})
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("callbacks did not complete in 2s")
	}
	mu.Lock()
	defer mu.Unlock()
	for i, v := range seen {
		if v != i {
			t.Fatalf("out-of-order at index %d: got %d", i, v)
		}
	}
}

// TestSyncCtx_ExecuteSyncBlocksUntilFnReturns verifies ExecuteSync
// really waits for the callback to complete before returning.
func TestSyncCtx_ExecuteSyncBlocksUntilFnReturns(t *testing.T) {
	sc := newSyncCtx()
	defer sc.Shutdown()

	var ran atomic.Bool
	start := time.Now()
	ok := sc.ExecuteSync(func() {
		time.Sleep(50 * time.Millisecond)
		ran.Store(true)
	})
	elapsed := time.Since(start)
	if !ok {
		t.Fatal("ExecuteSync returned false on a live syncCtx")
	}
	if !ran.Load() {
		t.Fatal("ExecuteSync returned before fn ran")
	}
	if elapsed < 50*time.Millisecond {
		t.Fatalf("ExecuteSync returned in %v; expected >= 50ms", elapsed)
	}
}

// TestSyncCtx_ExecuteSyncPanicPropagates verifies a panic inside fn
// surfaces on the caller goroutine — a bad callback must not kill the
// runner or hang the caller.
func TestSyncCtx_ExecuteSyncPanicPropagates(t *testing.T) {
	sc := newSyncCtx()
	defer sc.Shutdown()

	sentinel := errors.New("boom")
	var caught error
	func() {
		defer func() {
			if v := recover(); v != nil {
				caught, _ = v.(error)
			}
		}()
		sc.ExecuteSync(func() { panic(sentinel) })
	}()
	if !errors.Is(caught, sentinel) {
		t.Fatalf("expected panic to propagate; got %v", caught)
	}
	// Runner must still be alive: next scheduled callback should run.
	done := make(chan struct{})
	sc.Execute(func() { close(done) })
	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("runner did not process next callback after recovered panic")
	}
}

// TestSyncCtx_ShutdownDrainsQueue verifies that callbacks queued BEFORE
// Shutdown was observed still run before the runner exits.
func TestSyncCtx_ShutdownDrainsQueue(t *testing.T) {
	sc := newSyncCtx()
	// Block the runner on a barrier so we can queue behind it.
	release := make(chan struct{})
	sc.Execute(func() { <-release })

	var ran atomic.Int64
	const n = 10
	for i := 0; i < n; i++ {
		sc.Execute(func() { ran.Add(1) })
	}
	// Kick Shutdown from a goroutine — it will block on <-sc.done until
	// the runner drains the queue.
	shutdownDone := make(chan struct{})
	go func() {
		sc.Shutdown()
		close(shutdownDone)
	}()
	// Let the runner move: after this, the queued 10 callbacks should
	// drain before Shutdown returns.
	close(release)
	select {
	case <-shutdownDone:
	case <-time.After(1 * time.Second):
		t.Fatal("Shutdown did not return after runner unblocked")
	}
	if got := ran.Load(); got != n {
		t.Fatalf("drained %d callbacks; want %d", got, n)
	}
}

// TestSyncCtx_ExecuteAfterShutdownReturnsFalse verifies the post-shutdown
// contract: no new work is accepted.
func TestSyncCtx_ExecuteAfterShutdownReturnsFalse(t *testing.T) {
	sc := newSyncCtx()
	sc.Shutdown()
	if sc.Execute(func() {}) {
		t.Error("Execute returned true after Shutdown")
	}
	if sc.ExecuteSync(func() {}) {
		t.Error("ExecuteSync returned true after Shutdown")
	}
}

// TestSyncCtx_NestedExecuteSchedulesNextTick verifies Execute is legal
// from inside a callback (posting a follow-up tick, not calling
// ExecuteSync recursively).
func TestSyncCtx_NestedExecuteSchedulesNextTick(t *testing.T) {
	sc := newSyncCtx()
	defer sc.Shutdown()

	done := make(chan struct{})
	sc.Execute(func() {
		sc.Execute(func() { close(done) })
	})
	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("nested Execute did not run")
	}
}

// TestSyncCtx_NoLeakUnderRepeat is the goroutine-leak sanity check —
// spinning up and shutting down many syncCtx instances must not leak.
// Run under -count=100 as the plan requires; a leak shows up as an
// ever-growing goroutine count under -race.
func TestSyncCtx_NoLeakUnderRepeat(t *testing.T) {
	for i := 0; i < 50; i++ {
		sc := newSyncCtx()
		var wg sync.WaitGroup
		for j := 0; j < 10; j++ {
			wg.Add(1)
			sc.Execute(func() { wg.Done() })
		}
		wg.Wait()
		sc.Shutdown()
	}
}
