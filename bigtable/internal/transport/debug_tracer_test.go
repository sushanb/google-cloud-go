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
	"sync"
	"testing"
)

// TestDebugTag_RecordCounts verifies the in-memory counter tracks every
// recordDebugTag call, including repeats, and snapshotDebugTagCounts returns an
// independent snapshot (mutating the returned map doesn't affect state).
func TestDebugTag_RecordCounts(t *testing.T) {
	resetDebugTagCountsForTest()
	recordDebugTag("tag_a")
	recordDebugTag("tag_a")
	recordDebugTagAt(lvl.Error, "tag_b")

	got := snapshotDebugTagCounts()
	if got["tag_a"] != 2 {
		t.Errorf("tag_a: got %d, want 2", got["tag_a"])
	}
	if got["tag_b"] != 1 {
		t.Errorf("tag_b: got %d, want 1", got["tag_b"])
	}

	// Mutation on the returned map must not bleed into the tracer.
	got["tag_a"] = 999
	if again := snapshotDebugTagCounts(); again["tag_a"] != 2 {
		t.Errorf("snapshot leaked mutation: got %d, want 2", again["tag_a"])
	}
}

// TestDebugTag_LevelFloor verifies emissions below the runtime floor are
// dropped in-process. Restores the default floor at the end so downstream
// tests aren't polluted.
func TestDebugTag_LevelFloor(t *testing.T) {
	resetDebugTagCountsForTest()
	t.Cleanup(func() { setDebugTagLevelFloor(lvl.Warn) })

	setDebugTagLevelFloor(lvl.Error)
	recordDebugTag("warn_below_floor")
	recordDebugTagAt(lvl.Error, "error_at_floor")

	got := snapshotDebugTagCounts()
	if _, present := got["warn_below_floor"]; present {
		t.Errorf("warn_below_floor should have been dropped, got %v", got)
	}
	if got["error_at_floor"] != 1 {
		t.Errorf("error_at_floor: got %d, want 1", got["error_at_floor"])
	}
}

// TestDebugTag_AssertPassAndFail verifies both assert forms
// (formatted and format-free) return the predicate result and
// increment the counter only on the failing branch.
func TestDebugTag_AssertPassAndFail(t *testing.T) {
	resetDebugTagCountsForTest()

	// Formatted form — captures diagnostic context in the log message.
	if ok := assertDebugTagf(true, "assert_pass_f", "should not fire"); !ok {
		t.Errorf("assertDebugTagf(true) returned false")
	}
	if ok := assertDebugTagf(false, "assert_fail_f", "context=%s", "test"); ok {
		t.Errorf("assertDebugTagf(false) returned true")
	}

	// Format-free form — the tag name is the whole message.
	if ok := assertDebugTag(true, "assert_pass"); !ok {
		t.Errorf("assertDebugTag(true) returned false")
	}
	if ok := assertDebugTag(false, "assert_fail"); ok {
		t.Errorf("assertDebugTag(false) returned true")
	}

	got := snapshotDebugTagCounts()
	for _, name := range []string{"assert_pass_f", "assert_pass"} {
		if _, present := got[name]; present {
			t.Errorf("%s fired despite predicate holding: %v", name, got)
		}
	}
	for _, name := range []string{"assert_fail_f", "assert_fail"} {
		if got[name] != 1 {
			t.Errorf("%s: got %d, want 1", name, got[name])
		}
	}
}

// TestDebugTag_ConcurrentEmission is a smoke test for the RWMutex-guarded
// map: many goroutines hammering the same tag should tally exactly, no
// dropped increments, no data race under -race.
func TestDebugTag_ConcurrentEmission(t *testing.T) {
	resetDebugTagCountsForTest()

	const goroutines = 16
	const perGoroutine = 500
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < perGoroutine; j++ {
				recordDebugTag("hot_tag")
			}
		}()
	}
	wg.Wait()

	want := int64(goroutines * perGoroutine)
	if got := snapshotDebugTagCounts()["hot_tag"]; got != want {
		t.Errorf("hot_tag: got %d, want %d", got, want)
	}
}
