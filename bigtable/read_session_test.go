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

package bigtable

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/api/option"
)

func TestReadSessionSandbox(t *testing.T) {
	// This test connects to the sandbox endpoint and performs a ReadRow via session-based transport.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	project := "autonomous-mote-782"
	instance := "test-sushanb"
	table := "sushanb"
	endpoint := "test-bigtable.sandbox.googleapis.com:443"

	cfg := ClientConfig{
		EnableSessionPool: true,
	}

	// Create client configured to use sessions & the sandbox endpoint
	client, err := NewClientWithConfig(ctx, project, instance, cfg, option.WithEndpoint(endpoint))
	if err != nil {
		t.Fatalf("failed to create bigtable client: %v", err)
	}
	defer client.Close()

	tbl := client.OpenTable(table)
	rowKey := "myrow-0"

	t.Logf("Waiting for session pool to warm up...")
	time.Sleep(3 * time.Second)

	t.Logf("Reading row %q from table %q via session pool before write...", rowKey, table)
	row, err := tbl.ReadRow(ctx, rowKey)
	if err != nil {
		t.Fatalf("ReadRow before write failed: %v", err)
	}
	if row != nil {
		t.Logf("Successfully read row %q before write:", rowKey)
		for fam, items := range row {
			for _, item := range items {
				t.Logf("  Family: %s, Column: %s, Value: %s", fam, item.Column, string(item.Value))
			}
		}
	}

	t.Logf("Applying mutation write via session pool to row %q...", rowKey)
	mut := NewMutation()
	mut.Set("cf12", "colq1", ServerTime, []byte("val-applied-vrpc"))
	if err := tbl.Apply(ctx, rowKey, mut); err != nil {
		t.Fatalf("Apply mutation failed: %v", err)
	}
	t.Logf("Successfully applied mutation write.")

	t.Logf("Reading row %q from table %q via session pool after write...", rowKey, table)
	rowAfter, err := tbl.ReadRow(ctx, rowKey)
	if err != nil {
		t.Fatalf("ReadRow after write failed: %v", err)
	}
	if rowAfter == nil {
		t.Fatalf("Row %q not found after write", rowKey)
	}

	t.Logf("Successfully read row %q after write:", rowKey)
	for fam, items := range rowAfter {
		for _, item := range items {
			t.Logf("  Family: %s, Column: %s, Value: %s", fam, item.Column, string(item.Value))
		}
	}
}

func TestHighQpsSessionSandbox(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 11*time.Minute)
	defer cancel()

	project := "autonomous-mote-782"
	instance := "test-sushanb"
	table := "sushanb"
	endpoint := "test-bigtable.sandbox.googleapis.com:443"

	cfg := ClientConfig{
		EnableSessionPool: true,
		SessionPoolMin:    3,
		SessionPoolMax:    5,
	}

	client, err := NewClientWithConfig(ctx, project, instance, cfg, option.WithEndpoint(endpoint))
	if err != nil {
		t.Fatalf("failed to create bigtable client: %v", err)
	}
	defer client.Close()

	tbl := client.OpenTable(table)

	t.Logf("Waiting for session pool to warm up before high QPS test...")
	time.Sleep(3 * time.Second)

	concurrency := 10
	testDuration := 10 * time.Minute
	endTime := time.Now().Add(testDuration)

	var successWrites int64
	var successReads int64
	var failedWrites int64
	var failedReads int64

	var wg sync.WaitGroup
	t.Logf("Starting 10-minute high QPS sandbox test with %d concurrent workers...", concurrency)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			counter := 0
			for time.Now().Before(endTime) {
				counter++
				rowKey := fmt.Sprintf("sandbox-%d-%d", workerID, counter)
				mut := NewMutation()
				mut.Set("cf12", "colq1", ServerTime, []byte(fmt.Sprintf("val-worker-%d-%d", workerID, counter)))

				// Perform write
				if err := tbl.Apply(ctx, rowKey, mut); err != nil {
					atomic.AddInt64(&failedWrites, 1)
					fmt.Printf(">>> ERROR [worker-%d]: write failed: %v <<<\n", workerID, err)
				} else {
					atomic.AddInt64(&successWrites, 1)
				}

				// Perform read
				_, err := tbl.ReadRow(ctx, rowKey)
				if err != nil {
					atomic.AddInt64(&failedReads, 1)
					fmt.Printf(">>> ERROR [worker-%d]: read failed: %v <<<\n", workerID, err)
				} else {
					atomic.AddInt64(&successReads, 1)
				}

				time.Sleep(50 * time.Millisecond)
			}
		}(i)
	}

	// Background stats logger
	doneChan := make(chan struct{})
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		start := time.Now()

		for {
			select {
			case <-doneChan:
				return
			case <-ticker.C:
				sw := atomic.LoadInt64(&successWrites)
				sr := atomic.LoadInt64(&successReads)
				fw := atomic.LoadInt64(&failedWrites)
				fr := atomic.LoadInt64(&failedReads)
				elapsed := time.Since(start)
				qps := float64(sw+sr) / elapsed.Seconds()
				fmt.Printf(">>> STATS [%.1fs elapsed]: Success (W:%d, R:%d), Failed (W:%d, R:%d), QPS: %.2f <<<\n", elapsed.Seconds(), sw, sr, fw, fr, qps)
			}
		}
	}()

	wg.Wait()
	close(doneChan)

	finalSW := atomic.LoadInt64(&successWrites)
	finalSR := atomic.LoadInt64(&successReads)
	finalFW := atomic.LoadInt64(&failedWrites)
	finalFR := atomic.LoadInt64(&failedReads)
	t.Logf("10-minute high QPS test completed! Successful Writes: %d, Successful Reads: %d, Failed Writes: %d, Failed Reads: %d", finalSW, finalSR, finalFW, finalFR)
	if finalFW > 0 || finalFR > 0 {
		t.Errorf("Test had failed operations!")
	}
}

func TestSequentialReads(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Second)
	defer cancel()

	project := "autonomous-mote-782"
	instance := "test-sushanb"
	table := "sushanb"
	endpoint := "test-bigtable.sandbox.googleapis.com:443"

	cfg := ClientConfig{
		EnableSessionPool: false,
	}

	client, err := NewClientWithConfig(ctx, project, instance, cfg, option.WithEndpoint(endpoint))
	if err != nil {
		t.Fatalf("failed to nn bigtable client: %v", err)
	}
	defer client.Close()

	tbl := client.OpenTable(table)
	rowKey := "myrow-0"

	t.Logf("Waiting for session pool to warm up...")
	time.Sleep(3 * time.Second)

	for i := 0; i < 100; i++ {
		t.Logf("Sequential Read %d/10 for row %q...", i+1, rowKey)
		t.Logf("Sleeping for 1s...")
		time.Sleep(1 * time.Second)
		row, err := tbl.ReadRow(ctx, rowKey)
		if err != nil {
			t.Fatalf("Read %d failed: %v", i+1, err)
		}
		if row != nil {
			t.Logf("Read %d successfully. Columns read: %d", i+1, len(row["cf12"]))
		} else {
			t.Logf("Read %d: row %q not found", i+1, rowKey)
		}
	}
}

// TestReadNonExistentRow probes a guaranteed-not-present row and logs
// the caller-visible shape (row==nil / len / Key()) so parity between
// classic and session paths can be diffed. Which path is exercised is
// controlled by the CBT_FORCE_SESSION env var:
//   unset / anything-but-true → classic path (EnableSessionPool=false)
//   =true                     → session path (EnableSessionPool=true)
// Run once each and diff the logs to validate the protoRowToRow
// "empty result → nil" fix end-to-end.
func TestReadNonExistentRow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	project := "autonomous-mote-782"
	instance := "test-sushanb"
	table := "sushanb"
	endpoint := "test-bigtable.sandbox.googleapis.com:443"

	useSession, _ := strconv.ParseBool(os.Getenv("CBT_FORCE_SESSION"))
	path := "classic"
	if useSession {
		path = "session"
	}
	t.Logf("path=%s (CBT_FORCE_SESSION=%q)", path, os.Getenv("CBT_FORCE_SESSION"))

	// Row key that cannot collide with any test data. Timestamp keeps
	// it fresh across runs; the "__missing-" prefix flags intent.
	missingKey := fmt.Sprintf("__missing-row-%d", time.Now().UnixNano())
	t.Logf("Probing non-existent row key: %q", missingKey)

	client, err := NewClientWithConfig(ctx, project, instance,
		ClientConfig{EnableSessionPool: useSession},
		option.WithEndpoint(endpoint))
	if err != nil {
		t.Fatalf("failed to create %s client: %v", path, err)
	}
	defer client.Close()

	if useSession {
		t.Logf("Waiting 3s for session pool to warm...")
		time.Sleep(3 * time.Second)
	}

	row, err := client.OpenTable(table).ReadRow(ctx, missingKey)
	if err != nil {
		// Diagnostic dump — surface the actual per-session close reasons
		// so we can distinguish "circuit tripped from GoAway churn" from
		// "server rejected the vRPC / abnormal close".
		if p := client.SessionDebug(); p != nil {
			for _, snap := range p.Snapshot() {
				t.Logf("pool=%q CloseReasons=%v Opened=%d Closed=%d",
					snap.Name, snap.CloseReasons, snap.SessionsOpened, snap.SessionsClosed)
				for _, s := range snap.Sessions {
					t.Logf("  sess=%s state=%s CloseReason=%q OkRpcs=%d ErrorRpcs=%d events=%d",
						s.LogName, s.State, s.CloseReason, s.OkRpcs, s.ErrorRpcs, len(s.RecentEvents))
					for _, e := range s.RecentEvents {
						t.Logf("    event kind=%s %s", e.Kind, e.Message)
					}
				}
			}
		}
		t.Fatalf("%s ReadRow failed: %v", path, err)
	}

	t.Logf("%s : row==nil=%v, len=%d, Key()=%q", path, row == nil, len(row), row.Key())

	if row != nil {
		t.Errorf("%s ReadRow returned non-nil for missing row: %#v", path, row)
	}
}

// TestReadRow_FilteredToEmpty seeds a row and reads it back with three
// filters designed to strip all cells server-side. Path (classic vs
// session) is chosen by CBT_FORCE_SESSION — run once each and diff the
// per-filter output to compare paths.
//
// The seed row key is derived from a stable-across-runs env var
// FILTER_PROBE_KEY when set; otherwise a fresh timestamp key is used
// and re-seeded on each run (fine because we're only comparing the
// per-run observable shape, not persistent state).
func TestReadRow_FilteredToEmpty(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	project := "autonomous-mote-782"
	instance := "test-sushanb"
	table := "sushanb"
	endpoint := "test-bigtable.sandbox.googleapis.com:443"

	useSession, _ := strconv.ParseBool(os.Getenv("CBT_FORCE_SESSION"))
	path := "classic"
	if useSession {
		path = "session"
	}
	rowKey := os.Getenv("FILTER_PROBE_KEY")
	if rowKey == "" {
		rowKey = fmt.Sprintf("__filter-probe-%d", time.Now().UnixNano())
	}
	t.Logf("path=%s (CBT_FORCE_SESSION=%q) rowKey=%q", path, os.Getenv("CBT_FORCE_SESSION"), rowKey)

	client, err := NewClientWithConfig(ctx, project, instance,
		ClientConfig{EnableSessionPool: useSession},
		option.WithEndpoint(endpoint))
	if err != nil {
		t.Fatalf("%s client: %v", path, err)
	}
	defer client.Close()

	if useSession {
		t.Logf("Waiting 3s for session pool warm...")
		time.Sleep(3 * time.Second)
	}

	tbl := client.OpenTable(table)

	// Seed the row (idempotent — mutation with the same value is safe
	// to re-apply). Guarantees exactly one cell exists pre-filter.
	{
		mut := NewMutation()
		mut.Set("cf12", "colq1", ServerTime, []byte("filter-probe-val"))
		if err := tbl.Apply(ctx, rowKey, mut); err != nil {
			t.Fatalf("%s seed Apply: %v", path, err)
		}
	}

	// Sanity check: unfiltered read must return the seeded row.
	if r, err := tbl.ReadRow(ctx, rowKey); err != nil || r == nil {
		t.Fatalf("%s sanity: err=%v row==nil=%v", path, err, r == nil)
	}
	t.Logf("%s sanity: seeded row is visible (no filter).", path)

	// Filters that MUST strip all cells server-side.
	filters := []struct {
		name   string
		filter Filter
	}{
		{"ColumnFilter(__nonexistent_col__)", ColumnFilter("__nonexistent_col__")},
		{"FamilyFilter(__nonexistent_family__)", FamilyFilter("__nonexistent_family__")},
		{"ValueFilter(^__no_such_value_ever__$)", ValueFilter("^__no_such_value_ever__$")},
	}

	for _, f := range filters {
		t.Run(f.name, func(t *testing.T) {
			r, err := tbl.ReadRow(ctx, rowKey, RowFilter(f.filter))
			t.Logf("%s : err=%v row==nil=%v len=%d Key()=%q", path, err, r == nil, len(r), r.Key())
			// Both paths must return nil for filter-strips-everything to
			// match classic Table.ReadRow's not-found contract. Errors
			// (like NotFound on unknown families) are the server's
			// choice and must propagate verbatim on either path.
			if err == nil && r != nil {
				t.Errorf("%s got non-nil row for filter-strips-everything: %#v", path, r)
			}
		})
	}
}

// TestMutateRow exercises Apply through both classic and session paths
// via CBT_FORCE_SESSION. Subtests probe: happy path with ServerTime,
// happy path with explicit timestamp, error surface for unknown column
// family, empty mutation. Run once per path and diff the outputs.
func TestMutateRow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	project := "autonomous-mote-782"
	instance := "test-sushanb"
	table := "sushanb"
	endpoint := "test-bigtable.sandbox.googleapis.com:443"

	useSession, _ := strconv.ParseBool(os.Getenv("CBT_FORCE_SESSION"))
	path := "classic"
	if useSession {
		path = "session"
	}
	t.Logf("path=%s (CBT_FORCE_SESSION=%q)", path, os.Getenv("CBT_FORCE_SESSION"))

	client, err := NewClientWithConfig(ctx, project, instance,
		ClientConfig{EnableSessionPool: useSession},
		option.WithEndpoint(endpoint))
	if err != nil {
		t.Fatalf("%s client: %v", path, err)
	}
	defer client.Close()

	if useSession {
		t.Logf("Waiting 3s for session pool warm...")
		time.Sleep(3 * time.Second)
	}

	tbl := client.OpenTable(table)
	nowSuffix := time.Now().UnixNano()

	t.Run("HappyPath_ServerTime", func(t *testing.T) {
		rowKey := fmt.Sprintf("__mut-servertime-%d", nowSuffix)
		mut := NewMutation()
		mut.Set("cf12", "colq_st", ServerTime, []byte("val-server-time"))
		start := time.Now()
		err := tbl.Apply(ctx, rowKey, mut)
		t.Logf("%s : Apply(ServerTime) err=%v dur=%v", path, err, time.Since(start))
		if err != nil {
			t.Fatalf("Apply(ServerTime) unexpected err: %v", err)
		}
		// Read-back
		r, rErr := tbl.ReadRow(ctx, rowKey)
		t.Logf("%s : ReadRow after ServerTime write err=%v row==nil=%v len=%d", path, rErr, r == nil, len(r))
		if rErr != nil || r == nil {
			t.Errorf("read-back after ServerTime failed: err=%v row==nil=%v", rErr, r == nil)
		}
	})

	t.Run("HappyPath_ExplicitTimestamp", func(t *testing.T) {
		rowKey := fmt.Sprintf("__mut-explicit-%d", nowSuffix)
		explicitTs := Time(time.Now())
		mut := NewMutation()
		mut.Set("cf12", "colq_ex", explicitTs, []byte("val-explicit"))
		start := time.Now()
		err := tbl.Apply(ctx, rowKey, mut)
		t.Logf("%s : Apply(explicit ts=%v) err=%v dur=%v", path, explicitTs, err, time.Since(start))
		if err != nil {
			t.Fatalf("Apply(explicit) unexpected err: %v", err)
		}
		r, rErr := tbl.ReadRow(ctx, rowKey)
		t.Logf("%s : ReadRow after explicit write err=%v row==nil=%v len=%d", path, rErr, r == nil, len(r))
		if rErr != nil || r == nil {
			t.Errorf("read-back after explicit ts failed: err=%v row==nil=%v", rErr, r == nil)
		}
	})

	t.Run("Error_NonExistentColumnFamily", func(t *testing.T) {
		rowKey := fmt.Sprintf("__mut-badfam-%d", nowSuffix)
		mut := NewMutation()
		mut.Set("__nonexistent_family__", "colq", ServerTime, []byte("val"))
		err := tbl.Apply(ctx, rowKey, mut)
		t.Logf("%s : Apply(bad-family) err=%v", path, err)
		if err == nil {
			t.Fatalf("expected error for unknown column family, got nil")
		}
		// Full error dump so we can see the vRPC-transport leak on session
		// path (parallel to the ReadRow FamilyFilter finding).
		t.Logf("%s : err.Error() = %q", path, err.Error())
		t.Logf("%s : %%T = %T", path, err)
	})

	t.Run("EmptyMutation", func(t *testing.T) {
		rowKey := fmt.Sprintf("__mut-empty-%d", nowSuffix)
		mut := NewMutation() // no ops
		err := tbl.Apply(ctx, rowKey, mut)
		t.Logf("%s : Apply(empty mutation) err=%v", path, err)
		// Behavior may be "silently succeed" or "rejected by server" — log,
		// don't assert. The point is to compare across paths.
	})
}

func TestDequentialReadsParallel(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	project := "autonomous-mote-782"
	instance := "test-sushanb"
	table := "sushanb"
	endpoint := "test-bigtable.sandbox.googleapis.com:443"

	cfg := ClientConfig{
		EnableSessionPool: true,
	}

	client, err := NewClientWithConfig(ctx, project, instance, cfg, option.WithEndpoint(endpoint))
	if err != nil {
		t.Fatalf("failed to create bigtable client: %v", err)
	}
	defer client.Close()

	tbl := client.OpenTable(table)

	t.Logf("Waiting for session pool to warm up...")
	time.Sleep(3 * time.Second)

	var wg sync.WaitGroup
	for seed := 0; seed < 10; seed++ {
		wg.Add(1)
		go func(s int) {
			defer wg.Done()
			rowKey := fmt.Sprintf("myrow-%d", s)
			for i := 0; i < 10; i++ {
				t.Logf("Seed %d - Sequential Read %d/10 for row %q...", s, i+1, rowKey)
				row, err := tbl.ReadRow(ctx, rowKey)
				if err != nil {
					t.Errorf("Seed %d - Read %d failed: %v", s, i+1, err)
					return
				}
				if row != nil {
					t.Logf("Seed %d - Read %d successfully. Columns read: %d", s, i+1, len(row["cf12"]))
				} else {
					t.Logf("Seed %d - Read %d: row %q not found", s, i+1, rowKey)
				}
			}
		}(seed)
	}
	wg.Wait()
}
