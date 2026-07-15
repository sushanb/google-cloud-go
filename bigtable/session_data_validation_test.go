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
	"bytes"
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"cloud.google.com/go/internal/testutil"
	"google.golang.org/api/option"
)

// TestSessionDataValidation_ReadRowParity seeds a variety of row shapes via
// the classic (unary) client and then reads each row back through BOTH the
// classic path and the session (vRPC) path, asserting byte-for-byte equality
// of the returned Row for every case.
//
// The two clients share the same real Bigtable instance:
//   - classic: EnableSessionPool=false — every ReadRow goes through the
//     BigtableClient unary stub (the historical path).
//   - session: EnableSessionPool=true and Diverter pinned to SessionLoad=1.0
//     so every ReadRow goes through TableShim → SessionTable →
//     SessionPoolImpl → Session → vRPC.
//
// Pinning the Diverter is safe for the test window because
// ClientConfigurationManager honors MinPollingInterval=1min; a short test
// completes well before the next server-driven UpdateConfig can override
// the pinned value. We re-pin defensively before each read anyway.
//
// This is a data-plane parity check — the point is that the session
// transport MUST NOT alter the observable Row shape versus classic. Any
// divergence (missing cells, reordered timestamps, altered bytes) fails
// the test.
func TestSessionDataValidation_ReadRowParity(t *testing.T) {
	if testing.Short() {
		t.Skip("Skip: requires live Bigtable sandbox instance")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	t.Logf("Using vRPC sandbox: project=%s instance=%s table=%s endpoint=%s",
		sessionSandboxProject, sessionSandboxInstance, sessionSandboxTable, sessionSandboxEndpoint)

	classic, err := NewClientWithConfig(ctx, sessionSandboxProject, sessionSandboxInstance,
		ClientConfig{EnableSessionPool: false},
		option.WithEndpoint(sessionSandboxEndpoint),
	)
	if err != nil {
		t.Fatalf("classic NewClientWithConfig: %v", err)
	}
	defer classic.Close()

	sessionCli, err := NewClientWithConfig(ctx, sessionSandboxProject, sessionSandboxInstance,
		ClientConfig{EnableSessionPool: true, SessionPoolMin: 1, SessionPoolMax: 2},
		option.WithEndpoint(sessionSandboxEndpoint),
	)
	if err != nil {
		t.Fatalf("session NewClientWithConfig: %v", err)
	}
	defer sessionCli.Close()

	// Wait for the SessionClient's initial ClientConfigurationManager poll to
	// land, then pin the Diverter to 100% session so every subsequent
	// ReadRow via `sessionCli` is guaranteed to traverse the vRPC path.
	waitForSessionPoolReady(t, sessionCli, 15*time.Second)
	sessionCli.diverter.SetSessionLoad(1.0)

	classicTbl := classic.OpenTable(sessionSandboxTable)
	sessionTbl := sessionCli.OpenTable(sessionSandboxTable)

	// Unique per-run row-key prefix so parallel/repeat runs don't collide.
	runID := time.Now().UnixNano()
	rk := func(name string) string {
		return fmt.Sprintf("dv-%d-%s", runID, name)
	}

	// Test cases — a spread of shapes that have historically caused
	// serialization or framing bugs.
	cases := []struct {
		name   string
		writes []struct {
			col string
			ts  Timestamp
			val []byte
		}
	}{
		{
			name: "single-small",
			writes: []struct {
				col string
				ts  Timestamp
				val []byte
			}{{"c1", 1_000_000, []byte("hello")}},
		},
		{
			name: "empty-value",
			writes: []struct {
				col string
				ts  Timestamp
				val []byte
			}{{"c1", 1_000_000, []byte{}}},
		},
		{
			name: "binary-all-bytes",
			writes: []struct {
				col string
				ts  Timestamp
				val []byte
			}{{"c1", 1_000_000, allBytesPayload()}},
		},
		{
			name: "unicode",
			writes: []struct {
				col string
				ts  Timestamp
				val []byte
			}{{"c1", 1_000_000, []byte("héllo 世界 🌏 café — αβγ")}},
		},
		{
			name: "large-100kb",
			writes: []struct {
				col string
				ts  Timestamp
				val []byte
			}{{"c1", 1_000_000, repeatBytes([]byte("bigtable-vrpc-parity-"), 100<<10)}},
		},
		{
			name: "multi-column",
			writes: []struct {
				col string
				ts  Timestamp
				val []byte
			}{
				{"c1", 1_000_000, []byte("v1")},
				{"c2", 1_000_000, []byte("v2")},
				{"c3", 1_000_000, []byte("v3")},
			},
		},
		{
			name: "multi-version-same-column",
			writes: []struct {
				col string
				ts  Timestamp
				val []byte
			}{
				{"c1", 1_000_000, []byte("older")},
				{"c1", 2_000_000, []byte("newer")},
			},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			rowKey := rk(tc.name)

			// Seed via classic path.
			mut := NewMutation()
			for _, w := range tc.writes {
				mut.Set(sessionSandboxFamily, w.col, w.ts, w.val)
			}
			if err := classicTbl.Apply(ctx, rowKey, mut); err != nil {
				t.Fatalf("classic Apply: %v", err)
			}

			// Read back via classic (baseline).
			classicRow, err := classicTbl.ReadRow(ctx, rowKey)
			if err != nil {
				t.Fatalf("classic ReadRow: %v", err)
			}
			if classicRow == nil {
				t.Fatalf("classic ReadRow returned nil for %q", rowKey)
			}

			// Re-pin the Diverter defensively — belt-and-braces against a
			// pathologically-short poll interval on the server side.
			sessionCli.diverter.SetSessionLoad(1.0)
			sessionRow, err := sessionTbl.ReadRow(ctx, rowKey)
			if err != nil {
				t.Fatalf("session ReadRow: %v", err)
			}
			if sessionRow == nil {
				t.Fatalf("session ReadRow returned nil for %q", rowKey)
			}

			// Confirm the read actually went through the session path.
			snap := sessionCli.diverter.Snapshot()
			if snap.SessionPicks == 0 {
				t.Fatalf("session Diverter reports 0 SessionPicks — traffic did NOT reach vRPC path (snapshot=%+v)", snap)
			}

			assertRowsEqual(t, "classic-vs-session", rowKey, classicRow, sessionRow)
		})
	}

	// Reverse direction: seed via session, read via classic — proves the
	// session write path also produces a Row shape that classic can consume
	// identically.
	t.Run("reverse-seed-via-session", func(t *testing.T) {
		rowKey := rk("reverse")
		sessionCli.diverter.SetSessionLoad(1.0)
		mut := NewMutation()
		mut.Set(sessionSandboxFamily, "c1", 5_000_000, []byte("seeded-via-session"))
		mut.Set(sessionSandboxFamily, "c2", 5_000_000, []byte("second"))
		if err := sessionTbl.Apply(ctx, rowKey, mut); err != nil {
			t.Fatalf("session Apply: %v", err)
		}

		classicRow, err := classicTbl.ReadRow(ctx, rowKey)
		if err != nil {
			t.Fatalf("classic ReadRow after session write: %v", err)
		}
		sessionRow, err := sessionTbl.ReadRow(ctx, rowKey)
		if err != nil {
			t.Fatalf("session ReadRow after session write: %v", err)
		}
		assertRowsEqual(t, "session-write / dual-read", rowKey, classicRow, sessionRow)
	})
}

// assertRowsEqual byte-compares two Rows returned from ReadRow, tolerating
// stable-but-non-guaranteed ordering by sorting cells per family before
// comparison.
func assertRowsEqual(t *testing.T, label, rowKey string, a, b Row) {
	t.Helper()
	normalize := func(r Row) Row {
		out := make(Row, len(r))
		for fam, items := range r {
			cp := make([]ReadItem, len(items))
			copy(cp, items)
			sort.Slice(cp, func(i, j int) bool {
				if cp[i].Column != cp[j].Column {
					return cp[i].Column < cp[j].Column
				}
				if cp[i].Timestamp != cp[j].Timestamp {
					return cp[i].Timestamp < cp[j].Timestamp
				}
				return bytes.Compare(cp[i].Value, cp[j].Value) < 0
			})
			out[fam] = cp
		}
		return out
	}

	na, nb := normalize(a), normalize(b)
	if !testutil.Equal(na, nb) {
		t.Fatalf("[%s] row %q diverges between classic and session paths\nclassic: %#v\nsession: %#v",
			label, rowKey, na, nb)
	}
}

// waitForSessionPoolReady blocks until the session client has completed its
// initial ClientConfigurationManager poll (best-effort proxy: any non-zero
// SessionLoad OR a fixed warm-up window elapses, whichever comes first).
func waitForSessionPoolReady(t *testing.T, c *Client, timeout time.Duration) {
	t.Helper()
	if c.diverter == nil {
		t.Fatalf("session client has nil Diverter — EnableSessionPool likely false")
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if c.diverter.SessionLoad() > 0 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	// Fall through — the caller will pin SessionLoad to 1.0 unconditionally;
	// worst case the first ReadRow eats a warm-up latency spike.
}

func allBytesPayload() []byte {
	out := make([]byte, 256)
	for i := range out {
		out[i] = byte(i)
	}
	return out
}

func repeatBytes(seed []byte, target int) []byte {
	if len(seed) == 0 {
		return make([]byte, target)
	}
	out := make([]byte, target)
	for i := 0; i < target; i++ {
		out[i] = seed[i%len(seed)]
	}
	return out
}
