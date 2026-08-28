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
	"sort"
	"testing"
	"time"
)

// TestReadRowsSingleRowShapesSandbox drives Table.ReadRows against a
// real sandbox instance and asserts that every RowSet shape the shim
// recognizes as "single row" (see TableShim.ReadRows +
// SESSION_POOL_SPEC.md §3) is diverted to the session data path,
// while the wire result stays byte-for-byte identical to the classic
// path.
//
// Coverage per (SessionLoad ∈ {0.0, 1.0}) × (shape ∈ 4 shapes):
//   - Found key: callback fires exactly once, row.Key() matches the seed.
//   - Missing key: callback does NOT fire (row-not-found parity).
//   - Multi-row RowList: stays classic on both paths (routability
//     table entry #2), result matches on both.
//
// Gated on CBT_RUN_SANDBOX=1. Point CBT_SANDBOX_* env vars at your
// own instance if the sushanb-uc1 default isn't reachable.
func TestReadRowsSingleRowShapesSandbox(t *testing.T) {
	tgt := sandboxFromEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	c := newSandboxClient(ctx, t, tgt)
	defer c.Close()

	runID := time.Now().UnixNano()
	keyA := fmt.Sprintf("__sandbox-readrows-a-%d", runID)
	keyB := fmt.Sprintf("__sandbox-readrows-b-%d", runID)
	keyC := fmt.Sprintf("__sandbox-readrows-c-%d", runID)
	missing := fmt.Sprintf("__sandbox-readrows-missing-%d", runID)

	// Seed via classic so the seed itself is not affected by the
	// diverter under test.
	pinSessionLoad(c, 0.0)
	tbl := c.OpenTable(tgt.table)
	for _, k := range []string{keyA, keyB, keyC} {
		mut := NewMutation()
		mut.Set(tgt.family, "colq", ServerTime, []byte("val-"+k))
		if err := tbl.Apply(ctx, k, mut); err != nil {
			t.Fatalf("seed Apply(%q): %v", k, err)
		}
	}
	t.Logf("seeded rows: %s %s %s (missing sentinel: %s)", keyA, keyB, keyC, missing)

	singleShapes := []struct {
		name string
		set  RowSet
	}{
		{"RowList{k}", RowList{keyA}},
		{"SingleRow(k)", SingleRow(keyA)},
		{"NewClosedRange(k,k)", NewClosedRange(keyA, keyA)},
		{"RowRangeList{NewClosedRange(k,k)}", RowRangeList{NewClosedRange(keyA, keyA)}},
	}
	missingShapes := []struct {
		name string
		set  RowSet
	}{
		{"missing RowList{k}", RowList{missing}},
		{"missing SingleRow(k)", SingleRow(missing)},
		{"missing NewClosedRange(k,k)", NewClosedRange(missing, missing)},
		{"missing RowRangeList{NewClosedRange(k,k)}", RowRangeList{NewClosedRange(missing, missing)}},
	}

	loads := []struct {
		name string
		load float64
	}{
		{"classic", 0.0},
		{"session", 1.0},
	}

	// assertDiverted wraps a subtest with a pre/post Diverter snapshot
	// and asserts the counter for the expected side ticked by exactly 1
	// (and the other side did NOT move). This is the load-bearing check
	// — without it, result parity alone would not prove the session-
	// labeled call actually went via the session data path.
	//
	// Re-pins the load immediately before the snapshot to shrink the
	// race window with ClientConfigurationManager's SessionLoadListener
	// (see client.go — the CM periodically pushes the server-driven
	// SessionLoad into the diverter and can clobber a pin set at the
	// top of the subtest).
	assertDiverted := func(t *testing.T, load float64, body func(t *testing.T)) {
		t.Helper()
		wantSession := load == 1.0
		pinSessionLoad(c, load)
		before := c.diverter.Snapshot()
		body(t)
		after := c.diverter.Snapshot()
		dSession := after.SessionPicks - before.SessionPicks
		dClassic := after.ClassicPicks - before.ClassicPicks
		if wantSession {
			if dSession != 1 || dClassic != 0 {
				t.Errorf("diverter picks: ΔSession=%d ΔClassic=%d, want ΔSession=1 ΔClassic=0 (load=%.1f)", dSession, dClassic, load)
			}
		} else {
			if dClassic != 1 || dSession != 0 {
				t.Errorf("diverter picks: ΔSession=%d ΔClassic=%d, want ΔSession=0 ΔClassic=1 (load=%.1f)", dSession, dClassic, load)
			}
		}
	}

	for _, ld := range loads {
		t.Run(ld.name, func(t *testing.T) {
			for _, sh := range singleShapes {
				t.Run("found/"+sh.name, func(t *testing.T) {
					assertDiverted(t, ld.load, func(t *testing.T) {
						var got []Row
						err := tbl.ReadRows(ctx, sh.set, func(r Row) bool {
							got = append(got, r)
							return true
						})
						if err != nil {
							t.Fatalf("ReadRows(%s): %v", sh.name, err)
						}
						if len(got) != 1 {
							t.Fatalf("ReadRows(%s): got %d rows, want 1", sh.name, len(got))
						}
						if got[0].Key() != keyA {
							t.Errorf("ReadRows(%s): row.Key()=%q want %q", sh.name, got[0].Key(), keyA)
						}
						items := got[0][tgt.family]
						if len(items) == 0 {
							t.Fatalf("ReadRows(%s): family %q empty; row=%+v", sh.name, tgt.family, got[0])
						}
						if want := "val-" + keyA; string(items[0].Value) != want {
							t.Errorf("ReadRows(%s): value=%q want=%q", sh.name, items[0].Value, want)
						}
					})
				})
			}

			for _, sh := range missingShapes {
				t.Run(sh.name, func(t *testing.T) {
					assertDiverted(t, ld.load, func(t *testing.T) {
						n := 0
						err := tbl.ReadRows(ctx, sh.set, func(Row) bool {
							n++
							return true
						})
						if err != nil {
							t.Fatalf("ReadRows(%s): %v", sh.name, err)
						}
						if n != 0 {
							t.Errorf("ReadRows(%s): callback fired %d times, want 0 (row not found)", sh.name, n)
						}
					})
				})
			}

			// Multi-key RowList stays classic on both paths per the
			// routability table — the shim's non-single-row branch
			// never consults the diverter, so both counters stay put.
			// Observable result must be path-independent.
			t.Run("RowList{keyA,keyB} multi-row (stays classic; diverter not consulted)", func(t *testing.T) {
				pinSessionLoad(c, ld.load)
				before := c.diverter.Snapshot()
				var gotKeys []string
				err := tbl.ReadRows(ctx, RowList{keyA, keyB}, func(r Row) bool {
					gotKeys = append(gotKeys, r.Key())
					return true
				})
				if err != nil {
					t.Fatalf("ReadRows(multi): %v", err)
				}
				sort.Strings(gotKeys)
				if len(gotKeys) != 2 || gotKeys[0] != keyA || gotKeys[1] != keyB {
					t.Errorf("ReadRows(multi): got %v want [%s %s]", gotKeys, keyA, keyB)
				}
				after := c.diverter.Snapshot()
				if dS, dC := after.SessionPicks-before.SessionPicks, after.ClassicPicks-before.ClassicPicks; dS != 0 || dC != 0 {
					t.Errorf("diverter picks: ΔSession=%d ΔClassic=%d, want both 0 (multi-row does not consult diverter)", dS, dC)
				}
			})
		})
	}
}
