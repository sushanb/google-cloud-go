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
	"errors"
	"testing"

	btpb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
	btransport "cloud.google.com/go/bigtable/internal/transport"
)

// mockClassicTable stands in for a classic TableAPI (bigtable.Table
// wrapped in tableImpl). Records which method fired and forwards to
// per-method funcs.
type mockClassicTable struct {
	readRowFn   func(ctx context.Context, row string, opts ...ReadOption) (Row, error)
	applyFn     func(ctx context.Context, row string, m *Mutation, opts ...ApplyOption) error
	readRowsFn  func(ctx context.Context, arg RowSet, f func(Row) bool, opts ...ReadOption) error
	sampleFn    func(ctx context.Context) ([]string, error)
	applyBulkFn func(ctx context.Context, rowKeys []string, muts []*Mutation, opts ...ApplyOption) ([]error, error)
	rmwFn       func(ctx context.Context, row string, m *ReadModifyWrite) (Row, error)

	readRowCalls int
	applyCalls   int
}

func (m *mockClassicTable) ReadRow(ctx context.Context, row string, opts ...ReadOption) (Row, error) {
	m.readRowCalls++
	if m.readRowFn != nil {
		return m.readRowFn(ctx, row, opts...)
	}
	return Row{"fam": []ReadItem{{Row: row}}}, nil
}
func (m *mockClassicTable) Apply(ctx context.Context, row string, mut *Mutation, opts ...ApplyOption) error {
	m.applyCalls++
	if m.applyFn != nil {
		return m.applyFn(ctx, row, mut, opts...)
	}
	return nil
}
func (m *mockClassicTable) ReadRows(ctx context.Context, arg RowSet, f func(Row) bool, opts ...ReadOption) error {
	if m.readRowsFn != nil {
		return m.readRowsFn(ctx, arg, f, opts...)
	}
	return nil
}
func (m *mockClassicTable) SampleRowKeys(ctx context.Context) ([]string, error) {
	if m.sampleFn != nil {
		return m.sampleFn(ctx)
	}
	return nil, nil
}
func (m *mockClassicTable) ApplyBulk(ctx context.Context, rowKeys []string, muts []*Mutation, opts ...ApplyOption) ([]error, error) {
	if m.applyBulkFn != nil {
		return m.applyBulkFn(ctx, rowKeys, muts, opts...)
	}
	return nil, nil
}
func (m *mockClassicTable) ApplyReadModifyWrite(ctx context.Context, row string, mut *ReadModifyWrite) (Row, error) {
	if m.rmwFn != nil {
		return m.rmwFn(ctx, row, mut)
	}
	return nil, nil
}

// mockSessionTable is the proto-native session-side mock. Records
// which method fired and returns programmable responses.
type mockSessionTable struct {
	readRowFn   func(ctx context.Context, req *btpb.SessionReadRowRequest) (*btpb.SessionReadRowResponse, error)
	mutateRowFn func(ctx context.Context, req *btpb.SessionMutateRowRequest) (*btpb.SessionMutateRowResponse, error)

	readRowCalls   int
	mutateRowCalls int
}

func (m *mockSessionTable) ReadRow(ctx context.Context, req *btpb.SessionReadRowRequest) (*btpb.SessionReadRowResponse, error) {
	m.readRowCalls++
	if m.readRowFn != nil {
		return m.readRowFn(ctx, req)
	}
	// Return a proto Row with one cell so protoRowToRow produces a
	// non-empty map (Row.Key() reads from the first ReadItem's Row
	// field, not from proto.Key).
	return &btpb.SessionReadRowResponse{Row: &btpb.Row{
		Key: req.GetKey(),
		Families: []*btpb.Family{{
			Name: "fam",
			Columns: []*btpb.Column{{
				Qualifier: []byte("q"),
				Cells:     []*btpb.Cell{{Value: []byte("v")}},
			}},
		}},
	}}, nil
}
func (m *mockSessionTable) MutateRow(ctx context.Context, req *btpb.SessionMutateRowRequest) (*btpb.SessionMutateRowResponse, error) {
	m.mutateRowCalls++
	if m.mutateRowFn != nil {
		return m.mutateRowFn(ctx, req)
	}
	return &btpb.SessionMutateRowResponse{}, nil
}
func (m *mockSessionTable) Close() error { return nil }

// TestTableShim_ReadRow_RoutesByDiverter verifies the diverter gates
// classic vs session routing on ReadRow.
func TestTableShim_ReadRow_RoutesByDiverter(t *testing.T) {
	t.Run("classic when SessionLoad=0.0", func(t *testing.T) {
		classic := &mockClassicTable{}
		session := &mockSessionTable{}
		shim := NewTableShim(classic, session, btransport.NewDiverter(0.0))

		row, err := shim.ReadRow(context.Background(), "r1")
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if row.Key() != "r1" {
			t.Errorf("row.Key() = %q, want r1", row.Key())
		}
		if classic.readRowCalls != 1 {
			t.Errorf("classic.readRowCalls = %d, want 1", classic.readRowCalls)
		}
		if session.readRowCalls != 0 {
			t.Errorf("session.readRowCalls = %d, want 0", session.readRowCalls)
		}
	})

	t.Run("session when SessionLoad=1.0", func(t *testing.T) {
		classic := &mockClassicTable{}
		session := &mockSessionTable{}
		shim := NewTableShim(classic, session, btransport.NewDiverter(1.0))

		row, err := shim.ReadRow(context.Background(), "r2")
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if row.Key() != "r2" {
			t.Errorf("row.Key() = %q, want r2 (from protoRowToRow)", row.Key())
		}
		if classic.readRowCalls != 0 {
			t.Errorf("classic.readRowCalls = %d, want 0", classic.readRowCalls)
		}
		if session.readRowCalls != 1 {
			t.Errorf("session.readRowCalls = %d, want 1", session.readRowCalls)
		}
	})

	t.Run("classic fallback when session is nil", func(t *testing.T) {
		classic := &mockClassicTable{}
		shim := NewTableShim(classic, nil, btransport.NewDiverter(1.0))

		_, err := shim.ReadRow(context.Background(), "r3")
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if classic.readRowCalls != 1 {
			t.Errorf("classic.readRowCalls = %d, want 1 (must fall back when session=nil even with SessionLoad=1.0)", classic.readRowCalls)
		}
	})

	t.Run("session error propagates", func(t *testing.T) {
		wantErr := errors.New("session read failed")
		session := &mockSessionTable{
			readRowFn: func(ctx context.Context, req *btpb.SessionReadRowRequest) (*btpb.SessionReadRowResponse, error) {
				return nil, wantErr
			},
		}
		shim := NewTableShim(&mockClassicTable{}, session, btransport.NewDiverter(1.0))
		_, err := shim.ReadRow(context.Background(), "r4")
		if !errors.Is(err, wantErr) {
			t.Errorf("err = %v, want unwrap to %v", err, wantErr)
		}
	})
}

// TestTableShim_Apply_ConditionalAlwaysClassic verifies that conditional
// mutations bypass the session path regardless of diverter setting —
// CheckAndMutateRow has no session equivalent.
func TestTableShim_Apply_ConditionalAlwaysClassic(t *testing.T) {
	classic := &mockClassicTable{}
	session := &mockSessionTable{}
	shim := NewTableShim(classic, session, btransport.NewDiverter(1.0)) // diverter says session

	cond := NewCondMutation(PassAllFilter(), NewMutation(), nil)
	if err := shim.Apply(context.Background(), "r", cond); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if classic.applyCalls != 1 {
		t.Errorf("classic.applyCalls = %d, want 1 (conditional must go classic)", classic.applyCalls)
	}
	if session.mutateRowCalls != 0 {
		t.Errorf("session.mutateRowCalls = %d, want 0 (conditional must NOT go session)", session.mutateRowCalls)
	}
}

// TestTableShim_Apply_NonConditionalRoutesByDiverter verifies that
// non-conditional mutations follow the diverter.
func TestTableShim_Apply_NonConditionalRoutesByDiverter(t *testing.T) {
	t.Run("session when SessionLoad=1.0", func(t *testing.T) {
		classic := &mockClassicTable{}
		session := &mockSessionTable{}
		shim := NewTableShim(classic, session, btransport.NewDiverter(1.0))

		mut := NewMutation()
		mut.Set("fam", "col", 1_000_000, []byte("v"))
		if err := shim.Apply(context.Background(), "r", mut); err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if classic.applyCalls != 0 || session.mutateRowCalls != 1 {
			t.Errorf("classic=%d session=%d, want classic=0 session=1", classic.applyCalls, session.mutateRowCalls)
		}
	})

	t.Run("classic when SessionLoad=0.0", func(t *testing.T) {
		classic := &mockClassicTable{}
		session := &mockSessionTable{}
		shim := NewTableShim(classic, session, btransport.NewDiverter(0.0))

		mut := NewMutation()
		mut.Set("fam", "col", 1_000_000, []byte("v"))
		if err := shim.Apply(context.Background(), "r", mut); err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if classic.applyCalls != 1 || session.mutateRowCalls != 0 {
			t.Errorf("classic=%d session=%d, want classic=1 session=0", classic.applyCalls, session.mutateRowCalls)
		}
	})
}

// TestTableShim_ReadRows_AlwaysClassic — no session equivalent yet.
func TestTableShim_ReadRows_AlwaysClassic(t *testing.T) {
	classicCalled := 0
	classic := &mockClassicTable{
		readRowsFn: func(ctx context.Context, arg RowSet, f func(Row) bool, opts ...ReadOption) error {
			classicCalled++
			return nil
		},
	}
	shim := NewTableShim(classic, &mockSessionTable{}, btransport.NewDiverter(1.0))
	if err := shim.ReadRows(context.Background(), RowRange{}, func(Row) bool { return true }); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if classicCalled != 1 {
		t.Errorf("classic.ReadRows calls = %d, want 1 (ReadRows always goes classic)", classicCalled)
	}
}
