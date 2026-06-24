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

	btpb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
	internal "cloud.google.com/go/bigtable/internal/transport"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TableShim routes traffic between a classic TableAPI and a SessionTable.
// It is the sole owner of the classic table reference on the session path;
// SessionTable itself has no knowledge of the classic client.
type TableShim struct {
	classic  TableAPI
	session  TableAPI
	diverter *internal.Diverter
}

// NewTableShim creates a TableShim wrapping a classic table and a session table.
func NewTableShim(classic, session TableAPI, diverter *internal.Diverter) TableAPI {
	return &TableShim{
		classic:  classic,
		session:  session,
		diverter: diverter,
	}
}

// ReadRow implements TableAPI.
func (t *TableShim) ReadRow(ctx context.Context, row string, opts ...ReadOption) (Row, error) {
	if t.diverter.UseSession() {
		return t.session.ReadRow(ctx, row, opts...)
	}
	return t.classic.ReadRow(ctx, row, opts...)
}

// Apply implements TableAPI. Conditional mutations always go to classic because
// the session transport does not support CheckAndMutateRow.
func (t *TableShim) Apply(ctx context.Context, row string, m *Mutation, opts ...ApplyOption) error {
	if m.isConditional || !t.diverter.UseSession() {
		return t.classic.Apply(ctx, row, m, opts...)
	}
	return t.session.Apply(ctx, row, m, opts...)
}

// sessionTable returns the underlying *SessionTable, or nil if the session is
// not a *SessionTable (e.g. in tests using a mock).
func (t *TableShim) sessionTable() *SessionTable {
	st, _ := t.session.(*SessionTable)
	return st
}

// ReadRows implements TableAPI. Delegates to classic as session support is not yet implemented.
func (t *TableShim) ReadRows(ctx context.Context, arg RowSet, f func(Row) bool, opts ...ReadOption) error {
	return t.classic.ReadRows(ctx, arg, f, opts...)
}

// SampleRowKeys implements TableAPI. Delegates to classic.
func (t *TableShim) SampleRowKeys(ctx context.Context) ([]string, error) {
	return t.classic.SampleRowKeys(ctx)
}

// ApplyBulk implements TableAPI. Delegates to classic.
func (t *TableShim) ApplyBulk(ctx context.Context, rowKeys []string, muts []*Mutation, opts ...ApplyOption) ([]error, error) {
	return t.classic.ApplyBulk(ctx, rowKeys, muts, opts...)
}

// ApplyReadModifyWrite implements TableAPI. Delegates to classic.
func (t *TableShim) ApplyReadModifyWrite(ctx context.Context, row string, m *ReadModifyWrite) (Row, error) {
	return t.classic.ApplyReadModifyWrite(ctx, row, m)
}

// ReadRowProto returns the raw *btpb.Row proto, bypassing the bigtable.Row
// conversion. Returns (nil, nil) for a missing row.
func (t *TableShim) ReadRowProto(ctx context.Context, row string, filter *btpb.RowFilter) (*btpb.Row, error) {
	st := t.sessionTable()
	if st == nil {
		return nil, status.Errorf(codes.Unavailable, "session table not available")
	}
	return st.ReadRowProto(ctx, row, filter)
}

// MutateRowProto applies proto mutations directly without bigtable.Mutation boxing.
func (t *TableShim) MutateRowProto(ctx context.Context, row string, mutations []*btpb.Mutation) error {
	st := t.sessionTable()
	if st == nil {
		return status.Errorf(codes.Unavailable, "session table not available")
	}
	return st.MutateRowProto(ctx, row, mutations)
}
