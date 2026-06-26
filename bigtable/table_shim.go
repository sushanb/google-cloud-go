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
	"cloud.google.com/go/bigtable/internal/session"
	internal "cloud.google.com/go/bigtable/internal/transport"
)

// TableShim wraps a classic TableAPI and an optional session.SessionTableApi.
// It diverts the session-routable methods (ReadRow, Apply) between them based
// on the Diverter, bridging bigtable.Row / *bigtable.Mutation ↔ proto types at
// the session boundary. Everything else delegates to the classic implementation.
type TableShim struct {
	classic  TableAPI
	session  session.SessionTableApi // nil when no session path is available
	diverter *internal.Diverter
}

// NewTableShim creates a new TableShim. sessionAPI may be nil — in that case
// all calls route through classic regardless of the diverter.
func NewTableShim(classic TableAPI, sessionAPI session.SessionTableApi, diverter *internal.Diverter) TableAPI {
	return &TableShim{
		classic:  classic,
		session:  sessionAPI,
		diverter: diverter,
	}
}

// ReadRow implements TableAPI. When the diverter routes to the session path,
// it builds a SessionReadRowRequest (running any ReadOption through a scaffold
// to capture the filter) and converts the proto response back to bigtable.Row.
func (t *TableShim) ReadRow(ctx context.Context, row string, opts ...ReadOption) (Row, error) {
	if t.session == nil || !t.diverter.UseSession() {
		return t.classic.ReadRow(ctx, row, opts...)
	}

	// Scaffold a ReadRowsRequest so ReadOption.set can populate the filter
	// consistently with the classic path. Only the resulting Filter is
	// forwarded — TableName / Rows are not consumed by SessionReadRow.
	scaffold := &btpb.ReadRowsRequest{}
	settings := makeReadSettings(scaffold, 0)
	for _, opt := range opts {
		opt.set(&settings)
	}

	resp, err := t.session.ReadRow(ctx, &btpb.SessionReadRowRequest{
		Key:    []byte(row),
		Filter: scaffold.Filter,
	})
	if err != nil {
		return nil, err
	}
	return protoRowToRow(resp.GetRow()), nil
}

// Apply implements TableAPI. Conditional mutations cannot ride the session
// transport (no CheckAndMutateRow vRPC) and always route to classic.
func (t *TableShim) Apply(ctx context.Context, row string, m *Mutation, opts ...ApplyOption) error {
	if m.isConditional || t.session == nil || !t.diverter.UseSession() {
		return t.classic.Apply(ctx, row, m, opts...)
	}
	_, err := t.session.MutateRow(ctx, &btpb.SessionMutateRowRequest{
		Key:       []byte(row),
		Mutations: m.ops,
	})
	return err
}

// ReadRows, SampleRowKeys, ApplyBulk, and ApplyReadModifyWrite have no session
// transport equivalent yet — delegate to the classic implementation.

func (t *TableShim) ReadRows(ctx context.Context, arg RowSet, f func(Row) bool, opts ...ReadOption) error {
	return t.classic.ReadRows(ctx, arg, f, opts...)
}

func (t *TableShim) SampleRowKeys(ctx context.Context) ([]string, error) {
	return t.classic.SampleRowKeys(ctx)
}

func (t *TableShim) ApplyBulk(ctx context.Context, rowKeys []string, muts []*Mutation, opts ...ApplyOption) ([]error, error) {
	return t.classic.ApplyBulk(ctx, rowKeys, muts, opts...)
}

func (t *TableShim) ApplyReadModifyWrite(ctx context.Context, row string, m *ReadModifyWrite) (Row, error) {
	return t.classic.ApplyReadModifyWrite(ctx, row, m)
}

// protoRowToRow converts a *btpb.Row to a bigtable.Row. Returns nil when pr is
// nil (the proto for a missing row).
func protoRowToRow(pr *btpb.Row) Row {
	if pr == nil {
		return nil
	}
	rowMap := make(Row)
	rowKey := string(pr.Key)
	for _, fam := range pr.Families {
		familyName := fam.Name
		for _, col := range fam.Columns {
			columnName := familyName + ":" + string(col.Qualifier)
			var items []ReadItem
			for _, cell := range col.Cells {
				items = append(items, ReadItem{
					Row:       rowKey,
					Column:    columnName,
					Timestamp: Timestamp(cell.TimestampMicros),
					Value:     cell.Value,
					Labels:    cell.Labels,
				})
			}
			if len(items) > 0 {
				rowMap[familyName] = append(rowMap[familyName], items...)
			}
		}
	}
	return rowMap
}
