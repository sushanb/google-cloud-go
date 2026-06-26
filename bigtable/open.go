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
	"cloud.google.com/go/bigtable/internal/session"
	"google.golang.org/grpc/metadata"
)

// Open opens a table.
func (c *Client) Open(table string) *Table {
	return &Table{
		c:     c,
		table: table,
		md: metadata.Join(metadata.Pairs(
			resourcePrefixHeader, c.fullTableName(table),
			requestParamsHeader, c.reqParamsHeaderValTable(table),
		), c.featureFlagsMD),
	}
}

// OpenTable opens a table. When EnableSessionPool is set on the client config,
// session-routable operations (ReadRow, Apply) dispatch through the vRPC
// session transport via internal/session; everything else delegates to the
// classic *Table.
func (c *Client) OpenTable(table string) TableAPI {
	classic := c.Open(table)
	classicAPI := &tableImpl{*classic}

	if c.sessionImpl == nil || c.diverter == nil {
		return classicAPI
	}

	return NewTableShim(classicAPI, c.getOrCreateSessionTable(table), c.diverter)
}

// OpenAuthorizedView opens an authorized view. Session-transport routing for
// authorized views is not yet implemented in internal/session; all operations
// go through the classic path.
//
// TODO: add SessionTableApi support for authorized views (separate VRpc
// descriptors and OpenSessionRequest payload type).
func (c *Client) OpenAuthorizedView(table, authorizedView string) TableAPI {
	classic := &Table{
		c:     c,
		table: table,
		md: metadata.Join(metadata.Pairs(
			resourcePrefixHeader, c.fullAuthorizedViewName(table, authorizedView),
			requestParamsHeader, c.reqParamsHeaderValAuthorizedView(table, authorizedView),
		), c.featureFlagsMD),
		authorizedView: authorizedView,
	}
	return &tableImpl{*classic}
}

// OpenMaterializedView opens a materialized view. Session-transport routing
// for materialized views is not yet implemented in internal/session; reads go
// through the classic path.
//
// TODO: add SessionTableApi support for materialized views (read-only, with
// the READ_ROW_MAT_VIEW descriptor).
func (c *Client) OpenMaterializedView(materializedView string) TableAPI {
	classic := &Table{
		c: c,
		md: metadata.Join(metadata.Pairs(
			resourcePrefixHeader, c.fullMaterializedViewName(materializedView),
			requestParamsHeader, c.reqParamsHeaderValMaterializedView(materializedView),
		), c.featureFlagsMD),
		materializedView: materializedView,
	}
	return &tableImpl{*classic}
}

// getOrCreateSessionTable returns the cached SessionTableApi for table,
// creating one on miss. The cache lives on the Client because internal/session
// is intentionally cache-free.
func (c *Client) getOrCreateSessionTable(table string) session.SessionTableApi {
	// Lookup / create under one lock so concurrent OpenTable calls for the
	// same table do not stand up duplicate session pools.
	c.sessionTablesMu.Lock()
	defer c.sessionTablesMu.Unlock()
	if st, ok := c.sessionTables[table]; ok {
		return st
	}
	st := c.sessionImpl.NewSessionTable(table)
	c.sessionTables[table] = st
	return st
}
