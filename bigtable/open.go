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

// OpenTable opens a table. Returns a TableShim when the session pool is
// configured, else the classic tableImpl.
func (c *Client) OpenTable(table string) TableAPI {
	classic := c.Open(table)
	classicAPI := &tableImpl{Table: *classic}
	if c.sessionImpl == nil || c.diverter == nil {
		return classicAPI
	}
	return NewTableShim(classicAPI, c.getOrCreateSessionTable(table), c.diverter)
}

// OpenAuthorizedView opens an authorized view.
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
	classicAPI := &tableImpl{Table: *classic}
	if c.sessionImpl == nil || c.diverter == nil {
		return classicAPI
	}
	return NewTableShim(classicAPI, c.getOrCreateSessionAuthorizedView(table, authorizedView), c.diverter)
}

// OpenMaterializedView opens a materialized view.
func (c *Client) OpenMaterializedView(materializedView string) TableAPI {
	classic := &Table{
		c: c,
		md: metadata.Join(metadata.Pairs(
			resourcePrefixHeader, c.fullMaterializedViewName(materializedView),
			requestParamsHeader, c.reqParamsHeaderValMaterializedView(materializedView),
		), c.featureFlagsMD),
		materializedView: materializedView,
	}
	classicAPI := &tableImpl{Table: *classic}
	if c.sessionImpl == nil || c.diverter == nil {
		return classicAPI
	}
	return NewTableShim(classicAPI, c.getOrCreateSessionMaterializedView(materializedView), c.diverter)
}

// getOrCreateSessionTable returns the cached TableAPI for this
// table, opening a fresh one on cache miss.
func (c *Client) getOrCreateSessionTable(table string) session.TableAPI {
	c.sessionTablesMu.Lock()
	defer c.sessionTablesMu.Unlock()
	key := "tbl:" + table
	if st, ok := c.sessionTables[key]; ok {
		return st
	}
	st := c.sessionImpl.OpenSessionTable(table)
	c.sessionTables[key] = st
	return st
}

// getOrCreateSessionAuthorizedView is the TableAPI cache lookup
// for authorized views. Cache key is "av:<table>:<view>".
func (c *Client) getOrCreateSessionAuthorizedView(table, view string) session.TableAPI {
	c.sessionTablesMu.Lock()
	defer c.sessionTablesMu.Unlock()
	key := "av:" + table + ":" + view
	if st, ok := c.sessionTables[key]; ok {
		return st
	}
	st := c.sessionImpl.OpenAuthorizedView(table, view)
	c.sessionTables[key] = st
	return st
}

// getOrCreateSessionMaterializedView is the TableAPI cache
// lookup for materialized views. Cache key is "mv:<view>".
func (c *Client) getOrCreateSessionMaterializedView(view string) session.TableAPI {
	c.sessionTablesMu.Lock()
	defer c.sessionTablesMu.Unlock()
	key := "mv:" + view
	if st, ok := c.sessionTables[key]; ok {
		return st
	}
	st := c.sessionImpl.OpenMaterializedView(view)
	c.sessionTables[key] = st
	return st
}
