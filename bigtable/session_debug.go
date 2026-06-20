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
	btransport "cloud.google.com/go/bigtable/internal/transport"
)

// SessionDebugProvider exposes a snapshot of every session pool a Client
// currently owns. The bigtable/sessionz package consumes this to render an
// HTTP debug UI; callers can also use it to dump state programmatically.
//
// Snapshot is safe to call concurrently with any other Client operation. It
// adds no overhead to the RPC hot path — the underlying counters are
// incremented atomically and the snapshot path reads them lock-free.
type SessionDebugProvider interface {
	// Snapshot returns a snapshot of every session pool, ordered by pool key.
	Snapshot() []btransport.PoolSnapshot
}

// SessionDebug returns a SessionDebugProvider for this Client. Returns nil
// when ClientConfig.EnableSessionPool is false — in that case there is no
// session-based transport to report on.
func (c *Client) SessionDebug() SessionDebugProvider {
	if !c.config.EnableSessionPool || c.sessionMgr == nil {
		return nil
	}
	return sessionDebugAdapter{mgr: c.sessionMgr}
}

type sessionDebugAdapter struct {
	mgr *SessionManager
}

func (a sessionDebugAdapter) Snapshot() []btransport.PoolSnapshot {
	return a.mgr.ManagerSnapshot()
}
