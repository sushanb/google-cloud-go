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
	btransport "cloud.google.com/go/bigtable/internal/transport"
)

// LoadBalancingSnapshots returns per-pool picker + timing snapshots
// from the session data plane. Empty when the client was constructed
// with DisableSession or before the first session pool has opened.
// Together with DispatchTimings this lets *Client satisfy the
// debugview.Provider interface structurally — callers can pass a
// *bigtable.Client directly to debugview.Handler without an adapter.
func (c *Client) LoadBalancingSnapshots() []btransport.LoadBalancingSnapshot {
	if c.sessionImpl == nil {
		return nil
	}
	sd := c.sessionImpl.SessionDebug()
	if sd == nil {
		return nil
	}
	return sd.LoadBalancingSnapshots()
}

// DispatchTimings returns per-method dispatch-level latency and call
// counters from the session data plane. Empty when the client was
// constructed with DisableSession or before any session vRPC has run.
func (c *Client) DispatchTimings() []session.DispatchMethodTimings {
	if c.sessionImpl == nil {
		return nil
	}
	return c.sessionImpl.DispatchTimings()
}
