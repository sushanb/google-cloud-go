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

// TCPStats is the opt-in collector for per-session TCP statistics
// (RTT, retransmits, cwnd, etc.) exposed via /debug/tcpz/.
//
// Populated at session-open time: every session's handleOpenSession
// captures the (localAddr, remoteAddr) 5-tuple from
// peer.FromContext(stream.Context()) and registers it. When tcpz
// renders, we look up TCP_INFO for each live 5-tuple via netlink
// SOCK_DIAG_BY_FAMILY / INET_DIAG_INFO — no file descriptor needed.
//
// Works for DirectPath. Prior implementations wrapped the standard
// dialer via grpc.WithContextDialer, which DirectPath's xDS transport
// bypasses; the session-registration approach doesn't care how the
// underlying dial happened as long as the resulting bidi stream
// carries a peer address.
//
// Scope: session-pool conns ONLY. Classic-pool conns and admin-side
// dials are not captured — those don't go through handleOpenSession.
// DirectPath is a session-pool concern in practice, so this matches
// the operator use case.
//
// Linux only for TCP_INFO. On other platforms the registry still
// populates (so operators see peer addresses on tcpz) but every row's
// numeric TCP_INFO fields stay zero and Err is set.
//
// Construction is not exposed as a public constructor — a *TCPStats is
// available exclusively via (*Client).TCPStats() after
// NewClientWithConfig with EnableClientDebug=true. Callers who need a
// bespoke collector should implement their own stats.Handler; the
// built-in path is the one debugview.Handler renders.
type TCPStats struct {
	reg *btransport.SessionIPRegistry
}

// newTCPStatsFromRegistry wraps a SessionIPRegistry as a TCPStats.
// Unexported — Client is the only intended constructor. Returns nil
// when reg is nil so Client can stash the result unconditionally.
func newTCPStatsFromRegistry(reg *btransport.SessionIPRegistry) *TCPStats {
	if reg == nil {
		return nil
	}
	return &TCPStats{reg: reg}
}

// NewTCPStats constructs an empty *TCPStats not wired to any Client.
// Production callers should reach for (*Client).TCPStats() after
// NewClientWithConfig with EnableClientDebug=true; this constructor
// exists so debugview + external tests can build a *TCPStats for
// handler wiring without spinning up a full Client. Snapshot on a
// fresh empty TCPStats returns nil.
func NewTCPStats() *TCPStats {
	return &TCPStats{reg: btransport.NewSessionIPRegistry()}
}

// Snapshot returns one TCPInfoSnapshot per currently-registered
// session, ordered by session LogName. Address fields (RemoteAddr,
// LocalAddr, DialedAt) come from the registry; TCP_INFO fields come
// from a netlink lookup by 4-tuple. Netlink failures for a given
// entry populate Err on that row and leave numeric fields zero — the
// row still renders so operators can see the session exists.
func (t *TCPStats) Snapshot() []btransport.TCPInfoSnapshot {
	if t == nil || t.reg == nil {
		return nil
	}
	entries := t.reg.Snapshot()
	out := make([]btransport.TCPInfoSnapshot, 0, len(entries))
	for _, e := range entries {
		out = append(out, btransport.QueryTCPInfoForSessionEntry(e))
	}
	return out
}

// Len returns the number of currently-registered sessions.
func (t *TCPStats) Len() int {
	if t == nil {
		return 0
	}
	return t.reg.Len()
}

// DeadConns is retained for API compatibility with the earlier
// dial-intercept implementation, which tracked closed conns in a
// bounded graveyard ring. The registry-based collector drops entries
// on session close, so no dead-conn history is available; always
// returns nil. The tcpz template renders empty on nil input.
func (t *TCPStats) DeadConns() []btransport.DeadConnInfo { return nil }
