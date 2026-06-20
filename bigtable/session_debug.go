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
	// Diverter returns the client-wide session/classic split state.
	Diverter() btransport.DiverterSnapshot
}

// SessionDebug returns a SessionDebugProvider for this Client. Returns nil
// when ClientConfig.EnableSessionPool is false — in that case there is no
// session-based transport to report on.
func (c *Client) SessionDebug() SessionDebugProvider {
	if !c.config.EnableSessionPool || c.sessionMgr == nil {
		return nil
	}
	return sessionDebugAdapter{mgr: c.sessionMgr, diverter: c.diverter}
}

type sessionDebugAdapter struct {
	mgr      *SessionManager
	diverter *btransport.Diverter
}

func (a sessionDebugAdapter) Snapshot() []btransport.PoolSnapshot {
	return a.mgr.ManagerSnapshot()
}

func (a sessionDebugAdapter) Diverter() btransport.DiverterSnapshot {
	if a.diverter == nil {
		return btransport.DiverterSnapshot{}
	}
	return a.diverter.Snapshot()
}

// ConfigDebugProvider exposes a snapshot of the most recent
// GetClientConfiguration poll outcome. The bigtable/configz package consumes
// this to render an HTTP debug page; callers can also use it programmatically.
//
// Snapshot is safe to call concurrently with other Client operations. It
// adds no overhead to the polling loop — the manager already keeps the last
// response under its existing mutex.
type ConfigDebugProvider interface {
	// Snapshot returns the most recent GetClientConfiguration response (or
	// the most recent error if no poll has succeeded yet).
	Snapshot() btransport.ConfigSnapshot
}

// ConfigDebug returns a ConfigDebugProvider for this Client. Returns nil
// when the client was constructed without a configuration manager (i.e.
// before NewClientWithConfig wired one up).
func (c *Client) ConfigDebug() ConfigDebugProvider {
	if c.configManager == nil {
		return nil
	}
	return configDebugAdapter{mgr: c.configManager}
}

type configDebugAdapter struct {
	mgr *btransport.ClientConfigurationManager
}

func (a configDebugAdapter) Snapshot() btransport.ConfigSnapshot {
	return a.mgr.Snapshot()
}

// ChannelDebugProvider exposes a snapshot of every gRPC channel pool the
// Client currently owns — the classic data-plane pool and (when session
// pooling is enabled) the dedicated session pool. The bigtable/channelz
// package consumes this to render a per-channel debug view.
//
// Each entry in the returned slice corresponds to one BigtableChannelPool.
// Snapshot is safe to call concurrently with other Client operations and
// adds no overhead to the request path — it reads the existing atomics
// non-destructively.
type ChannelDebugProvider interface {
	// Snapshot returns one ChannelPoolSnapshot per BigtableChannelPool the
	// client holds, labeled by Role ("classic" / "session"). The slice is
	// empty when the client uses a non-Bigtable connection pool (e.g. the
	// caller passed option.WithGRPCConn).
	Snapshot() []ChannelPoolDebug
}

// ChannelPoolDebug names a single channel pool and carries its snapshot.
type ChannelPoolDebug struct {
	// Role labels the pool — "classic" for the data-plane pool, "session"
	// for the dedicated session pool created when EnableSessionPool is on.
	Role     string
	Snapshot btransport.ChannelPoolSnapshot
	// SessionsByChannel maps a connEntry index to the session log names
	// currently riding on it. Populated only for the "session" role —
	// classic-path RPCs aren't associated with any session. nil when no
	// sessions are linked (e.g. underlying pool isn't a
	// BigtableChannelPool, so the pick-hint never fired).
	SessionsByChannel map[int][]string
}

// ChannelDebug returns a ChannelDebugProvider for this Client. Always
// non-nil; if the Client uses a non-Bigtable pool (caller passed
// option.WithGRPCConn) the snapshot will be empty.
func (c *Client) ChannelDebug() ChannelDebugProvider {
	return channelDebugAdapter{client: c}
}

type channelDebugAdapter struct {
	client *Client
}

func (a channelDebugAdapter) Snapshot() []ChannelPoolDebug {
	out := make([]ChannelPoolDebug, 0, 2)
	if p := bigtableChannelPool(a.client.classicPool.pool); p != nil {
		out = append(out, ChannelPoolDebug{Role: "classic", Snapshot: p.ChannelPoolSnapshot()})
	}
	if a.client.config.EnableSessionPool {
		// The session channel pool lives on the SessionManager because the
		// SessionManager owns its managedChannelPool. We can fetch it via
		// the manager's accessor.
		if sp := a.client.sessionMgr.channelChannelPool(); sp != nil {
			byChan := a.sessionsByChannelForSessionPool()
			out = append(out, ChannelPoolDebug{
				Role:              "session",
				Snapshot:          sp.ChannelPoolSnapshot(),
				SessionsByChannel: byChan,
			})
		}
	}
	return out
}

// bigtableChannelPool extracts the *btransport.BigtableChannelPool from a
// gtransport.ConnPool, returning nil if the pool isn't a BigtableChannelPool
// (e.g. the caller built the client with option.WithGRPCConn).
func bigtableChannelPool(p interface{}) *btransport.BigtableChannelPool {
	if p == nil {
		return nil
	}
	bp, _ := p.(*btransport.BigtableChannelPool)
	return bp
}

// sessionsByChannelForSessionPool walks every session pool the
// SessionManager owns and groups the sessions' log names by their
// ChannelIndex. Sessions without a valid index (sentinel -1) are skipped.
// Returns nil if no sessions are linked.
func (a channelDebugAdapter) sessionsByChannelForSessionPool() map[int][]string {
	if a.client.sessionMgr == nil {
		return nil
	}
	pools := a.client.sessionMgr.ManagerSnapshot()
	var out map[int][]string
	for _, pool := range pools {
		for _, s := range pool.Sessions {
			if s.ChannelIndex < 0 {
				continue
			}
			if out == nil {
				out = map[int][]string{}
			}
			out[s.ChannelIndex] = append(out[s.ChannelIndex], s.LogName)
		}
	}
	return out
}
