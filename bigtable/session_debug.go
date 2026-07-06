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
	// LoadBalancingSnapshots returns one per-pool picker + pick-history
	// snapshot for the loadz debug page.
	LoadBalancingSnapshots() []btransport.LoadBalancingSnapshot
}

// SessionDebug returns a SessionDebugProvider for this Client. Returns nil
// when ClientConfig.EnableSessionPool is false — in that case there is no
// session-based transport to report on.
func (c *Client) SessionDebug() SessionDebugProvider {
	if !c.config.EnableSessionPool || c.sessionImpl == nil {
		return nil
	}
	dbg, ok := c.sessionImpl.(session.DebugAccess)
	if !ok {
		return nil
	}
	return sessionDebugAdapter{dbg: dbg, diverter: c.diverter}
}

type sessionDebugAdapter struct {
	dbg      session.DebugAccess
	diverter *btransport.Diverter
}

func (a sessionDebugAdapter) Snapshot() []btransport.PoolSnapshot {
	return a.dbg.PoolSnapshots()
}

func (a sessionDebugAdapter) LoadBalancingSnapshots() []btransport.LoadBalancingSnapshot {
	return a.dbg.LoadBalancingSnapshots()
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
type ConfigDebugProvider interface {
	// Snapshot returns the most recent GetClientConfiguration response (or
	// the most recent error if no poll has succeeded yet).
	Snapshot() btransport.ConfigSnapshot
}

// ConfigDebug returns a ConfigDebugProvider for this Client. Returns nil
// when session pool is disabled (no configuration manager is constructed
// in that mode).
func (c *Client) ConfigDebug() ConfigDebugProvider {
	if c.sessionImpl == nil {
		return nil
	}
	dbg, ok := c.sessionImpl.(session.DebugAccess)
	if !ok {
		return nil
	}
	mgr := dbg.ConfigManager()
	if mgr == nil {
		return nil
	}
	return configDebugAdapter{mgr: mgr}
}

type configDebugAdapter struct {
	mgr *btransport.ClientConfigurationManager
}

func (a configDebugAdapter) Snapshot() btransport.ConfigSnapshot {
	return a.mgr.Snapshot()
}

// ChannelDebugProvider exposes a snapshot of every gRPC channel pool the
// Client currently owns — the classic data-plane pool and (when session
// pooling is enabled) the dedicated session pool.
type ChannelDebugProvider interface {
	// Snapshot returns one ChannelPoolSnapshot per BigtableChannelPool the
	// client holds, labeled by Role ("classic" / "session").
	Snapshot() []ChannelPoolDebug
}

// ChannelPoolDebug names a single channel pool and carries its snapshot.
type ChannelPoolDebug struct {
	// Role labels the pool — "classic" for the data-plane pool, "session"
	// for the dedicated session pool created when EnableSessionPool is on.
	Role     string
	Snapshot btransport.ChannelPoolSnapshot
	// SessionsByChannel maps a connEntry index to the sessions riding on
	// it. Populated only for the "session" role.
	SessionsByChannel map[int][]SessionRef
}

// SessionRef identifies one session for the channelz → sessionz reverse
// link. PoolName matches the sessionz /pool/{name} URL segment; LogName
// is the per-session row anchor (id="session-{LogName}") in sessionz.
type SessionRef struct {
	PoolName string
	LogName  string
}

// ChannelDebug returns a ChannelDebugProvider for this Client. Always
// non-nil; if the Client uses a non-Bigtable pool the snapshot will be empty.
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
	if a.client.config.EnableSessionPool && a.client.sessionImpl != nil {
		if dbg, ok := a.client.sessionImpl.(session.DebugAccess); ok {
			if sp := dbg.ChannelPool(); sp != nil {
				byChan := a.sessionsByChannelForSessionPool(dbg)
				out = append(out, ChannelPoolDebug{
					Role:              "session",
					Snapshot:          sp.ChannelPoolSnapshot(),
					SessionsByChannel: byChan,
				})
			}
		}
	}
	return out
}

// bigtableChannelPool extracts the *btransport.BigtableChannelPool from a
// gtransport.ConnPool, returning nil if the pool isn't a BigtableChannelPool.
func bigtableChannelPool(p interface{}) *btransport.BigtableChannelPool {
	if p == nil {
		return nil
	}
	bp, _ := p.(*btransport.BigtableChannelPool)
	return bp
}

// sessionsByChannelForSessionPool walks every session pool the
// SessionClient owns and groups the sessions by their ChannelIndex.
// Sessions without a valid channel index (sentinel -1) are skipped.
func (a channelDebugAdapter) sessionsByChannelForSessionPool(dbg session.DebugAccess) map[int][]SessionRef {
	pools := dbg.PoolSnapshots()
	var out map[int][]SessionRef
	for _, pool := range pools {
		for _, s := range pool.Sessions {
			if s.ChannelIndex < 0 {
				continue
			}
			if out == nil {
				out = map[int][]SessionRef{}
			}
			out[s.ChannelIndex] = append(out[s.ChannelIndex], SessionRef{
				PoolName: pool.Name,
				LogName:  s.LogName,
			})
		}
	}
	return out
}
