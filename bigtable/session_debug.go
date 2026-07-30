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

// The provider interface + struct types live in internal/transport so
// internal/session.SessionClient can implement them without an import
// cycle. Re-exported here as type aliases — external names and identity
// stay unchanged.

// SessionDebugProvider exposes a snapshot of every session pool a Client
// currently owns.
type SessionDebugProvider = btransport.SessionDebugProvider

// ChannelDebugProvider exposes a snapshot of every gRPC channel pool the
// Client currently owns.
type ChannelDebugProvider = btransport.ChannelDebugProvider

// ChannelPoolDebug names a single channel pool and carries its snapshot.
type ChannelPoolDebug = btransport.ChannelPoolDebug

// SessionRef identifies one session for the channelz → sessionz reverse
// link.
type SessionRef = btransport.SessionRef

// ConfigDebugProvider exposes a snapshot of the most recent
// GetClientConfiguration poll outcome.
type ConfigDebugProvider = btransport.ConfigDebugProvider

// SessionDebug returns a SessionDebugProvider for this Client. Returns
// nil when ClientConfig.EnableClientDebug is false — there is no
// session-based snapshot state to report on (debug recorders skipped
// entirely for zero hot-path overhead). Callers should treat nil as
// "no debug data available"; the debugview handler renders a "not
// enabled" panel in that case.
//
// The concrete provider layers the mixed-mode Diverter on top of the
// SessionClient's own session-only provider. sessionImpl.SessionDebug()
// already produces the Snapshot + LoadBalancingSnapshots parts, and
// itself returns nil when EnableClientDebug is false — that nil
// propagates through the check on the next few lines.
func (c *Client) SessionDebug() SessionDebugProvider {
	if c.sessionImpl == nil {
		return nil
	}
	base := c.sessionImpl.SessionDebug()
	if base == nil {
		return nil
	}
	return mixedModeSessionDebug{base: base, diverter: c.diverter}
}

// mixedModeSessionDebug wraps a session-only provider with the Client's
// classic/session Diverter. Snapshot + LoadBalancingSnapshots pass
// through unchanged; only Diverter() overrides.
type mixedModeSessionDebug struct {
	base     SessionDebugProvider
	diverter *btransport.Diverter
}

func (m mixedModeSessionDebug) Snapshot() []btransport.PoolSnapshot {
	return m.base.Snapshot()
}

func (m mixedModeSessionDebug) LoadBalancingSnapshots() []btransport.LoadBalancingSnapshot {
	return m.base.LoadBalancingSnapshots()
}

func (m mixedModeSessionDebug) Diverter() btransport.DiverterSnapshot {
	if m.diverter == nil {
		return btransport.DiverterSnapshot{}
	}
	return m.diverter.Snapshot()
}

// ConfigDebug returns a ConfigDebugProvider for this Client. Returns nil
// when session pool is disabled (no configuration manager is
// constructed in that mode). Delegates to sessionImpl.
func (c *Client) ConfigDebug() ConfigDebugProvider {
	if c.sessionImpl == nil {
		return nil
	}
	return c.sessionImpl.ConfigDebug()
}

// ChannelDebug returns a ChannelDebugProvider for this Client. Always
// non-nil; if the Client uses a non-Bigtable pool the classic entry is
// omitted. The session entry (Role="session") is contributed by
// sessionImpl.ChannelDebug when session pooling is enabled.
func (c *Client) ChannelDebug() ChannelDebugProvider {
	return mixedModeChannelDebug{client: c}
}

// mixedModeChannelDebug composes the Client's classic channel pool with
// the session pool contributed by sessionImpl. Emits the classic entry
// first (Role="classic"), then the session entry (Role="session") when
// session pooling is enabled.
type mixedModeChannelDebug struct {
	client *Client
}

func (a mixedModeChannelDebug) Snapshot() []ChannelPoolDebug {
	out := make([]ChannelPoolDebug, 0, 2)
	if p := bigtableChannelPool(a.client.mPool.Pool); p != nil {
		out = append(out, ChannelPoolDebug{Role: "classic", Snapshot: p.ChannelPoolSnapshot()})
	}
	if a.client.sessionImpl != nil {
		if sp := a.client.sessionImpl.ChannelDebug(); sp != nil {
			out = append(out, sp.Snapshot()...)
		}
	}
	return out
}

// bigtableChannelPool extracts the *btransport.BigtableChannelPool from
// a gtransport.ConnPool, returning nil if the pool isn't a
// BigtableChannelPool.
func bigtableChannelPool(p interface{}) *btransport.BigtableChannelPool {
	if p == nil {
		return nil
	}
	bp, _ := p.(*btransport.BigtableChannelPool)
	return bp
}
