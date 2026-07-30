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

package internal

import (
	"errors"
	"sort"
	"sync"
	"time"

	spb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
)

// ErrTCPInfoUnsupported is returned by TCP_INFO readers on non-Linux
// platforms. Callers surface it via TCPInfoSnapshot.Err so the
// operator sees a row with addresses but no numeric fields.
var ErrTCPInfoUnsupported = errors.New("tcp_info not supported on this platform")

// DeadConnInfo is retained for API compatibility with debugview's tcpz
// hist-panel builder, which formerly plotted lifetime distributions
// across recently-departed conns. The registry-based collector drops
// entries on session close and doesn't ring-buffer deaths; TCPStats
// returns nil DeadConns so the panel renders empty. Fields kept
// identical to the pre-refactor shape so debugview templates that
// reference them still parse.
type DeadConnInfo struct {
	RemoteAddr string
	LocalAddr  string
	DialedAt   time.Time
	DiedAt     time.Time
}

// SessionIPRegistry captures the (localAddr, remoteAddr) 5-tuple of every
// live session bidi stream so /debug/tcpz/ can render TCP_INFO by
// 4-tuple lookup (netlink INET_DIAG_INFO) instead of by fd. Populated
// from Session.handleOpenSession right after PeerInfo is parsed —
// same moment we know the stream is established, and the moment we
// have peer.Peer available via peer.FromContext(stream.Context()).
//
// Works for DirectPath. The previous fd-based collector held a
// *net.TCPConn from grpc.WithContextDialer, which DirectPath's xDS
// transport skips entirely; the addresses-only registry doesn't
// care how the underlying dial happened as long as the resulting
// stream carries peer info.
//
// Scope: session pool conns ONLY. Classic pool conns don't route
// through handleOpenSession, so they're not captured here. That's
// intentional per plan — DirectPath is a session-pool concern.
type SessionIPRegistry struct {
	mu      sync.RWMutex
	entries map[string]*SessionIPEntry // key: session.LogName()
}

// SessionIPEntry is one row in the registry.
type SessionIPEntry struct {
	LogName    string        // Session.LogName() — cross-links to sessionz
	RemoteAddr string        // ip:port
	LocalAddr  string        // ip:port (may be empty for DirectPath if peer.LocalAddr is nil)
	DialedAt   time.Time     // handleOpenSession timestamp
	PeerInfo   *spb.PeerInfo // AFE / subzone / transport-type for tcpz row label
}

// NewSessionIPRegistry constructs an empty registry. Caller (session
// package) constructs one per Client when EnableDebug is true and
// hands it to every SessionPoolImpl the Client owns.
func NewSessionIPRegistry() *SessionIPRegistry {
	return &SessionIPRegistry{
		entries: make(map[string]*SessionIPEntry),
	}
}

// Add records the 5-tuple for a session that just finished OpenSession
// handshake. Idempotent: second Add for the same LogName replaces the
// prior entry (matches handleOpenSession's re-delivery-is-idempotent
// contract). Nil-safe on the receiver so Sessions without a registry
// (unit tests) can call unconditionally.
func (r *SessionIPRegistry) Add(logName, remote, local string, peer *spb.PeerInfo) {
	if r == nil {
		return
	}
	r.mu.Lock()
	r.entries[logName] = &SessionIPEntry{
		LogName:    logName,
		RemoteAddr: remote,
		LocalAddr:  local,
		DialedAt:   time.Now(),
		PeerInfo:   peer,
	}
	r.mu.Unlock()
}

// Remove drops a session's entry — called from notifyClosed so the
// registry doesn't hold stale entries for torn-down sessions. Nil-safe.
func (r *SessionIPRegistry) Remove(logName string) {
	if r == nil {
		return
	}
	r.mu.Lock()
	delete(r.entries, logName)
	r.mu.Unlock()
}

// Snapshot returns every live entry, sorted by LogName for stable
// rendering. Callers hold no reference to registry-internal state
// after Snapshot returns — the returned slice + its elements are
// caller-owned copies.
func (r *SessionIPRegistry) Snapshot() []SessionIPEntry {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	out := make([]SessionIPEntry, 0, len(r.entries))
	for _, e := range r.entries {
		out = append(out, *e)
	}
	r.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool { return out[i].LogName < out[j].LogName })
	return out
}

// Len returns the current entry count. Cheap — one RLock + len().
func (r *SessionIPRegistry) Len() int {
	if r == nil {
		return 0
	}
	r.mu.RLock()
	n := len(r.entries)
	r.mu.RUnlock()
	return n
}
