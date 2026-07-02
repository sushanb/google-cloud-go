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
	"context"
	"fmt"
	"net"
	"sync"
	"time"
)

// ConnRegistry tracks every *net.TCPConn returned by a custom gRPC dialer
// so tcpz can render live TCP_INFO for each. The registry never wraps the
// returned conn — gRPC receives the raw *net.TCPConn unchanged, so nothing
// in the RPC hot path traverses ConnRegistry code. Registry state is
// touched only on dial (rare) and Snapshot (only when someone renders
// tcpz). Dead entries are pruned lazily during Snapshot when getsockopt
// reports the fd is gone.
type ConnRegistry struct {
	mu    sync.RWMutex
	seq   uint64 // monotonic id so keys are unique across identical addr pairs
	conns map[uint64]*trackedConn
}

// trackedConn holds one dial's outputs plus a strong ref to the conn so we
// can call SyscallConn on it during Snapshot. Strong-ref-with-lazy-prune
// (vs weak refs or finalizers) trades a small window of stale entries for
// zero interference with gRPC's lifecycle expectations.
type trackedConn struct {
	remoteAddr string
	localAddr  string
	dialedAt   time.Time
	conn       *net.TCPConn
}

// NewConnRegistry constructs an empty registry.
func NewConnRegistry() *ConnRegistry {
	return &ConnRegistry{conns: make(map[uint64]*trackedConn)}
}

// Dial is the entry point wired into grpc.WithContextDialer. Delegates to
// net.Dialer.DialContext ("tcp" over addr), and on success records the
// *net.TCPConn in the registry before returning the raw conn to gRPC —
// no wrapping, so gRPC's type assertions (*net.TCPConn, SyscallConn, TLS
// deadline handling) all see exactly what they would without tcpz.
func (r *ConnRegistry) Dial(ctx context.Context, addr string) (net.Conn, error) {
	d := &net.Dialer{}
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	if tc, ok := conn.(*net.TCPConn); ok {
		r.add(tc)
	}
	return conn, nil
}

func (r *ConnRegistry) add(tc *net.TCPConn) {
	r.mu.Lock()
	r.seq++
	r.conns[r.seq] = &trackedConn{
		remoteAddr: tc.RemoteAddr().String(),
		localAddr:  tc.LocalAddr().String(),
		dialedAt:   time.Now(),
		conn:       tc,
	}
	r.mu.Unlock()
}

// TCPInfoSnapshot is the platform-agnostic view of one registered conn as
// of Snapshot time. On Linux the numeric fields come from struct tcp_info
// via getsockopt(TCP_INFO); on other platforms Err is populated and the
// numeric fields are zero. RemoteAddr / LocalAddr / DialedAt are captured
// at dial time and are always present.
type TCPInfoSnapshot struct {
	RemoteAddr   string
	LocalAddr    string
	DialedAt     time.Time
	State        string
	RTT          time.Duration
	RTTVar       time.Duration
	MinRTT       time.Duration
	MSS          uint32
	SndCwnd      uint32
	Retransmits  uint32
	TotalRetrans uint32
	Lost         uint32
	Unacked      uint32
	// LastDataRecv / LastDataSent express "how long since the socket last
	// carried data" — helps distinguish "idle since forever" from "just
	// hung." Both derived from tcp_info's last_data_{recv,sent} which are
	// in milliseconds since the last event.
	LastDataRecv time.Duration
	LastDataSent time.Duration
	// Err is set when this platform can't read TCP_INFO or the read
	// failed on a live fd. Dead fds (EBADF/ENOTCONN) are pruned rather
	// than surfaced, so a populated Err always means "conn exists but
	// info wasn't readable" — e.g. non-Linux OS.
	Err string
}

// Snapshot reads TCP_INFO for every registered conn and returns the
// results, oldest dial first. Dead entries (readTCPInfo returned an
// isDeadConn error) are removed from the registry before returning so a
// gRPC-closed conn doesn't linger indefinitely. All syscalls happen
// outside the registry lock so a slow syscall can't block dials or other
// snapshots.
func (r *ConnRegistry) Snapshot() []TCPInfoSnapshot {
	r.mu.RLock()
	keys := make([]uint64, 0, len(r.conns))
	byKey := make(map[uint64]*trackedConn, len(r.conns))
	for k, tc := range r.conns {
		keys = append(keys, k)
		byKey[k] = tc
	}
	r.mu.RUnlock()

	sortKeysAscending(keys)

	out := make([]TCPInfoSnapshot, 0, len(keys))
	var dead []uint64
	for _, k := range keys {
		tc := byKey[k]
		snap, err := readTCPInfo(tc.conn)
		if err != nil {
			if isDeadConn(err) {
				dead = append(dead, k)
				continue
			}
			snap = TCPInfoSnapshot{Err: err.Error()}
		}
		snap.RemoteAddr = tc.remoteAddr
		snap.LocalAddr = tc.localAddr
		snap.DialedAt = tc.dialedAt
		out = append(out, snap)
	}
	if len(dead) > 0 {
		r.mu.Lock()
		for _, k := range dead {
			delete(r.conns, k)
		}
		r.mu.Unlock()
	}
	return out
}

// Len reports the registered conn count (including any not-yet-pruned
// dead entries). Cheap; useful for a "conns=N" summary row.
func (r *ConnRegistry) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.conns)
}

// sortKeysAscending is a tiny helper — uint64 sort without pulling
// sort.Slice's reflection overhead into a debug path that runs frequently.
func sortKeysAscending(ks []uint64) {
	for i := 1; i < len(ks); i++ {
		for j := i; j > 0 && ks[j-1] > ks[j]; j-- {
			ks[j-1], ks[j] = ks[j], ks[j-1]
		}
	}
}

// unsupportedTCPInfoErr is the error tcp_info_*.go implementations return
// when the platform can't expose TCP_INFO. Exposed as a constant so tests
// can assert on it without importing platform-specific code.
type unsupportedTCPInfoErr struct{}

func (unsupportedTCPInfoErr) Error() string { return "tcp_info not supported on this platform" }

// ErrTCPInfoUnsupported is the sentinel returned by readTCPInfo on non-Linux.
var ErrTCPInfoUnsupported = unsupportedTCPInfoErr{}

// annotateReadErr is a small helper used by platform-specific readers when
// they want to prefix a syscall error with context.
func annotateReadErr(op string, err error) error {
	return fmt.Errorf("%s: %w", op, err)
}
