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
	"fmt"
	"time"
)

// TCPInfoSnapshot is the platform-agnostic view of one registered conn as
// of Snapshot time. On Linux the numeric fields come from struct tcp_info
// via getsockopt(TCP_INFO) or netlink INET_DIAG_INFO; on other platforms
// Err is populated and the numeric fields are zero. RemoteAddr / LocalAddr
// / DialedAt are captured at session-open time and are always present.
//
// Fields are grouped by what question they answer. "Why is TotalRetrans
// high?" is answered by the Congestion + Loss-classification blocks:
// CAState/Backoff say what the kernel thinks the loss regime is; DSACK,
// Reordering, and DeliveredCE distinguish real drops from spurious /
// out-of-order / ECN-signaled events; BytesRetrans + BytesSent give the
// actual retrans ratio.
type TCPInfoSnapshot struct {
	// Identity — captured at session-open time.
	RemoteAddr string
	LocalAddr  string
	DialedAt   time.Time

	// TCP + congestion state.
	State   string // TCP FSM state (ESTABLISHED, CLOSE_WAIT, …)
	CAState string // congestion-control state (Open/Disorder/CWR/Recovery/Loss)
	Backoff uint32 // RTO exponential-backoff count; >0 = we've timed out and are waiting longer

	// Round-trip time.
	RTT    time.Duration
	RTTVar time.Duration
	MinRTT time.Duration

	// Window + segment sizing.
	MSS         uint32 // send MSS
	PMTU        uint32 // path MTU (bytes). <1500 = tunneling/VPN in path; silent throughput killer if a middlebox black-holes PMTUD ICMP.
	SndCwnd     uint32 // send congestion window (MSS units)
	SndSsthresh uint32 // slow-start threshold; drop = we've reduced cwnd from loss
	SndWnd      uint32 // current send window (bytes)
	RcvWnd      uint32 // current receive window (bytes)

	// Loss + retransmit counters.
	Retransmits  uint32
	Retrans      uint32
	TotalRetrans uint32
	Lost         uint32
	Sacked       uint32
	Unacked      uint32
	Reordering   uint32
	ReordSeen    uint32
	DsackDups    uint32
	DeliveredCE  uint32
	RcvOooPack   uint32

	// RTO / probe timing.
	RTO                time.Duration
	ATO                time.Duration
	Probes             uint32
	TotalRTO           uint32
	TotalRTORecoveries uint32
	TotalRTOTime       time.Duration

	// Volume / rate.
	SegsOut       uint32
	SegsIn        uint32
	DataSegsOut   uint32
	DataSegsIn    uint32
	BytesSent     uint64
	BytesAcked    uint64
	BytesRetrans  uint64
	BytesReceived uint64
	Delivered     uint32
	DeliveryRate  uint64
	PacingRate    uint64
	NotsentBytes  uint32
	BusyTime      time.Duration
	RwndLimited   time.Duration
	SndbufLimited time.Duration
	Rehash        uint32

	LastDataRecv time.Duration
	LastDataSent time.Duration

	// RetransRatioPct = BytesRetrans / BytesSent * 100, precomputed for
	// the common "% of bytes retransmitted" view. Zero when BytesSent is
	// zero (no data has flowed yet).
	RetransRatioPct float64

	// Err is set when this platform can't read TCP_INFO or the read
	// failed. Populated Err means "session exists but info wasn't
	// readable" — e.g. non-Linux OS, or the socket died between
	// SessionIPRegistry.Add and the netlink query.
	Err string
}

// annotateReadErr is a small helper used by platform-specific readers
// when they want to prefix a syscall error with context.
func annotateReadErr(op string, err error) error {
	return fmt.Errorf("%s: %w", op, err)
}
