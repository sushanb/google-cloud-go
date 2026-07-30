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

//go:build linux

package internal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"os"
	"syscall"
	"unsafe"

	"golang.org/x/sys/unix"
)

// queryTCPInfoByAddrs asks the kernel for TCP_INFO of the socket
// matching (family, srcIP:srcPort, dstIP:dstPort) via netlink
// SOCK_DIAG_BY_FAMILY + INET_DIAG_INFO. No fd needed — the kernel
// resolves the socket by 4-tuple.
//
// Solves the DirectPath capture problem: grpc.WithContextDialer is
// skipped by DirectPath's xDS transport so the fd-based readTCPInfo
// never runs, but the socket still exists in the kernel and netlink
// finds it just fine.
//
// Cost: one netlink round-trip per call. Sub-millisecond on the
// loopback socket. Callers that render tcpz should batch by iterating
// SessionIPRegistry.Snapshot() and issuing one query per entry.
func queryTCPInfoByAddrs(srcIP net.IP, srcPort uint16, dstIP net.IP, dstPort uint16) (TCPInfoSnapshot, error) {
	family := uint8(unix.AF_INET)
	if srcIP.To4() == nil || dstIP.To4() == nil {
		family = uint8(unix.AF_INET6)
	}

	fd, err := unix.Socket(unix.AF_NETLINK, unix.SOCK_DGRAM|unix.SOCK_CLOEXEC, unix.NETLINK_INET_DIAG)
	if err != nil {
		return TCPInfoSnapshot{}, fmt.Errorf("netlink socket: %w", err)
	}
	defer unix.Close(fd)

	if err := unix.Bind(fd, &unix.SockaddrNetlink{Family: unix.AF_NETLINK}); err != nil {
		return TCPInfoSnapshot{}, fmt.Errorf("netlink bind: %w", err)
	}

	req := buildInetDiagReq(family, srcIP, srcPort, dstIP, dstPort)
	sa := &unix.SockaddrNetlink{Family: unix.AF_NETLINK}
	if err := unix.Sendto(fd, req, 0, sa); err != nil {
		return TCPInfoSnapshot{}, fmt.Errorf("netlink send: %w", err)
	}

	buf := make([]byte, 32*1024)
	n, _, err := unix.Recvfrom(fd, buf, 0)
	if err != nil {
		return TCPInfoSnapshot{}, fmt.Errorf("netlink recv: %w", err)
	}
	buf = buf[:n]

	msgs, err := syscall.ParseNetlinkMessage(buf)
	if err != nil {
		return TCPInfoSnapshot{}, fmt.Errorf("parse netlink: %w", err)
	}
	for _, m := range msgs {
		switch m.Header.Type {
		case unix.NLMSG_DONE:
			return TCPInfoSnapshot{}, os.ErrNotExist
		case unix.NLMSG_ERROR:
			// Payload is int32 errno + original nlmsghdr; parse errno only.
			if len(m.Data) < 4 {
				return TCPInfoSnapshot{}, errors.New("netlink NLMSG_ERROR without payload")
			}
			errno := int32(binary.LittleEndian.Uint32(m.Data[:4]))
			if errno == 0 {
				continue // ack
			}
			return TCPInfoSnapshot{}, fmt.Errorf("netlink NLMSG_ERROR errno=%d", -errno)
		case unix.SOCK_DIAG_BY_FAMILY:
			// Data: inet_diag_msg (72 bytes) + attributes.
			info, ok := extractTCPInfoAttr(m.Data)
			if !ok {
				return TCPInfoSnapshot{}, errors.New("INET_DIAG_INFO attribute not found")
			}
			return tcpInfoToSnapshot(info), nil
		}
	}
	return TCPInfoSnapshot{}, os.ErrNotExist
}

// inetDiagReqV2Size is sizeof(struct inet_diag_req_v2) — 8 fixed bytes
// + 48-byte embedded inet_diag_sockid.
const inetDiagReqV2Size = 8 + 48

// inetDiagMsgSize is sizeof(struct inet_diag_msg) — 4 fixed bytes +
// 48-byte sockid + 20 bytes of counters.
const inetDiagMsgSize = 4 + 48 + 20

// INET_DIAG_INFO attribute type per include/uapi/linux/inet_diag.h.
const inetDiagInfo uint16 = 2

// buildInetDiagReq marshals nlmsghdr + inet_diag_req_v2 for a single
// 4-tuple query. IPv4 addresses land in the first 4 bytes of the
// 16-byte address slots; IPv6 fills all 16.
func buildInetDiagReq(family uint8, srcIP net.IP, srcPort uint16, dstIP net.IP, dstPort uint16) []byte {
	total := unix.NLMSG_HDRLEN + inetDiagReqV2Size
	buf := make([]byte, total)

	// nlmsghdr
	hdr := (*unix.NlMsghdr)(unsafe.Pointer(&buf[0]))
	hdr.Len = uint32(total)
	hdr.Type = unix.SOCK_DIAG_BY_FAMILY
	hdr.Flags = unix.NLM_F_REQUEST
	hdr.Seq = 1
	hdr.Pid = 0

	// inet_diag_req_v2 lives right after the nlmsghdr.
	body := buf[unix.NLMSG_HDRLEN:]
	body[0] = family                // sdiag_family
	body[1] = unix.IPPROTO_TCP      // sdiag_protocol
	body[2] = 1 << (inetDiagInfo - 1) // idiag_ext bitmask; bit for INET_DIAG_INFO
	body[3] = 0                     // pad
	binary.LittleEndian.PutUint32(body[4:8], 0xFFFFFFFF) // idiag_states: all states

	// inet_diag_sockid — ports are network-byte-order (big-endian),
	// addresses are network-byte-order too.
	sockid := body[8:]
	binary.BigEndian.PutUint16(sockid[0:2], srcPort)
	binary.BigEndian.PutUint16(sockid[2:4], dstPort)
	writeAddr(sockid[4:20], srcIP)
	writeAddr(sockid[20:36], dstIP)
	// idiag_if (4B) + idiag_cookie[2] (8B) already zero via make.
	// Cookie left at 0 (wildcard).

	return buf
}

// writeAddr copies IPv4 (4B) or IPv6 (16B) into the 16-byte slot,
// left-aligned. Kernel expects raw big-endian; net.IP is already in
// that form.
func writeAddr(dst []byte, ip net.IP) {
	if v4 := ip.To4(); v4 != nil {
		copy(dst[:4], v4)
		return
	}
	if v6 := ip.To16(); v6 != nil {
		copy(dst[:16], v6)
	}
}

// extractTCPInfoAttr walks the attribute chain that follows the
// inet_diag_msg header, returning the TCPInfo bytes cast to
// *unix.TCPInfo when the INET_DIAG_INFO attribute is present.
func extractTCPInfoAttr(payload []byte) (*unix.TCPInfo, bool) {
	if len(payload) < inetDiagMsgSize {
		return nil, false
	}
	attrs := payload[inetDiagMsgSize:]
	for len(attrs) >= unix.SizeofRtAttr {
		aLen := binary.LittleEndian.Uint16(attrs[0:2])
		aType := binary.LittleEndian.Uint16(attrs[2:4])
		if int(aLen) > len(attrs) || aLen < unix.SizeofRtAttr {
			return nil, false
		}
		if aType == inetDiagInfo {
			data := attrs[unix.SizeofRtAttr:aLen]
			if len(data) < int(unsafe.Sizeof(unix.TCPInfo{})) {
				return nil, false
			}
			return (*unix.TCPInfo)(unsafe.Pointer(&data[0])), true
		}
		// rtattr entries are 4-byte-aligned.
		aligned := (int(aLen) + 3) &^ 3
		if aligned > len(attrs) {
			return nil, false
		}
		attrs = attrs[aligned:]
	}
	return nil, false
}
