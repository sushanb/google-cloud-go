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
	"net"
	"strconv"
)

// QueryTCPInfoForSessionEntry adapts a SessionIPEntry (stringy addrs)
// to a queryTCPInfoByAddrs call (IPs + ports). The returned snapshot
// always carries the entry's address fields even when the netlink
// query fails — the tcpz row is still useful showing "session exists,
// TCP_INFO unavailable, err=..." rather than getting dropped silently.
//
// Exported (unusual for internal pkg) because the top-level bigtable
// package's TCPStats.Snapshot needs it and there's no cleaner place
// to put the adapter — it depends on both the registry entry type
// and the platform-conditional queryTCPInfoByAddrs.
func QueryTCPInfoForSessionEntry(e SessionIPEntry) TCPInfoSnapshot {
	snap := TCPInfoSnapshot{
		RemoteAddr: e.RemoteAddr,
		LocalAddr:  e.LocalAddr,
		DialedAt:   e.DialedAt,
	}
	srcIP, srcPort, ok := parseIPPort(e.LocalAddr)
	if !ok {
		snap.Err = "unparseable local addr: " + e.LocalAddr
		return snap
	}
	dstIP, dstPort, ok := parseIPPort(e.RemoteAddr)
	if !ok {
		snap.Err = "unparseable remote addr: " + e.RemoteAddr
		return snap
	}
	info, err := queryTCPInfoByAddrs(srcIP, srcPort, dstIP, dstPort)
	if err != nil {
		snap.Err = err.Error()
		return snap
	}
	// queryTCPInfoByAddrs already populated the TCP_INFO fields via
	// tcpInfoToSnapshot; splice the address block from the registry
	// (queryTCPInfoByAddrs doesn't know the entry's DialedAt).
	info.RemoteAddr = e.RemoteAddr
	info.LocalAddr = e.LocalAddr
	info.DialedAt = e.DialedAt
	return info
}

// parseIPPort accepts "ip:port" (IPv4) or "[ipv6]:port" (IPv6), the
// two forms net.Addr.String() produces for TCP peers.
func parseIPPort(s string) (net.IP, uint16, bool) {
	if s == "" {
		return nil, 0, false
	}
	host, portStr, err := net.SplitHostPort(s)
	if err != nil {
		return nil, 0, false
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return nil, 0, false
	}
	p, err := strconv.ParseUint(portStr, 10, 16)
	if err != nil {
		return nil, 0, false
	}
	return ip, uint16(p), true
}
