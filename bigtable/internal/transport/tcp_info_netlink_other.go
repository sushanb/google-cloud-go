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

//go:build !linux

package internal

import "net"

// queryTCPInfoByAddrs on non-Linux is a stub that returns the
// unsupported sentinel. Callers upstream render a TCPInfoSnapshot with
// only the address fields populated + Err set.
func queryTCPInfoByAddrs(srcIP net.IP, srcPort uint16, dstIP net.IP, dstPort uint16) (TCPInfoSnapshot, error) {
	return TCPInfoSnapshot{}, ErrTCPInfoUnsupported
}

// TCPInfoAttrDebug always empty on non-Linux — no netlink to talk to.
func TCPInfoAttrDebug() []uint16 { return nil }

// TCPInfoScanDebug always empty on non-Linux.
func TCPInfoScanDebug() []string { return nil }
