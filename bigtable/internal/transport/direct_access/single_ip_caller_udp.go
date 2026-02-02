// Copyright 2025 Google LLC
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
	"log"
	"net"
	"strconv"
	"syscall"
)

// CheckLocalIPv6Routes checks if the kernel can route traffic to the given IPv6 backend address
// using the VM's DirectPath IPv6 address.
func CheckLocalIPv6Routes(localAddress *net.IP, backendAddress string) error {
	if localAddress == nil {
		return fmt.Errorf("skipping IPv6 route check, no local IPv6 address available")
	}
	if backendAddress == "" {
		return fmt.Errorf("skipping IPv6 route check, no backend address provided")
	}

	destIPStr, destPortStr, err := net.SplitHostPort(backendAddress)
	if err != nil {
		return fmt.Errorf("failed to split backend address: %v into host and port components: %w", backendAddress, err)
	}

	destIP := net.ParseIP(destIPStr)
	if destIP == nil || destIP.To4() != nil {
		return fmt.Errorf("backend address %s is not a valid IPv6 address", backendAddress)
	}

	destPort, err := strconv.Atoi(destPortStr)
	if err != nil {
		return fmt.Errorf("failed to parse port from backend address: %v: %w", backendAddress, err)
	}

	sourceStr := net.JoinHostPort(localAddress.String(), "0")
	log.Printf("Checking kernel routability for IPv6: %s -> %s", sourceStr, backendAddress)

	// Create a raw UDP socket
	fd, err := syscall.Socket(syscall.AF_INET6, syscall.SOCK_DGRAM, syscall.IPPROTO_UDP)
	if err != nil {
		return fmt.Errorf("error creating IPv6/UDP socket: %w", err)
	}
	defer syscall.Close(fd)

	// Bind to the specific local source address
	source := &syscall.SockaddrInet6{Port: 0}
	copy(source.Addr[:], (*localAddress))
	if err := syscall.Bind(fd, source); err != nil {
		return fmt.Errorf("error binding UDP/IPV6 socket to %s: %w", sourceStr, err)
	}

	// Try to connect to the destination
	dest := &syscall.SockaddrInet6{Port: destPort}
	copy(dest.Addr[:], destIP)
	if err := syscall.Connect(fd, dest); err != nil {
		return fmt.Errorf("failed to connect UDP socket (source: %s) to dest: %s, err: %w. This indicates the DirectPath/IPv6 backends aren't routable", sourceStr, backendAddress, err)
	}

	return nil
}

// CheckLocalIPv4Routes checks if the kernel can route traffic to the given IPv4 backend address
// using the VM's primary IPv4 address.
func CheckLocalIPv4Routes(localAddress *net.IP, backendAddress string) error {
	if localAddress == nil {
		return fmt.Errorf("skipping IPv4 route check, no local IPv4 address available")
	}
	if backendAddress == "" {
		return fmt.Errorf("skipping IPv4 route check, no backend address provided")
	}

	destIPStr, destPortStr, err := net.SplitHostPort(backendAddress)
	if err != nil {
		return fmt.Errorf("failed to split backend address: %v into host and port components: %w", backendAddress, err)
	}

	destIP := net.ParseIP(destIPStr)
	if destIP == nil || destIP.To4() == nil {
		return fmt.Errorf("backend address %s is not a valid IPv4 address", backendAddress)
	}

	destPort, err := strconv.Atoi(destPortStr)
	if err != nil {
		return fmt.Errorf("failed to parse port from backend address: %v: %w", backendAddress, err)
	}

	sourceStr := net.JoinHostPort(localAddress.String(), "0")
	log.Printf("Checking kernel routability for IPv4: %s -> %s", sourceStr, backendAddress)

	// Create a raw UDP socket
	fd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_DGRAM, syscall.IPPROTO_UDP)
	if err != nil {
		return fmt.Errorf("error creating IPv4/UDP socket: %w", err)
	}
	defer syscall.Close(fd)

	// Bind to the specific local source address
	source := &syscall.SockaddrInet4{Port: 0}
	copy(source.Addr[:], (*localAddress).To4())
	if err := syscall.Bind(fd, source); err != nil {
		return fmt.Errorf("error binding UDP/IPV4 socket to %s: %w", sourceStr, err)
	}

	// Try to connect to the destination
	dest := &syscall.SockaddrInet4{Port: destPort}
	copy(dest.Addr[:], destIP.To4())
	if err := syscall.Connect(fd, dest); err != nil {
		return fmt.Errorf("failed to connect UDP socket (source: %s) to dest: %s, err: %w. This indicates the DirectPath/IPv4 backends aren't routable", sourceStr, backendAddress, err)
	}

	return nil
}
