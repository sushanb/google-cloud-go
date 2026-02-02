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
)

// CheckLoopbackInterfaceUp verifies that at least one loopback interface exists
// and is marked as UP.
func CheckLoopbackInterfaceUp() error {
	ifaces, err := net.Interfaces()
	if err != nil {
		return fmt.Errorf("failed to list network interfaces: %w", err)
	}

	for _, iface := range ifaces {
		// Check if it's a loopback interface
		if iface.Flags&net.FlagLoopback != 0 {
			// Check if it is UP
			if iface.Flags&net.FlagUp != 0 {
				log.Printf("Loopback interface found and UP: %s", iface.Name)
				return nil
			}
			log.Printf("Warning: Loopback interface found but DOWN: %s", iface.Name)
		}
	}

	return fmt.Errorf("no loopback interface found in UP state")
}

func skipLoopback(iface net.Interface) error {
	if iface.Flags&net.FlagLoopback != 0 {
		return fmt.Errorf("interface has loopback flag")
	}
	if iface.Flags&net.FlagUp != net.FlagUp {
		return fmt.Errorf("interface is not marked up")
	}
	return nil
}

func onlyLoopback(iface net.Interface) error {
	if iface.Flags&net.FlagLoopback == 0 {
		return fmt.Errorf("interface does not have loopback flag")
	}
	if iface.Flags&net.FlagUp != net.FlagUp {
		return fmt.Errorf("interface is not marked up")
	}
	return nil
}

// CheckLocalIPv6Addresses verifies that the IPv6 address assigned to the VM by the metadata server
// is actually configured on a local network interface (excluding loopback).
func CheckLocalIPv6Addresses(ipv6FromMetadataServer *net.IP) (*net.Interface, error) {
	if ipv6FromMetadataServer == nil {
		return nil, fmt.Errorf("no IPv6 address from metadata server to check")
	}
	log.Println("Checking for local IPv6 address interface...")
	return findLocalAddress(func(ip net.IP) bool { return ip.To4() == nil && ip.Equal(*ipv6FromMetadataServer) }, skipLoopback)
}

// CheckLocalIPv6LoopbackAddress checks for the presence of the IPv6 loopback address (::1)
// on a local network interface.
func CheckLocalIPv6LoopbackAddress() error {
	log.Println("Checking for local IPv6 loopback address (::1)...")
	ipv6Loopback := net.ParseIP("::1")
	_, err := findLocalAddress(func(ip net.IP) bool { return ip.Equal(ipv6Loopback) }, onlyLoopback)
	return err
}

// CheckLocalIPv4Addresses verifies that the IPv4 address assigned to the VM by the metadata server
// is actually configured on a local network interface (excluding loopback).
func CheckLocalIPv4Addresses(ipv4FromMetadataServer *net.IP) (*net.Interface, error) {
	if ipv4FromMetadataServer == nil {
		return nil, fmt.Errorf("no IPv4 address from metadata server to check")
	}
	log.Println("Checking for local IPv4 address interface...")
	return findLocalAddress(func(ip net.IP) bool { return ip.To4() != nil && ip.Equal(*ipv4FromMetadataServer) }, skipLoopback)
}

// CheckLocalIPv4LoopbackAddress checks for the presence of the IPv4 loopback address (127.0.0.1).
func CheckLocalIPv4LoopbackAddress() error {
	log.Println("Checking for local IPv4 loopback address (127.0.0.1)...")
	ipv4Loopback := net.ParseIP("127.0.0.1")
	_, err := findLocalAddress(func(ip net.IP) bool { return ip.Equal(ipv4Loopback) }, onlyLoopback)
	return err
}

func findLocalAddress(ipMatches func(net.IP) bool, ifaceFilter func(iface net.Interface) error) (*net.Interface, error) {
	ifaces, err := net.Interfaces()
	if err != nil {
		return nil, err
	}
	var match *net.Interface
	foundMatch := false
	for _, iface := range ifaces {
		currentIface := iface // Capture range variable
		if err := ifaceFilter(currentIface); err != nil {
			continue
		}
		ifaddrs, err := currentIface.Addrs()
		if err != nil {
			continue
		}
		for _, ifaddr := range ifaddrs {
			ip := ifaddr.(*net.IPNet).IP
			if ipMatches(ip) {
				if foundMatch {
					log.Printf("Warning: Found multiple interfaces with matching IP. Using first one: %s", match.Name)
				} else {
					match = &currentIface
					foundMatch = true
				}
			}
		}
	}
	if !foundMatch {
		return nil, fmt.Errorf("failed to find matching address on any interface")
	}
	log.Printf("Found matching IP on interface %s", match.Name)
	return match, nil
}
