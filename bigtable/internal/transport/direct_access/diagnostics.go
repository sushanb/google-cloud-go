// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
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
	"log"
	"net"
)

// Check defines a single diagnostic step.
type Check interface {
	// Name returns the display name of the check.
	Name() string
	// Stage returns the category or phase of the check.
	Stage() string
	// Execute performs the check. Returns true if successful, false otherwise, along with error details.
	Execute() (bool, error)
}

// -----------------------------------------------------------------------------
// Stage 1: Environment & Metadata
// -----------------------------------------------------------------------------

type GCPEnvCheck struct{}

func (c *GCPEnvCheck) Name() string  { return "GCP Environment Check" }
func (c *GCPEnvCheck) Stage() string { return "Environment" }
func (c *GCPEnvCheck) Execute() (bool, error) {
	if err := IsRunningOnGCP(); err != nil {
		return false, err
	}
	return true, nil
}

type MetadataServerCheck struct{}

func (c *MetadataServerCheck) Name() string  { return "Metadata Server Reachability" }
func (c *MetadataServerCheck) Stage() string { return "Metadata" }
func (c *MetadataServerCheck) Execute() (bool, error) {
	if err := CheckMetadataServerReachability(); err != nil {
		return false, err
	}
	return true, nil
}

type MetadataIPFetchCheck struct {
	AddrFamily string  // "IPv4" or "IPv6"
	ResultIP   *net.IP // Output: Stores the fetched IP
}

func (c *MetadataIPFetchCheck) Name() string {
	return fmt.Sprintf("Fetch %s from Metadata", c.AddrFamily)
}
func (c *MetadataIPFetchCheck) Stage() string { return "Metadata" }
func (c *MetadataIPFetchCheck) Execute() (bool, error) {
	ip, err := FetchIPFromMetadataServer(c.AddrFamily)
	if err != nil {
		return false, err
	}
	c.ResultIP = ip
	return true, nil
}

// -----------------------------------------------------------------------------
// Stage 2: Local Network
// -----------------------------------------------------------------------------

type LoopbackInterfaceCheck struct{}

func (c *LoopbackInterfaceCheck) Name() string  { return "Loopback Interface UP Status" }
func (c *LoopbackInterfaceCheck) Stage() string { return "Local Network" }
func (c *LoopbackInterfaceCheck) Execute() (bool, error) {
	if err := CheckLoopbackInterfaceUp(); err != nil {
		return false, err
	}
	return true, nil
}

type LoopbackAddressCheck struct {
	AddrFamily string // "IPv4" or "IPv6"
}

func (c *LoopbackAddressCheck) Name() string {
	return fmt.Sprintf("Local %s Loopback Addr Config", c.AddrFamily)
}
func (c *LoopbackAddressCheck) Stage() string { return "Local Network" }
func (c *LoopbackAddressCheck) Execute() (bool, error) {
	var err error
	if c.AddrFamily == "IPv6" {
		err = CheckLocalIPv6LoopbackAddress()
	} else {
		err = CheckLocalIPv4LoopbackAddress()
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// -----------------------------------------------------------------------------
// Stage 3: xDS (Control Plane)
// -----------------------------------------------------------------------------

type XdsDiscoveryCheck struct {
	NodeID string
	Zone   string
}

func (c *XdsDiscoveryCheck) Name() string  { return "Traffic Director Discovery (LDS)" }
func (c *XdsDiscoveryCheck) Stage() string { return "Control Plane" }
func (c *XdsDiscoveryCheck) Execute() (bool, error) {
	ctx := context.Background()
	if err := CallXdsDiscovery(ctx, c.NodeID, c.Zone); err != nil {
		return false, err
	}
	return true, nil
}

type XdsEdsCheck struct {
	NodeID          string
	Zone            string
	ClusterResource string
	ResultIPs       []string // Output: Stores discovered IPs
}

func (c *XdsEdsCheck) Name() string  { return "Traffic Director EDS (Backend Lookup)" }
func (c *XdsEdsCheck) Stage() string { return "Control Plane" }
func (c *XdsEdsCheck) Execute() (bool, error) {
	ctx := context.Background()
	ips, err := CallXdsEndpointDiscovery(ctx, c.NodeID, c.Zone, c.ClusterResource)
	if err != nil {
		return false, err
	}
	c.ResultIPs = ips
	return true, nil
}

// -----------------------------------------------------------------------------
// Stage 4: Data Plane Routability
// -----------------------------------------------------------------------------

type RouteCheck struct {
	AddrFamily     string
	LocalAddress   *net.IP
	BackendAddress string
}

func (c *RouteCheck) Name() string  { return fmt.Sprintf("Kernel Route Check (%s)", c.AddrFamily) }
func (c *RouteCheck) Stage() string { return "Data Plane" }
func (c *RouteCheck) Execute() (bool, error) {
	var err error
	if c.AddrFamily == "IPv6" {
		err = CheckLocalIPv6Routes(c.LocalAddress, c.BackendAddress)
	} else {
		err = CheckLocalIPv4Routes(c.LocalAddress, c.BackendAddress)
	}

	if err != nil {
		return false, err
	}
	return true, nil
}

type DirectPathConnectivityCheck struct {
	Project    string
	Instance   string
	AppProfile string
}

func (c *DirectPathConnectivityCheck) Name() string  { return "DirectPath Connectivity (C2P)" }
func (c *DirectPathConnectivityCheck) Stage() string { return "Connectivity" }
func (c *DirectPathConnectivityCheck) Execute() (bool, error) {
	ctx := context.Background()
	fullInstanceName := fmt.Sprintf("projects/%s/instances/%s", c.Project, c.Instance)
	if err := CallSingleChannel(ctx, fullInstanceName, c.AppProfile); err != nil {
		return false, err
	}
	return true, nil
}

// -----------------------------------------------------------------------------
// Orchestration
// -----------------------------------------------------------------------------

// runCheckOrExit executes a check. If it fails, it prints the error and Exits the program.
func runCheckOrExit(c Check) {
	fmt.Printf("[%s] Running %s... ", c.Stage(), c.Name())
	passed, err := c.Execute()
	if passed {
		fmt.Println("PASSED")
	} else {
		fmt.Printf("FAILED\n")
		log.Fatalf("Critical Check Failed: %v", err) // Exit with code 1
	}
}

// runCheckQuiet executes a check but returns the status instead of exiting.
func runCheckQuiet(c Check) bool {
	fmt.Printf("[%s] Running %s... ", c.Stage(), c.Name())
	passed, err := c.Execute()
	if passed {
		fmt.Println("PASSED")
		return true
	}
	fmt.Printf("SKIPPED/FAILED (%v)\n", err)
	return false
}

func RunDiagnostics(project, instance, appProfile, zone string) {
	fmt.Println("===============================================================")
	fmt.Println("       Starting Bigtable DirectPath Diagnostic Suite")
	fmt.Println("===============================================================")

	// 1. RunOnGce (Exit if fails)
	runCheckOrExit(&GCPEnvCheck{})

	// 2. MetadataServerCheck (Exit if fails)
	runCheckOrExit(&MetadataServerCheck{})

	// 3. Determine IP Family (IPv6 Priority)
	var activeFamily string
	var activeMetadataIP *net.IP

	ipv6Check := &MetadataIPFetchCheck{AddrFamily: "IPv6"}
	if runCheckQuiet(ipv6Check) {
		activeFamily = "IPv6"
		activeMetadataIP = ipv6Check.ResultIP
		log.Println("Mode: IPv6 Detected. Using IPv6 for all subsequent checks.")
	} else {
		ipv4Check := &MetadataIPFetchCheck{AddrFamily: "IPv4"}
		if runCheckQuiet(ipv4Check) {
			activeFamily = "IPv4"
			activeMetadataIP = ipv4Check.ResultIP
			log.Println("Mode: IPv4 Detected. Using IPv4 for all subsequent checks.")
		} else {
			log.Fatal("Critical Failure: Unable to fetch neither IPv6 nor IPv4 address from Metadata server. Exiting.")
		}
	}

	// 4. LoopbackInterfaceCheck (Exit if fails)
	runCheckOrExit(&LoopbackInterfaceCheck{})

	// 5. LoopbackAddressCheck based on family (Exit if fails)
	runCheckOrExit(&LoopbackAddressCheck{AddrFamily: activeFamily})

	// 6. CallXdsDiscovery (Exit if fails)
	runCheckOrExit(&XdsDiscoveryCheck{NodeID: "test-node", Zone: zone})

	// 7. CallXdsEndpointDiscovery (Exit if fails)
	// We use a hardcoded Bigtable EDS cluster name for diagnostic purposes
	const edsClusterName = "xdstp://traffic-director-c2p.xds.googleapis.com/envoy.config.cluster.v3.Cluster/us-east7-bigtable.googleapis.com/eds_cluster"

	edsCheck := &XdsEdsCheck{
		NodeID:          "test-node",
		Zone:            zone,
		ClusterResource: edsClusterName,
	}
	runCheckOrExit(edsCheck)

	// 8. CheckLocalRoutes using the discovered IP (Exit if fails)
	// We use the first discovered IP from the EDS check
	if len(edsCheck.ResultIPs) == 0 {
		log.Fatal("Critical Failure: EDS succeeded but returned 0 endpoints. Cannot perform Route Check.")
	}
	targetEndpoint := edsCheck.ResultIPs[0]

	runCheckOrExit(&RouteCheck{
		AddrFamily:     activeFamily,
		LocalAddress:   activeMetadataIP, // Bound to the source IP fetched in Step 3
		BackendAddress: targetEndpoint,   // Trying to reach the endpoint found in Step 7
	})

	// 9. CallSingleChannel (Exit if fails)
	runCheckOrExit(&DirectPathConnectivityCheck{
		Project:    project,
		Instance:   instance,
		AppProfile: appProfile,
	})

	fmt.Println("===============================================================")
	fmt.Println("                 Diagnostics Complete")
	fmt.Println("===============================================================")
}
