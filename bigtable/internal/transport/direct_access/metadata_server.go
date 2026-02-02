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
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"time"
)

// note this is listening on port 80.
const (
	MetadataBaseURL = "http://metadata.google.internal/computeMetadata/v1/"
	MetadataIPURL   = "http://metadata.google.internal/computeMetadata/v1/instance/network-interfaces/0/ip"
	MetadataIPv6URL = "http://metadata.google.internal/computeMetadata/v1/instance/network-interfaces/0/ipv6s"
)

// CheckMetadataServerReachability performs a basic connectivity check to the GCE metadata server.
// It verifies DNS resolution, TCP connection, and correct header validation.
func CheckMetadataServerReachability() error {
	log.Printf("Checking GCE Metadata Server reachability at %s...", MetadataBaseURL)

	req, err := http.NewRequest("GET", MetadataBaseURL, nil)
	if err != nil {
		return fmt.Errorf("failed to create metadata request: %w", err)
	}
	// The Metadata-Flavor header is required for all requests to the v1 metadata server.
	req.Header.Add("Metadata-Flavor", "Google")

	client := &http.Client{
		// Set a short timeout to fail fast if the metadata server is unreachable
		// (e.g., if running locally or firewall rules block it).
		Timeout: 2 * time.Second,
	}

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to connect to GCE Metadata Server: %w. (Are you running on a GCE VM?)", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("GCE Metadata Server is reachable but returned status code: %d", resp.StatusCode)
	}

	log.Println("GCE Metadata Server is accessible.")
	return nil
}

// FetchIPFromMetadataServer fetches the IP address (IPv4 or IPv6) from the metadata server.
// This confirms the VM has been *assigned* the necessary IPs by GCP.
func FetchIPFromMetadataServer(addrFamilyStr string) (*net.IP, error) {
	var metadataServerURL string
	switch addrFamilyStr {
	case "IPv4":
		metadataServerURL = MetadataIPURL
	case "IPv6":
		metadataServerURL = MetadataIPv6URL
	default:
		return nil, fmt.Errorf("invalid address family %v is not IPv4 or IPv6", addrFamilyStr)
	}

	log.Printf("Fetching %s address from metadata server: %s", addrFamilyStr, metadataServerURL)
	req, err := http.NewRequest("GET", metadataServerURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Add("Metadata-Flavor", "Google")

	client := &http.Client{
		Timeout: 5 * time.Second,
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode == 200 {
		address := net.ParseIP(strings.TrimSuffix(string(body), "\n"))
		if address == nil {
			return nil, fmt.Errorf("failed to parse IP address from metadata server response: %s", string(body))
		}
		log.Printf("Received %s address %s from metadata server", addrFamilyStr, address)
		return &address, nil
	}
	if resp.StatusCode == 404 {
		return nil, fmt.Errorf("this VM doesn't have a %s address allocated to its primary network interface", addrFamilyStr)
	}
	return nil, fmt.Errorf("received status code %d in response to metadata server GET request to URL: %s", resp.StatusCode, metadataServerURL)
}
