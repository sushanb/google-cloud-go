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
	"context"
	"fmt"
	"log"
	"net"
	"strconv"

	"cloud.google.com/go/bigtable/internal"
	corepb "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	endpointpb "github.com/envoyproxy/go-control-plane/envoy/config/endpoint/v3"
	listenerpb "github.com/envoyproxy/go-control-plane/envoy/config/listener/v3"
	discoverypb "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/google"
	"google.golang.org/protobuf/encoding/protojson"
)

const (
	xdsTarget        = "directpath-pa.googleapis.com:443"
	xdsResourceName  = "xdstp://traffic-director-c2p.xds.googleapis.com/envoy.config.listener.v3.Listener/bigtable.googleapis.com"
	xdsTypeUrl       = "type.googleapis.com/envoy.config.listener.v3.Listener"
	xdsEdsTypeUrl    = "type.googleapis.com/envoy.config.endpoint.v3.ClusterLoadAssignment"
	defaultUserAgent = "gRPC Go"
)

// CallXdsDiscovery connects to the Traffic Director control plane and requests the
// Bigtable listener resource.
func CallXdsDiscovery(ctx context.Context, nodeID, zone string) error {
	// 1. Create connection with Google Default Credentials (ALTS/Compute creds)
	// NewClient returns immediately (lazy connection).
	conn, err := grpc.NewClient(xdsTarget,
		grpc.WithCredentialsBundle(google.NewDefaultCredentials()),
	)
	if err != nil {
		return fmt.Errorf("failed to create client for xDS target %s: %w", xdsTarget, err)
	}
	defer conn.Close()

	client := discoverypb.NewAggregatedDiscoveryServiceClient(conn)

	// 2. Start the bidirectional stream
	// The context here controls the stream lifetime and connection timeout.
	stream, err := client.StreamAggregatedResources(ctx)
	if err != nil {
		return fmt.Errorf("failed to create ADS stream: %w", err)
	}

	// 3. Construct the Node identifier
	node := &corepb.Node{
		Id: nodeID,
		Locality: &corepb.Locality{
			Zone: zone,
		},
		UserAgentName: fmt.Sprintf("go-bigtable/%v", internal.Version),
		ClientFeatures: []string{
			"envoy.lb.does_not_support_overprovisioning",
			"xds.config.resource-in-sotw",
		},
	}

	// 4. Construct the DiscoveryRequest
	req := &discoverypb.DiscoveryRequest{
		Node:          node,
		TypeUrl:       xdsTypeUrl,
		ResourceNames: []string{xdsResourceName},
	}

	log.Printf("Sending DiscoveryRequest for: %s (UserAgent: %s %s)", xdsResourceName, defaultUserAgent, grpc.Version)
	if err := stream.Send(req); err != nil {
		return fmt.Errorf("failed to send discovery request: %w", err)
	}

	// 5. Receive and Print Response
	resp, err := stream.Recv()
	if err != nil {
		return fmt.Errorf("failed to receive discovery response: %w", err)
	}

	log.Printf("Received DiscoveryResponse:")
	log.Printf("Version Info: %s", resp.GetVersionInfo())
	log.Printf("Type URL: %s", resp.GetTypeUrl())

	// Unmarshal and pretty-print resources
	marshaler := protojson.MarshalOptions{
		Multiline:       true,
		Indent:          "  ",
		UseProtoNames:   true,
		EmitUnpopulated: true,
	}

	for _, res := range resp.GetResources() {
		// Attempt to unmarshal as Listener to see details
		if res.TypeUrl == xdsTypeUrl {
			listener := &listenerpb.Listener{}

			if err := res.UnmarshalTo(listener); err != nil {
				log.Printf("Failed to unmarshal Any to Listener: %v", err)
				continue
			}

			jsonStr, err := marshaler.Marshal(listener)
			if err != nil {
				log.Printf("Failed to marshal listener to JSON: %v", err)
				continue
			}
			fmt.Printf("Parsed Listener JSON:\n%s\n", string(jsonStr))
		} else {
			log.Printf("Skipping non-listener resource: %s", res.TypeUrl)
		}
	}

	return nil
}

// CallXdsEndpointDiscovery connects to Traffic Director and fetches the endpoints (IPs)
// for a specific EDS cluster resource name.
// It returns a slice of IP:Port strings found in the response.
func CallXdsEndpointDiscovery(ctx context.Context, nodeID, zone, clusterResourceName string) ([]string, error) {
	// 1. Create connection
	conn, err := grpc.NewClient(xdsTarget,
		grpc.WithCredentialsBundle(google.NewDefaultCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create client for xDS target %s: %w", xdsTarget, err)
	}
	defer conn.Close()

	client := discoverypb.NewAggregatedDiscoveryServiceClient(conn)

	// 2. Start stream
	stream, err := client.StreamAggregatedResources(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create ADS stream: %w", err)
	}

	// 3. Construct Node
	node := &corepb.Node{
		Id: nodeID,
		Locality: &corepb.Locality{
			Zone: zone,
		},
		UserAgentName: fmt.Sprintf("go-bigtable/%v", internal.Version),
		ClientFeatures: []string{
			"envoy.lb.does_not_support_overprovisioning",
			"xds.config.resource-in-sotw",
		},
	}

	// 4. Construct EDS Request
	req := &discoverypb.DiscoveryRequest{
		Node:          node,
		TypeUrl:       xdsEdsTypeUrl,
		ResourceNames: []string{clusterResourceName},
	}

	log.Printf("Sending EDS DiscoveryRequest for: %s", clusterResourceName)
	if err := stream.Send(req); err != nil {
		return nil, fmt.Errorf("failed to send EDS request: %w", err)
	}

	// 5. Receive Response
	resp, err := stream.Recv()
	if err != nil {
		return nil, fmt.Errorf("failed to receive EDS response: %w", err)
	}

	// 6. Parse Resources and Collect IPs
	var discoveredIPs []string
	foundEndpoints := false

	for _, res := range resp.GetResources() {
		if res.TypeUrl == xdsEdsTypeUrl {
			cla := &endpointpb.ClusterLoadAssignment{}
			if err := res.UnmarshalTo(cla); err != nil {
				log.Printf("Failed to unmarshal to ClusterLoadAssignment: %v", err)
				continue
			}
			foundEndpoints = true

			// Extract IPs
			for _, locality := range cla.Endpoints {
				for _, lbEndpoint := range locality.LbEndpoints {
					endpoint := lbEndpoint.GetEndpoint()
					addr := endpoint.GetAddress().GetSocketAddress()

					// Format as IP:Port (e.g., "10.0.0.1:443" or "[2001:db8::1]:443")
					ipStr := addr.GetAddress()
					port := addr.GetPortValue()

					// Basic check to see if it's an IPv6 literal that needs brackets
					// (Simple heuristic: if it contains colon, wrap in brackets)
					fullAddr := fmt.Sprintf("%s:%d", ipStr, port)
					if len(ipStr) > 0 {
						// Using net.JoinHostPort is safer
						fullAddr = net.JoinHostPort(ipStr, strconv.Itoa(int(port)))
					}

					discoveredIPs = append(discoveredIPs, fullAddr)
				}
			}
		}
	}

	if !foundEndpoints {
		return nil, fmt.Errorf("received response but found no ClusterLoadAssignment resources")
	}

	if len(discoveredIPs) == 0 {
		return nil, fmt.Errorf("found ClusterLoadAssignment but no endpoints (IPs) were present")
	}

	log.Printf("Discovered %d endpoints from EDS.", len(discoveredIPs))
	return discoveredIPs, nil
}
