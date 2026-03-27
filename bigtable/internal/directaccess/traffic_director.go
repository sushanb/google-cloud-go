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

package directaccess

import (
	"context"
	"fmt"
	"net"
	"strconv"

	btopt "cloud.google.com/go/bigtable/internal/option"
	clusterpb "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	corepb "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	endpointpb "github.com/envoyproxy/go-control-plane/envoy/config/endpoint/v3"
	discoverypb "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/google"
)

const (
	XdsTarget     = "directpath-pa.googleapis.com:443"
	XdsCdsTypeUrl = "type.googleapis.com/envoy.config.cluster.v3.Cluster"
	XdsEdsTypeUrl = "type.googleapis.com/envoy.config.endpoint.v3.ClusterLoadAssignment"
)

// FetchXdsEndpoints connects to Traffic Director, performs a CDS check for the given cluster URI,
// extracts the EDS service name, and fetches the endpoints (IPs) using a single multiplexed ADS stream.
func FetchXdsEndpoints(ctx context.Context, nodeID, zone, cdsResourceURI string) ([]string, string, error) {
	btopt.Debugf(nil, "directaccess: Dialing Traffic Director at %s", XdsTarget)
	// 1. Dial Traffic Director using Google Default Credentials (ALTS/TLS)
	conn, err := grpc.NewClient(XdsTarget, grpc.WithCredentialsBundle(google.NewDefaultCredentials()))
	if err != nil {
		return nil, "xds_reachability_failed", fmt.Errorf("failed to create xDS client: %w", err)
	}
	defer conn.Close()

	client := discoverypb.NewAggregatedDiscoveryServiceClient(conn)
	stream, err := client.StreamAggregatedResources(ctx)
	if err != nil {
		return nil, "xds_reachability_failed", fmt.Errorf("failed to create ADS stream: %w", err)
	}

	node := &corepb.Node{
		Id:       nodeID,
		Locality: &corepb.Locality{Zone: zone},
	}

	btopt.Debugf(nil, "directaccess: Sending CDS request for URI: %s", cdsResourceURI)

	// ----------------------------------------------------------------
	// Step 1: CDS (Cluster) Request to verify control-plane routing
	// ----------------------------------------------------------------
	cdsReq := &discoverypb.DiscoveryRequest{
		Node:          node,
		TypeUrl:       XdsCdsTypeUrl,
		ResourceNames: []string{cdsResourceURI},
	}
	if err := stream.Send(cdsReq); err != nil {
		return nil, "xds_reachability_failed", fmt.Errorf("failed to send CDS request: %w", err)
	}

	cdsResp, err := stream.Recv()
	if err != nil {
		return nil, "xds_reachability_failed", fmt.Errorf("failed to receive CDS response: %w", err)
	}

	// Extract the EDS Resource Name from the CDS Response
	var edsResourceName string
	for _, res := range cdsResp.GetResources() {
		if res.TypeUrl == XdsCdsTypeUrl {
			cluster := &clusterpb.Cluster{}
			if err := res.UnmarshalTo(cluster); err == nil {
				// Envoy uses the EdsClusterConfig ServiceName if present, otherwise defaults to the CDS name itself
				edsConfig := cluster.GetEdsClusterConfig()
				if edsConfig != nil && edsConfig.GetServiceName() != "" {
					edsResourceName = edsConfig.GetServiceName()
				} else {
					edsResourceName = cluster.GetName()
				}
				break
			}
		}
	}

	if edsResourceName == "" {
		// Fallback to the CDS URI if we couldn't parse the cluster properly but received a response
		edsResourceName = cdsResourceURI
	}

	btopt.Debugf(nil, "directaccess: Sending EDS request for cluster name: %s", edsResourceName)
	// ----------------------------------------------------------------
	// Step 2: EDS (Endpoint) Request on the SAME stream to get backends
	// ----------------------------------------------------------------
	edsReq := &discoverypb.DiscoveryRequest{
		Node:          node,
		TypeUrl:       XdsEdsTypeUrl,
		ResourceNames: []string{edsResourceName},
	}
	if err := stream.Send(edsReq); err != nil {
		return nil, "xds_eds_failed", fmt.Errorf("failed to send EDS request: %w", err)
	}

	edsResp, err := stream.Recv()
	if err != nil {
		return nil, "xds_eds_failed", fmt.Errorf("failed to receive EDS response: %w", err)
	}

	// ----------------------------------------------------------------
	// Step 3: Parse EDS Response to extract IP addresses
	// ----------------------------------------------------------------
	var discoveredIPs []string
	for _, res := range edsResp.GetResources() {
		if res.TypeUrl == XdsEdsTypeUrl {
			cla := &endpointpb.ClusterLoadAssignment{}
			if err := res.UnmarshalTo(cla); err == nil {
				for _, locality := range cla.Endpoints {
					for _, lbEndpoint := range locality.LbEndpoints {
						addr := lbEndpoint.GetEndpoint().GetAddress().GetSocketAddress()
						ipStr := addr.GetAddress()
						portStr := strconv.Itoa(int(addr.GetPortValue()))

						discoveredIPs = append(discoveredIPs, net.JoinHostPort(ipStr, portStr))
					}
				}
			}
		}
	}

	if len(discoveredIPs) == 0 {
		btopt.Debugf(nil, "directaccess: Traffic Director returned an EDS response, but with zero endpoints")
		return nil, "xds_eds_failed", fmt.Errorf("found no endpoints in the EDS response")
	}
	btopt.Debugf(nil, "directaccess: Traffic Director successfully returned %d endpoints", len(discoveredIPs))

	return discoveredIPs, "", nil
}
