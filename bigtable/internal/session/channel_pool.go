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

package session

import (
	"context"
	"encoding/base64"

	btpb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
	"google.golang.org/api/option"
	"google.golang.org/api/option/internaloption"
	gtransport "google.golang.org/api/transport/grpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

const (
	bigtableEndpoint      = "bigtable.googleapis.com:443"
	bigtableMTLSEndpoint  = "bigtable.mtls.googleapis.com:443"
	bigtableScope         = "https://www.googleapis.com/auth/bigtable.data"
	featureFlagsHeaderKey = "bigtable-features"
)

// dialBigtable creates a single authenticated gRPC connection and returns it
// alongside a BigtableClient stub.
//
// TODO: replace with a per-AFE pool (one channel pinned per AFE) whose channel
// selection is driven by ClientConfigurationManager — see
// SESSION_CLIENT_REFACTOR.md ("Why a separate channel pool"). For now we use a
// single connection so the rest of the structure is exercisable end-to-end.
func dialBigtable(ctx context.Context, opts ...option.ClientOption) (*grpc.ClientConn, btpb.BigtableClient, error) {
	defaults := []option.ClientOption{
		internaloption.WithDefaultEndpoint(bigtableEndpoint),
		internaloption.WithDefaultMTLSEndpoint(bigtableMTLSEndpoint),
		internaloption.WithDefaultScopes(bigtableScope),
		internaloption.AllowNonDefaultServiceAccount(true),
		option.WithGRPCDialOption(grpc.WithDefaultCallOptions(
			grpc.MaxCallSendMsgSize(1<<28),
			grpc.MaxCallRecvMsgSize(1<<28),
		)),
	}
	conn, err := gtransport.Dial(ctx, append(defaults, opts...)...)
	if err != nil {
		return nil, nil, err
	}
	return conn, btpb.NewBigtableClient(conn), nil
}

// buildFeatureFlagsMD proto-marshals the session-compatible feature flags and
// base64-URL-encodes them into the bigtable-features header.
func buildFeatureFlagsMD() metadata.MD {
	ff := &btpb.FeatureFlags{
		RoutingCookie:           true,
		ReverseScans:            true,
		LastScannedRowResponses: true,
		RetryInfo:               true,
		TrafficDirectorEnabled:  true,
		DirectAccessRequested:   true,
		SessionsCompatible:      true,
		PeerInfo:                true,
	}
	b, err := proto.Marshal(ff)
	if err != nil {
		return metadata.MD{}
	}
	return metadata.Pairs(featureFlagsHeaderKey, base64.URLEncoding.EncodeToString(b))
}
