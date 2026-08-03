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

package bigtable

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	btpb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
	btransport "cloud.google.com/go/bigtable/internal/transport"
	"google.golang.org/api/option"
	gtransport "google.golang.org/api/transport/grpc"
	"google.golang.org/protobuf/encoding/prototext"
)

// TestExperiment_GetClientConfiguration is a probe against real Cloud
// Bigtable that shows:
//   - The FeatureFlags proto the client would send (driven by
//     CBT_FORCE_SESSION env var per feature_flags.go:56).
//   - The base64-encoded bigtable-features header value.
//   - The raw ClientConfiguration proto the server returns.
//
// Gated on CBT_EXPERIMENT_LIVE=1 so it never runs in normal CI. Uses
// ambient Application Default Credentials.
//
// Env vars:
//   CBT_EXPERIMENT_LIVE   must be "1" to run
//   CBT_PROJECT           project id (required)
//   CBT_INSTANCE          instance id (required)
//   CBT_APP_PROFILE       app profile id (default: "default")
//   CBT_ENDPOINT          gRPC endpoint (default: "bigtable.googleapis.com:443")
//   CBT_FORCE_SESSION     read by NewFeatureFlagsProto — "true" sets
//                         SessionsCompatible=true + SessionsRequired=true;
//                         "false" sets both false; unset defaults to
//                         (true, false)
//
// Example:
//   CBT_EXPERIMENT_LIVE=1 CBT_FORCE_SESSION=true \
//   CBT_PROJECT=autonomous-mote-782 CBT_INSTANCE=sushanb-uc1 \
//   go test ./bigtable -run TestExperiment_GetClientConfiguration -v -count=1
func TestExperiment_GetClientConfiguration(t *testing.T) {
	if os.Getenv("CBT_EXPERIMENT_LIVE") != "1" {
		t.Skip("skipping live probe; set CBT_EXPERIMENT_LIVE=1 to run")
	}
	project := requireEnv(t, "CBT_PROJECT")
	instance := requireEnv(t, "CBT_INSTANCE")
	appProfile := os.Getenv("CBT_APP_PROFILE")
	endpoint := os.Getenv("CBT_ENDPOINT")
	if endpoint == "" {
		endpoint = "bigtable.googleapis.com:443"
	}

	// Build the FeatureFlags proto the way NewClientWithConfig does.
	// NewFeatureFlagsProto reads CBT_FORCE_SESSION internally.
	ff := btransport.NewFeatureFlagsProto(btransport.FeatureFlagsInput{
		ClientSideMetricsEnabled: true,
		EnableDirectAccess:       false,
	})

	// Header (base64-encoded proto) — this is what the server sees.
	ffMD := btransport.MarshalFeatureFlagsMD(ff)
	headerVal := ""
	if vs := ffMD.Get(btransport.FeatureFlagsHeader); len(vs) > 0 {
		headerVal = vs[0]
	}

	t.Logf("=== INPUT ===")
	t.Logf("CBT_FORCE_SESSION env: %q", os.Getenv("CBT_FORCE_SESSION"))
	t.Logf("endpoint: %s", endpoint)
	t.Logf("project=%s instance=%s appProfile=%q", project, instance, appProfile)
	t.Logf("")
	t.Logf("=== FeatureFlags proto (textproto) ===")
	t.Logf("\n%s", prototext.Format(ff))
	t.Logf("=== FeatureFlags proto (JSON snapshot) ===")
	t.Logf("\n%s", protoToJSON(ff))
	t.Logf("=== bigtable-features header value (base64) ===")
	t.Logf("%s", headerVal)
	t.Logf("")

	// Dial + call GetClientConfiguration.
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	opts := []option.ClientOption{
		option.WithEndpoint(endpoint),
		option.WithScopes("https://www.googleapis.com/auth/cloud-platform",
			"https://www.googleapis.com/auth/bigtable.data"),
	}
	conn, err := gtransport.Dial(ctx, opts...)
	if err != nil {
		t.Fatalf("gtransport.Dial: %v", err)
	}
	defer conn.Close()

	stub := btpb.NewBigtableClient(conn)
	instanceName := fmt.Sprintf("projects/%s/instances/%s", project, instance)

	resp, header, trailer, err := btransport.FetchClientConfigurationOnce(ctx, stub, instanceName, appProfile, ffMD)

	t.Logf("=== RPC RESULT ===")
	if err != nil {
		t.Logf("GetClientConfiguration ERROR: %v", err)
	} else {
		t.Logf("GetClientConfiguration OK")
	}
	t.Logf("response header metadata: %v", header)
	t.Logf("response trailer metadata: %v", trailer)
	t.Logf("")

	if resp == nil {
		t.Logf("(nil response)")
		return
	}
	t.Logf("=== ClientConfiguration proto (textproto) ===")
	t.Logf("\n%s", prototext.Format(resp))
	t.Logf("=== ClientConfiguration proto (JSON snapshot) ===")
	t.Logf("\n%s", protoToJSON(resp))
}

func requireEnv(t *testing.T, name string) string {
	t.Helper()
	v := os.Getenv(name)
	if v == "" {
		t.Fatalf("required env var %s is not set", name)
	}
	return v
}

// protoToJSON marshals a proto message via encoding/json on its
// prototext form's rough shape — used only to make the log easier for
// operators to grep by field name.
func protoToJSON(m interface{}) string {
	b, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return fmt.Sprintf("(json marshal err: %v)", err)
	}
	return string(b)
}
