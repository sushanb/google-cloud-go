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

// sessionclientz-demo is a minimal harness that stands up a raw
// internal/session.SessionClient (no top-level *bigtable.Client
// involved) and mounts the debugview UI against it. Illustrates that a
// session.SessionClient directly satisfies debugview.DebugProviders —
// no adapter, no *bigtable.Client wrapper needed.
//
// Not a supported public API: this lives inside the bigtable module
// specifically so it can import cloud.google.com/go/bigtable/internal/*.
// The eventual public bigtable.NewSessionClient wrapper is what external
// callers will use; this demo is the internal-module equivalent.
//
// Usage:
//
//	go run ./cmd/sessionclientz-demo/ -project=P -instance=I -table=T
//
// Then open http://localhost:6062/debug/ to see sessionz / afez / loadz /
// channelz / configz for a SessionClient-only setup. The channelz page
// shows exactly one pool (Role="session"); the sessionz Diverter block
// is empty (classic/session split is a mixed-mode concept, not present
// on a standalone SessionClient).
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"time"

	"cloud.google.com/go/bigtable"
	btpb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
	"cloud.google.com/go/bigtable/debugview"
	"cloud.google.com/go/bigtable/internal/session"

	"google.golang.org/api/option"
)

var (
	project  = flag.String("project", "", "GCP project id (required)")
	instance = flag.String("instance", "", "Bigtable instance id (required)")
	table    = flag.String("table", "", "Bigtable table id (required)")
	appProf  = flag.String("app_profile", "default", "AppProfile id")
	endpoint = flag.String("endpoint", "bigtable.googleapis.com:443", "Bigtable data endpoint")
	poolSize = flag.Int("pool_size", 4, "gRPC connection pool size for the session channel pool")
	port     = flag.Int("port", 6062, "HTTP port for the /debug/ mux")
)

func main() {
	flag.Parse()
	if *project == "" || *instance == "" || *table == "" {
		log.Fatal("required: -project, -instance, -table")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// TCP stats collector — installed via grpc.WithContextDialer at dial
	// time. Passed both into the SessionClient's dial opts (so its pool
	// gets captured) and into debugview.Handler (so /tcpz/ can render).
	tcpStats := bigtable.NewTCPStats()

	dialOpts := []option.ClientOption{
		option.WithEndpoint(*endpoint),
		option.WithGRPCConnectionPool(*poolSize),
		tcpStats.ClientOption(),
	}

	// -------------------------------------------------------------------
	// One-line SessionClient construction. Builds the channel pool, the
	// gRPC stub, the metrics factory, and the ClientConfigurationManager
	// internally. Pass nil MetricsProvider for default (enabled) metrics;
	// bigtable.NoopMetricsProvider{} to disable.
	// -------------------------------------------------------------------
	sc, err := session.NewSessionClient(ctx, *project, *instance, *appProf, nil, dialOpts...)
	if err != nil {
		log.Fatalf("session.NewSessionClient: %v", err)
	}
	defer sc.Close()

	// Drive some traffic so sessionz / loadz have live data to render.
	tbl := sc.OpenSessionTable(*table)
	defer tbl.Close()
	go driveTraffic(ctx, tbl)

	// -------------------------------------------------------------------
	// Mount debugview.Handler against the SessionClient directly. sc
	// satisfies debugview.DebugProviders (compile-time-checked in
	// debugview/handler_iface_test.go) — no adapter needed.
	// -------------------------------------------------------------------
	mux := http.NewServeMux()
	mux.Handle("/debug/", http.StripPrefix("/debug",
		debugview.Handler(sc, tcpStats)))

	addr := fmt.Sprintf(":%d", *port)
	log.Printf("sessionclientz-demo listening on http://localhost%s/debug/", addr)
	log.Printf("  channelz shows one pool (Role=session).")
	log.Printf("  sessionz Diverter block is empty (no classic/session split in a session-only client).")
	log.Printf("  tcpz shows one row per open TCP conn (RTT, retrans, PMTU, ...).")
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalf("http.ListenAndServe: %v", err)
	}
}

// driveTraffic keeps a small stream of ReadRow calls going so
// sessionz / loadz have live data to render. SessionReadRowRequest
// carries only the row key + optional filter — table / app-profile
// identity is baked into the underlying session (established by the
// OpenTableRequest that session.OpenSessionTable emits behind the
// scenes). Errors are logged but don't stop the loop — the demo
// prioritizes the debug view working over any single RPC succeeding.
func driveTraffic(ctx context.Context, tbl session.TableAPI) {
	tick := time.NewTicker(500 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			reqCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			_, err := tbl.ReadRow(reqCtx, &btpb.SessionReadRowRequest{
				Key: []byte("demo-row"),
			})
			cancel()
			if err != nil {
				log.Printf("ReadRow demo-row: %v", err)
			}
		}
	}
}
