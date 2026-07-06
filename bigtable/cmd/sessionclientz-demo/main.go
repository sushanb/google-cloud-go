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
// The eventual public bigtable.SessionClient wrapper is what external
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

	btpb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
	"cloud.google.com/go/bigtable/debugview"
	"cloud.google.com/go/bigtable/internal/metrics"
	btopt "cloud.google.com/go/bigtable/internal/option"
	"cloud.google.com/go/bigtable/internal/session"
	btransport "cloud.google.com/go/bigtable/internal/transport"

	"google.golang.org/api/option"
	"google.golang.org/api/option/internaloption"
	gtransport "google.golang.org/api/transport/grpc"
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

	// -------------------------------------------------------------------
	// 1. Build the four inputs session.NewSessionClient needs.
	// -------------------------------------------------------------------
	//
	//   sc := session.NewSessionClient(pool, stub, factory, cfg)
	//
	// pool    — btransport.ChannelPool (interface, satisfied by *BigtableChannelPool)
	// stub    — btpb.BigtableClient (built by btpb.NewBigtableClient on the pool)
	// factory — *metrics.Factory (NoopMetricsProvider disables telemetry)
	// cfg     — session.Config (project/instance/app-profile + BackgroundCtx)
	// -------------------------------------------------------------------

	fullInstance := fmt.Sprintf("projects/%s/instances/%s", *project, *instance)

	// gRPC dial options — standard Bigtable data-plane options. In
	// production you'd add credentials, user-agent, keepalive, etc.
	dialOpts := []option.ClientOption{
		option.WithEndpoint(*endpoint),
		option.WithGRPCConnectionPool(*poolSize),
		internaloption.EnableDirectPath(false), // this demo skips DirectPath
	}

	dial := func() (*btransport.BigtableConn, error) {
		grpcConn, err := gtransport.Dial(ctx, dialOpts...)
		if err != nil {
			return nil, err
		}
		return btransport.NewBigtableConn(grpcConn), nil
	}

	// A single ChannelPrimer shared for both the pool's connection
	// factory and (unused here) the DirectAccessChecker — keeps the
	// exact PingAndWarm invocation in one place.
	primer := btransport.NewPingAndWarmChannelPrimer(fullInstance, *appProf, nil)

	pool, err := btransport.NewBigtableChannelPool(
		ctx,
		*poolSize,
		btopt.BigtableLoadBalancingStrategy(),
		dial,
		time.Now(),
		btransport.WithInstanceName(fullInstance),   // for channelz identity
		btransport.WithAppProfile(*appProf),         // for channelz identity
		btransport.WithChannelPrimer(primer),        // PingAndWarm on each new conn
		btransport.WithDirectAccessChecker(          // required, even when off
			btransport.NewDisabledDirectAccessChecker(nil, nil)),
	)
	if err != nil {
		log.Fatalf("NewBigtableChannelPool: %v", err)
	}
	defer pool.Close()

	stub := btpb.NewBigtableClient(pool)

	factory, err := metrics.NewFactory(ctx, *project, *instance, *appProf,
		metrics.NoopMetricsProvider{}) // demo — no metrics export
	if err != nil {
		log.Fatalf("metrics.NewFactory: %v", err)
	}

	sc := session.NewSessionClient(pool, stub, factory, session.Config{
		Project:       *project,
		Instance:      *instance,
		AppProfile:    *appProf,
		BackgroundCtx: ctx, // cancel with the main ctx so background loops unwind
	})
	defer sc.Close()

	// -------------------------------------------------------------------
	// 2. Drive some traffic so sessionz has something to show.
	// -------------------------------------------------------------------
	tbl := sc.OpenSessionTable(*table)
	defer tbl.Close()

	go driveTraffic(ctx, tbl)

	// -------------------------------------------------------------------
	// 3. Mount debugview.Handler against the SessionClient directly.
	// -------------------------------------------------------------------
	//
	// This is the one-line ask — sc satisfies debugview.DebugProviders
	// (compile-time-checked in debugview/handler_iface_test.go), so
	// no adapter is needed. Same shape as passing *bigtable.Client.
	//
	// tcpStats is nil here; pass a real *bigtable.TCPStats to populate
	// /debug/tcpz/.
	//
	// -------------------------------------------------------------------
	mux := http.NewServeMux()
	mux.Handle("/debug/", http.StripPrefix("/debug",
		debugview.Handler(sc, nil)))

	addr := fmt.Sprintf(":%d", *port)
	log.Printf("sessionclientz-demo listening on http://localhost%s/debug/", addr)
	log.Printf("  channelz shows one pool (Role=session).")
	log.Printf("  sessionz Diverter block is empty (no classic/session split in a session-only client).")
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
func driveTraffic(ctx context.Context, tbl session.SessionTableApi) {
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
