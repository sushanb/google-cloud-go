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

// sessionz-demo opens a *bigtable.Client with session pooling enabled, drives
// continuous ReadRow traffic against a table, and serves the sessionz debug
// UI on a chosen port so you can inspect live session state in a browser.
//
// Quick start (prod):
//
//	go run ./cmd/sessionz-demo \
//	    -project=my-proj -instance=my-inst -table=my-table \
//	    -row=some-row -port=6060
//
// then visit http://localhost:6060/debug/sessionz/.
//
// Quick start (emulator):
//
//	BIGTABLE_EMULATOR_HOST=localhost:9000 \
//	go run ./cmd/sessionz-demo \
//	    -project=p -instance=i -table=t -row=k -port=6060
//
// Flags:
//
//	-project, -instance, -table, -row : Bigtable target + a row to read.
//	-port                              : HTTP port for the debug UI.
//	-pool-min, -pool-max               : session pool bounds.
//	-rps                               : approximate ReadRow rate to generate.
//	-concurrency                       : how many parallel ReadRow goroutines.
//	-app-profile                       : optional app profile id.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"cloud.google.com/go/bigtable"
	"cloud.google.com/go/bigtable/sessionz"
)

var (
	project     = flag.String("project", "", "GCP project id (required)")
	instance    = flag.String("instance", "", "Bigtable instance id (required)")
	table       = flag.String("table", "", "Bigtable table id (required)")
	row         = flag.String("row", "", "row key to read (required)")
	appProfile  = flag.String("app-profile", "", "app profile id (optional)")
	port        = flag.Int("port", 6060, "HTTP port for the sessionz debug UI")
	poolMin     = flag.Int("pool-min", 2, "minimum sessions in the pool")
	poolMax     = flag.Int("pool-max", 10, "maximum sessions in the pool")
	rps         = flag.Float64("rps", 50.0, "approximate ReadRow requests per second to generate")
	concurrency = flag.Int("concurrency", 4, "number of parallel ReadRow goroutines")
)

func main() {
	flag.Parse()
	if *project == "" || *instance == "" || *table == "" || *row == "" {
		flag.Usage()
		log.Fatal("-project, -instance, -table, and -row are required")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := bigtable.ClientConfig{
		AppProfile:        *appProfile,
		EnableSessionPool: true,
		SessionPoolMin:    *poolMin,
		SessionPoolMax:    *poolMax,
	}
	client, err := bigtable.NewClientWithConfig(ctx, *project, *instance, cfg)
	if err != nil {
		log.Fatalf("bigtable.NewClientWithConfig: %v", err)
	}
	defer client.Close()

	// Mount the sessionz handler under /debug/sessionz/. http.StripPrefix
	// lets the handler use root-relative routes (/, /pool/{key}).
	mux := http.NewServeMux()
	mux.Handle("/debug/sessionz/", http.StripPrefix("/debug/sessionz", sessionz.Handler(client)))
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/debug/sessionz/", http.StatusFound)
	})

	addr := fmt.Sprintf(":%d", *port)
	srv := &http.Server{Addr: addr, Handler: mux}
	go func() {
		log.Printf("sessionz debug UI listening on http://localhost%s/debug/sessionz/", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http.ListenAndServe: %v", err)
		}
	}()

	// OpenTable (not Open) gives the session-routed TableAPI so the
	// ReadRows below traverse the session pool we're trying to inspect.
	tbl := client.OpenTable(*table)
	go runLoad(ctx, tbl, *row, *rps, *concurrency)

	// Wait for SIGINT/SIGTERM, then shut everything down cleanly.
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	<-sigs
	log.Printf("shutting down…")
	cancel()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	_ = srv.Shutdown(shutdownCtx)
}

// runLoad spawns `concurrency` goroutines, each pacing itself to rps/concurrency
// ReadRow calls per second. Counts and errors are logged every 5 seconds so you
// can correlate what you see in the UI with what the load is actually doing.
func runLoad(ctx context.Context, tbl bigtable.TableAPI, rowKey string, rps float64, concurrency int) {
	if rps <= 0 || concurrency <= 0 {
		return
	}
	perWorker := rps / float64(concurrency)
	interval := time.Duration(float64(time.Second) / perWorker)

	var ok, errs atomic.Int64
	for i := 0; i < concurrency; i++ {
		go func() {
			t := time.NewTicker(interval)
			defer t.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-t.C:
				}
				rctx, rcancel := context.WithTimeout(ctx, 5*time.Second)
				_, err := tbl.ReadRow(rctx, rowKey)
				rcancel()
				if err != nil {
					errs.Add(1)
				} else {
					ok.Add(1)
				}
			}
		}()
	}

	logTicker := time.NewTicker(5 * time.Second)
	defer logTicker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-logTicker.C:
			log.Printf("load so far: ok=%d errors=%d", ok.Load(), errs.Load())
		}
	}
}
