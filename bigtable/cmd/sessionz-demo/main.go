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
	"cloud.google.com/go/bigtable/channelz"
	"cloud.google.com/go/bigtable/configz"
	"cloud.google.com/go/bigtable/sessionz"
	"google.golang.org/api/option"
)

var (
	project     = flag.String("project", "", "GCP project id (required)")
	instance    = flag.String("instance", "", "Bigtable instance id (required)")
	table       = flag.String("table", "", "Bigtable table id (required)")
	row         = flag.String("row", "", "row key to read (required)")
	appProfile  = flag.String("app-profile", "", "app profile id (optional)")
	endpoint    = flag.String("endpoint", "", "override Bigtable endpoint (e.g. test-bigtable.sandbox.googleapis.com:443)")
	port        = flag.Int("port", 6060, "HTTP port for the sessionz debug UI")
	poolMin     = flag.Int("pool-min", 2, "minimum sessions in the pool")
	poolMax     = flag.Int("pool-max", 10, "maximum sessions in the pool")
	rps         = flag.Float64("rps", 50.0, "approximate ReadRow requests per second to generate (interpreted as the BASE rate; modulated by -pattern)")
	concurrency = flag.Int("concurrency", 4, "number of parallel ReadRow goroutines")
	pattern     = flag.String("pattern", "constant", `load pattern: "constant" (steady -rps), "wave" (low/high square wave to exercise the sizer)`)
	cycle       = flag.Duration("cycle", 60*time.Second, "wave-pattern full cycle period (one low + one high phase). Ignored for -pattern=constant.")
	waveLow     = flag.Float64("wave-low", 0.1, "wave low-phase multiplier applied to -rps")
	waveHigh    = flag.Float64("wave-high", 10.0, "wave high-phase multiplier applied to -rps")
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
	var opts []option.ClientOption
	if *endpoint != "" {
		opts = append(opts, option.WithEndpoint(*endpoint))
	}
	client, err := bigtable.NewClientWithConfig(ctx, *project, *instance, cfg, opts...)
	if err != nil {
		log.Fatalf("bigtable.NewClientWithConfig: %v", err)
	}
	defer client.Close()

	// Mount the three debug UIs under /debug/. http.StripPrefix lets each
	// handler use root-relative routes (/, /pool/{key}, etc).
	mux := http.NewServeMux()
	mux.Handle("/debug/sessionz/", http.StripPrefix("/debug/sessionz", sessionz.Handler(client)))
	mux.Handle("/debug/configz/", http.StripPrefix("/debug/configz", configz.Handler(client)))
	mux.Handle("/debug/channelz/", http.StripPrefix("/debug/channelz", channelz.Handler(client)))

	// Index page lists every debug surface. /debug, /debug/, and / all
	// render the same listing so the operator can land on any of them.
	debugIndex := func(w http.ResponseWriter, r *http.Request) {
		// Only render the index for /debug and /debug/; anything else under
		// /debug/ should 404 (specific debug surfaces have their own mounts).
		if r.URL.Path != "/debug" && r.URL.Path != "/debug/" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		_, _ = w.Write([]byte(`<!doctype html>
<html><head>
<meta charset="utf-8">
<title>bigtable debug</title>
<style>
body{font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;margin:2em;color:#222;background:#fafafa}
h1{font-size:1.4em;margin:0 0 1em 0}
ul{list-style:none;padding:0;background:#fff;box-shadow:0 1px 2px rgba(0,0,0,.06)}
li{padding:.8em 1em;border-bottom:1px solid #eee}
li:last-child{border-bottom:none}
li a{color:#1a5fb4;text-decoration:none;font-weight:600;font-size:1.05em}
li a:hover{text-decoration:underline}
li .desc{display:block;color:#666;font-size:.9em;margin-top:.2em}
li .json{margin-left:.6em;font-size:.85em;color:#888}
</style>
</head><body>
<h1>Bigtable debug</h1>
<ul>
  <li>
    <a href="/debug/sessionz/">sessionz</a>
    <a class="json" href="/debug/sessionz/?format=json">JSON</a>
    <span class="desc">Live session pools — per-session state, PeerInfo (transport / AFE / GFE), OK/error/retry counts, msg sent/recv with per-type breakdown, picks, EWMA, heartbeat, plus pool-level lifecycle (open/close + reasons), config-listener fires, throttler budget, and scaling history.</span>
  </li>
  <li>
    <a href="/debug/channelz/">channelz</a>
    <a class="json" href="/debug/channelz/?format=json">JSON</a>
    <span class="desc">gRPC channel pools (classic + session) — LB policy, per-channel ALTS/IP, in-flight unary/streaming load, picks, last activity, draining flag, and error counts.</span>
  </li>
  <li>
    <a href="/debug/configz/">configz</a>
    <a class="json" href="/debug/configz/?format=json">JSON</a>
    <span class="desc">Latest GetClientConfiguration response from the server — full proto rendered as JSON — plus poll history (timestamp, duration, result) so you can spot stuck or failing polls.</span>
  </li>
</ul>
</body></html>`))
	}
	mux.HandleFunc("/debug/", debugIndex)
	mux.HandleFunc("/debug", debugIndex)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			http.Redirect(w, r, "/debug/", http.StatusFound)
			return
		}
		http.NotFound(w, r)
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
	go runLoad(ctx, tbl, *row, *rps, *concurrency, *pattern, *cycle, *waveLow, *waveHigh)

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

// runLoad spawns `concurrency` worker goroutines and a controller. The
// workers each pace themselves to the SHARED per-op interval published by
// the controller; that way changing the rate over time (e.g. for the
// "wave" pattern) takes effect on every worker without restarting them.
//
// pattern semantics:
//   - "constant": the controller publishes one interval (derived from rps)
//     once and never changes it. Equivalent to the original fixed-rate load.
//   - "wave": square wave — alternates a low and high target rate every
//     cycle/2 so the pool sizer scales up during the high phase and prunes
//     during the low phase. Multipliers applied to rps:
//       low  = rps * waveLow      (default 0.1× — gentle, lets prune fire)
//       high = rps * waveHigh     (default 10× — aggressive, forces scale-up)
//     Each phase logs the transition so the sessionz timeline is readable.
func runLoad(
	ctx context.Context,
	tbl bigtable.TableAPI,
	rowKey string,
	rps float64,
	concurrency int,
	pattern string,
	cycle time.Duration,
	waveLow, waveHigh float64,
) {
	if rps <= 0 || concurrency <= 0 {
		return
	}

	// currentIntervalNanos is the per-op delay each worker waits between
	// requests. Updated atomically by the controller; workers read on
	// every tick so a phase change takes effect immediately.
	var currentIntervalNanos atomic.Int64
	intervalFor := func(targetRps float64) time.Duration {
		if targetRps <= 0 {
			return time.Hour // park workers
		}
		perWorker := targetRps / float64(concurrency)
		return time.Duration(float64(time.Second) / perWorker)
	}
	currentIntervalNanos.Store(int64(intervalFor(rps)))

	var ok, errs atomic.Int64
	for i := 0; i < concurrency; i++ {
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case <-time.After(time.Duration(currentIntervalNanos.Load())):
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

	// Controller goroutine: drives currentIntervalNanos for non-constant
	// patterns. For "constant" we just leave the initial interval in place.
	if pattern == "wave" && cycle > 0 {
		go func() {
			phase := time.Duration(0)
			if cycle/2 > 0 {
				phase = cycle / 2
			}
			high := false
			ticker := time.NewTicker(phase)
			defer ticker.Stop()
			for {
				high = !high
				var target float64
				if high {
					target = rps * waveHigh
				} else {
					target = rps * waveLow
				}
				iv := intervalFor(target)
				currentIntervalNanos.Store(int64(iv))
				log.Printf("workload: phase=%s target=%.1f rps interval=%v",
					phaseLabel(high), target, iv)
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
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
			log.Printf("load so far: ok=%d errors=%d (current interval %v)",
				ok.Load(), errs.Load(),
				time.Duration(currentIntervalNanos.Load()))
		}
	}
}

func phaseLabel(high bool) string {
	if high {
		return "HIGH"
	}
	return "LOW"
}
