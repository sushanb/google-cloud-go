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

package sessionz_test

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"cloud.google.com/go/bigtable"
	"cloud.google.com/go/bigtable/sessionz"
	"google.golang.org/api/option"
)

// TestHighQpsSessionSandboxWithDebugUI mirrors TestHighQpsSessionSandbox in
// bigtable/read_session_test.go but additionally mounts the sessionz debug
// handler on a local HTTP port so the live session pool can be inspected in
// a browser while the load runs.
//
// This test hits the sandbox endpoint and requires GCP credentials. Run with:
//
//	SESSIONZ_PORT=6060 go test -run TestHighQpsSessionSandboxWithDebugUI \
//	    ./sessionz/ -timeout 15m -v
//
// Set SESSIONZ_PORT=0 (or unset) to pick a random free port; the URL is
// logged on startup either way.
func TestHighQpsSessionSandboxWithDebugUI(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 11*time.Minute)
	defer cancel()

	project := "autonomous-mote-782"
	instance := "test-sushanb"
	table := "sushanb"
	endpoint := "test-bigtable.sandbox.googleapis.com:443"

	cfg := bigtable.ClientConfig{
		EnableSessionPool: true,
		SessionPoolMin:    3,
		SessionPoolMax:    5,
	}

	client, err := bigtable.NewClientWithConfig(ctx, project, instance, cfg, option.WithEndpoint(endpoint))
	if err != nil {
		t.Fatalf("failed to create bigtable client: %v", err)
	}
	defer client.Close()

	// Mount sessionz on a TCP port. SESSIONZ_PORT=0 (or unset) → ephemeral
	// port; an explicit port is honored verbatim so the user can bookmark
	// a known URL.
	port := 0
	if p := os.Getenv("SESSIONZ_PORT"); p != "" {
		if n, err := strconv.Atoi(p); err == nil {
			port = n
		}
	}
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		t.Fatalf("net.Listen on port %d: %v", port, err)
	}
	mux := http.NewServeMux()
	mux.Handle("/debug/sessionz/", http.StripPrefix("/debug/sessionz", sessionz.Handler(client)))
	srv := &http.Server{Handler: mux}
	go func() {
		if err := srv.Serve(lis); err != nil && err != http.ErrServerClosed {
			t.Logf("sessionz http server exited: %v", err)
		}
	}()
	defer func() {
		shutdownCtx, c := context.WithTimeout(context.Background(), 5*time.Second)
		defer c()
		_ = srv.Shutdown(shutdownCtx)
	}()
	t.Logf("sessionz debug UI: http://%s/debug/sessionz/", lis.Addr().String())
	fmt.Printf(">>> SESSIONZ UI: http://%s/debug/sessionz/ <<<\n", lis.Addr().String())

	tbl := client.OpenTable(table)

	t.Logf("Waiting for session pool to warm up before high QPS test...")
	time.Sleep(3 * time.Second)

	concurrency := 10
	testDuration := 10 * time.Minute
	endTime := time.Now().Add(testDuration)

	var successWrites, successReads, failedWrites, failedReads int64

	var wg sync.WaitGroup
	t.Logf("Starting 10-minute high QPS sandbox test with %d concurrent workers...", concurrency)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			counter := 0
			for time.Now().Before(endTime) {
				counter++
				rowKey := fmt.Sprintf("sandbox-%d-%d", workerID, counter)
				mut := bigtable.NewMutation()
				mut.Set("cf12", "colq1", bigtable.ServerTime, []byte(fmt.Sprintf("val-worker-%d-%d", workerID, counter)))

				if err := tbl.Apply(ctx, rowKey, mut); err != nil {
					atomic.AddInt64(&failedWrites, 1)
					fmt.Printf(">>> ERROR [worker-%d]: write failed: %v <<<\n", workerID, err)
				} else {
					atomic.AddInt64(&successWrites, 1)
				}

				if _, err := tbl.ReadRow(ctx, rowKey); err != nil {
					atomic.AddInt64(&failedReads, 1)
					fmt.Printf(">>> ERROR [worker-%d]: read failed: %v <<<\n", workerID, err)
				} else {
					atomic.AddInt64(&successReads, 1)
				}

				time.Sleep(50 * time.Millisecond)
			}
		}(i)
	}

	doneChan := make(chan struct{})
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		start := time.Now()
		for {
			select {
			case <-doneChan:
				return
			case <-ticker.C:
				sw := atomic.LoadInt64(&successWrites)
				sr := atomic.LoadInt64(&successReads)
				fw := atomic.LoadInt64(&failedWrites)
				fr := atomic.LoadInt64(&failedReads)
				elapsed := time.Since(start)
				qps := float64(sw+sr) / elapsed.Seconds()
				fmt.Printf(">>> STATS [%.1fs elapsed]: Success (W:%d, R:%d), Failed (W:%d, R:%d), QPS: %.2f — UI http://%s/debug/sessionz/ <<<\n",
					elapsed.Seconds(), sw, sr, fw, fr, qps, lis.Addr().String())
			}
		}
	}()

	wg.Wait()
	close(doneChan)

	finalSW := atomic.LoadInt64(&successWrites)
	finalSR := atomic.LoadInt64(&successReads)
	finalFW := atomic.LoadInt64(&failedWrites)
	finalFR := atomic.LoadInt64(&failedReads)
	t.Logf("10-minute high QPS test completed! Successful Writes: %d, Successful Reads: %d, Failed Writes: %d, Failed Reads: %d", finalSW, finalSR, finalFW, finalFR)
	if finalFW > 0 || finalFR > 0 {
		t.Errorf("Test had failed operations!")
	}
}
