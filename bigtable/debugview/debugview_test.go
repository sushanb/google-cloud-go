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

package debugview

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	session "cloud.google.com/go/bigtable/internal/session"
	btransport "cloud.google.com/go/bigtable/internal/transport"
)

// fakeProvider hand-builds the two snapshot slices the debugview
// consumes so the tests stay decoupled from wiring a real session
// client / channel pool / transport stack. Keeps the harness pure —
// package builds against upstream/main without any test-only export
// from the transport package.
type fakeProvider struct {
	lb   []btransport.LoadBalancingSnapshot
	disp []session.DispatchMethodTimings
}

func (f fakeProvider) LoadBalancingSnapshots() []btransport.LoadBalancingSnapshot {
	return f.lb
}
func (f fakeProvider) DispatchTimings() []session.DispatchMethodTimings { return f.disp }

func newFakeProvider() fakeProvider {
	return fakeProvider{
		lb: []btransport.LoadBalancingSnapshot{{
			PoolName:   "pool-A",
			PickerName: "LeastInFlightAfePicker",
			CapturedAt: time.Now(),
			Timings: btransport.CheckoutTimingsSnapshot{
				Segments: []btransport.TimingSegment{
					{Name: "checkout_total", N: 42, P50: 12 * time.Microsecond, P95: 60 * time.Microsecond, P99: 200 * time.Microsecond},
					{Name: "checkout_pick_afe", N: 42, P50: 1 * time.Microsecond, P95: 3 * time.Microsecond, P99: 8 * time.Microsecond},
					{Name: "session_await_result", N: 40, P50: 900 * time.Microsecond, P95: 4 * time.Millisecond, P99: 15 * time.Millisecond},
				},
				Counts: btransport.PathCounts{
					FastPathHits: 40,
					SlowPathHits: 2,
					PickLostRace: 1,
					EmptyKicks:   1,
					RefillKicks:  3,
				},
			},
		}},
		disp: []session.DispatchMethodTimings{{
			Method:     "MutateRow",
			Calls:      42,
			TotalP50:   time.Millisecond,
			TotalP95:   5 * time.Millisecond,
			TotalP99:   15 * time.Millisecond,
			TotalN:     42,
			ChainedP50: 900 * time.Microsecond,
			ChainedP95: 4 * time.Millisecond,
			ChainedP99: 14 * time.Millisecond,
			ChainedN:   42,
		}},
	}
}

func TestHandler_Index(t *testing.T) {
	srv := httptest.NewServer(Handler(newFakeProvider()))
	defer srv.Close()

	body := mustGet(t, srv.URL+"/")
	for _, want := range []string{"bigtable/debugview", `href="timings/"`} {
		if !strings.Contains(body, want) {
			t.Errorf("index missing %q\nbody: %s", want, body)
		}
	}
}

func TestHandler_TimingsHTML(t *testing.T) {
	srv := httptest.NewServer(Handler(newFakeProvider()))
	defer srv.Close()

	body := mustGet(t, srv.URL+"/timings/")
	for _, want := range []string{
		"pool-A", "LeastInFlightAfePicker",
		"checkout_total", "checkout_pick_afe", "session_await_result",
		"fast_path_hits", "MutateRow",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("timings HTML missing %q", want)
		}
	}
}

func TestHandler_TimingsJSON(t *testing.T) {
	srv := httptest.NewServer(Handler(newFakeProvider()))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/timings/?format=json")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	if got := resp.Header.Get("Content-Type"); got != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", got)
	}
	var got timingsData
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !got.Enabled {
		t.Error("Enabled = false, want true")
	}
	if len(got.Pools) != 1 || got.Pools[0].Name != "pool-A" {
		t.Fatalf("Pools = %+v, want one entry named pool-A", got.Pools)
	}
	if len(got.Dispatch) != 1 || got.Dispatch[0].Method != "MutateRow" {
		t.Fatalf("Dispatch = %+v, want one MutateRow entry", got.Dispatch)
	}
}

// Nil provider must not crash — instead render a friendly "not enabled"
// panel. Verified across HTML + JSON so operators debugging the wrong
// mount aren't left staring at a 500.
func TestHandler_NilProvider(t *testing.T) {
	srv := httptest.NewServer(Handler(nil))
	defer srv.Close()

	body := mustGet(t, srv.URL+"/timings/")
	if !strings.Contains(body, "not enabled") &&
		!strings.Contains(body, "Debug provider is nil") {
		t.Errorf("nil-provider HTML missing not-enabled hint\nbody: %s", body)
	}

	// JSON path should still be well-formed with Enabled=false.
	resp, err := http.Get(srv.URL + "/timings/?format=json")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	var got timingsData
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Enabled {
		t.Error("Enabled = true, want false")
	}
	if len(got.Pools) != 0 || len(got.Dispatch) != 0 {
		t.Errorf("nil provider returned data: pools=%d dispatch=%d",
			len(got.Pools), len(got.Dispatch))
	}
}

// /timings (no trailing slash) redirects to /timings/ so the mount
// works whether callers link one form or the other.
func TestHandler_TimingsRedirectAddsSlash(t *testing.T) {
	srv := httptest.NewServer(Handler(newFakeProvider()))
	defer srv.Close()

	// Disable automatic redirect-following so we can inspect the response.
	client := &http.Client{
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	resp, err := client.Get(srv.URL + "/timings")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusMovedPermanently {
		t.Fatalf("status = %d, want 301", resp.StatusCode)
	}
	loc := resp.Header.Get("Location")
	if !strings.HasSuffix(loc, "/timings/") {
		t.Errorf("Location = %q, want suffix /timings/", loc)
	}
}

func mustGet(t *testing.T, url string) string {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s: status = %d", url, resp.StatusCode)
	}
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return string(b)
}
