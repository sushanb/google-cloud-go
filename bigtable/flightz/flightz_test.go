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

package flightz

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	btransport "cloud.google.com/go/bigtable/internal/transport"
)

type fakeProvider struct {
	pools []btransport.PoolSnapshot
}

func (f fakeProvider) Snapshot() []btransport.PoolSnapshot { return f.pools }
func (fakeProvider) Diverter() btransport.DiverterSnapshot { return btransport.DiverterSnapshot{} }

// samplePools returns a two-pool snapshot with a controlled mix of
// in-flight and idle sessions so tests can pin exact row counts and
// sort order.
func samplePools(now time.Time) []btransport.PoolSnapshot {
	return []btransport.PoolSnapshot{
		{
			Name: "OpenTable1[READ]",
			Sessions: []btransport.SessionSnapshot{
				{
					LogName: "session-1a", State: "Active",
					InFlight: &btransport.InFlightVRpc{
						RpcID:    42,
						Method:   "ReadRow",
						SentAt:   now.Add(-100 * time.Millisecond),
						Deadline: now.Add(2 * time.Second),
						Attempt:  1,
					},
					Peer: btransport.PeerInfoSnapshot{ApplicationFrontendID: 0xabc, ApplicationFrontendRegion: "us-central1", ApplicationFrontendSubzone: "tm"},
				},
				{LogName: "session-1b", State: "Active"}, // idle — no InFlight
				{
					LogName: "session-1c", State: "Active",
					InFlight: &btransport.InFlightVRpc{
						RpcID:   7,
						Method:  "ReadRow",
						SentAt:  now.Add(-6 * time.Second), // stuck — should be top row
						Attempt: 2,                          // retry
					},
					Peer: btransport.PeerInfoSnapshot{ApplicationFrontendID: 0xdef, ApplicationFrontendRegion: "us-central1", ApplicationFrontendSubzone: "tm"},
				},
			},
		},
		{
			Name: "OpenTable1[WRITE]",
			Sessions: []btransport.SessionSnapshot{
				{
					LogName: "session-2a", State: "Active",
					InFlight: &btransport.InFlightVRpc{
						RpcID:    3,
						Method:   "MutateRow",
						SentAt:   now.Add(-1200 * time.Millisecond),
						Deadline: now.Add(-50 * time.Millisecond), // deadline already passed
						Attempt:  1,
					},
				},
				{LogName: "session-2b", State: "Active"}, // idle
			},
		},
	}
}

func newTestServer(pools []btransport.PoolSnapshot) *httptest.Server {
	return httptest.NewServer(HandlerFromProvider(fakeProvider{pools: pools}))
}

func TestFlightz_CrossPool_RowCountAndSortOrder(t *testing.T) {
	now := time.Now()
	ts := newTestServer(samplePools(now))
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/?format=json")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	var got struct {
		InFlight       []Row `json:"inFlight"`
		TotalSessions  int   `json:"totalSessions"`
		ActiveSessions int   `json:"activeSessions"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if want := 3; len(got.InFlight) != want {
		t.Fatalf("inFlight rows = %d, want %d", len(got.InFlight), want)
	}
	if got.TotalSessions != 5 {
		t.Errorf("totalSessions = %d, want 5", got.TotalSessions)
	}
	if got.ActiveSessions != 3 {
		t.Errorf("activeSessions = %d, want 3", got.ActiveSessions)
	}

	// Oldest first: session-1c (6s) → session-2a (1.2s) → session-1a (100ms).
	wantOrder := []string{"session-1c", "session-2a", "session-1a"}
	for i, want := range wantOrder {
		if got.InFlight[i].Session != want {
			t.Errorf("row %d session = %q, want %q (rows: %+v)", i, got.InFlight[i].Session, want, got.InFlight)
		}
	}
}

func TestFlightz_PoolFilter(t *testing.T) {
	now := time.Now()
	ts := newTestServer(samplePools(now))
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/pool/OpenTable1%5BWRITE%5D?format=json")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var got struct {
		InFlight       []Row  `json:"inFlight"`
		PoolFilter     string `json:"poolFilter"`
		TotalSessions  int    `json:"totalSessions"`
		ActiveSessions int    `json:"activeSessions"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if got.PoolFilter != "OpenTable1[WRITE]" {
		t.Errorf("poolFilter = %q, want OpenTable1[WRITE]", got.PoolFilter)
	}
	if len(got.InFlight) != 1 {
		t.Fatalf("filtered rows = %d, want 1", len(got.InFlight))
	}
	if got.InFlight[0].Method != "MutateRow" {
		t.Errorf("method = %q, want MutateRow", got.InFlight[0].Method)
	}
	// Header totals stay cross-pool even when filtered.
	if got.TotalSessions != 5 {
		t.Errorf("totalSessions (cross-pool) = %d, want 5", got.TotalSessions)
	}
	if got.ActiveSessions != 3 {
		t.Errorf("activeSessions (cross-pool) = %d, want 3", got.ActiveSessions)
	}
}

func TestFlightz_EmptyState(t *testing.T) {
	now := time.Now()
	// All sessions idle → HTML page shows the empty-state banner.
	pools := []btransport.PoolSnapshot{
		{
			Name: "idle-pool",
			Sessions: []btransport.SessionSnapshot{
				{LogName: "s1", State: "Active"},
				{LogName: "s2", State: "Active"},
			},
		},
	}
	_ = now
	ts := newTestServer(pools)
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "No in-flight vRPCs") {
		t.Errorf("empty-state banner missing from HTML; body starts:\n%s", head(body, 400))
	}
	if !strings.Contains(string(body), "All 2 sessions idle") {
		t.Errorf("empty-state session count missing from HTML; body starts:\n%s", head(body, 400))
	}
}

func TestFlightz_DeadlineExpiredMarkedRed(t *testing.T) {
	now := time.Now()
	ts := newTestServer(samplePools(now))
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	// session-2a has a passed deadline → deadline cell gets the deadline-exp class.
	if !strings.Contains(string(body), "deadline-exp") {
		t.Errorf("passed-deadline row not marked with deadline-exp class; body:\n%s", head(body, 800))
	}
}

func TestFlightz_RetryMarked(t *testing.T) {
	now := time.Now()
	ts := newTestServer(samplePools(now))
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	// session-1c is on attempt=2; the attempt cell should get the attempt-retry class.
	if !strings.Contains(string(body), "attempt-retry") {
		t.Errorf("retry attempt not visually flagged; body:\n%s", head(body, 800))
	}
}

func TestFlightz_NilProvider(t *testing.T) {
	ts := httptest.NewServer(HandlerFromProvider(nil))
	defer ts.Close()

	// Nil provider must not panic — should render an empty page with 0 sessions.
	resp, err := http.Get(ts.URL + "/?format=json")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var got struct {
		InFlight       []Row `json:"inFlight"`
		TotalSessions  int   `json:"totalSessions"`
		ActiveSessions int   `json:"activeSessions"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got.InFlight) != 0 || got.TotalSessions != 0 || got.ActiveSessions != 0 {
		t.Errorf("nil-provider response = %+v, want all zero", got)
	}
}

func head(b []byte, n int) string {
	if len(b) <= n {
		return string(b)
	}
	return string(b[:n]) + "…"
}
