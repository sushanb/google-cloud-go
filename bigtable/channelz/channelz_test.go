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

package channelz

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/bigtable"
	btransport "cloud.google.com/go/bigtable/internal/transport"
)

type fakeProvider struct {
	pools []bigtable.ChannelPoolDebug
}

func (f fakeProvider) Snapshot() []bigtable.ChannelPoolDebug { return f.pools }

func samplePools() []bigtable.ChannelPoolDebug {
	now := time.Now()
	return []bigtable.ChannelPoolDebug{
		{
			Role: "classic",
			Snapshot: btransport.ChannelPoolSnapshot{
				LBPolicy:     "RoundRobin",
				InstanceName: "projects/p/instances/inst",
				AppProfile:   "my-app",
				TotalConns:   2,
				CapturedAt:   now,
				Channels: []btransport.ChannelSnapshot{
					{
						Index: 0, OutstandingUnary: 3, OutstandingStreaming: 1,
						ErrorCount: 0, IsALTSUsed: true, IPProtocol: "ipv6",
						TargetState: "READY", CreatedAt: now.Add(-90 * time.Second),
					},
					{
						Index: 1, OutstandingUnary: 0, OutstandingStreaming: 0,
						ErrorCount: 5, IsALTSUsed: true, IPProtocol: "ipv6",
						TargetState: "TRANSIENT_FAILURE", IsDraining: true,
						CreatedAt: now.Add(-2 * time.Minute),
					},
				},
			},
		},
		{
			Role: "session",
			Snapshot: btransport.ChannelPoolSnapshot{
				LBPolicy:     "PeakEwma",
				InstanceName: "projects/p/instances/inst",
				AppProfile:   "my-app",
				TotalConns:   1,
				CapturedAt:   now,
				Channels: []btransport.ChannelSnapshot{
					{
						Index: 0, OutstandingUnary: 10, OutstandingStreaming: 4,
						ErrorCount: 0, IsALTSUsed: true, IPProtocol: "ipv6",
						TargetState: "READY", CreatedAt: now.Add(-30 * time.Second),
					},
				},
			},
		},
	}
}

func get(t *testing.T, h http.Handler, target string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, target, nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestChannelz_HTML(t *testing.T) {
	h := HandlerFromProvider(fakeProvider{pools: samplePools()})
	rec := get(t, h, "/")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	body := rec.Body.String()
	for _, want := range []string{
		"classic pool", "session pool",
		"RoundRobin", "PeakEwma",
		"READY", "TRANSIENT_FAILURE",
		"projects/p/instances/inst",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("page missing %q", want)
		}
	}
}

func TestChannelz_JSON(t *testing.T) {
	h := HandlerFromProvider(fakeProvider{pools: samplePools()})
	rec := get(t, h, "/?format=json")
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}
	var got []bigtable.ChannelPoolDebug
	if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got) != 2 || got[0].Role != "classic" || got[1].Role != "session" {
		t.Errorf("unexpected pools: %+v", got)
	}
}

func TestChannelz_NoProvider(t *testing.T) {
	h := HandlerFromProvider(nil)
	rec := get(t, h, "/")
	if !strings.Contains(rec.Body.String(), "No channel debug provider") {
		t.Errorf("expected no-provider message; got %q", rec.Body.String())
	}
}

func TestChannelz_NoPools(t *testing.T) {
	h := HandlerFromProvider(fakeProvider{pools: nil})
	rec := get(t, h, "/")
	if !strings.Contains(rec.Body.String(), "No Bigtable channel pools") {
		t.Errorf("expected empty-state message; got %q", rec.Body.String())
	}
}

func TestChannelz_NotFound(t *testing.T) {
	h := HandlerFromProvider(fakeProvider{pools: samplePools()})
	rec := get(t, h, "/anything")
	if rec.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", rec.Code)
	}
}
