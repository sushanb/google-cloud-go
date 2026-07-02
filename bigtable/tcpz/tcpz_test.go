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

package tcpz

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"cloud.google.com/go/bigtable"
)

// TestHandler_EmptyRenders confirms the handler serves a valid HTML page
// with the "no conns registered" hint when no dials have happened yet.
// Exercises the template on the empty-slice path (guards against a
// template bug that only surfaces without data).
func TestHandler_EmptyRenders(t *testing.T) {
	h := Handler(bigtable.NewTCPStats())

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("HTTP %d, want 200", rr.Code)
	}
	if ct := rr.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/html") {
		t.Errorf("Content-Type = %q, want text/html*", ct)
	}
	body := rr.Body.String()
	if !strings.Contains(body, "No conns registered") {
		t.Errorf("empty page did not render the empty-state hint; body:\n%s", body)
	}
}

// TestHandler_JSON confirms ?format=json returns a JSON array (empty is
// fine) with the right Content-Type. Cheap regression guard for template
// bypass logic.
func TestHandler_JSON(t *testing.T) {
	h := Handler(bigtable.NewTCPStats())

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/?format=json", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("HTTP %d, want 200", rr.Code)
	}
	if ct := rr.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}
	var rows []map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &rows); err != nil {
		t.Fatalf("JSON decode: %v — body: %s", err, rr.Body.String())
	}
	if len(rows) != 0 {
		t.Errorf("empty registry → rows = %d, want 0", len(rows))
	}
}

// TestHandler_NilStatsSafe guards against a panic when a caller wires the
// handler without a TCPStats. Renders as if empty.
func TestHandler_NilStatsSafe(t *testing.T) {
	h := Handler(nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("HTTP %d, want 200", rr.Code)
	}
}
