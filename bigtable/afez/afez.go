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

// Package afez renders a live per-AFE (Application Front End) view of every
// Bigtable session pool. It exposes the two-tier picker's primary
// bucketing unit — the group of sessions server-pinned to each AFE — so
// operators can answer "is my traffic actually spread across AFEs?" or
// "which AFE is slow?" without cross-referencing raw session tables.
//
// Mount into any http.ServeMux:
//
//	http.Handle("/debug/afez/", http.StripPrefix("/debug/afez", afez.Handler(c)))
//
// The page groups AFE rows by pool: id, ref-count (idle+in-use), idle,
// in-use, transport EWMA, e2e EWMA, last-connected age. AFEs with
// ref-count == 0 (empty buckets awaiting GC) are faded.
//
// Complements the other debug views:
//   - sessionz: per-session state, latency histograms, slow-vRPC log.
//   - flightz : live in-flight vRPCs.
//   - channelz: gRPC connection pool state.
package afez

import (
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
	"time"

	"cloud.google.com/go/bigtable"
	btransport "cloud.google.com/go/bigtable/internal/transport"
)

// Handler returns an http.Handler that renders the per-AFE view of c's
// session pools. Returns a handler that serves an empty page when c has
// no session pool provider (e.g. EnableSessionPool is off).
//
// Routes (relative to the mount point):
//
//	GET /               → HTML table of all AFEs across all pools.
//	GET /?format=json   → JSON array of every AFE row.
//
// HTML responses set Cache-Control: no-store and a 5-second
// auto-refresh; JSON responses set Content-Type: application/json.
func Handler(c *bigtable.Client) http.Handler {
	return HandlerFromProvider(providerForClient(c))
}

// HandlerFromProvider is Handler for an arbitrary SessionDebugProvider —
// useful for tests and for adapters that fan multiple clients into one
// debug surface.
func HandlerFromProvider(p bigtable.SessionDebugProvider) http.Handler {
	mux := http.NewServeMux()
	srv := &server{provider: p}
	mux.HandleFunc("/", srv.handleIndex)
	return mux
}

func providerForClient(c *bigtable.Client) bigtable.SessionDebugProvider {
	if c == nil {
		return nil
	}
	return c.SessionDebug()
}

type server struct {
	provider bigtable.SessionDebugProvider
}

// Row is one AFE — the JSON-friendly representation the HTML template
// renders and the ?format=json response emits verbatim.
type Row struct {
	Pool           string        `json:"pool"`
	AfeID          int64         `json:"afeId"`
	AfeIDHex       string        `json:"afeIdHex"`
	RefCount       int           `json:"refCount"`
	IdleCount      int           `json:"idleCount"`
	InUseCount     int           `json:"inUseCount"`
	TransportEwma  time.Duration `json:"transportEwmaNanos"`
	E2eEwma        time.Duration `json:"e2eEwmaNanos"`
	LastConnected  time.Time     `json:"lastConnected"`
	IdleAge        time.Duration `json:"idleAgeNanos"`
	PendingGC      bool          `json:"pendingGC"`
}

type page struct {
	Generated time.Time
	Pools     []poolBlock
	Total     int
}

type poolBlock struct {
	Name string
	Rows []Row
}

func (s *server) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" && r.URL.Path != "" {
		http.NotFound(w, r)
		return
	}
	now := time.Now()
	pools, total := s.collect(now)

	if r.URL.Query().Get("format") == "json" {
		var flat []Row
		for _, pb := range pools {
			flat = append(flat, pb.Rows...)
		}
		writeJSON(w, struct {
			CapturedAt time.Time `json:"capturedAt"`
			AFEs       []Row     `json:"afes"`
			Total      int       `json:"total"`
		}{now, flat, total})
		return
	}

	writeHTML(w, tpl, page{Generated: now, Pools: pools, Total: total})
}

// collect walks every pool snapshot and returns per-pool rows sorted by
// AFE id (sessionList.Snapshot already sorts).
func (s *server) collect(now time.Time) (pools []poolBlock, total int) {
	if s.provider == nil {
		return nil, 0
	}
	for _, p := range s.provider.Snapshot() {
		if len(p.AFEs) == 0 {
			continue
		}
		rows := make([]Row, 0, len(p.AFEs))
		for _, a := range p.AFEs {
			inUse := a.RefCount - a.IdleCount
			if inUse < 0 {
				inUse = 0
			}
			var idleAge time.Duration
			if !a.LastConnected.IsZero() {
				idleAge = now.Sub(a.LastConnected)
			}
			rows = append(rows, Row{
				Pool:          p.Name,
				AfeID:         a.ID,
				AfeIDHex:      fmt.Sprintf("%x", uint64(a.ID)),
				RefCount:      a.RefCount,
				IdleCount:     a.IdleCount,
				InUseCount:    inUse,
				TransportEwma: a.TransportEwma,
				E2eEwma:       a.E2eEwma,
				LastConnected: a.LastConnected,
				IdleAge:       idleAge,
				PendingGC:     a.RefCount == 0,
			})
			total++
		}
		pools = append(pools, poolBlock{Name: p.Name, Rows: rows})
	}
	return pools, total
}

func writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func writeHTML(w http.ResponseWriter, tpl *template.Template, data interface{}) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	if err := tpl.Execute(w, data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// roundDuration keeps the EWMA columns terse without dropping the
// resolution operators care about at the tail. Mirrors flightz.
func roundDuration(d time.Duration) time.Duration {
	switch {
	case d < 0:
		return -roundDuration(-d)
	case d == 0:
		return 0
	case d < time.Millisecond:
		return d.Round(time.Microsecond)
	case d < time.Second:
		return d.Round(time.Millisecond)
	default:
		return d.Round(10 * time.Millisecond)
	}
}

var _ = btransport.AfeSnapshotRow{} // guard against accidental unused-import trim

var funcs = template.FuncMap{
	"dur": func(d time.Duration) string {
		if d == 0 {
			return "—"
		}
		return roundDuration(d).String()
	},
	"age": func(d time.Duration) string {
		if d <= 0 {
			return "—"
		}
		return roundDuration(d).String()
	},
}

var tpl = template.Must(template.New("afez").Funcs(funcs).Parse(tplSrc))

const tplSrc = `<!doctype html>
<html lang="en"><head>
<meta charset="utf-8">
<meta http-equiv="refresh" content="5">
<title>afez</title>
<style>
body { font-family: -apple-system,Segoe UI,Roboto,sans-serif; margin: 1rem; }
h1 { font-size: 1.1rem; margin: 0 0 .5rem; }
h2 { font-size: .95rem; margin: 1.25rem 0 .25rem; color: #444; }
table { border-collapse: collapse; width: 100%; font-size: .85rem; }
th, td { padding: .3rem .55rem; text-align: right; border-bottom: 1px solid #eee; font-variant-numeric: tabular-nums; }
th:first-child, td:first-child { text-align: left; }
th { background: #f6f6f6; font-weight: 600; }
tr.pending-gc td { color: #999; font-style: italic; }
.empty { color: #888; margin: 1rem 0; }
.gen { color: #888; font-size: .8rem; margin-top: 1rem; }
</style>
</head><body>
<h1>Bigtable AFE view — {{.Total}} AFE(s) across {{len .Pools}} pool(s)</h1>
{{if .Pools}}
{{range .Pools}}
<h2>{{.Name}}</h2>
<table>
  <thead><tr>
    <th>AFE id</th><th>ref</th><th>idle</th><th>in-use</th>
    <th>transport EWMA</th><th>e2e EWMA</th><th>last connected</th>
  </tr></thead>
  <tbody>
  {{range .Rows}}
  <tr{{if .PendingGC}} class="pending-gc"{{end}}>
    <td>{{if .AfeID}}0x{{.AfeIDHex}}{{else}}<em>unknown</em>{{end}}</td>
    <td>{{.RefCount}}</td>
    <td>{{.IdleCount}}</td>
    <td>{{.InUseCount}}</td>
    <td>{{dur .TransportEwma}}</td>
    <td>{{dur .E2eEwma}}</td>
    <td>{{age .IdleAge}} ago</td>
  </tr>
  {{end}}
  </tbody>
</table>
{{end}}
{{else}}
<p class="empty">No AFE buckets recorded yet. Sessions may still be handshaking, or session pooling may be disabled.</p>
{{end}}
<p class="gen">Generated {{.Generated.Format "15:04:05.000 MST"}} — auto-refresh 5s.</p>
</body></html>`
