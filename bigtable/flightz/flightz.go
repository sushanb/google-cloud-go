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

// Package flightz renders a live snapshot of every vRPC currently
// in-flight across a Bigtable client's session pools. It answers the
// single most common incident question — "what's stuck right now?" —
// without needing traces, goroutine dumps, or log grepping.
//
// Mount into any http.ServeMux:
//
//	http.Handle("/debug/flightz/", http.StripPrefix("/debug/flightz",
//	    flightz.Handler(c)))
//
// The rendered page is a table of in-flight vRPCs, oldest at the top:
//
//	Age  Method     Ctx-deadline  RpcID  Session  Pool  Peer  State  Attempt
//
// When the pool is idle the page renders "No in-flight vRPCs." The
// HTML view sets a 2-second auto-refresh so operators can watch a
// suspect request advance (or stall) in real time.
//
// Complements the aggregate views:
//   - sessionz: per-pool session state, latency histograms, slow-vRPC log.
//   - channelz: gRPC connection pool state.
//   - configz : server-driven client configuration.
package flightz

import (
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"cloud.google.com/go/bigtable"
	btransport "cloud.google.com/go/bigtable/internal/transport"
)

// Handler returns an http.Handler that renders a live snapshot of every
// in-flight vRPC across c's session pools. Bound to c for its lifetime;
// closing c leaves the handler serving an empty snapshot.
//
// Routes (relative to the mount point):
//
//	GET /                       → HTML table, oldest in-flight first.
//	GET /pool/{key}             → HTML table filtered to one pool.
//	GET /?format=json           → JSON array of every in-flight vRPC.
//	GET /pool/{key}?format=json → JSON filtered to one pool.
//
// All HTML pages set Cache-Control: no-store and a 2-second
// auto-refresh; JSON responses set Content-Type: application/json.
func Handler(c *bigtable.Client) http.Handler {
	return HandlerFromProvider(providerForClient(c))
}

// HandlerFromProvider is the same as Handler but accepts an arbitrary
// SessionDebugProvider — useful for tests and for adapters that want
// to fan multiple clients into one debug surface.
func HandlerFromProvider(p bigtable.SessionDebugProvider) http.Handler {
	mux := http.NewServeMux()
	srv := &server{provider: p}
	mux.HandleFunc("/", srv.handleIndex)
	mux.HandleFunc("/pool/", srv.handlePool)
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

// Row is one in-flight vRPC — the JSON-friendly representation the
// HTML template renders and the ?format=json response emits verbatim.
type Row struct {
	Pool                   string                     `json:"pool"`
	Session                string                     `json:"session"`
	SessionState           string                     `json:"sessionState"`
	RpcID                  int64                      `json:"rpcId"`
	Method                 string                     `json:"method"`
	SentAt                 time.Time                  `json:"sentAt"`
	Age                    time.Duration              `json:"ageNanos"`
	Deadline               time.Time                  `json:"deadline,omitempty"`
	DeadlineRemaining      time.Duration              `json:"deadlineRemainingNanos,omitempty"`
	HasDeadline            bool                       `json:"hasDeadline"`
	Attempt                int32                      `json:"attempt"`
	Peer                   btransport.PeerInfoSnapshot `json:"peer"`
}

type page struct {
	Generated      time.Time
	Rows           []Row
	TotalSessions  int
	ActiveSessions int
	PoolFilter     string // empty for cross-pool view
}

func (s *server) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" && r.URL.Path != "" {
		http.NotFound(w, r)
		return
	}
	s.render(w, r, "")
}

func (s *server) handlePool(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/pool/")
	if key == "" {
		http.NotFound(w, r)
		return
	}
	s.render(w, r, key)
}

func (s *server) render(w http.ResponseWriter, r *http.Request, poolFilter string) {
	now := time.Now()
	rows, totalSess, activeSess := s.collect(now, poolFilter)
	// Oldest-first: the stuck rows the operator opened the page for.
	sort.Slice(rows, func(i, j int) bool { return rows[i].Age > rows[j].Age })

	if r.URL.Query().Get("format") == "json" {
		writeJSON(w, struct {
			CapturedAt     time.Time `json:"capturedAt"`
			InFlight       []Row     `json:"inFlight"`
			TotalSessions  int       `json:"totalSessions"`
			ActiveSessions int       `json:"activeSessions"`
			PoolFilter     string    `json:"poolFilter,omitempty"`
		}{now, rows, totalSess, activeSess, poolFilter})
		return
	}

	writeHTML(w, tpl, page{
		Generated:      now,
		Rows:           rows,
		TotalSessions:  totalSess,
		ActiveSessions: activeSess,
		PoolFilter:     poolFilter,
	})
}

// collect walks every pool snapshot and returns one Row per in-flight
// vRPC. When poolFilter is non-empty, only that pool's rows appear;
// totalSess / activeSess are always computed across the FULL client so
// the header numbers stay consistent between filtered and unfiltered
// views.
func (s *server) collect(now time.Time, poolFilter string) (rows []Row, totalSess, activeSess int) {
	if s.provider == nil {
		return nil, 0, 0
	}
	pools := s.provider.Snapshot()
	for _, p := range pools {
		for _, sess := range p.Sessions {
			totalSess++
			if sess.InFlight == nil {
				continue
			}
			activeSess++
			if poolFilter != "" && p.Name != poolFilter {
				continue
			}
			rows = append(rows, Row{
				Pool:              p.Name,
				Session:           sess.LogName,
				SessionState:      sess.State,
				RpcID:             sess.InFlight.RpcID,
				Method:            sess.InFlight.Method,
				SentAt:            sess.InFlight.SentAt,
				Age:               ageOf(now, sess.InFlight.SentAt),
				Deadline:          sess.InFlight.Deadline,
				DeadlineRemaining: remainingOf(now, sess.InFlight.Deadline),
				HasDeadline:       !sess.InFlight.Deadline.IsZero(),
				Attempt:           sess.InFlight.Attempt,
				Peer:              sess.Peer,
			})
		}
	}
	return rows, totalSess, activeSess
}

func ageOf(now, sentAt time.Time) time.Duration {
	if sentAt.IsZero() {
		return 0
	}
	return now.Sub(sentAt)
}

func remainingOf(now, deadline time.Time) time.Duration {
	if deadline.IsZero() {
		return 0
	}
	return deadline.Sub(now)
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

// --- rendering ---------------------------------------------------------------

// roundDuration keeps the age / deadline columns terse without dropping
// the millisecond resolution operators actually care about at the tail
// of the tail (a 6.42 s stuck request reads better than 6.418293s).
func roundDuration(d time.Duration) time.Duration {
	switch {
	case d < 0:
		return -roundDuration(-d)
	case d < time.Millisecond:
		return d.Round(time.Microsecond)
	case d < time.Second:
		return d.Round(time.Millisecond)
	default:
		return d.Round(10 * time.Millisecond)
	}
}

// ageClass buckets an in-flight age into a CSS hotness class. Aligns
// with the plan's color spec: green ≤ 100 ms, gray ≤ 1 s, orange
// ≤ 5 s, red > 5 s.
func ageClass(d time.Duration) string {
	switch {
	case d <= 100*time.Millisecond:
		return "age-cold"
	case d <= time.Second:
		return "age-warm"
	case d <= 5*time.Second:
		return "age-hot"
	default:
		return "age-stuck"
	}
}

// peerShort renders the AFE id in hex, truncated, followed by
// region/subzone. Empty when the header hasn't landed yet.
func peerShort(p btransport.PeerInfoSnapshot) string {
	if p.ApplicationFrontendID == 0 && p.ApplicationFrontendRegion == "" && p.ApplicationFrontendSubzone == "" {
		return ""
	}
	return fmt.Sprintf("%x/%s/%s",
		p.ApplicationFrontendID,
		p.ApplicationFrontendRegion,
		p.ApplicationFrontendSubzone)
}

var funcs = template.FuncMap{
	"dur": func(d time.Duration) string {
		if d == 0 {
			return "—"
		}
		return roundDuration(d).String()
	},
	"remaining": func(d time.Duration) string {
		return roundDuration(d).String()
	},
	"ageClass":   ageClass,
	"peerShort":  peerShort,
	"queryEscape": url.QueryEscape,
	"pathEscape":  url.PathEscape,
}

var tpl = template.Must(template.New("flightz").Funcs(funcs).Parse(tplSrc))

const tplSrc = `<!doctype html>
<html lang="en"><head>
<meta charset="utf-8">
<meta http-equiv="refresh" content="2">
<title>flightz{{if .PoolFilter}} — {{.PoolFilter}}{{end}}</title>
<style>
body { font: 13px/1.4 -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif; margin: 1.5em; color: #222; }
h1 { font-size: 1.15em; margin: 0 0 .3em 0; }
.sub { color: #666; font-size: .9em; margin-bottom: 1em; }
table { border-collapse: collapse; width: 100%; margin-top: .5em; }
th, td { text-align: left; padding: .35em .6em; border-bottom: 1px solid #eee; vertical-align: top; }
th { background: #f7f7f7; font-weight: 600; }
td.num { text-align: right; font-variant-numeric: tabular-nums; }
td.mono, td.age, td.deadline { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: .92em; }
.age-cold  { color: #2a7; }
.age-warm  { color: #666; }
.age-hot   { color: #b60; }
.age-stuck { color: #a11; font-weight: 600; }
.deadline-exp { color: #a11; font-weight: 600; }
.attempt-retry { color: #b60; font-weight: 600; }
.foot { margin-top: 2em; color: #999; font-size: .85em; }
.foot a { color: #06a; margin-right: 1em; }
.empty { color: #666; padding: 1.5em; background: #f7f7f7; border-radius: 4px; margin-top: .5em; }
</style>
</head><body>
<h1>flightz{{if .PoolFilter}} — <span class="mono">{{.PoolFilter}}</span>{{end}}</h1>
<div class="sub">
{{len .Rows}} in-flight vRPC{{if ne (len .Rows) 1}}s{{end}}
· {{.ActiveSessions}}/{{.TotalSessions}} sessions active
· generated {{.Generated.Format "15:04:05.000"}}
· auto-refresh every 2s
{{if .PoolFilter}} · <a href="../">all pools</a>{{end}}
</div>

{{if .Rows}}
<table>
<thead><tr>
<th class="num" title="Time since s.Send(sessionReq); sort key.">Age</th>
<th>Method</th>
<th title="ctx.Deadline() − now. Red when the deadline already passed but no response has landed.">Ctx-deadline</th>
<th class="num" title="Per-session monotonic RPC id. Small values on old ages = fresh session cold-start.">RpcID</th>
<th title="Session log-name. Links to the session's sessionz row.">Session</th>
{{if not $.PoolFilter}}<th title="Pool name. Links to this pool's flightz view.">Pool</th>{{end}}
<th title="AFE routing: id (hex) / region / subzone.">Peer</th>
<th title="Session lifecycle state; anything other than Active on an in-flight row is a red flag.">State</th>
<th class="num" title="Attempt number: 1 = first try, 2+ = retry from the retrying interceptor.">Attempt</th>
</tr></thead>
<tbody>
{{range .Rows}}
<tr>
<td class="num age {{ageClass .Age}}" title="Sent at {{.SentAt.Format "15:04:05.000"}}">{{dur .Age}}</td>
<td class="mono">{{.Method}}</td>
<td class="deadline{{if and .HasDeadline (lt .DeadlineRemaining 0)}} deadline-exp{{end}}">
  {{if .HasDeadline}}{{remaining .DeadlineRemaining}}{{else}}—{{end}}
</td>
<td class="num">{{.RpcID}}</td>
<td class="mono"><a href="../sessionz/pool/{{queryEscape .Pool}}#session-{{.Session}}" title="Open this session in sessionz">{{.Session}}</a></td>
{{if not $.PoolFilter}}<td class="mono"><a href="pool/{{pathEscape .Pool}}" title="Filter flightz to this pool">{{.Pool}}</a></td>{{end}}
<td class="mono">{{peerShort .Peer}}</td>
<td>{{.SessionState}}</td>
<td class="num{{if gt .Attempt 1}} attempt-retry{{end}}">{{.Attempt}}</td>
</tr>
{{end}}
</tbody>
</table>
{{else}}
<div class="empty">
No in-flight vRPCs.
{{if .TotalSessions}}All {{.TotalSessions}} session{{if ne .TotalSessions 1}}s{{end}} idle.{{else}}No session pools registered yet.{{end}}
</div>
{{end}}

<div class="foot">
<a href="?format=json">JSON</a>
<a href="../sessionz/">sessionz</a>
<a href="../channelz/">channelz</a>
<a href="../configz/">configz</a>
</div>
</body></html>
`
