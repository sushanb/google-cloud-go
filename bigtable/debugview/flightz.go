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

// flightz view — one row per in-flight vRPC, oldest at the top.
// Answers the incident question "what's stuck right now?" without
// traces or goroutine dumps.

package debugview

import (
	"html/template"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"cloud.google.com/go/bigtable"
	btransport "cloud.google.com/go/bigtable/internal/transport"
)

func newFlightzHandler(p bigtable.SessionDebugProvider) http.Handler {
	mux := http.NewServeMux()
	srv := &flightzServer{provider: p}
	mux.HandleFunc("/", srv.handleIndex)
	mux.HandleFunc("/pool/", srv.handlePool)
	return mux
}

type flightzServer struct {
	provider bigtable.SessionDebugProvider
}

// flightzRow is one in-flight vRPC — the JSON-friendly representation
// the HTML template renders and the ?format=json response emits
// verbatim. JSON field names stable across the fold.
type flightzRow struct {
	Pool              string                      `json:"pool"`
	Session           string                      `json:"session"`
	SessionState      string                      `json:"sessionState"`
	RpcID             int64                       `json:"rpcId"`
	Method            string                      `json:"method"`
	SentAt            time.Time                   `json:"sentAt"`
	Age               time.Duration               `json:"ageNanos"`
	Deadline          time.Time                   `json:"deadline,omitempty"`
	DeadlineRemaining time.Duration               `json:"deadlineRemainingNanos,omitempty"`
	HasDeadline       bool                        `json:"hasDeadline"`
	Attempt           int32                       `json:"attempt"`
	Peer              btransport.PeerInfoSnapshot `json:"peer"`
}

type flightzPage struct {
	Generated      time.Time
	Rows           []flightzRow
	TotalSessions  int
	ActiveSessions int
	PoolFilter     string // empty for cross-pool view
	// LinkBase is the relative prefix templates use for cross-view links
	// (../sessionz/ etc). "../" on the /flightz/ index; "../../" on the
	// /flightz/pool/{key} sub-page so links resolve to /debug/<view>/.
	LinkBase string
}

func (s *flightzServer) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" && r.URL.Path != "" {
		http.NotFound(w, r)
		return
	}
	s.render(w, r, "")
}

func (s *flightzServer) handlePool(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/pool/")
	if key == "" {
		http.NotFound(w, r)
		return
	}
	s.render(w, r, key)
}

func (s *flightzServer) render(w http.ResponseWriter, r *http.Request, poolFilter string) {
	now := time.Now()
	rows, totalSess, activeSess := s.collect(now, poolFilter)
	// Oldest-first: the stuck rows the operator opened the page for.
	sort.Slice(rows, func(i, j int) bool { return rows[i].Age > rows[j].Age })

	if r.URL.Query().Get("format") == "json" {
		writeJSON(w, struct {
			CapturedAt     time.Time    `json:"capturedAt"`
			InFlight       []flightzRow `json:"inFlight"`
			TotalSessions  int          `json:"totalSessions"`
			ActiveSessions int          `json:"activeSessions"`
			PoolFilter     string       `json:"poolFilter,omitempty"`
		}{now, rows, totalSess, activeSess, poolFilter})
		return
	}

	linkBase := "../"
	if poolFilter != "" {
		linkBase = "../../"
	}
	writeHTML(w, flightzTpl, flightzPage{
		Generated:      now,
		Rows:           rows,
		TotalSessions:  totalSess,
		ActiveSessions: activeSess,
		PoolFilter:     poolFilter,
		LinkBase:       linkBase,
	})
}

// collect walks every pool snapshot and returns one flightzRow per
// in-flight vRPC. When poolFilter is non-empty, only that pool's rows
// appear; totalSess / activeSess are always computed across the FULL
// client so the header numbers stay consistent between filtered and
// unfiltered views.
func (s *flightzServer) collect(now time.Time, poolFilter string) (rows []flightzRow, totalSess, activeSess int) {
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
			rows = append(rows, flightzRow{
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

// flightzAgeClass buckets an in-flight age into a CSS hotness class.
// Aligns with the plan's color spec: green ≤ 100 ms, gray ≤ 1 s,
// orange ≤ 5 s, red > 5 s.
func flightzAgeClass(d time.Duration) string {
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

func flightzFuncs() template.FuncMap {
	m := commonFuncs()
	m["dur"] = func(d time.Duration) string {
		if d == 0 {
			return "—"
		}
		return roundDurationShort(d).String()
	}
	m["remaining"] = func(d time.Duration) string {
		return roundDurationShort(d).String()
	}
	m["ageClass"] = flightzAgeClass
	m["peerShort"] = peerShort
	m["queryEscape"] = url.QueryEscape
	m["pathEscape"] = url.PathEscape
	return m
}

var flightzTpl = template.Must(template.New("flightz").Funcs(flightzFuncs()).Parse(flightzTplSrc))

const flightzTplSrc = `<!doctype html>
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
<td class="mono"><a href="{{$.LinkBase}}sessionz/pool/{{queryEscape .Pool}}#session-{{.Session}}" title="Open this session in sessionz">{{.Session}}</a></td>
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
<a href="{{.LinkBase}}sessionz/">sessionz</a>
<a href="{{.LinkBase}}channelz/">channelz</a>
<a href="{{.LinkBase}}configz/">configz</a>
</div>
</body></html>
`
