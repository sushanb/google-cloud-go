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

// Package sessionz provides an HTTP debug UI for a *bigtable.Client's session
// pools — analogous to net/http/pprof for goroutines or rantav/go-grpc-channelz
// for gRPC channels.
//
// Mount the handler under any path you like (the routes are relative to the
// mount point):
//
//	c, _ := bigtable.NewClientWithConfig(ctx, "proj", "inst",
//	    bigtable.ClientConfig{EnableSessionPool: true})
//	http.Handle("/debug/sessionz/", http.StripPrefix("/debug/sessionz",
//	    sessionz.Handler(c)))
//	go http.ListenAndServe(":6060", nil)
//
// The handler pulls live state on each request — there are no background
// goroutines and no per-RPC overhead beyond two atomic increments. Mounting
// it on a client whose ClientConfig.EnableSessionPool is false yields a
// page reporting that session pooling is disabled.
package sessionz

import (
	"encoding/json"
	"html/template"
	"net/http"
	"strings"
	"time"

	"cloud.google.com/go/bigtable"
	btransport "cloud.google.com/go/bigtable/internal/transport"
)

// Handler returns an http.Handler that renders a debug UI for the session
// pools owned by c. The returned handler is bound to c for its lifetime —
// close c and the handler will simply report no pools.
//
// Routes (relative to the mount point):
//
//	GET /                  → HTML index of pools.
//	GET /pool/{key}        → HTML detail of one pool and its sessions.
//	GET /?format=json      → JSON dump of every pool snapshot.
//	GET /pool/{key}?format=json → JSON dump of one pool.
//
// All HTML pages set Cache-Control: no-store; JSON responses set
// Content-Type: application/json.
func Handler(c *bigtable.Client) http.Handler {
	return HandlerFromProvider(providerForClient(c))
}

// HandlerFromProvider is the same as Handler but accepts an arbitrary
// SessionDebugProvider — useful for tests and for adapters that want to
// fan multiple clients into one debug surface.
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

func (s *server) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" && r.URL.Path != "" {
		http.NotFound(w, r)
		return
	}
	snaps := s.snapshot()
	if r.URL.Query().Get("format") == "json" {
		writeJSON(w, snaps)
		return
	}
	writeHTML(w, indexTpl, indexData{
		Pools:      snaps,
		Generated:  time.Now(),
		HasProvider: s.provider != nil,
	})
}

func (s *server) handlePool(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/pool/")
	if key == "" {
		http.NotFound(w, r)
		return
	}
	snaps := s.snapshot()
	var found *btransport.PoolSnapshot
	for i := range snaps {
		if snaps[i].Name == key {
			found = &snaps[i]
			break
		}
	}
	if found == nil {
		http.NotFound(w, r)
		return
	}
	if r.URL.Query().Get("format") == "json" {
		writeJSON(w, found)
		return
	}
	writeHTML(w, poolTpl, poolData{
		Pool:      *found,
		Generated: time.Now(),
	})
}

func (s *server) snapshot() []btransport.PoolSnapshot {
	if s.provider == nil {
		return nil
	}
	return s.provider.Snapshot()
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

type indexData struct {
	Pools       []btransport.PoolSnapshot
	Generated   time.Time
	HasProvider bool
}

type poolData struct {
	Pool      btransport.PoolSnapshot
	Generated time.Time
}

var funcs = template.FuncMap{
	"age": func(t time.Time) string {
		if t.IsZero() {
			return "—"
		}
		return roundDuration(time.Since(t)).String()
	},
	"untilNow": func(t time.Time) string {
		if t.IsZero() {
			return "—"
		}
		d := time.Until(t)
		if d < 0 {
			return "expired " + roundDuration(-d).String() + " ago"
		}
		return "in " + roundDuration(d).String()
	},
	"dur": func(d time.Duration) string {
		if d == 0 {
			return "—"
		}
		return roundDuration(d).String()
	},
	"orDash": func(s string) string {
		if s == "" {
			return "—"
		}
		return s
	},
	"stateClass": func(state string) string {
		switch state {
		case "Active":
			return "state-active"
		case "Starting", "New":
			return "state-starting"
		case "Closing":
			return "state-closing"
		case "Closed":
			return "state-closed"
		}
		return ""
	},
	"timestamp": func(t time.Time) string {
		return t.Format(time.RFC3339)
	},
}

func roundDuration(d time.Duration) time.Duration {
	switch {
	case d > time.Hour:
		return d.Round(time.Minute)
	case d > time.Minute:
		return d.Round(time.Second)
	case d > time.Second:
		return d.Round(10 * time.Millisecond)
	default:
		return d.Round(time.Microsecond)
	}
}

const indexTplSrc = `<!doctype html>
<html><head>
<meta charset="utf-8">
<title>bigtable sessionz</title>
<meta http-equiv="refresh" content="5">
<style>
body{font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;margin:1.5em;color:#222;background:#fafafa}
h1{font-size:1.3em;margin:0 0 .5em 0}
h2{font-size:1em;color:#666;font-weight:normal;margin:0 0 1em 0}
table{border-collapse:collapse;width:100%;background:#fff;box-shadow:0 1px 2px rgba(0,0,0,.06)}
th,td{padding:.45em .8em;text-align:left;border-bottom:1px solid #eee;font-size:.92em}
th{background:#f3f3f3;font-weight:600}
tr:hover td{background:#fafafa}
.num{text-align:right;font-variant-numeric:tabular-nums}
a{color:#1a5fb4;text-decoration:none}
a:hover{text-decoration:underline}
.empty{color:#888;font-style:italic;padding:.8em 0}
.foot{margin-top:1.5em;color:#888;font-size:.8em}
</style>
</head><body>
<h1>Bigtable Session Pools</h1>
<h2>generated {{timestamp .Generated}} · auto-refresh 5s</h2>
{{if not .HasProvider}}
<div class="empty">Session pooling is disabled on this client (ClientConfig.EnableSessionPool is false).</div>
{{else if not .Pools}}
<div class="empty">No session pools — no session-routed traffic has run yet.</div>
{{else}}
<table>
<thead><tr>
<th>Pool</th><th>Type</th><th>Picker</th>
<th class="num">Sessions</th><th class="num">Ready</th><th class="num">Starting</th>
<th class="num">In&nbsp;use</th><th class="num">Pending</th><th class="num">Min/Max</th>
</tr></thead>
<tbody>
{{range .Pools}}
<tr>
<td><a href="pool/{{.Name}}">{{.Name}}</a></td>
<td>{{.SessionType}}</td>
<td>{{.PickerType}}</td>
<td class="num">{{.TotalSessions}}</td>
<td class="num">{{.ReadyCount}}</td>
<td class="num">{{.StartingCount}}</td>
<td class="num">{{.InUseCount}}</td>
<td class="num">{{.PendingCount}}</td>
<td class="num">{{.MinSessions}} / {{.MaxSessions}}</td>
</tr>
{{end}}
</tbody>
</table>
{{end}}
<div class="foot"><a href="?format=json">JSON</a></div>
</body></html>
`

const poolTplSrc = `<!doctype html>
<html><head>
<meta charset="utf-8">
<title>bigtable sessionz · {{.Pool.Name}}</title>
<meta http-equiv="refresh" content="5">
<style>
body{font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;margin:1.5em;color:#222;background:#fafafa}
h1{font-size:1.3em;margin:0 0 .25em 0}
h2{font-size:1em;color:#666;font-weight:normal;margin:0 0 1em 0}
table{border-collapse:collapse;width:100%;background:#fff;box-shadow:0 1px 2px rgba(0,0,0,.06)}
th,td{padding:.4em .7em;text-align:left;border-bottom:1px solid #eee;font-size:.88em;vertical-align:top}
th{background:#f3f3f3;font-weight:600}
tr:hover td{background:#fafafa}
.num{text-align:right;font-variant-numeric:tabular-nums}
.mono{font-family:ui-monospace,Consolas,monospace;font-size:.85em}
.state-active{color:#197a1f;font-weight:600}
.state-starting{color:#a07000;font-weight:600}
.state-closing{color:#a04500;font-weight:600}
.state-closed{color:#888}
a{color:#1a5fb4;text-decoration:none}
a:hover{text-decoration:underline}
.summary{margin-bottom:1em;background:#fff;padding:.75em 1em;box-shadow:0 1px 2px rgba(0,0,0,.06)}
.summary span{display:inline-block;margin-right:1.4em}
.summary b{color:#444}
.empty{color:#888;font-style:italic;padding:.8em 0}
.foot{margin-top:1.5em;color:#888;font-size:.8em}
</style>
</head><body>
<h1>Pool <span class="mono">{{.Pool.Name}}</span></h1>
<h2><a href="../">← all pools</a> · generated {{timestamp .Generated}} · auto-refresh 5s</h2>
<div class="summary">
<span><b>Type</b> {{.Pool.SessionType}}</span>
<span><b>Picker</b> {{.Pool.PickerType}}</span>
<span><b>Min / Max</b> {{.Pool.MinSessions}} / {{.Pool.MaxSessions}}</span>
<span><b>Ready</b> {{.Pool.ReadyCount}}</span>
<span><b>Starting</b> {{.Pool.StartingCount}}</span>
<span><b>In&nbsp;use</b> {{.Pool.InUseCount}}</span>
<span><b>Pending</b> {{.Pool.PendingCount}}</span>
<span><b>Total</b> {{.Pool.TotalSessions}}</span>
</div>
{{if not .Pool.Sessions}}
<div class="empty">No sessions registered in this pool right now.</div>
{{else}}
<table>
<thead><tr>
<th>Session</th><th>State</th><th>Transport</th><th>AFE region</th><th>AFE subzone</th>
<th class="num">GFE&nbsp;id</th><th class="num">AFE&nbsp;id</th>
<th class="num">OK</th><th class="num">Err</th><th class="num">In&nbsp;flight</th>
<th class="num">Outstanding</th><th class="num">EWMA</th>
<th>Last&nbsp;activity</th><th>Last&nbsp;state&nbsp;change</th><th>Next&nbsp;heartbeat</th>
</tr></thead>
<tbody>
{{range .Pool.Sessions}}
<tr>
<td class="mono">{{.LogName}}</td>
<td class="{{stateClass .State}}">{{.State}}</td>
<td>{{orDash .Peer.TransportType}}</td>
<td>{{orDash .Peer.ApplicationFrontendRegion}}</td>
<td>{{orDash .Peer.ApplicationFrontendSubzone}}</td>
<td class="num">{{.Peer.GoogleFrontendID}}</td>
<td class="num">{{.Peer.ApplicationFrontendID}}</td>
<td class="num">{{.OkRpcs}}</td>
<td class="num">{{.ErrorRpcs}}</td>
<td class="num">{{.ActiveRpcs}}</td>
<td class="num">{{.Handle.Outstanding}}</td>
<td class="num">{{dur .Handle.EwmaLatency}}</td>
<td>{{age .Handle.LastActivity}} ago</td>
<td>{{age .LastStateChange}} ago</td>
<td>{{untilNow .NextHeartbeat}}</td>
</tr>
{{end}}
</tbody>
</table>
{{end}}
<div class="foot"><a href="?format=json">JSON</a> · <a href="../">all pools</a></div>
</body></html>
`

var (
	indexTpl = template.Must(template.New("index").Funcs(funcs).Parse(indexTplSrc))
	poolTpl  = template.Must(template.New("pool").Funcs(funcs).Parse(poolTplSrc))
)
