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

// Package tcpz renders live per-connection TCP_INFO (RTT, retransmits,
// cwnd, MSS, TCP state) for every gRPC dial a bigtable client made
// through bigtable.TCPStats. Answers "is wire really <1ms?" without
// leaving the browser.
//
// Wiring:
//
//	stats := bigtable.NewTCPStats()
//	client, err := bigtable.NewClient(ctx, proj, inst, stats.ClientOption())
//	http.Handle("/debug/tcpz/", http.StripPrefix("/debug/tcpz", tcpz.Handler(stats)))
//
// Routes (relative to the mount point):
//
//	GET /              → HTML table, oldest dial first, 2-second refresh.
//	GET /?format=json  → JSON array of every registered conn's TCP_INFO.
//
// Linux only. On other platforms every row surfaces "tcp_info not
// supported on this platform" in the Err column. Not compatible with
// DirectPath (xDS bypasses the standard dialer, so nothing is captured).
package tcpz

import (
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
	"time"

	"cloud.google.com/go/bigtable"
	btransport "cloud.google.com/go/bigtable/internal/transport"
)

// Handler returns an http.Handler that renders per-connection TCP_INFO
// for every conn captured by stats. Bound to stats for its lifetime;
// draining the underlying registry (all conns closed) leaves the handler
// serving an empty snapshot.
func Handler(stats *bigtable.TCPStats) http.Handler {
	mux := http.NewServeMux()
	srv := &server{stats: stats}
	mux.HandleFunc("/", srv.handleIndex)
	return mux
}

type server struct {
	stats *bigtable.TCPStats
}

func (s *server) snapshot() []btransport.TCPInfoSnapshot {
	if s.stats == nil {
		return nil
	}
	return s.stats.Snapshot()
}

func (s *server) handleIndex(w http.ResponseWriter, r *http.Request) {
	rows := s.snapshot()

	if r.URL.Query().Get("format") == "json" {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		_ = json.NewEncoder(w).Encode(rows)
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	data := struct {
		Rows      []btransport.TCPInfoSnapshot
		Count     int
		Generated time.Time
	}{
		Rows:      rows,
		Count:     len(rows),
		Generated: time.Now(),
	}
	if err := tpl.Execute(w, data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

var funcs = template.FuncMap{
	"dur": func(d time.Duration) string {
		if d == 0 {
			return "—"
		}
		return d.String()
	},
	"ago": func(t time.Time) string {
		if t.IsZero() {
			return "—"
		}
		return time.Since(t).Round(time.Second).String()
	},
	"num": func(v uint32) string {
		if v == 0 {
			return "—"
		}
		return fmt.Sprintf("%d", v)
	},
	"or": func(s, fallback string) string {
		if s == "" {
			return fallback
		}
		return s
	},
}

var tpl = template.Must(template.New("tcpz").Funcs(funcs).Parse(tplSrc))

const tplSrc = `<!doctype html>
<html>
<head>
<meta charset="utf-8">
<title>tcpz — {{.Count}} conns</title>
<meta http-equiv="refresh" content="2">
<style>
body { font: 13px/1.4 -apple-system, "Segoe UI", Helvetica, Arial, sans-serif; margin: 1em; color: #222; }
h1 { font-size: 1.1em; margin: 0 0 .5em 0; }
.meta { color: #666; margin-bottom: .8em; }
table { border-collapse: collapse; width: 100%; }
th, td { text-align: left; padding: 4px 8px; border-bottom: 1px solid #eee; }
th { background: #f4f4f4; font-weight: 600; }
td.num { text-align: right; font-variant-numeric: tabular-nums; }
td.mono, th.mono { font-family: SFMono-Regular, Menlo, Consolas, monospace; font-size: 12px; }
tr:hover td { background: #fafafa; }
.empty { color: #888; margin: 2em 0; }
.err { color: #a04500; }
.state-ESTABLISHED { color: #197a1f; }
.state-CLOSE_WAIT, .state-FIN_WAIT1, .state-FIN_WAIT2, .state-CLOSING, .state-LAST_ACK, .state-TIME_WAIT { color: #a04500; }
</style>
</head>
<body>
<h1>tcpz — {{.Count}} conn{{if ne .Count 1}}s{{end}}</h1>
<div class="meta">Snapshot at {{.Generated.Format "15:04:05.000"}} · auto-refresh 2s · <a href="?format=json">JSON</a></div>
{{if not .Rows}}
<div class="empty">No conns registered. Either the client uses DirectPath (xDS bypasses the standard dialer, so nothing is captured), no traffic has been dialed yet, or bigtable.TCPStats was never passed into the Client's options.</div>
{{else}}
<table>
<thead><tr>
<th class="mono" title="Peer address (ip:port).">Remote</th>
<th class="mono" title="Local socket address.">Local</th>
<th title="Time since this conn was dialed.">Age</th>
<th title="Linux TCP state (ESTABLISHED, CLOSE_WAIT, etc.).">State</th>
<th class="num" title="Smoothed round-trip time — the primary 'wire' latency signal.">RTT</th>
<th class="num" title="RTT variance (jitter). High values suggest an unstable path.">RTTVar</th>
<th class="num" title="Minimum RTT observed on this conn — the floor of what the network can deliver.">MinRTT</th>
<th class="num" title="Send MSS (max segment size).">MSS</th>
<th class="num" title="Send congestion window in MSS units.">CWnd</th>
<th class="num" title="Recent retransmits (kernel counter).">Retr</th>
<th class="num" title="Total retransmits since conn open — high count means path is lossy.">TotalRetr</th>
<th class="num" title="Segments the kernel considers lost.">Lost</th>
<th class="num" title="Segments sent but not yet ACKed.">Unacked</th>
<th title="Time since the socket last received data.">LastRecv</th>
<th title="Time since the socket last sent data.">LastSent</th>
<th class="err" title="Populated when TCP_INFO couldn't be read on a live fd (e.g. non-Linux OS).">Err</th>
</tr></thead>
<tbody>
{{range .Rows}}
<tr>
<td class="mono">{{.RemoteAddr}}</td>
<td class="mono">{{.LocalAddr}}</td>
<td>{{ago .DialedAt}}</td>
<td class="state-{{or .State "UNKNOWN"}}">{{or .State "—"}}</td>
<td class="num">{{dur .RTT}}</td>
<td class="num">{{dur .RTTVar}}</td>
<td class="num">{{dur .MinRTT}}</td>
<td class="num">{{num .MSS}}</td>
<td class="num">{{num .SndCwnd}}</td>
<td class="num">{{num .Retransmits}}</td>
<td class="num">{{num .TotalRetrans}}</td>
<td class="num">{{num .Lost}}</td>
<td class="num">{{num .Unacked}}</td>
<td>{{dur .LastDataRecv}}</td>
<td>{{dur .LastDataSent}}</td>
<td class="err">{{.Err}}</td>
</tr>
{{end}}
</tbody>
</table>
{{end}}
</body>
</html>
`
