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
	"strings"
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
	all := r.URL.Query().Get("all") == "1"
	rows := s.snapshot()
	total := len(rows)
	hidden := 0
	if !all {
		filtered := rows[:0]
		for _, row := range rows {
			// Default view hides remote :443 conns — those are the
			// classic-path CFE / metrics-export channels, not the AFE
			// data path most tcpz users care about. ?all=1 restores.
			if strings.HasSuffix(row.RemoteAddr, ":443") {
				hidden++
				continue
			}
			filtered = append(filtered, row)
		}
		rows = filtered
	}

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
		Total     int
		Hidden    int
		ShowAll   bool
		Generated time.Time
	}{
		Rows:      rows,
		Count:     len(rows),
		Total:     total,
		Hidden:    hidden,
		ShowAll:   all,
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
	"num64": func(v uint64) string {
		if v == 0 {
			return "—"
		}
		return fmt.Sprintf("%d", v)
	},
	"pct": func(v float64) string {
		if v == 0 {
			return "—"
		}
		if v < 0.01 {
			return "<0.01%"
		}
		return fmt.Sprintf("%.2f%%", v)
	},
	// rate renders a bytes/sec value as a human-scaled MB/s or KB/s.
	"rate": func(v uint64) string {
		if v == 0 {
			return "—"
		}
		f := float64(v)
		switch {
		case f >= 1<<20:
			return fmt.Sprintf("%.1f MB/s", f/(1<<20))
		case f >= 1<<10:
			return fmt.Sprintf("%.1f KB/s", f/(1<<10))
		default:
			return fmt.Sprintf("%.0f B/s", f)
		}
	},
	// bytes renders a byte count similarly for BytesSent / BytesRetrans.
	"bytes": func(v uint64) string {
		if v == 0 {
			return "—"
		}
		f := float64(v)
		switch {
		case f >= 1<<30:
			return fmt.Sprintf("%.1f GiB", f/(1<<30))
		case f >= 1<<20:
			return fmt.Sprintf("%.1f MiB", f/(1<<20))
		case f >= 1<<10:
			return fmt.Sprintf("%.1f KiB", f/(1<<10))
		default:
			return fmt.Sprintf("%d B", v)
		}
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
.ca-Open { color: #197a1f; }
.ca-Disorder { color: #a06a00; }
.ca-CWR { color: #a04500; }
.ca-Recovery { color: #b32222; }
.ca-Loss { color: #b32222; font-weight: 600; }
</style>
</head>
<body>
<h1>tcpz — {{.Count}} conn{{if ne .Count 1}}s{{end}}{{if .Hidden}} <span style="color:#888;font-weight:400;font-size:.85em">({{.Hidden}} :443 hidden)</span>{{end}}</h1>
<div class="meta">Snapshot at {{.Generated.Format "15:04:05.000"}} · auto-refresh 2s · <a href="?format=json{{if .ShowAll}}&amp;all=1{{end}}">JSON</a>
{{if .ShowAll}} · <a href="?">hide :443 (default)</a>{{else if .Hidden}} · <a href="?all=1">show all ({{.Total}})</a>{{end}}</div>
{{if not .Rows}}
<div class="empty">No conns registered. Either the client uses DirectPath (xDS bypasses the standard dialer, so nothing is captured), no traffic has been dialed yet, or bigtable.TCPStats was never passed into the Client's options.</div>
{{else}}
<table>
<thead><tr>
<th class="mono" title="Peer address (ip:port).">Remote</th>
<th class="mono" title="Local socket address.">Local</th>
<th title="Time since this conn was dialed.">Age</th>
<th title="Linux TCP state (ESTABLISHED, CLOSE_WAIT, etc.).">State</th>
<th title="Congestion-control state: Open=healthy, Disorder=watching for loss, CWR=cwnd-reducing after ECN, Recovery=fast-retransmitting, Loss=RTO-driven collapse (worst).">Ca</th>
<th class="num" title="RTO backoff count. >0 means we've timed out at least once and are waiting exponentially longer.">Bkoff</th>
<th class="num" title="Smoothed round-trip time — the primary 'wire' latency signal.">RTT</th>
<th class="num" title="RTT variance (jitter). High values suggest an unstable path.">RTTVar</th>
<th class="num" title="Minimum RTT observed on this conn — the floor of what the network can deliver.">MinRTT</th>
<th class="num" title="Current retransmit timeout — kernel will re-send unacked bytes after this long. Grows with backoff.">RTO</th>
<th class="num" title="Send MSS (max segment size).">MSS</th>
<th class="num" title="Send congestion window in MSS units.">CWnd</th>
<th class="num" title="Slow-start threshold in MSS units. When cwnd &lt; ssthresh we're in slow-start (often after a loss event).">SSTh</th>
<th class="num" title="Recent retransmits (kernel counter).">Retr</th>
<th class="num" title="Total retransmits since conn open — high count means path is lossy.">TotalRetr</th>
<th class="num" title="Retransmit ratio: bytes retransmitted / bytes sent. The 'actual loss rate' this conn observed.">RtxRate</th>
<th class="num" title="Segments the kernel considers lost right now.">Lost</th>
<th class="num" title="Segments selectively-ACK'd by the receiver.">SACKd</th>
<th class="num" title="Segments sent but not yet ACKed (in flight).">Unacked</th>
<th class="num" title="Duplicate-SACK count — number of SPURIOUS retransmits (we resent bytes the receiver actually got). High DSACK relative to TotalRetr = timing false-positives, not real loss.">DSACK</th>
<th class="num" title="Times reordering was observed. Reordering can trigger fast-retransmit even without loss.">ReordS</th>
<th class="num" title="Packets delivered with ECN Congestion-Experienced marks. Non-zero = a router is signaling congestion before dropping.">ECN</th>
<th class="num" title="Total bytes sent (data).">Sent</th>
<th class="num" title="Total bytes retransmitted (data).">Retrans</th>
<th class="num" title="Recent delivery rate (BBR estimate).">DelRate</th>
<th class="num" title="Bytes buffered but not yet on wire. High = we're app-limited or CPU-limited, not network-limited.">NotSent</th>
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
<td class="ca-{{or .CAState "Unknown"}}">{{or .CAState "—"}}</td>
<td class="num">{{num .Backoff}}</td>
<td class="num">{{dur .RTT}}</td>
<td class="num">{{dur .RTTVar}}</td>
<td class="num">{{dur .MinRTT}}</td>
<td class="num">{{dur .RTO}}</td>
<td class="num">{{num .MSS}}</td>
<td class="num">{{num .SndCwnd}}</td>
<td class="num">{{num .SndSsthresh}}</td>
<td class="num">{{num .Retransmits}}</td>
<td class="num">{{num .TotalRetrans}}</td>
<td class="num">{{pct .RetransRatioPct}}</td>
<td class="num">{{num .Lost}}</td>
<td class="num">{{num .Sacked}}</td>
<td class="num">{{num .Unacked}}</td>
<td class="num">{{num .DsackDups}}</td>
<td class="num">{{num .ReordSeen}}</td>
<td class="num">{{num .DeliveredCE}}</td>
<td class="num">{{bytes .BytesSent}}</td>
<td class="num">{{bytes .BytesRetrans}}</td>
<td class="num">{{rate .DeliveryRate}}</td>
<td class="num">{{num .NotsentBytes}}</td>
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
