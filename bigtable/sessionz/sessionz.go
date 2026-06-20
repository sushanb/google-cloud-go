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
	"fmt"
	"html/template"
	"net/http"
	"sort"
	"strconv"
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
	var diverter btransport.DiverterSnapshot
	if s.provider != nil {
		diverter = s.provider.Diverter()
	}
	if r.URL.Query().Get("format") == "json" {
		writeJSON(w, struct {
			Pools    []btransport.PoolSnapshot `json:"pools"`
			Diverter btransport.DiverterSnapshot `json:"diverter"`
		}{snaps, diverter})
		return
	}
	writeHTML(w, indexTpl, indexData{
		Pools:       snaps,
		Diverter:    diverter,
		Generated:   time.Now(),
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
	Diverter    btransport.DiverterSnapshot
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
		case "Closing", "WaitServerClose":
			return "state-closing"
		case "Closed":
			return "state-closed"
		}
		return ""
	},
	"timestamp": func(t time.Time) string {
		return t.Format(time.RFC3339)
	},
	"signed": func(n int) string {
		if n > 0 {
			return "+" + strconv.Itoa(n)
		}
		return strconv.Itoa(n)
	},
	// clusterList renders a per-cluster response tally as a flat,
	// sorted-by-count list: "[cluster-c1: 1350, cluster-c2: 412]". Order
	// is stable (descending count, then alphabetical cluster id).
	"reverseSlow": func(s []btransport.SlowVRpcEvent) []btransport.SlowVRpcEvent {
		// Operators want newest-first in a slow log — the most recent
		// incident is the one they care about. Return a reversed copy
		// rather than mutating the source.
		out := make([]btransport.SlowVRpcEvent, len(s))
		for i := range s {
			out[i] = s[len(s)-1-i]
		}
		return out
	},
	"latencyClass": func(d time.Duration) string {
		// Color-code latency severity so spikes pop visually.
		switch {
		case d >= 5*time.Second:
			return "lat-red"
		case d >= 2*time.Second:
			return "lat-orange"
		case d >= time.Second:
			return "lat-amber"
		}
		return ""
	},
	"clusterList": func(m map[string]int64) template.HTML {
		if len(m) == 0 {
			return template.HTML("—")
		}
		type kv struct {
			k string
			v int64
		}
		pairs := make([]kv, 0, len(m))
		for k, v := range m {
			pairs = append(pairs, kv{k, v})
		}
		sort.Slice(pairs, func(i, j int) bool {
			if pairs[i].v != pairs[j].v {
				return pairs[i].v > pairs[j].v
			}
			return pairs[i].k < pairs[j].k
		})
		var b strings.Builder
		b.WriteString("[")
		for i, p := range pairs {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(`<span class="mono">`)
			b.WriteString(template.HTMLEscapeString(p.k))
			b.WriteString(`</span>: `)
			b.WriteString(strconv.FormatInt(p.v, 10))
		}
		b.WriteString("]")
		return template.HTML(b.String())
	},
	// opaqueID renders an int64 field that the server uses as an opaque
	// uint64 identifier (GFE / AFE id) as hex. The proto field is signed
	// so very large IDs come back negative ("-5686685877648862677") which
	// is technically the correct bit pattern but looks like a bug.
	"opaqueID": func(n int64) string {
		if n == 0 {
			return "—"
		}
		return fmt.Sprintf("0x%016x", uint64(n))
	},
	"scalingOutcome": func(ev btransport.ScalingEvent) string {
		switch {
		case ev.Requested > 0 && ev.Launched > 0:
			return strconv.Itoa(ev.Launched) + " launched"
		case ev.Requested > 0:
			return "0 launched (failed)"
		case ev.Requested < 0 && ev.Launched < 0:
			return strconv.Itoa(-ev.Launched) + " pruned"
		case ev.Requested < 0:
			return "0 pruned (none eligible)"
		default:
			return "—"
		}
	},
	// stateChips renders the per-state population summary as small colored
	// inline chips ("5 Active · 1 Closing · 2 WaitServerClose"). Returns
	// just the active count as plain text when the only state is Active.
	"stateChips": func(m map[string]int) template.HTML {
		if len(m) == 0 {
			return template.HTML("—")
		}
		// Render in a stable canonical order so similar pools line up.
		order := []string{"New", "Starting", "Active", "Closing", "WaitServerClose", "Closed"}
		var b strings.Builder
		for _, k := range order {
			v, ok := m[k]
			if !ok || v == 0 {
				continue
			}
			cls := "chip"
			switch k {
			case "Active":
				cls += " chip-active"
			case "Starting", "New":
				cls += " chip-starting"
			case "Closing", "WaitServerClose":
				cls += " chip-closing"
			case "Closed":
				cls += " chip-closed"
			}
			if b.Len() > 0 {
				b.WriteString(" ")
			}
			b.WriteString(`<span class="`)
			b.WriteString(cls)
			b.WriteString(`">`)
			b.WriteString(strconv.Itoa(v))
			b.WriteString(`&nbsp;`)
			b.WriteString(template.HTMLEscapeString(k))
			b.WriteString(`</span>`)
		}
		return template.HTML(b.String())
	},
	"bucketMax": func(b []btransport.LifetimeBucketCount) int {
		m := 0
		for _, x := range b {
			if x.Count > m {
				m = x.Count
			}
		}
		return m
	},
	"barWidth": func(count, max int) int {
		if max <= 0 {
			return 0
		}
		w := count * 100 / max
		if w == 0 && count > 0 {
			return 2
		}
		return w
	},
	"actualRatio": func(sess, classic int64) string {
		total := sess + classic
		if total == 0 {
			return "—"
		}
		return strconv.FormatFloat(float64(sess)/float64(total), 'f', 2, 64)
	},
	"sumMap": func(m map[string]int64) int64 {
		var total int64
		for _, v := range m {
			total += v
		}
		return total
	},
	"closeReasonsShort": func(m map[string]int64) string {
		if len(m) == 0 {
			return "—"
		}
		var total int64
		for _, v := range m {
			total += v
		}
		// Render as "N total" with the per-reason breakdown in the tooltip.
		return strconv.FormatInt(total, 10) + " total"
	},
	"msgBreakdown": func(m map[string]int64) string {
		if len(m) == 0 {
			return ""
		}
		keys := make([]string, 0, len(m))
		for k := range m {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		var b strings.Builder
		for i, k := range keys {
			if i > 0 {
				b.WriteString("\n")
			}
			b.WriteString(k)
			b.WriteString(": ")
			b.WriteString(strconv.FormatInt(m[k], 10))
		}
		return b.String()
	},
	// sparkline renders a tiny inline SVG line chart for a numeric series.
	// width × height are pixels; the SVG auto-scales the Y axis to fit
	// min/max of the data. Returns empty string if the series is too short.
	"sparkline": func(width, height int, color string, values []float64) template.HTML {
		if len(values) < 2 {
			return ""
		}
		mn, mx := values[0], values[0]
		for _, v := range values {
			if v < mn {
				mn = v
			}
			if v > mx {
				mx = v
			}
		}
		span := mx - mn
		if span == 0 {
			span = 1
		}
		var b strings.Builder
		b.WriteString(`<svg width="`)
		b.WriteString(strconv.Itoa(width))
		b.WriteString(`" height="`)
		b.WriteString(strconv.Itoa(height))
		b.WriteString(`" style="vertical-align:middle"><polyline fill="none" stroke="`)
		b.WriteString(color)
		b.WriteString(`" stroke-width="1.2" points="`)
		stepX := float64(width-2) / float64(len(values)-1)
		for i, v := range values {
			if i > 0 {
				b.WriteString(" ")
			}
			x := 1 + float64(i)*stepX
			y := float64(height-1) - (v-mn)/span*float64(height-2)
			b.WriteString(strconv.FormatFloat(x, 'f', 1, 64))
			b.WriteString(",")
			b.WriteString(strconv.FormatFloat(y, 'f', 1, 64))
		}
		b.WriteString(`"/></svg>`)
		return template.HTML(b.String())
	},
	// extractSeries pulls a single numeric series out of TimeSeries samples.
	// Used by the template to feed sparkline().
	"okSeries": func(ts []btransport.TimeSeriesSample) []float64 {
		out := make([]float64, len(ts))
		for i, s := range ts {
			out[i] = s.OkPerSec
		}
		return out
	},
	"errSeries": func(ts []btransport.TimeSeriesSample) []float64 {
		out := make([]float64, len(ts))
		for i, s := range ts {
			out[i] = s.ErrPerSec
		}
		return out
	},
	"sessionsSeries": func(ts []btransport.TimeSeriesSample) []float64 {
		out := make([]float64, len(ts))
		for i, s := range ts {
			out[i] = float64(s.Sessions)
		}
		return out
	},
	// msgCell renders a count + a click-to-expand HTML5 <details> disclosure
	// listing the per-type breakdown. Numbers stay tabular; the per-type rows
	// only render after the user clicks the count.
	"msgCell": func(total int64, m map[string]int64) template.HTML {
		if len(m) == 0 {
			return template.HTML(strconv.FormatInt(total, 10))
		}
		keys := make([]string, 0, len(m))
		for k := range m {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		var b strings.Builder
		b.WriteString(`<details class="msgcell"><summary>`)
		b.WriteString(strconv.FormatInt(total, 10))
		b.WriteString(`</summary><div class="msgcell-body">`)
		for _, k := range keys {
			b.WriteString(`<div><span class="msgcell-k">`)
			b.WriteString(template.HTMLEscapeString(k))
			b.WriteString(`</span><span class="msgcell-v">`)
			b.WriteString(strconv.FormatInt(m[k], 10))
			b.WriteString(`</span></div>`)
		}
		b.WriteString(`</div></details>`)
		return template.HTML(b.String())
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
.chip{display:inline-block;padding:.05em .45em;border-radius:3px;font-size:.78em;background:#eee;color:#444;font-variant-numeric:tabular-nums;white-space:nowrap}
.chip-active{background:#dff5d8;color:#197a1f}
.chip-starting{background:#fff1c8;color:#a07000}
.chip-closing{background:#ffe2cd;color:#a04500}
.chip-closed{background:#e0e0e0;color:#666}
.foot{margin-top:1.5em;color:#888;font-size:.8em}
</style>
</head><body>
<h1>Bigtable Session Pools</h1>
<h2>generated {{timestamp .Generated}} · auto-refresh 5s</h2>
{{if .HasProvider}}
<div style="margin-bottom:1em;background:#fff;padding:.6em 1em;box-shadow:0 1px 2px rgba(0,0,0,.06)">
<b>Diverter</b>
target session load <b>{{printf "%.2f" .Diverter.SessionLoad}}</b> ·
session picks {{.Diverter.SessionPicks}} · classic picks {{.Diverter.ClassicPicks}}
{{if or .Diverter.SessionPicks .Diverter.ClassicPicks}}
· actual ratio <b>{{actualRatio .Diverter.SessionPicks .Diverter.ClassicPicks}}</b>
{{end}}
</div>
{{end}}
{{if not .HasProvider}}
<div class="empty">Session pooling is disabled on this client (ClientConfig.EnableSessionPool is false).</div>
{{else if not .Pools}}
<div class="empty">No session pools — no session-routed traffic has run yet.</div>
{{else}}
<table>
<thead><tr>
<th>Pool</th><th>Type</th><th>Picker</th>
<th class="num">Sessions</th><th>States</th>
<th class="num">In&nbsp;use</th><th class="num">Pending</th><th class="num">Min/Max</th>
</tr></thead>
<tbody>
{{range .Pools}}
<tr>
<td><a href="pool/{{.Name}}">{{.Name}}</a></td>
<td>{{.SessionType}}</td>
<td>{{.PickerType}}</td>
<td class="num">{{.TotalSessions}}</td>
<td>{{stateChips .StateCounts}}</td>
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
tr:target td{background:#fff4c2}
tr:target td:first-child{border-left:3px solid #f0a000}
.lat-amber{background:#fff7d6;color:#7a5a00;font-weight:600}
.lat-orange{background:#ffe0c2;color:#a04500;font-weight:600}
.lat-red{background:#ffd0d0;color:#922;font-weight:700}
a{color:#1a5fb4;text-decoration:none}
a:hover{text-decoration:underline}
.summary{margin-bottom:1em;background:#fff;padding:.75em 1em;box-shadow:0 1px 2px rgba(0,0,0,.06)}
.summary span{display:inline-block;margin-right:1.4em}
.summary b{color:#444}
.empty{color:#888;font-style:italic;padding:.8em 0}
.chip{display:inline-block;padding:.05em .45em;border-radius:3px;font-size:.78em;background:#eee;color:#444;font-variant-numeric:tabular-nums;white-space:nowrap}
.chip-active{background:#dff5d8;color:#197a1f}
.chip-starting{background:#fff1c8;color:#a07000}
.chip-closing{background:#ffe2cd;color:#a04500}
.chip-closed{background:#e0e0e0;color:#666}
.foot{margin-top:1.5em;color:#888;font-size:.8em}
details.openreq{margin-bottom:1em;background:#fff;padding:.5em 1em;box-shadow:0 1px 2px rgba(0,0,0,.06)}
details.openreq>summary{cursor:pointer;color:#1a5fb4;padding:.25em 0}
details.openreq>summary:hover{color:#15498a}
.openreq-body h4{font-size:.9em;margin:.8em 0 .25em 0;color:#444}
.openreq-body pre{background:#f7f7f7;padding:.6em .8em;border-left:3px solid #1a5fb4;font-family:ui-monospace,Consolas,monospace;font-size:.82em;line-height:1.4;margin:0;overflow-x:auto}
details.msgcell{display:inline-block}
details.msgcell>summary{cursor:pointer;list-style:none;color:#1a5fb4;text-decoration:underline dotted}
details.msgcell>summary::-webkit-details-marker{display:none}
details.msgcell>summary:hover{color:#15498a}
.msgcell-body{position:absolute;background:#fff;border:1px solid #ddd;box-shadow:0 4px 10px rgba(0,0,0,.12);padding:.5em .75em;margin-top:.25em;font-size:.85em;text-align:left;z-index:10;min-width:14em}
.msgcell-body div{display:flex;justify-content:space-between;gap:1em;padding:.1em 0}
.msgcell-k{color:#444;font-family:ui-monospace,Consolas,monospace}
.msgcell-v{font-variant-numeric:tabular-nums;color:#222}
</style>
</head><body>
<h1>Pool <span class="mono">{{.Pool.Name}}</span></h1>
<h2><a href="../">← all pools</a> · generated {{timestamp .Generated}} · auto-refresh 5s</h2>
<div class="summary">
<span><b>Type</b> {{.Pool.SessionType}}</span>
<span><b>Picker</b> {{.Pool.PickerType}}</span>
<span><b>Min / Max</b> {{.Pool.MinSessions}} / {{.Pool.MaxSessions}}</span>
<span><b>Total</b> {{.Pool.TotalSessions}}</span>
<span><b>States</b> {{stateChips .Pool.StateCounts}}</span>
<span><b>In&nbsp;use</b> {{.Pool.InUseCount}}</span>
<span><b>Pending</b> {{.Pool.PendingCount}}</span>
<span><b>Starting</b> {{.Pool.StartingCount}}</span>
</div>
<div class="summary">
<span><b>Sessions opened</b> {{.Pool.SessionsOpened}}</span>
<span><b>Sessions closed</b> {{.Pool.SessionsClosed}}</span>
<span><b>Close reasons</b> {{msgCell (sumMap .Pool.CloseReasons) .Pool.CloseReasons}}</span>
<span><b>Config listener fires</b> {{.Pool.ListenerFires}}</span>
<span><b>Creation budget</b> {{.Pool.Throttler.InUse}} / {{.Pool.Throttler.Capacity}} (penalty {{dur .Pool.Throttler.PenaltyDuration}})</span>
</div>
<div class="summary">
<span><b>BackendLatency</b> p50 {{dur .Pool.LatencyP50}} · p95 {{dur .Pool.LatencyP95}} · p99 {{dur .Pool.LatencyP99}} <span class="mono">(n={{.Pool.LatencyN}})</span></span>
{{if .Pool.TimeSeries}}
<span><b>sessions</b> {{sparkline 120 28 "#1a5fb4" (sessionsSeries .Pool.TimeSeries)}}</span>
<span><b>ok/s</b> {{sparkline 120 28 "#197a1f" (okSeries .Pool.TimeSeries)}}</span>
<span><b>err/s</b> {{sparkline 120 28 "#a04500" (errSeries .Pool.TimeSeries)}}</span>
{{end}}
</div>

{{if .Pool.ClusterCounts}}
<div class="summary">
<b>Clusters</b> ({{sumMap .Pool.ClusterCounts}} total responses) — {{clusterList .Pool.ClusterCounts}}
</div>
{{end}}

{{if .Pool.OpenRequest}}
<details class="openreq">
<summary><b>OpenSessionRequest</b> <span class="mono">{{.Pool.OpenRequest.PayloadType}}</span> (protocol v{{.Pool.OpenRequest.ProtocolVersion}}) — click to expand</summary>
<div class="openreq-body">
{{if .Pool.OpenRequest.PayloadJSON}}
<h4>Payload</h4>
<pre>{{.Pool.OpenRequest.PayloadJSON}}</pre>
{{end}}
{{if .Pool.OpenRequest.FlagsJSON}}
<h4>FeatureFlags</h4>
<pre>{{.Pool.OpenRequest.FlagsJSON}}</pre>
{{end}}
</div>
</details>
{{end}}
{{if not .Pool.Sessions}}
<div class="empty">No sessions registered in this pool right now.</div>
{{else}}
<table>
<thead><tr>
<th>Session</th><th>State</th><th>Transport</th><th>AFE region</th><th>AFE subzone</th>
<th class="num">GFE&nbsp;id</th><th class="num">AFE&nbsp;id</th>
<th class="num">Ch&nbsp;#</th>
<th class="num">OK</th><th class="num">Err</th><th class="num">Retries</th>
<th class="num">Msgs&nbsp;sent</th><th class="num">Msgs&nbsp;recv</th><th class="num">In&nbsp;flight</th>
<th class="num">Outstanding</th><th class="num">Picks</th>
<th class="num">p50</th><th class="num">p95</th><th class="num">p99</th>
<th>Last&nbsp;activity</th><th>Last&nbsp;state&nbsp;change</th><th>Next&nbsp;heartbeat</th>
</tr></thead>
<tbody>
{{range .Pool.Sessions}}
<tr id="{{.LogName}}">
<td class="mono">{{.LogName}}</td>
<td class="{{stateClass .State}}">{{.State}}</td>
<td>{{orDash .Peer.TransportType}}</td>
<td>{{orDash .Peer.ApplicationFrontendRegion}}</td>
<td>{{orDash .Peer.ApplicationFrontendSubzone}}</td>
<td class="mono num" title="opaque int64; rendered as hex of the uint64 bit pattern">{{opaqueID .Peer.GoogleFrontendID}}</td>
<td class="mono num" title="opaque int64; rendered as hex of the uint64 bit pattern">{{opaqueID .Peer.ApplicationFrontendID}}</td>
<td class="num">{{if ge .ChannelIndex 0}}<a href="../../channelz/#channel-session-{{.ChannelIndex}}" title="jump to this channel in channelz">{{.ChannelIndex}}</a>{{else}}—{{end}}</td>
<td class="num">{{.OkRpcs}}</td>
<td class="num">{{.ErrorRpcs}}</td>
<td class="num">{{.Retries}}</td>
<td class="num">{{msgCell .MsgsSent .MsgsSentByType}}</td>
<td class="num">{{msgCell .MsgsRecv .MsgsRecvByType}}</td>
<td class="num">{{.ActiveRpcs}}</td>
<td class="num">{{.Handle.Outstanding}}</td>
<td class="num">{{.Handle.Picks}}</td>
<td class="num">{{dur .LatencyP50}}</td>
<td class="num">{{dur .LatencyP95}}</td>
<td class="num">{{dur .LatencyP99}}</td>
<td>{{age .Handle.LastActivity}} ago</td>
<td>{{age .LastStateChange}} ago</td>
<td>{{untilNow .NextHeartbeat}}</td>
</tr>
{{end}}
</tbody>
</table>
{{end}}

{{if .Pool.LifetimeHistogram}}
<h3 style="font-size:1em;margin:1.4em 0 .4em 0;color:#444">Session lifetimes (n={{.Pool.LifetimeN}}) · p50 {{dur .Pool.LifetimeP50}} · p95 {{dur .Pool.LifetimeP95}} · p99 {{dur .Pool.LifetimeP99}}</h3>
<table>
<thead><tr><th>Bucket</th><th class="num">Count</th><th>Distribution</th></tr></thead>
<tbody>
{{$max := bucketMax .Pool.LifetimeHistogram}}
{{range .Pool.LifetimeHistogram}}
<tr>
<td class="mono">{{.Label}}</td>
<td class="num">{{.Count}}</td>
<td><div style="display:inline-block;background:#1a5fb4;height:.9em;width:{{barWidth .Count $max}}%;min-width:1px"></div></td>
</tr>
{{end}}
</tbody>
</table>
<div style="font-size:.78em;color:#888;margin-top:.3em">
Captures the lifetime (admission → retirement) of the most recent {{.Pool.LifetimeN}} closed sessions.
A spike in the &lt;1m bucket indicates churn (sessions dying young — usually GoAway / Heartbeat / Error).
</div>
{{end}}

{{if .Pool.SlowVRpcs}}
<h3 style="font-size:1em;margin:1.4em 0 .4em 0;color:#444">Slow vRPCs (last {{len .Pool.SlowVRpcs}}, newest first)</h3>
<table>
<thead><tr>
<th>When</th><th>Method</th><th class="num">Latency</th>
<th class="num">SemWait</th><th class="num">Backend</th>
<th>Session</th><th class="num">RpcID</th><th class="num">SessionAge</th>
<th>Status</th>
</tr></thead>
<tbody>
{{range reverseSlow .Pool.SlowVRpcs}}
<tr>
<td>{{age .At}} ago</td>
<td class="mono">{{.Method}}</td>
<td class="num {{latencyClass .Latency}}">{{dur .Latency}}</td>
<td class="num" title="time spent in vrpcSem.Acquire — queue wait for the session's single in-flight slot">{{dur .SemWait}}</td>
<td class="num" title="server-reported BackendLatency">{{dur .BackendLatency}}</td>
<td class="mono">{{.Session}}</td>
<td class="num" title="per-session 1-indexed RPC id; small values indicate a fresh session">{{.RpcIDOnSession}}</td>
<td class="num" title="age of the session at the time of this call">{{dur .SessionAge}}</td>
<td>{{if .Success}}OK{{else}}<span style="color:#922">{{.ErrCode}}</span>{{end}}</td>
</tr>
{{end}}
</tbody>
</table>
<div style="font-size:.78em;color:#888;margin-top:.3em">
<b>SemWait</b> ≈ <b>Latency</b> → call was queued on the session semaphore (head-of-line blocking).
<b>Backend</b> close to <b>Latency</b> → server itself was slow.
Low <b>RpcID</b> + small <b>SessionAge</b> → fresh session warm-up cost.
</div>
{{end}}

{{if .Pool.ScalingHistory}}
<h3 style="font-size:1em;margin:1.4em 0 .4em 0;color:#444">Scaling history (newest last, last {{len .Pool.ScalingHistory}})</h3>
<table>
<thead><tr>
<th>When</th>
<th class="num">Pool&nbsp;was</th>
<th class="num">Sizer&nbsp;asked</th>
<th class="num">Action&nbsp;result</th>
<th>Reason</th>
</tr></thead>
<tbody>
{{range .Pool.ScalingHistory}}
<tr>
<td>{{age .At}} ago</td>
<td class="num">{{.Before}}</td>
<td class="num">{{signed .Requested}}</td>
<td class="num">{{scalingOutcome .}}</td>
<td>{{.Reason}}</td>
</tr>
{{end}}
</tbody>
</table>
<div style="font-size:.78em;color:#888;margin-top:.3em">
<b>Pool&nbsp;was</b> = live pool size when the sizer decided.
<b>Sizer&nbsp;asked</b> = delta requested (+ scale up, − scale down).
<b>Action&nbsp;result</b> = sessions launched (scale up — these are handshaking and become Active shortly after) or sessions pruned (scale down).
</div>
{{end}}

<div class="foot"><a href="?format=json">JSON</a> · <a href="../">all pools</a></div>
</body></html>
`

var (
	indexTpl = template.Must(template.New("index").Funcs(funcs).Parse(indexTplSrc))
	poolTpl  = template.Must(template.New("pool").Funcs(funcs).Parse(poolTplSrc))
)
