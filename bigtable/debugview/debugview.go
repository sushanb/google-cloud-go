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

// Package debugview mounts a small set of HTML/JSON debug pages that
// surface Bigtable session-pool instrumentation added under the
// instrument/session-pool-checkout-timings work. Today it exposes one
// page — /timings — showing per-segment CheckoutSession + Invoke
// timings plus per-method dispatch timings. More pages can slot in via
// mux registration inside Handler.
//
// Typical mount:
//
//	mux.Handle("/debug/", http.StripPrefix("/debug",
//	    debugview.Handler(client))) // client is *bigtable.Client
//
// Provider is intentionally narrow so callers can adapt any type that
// exposes the two accessors. Both *bigtable.Client (via its
// LoadBalancingSnapshots + DispatchTimings methods) and the internal
// session.Client satisfy it directly — no explicit adapter needed for
// the common case.
package debugview

import (
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
	"time"

	session "cloud.google.com/go/bigtable/internal/session"
	btransport "cloud.google.com/go/bigtable/internal/transport"
)

// Provider is the narrow surface debugview needs. Any type that vends
// LoadBalancingSnapshots + DispatchTimings satisfies it. Nil is a
// legal argument to Handler — pages render "not enabled" instead of
// panicking so a client built with EnableDebug=false still mounts
// cleanly.
type Provider interface {
	// LoadBalancingSnapshots returns one entry per session pool. Each
	// entry carries the per-segment CheckoutSession + Invoke timing
	// histograms via .Timings.
	LoadBalancingSnapshots() []btransport.LoadBalancingSnapshot
	// DispatchTimings returns per-method dispatch-level latencies.
	DispatchTimings() []session.DispatchMethodTimings
}

// clientAdapter lifts session.Client (which exposes SessionDebug() +
// DispatchTimings()) into the flat Provider interface debugview needs.
type clientAdapter struct{ c session.Client }

func (a clientAdapter) LoadBalancingSnapshots() []btransport.LoadBalancingSnapshot {
	if a.c == nil {
		return nil
	}
	sd := a.c.SessionDebug()
	if sd == nil {
		return nil
	}
	return sd.LoadBalancingSnapshots()
}

func (a clientAdapter) DispatchTimings() []session.DispatchMethodTimings {
	if a.c == nil {
		return nil
	}
	return a.c.DispatchTimings()
}

// FromSessionClient adapts a session.Client into a debugview.Provider
// so callers with the concrete client type don't have to write the
// two-method wrapper themselves. Nil-safe.
func FromSessionClient(c session.Client) Provider { return clientAdapter{c: c} }

// Handler returns an http.Handler serving the debug pages under the
// mount point the caller strips. p may be nil — pages then render a
// friendly "debug not enabled" panel instead of crashing.
//
// Routes:
//
//	/           → index of available pages
//	/timings/   → timings dashboard (HTML)
//	/timings/?format=json → same data as JSON
func Handler(p Provider) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" && r.URL.Path != "" {
			http.NotFound(w, r)
			return
		}
		writeIndex(w)
	})
	mux.HandleFunc("/timings/", func(w http.ResponseWriter, r *http.Request) {
		serveTimings(w, r, p)
	})
	mux.HandleFunc("/timings", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, r.URL.Path+"/", http.StatusMovedPermanently)
	})
	return mux
}

// indexEntry is one link on the debugview root page.
type indexEntry struct {
	Path string
	Desc string
}

var indexPages = []indexEntry{
	{"timings/", "Session-pool checkout + dispatch latency dashboards"},
}

var indexTmpl = template.Must(template.New("index").Parse(`<!doctype html>
<html><head><meta charset="utf-8"><title>bigtable debug</title>
<style>body{font-family:system-ui,sans-serif;max-width:720px;margin:2em auto;padding:0 1em}
h1{font-size:1.25em}ul{list-style:none;padding:0}li{margin:.5em 0}
a{color:#1a73e8;text-decoration:none}a:hover{text-decoration:underline}
code{background:#f4f4f4;padding:.1em .4em;border-radius:3px}</style></head>
<body><h1>bigtable/debugview</h1>
<ul>{{range .}}<li><a href="{{.Path}}"><code>{{.Path}}</code></a> — {{.Desc}}</li>{{end}}</ul>
</body></html>`))

func writeIndex(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_ = indexTmpl.Execute(w, indexPages)
}

// serveTimings renders the timings dashboard. HTML by default, JSON
// when the caller passes ?format=json.
func serveTimings(w http.ResponseWriter, r *http.Request, p Provider) {
	data := buildTimingsData(p)
	if r.URL.Query().Get("format") == "json" {
		writeJSON(w, data)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := timingsTmpl.Execute(w, data); err != nil {
		http.Error(w, fmt.Sprintf("template: %v", err), http.StatusInternalServerError)
	}
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

// timingsData is what the template renders. Marshals cleanly to JSON
// for the ?format=json path.
type timingsData struct {
	At           time.Time
	Enabled      bool
	Summary      []summaryLine
	Pools        []poolTimingRow
	Dispatch     []session.DispatchMethodTimings
	RefreshEvery time.Duration
}

type poolTimingRow struct {
	Name       string
	PickerName string
	Segments   []btransport.TimingSegment
	Counts     btransport.PathCounts
	CapturedAt time.Time
}

// summaryLine is one row in the "at a glance" panel above the raw
// tables. Value is pre-formatted for HTML; Hint appears as a small
// gray annotation. The panel is skipped entirely when no line has
// enough sample volume to render (Enabled=false or all-zero pools).
type summaryLine struct {
	Label string
	Value string
	Hint  string
}

func buildTimingsData(p Provider) timingsData {
	data := timingsData{
		At:           time.Now(),
		Enabled:      p != nil,
		RefreshEvery: 5 * time.Second,
	}
	if p == nil {
		return data
	}
	for _, lb := range p.LoadBalancingSnapshots() {
		data.Pools = append(data.Pools, poolTimingRow{
			Name:       lb.PoolName,
			PickerName: lb.PickerName,
			Segments:   lb.Timings.Segments,
			Counts:     lb.Timings.Counts,
			CapturedAt: lb.CapturedAt,
		})
	}
	data.Dispatch = p.DispatchTimings()
	data.Summary = buildSummary(data.Pools, data.Dispatch)
	return data
}

// segByName pulls a segment out of a pool's Segments slice; nil when
// missing. Keeps buildSummary readable — it's iterating a small
// fixed-shape slice per pool, so linear scan is fine.
func segByName(segs []btransport.TimingSegment, name string) *btransport.TimingSegment {
	for i := range segs {
		if segs[i].Name == name {
			return &segs[i]
		}
	}
	return nil
}

// buildSummary distills the raw histograms into the handful of
// operator-facing facts that the ranked recommendation calls out:
// where does the wall-clock go, how healthy is the checkout path,
// are retries firing. Emits at most one row per fact per pool /
// per method; skips a fact when the underlying histogram has no
// samples yet so the panel doesn't fill with "—" during warmup.
func buildSummary(pools []poolTimingRow, disp []session.DispatchMethodTimings) []summaryLine {
	var out []summaryLine
	for _, pool := range pools {
		invoke := segByName(pool.Segments, "session_invoke_total")
		await := segByName(pool.Segments, "session_await_result")
		chanRecv := segByName(pool.Segments, "session_await_chan_recv")
		decode := segByName(pool.Segments, "session_await_decode")

		// Where does session_invoke_total go? Prefer chan_recv over
		// await_result — chan_recv isolates the pure server round-trip.
		if invoke != nil && chanRecv != nil && invoke.P50 > 0 {
			pct := 100.0 * float64(chanRecv.P50) / float64(invoke.P50)
			out = append(out, summaryLine{
				Label: fmt.Sprintf("pool %s · server round-trip share", pool.Name),
				Value: fmt.Sprintf("%.1f%%", pct),
				Hint:  fmt.Sprintf("chan_recv p50 %s / invoke_total p50 %s", shortDur(chanRecv.P50), shortDur(invoke.P50)),
			})
		} else if invoke != nil && await != nil && invoke.P50 > 0 {
			pct := 100.0 * float64(await.P50) / float64(invoke.P50)
			out = append(out, summaryLine{
				Label: fmt.Sprintf("pool %s · await share", pool.Name),
				Value: fmt.Sprintf("%.1f%%", pct),
				Hint:  fmt.Sprintf("await p50 %s / invoke_total p50 %s", shortDur(await.P50), shortDur(invoke.P50)),
			})
		}
		if decode != nil && decode.N > 0 {
			out = append(out, summaryLine{
				Label: fmt.Sprintf("pool %s · decode p99", pool.Name),
				Value: shortDur(decode.P99),
				Hint:  "large p99 = big response payload or CPU-bound handler",
			})
		}

		// Checkout health — fast vs slow path share.
		total := pool.Counts.FastPathHits + pool.Counts.SlowPathHits
		if total > 0 {
			slowPct := 100.0 * float64(pool.Counts.SlowPathHits) / float64(total)
			out = append(out, summaryLine{
				Label: fmt.Sprintf("pool %s · slow-path share", pool.Name),
				Value: fmt.Sprintf("%.2f%%", slowPct),
				Hint:  fmt.Sprintf("slow=%d fast=%d", pool.Counts.SlowPathHits, pool.Counts.FastPathHits),
			})
		}
		if pool.Counts.FastPathHits > 0 && pool.Counts.PickLostRace > 0 {
			racePct := 100.0 * float64(pool.Counts.PickLostRace) / float64(pool.Counts.FastPathHits)
			out = append(out, summaryLine{
				Label: fmt.Sprintf("pool %s · pick-lost-race share", pool.Name),
				Value: fmt.Sprintf("%.3f%%", racePct),
				Hint:  fmt.Sprintf("lost=%d fast=%d", pool.Counts.PickLostRace, pool.Counts.FastPathHits),
			})
		}

		// Slow-wait latency (only rendered when the slow path fired).
		if sw := segByName(pool.Segments, "checkout_slow_wait"); sw != nil && sw.N > 0 {
			out = append(out, summaryLine{
				Label: fmt.Sprintf("pool %s · slow-wait p99", pool.Name),
				Value: shortDur(sw.P99),
				Hint:  fmt.Sprintf("n=%d — the tail cost of pool saturation", sw.N),
			})
		}
	}
	// Dispatch: avg attempts per call. >1.02 flags retries firing.
	for _, m := range disp {
		if m.Calls == 0 {
			continue
		}
		avg := float64(m.Attempts) / float64(m.Calls)
		out = append(out, summaryLine{
			Label: fmt.Sprintf("dispatch %s · avg attempts/call", m.Method),
			Value: fmt.Sprintf("%.3f", avg),
			Hint:  fmt.Sprintf("attempts=%d calls=%d — >1.02 means retries are firing", m.Attempts, m.Calls),
		})
	}
	return out
}

// shortDur formats a Duration compactly for the dashboard cells.
// time.Duration.String() renders "1.234567ms" which crowds the table;
// truncating to microseconds keeps the columns aligned without losing
// resolution operators actually care about.
func shortDur(d time.Duration) string {
	if d == 0 {
		return "—"
	}
	switch {
	case d < time.Microsecond:
		return fmt.Sprintf("%dns", d.Nanoseconds())
	case d < time.Millisecond:
		return fmt.Sprintf("%.1fµs", float64(d.Microseconds()))
	case d < time.Second:
		return fmt.Sprintf("%.2fms", float64(d.Nanoseconds())/1e6)
	default:
		return fmt.Sprintf("%.3fs", d.Seconds())
	}
}

var timingsTmpl = template.Must(template.New("timings").Funcs(template.FuncMap{
	"dur": shortDur,
}).Parse(timingsHTML))

// timingsHTML is the dashboard template. Two tables per pool
// (checkout segments + path counters) plus one dispatch table.
const timingsHTML = `<!doctype html>
<html><head><meta charset="utf-8">
<title>bigtable timings</title>
<meta http-equiv="refresh" content="{{.RefreshEvery.Seconds}}">
<style>
body{font-family:system-ui,sans-serif;margin:1.5em;color:#222}
h1{font-size:1.25em;margin:.2em 0}
h2{font-size:1em;margin:1.2em 0 .3em}
h3{font-size:.9em;margin:.8em 0 .3em;color:#555}
table{border-collapse:collapse;font-size:.85em;margin-bottom:.5em}
th,td{border:1px solid #ddd;padding:3px 8px;text-align:right}
th{background:#f4f4f4;text-align:left}
td:first-child,th:first-child{text-align:left}
.meta{color:#666;font-size:.8em;margin-bottom:1em}
.counts td{background:#fafafa}
.summary td b{color:#1a73e8}
.warn{color:#b06000}
a.jsonlink{color:#1a73e8;text-decoration:none;font-size:.8em}
a.jsonlink:hover{text-decoration:underline}
.zero td{color:#aaa}
</style></head>
<body>
<h1>bigtable timings dashboard</h1>
<div class="meta">rendered {{.At.Format "2006-01-02 15:04:05.000 MST"}}
· auto-refresh {{.RefreshEvery}}
· <a class="jsonlink" href="?format=json">JSON</a></div>

{{if not .Enabled}}
<p class="warn">Debug provider is nil — either the client was constructed with
<code>EnableDebug=false</code>, or no Provider was passed to
<code>debugview.Handler</code>. No numbers to show.</p>
{{else if not .Pools}}
<p class="warn">No session pools yet — either none have opened, or all counters
are still zero because no traffic has flowed. Kick a ReadRow / MutateRow and
refresh.</p>
{{end}}

{{if .Summary}}
<h2>at a glance</h2>
<table class="summary">
<tr><th>signal</th><th>value</th><th>hint</th></tr>
{{range .Summary}}
<tr><td>{{.Label}}</td><td><b>{{.Value}}</b></td><td class="meta">{{.Hint}}</td></tr>
{{end}}
</table>
{{end}}

{{range .Pools}}
<h2>pool <code>{{.Name}}</code> <span class="meta">picker={{.PickerName}} · captured {{.CapturedAt.Format "15:04:05.000"}}</span></h2>
<h3>latency segments</h3>
<table>
<tr><th>segment</th><th>n</th><th>p50</th><th>p95</th><th>p99</th></tr>
{{range .Segments}}
<tr {{if eq .N 0}}class="zero"{{end}}><td>{{.Name}}</td><td>{{.N}}</td><td>{{dur .P50}}</td><td>{{dur .P95}}</td><td>{{dur .P99}}</td></tr>
{{end}}
</table>
<h3>path counters</h3>
<table class="counts">
<tr><th>fast_path_hits</th><th>slow_path_hits</th><th>pick_lost_race</th><th>empty_kicks</th><th>refill_kicks</th></tr>
<tr><td>{{.Counts.FastPathHits}}</td><td>{{.Counts.SlowPathHits}}</td><td>{{.Counts.PickLostRace}}</td><td>{{.Counts.EmptyKicks}}</td><td>{{.Counts.RefillKicks}}</td></tr>
</table>
{{end}}

{{if .Dispatch}}
<h2>dispatch timings (per method)</h2>
<table>
<tr><th>method</th><th>calls</th><th>attempts</th><th>pool_get_miss</th>
<th>total p50</th><th>total p95</th><th>total p99</th>
<th>pool_get p50</th><th>pool_get p95</th><th>pool_get p99</th>
<th>chained p50</th><th>chained p95</th><th>chained p99</th></tr>
{{range .Dispatch}}
<tr><td>{{.Method}}</td><td>{{.Calls}}</td><td>{{.Attempts}}</td><td>{{.PoolGetMiss}}</td>
<td>{{dur .TotalP50}}</td><td>{{dur .TotalP95}}</td><td>{{dur .TotalP99}}</td>
<td>{{dur .PoolGetP50}}</td><td>{{dur .PoolGetP95}}</td><td>{{dur .PoolGetP99}}</td>
<td>{{dur .ChainedP50}}</td><td>{{dur .ChainedP95}}</td><td>{{dur .ChainedP99}}</td></tr>
{{end}}
</table>
{{end}}

</body></html>`
