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

// Package loadz renders the picker's decision reasoning for every
// session pool a Bigtable client owns. It answers "why did the picker
// choose this AFE?" by surfacing:
//
//   - the current picker + algorithm gloss
//   - the actual per-AFE traffic share vs. uniform ideal (with skew)
//   - a ring buffer of the last N pick decisions (candidates + cost + winner)
//   - per-AFE latency signals the LeastLatencyPicker consumes
//
// Complements the other debug views: afez shows per-AFE state, sessionz
// shows per-session lifecycle, flightz shows in-flight vRPCs. loadz is
// the *why* view — the narrative layer on top.
//
// Mount into any http.ServeMux:
//
//	http.Handle("/debug/loadz/", http.StripPrefix("/debug/loadz",
//	    loadz.Handler(c)))
//
// The page auto-refreshes every 3s. JSON at ?format=json.
package loadz

import (
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
	"sort"
	"time"

	"cloud.google.com/go/bigtable"
	btransport "cloud.google.com/go/bigtable/internal/transport"
)

// Handler returns an http.Handler rendering the loadz view for c's
// session pools. Handler(nil) or a client without EnableSessionPool
// yields a handler that serves an empty page.
func Handler(c *bigtable.Client) http.Handler {
	return HandlerFromProvider(providerForClient(c))
}

// HandlerFromProvider is Handler for an arbitrary SessionDebugProvider.
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

// PoolView is one pool block on the loadz page. All fields JSON-tagged
// so ?format=json returns a stable machine-readable shape.
type PoolView struct {
	PoolName      string     `json:"pool"`
	PickerName    string     `json:"picker"`
	PickerGloss   string     `json:"pickerGloss"`
	TotalPicks    int64      `json:"totalPicks"`
	AFEs          []AfeRow   `json:"afes"`
	RecentPicks   []PickRow  `json:"recentPicks"`
	CapturedAt    time.Time  `json:"capturedAt"`
}

// AfeRow is one row in the per-AFE fanout table — actual traffic share
// side-by-side with the uniform-ideal share and the skew.
type AfeRow struct {
	AfeID          int64         `json:"afeId"`
	AfeIDHex       string        `json:"afeIdHex"`
	Idle           int           `json:"idle"`
	InUse          int           `json:"inUse"`
	RefCount       int           `json:"refCount"`
	Picks          int64         `json:"picks"`
	ActualSharePct float64       `json:"actualSharePct"`
	IdealSharePct  float64       `json:"idealSharePct"`
	SkewPP         float64       `json:"skewPP"`
	TransportEwma  time.Duration `json:"transportEwmaNanos"`
	E2eEwma        time.Duration `json:"e2eEwmaNanos"`
	// LastConnected is when the AFE last saw a fresh session become
	// Ready — useful for spotting stale buckets.
	LastConnected time.Time `json:"lastConnected"`
}

// PickRow is one row in the recent-picks table. Candidates lists what
// the picker sampled (K-choice draw); Winner is the chosen AFE.
type PickRow struct {
	At         time.Time    `json:"at"`
	PickerName string       `json:"picker"`
	Winner     int64        `json:"winner"`
	Reason     string       `json:"reason"`
	Candidates []PickCandRow `json:"candidates"`
}

// PickCandRow is one candidate from a K-choice draw. Cost's units
// depend on the picker: NumOutstanding (int-as-float) for
// least-inflight, e2e PeakEwma nanos for least-latency.
type PickCandRow struct {
	AfeID    int64   `json:"afeId"`
	AfeIDHex string  `json:"afeIdHex"`
	Cost     float64 `json:"cost"`
}

type page struct {
	Generated time.Time
	Pools     []PoolView
}

func (s *server) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" && r.URL.Path != "" {
		http.NotFound(w, r)
		return
	}
	now := time.Now()
	views := s.collect(now)

	if r.URL.Query().Get("format") == "json" {
		writeJSON(w, struct {
			CapturedAt time.Time  `json:"capturedAt"`
			Pools      []PoolView `json:"pools"`
		}{now, views})
		return
	}

	writeHTML(w, tpl, page{Generated: now, Pools: views})
}

// collect turns each per-pool LoadBalancingSnapshot into a PoolView
// enriched with derived fields (actual-share, ideal-share, skew).
func (s *server) collect(now time.Time) []PoolView {
	if s.provider == nil {
		return nil
	}
	snaps := s.provider.LoadBalancingSnapshots()
	views := make([]PoolView, 0, len(snaps))
	for _, snap := range snaps {
		views = append(views, buildPoolView(snap, now))
	}
	return views
}

func buildPoolView(snap btransport.LoadBalancingSnapshot, now time.Time) PoolView {
	// Total picks = sum of per-AFE counts. Use it to derive actual
	// share.
	var total int64
	for _, n := range snap.PickCounts {
		total += n
	}
	idealShare := 0.0
	if len(snap.AFEs) > 0 {
		idealShare = 100.0 / float64(len(snap.AFEs))
	}

	// Merge AFE rows with per-AFE pick counts so callers see one table.
	afeRows := make([]AfeRow, 0, len(snap.AFEs))
	for _, a := range snap.AFEs {
		picks := snap.PickCounts[a.ID]
		actual := 0.0
		if total > 0 {
			actual = 100.0 * float64(picks) / float64(total)
		}
		inUse := a.RefCount - a.IdleCount
		if inUse < 0 {
			inUse = 0
		}
		afeRows = append(afeRows, AfeRow{
			AfeID:          a.ID,
			AfeIDHex:       fmt.Sprintf("%x", uint64(a.ID)),
			Idle:           a.IdleCount,
			InUse:          inUse,
			RefCount:       a.RefCount,
			Picks:          picks,
			ActualSharePct: actual,
			IdealSharePct:  idealShare,
			SkewPP:         actual - idealShare,
			TransportEwma:  a.TransportEwma,
			E2eEwma:        a.E2eEwma,
			LastConnected:  a.LastConnected,
		})
	}
	sort.Slice(afeRows, func(i, j int) bool { return afeRows[i].AfeID < afeRows[j].AfeID })

	// Recent picks are stored oldest-first in the ring buffer; render
	// newest-first so the top of the table is the freshest.
	pickRows := make([]PickRow, 0, len(snap.Recent))
	for i := len(snap.Recent) - 1; i >= 0; i-- {
		ev := snap.Recent[i]
		cands := make([]PickCandRow, 0, len(ev.Decision.Candidates))
		for _, c := range ev.Decision.Candidates {
			cands = append(cands, PickCandRow{
				AfeID:    int64(c.AfeID),
				AfeIDHex: fmt.Sprintf("%x", uint64(c.AfeID)),
				Cost:     c.Cost,
			})
		}
		pickRows = append(pickRows, PickRow{
			At:         ev.At,
			PickerName: ev.PickerName,
			Winner:     int64(ev.Decision.Winner),
			Reason:     ev.Decision.Reason,
			Candidates: cands,
		})
	}

	return PoolView{
		PoolName:    snap.PoolName,
		PickerName:  snap.PickerName,
		PickerGloss: glossFor(snap.PickerName),
		TotalPicks:  total,
		AFEs:        afeRows,
		RecentPicks: pickRows,
		CapturedAt:  snap.CapturedAt,
	}
}

// glossFor maps a picker's Name() to a plain-English one-liner
// operators can read without knowing the picker's internals.
func glossFor(name string) string {
	switch name {
	case "simple":
		return "Uniform random over ready AFEs."
	case "least-inflight":
		return "K-choice-2 random draw; wins the AFE with fewer in-flight vRPCs."
	case "least-latency":
		return "K-choice-2 random draw; wins the AFE with lower per-AFE e2e PeakEwma (OK-gated)."
	default:
		return ""
	}
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

// roundDuration keeps latency columns terse — matches the flightz /
// afez rounding rules for consistent debug UX.
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

var funcs = template.FuncMap{
	"dur": func(d time.Duration) string {
		if d == 0 {
			return "—"
		}
		return roundDuration(d).String()
	},
	"ago": func(t time.Time) string {
		if t.IsZero() {
			return "—"
		}
		return roundDuration(time.Since(t)).String() + " ago"
	},
	"timeHM": func(t time.Time) string {
		return t.Format("15:04:05.000")
	},
	"pct": func(v float64) string {
		return fmt.Sprintf("%.1f%%", v)
	},
	"ppSigned": func(v float64) string {
		if v > 0 {
			return fmt.Sprintf("+%.1fpp", v)
		}
		return fmt.Sprintf("%.1fpp", v)
	},
	"skewClass": func(v float64) string {
		abs := v
		if abs < 0 {
			abs = -abs
		}
		switch {
		case abs < 5:
			return "skew-ok"
		case abs < 15:
			return "skew-warn"
		default:
			return "skew-hot"
		}
	},
	// bar renders an inline ASCII share bar 0..100% at fixed width 12.
	"bar": func(v float64) string {
		if v < 0 {
			v = 0
		}
		if v > 100 {
			v = 100
		}
		width := 12
		fill := int(v / 100.0 * float64(width))
		out := make([]byte, width)
		for i := 0; i < width; i++ {
			if i < fill {
				out[i] = '#'
			} else {
				out[i] = '.'
			}
		}
		return string(out)
	},
	"cost": func(v float64) string {
		if v == 0 {
			return "0"
		}
		if v >= 1 {
			return fmt.Sprintf("%.1f", v)
		}
		return fmt.Sprintf("%.3f", v)
	},
	"hex": func(v int64) string {
		if v == 0 {
			return "unknown"
		}
		return fmt.Sprintf("0x%x", uint64(v))
	},
}

var tpl = template.Must(template.New("loadz").Funcs(funcs).Parse(tplSrc))

const tplSrc = `<!doctype html>
<html lang="en"><head>
<meta charset="utf-8">
<meta http-equiv="refresh" content="3">
<title>loadz</title>
<style>
body { font-family: -apple-system,Segoe UI,Roboto,sans-serif; margin: 1rem; color: #222; }
h1 { font-size: 1.15rem; margin: 0 0 .5rem; }
h2 { font-size: 1rem; margin: 1.2rem 0 .2rem; color: #333; }
.gloss { color: #666; font-size: .85rem; margin: 0 0 .4rem; font-style: italic; }
table { border-collapse: collapse; width: 100%; font-size: .82rem; margin-bottom: .3rem; }
th, td { padding: .28rem .5rem; text-align: right; border-bottom: 1px solid #eee; font-variant-numeric: tabular-nums; }
th:first-child, td:first-child { text-align: left; }
th { background: #f6f6f6; font-weight: 600; }
.bar { font-family: monospace; letter-spacing: -1px; color: #4a5; }
.skew-ok { color: #4a5; }
.skew-warn { color: #c80; font-weight: 600; }
.skew-hot { color: #c33; font-weight: 700; }
.empty { color: #888; margin: 1rem 0; }
.gen { color: #888; font-size: .78rem; margin-top: 1rem; }
.reason { color: #555; font-size: .78rem; }
.picker { color: #555; font-family: monospace; }
.total { color: #666; font-size: .82rem; }
details { margin: .1rem 0; }
summary { cursor: pointer; padding: .1rem 0; }
.cands { color: #666; font-size: .78rem; margin: .1rem 0 .3rem 1.5rem; }
</style>
</head><body>
<h1>Bigtable load balancing — {{len .Pools}} pool(s)</h1>
{{if .Pools}}
{{range .Pools}}
<h2>{{.PoolName}} — <span class="picker">{{.PickerName}}</span> · <span class="total">total picks: {{.TotalPicks}}</span></h2>
<p class="gloss">{{.PickerGloss}}</p>

<table>
  <thead><tr>
    <th>AFE id</th><th>idle</th><th>in-use</th><th>ref</th><th>picks</th>
    <th>actual</th><th></th><th>ideal</th><th>skew</th>
    <th>transport EWMA</th><th>e2e EWMA</th><th>last conn</th>
  </tr></thead>
  <tbody>
  {{range .AFEs}}
  <tr>
    <td>{{hex .AfeID}}</td>
    <td>{{.Idle}}</td>
    <td>{{.InUse}}</td>
    <td>{{.RefCount}}</td>
    <td>{{.Picks}}</td>
    <td>{{pct .ActualSharePct}}</td>
    <td><span class="bar">{{bar .ActualSharePct}}</span></td>
    <td>{{pct .IdealSharePct}}</td>
    <td class="{{skewClass .SkewPP}}">{{ppSigned .SkewPP}}</td>
    <td>{{dur .TransportEwma}}</td>
    <td>{{dur .E2eEwma}}</td>
    <td>{{ago .LastConnected}}</td>
  </tr>
  {{end}}
  </tbody>
</table>

<h2 style="margin-top:.9rem">Recent picks — last {{len .RecentPicks}}</h2>
{{if .RecentPicks}}
<table>
  <thead><tr>
    <th>time</th><th>picker</th><th>winner</th><th>reason</th><th>candidates</th>
  </tr></thead>
  <tbody>
  {{range .RecentPicks}}
  <tr>
    <td>{{timeHM .At}}</td>
    <td class="picker">{{.PickerName}}</td>
    <td>{{hex .Winner}}</td>
    <td class="reason">{{.Reason}}</td>
    <td>
      {{range .Candidates}}
        <code>{{hex .AfeID}}</code>(<span class="reason">{{cost .Cost}}</span>) &nbsp;
      {{end}}
    </td>
  </tr>
  {{end}}
  </tbody>
</table>
{{else}}
<p class="empty">No pick decisions yet.</p>
{{end}}

{{end}}
{{else}}
<p class="empty">No session pools recorded yet.</p>
{{end}}
<p class="gen">Generated {{.Generated.Format "15:04:05.000 MST"}} — auto-refresh 3s.</p>
</body></html>`
