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
//	GET /              → HTML table, interesting conns first, 2-second refresh.
//	GET /?format=json  → JSON array of every registered conn's TCP_INFO.
//	GET /?only=hot     → HTML, hides healthy rows (only warn+crit).
//	GET /?all=1        → HTML, includes remote :443 conns (default hides).
//	GET /?sort=<key>&dir=<asc|desc>
//	                   → HTML, sorted by any column (click a header to set).
//	                     Special keys: sev (default, interesting-first) and
//	                     dial (oldest-first). All other keys are per-column
//	                     — e.g. sort=rtt&dir=desc for slowest wire first.
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
	"net/url"
	"sort"
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

// severity ranks conns by how much the kernel's TCP_INFO says is wrong
// with them. Ordering is exploited by the sort (higher first) and by the
// row-color CSS classes below.
type severity int

const (
	sevOK   severity = iota // healthy, no signal
	sevNote                 // interesting but not a problem (draining, unreadable)
	sevWarn                 // any real loss/retrans/ECN/reord signal
	sevCrit                 // RTO-driven Loss state or currently backing off
)

// classify inspects one TCP_INFO snapshot and returns its severity plus a
// short "why" list of the specific signals that triggered it. The why list
// drives the tooltip on the colored row so a user can see at a glance
// *what* made this row interesting (e.g. "dsack+ecn") without scanning
// 25 numeric cells. Empty list ↔ sevOK.
//
// Rules (highest wins):
//   - crit: CAState=Loss OR Backoff>0 — RTO-driven loss or currently timing out
//   - warn: CAState ∈ {Disorder, CWR, Recovery} OR any non-zero retrans /
//     lost / dsack / ECN / reordering counter
//   - note: State != ESTABLISHED (closing/draining) OR Err populated
//     (couldn't read info — usually non-Linux)
//   - ok:   everything else
func classify(r btransport.TCPInfoSnapshot) (severity, []string) {
	if r.Err != "" {
		return sevNote, []string{"unreadable"}
	}
	var why []string
	sev := sevOK
	bump := func(s severity, tag string) {
		if s > sev {
			sev = s
		}
		why = append(why, tag)
	}

	if r.CAState == "Loss" {
		bump(sevCrit, "Loss")
	}
	if r.Backoff > 0 {
		bump(sevCrit, "backoff")
	}
	switch r.CAState {
	case "Recovery":
		bump(sevWarn, "Recovery")
	case "CWR":
		bump(sevWarn, "CWR")
	case "Disorder":
		bump(sevWarn, "Disorder")
	}
	if r.Retransmits > 0 {
		bump(sevWarn, "retrans")
	}
	if r.TotalRetrans > 0 && r.Retransmits == 0 {
		// only mention historical retrans if there isn't a current burst
		bump(sevWarn, "past-retrans")
	}
	if r.Lost > 0 {
		bump(sevWarn, "lost")
	}
	if r.DsackDups > 0 {
		bump(sevWarn, "dsack")
	}
	if r.DeliveredCE > 0 {
		bump(sevWarn, "ECN")
	}
	if r.ReordSeen > 0 {
		bump(sevWarn, "reord")
	}

	if sev == sevOK && r.State != "" && r.State != "ESTABLISHED" {
		bump(sevNote, r.State)
	}
	return sev, why
}

// rowClass returns the CSS class for the row background.
func (s severity) rowClass() string {
	switch s {
	case sevCrit:
		return "row-crit"
	case sevWarn:
		return "row-warn"
	case sevNote:
		return "row-note"
	}
	return "row-ok"
}

// row bundles a snapshot with its precomputed severity so the template
// doesn't have to re-classify on every template action.
type row struct {
	btransport.TCPInfoSnapshot
	Sev      string // rowClass string ("row-crit", …)
	Why      string // joined why list, e.g. "Loss+backoff+lost"
	Interest bool   // sev > sevOK — the "N interesting" count uses this
}

// colDef describes one column of the tcpz table: header label, tooltip,
// CSS class for both th/td, and (if sortable) a comparator returning
// <0/0/>0 in the natural ascending sense. The "Body" field is the exact
// template snippet used inside the row's <td> — kept alongside the header
// so adding a column is a single-slice-entry change.
//
// Comparators receive raw snapshots (not row-wrapped) so callers can share
// them with alternative renderers.
type colDef struct {
	Key   string // URL sort key; empty = not sortable
	Label string
	Class string // "num" / "mono" / "err" / ""
	Title string
	Body  string // inner-cell template action, e.g. `{{num .MSS}}`
	// Cmp orders two snapshots ascending on this column. Nil for the
	// "Why" and "Err" columns where clicking a header wouldn't buy
	// anything (Why is derived from severity, Err is almost always
	// empty on Linux).
	Cmp func(a, b btransport.TCPInfoSnapshot) int
	// Desc = true if the natural first-click direction is descending
	// (biggest / worst first). True for counters and latency; false for
	// alphabetical / oldest-first columns.
	Desc bool
}

// cols is the single source of truth for every column in the tcpz table.
// The template renders <thead> and <tbody> by iterating this slice. Order
// here is the display order. Adding, removing, or reordering columns is a
// one-slice-entry edit — no template surgery required.
var cols = []colDef{
	{Key: "", Label: "Why", Class: "why", Title: "Why this row is highlighted — the list of TCP_INFO signals that classified it.", Body: `{{.Why}}`},
	{Key: "remote", Label: "Remote", Class: "mono", Title: "Peer address (ip:port).", Body: `{{.RemoteAddr}}`, Cmp: cmpStr(func(s btransport.TCPInfoSnapshot) string { return s.RemoteAddr })},
	{Key: "local", Label: "Local", Class: "mono", Title: "Local socket address.", Body: `{{.LocalAddr}}`, Cmp: cmpStr(func(s btransport.TCPInfoSnapshot) string { return s.LocalAddr })},
	{Key: "age", Label: "Age", Title: "Time since this conn was dialed.", Body: `{{ago .DialedAt}}`, Cmp: cmpTime(func(s btransport.TCPInfoSnapshot) time.Time { return s.DialedAt }), Desc: true},
	{Key: "state", Label: "State", Title: "Linux TCP state (ESTABLISHED, CLOSE_WAIT, etc.).", Body: `<span class="state-{{or .State "UNKNOWN"}}">{{or .State "—"}}</span>`, Cmp: cmpStr(func(s btransport.TCPInfoSnapshot) string { return s.State })},
	{Key: "ca", Label: "Ca", Title: "Congestion-control state: Open=healthy, Disorder=watching for loss, CWR=cwnd-reducing after ECN, Recovery=fast-retransmitting, Loss=RTO-driven collapse (worst).", Body: `<span class="ca-{{or .CAState "Unknown"}}">{{or .CAState "—"}}</span>`, Cmp: cmpStr(func(s btransport.TCPInfoSnapshot) string { return s.CAState })},
	{Key: "bkoff", Label: "Bkoff", Class: "num", Title: "RTO backoff count. >0 means we've timed out at least once and are waiting exponentially longer.", Body: `{{critNum .Backoff}}`, Cmp: cmpU32(func(s btransport.TCPInfoSnapshot) uint32 { return s.Backoff }), Desc: true},
	{Key: "rtt", Label: "RTT", Class: "num", Title: "Smoothed round-trip time — the primary 'wire' latency signal.", Body: `{{dur .RTT}}`, Cmp: cmpDur(func(s btransport.TCPInfoSnapshot) time.Duration { return s.RTT }), Desc: true},
	{Key: "rttvar", Label: "RTTVar", Class: "num", Title: "RTT variance (jitter). High values suggest an unstable path.", Body: `{{dur .RTTVar}}`, Cmp: cmpDur(func(s btransport.TCPInfoSnapshot) time.Duration { return s.RTTVar }), Desc: true},
	{Key: "minrtt", Label: "MinRTT", Class: "num", Title: "Minimum RTT observed on this conn — the floor of what the network can deliver.", Body: `{{dur .MinRTT}}`, Cmp: cmpDur(func(s btransport.TCPInfoSnapshot) time.Duration { return s.MinRTT }), Desc: true},
	{Key: "rto", Label: "RTO", Class: "num", Title: "Current retransmit timeout — kernel will re-send unacked bytes after this long. Grows with backoff.", Body: `{{dur .RTO}}`, Cmp: cmpDur(func(s btransport.TCPInfoSnapshot) time.Duration { return s.RTO }), Desc: true},
	{Key: "mss", Label: "MSS", Class: "num", Title: "Send MSS (max segment size).", Body: `{{num .MSS}}`, Cmp: cmpU32(func(s btransport.TCPInfoSnapshot) uint32 { return s.MSS }), Desc: true},
	{Key: "pmtu", Label: "PMTU", Class: "num", Title: "Path MTU (bytes). <1500 = tunneling/VPN in path. Watch for PMTU black holes: silent drops if ICMP frag-needed replies are filtered.", Body: `{{pmtu .PMTU}}`, Cmp: cmpU32(func(s btransport.TCPInfoSnapshot) uint32 { return s.PMTU }), Desc: false},
	{Key: "cwnd", Label: "CWnd", Class: "num", Title: "Send congestion window in MSS units.", Body: `{{num .SndCwnd}}`, Cmp: cmpU32(func(s btransport.TCPInfoSnapshot) uint32 { return s.SndCwnd }), Desc: true},
	{Key: "ssth", Label: "SSTh", Class: "num", Title: "Slow-start threshold in MSS units. When cwnd &lt; ssthresh we're in slow-start (often after a loss event).", Body: `{{num .SndSsthresh}}`, Cmp: cmpU32(func(s btransport.TCPInfoSnapshot) uint32 { return s.SndSsthresh }), Desc: true},
	{Key: "retr", Label: "Retr", Class: "num", Title: "Recent retransmits (kernel counter).", Body: `{{hotNum .Retransmits}}`, Cmp: cmpU32(func(s btransport.TCPInfoSnapshot) uint32 { return s.Retransmits }), Desc: true},
	{Key: "totalretr", Label: "TotalRetr", Class: "num", Title: "Total retransmits since conn open — high count means path is lossy.", Body: `{{hotNum .TotalRetrans}}`, Cmp: cmpU32(func(s btransport.TCPInfoSnapshot) uint32 { return s.TotalRetrans }), Desc: true},
	{Key: "rtxrate", Label: "RtxRate", Class: "num", Title: "Retransmit ratio: bytes retransmitted / bytes sent. The 'actual loss rate' this conn observed.", Body: `{{hotPct .RetransRatioPct}}`, Cmp: cmpF64(func(s btransport.TCPInfoSnapshot) float64 { return s.RetransRatioPct }), Desc: true},
	{Key: "lost", Label: "Lost", Class: "num", Title: "Segments the kernel considers lost right now.", Body: `{{hotNum .Lost}}`, Cmp: cmpU32(func(s btransport.TCPInfoSnapshot) uint32 { return s.Lost }), Desc: true},
	{Key: "sackd", Label: "SACKd", Class: "num", Title: "Segments selectively-ACK'd by the receiver.", Body: `{{num .Sacked}}`, Cmp: cmpU32(func(s btransport.TCPInfoSnapshot) uint32 { return s.Sacked }), Desc: true},
	{Key: "unacked", Label: "Unacked", Class: "num", Title: "Segments sent but not yet ACKed (in flight).", Body: `{{num .Unacked}}`, Cmp: cmpU32(func(s btransport.TCPInfoSnapshot) uint32 { return s.Unacked }), Desc: true},
	{Key: "dsack", Label: "DSACK", Class: "num", Title: "Duplicate-SACK count — number of SPURIOUS retransmits (we resent bytes the receiver actually got). High DSACK relative to TotalRetr = timing false-positives, not real loss.", Body: `{{hotNum .DsackDups}}`, Cmp: cmpU32(func(s btransport.TCPInfoSnapshot) uint32 { return s.DsackDups }), Desc: true},
	{Key: "reord", Label: "ReordS", Class: "num", Title: "Times reordering was observed. Reordering can trigger fast-retransmit even without loss.", Body: `{{hotNum .ReordSeen}}`, Cmp: cmpU32(func(s btransport.TCPInfoSnapshot) uint32 { return s.ReordSeen }), Desc: true},
	{Key: "ecn", Label: "ECN", Class: "num", Title: "Packets delivered with ECN Congestion-Experienced marks. Non-zero = a router is signaling congestion before dropping.", Body: `{{hotNum .DeliveredCE}}`, Cmp: cmpU32(func(s btransport.TCPInfoSnapshot) uint32 { return s.DeliveredCE }), Desc: true},
	{Key: "sent", Label: "Sent", Class: "num", Title: "Total bytes sent (data).", Body: `{{bytes .BytesSent}}`, Cmp: cmpU64(func(s btransport.TCPInfoSnapshot) uint64 { return s.BytesSent }), Desc: true},
	{Key: "retrans", Label: "Retrans", Class: "num", Title: "Total bytes retransmitted (data).", Body: `{{hotBytes .BytesRetrans}}`, Cmp: cmpU64(func(s btransport.TCPInfoSnapshot) uint64 { return s.BytesRetrans }), Desc: true},
	{Key: "delrate", Label: "DelRate", Class: "num", Title: "Recent delivery rate (BBR estimate).", Body: `{{rate .DeliveryRate}}`, Cmp: cmpU64(func(s btransport.TCPInfoSnapshot) uint64 { return s.DeliveryRate }), Desc: true},
	{Key: "notsent", Label: "NotSent", Class: "num", Title: "Bytes buffered but not yet on wire. High = we're app-limited or CPU-limited, not network-limited.", Body: `{{num .NotsentBytes}}`, Cmp: cmpU32(func(s btransport.TCPInfoSnapshot) uint32 { return s.NotsentBytes }), Desc: true},
	{Key: "lastrecv", Label: "LastRecv", Title: "Time since the socket last received data.", Body: `{{dur .LastDataRecv}}`, Cmp: cmpDur(func(s btransport.TCPInfoSnapshot) time.Duration { return s.LastDataRecv }), Desc: true},
	{Key: "lastsent", Label: "LastSent", Title: "Time since the socket last sent data.", Body: `{{dur .LastDataSent}}`, Cmp: cmpDur(func(s btransport.TCPInfoSnapshot) time.Duration { return s.LastDataSent }), Desc: true},
	{Key: "", Label: "Err", Class: "err", Title: "Populated when TCP_INFO couldn't be read on a live fd (e.g. non-Linux OS).", Body: `{{.Err}}`},
}

// colByKey builds a lookup for cols; done once at init so the request
// handler doesn't re-scan the slice per request.
var colByKey = func() map[string]*colDef {
	m := make(map[string]*colDef, len(cols))
	for i := range cols {
		if cols[i].Key != "" {
			m[cols[i].Key] = &cols[i]
		}
	}
	return m
}()

// cmp* factories: thin wrappers that turn "extract field X" into a
// comparator. Keeps the colDef literals dense and readable.
func cmpStr(get func(btransport.TCPInfoSnapshot) string) func(a, b btransport.TCPInfoSnapshot) int {
	return func(a, b btransport.TCPInfoSnapshot) int { return strings.Compare(get(a), get(b)) }
}
func cmpU32(get func(btransport.TCPInfoSnapshot) uint32) func(a, b btransport.TCPInfoSnapshot) int {
	return func(a, b btransport.TCPInfoSnapshot) int {
		x, y := get(a), get(b)
		switch {
		case x < y:
			return -1
		case x > y:
			return 1
		}
		return 0
	}
}
func cmpU64(get func(btransport.TCPInfoSnapshot) uint64) func(a, b btransport.TCPInfoSnapshot) int {
	return func(a, b btransport.TCPInfoSnapshot) int {
		x, y := get(a), get(b)
		switch {
		case x < y:
			return -1
		case x > y:
			return 1
		}
		return 0
	}
}
func cmpF64(get func(btransport.TCPInfoSnapshot) float64) func(a, b btransport.TCPInfoSnapshot) int {
	return func(a, b btransport.TCPInfoSnapshot) int {
		x, y := get(a), get(b)
		switch {
		case x < y:
			return -1
		case x > y:
			return 1
		}
		return 0
	}
}
func cmpDur(get func(btransport.TCPInfoSnapshot) time.Duration) func(a, b btransport.TCPInfoSnapshot) int {
	return func(a, b btransport.TCPInfoSnapshot) int {
		x, y := get(a), get(b)
		switch {
		case x < y:
			return -1
		case x > y:
			return 1
		}
		return 0
	}
}
func cmpTime(get func(btransport.TCPInfoSnapshot) time.Time) func(a, b btransport.TCPInfoSnapshot) int {
	return func(a, b btransport.TCPInfoSnapshot) int {
		x, y := get(a), get(b)
		switch {
		case x.Before(y):
			return -1
		case x.After(y):
			return 1
		}
		return 0
	}
}

// headerCell is the per-column view struct the template iterates. Built
// once per request so all the "which arrow, which link" logic lives in
// Go — the template just prints strings.
type headerCell struct {
	Label     string
	Class     string
	Title     string
	Href      string        // "" if not sortable
	Arrow     string        // "" / "↑" / "↓"
	BodyTpl   template.HTML // <td>…</td> inner action, wrapped with class
	CellClass string        // repeated per row via BodyTpl already; kept for possible reuse
}

func (s *server) handleIndex(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	all := q.Get("all") == "1"
	onlyHot := q.Get("only") == "hot"
	// ?remote=<ip:port> narrows to conns whose RemoteAddr matches exactly.
	// Sessionz uses this to link a slow-vRPC row to the specific conn(s)
	// its session was bound to. Empty string means no filter.
	remoteFilter := q.Get("remote")
	sortKey, sortDir := parseSort(q)

	raw := s.snapshot()
	total := len(raw)
	hidden := 0
	if remoteFilter != "" {
		filtered := raw[:0]
		for _, snap := range raw {
			if snap.RemoteAddr != remoteFilter {
				hidden++
				continue
			}
			filtered = append(filtered, snap)
		}
		raw = filtered
	} else if !all {
		filtered := raw[:0]
		for _, snap := range raw {
			// Default view hides remote :443 conns — those are the
			// classic-path CFE / metrics-export channels, not the AFE
			// data path most tcpz users care about. ?all=1 restores.
			if strings.HasSuffix(snap.RemoteAddr, ":443") {
				hidden++
				continue
			}
			filtered = append(filtered, snap)
		}
		raw = filtered
	}

	// Serve JSON before we build display-only fields.
	if q.Get("format") == "json" {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		_ = json.NewEncoder(w).Encode(raw)
		return
	}

	rows := make([]row, 0, len(raw))
	interesting := 0
	dropped := 0
	for _, snap := range raw {
		sev, why := classify(snap)
		if sev > sevOK {
			interesting++
		}
		if onlyHot && sev < sevWarn {
			dropped++
			continue
		}
		rows = append(rows, row{
			TCPInfoSnapshot: snap,
			Sev:             sev.rowClass(),
			Why:             strings.Join(why, "+"),
			Interest:        sev > sevOK,
		})
	}

	sortRows(rows, sortKey, sortDir)

	// Build the header cells once per request — the "which arrow, which
	// link" logic lives here so the template just concatenates strings.
	baseParams := url.Values{}
	if all {
		baseParams.Set("all", "1")
	}
	if onlyHot {
		baseParams.Set("only", "hot")
	}
	headers := make([]headerCell, len(cols))
	for i, c := range cols {
		hc := headerCell{Label: c.Label, Class: c.Class, Title: c.Title}
		if c.Cmp != nil {
			// Toggle direction when clicking the already-active column;
			// otherwise use the column's natural first-click direction.
			nextDir := "asc"
			if c.Desc {
				nextDir = "desc"
			}
			if sortKey == c.Key {
				if sortDir == "asc" {
					hc.Arrow = "↑"
					nextDir = "desc"
				} else {
					hc.Arrow = "↓"
					nextDir = "asc"
				}
			}
			p := cloneValues(baseParams)
			p.Set("sort", c.Key)
			p.Set("dir", nextDir)
			hc.Href = "?" + p.Encode()
		}
		headers[i] = hc
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	data := struct {
		Rows        []row
		Cols        []colDef
		Headers     []headerCell
		Count       int
		Total       int
		Hidden      int
		Interesting int
		Dropped     int
		ShowAll     bool
		OnlyHot     bool
		SortKey     string
		SortDir     string
		SortByDial  bool
		Generated   time.Time
	}{
		Rows:        rows,
		Cols:        cols,
		Headers:     headers,
		Count:       len(rows),
		Total:       total,
		Hidden:      hidden,
		Interesting: interesting,
		Dropped:     dropped,
		ShowAll:     all,
		OnlyHot:     onlyHot,
		SortKey:     sortKey,
		SortDir:     sortDir,
		SortByDial:  sortKey == "dial",
		Generated:   time.Now(),
	}
	if err := tpl.Execute(w, data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// parseSort normalizes ?sort=<key>&dir=<asc|desc>. Unknown keys silently
// fall back to "sev" so a stale bookmarked URL doesn't 404. dir="" means
// "use the column's natural direction" (handled by sortRows).
func parseSort(q url.Values) (key, dir string) {
	key = q.Get("sort")
	dir = q.Get("dir")
	if dir != "asc" && dir != "desc" {
		dir = ""
	}
	switch key {
	case "", "sev", "dial":
		if key == "" {
			key = "sev"
		}
		return key, dir
	}
	if _, ok := colByKey[key]; !ok {
		return "sev", "" // unknown column — fall back cleanly
	}
	return key, dir
}

// sortRows reorders rows in place per key+dir. Special keys "sev" and
// "dial" don't map to columns and get their own comparators. All other
// keys map to a colDef.Cmp.
func sortRows(rows []row, key, dir string) {
	switch key {
	case "sev":
		// Interesting-first, ties broken by dial order (older on top) so
		// a still-hot conn keeps its position across the 2s refresh.
		sort.SliceStable(rows, func(i, j int) bool {
			si := sevRank(rows[i].Sev)
			sj := sevRank(rows[j].Sev)
			if si != sj {
				return si > sj
			}
			return rows[i].DialedAt.Before(rows[j].DialedAt)
		})
		return
	case "dial":
		sort.SliceStable(rows, func(i, j int) bool {
			return rows[i].DialedAt.Before(rows[j].DialedAt)
		})
		return
	}
	c, ok := colByKey[key]
	if !ok || c.Cmp == nil {
		return
	}
	// Direction: explicit "asc"/"desc" wins; otherwise use the column's
	// natural direction so a fresh click on an unset column lands on the
	// direction the user usually wants (biggest-first for counters,
	// alphabetical for strings).
	desc := c.Desc
	if dir == "asc" {
		desc = false
	} else if dir == "desc" {
		desc = true
	}
	sort.SliceStable(rows, func(i, j int) bool {
		r := c.Cmp(rows[i].TCPInfoSnapshot, rows[j].TCPInfoSnapshot)
		if r == 0 {
			// Deterministic tie-break by dial order so refreshes are
			// stable when two conns have identical counter values.
			return rows[i].DialedAt.Before(rows[j].DialedAt)
		}
		if desc {
			return r > 0
		}
		return r < 0
	})
}

// cloneValues returns a shallow copy of v so we can mutate it per header
// link without polluting the base params slice.
func cloneValues(v url.Values) url.Values {
	out := make(url.Values, len(v))
	for k, vs := range v {
		out[k] = append([]string(nil), vs...)
	}
	return out
}

// sevRank maps the row-class string back to a comparable int so the sort
// comparator doesn't have to hold onto the severity enum separately.
func sevRank(cls string) int {
	switch cls {
	case "row-crit":
		return 3
	case "row-warn":
		return 2
	case "row-note":
		return 1
	}
	return 0
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
	// pmtu renders a path-MTU value with a subtle hint about the path:
	//   0     → "—" (unknown / not populated on this kernel)
	//   1500  → plain (standard ethernet — nothing exotic)
	//   <1500 → dimmed with the delta from 1500 shown in a tooltip; the
	//           delta is exactly how many bytes of tunnel headers the
	//           path is eating (Andromeda ≈ 40, IPsec ≈ 60, GRE ≈ 24…).
	//   <1300 → hot (unusually heavy encapsulation — worth investigating)
	"pmtu": func(v uint32) template.HTML {
		if v == 0 {
			return template.HTML("—")
		}
		switch {
		case v >= 1500:
			return template.HTML(fmt.Sprintf("%d", v))
		case v < 1300:
			return template.HTML(fmt.Sprintf(`<b class="hot" title="%d B below 1500 — unusually heavy encapsulation">%d</b>`, 1500-v, v))
		default:
			return template.HTML(fmt.Sprintf(`<span title="%d B below 1500 — tunneling in path">%d</span>`, 1500-v, v))
		}
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
	// hotNum / hotBytes / hotPct wrap num/bytes/pct with a <b class="hot">
	// span when non-zero. Row color says "this row is interesting"; hot
	// cells say "and here's the specific counter that flagged it."
	"hotNum": func(v uint32) template.HTML {
		if v == 0 {
			return template.HTML("—")
		}
		return template.HTML(fmt.Sprintf(`<b class="hot">%d</b>`, v))
	},
	"critNum": func(v uint32) template.HTML {
		// critNum is for Backoff — non-zero means we're actively timing
		// out, which is the sharpest single signal in TCP_INFO.
		if v == 0 {
			return template.HTML("—")
		}
		return template.HTML(fmt.Sprintf(`<b class="hot crit">%d</b>`, v))
	},
	"hotBytes": func(v uint64) template.HTML {
		if v == 0 {
			return template.HTML("—")
		}
		f := float64(v)
		var s string
		switch {
		case f >= 1<<30:
			s = fmt.Sprintf("%.1f GiB", f/(1<<30))
		case f >= 1<<20:
			s = fmt.Sprintf("%.1f MiB", f/(1<<20))
		case f >= 1<<10:
			s = fmt.Sprintf("%.1f KiB", f/(1<<10))
		default:
			s = fmt.Sprintf("%d B", v)
		}
		return template.HTML(fmt.Sprintf(`<b class="hot">%s</b>`, s))
	},
	"hotPct": func(v float64) template.HTML {
		if v == 0 {
			return template.HTML("—")
		}
		var s string
		if v < 0.01 {
			s = "<0.01%"
		} else {
			s = fmt.Sprintf("%.2f%%", v)
		}
		return template.HTML(fmt.Sprintf(`<b class="hot">%s</b>`, s))
	},
	"or": func(s, fallback string) string {
		if s == "" {
			return fallback
		}
		return s
	},
	// cell renders one column's inner HTML for one row by executing the
	// per-column body template. bodyTpls is populated in init(); Parse
	// only needs the func to exist, not for bodies to be ready yet.
	"cell": func(idx int, r row) (template.HTML, error) {
		var buf strings.Builder
		if err := bodyTpls[idx].Execute(&buf, r); err != nil {
			return "", err
		}
		return template.HTML(buf.String()), nil
	},
}

// bodyTpls holds each column's cell-body snippet, parsed once at package
// init against the same funcs map the outer template uses. The outer
// template's per-row loop calls {{cell $i $row}} which executes the
// matching bodyTpl — this keeps the outer template short and lets column
// order / additions be a one-slice-entry change to cols.
//
// Populated in init() (see below). The "cell" template func in funcs
// closes over this variable; funcs is finalized before Parse so the
// outer template parses cleanly, then init() fills the bodies before
// any Execute call runs.
var bodyTpls []*template.Template

func init() {
	bodyTpls = make([]*template.Template, len(cols))
	for i, c := range cols {
		t, err := template.New(fmt.Sprintf("col-%d", i)).Funcs(funcs).Parse(c.Body)
		if err != nil {
			panic(fmt.Sprintf("tcpz: parse column %q body: %v", c.Label, err))
		}
		bodyTpls[i] = t
	}
}

var tpl = template.Must(template.New("tcpz").Funcs(funcs).Parse(tplSrc))

const tplSrc = `<!doctype html>
<html>
<head>
<meta charset="utf-8">
<title>tcpz — {{.Count}} conns{{if .Interesting}} · {{.Interesting}} hot{{end}}</title>
<meta http-equiv="refresh" content="2">
<style>
body { font: 13px/1.4 -apple-system, "Segoe UI", Helvetica, Arial, sans-serif; margin: 1em; color: #222; }
h1 { font-size: 1.1em; margin: 0 0 .3em 0; }
.meta { color: #666; margin-bottom: .3em; }
.legend { color: #666; margin-bottom: .8em; font-size: 12px; }
.legend .sw { display: inline-block; width: .85em; height: .85em; vertical-align: -1px; border: 1px solid #ccc; margin-right: 3px; }
.legend .sw.crit { background: #fdecea; border-color: #f2c2bd; }
.legend .sw.warn { background: #fff5e0; border-color: #ecd9a3; }
.legend .sw.note { background: #eef4fb; border-color: #cddceb; }
.legend .sw.ok   { background: #ffffff; }
table { border-collapse: collapse; width: 100%; }
th, td { text-align: left; padding: 4px 8px; border-bottom: 1px solid #eee; }
th { background: #f4f4f4; font-weight: 600; position: sticky; top: 0; }
td.num { text-align: right; font-variant-numeric: tabular-nums; }
td.mono, th.mono { font-family: SFMono-Regular, Menlo, Consolas, monospace; font-size: 12px; }
tr.row-crit td { background: #fdecea; }
tr.row-warn td { background: #fff5e0; }
tr.row-note td { background: #eef4fb; }
tr.row-crit:hover td { background: #fadbd7; }
tr.row-warn:hover td { background: #ffecc4; }
tr.row-note:hover td { background: #dde9f4; }
tr.row-ok:hover td   { background: #fafafa; }
.why { color: #666; font-size: 11px; font-family: SFMono-Regular, Menlo, Consolas, monospace; }
tr.row-crit .why { color: #b32222; }
tr.row-warn .why { color: #a04500; }
b.hot { color: #a04500; }
b.hot.crit { color: #b32222; background: #f7d7d3; padding: 0 3px; border-radius: 2px; }
a.col-sort { color: inherit; text-decoration: none; }
a.col-sort:hover { text-decoration: underline; }
a.col-sort .arr { color: #d95700; margin-left: 2px; }
.empty { color: #888; margin: 2em 0; }
.err { color: #a04500; }
.state-ESTABLISHED { color: #197a1f; }
.state-CLOSE_WAIT, .state-FIN_WAIT1, .state-FIN_WAIT2, .state-CLOSING, .state-LAST_ACK, .state-TIME_WAIT { color: #a04500; font-weight: 600; }
.ca-Open { color: #197a1f; }
.ca-Disorder { color: #a06a00; font-weight: 600; }
.ca-CWR { color: #a04500; font-weight: 600; }
.ca-Recovery { color: #b32222; font-weight: 600; }
.ca-Loss { color: #b32222; font-weight: 700; }
</style>
</head>
<body>
<h1>tcpz — {{.Count}} conn{{if ne .Count 1}}s{{end}}{{if .Interesting}} · <span style="color:#b32222">{{.Interesting}} interesting</span>{{end}}{{if .Hidden}} <span style="color:#888;font-weight:400;font-size:.85em">({{.Hidden}} :443 hidden)</span>{{end}}{{if .Dropped}} <span style="color:#888;font-weight:400;font-size:.85em">({{.Dropped}} healthy hidden)</span>{{end}}</h1>
<div class="meta">Snapshot at {{.Generated.Format "15:04:05.000"}} · auto-refresh 2s · <a href="?format=json{{if .ShowAll}}&amp;all=1{{end}}">JSON</a>
{{if .ShowAll}} · <a href="?">hide :443 (default)</a>{{else if .Hidden}} · <a href="?all=1">show all ({{.Total}})</a>{{end}}
{{if .OnlyHot}} · <a href="?{{if .ShowAll}}all=1{{end}}">show healthy too</a>{{else}} · <a href="?only=hot{{if .ShowAll}}&amp;all=1{{end}}">only hot</a>{{end}}
· sort: {{if eq .SortKey "sev"}}<b>severity</b>{{else}}<a href="?{{if .ShowAll}}all=1&amp;{{end}}{{if .OnlyHot}}only=hot&amp;{{end}}sort=sev">severity</a>{{end}}
| {{if eq .SortKey "dial"}}<b>dial order</b>{{else}}<a href="?{{if .ShowAll}}all=1&amp;{{end}}{{if .OnlyHot}}only=hot&amp;{{end}}sort=dial">dial order</a>{{end}}
{{if and (ne .SortKey "sev") (ne .SortKey "dial")}} | column: <b>{{.SortKey}} {{if eq .SortDir "asc"}}↑{{else}}↓{{end}}</b>{{end}}
</div>
<div class="legend">Row color:
<span class="sw crit"></span>Loss / backoff (RTO-driven)
<span class="sw warn"></span>retrans / DSACK / ECN / reorder
<span class="sw note"></span>non-ESTABLISHED / unreadable
<span class="sw ok"></span>healthy
· <b class="hot">bold orange</b> = the specific counter that flagged the row.
</div>
{{if not .Rows}}
<div class="empty">No conns registered. Either the client uses DirectPath (xDS bypasses the standard dialer, so nothing is captured), no traffic has been dialed yet, {{if .OnlyHot}}every conn is healthy (try <a href="?">without ?only=hot</a>), {{end}}or bigtable.TCPStats was never passed into the Client's options.</div>
{{else}}
<table>
<thead><tr>
{{range .Headers}}
<th{{if .Class}} class="{{.Class}}"{{end}} title="{{.Title}}">{{if .Href}}<a class="col-sort" href="{{.Href}}">{{.Label}}{{if .Arrow}}<span class="arr">{{.Arrow}}</span>{{end}}</a>{{else}}{{.Label}}{{end}}</th>
{{end}}
</tr></thead>
<tbody>
{{range $r := .Rows}}
<tr class="{{$r.Sev}}">{{range $i, $c := $.Cols}}<td{{if $c.Class}} class="{{$c.Class}}"{{end}}>{{cell $i $r}}</td>{{end}}</tr>
{{end}}
</tbody>
</table>
{{end}}
</body>
</html>
`
