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

// Package debugview mounts every Bigtable -z debug page (sessionz / afez /
// flightz / loadz / channelz / configz / tcpz) behind a single
// http.Handler. Serves a link index at "/" and each view at "/<view>/".
//
// Typical wiring is one line:
//
//	stats := bigtable.NewTCPStats()
//	client, _ := bigtable.NewClientWithConfig(ctx, "p", "i",
//	    bigtable.ClientConfig{EnableSessionPool: true},
//	    stats.ClientOption())
//	http.Handle("/debug/", http.StripPrefix("/debug",
//	    debugview.Handler(client, stats)))
//
// Both arguments are nil-safe: client-backed views on a nil client render
// their "not enabled" empty state, and /tcpz/ with nil stats renders
// "TCP stats collector not attached".
package debugview

import (
	"html/template"
	"net/http"
	"time"

	"cloud.google.com/go/bigtable"
)

// Handler returns the combined debug mux. See package doc for the routes
// it exposes. Panics on template parse errors would surface at
// package-init time (see the per-view *TplSrc constants), not here.
func Handler(c *bigtable.Client, s *bigtable.TCPStats) http.Handler {
	mux := http.NewServeMux()

	sessionProv := sessionProviderForClient(c)
	channelProv := channelProviderForClient(c)
	configProv := configProviderForClient(c)

	mux.Handle("/sessionz/", http.StripPrefix("/sessionz", newSessionzHandler(sessionProv)))
	mux.Handle("/afez/", http.StripPrefix("/afez", newAfezHandler(sessionProv)))
	mux.Handle("/flightz/", http.StripPrefix("/flightz", newFlightzHandler(sessionProv)))
	mux.Handle("/loadz/", http.StripPrefix("/loadz", newLoadzHandler(sessionProv)))
	mux.Handle("/channelz/", http.StripPrefix("/channelz", newChannelzHandler(channelProv)))
	mux.Handle("/configz/", http.StripPrefix("/configz", newConfigzHandler(configProv)))
	mux.Handle("/tcpz/", http.StripPrefix("/tcpz", newTcpzHandler(s)))

	// Index page lives at the root. Anything else that lands here (a
	// mis-typed sub-path) 404s cleanly rather than serving the index.
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" && r.URL.Path != "" {
			http.NotFound(w, r)
			return
		}
		writeHTML(w, indexTpl, indexPageData{Generated: time.Now()})
	})

	return mux
}

// sessionProviderForClient returns c.SessionDebug() with a nil-safe
// short-circuit so callers don't have to check nil twice.
func sessionProviderForClient(c *bigtable.Client) bigtable.SessionDebugProvider {
	if c == nil {
		return nil
	}
	return c.SessionDebug()
}

func channelProviderForClient(c *bigtable.Client) bigtable.ChannelDebugProvider {
	if c == nil {
		return nil
	}
	return c.ChannelDebug()
}

func configProviderForClient(c *bigtable.Client) bigtable.ConfigDebugProvider {
	if c == nil {
		return nil
	}
	return c.ConfigDebug()
}

type indexPageData struct {
	Generated time.Time
}

const indexTplSrc = `<!doctype html>
<html><head>
<meta charset="utf-8">
<title>bigtable debug</title>
<style>
body{font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;margin:2em;color:#222;background:#fafafa}
h1{font-size:1.3em;margin:0 0 .3em 0}
h2{font-size:1em;color:#666;font-weight:normal;margin:0 0 1.5em 0}
ul{list-style:none;padding:0;max-width:38em}
li{background:#fff;margin-bottom:.6em;padding:.6em .9em;box-shadow:0 1px 2px rgba(0,0,0,.06)}
li a{color:#1a5fb4;text-decoration:none;font-weight:600;font-size:1em}
li a:hover{text-decoration:underline}
li .desc{color:#666;font-size:.88em;margin-top:.15em}
</style>
</head><body>
<h1>Bigtable debug views</h1>
<h2>generated {{.Generated.Format "15:04:05 MST"}}</h2>
<ul>
<li><a href="sessionz/">sessionz</a> <div class="desc">per-pool sessions, states, latency histograms, slow-vRPC log, scaling history.</div></li>
<li><a href="afez/">afez</a> <div class="desc">per-AFE bucketing: refCount, idle, in-use, EWMAs, last-connected.</div></li>
<li><a href="flightz/">flightz</a> <div class="desc">live in-flight vRPCs, oldest first (stuck-request forensics).</div></li>
<li><a href="loadz/">loadz</a> <div class="desc">picker decision reasoning, actual-vs-ideal AFE fanout, K-choice trace.</div></li>
<li><a href="channelz/">channelz</a> <div class="desc">gRPC channel pool state (classic + session).</div></li>
<li><a href="configz/">configz</a> <div class="desc">server-driven client configuration (GetClientConfiguration).</div></li>
<li><a href="tcpz/">tcpz</a> <div class="desc">per-connection TCP_INFO (RTT, retrans, PMTU) — requires bigtable.TCPStats.</div></li>
</ul>
</body></html>
`

var indexTpl = template.Must(template.New("debugview-index").Parse(indexTplSrc))
