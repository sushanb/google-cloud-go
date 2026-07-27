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

// Load-balancing simulation harness for SessionPoolImpl.
//
// Wraps fakeStream in an auto-responding simServer so the pool can go
// through its real Start / handshake / vRPC / GOAWAY / close paths without
// dialing a real backend. Each scenario logs a distribution + latency +
// goroutine-diff report via t.Logf; assertions are used only for hard
// invariants (pool didn't return, unbounded goroutine growth, panics).
//
// The output is informational — this is a bug-hunting harness, not a
// pass/fail gate. Read the t.Logf report of any surprising scenario.
//
// Runnable individually:
//   go test -run '^TestAfeLbSim_UniformDistribution$' -v ./internal/transport/
// Or as a batch:
//   go test -run AfeLbSim -v -timeout=15m ./internal/transport/

package internal

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	spb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
	rpcstatus "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

// ---------------------------------------------------------------------------
// simAfe / simServer — auto-responding fake backend
// ---------------------------------------------------------------------------

// simAfe is one simulated AFE. Multiple simAfe values are handed to
// newSimServer; each new Stream produced by StreamFactory is assigned to
// an AFE round-robin.
type simAfe struct {
	id            int64
	baseLatency   time.Duration // per-vRPC latency floor
	jitter        time.Duration // uniform ± jitter added to baseLatency
	vRpcErrorRate float64       // 0..1 chance of returning an ErrorResponse
	// openFailRate: 0..1 chance the auto-responder will push a stream-
	// error frame instead of OpenSessionResponse. Drives handleClose ->
	// StreamEnd:Unavailable, which is classified as an abnormal close and
	// feeds the pool's consecutive-failure breaker.
	openFailRate float64
	// goAwayEvery: if > 0, after this many vRPCs served the auto-
	// responder pushes a GoAway frame. Used by scenario 11.
	goAwayEvery int64
	// stallVRpc: if true, never send a VirtualRpcResponse. The client's
	// per-vRPC heartbeat watchdog eventually fires. Used by scenario 15.
	stallVRpc bool
}

// simAfeStats is per-AFE counter output from simServer.Snapshot().
type simAfeStats struct {
	ID              int64
	SessionsOpened  int64
	OpensFailed     int64
	VRpcsServed     int64
	VRpcsErrored    int64
	GoAwaysSent     int64
	CurrentInflight int64
}

// simServer wraps a set of simAfes and produces fakeStreams that respond
// to Session Send calls automatically.
type simServer struct {
	t        testing.TB
	handshakeDelay time.Duration

	afesMu sync.RWMutex
	afes   []simAfe

	rr atomic.Int64 // stream -> AFE round-robin

	statsMu sync.Mutex
	stats   map[int64]*simAfeStats

	streamsMu sync.Mutex
	streams   []*fakeStream
	closed    atomic.Bool
	pending   sync.WaitGroup // in-flight response goroutines
}

func newSimServer(t testing.TB, afes []simAfe) *simServer {
	s := &simServer{
		t:              t,
		handshakeDelay: 2 * time.Millisecond,
		afes:           append([]simAfe(nil), afes...),
		stats:          make(map[int64]*simAfeStats),
	}
	for _, a := range afes {
		s.stats[a.id] = &simAfeStats{ID: a.id}
	}
	return s
}

// StreamFactory returns a stream factory suitable for NewSessionPoolImpl.
// The returned factory refuses new streams once closeAll has been called
// so a late spawnTickOnce can't leak a live goroutine past the test.
func (s *simServer) StreamFactory() func(context.Context) (Stream, error) {
	return func(_ context.Context) (Stream, error) {
		if s.closed.Load() {
			return nil, errors.New("simServer: closed")
		}
		s.afesMu.RLock()
		if len(s.afes) == 0 {
			s.afesMu.RUnlock()
			return nil, errors.New("simServer: no AFEs configured")
		}
		idx := int(s.rr.Add(1)-1) % len(s.afes)
		afe := s.afes[idx]
		s.afesMu.RUnlock()

		stream := newFakeStream()
		s.streamsMu.Lock()
		s.streams = append(s.streams, stream)
		s.streamsMu.Unlock()

		s.statsMu.Lock()
		if s.stats[afe.id] == nil {
			s.stats[afe.id] = &simAfeStats{ID: afe.id}
		}
		s.statsMu.Unlock()

		// Populate PeerInfo header BEFORE anyone sends anything on the
		// stream so handleOpenSession's stream.Header() sees it.
		if err := s.stampPeerInfo(stream, afe.id); err != nil {
			s.t.Fatalf("simServer stamp peer info: %v", err)
		}

		// Per-stream state kept in closures so we don't need a map lookup
		// on every Send.
		var vrpcCount atomic.Int64
		var opened atomic.Bool

		stream.sendFn = func(req *spb.SessionRequest) error {
			if s.closed.Load() {
				return nil
			}
			// Snapshot the AFE config live so mid-run reconfig
			// (scenarios 13/14) takes effect.
			cur := s.currentAfeByID(afe.id)
			switch p := req.GetPayload().(type) {
			case *spb.SessionRequest_OpenSession:
				_ = p
				if opened.Swap(true) {
					return nil // already handled; retransmit shouldn't happen
				}
				s.spawnHandshake(stream, cur)
			case *spb.SessionRequest_VirtualRpc:
				s.spawnVRpcResponse(stream, cur, p.VirtualRpc.GetRpcId(), &vrpcCount)
			case *spb.SessionRequest_CloseSession:
				s.spawnClose(stream)
			}
			return nil
		}
		return stream, nil
	}
}

func (s *simServer) currentAfeByID(id int64) simAfe {
	s.afesMu.RLock()
	defer s.afesMu.RUnlock()
	for _, a := range s.afes {
		if a.id == id {
			return a
		}
	}
	// AFE removed mid-run — behave like a healthy AFE with zero data
	// so we don't false-cause a leak. Scenario 13 fills in specific
	// bad behavior via SetAfeConfig below.
	return simAfe{id: id, baseLatency: time.Millisecond}
}

// SetAfeConfig atomically replaces the configuration for one AFE id.
// Sessions in flight see the new config on their next Send.
func (s *simServer) SetAfeConfig(id int64, cfg simAfe) {
	s.afesMu.Lock()
	defer s.afesMu.Unlock()
	for i, a := range s.afes {
		if a.id == id {
			s.afes[i] = cfg
			return
		}
	}
	s.afes = append(s.afes, cfg)
}

// AddAfe appends a new AFE to the round-robin. Existing sessions keep
// their AFE; only future streams see it.
func (s *simServer) AddAfe(cfg simAfe) {
	s.afesMu.Lock()
	s.afes = append(s.afes, cfg)
	s.afesMu.Unlock()
	s.statsMu.Lock()
	if s.stats[cfg.id] == nil {
		s.stats[cfg.id] = &simAfeStats{ID: cfg.id}
	}
	s.statsMu.Unlock()
}

func (s *simServer) stampPeerInfo(stream *fakeStream, afeID int64) error {
	pi := &spb.PeerInfo{ApplicationFrontendId: afeID}
	raw, err := proto.Marshal(pi)
	if err != nil {
		return err
	}
	stream.hdr = metadata.MD{peerInfoHeaderKey: []string{base64.RawURLEncoding.EncodeToString(raw)}}
	return nil
}

// spawnHandshake pushes an OpenSessionResponse (or a stream error on
// openFailRate) after handshakeDelay.
func (s *simServer) spawnHandshake(stream *fakeStream, afe simAfe) {
	s.pending.Add(1)
	go func() {
		defer s.pending.Done()
		s.sleep(s.handshakeDelay)
		if s.closed.Load() {
			return
		}
		if afe.openFailRate > 0 && rand.Float64() < afe.openFailRate {
			s.bumpOpenFail(afe.id)
			// Server-rejected OpenSession — push a stream-level error.
			// handleClose classifies it as StreamEnd:Unavailable, feeds
			// noteAbnormalCloseIfAny, and (past threshold) trips the
			// pool's consecutive-failure breaker.
			s.tryPush(stream, recvOp{err: fmt.Errorf("simServer: open rejected")})
			return
		}
		s.bumpOpenOK(afe.id)
		s.tryPush(stream, recvOp{resp: &spb.SessionResponse{
			Payload: &spb.SessionResponse_OpenSession{OpenSession: &spb.OpenSessionResponse{}},
		}})
	}()
}

// spawnVRpcResponse pushes a VirtualRpcResponse (or ErrorResponse per
// vRpcErrorRate) after baseLatency+jitter. Also fires GOAWAY when
// goAwayEvery is set.
func (s *simServer) spawnVRpcResponse(stream *fakeStream, afe simAfe, rpcID int64, vrpcCount *atomic.Int64) {
	s.pending.Add(1)
	go func() {
		defer s.pending.Done()
		s.bumpInflight(afe.id, 1)
		defer s.bumpInflight(afe.id, -1)

		lat := afe.baseLatency + jitterDur(afe.jitter)
		s.sleep(lat)
		if s.closed.Load() || afe.stallVRpc {
			return // client's heartbeat watchdog will do the cleanup
		}

		n := vrpcCount.Add(1)
		if afe.vRpcErrorRate > 0 && rand.Float64() < afe.vRpcErrorRate {
			s.bumpVRpc(afe.id, false)
			s.tryPush(stream, recvOp{resp: &spb.SessionResponse{
				Payload: &spb.SessionResponse_Error{Error: &spb.ErrorResponse{
					RpcId:  rpcID,
					Status: &rpcstatus.Status{Code: int32(codes.FailedPrecondition), Message: "sim error"},
				}},
			}})
		} else {
			s.bumpVRpc(afe.id, true)
			s.tryPush(stream, recvOp{resp: &spb.SessionResponse{
				Payload: &spb.SessionResponse_VirtualRpc{VirtualRpc: &spb.VirtualRpcResponse{
					RpcId:   rpcID,
					Payload: []byte("ok"),
				}},
			}})
		}
		if afe.goAwayEvery > 0 && n%afe.goAwayEvery == 0 {
			s.bumpGoAway(afe.id)
			s.tryPush(stream, recvOp{resp: &spb.SessionResponse{
				Payload: &spb.SessionResponse_GoAway{GoAway: &spb.GoAwayResponse{
					Reason: "sim goaway", LastRpcIdAdmitted: rpcID,
				}},
			}})
		}
	}()
}

func (s *simServer) spawnClose(stream *fakeStream) {
	s.pending.Add(1)
	go func() {
		defer s.pending.Done()
		s.sleep(s.handshakeDelay)
		stream.Close()
	}()
}

func (s *simServer) sleep(d time.Duration) {
	if d <= 0 {
		return
	}
	select {
	case <-time.After(d):
	}
}

// tryPush guards against a panic when stream.recv has been closed by
// stream.Close (raced by teardown).
func (s *simServer) tryPush(stream *fakeStream, op recvOp) {
	defer func() { _ = recover() }()
	select {
	case stream.recv <- op:
	case <-time.After(2 * time.Second):
		// Reader isn't consuming — session is likely torn down. Give up.
	}
}

func (s *simServer) bumpOpenOK(id int64) {
	s.statsMu.Lock()
	defer s.statsMu.Unlock()
	if st := s.stats[id]; st != nil {
		st.SessionsOpened++
	}
}
func (s *simServer) bumpOpenFail(id int64) {
	s.statsMu.Lock()
	defer s.statsMu.Unlock()
	if st := s.stats[id]; st != nil {
		st.OpensFailed++
	}
}
func (s *simServer) bumpVRpc(id int64, ok bool) {
	s.statsMu.Lock()
	defer s.statsMu.Unlock()
	st := s.stats[id]
	if st == nil {
		return
	}
	if ok {
		st.VRpcsServed++
	} else {
		st.VRpcsErrored++
	}
}
func (s *simServer) bumpGoAway(id int64) {
	s.statsMu.Lock()
	defer s.statsMu.Unlock()
	if st := s.stats[id]; st != nil {
		st.GoAwaysSent++
	}
}
func (s *simServer) bumpInflight(id int64, delta int64) {
	s.statsMu.Lock()
	defer s.statsMu.Unlock()
	if st := s.stats[id]; st != nil {
		st.CurrentInflight += delta
	}
}

// Snapshot returns a copy of the per-AFE counters.
func (s *simServer) Snapshot() []simAfeStats {
	s.statsMu.Lock()
	defer s.statsMu.Unlock()
	out := make([]simAfeStats, 0, len(s.stats))
	for _, st := range s.stats {
		out = append(out, *st)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// closeAll releases all fake streams and waits for outstanding response
// goroutines to exit.
func (s *simServer) closeAll() {
	s.closed.Store(true)
	s.streamsMu.Lock()
	for _, st := range s.streams {
		st.Close()
	}
	s.streamsMu.Unlock()
	// Give response goroutines a chance to exit their sleep().
	waitCh := make(chan struct{})
	go func() { s.pending.Wait(); close(waitCh) }()
	select {
	case <-waitCh:
	case <-time.After(3 * time.Second):
		// Late-arriving goroutines will discover s.closed on wake and
		// bail via tryPush's recover; we don't block the test.
	}
}

func jitterDur(j time.Duration) time.Duration {
	if j <= 0 {
		return 0
	}
	// uniform in [-j, +j]
	return time.Duration(rand.Int64N(int64(2*j))) - j
}

// ---------------------------------------------------------------------------
// simMetrics — per-scenario counter + latency + goroutine report
// ---------------------------------------------------------------------------

type simAfeCounters struct {
	picks      int64
	oks        int64
	errs       int64
	sumLatency time.Duration
}

type simMetrics struct {
	mu                sync.Mutex
	perAfe            map[int64]*simAfeCounters
	latencySamples    []time.Duration
	goroutineBaseline int
	goroutineEnd      int
	errors            map[string]int64
}

func newSimMetrics() *simMetrics {
	return &simMetrics{
		perAfe: make(map[int64]*simAfeCounters),
		errors: make(map[string]int64),
	}
}

func (m *simMetrics) record(afeID int64, latency time.Duration, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	c, ok := m.perAfe[afeID]
	if !ok {
		c = &simAfeCounters{}
		m.perAfe[afeID] = c
	}
	c.picks++
	c.sumLatency += latency
	if latency > 0 && err == nil {
		m.latencySamples = append(m.latencySamples, latency)
	}
	if err == nil {
		c.oks++
	} else {
		c.errs++
		m.errors[errKey(err)]++
	}
}

func errKey(err error) string {
	if err == nil {
		return ""
	}
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return "DeadlineExceeded"
	case errors.Is(err, context.Canceled):
		return "Canceled"
	case errors.Is(err, ErrConsecutiveFailures):
		return "ConsecutiveFailures"
	case errors.Is(err, ErrPoolClosed):
		return "PoolClosed"
	case errors.Is(err, ErrNoSessionsAvailable):
		return "NoSessionsAvailable"
	default:
		return fmt.Sprintf("%T", err)
	}
}

// simPercentile returns the value at pct (0..1) after sorting a copy.
// Named with a sim- prefix to avoid collision with the pool's percentile
// helper (session_debug.go).
func simPercentile(samples []time.Duration, pct float64) time.Duration {
	if len(samples) == 0 {
		return 0
	}
	tmp := append([]time.Duration(nil), samples...)
	sort.Slice(tmp, func(i, j int) bool { return tmp[i] < tmp[j] })
	idx := int(float64(len(tmp)-1) * pct)
	if idx < 0 {
		idx = 0
	}
	if idx >= len(tmp) {
		idx = len(tmp) - 1
	}
	return tmp[idx]
}

// gini computes the Gini coefficient of the given non-negative shares.
// Returns 0 for a perfectly uniform distribution.
func gini(shares []float64) float64 {
	n := len(shares)
	if n == 0 {
		return 0
	}
	tmp := append([]float64(nil), shares...)
	sort.Float64s(tmp)
	var sum, sumAll float64
	for i, s := range tmp {
		sum += float64(i+1) * s
		sumAll += s
	}
	if sumAll == 0 {
		return 0
	}
	return (2*sum)/(float64(n)*sumAll) - float64(n+1)/float64(n)
}

// PrintReport dumps a t.Logf-formatted summary — AFE fanout, latency
// percentiles, error breakdown, goroutine delta.
func (m *simMetrics) PrintReport(t testing.TB, scenarioName string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	var total int64
	afeIDs := make([]int64, 0, len(m.perAfe))
	for id, c := range m.perAfe {
		total += c.picks
		afeIDs = append(afeIDs, id)
	}
	sort.Slice(afeIDs, func(i, j int) bool { return afeIDs[i] < afeIDs[j] })

	t.Logf("=== %s ===", scenarioName)
	t.Logf("Total picks: %d", total)

	shares := make([]float64, 0, len(afeIDs))
	for _, id := range afeIDs {
		c := m.perAfe[id]
		var share float64
		if total > 0 {
			share = float64(c.picks) / float64(total) * 100
		}
		shares = append(shares, float64(c.picks))
		var avg time.Duration
		if c.picks > 0 {
			avg = c.sumLatency / time.Duration(c.picks)
		}
		t.Logf("  AFE-%d: %d picks (%.1f%%) avg=%s err=%d",
			id, c.picks, share, avg.Round(time.Microsecond), c.errs)
	}
	if len(shares) > 1 {
		t.Logf("  Gini: %.3f (0=uniform, 1=fully concentrated)", gini(shares))
	}
	p50 := simPercentile(m.latencySamples, 0.50)
	p95 := simPercentile(m.latencySamples, 0.95)
	p99 := simPercentile(m.latencySamples, 0.99)
	t.Logf("  Latency (OK samples n=%d): p50=%s p95=%s p99=%s",
		len(m.latencySamples), p50.Round(time.Microsecond), p95.Round(time.Microsecond), p99.Round(time.Microsecond))

	diff := m.goroutineEnd - m.goroutineBaseline
	marker := ""
	if diff > 10 {
		marker = "  <-- >10 diff, worth investigating"
	}
	t.Logf("  Goroutines: start=%d end=%d diff=%+d%s",
		m.goroutineBaseline, m.goroutineEnd, diff, marker)

	if len(m.errors) > 0 {
		errKeys := make([]string, 0, len(m.errors))
		for k := range m.errors {
			errKeys = append(errKeys, k)
		}
		sort.Strings(errKeys)
		out := "  Errors:"
		for _, k := range errKeys {
			out += fmt.Sprintf(" %s=%d", k, m.errors[k])
		}
		t.Logf("%s", out)
	} else {
		t.Logf("  Errors: none")
	}
}

// ---------------------------------------------------------------------------
// simConfig / runSim — the driver
// ---------------------------------------------------------------------------

type qpsPoint struct {
	At  time.Duration
	Qps float64
}

type simConfig struct {
	Name          string
	Afes          []simAfe
	PoolMin       int
	PoolMax       int
	Picker        string // "simple" | "least-inflight" | "least-latency"
	SubsetSize    int    // K for the K-choice pickers; 0 -> default
	Duration      time.Duration
	Workers       int
	Qps           float64 // used when QpsRamp is empty
	QpsRamp       []qpsPoint
	CallTimeout   time.Duration // per-request context timeout (0 = 5s default)
	StartPoolLoops bool          // if true, call p.Start (Tick/prune/sweep loops)
	OnMidRun      func(t testing.TB, elapsed time.Duration, s *simServer, p *SessionPoolImpl) // hook for scenario 13/14/6
}

// setPicker installs the requested picker on the pool.
func setPicker(p *SessionPoolImpl, name string, subset int) {
	if subset <= 0 {
		subset = defaultAfeRandomSubsetSize
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	switch name {
	case "simple":
		p.picker = NewSimpleAfePicker()
	case "least-latency":
		p.picker = NewLeastLatencyAfePicker(subset)
	default:
		p.picker = NewLeastInFlightAfePicker(subset)
	}
}

// runSim wires the simServer into a NewSessionPoolImpl, drives it under
// the configured QPS/workers, and returns the collected metrics.
func runSim(t testing.TB, cfg simConfig) *simMetrics {
	t.Helper()

	m := newSimMetrics()
	m.goroutineBaseline = runtime.NumGoroutine()

	server := newSimServer(t, cfg.Afes)
	p := NewSessionPoolImpl(
		uint64(42),
		"sim-pool",
		cfg.PoolMin, cfg.PoolMax,
		server.StreamFactory(),
		&spb.OpenSessionRequest{ProtocolVersion: 1},
		nil,
		SessionTypeTable,
	)
	setPicker(p, cfg.Picker, cfg.SubsetSize)

	if cfg.StartPoolLoops {
		p.Start(p.poolCtx)
	}

	// t.Cleanup: close streams first (so readLoops unpark), then close
	// pool. Same order as newTestPool.
	t.Cleanup(func() {
		server.closeAll()
		_ = p.Close()
		// Sample goroutine count after teardown so tests that inherit
		// long-lived loops (Tick / prune / sweep) don't false-alarm.
		time.Sleep(50 * time.Millisecond)
		m.goroutineEnd = runtime.NumGoroutine()
	})

	// Cap the whole run so a bug can't hang the test binary.
	simCtx, simCancel := context.WithTimeout(context.Background(), cfg.Duration+2*time.Second)
	defer simCancel()

	desc := &fakeDesc{
		method: "SimVRpc",
		enc:    func(req interface{}) ([]byte, error) { return []byte(fmt.Sprint(req)), nil },
		dec:    func(buf []byte) (interface{}, error) { return string(buf), nil },
	}

	callTimeout := cfg.CallTimeout
	if callTimeout <= 0 {
		callTimeout = 5 * time.Second
	}

	workers := cfg.Workers
	if workers <= 0 {
		workers = 8
	}

	// currentQps snapshotting from a ramp — refreshed periodically.
	var currentQps atomic.Uint64
	setQps := func(v float64) {
		if v < 0 {
			v = 0
		}
		currentQps.Store(math.Float64bits(v))
	}
	getQps := func() float64 { return math.Float64frombits(currentQps.Load()) }
	if len(cfg.QpsRamp) > 0 {
		setQps(cfg.QpsRamp[0].Qps)
	} else {
		setQps(cfg.Qps)
	}

	startTime := time.Now()

	// Ramp / mid-run driver.
	var driverWg sync.WaitGroup
	driverWg.Add(1)
	go func() {
		defer driverWg.Done()
		ramp := cfg.QpsRamp
		next := 0
		tick := time.NewTicker(50 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-simCtx.Done():
				return
			case now := <-tick.C:
				elapsed := now.Sub(startTime)
				if elapsed > cfg.Duration {
					return
				}
				for next < len(ramp) && elapsed >= ramp[next].At {
					setQps(ramp[next].Qps)
					next++
				}
				if cfg.OnMidRun != nil {
					cfg.OnMidRun(t, elapsed, server, p)
				}
			}
		}
	}()

	// Worker pool.
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				qps := getQps()
				if qps <= 0 {
					select {
					case <-time.After(10 * time.Millisecond):
					case <-stop:
						return
					}
					continue
				}
				// per-worker inter-arrival = workers / qps
				interval := time.Duration(float64(workers) / qps * float64(time.Second))
				if interval < time.Microsecond {
					interval = time.Microsecond
				}

				ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
				callStart := time.Now()
				res, err := p.Invoke(ctx, desc, "sim")
				lat := time.Since(callStart)
				var afeID int64
				if res.PeerInfo != nil {
					afeID = res.PeerInfo.GetApplicationFrontendId()
				}
				m.record(afeID, lat, err)
				cancel()

				select {
				case <-time.After(interval):
				case <-stop:
					return
				}
			}
		}()
	}

	// Run for cfg.Duration.
	select {
	case <-time.After(cfg.Duration):
	case <-simCtx.Done():
	}
	close(stop)
	wg.Wait()
	driverWg.Wait()

	return m
}

// dumpStacksOnFailure captures a goroutine snapshot when a scenario has
// gone visibly wrong (deadlock, hung close). Never called on success.
func dumpStacksOnFailure(t testing.TB, reason string) {
	buf := make([]byte, 1<<20)
	n := runtime.Stack(buf, true)
	t.Logf("STACK DUMP (%s):\n%s", reason, buf[:n])
}

// ---------------------------------------------------------------------------
// Scenarios 1-10
// ---------------------------------------------------------------------------

// 1. Uniform distribution — 5 identical AFEs, LeastLatency picker,
//    modest QPS. Report Gini, latencies, goroutines.
func TestAfeLbSim_UniformDistribution(t *testing.T) {
	afes := make([]simAfe, 5)
	for i := range afes {
		afes[i] = simAfe{id: int64(100 + i), baseLatency: 10 * time.Millisecond, jitter: 2 * time.Millisecond}
	}
	m := runSim(t, simConfig{
		Name: "UniformDistribution", Afes: afes,
		PoolMin: 5, PoolMax: 25, Picker: "least-latency", SubsetSize: 2,
		Duration: 4 * time.Second, Workers: 16, Qps: 200,
		StartPoolLoops: true,
	})
	m.PrintReport(t, "UniformDistribution")
}

// 2. Slow AFE — one AFE is 5x the base latency. LeastLatency should
//    starve it; report share.
func TestAfeLbSim_SlowAfeStarved(t *testing.T) {
	afes := []simAfe{
		{id: 201, baseLatency: 10 * time.Millisecond, jitter: 2 * time.Millisecond},
		{id: 202, baseLatency: 10 * time.Millisecond, jitter: 2 * time.Millisecond},
		{id: 203, baseLatency: 10 * time.Millisecond, jitter: 2 * time.Millisecond},
		{id: 204, baseLatency: 10 * time.Millisecond, jitter: 2 * time.Millisecond},
		{id: 205, baseLatency: 50 * time.Millisecond, jitter: 5 * time.Millisecond}, // slow
	}
	m := runSim(t, simConfig{
		Name: "SlowAfeStarved", Afes: afes,
		PoolMin: 5, PoolMax: 30, Picker: "least-latency", SubsetSize: 3,
		Duration: 5 * time.Second, Workers: 16, Qps: 200,
		StartPoolLoops: true,
	})
	m.PrintReport(t, "SlowAfeStarved")
	if c := m.perAfe[205]; c != nil {
		var total int64
		for _, cc := range m.perAfe {
			total += cc.picks
		}
		if total > 0 {
			share := float64(c.picks) / float64(total) * 100
			t.Logf("  ANALYSIS: slow AFE 205 share = %.1f%% (want < ~30%% with 5 AFEs, uniform=20%%)", share)
		}
	}
}

// 3. Fast-failing AFE — OK-gate should prevent the erroring AFE from
//    gaming LeastLatency into concentrating traffic on it.
func TestAfeLbSim_ErrorRateNotGamed(t *testing.T) {
	afes := []simAfe{
		{id: 301, baseLatency: 10 * time.Millisecond, jitter: 2 * time.Millisecond},
		{id: 302, baseLatency: 10 * time.Millisecond, jitter: 2 * time.Millisecond},
		{id: 303, baseLatency: 10 * time.Millisecond, jitter: 2 * time.Millisecond},
		{id: 304, baseLatency: 10 * time.Millisecond, jitter: 2 * time.Millisecond},
		// AFE 305: fast reply but 50% ErrorResponse. OK-gate should
		// keep its e2eEwma from dropping toward the fast time.
		{id: 305, baseLatency: 1 * time.Millisecond, vRpcErrorRate: 0.5},
	}
	m := runSim(t, simConfig{
		Name: "ErrorRateNotGamed", Afes: afes,
		PoolMin: 5, PoolMax: 30, Picker: "least-latency", SubsetSize: 3,
		Duration: 5 * time.Second, Workers: 16, Qps: 200,
		StartPoolLoops: true,
	})
	m.PrintReport(t, "ErrorRateNotGamed")
	t.Logf("  ANALYSIS: OK-gate holds if AFE-305 share is NOT dominant (~20-40%%); dominant share = OK-gate broken")
}

// 4. Cold-start fan-out — pool starts min=0 and gets a burst of concurrent
//    Checkouts. New sessions should spread across AFEs at open time.
func TestAfeLbSim_ColdStartFanOut(t *testing.T) {
	afes := []simAfe{
		{id: 401, baseLatency: 10 * time.Millisecond},
		{id: 402, baseLatency: 10 * time.Millisecond},
		{id: 403, baseLatency: 10 * time.Millisecond},
	}
	m := runSim(t, simConfig{
		Name: "ColdStartFanOut", Afes: afes,
		PoolMin: 0, PoolMax: 50, Picker: "least-inflight", SubsetSize: 2,
		Duration: 3 * time.Second, Workers: 50, Qps: 500,
		StartPoolLoops: true,
	})
	m.PrintReport(t, "ColdStartFanOut")
	snap := m
	t.Logf("  ANALYSIS: with cold-start round-robin over AFEs, each AFE should hold ~1/N of sessions")
	_ = snap
}

// 5. Breaker trip on all-open-fail. All AFEs reject OpenSession.
//    Breaker should trip within ~consecutiveFailureThreshold=10 attempts
//    and parked waiters receive ErrConsecutiveFailures.
func TestAfeLbSim_BreakerTripsOnAllOpenFail(t *testing.T) {
	afes := []simAfe{
		{id: 501, baseLatency: 5 * time.Millisecond, openFailRate: 1.0},
		{id: 502, baseLatency: 5 * time.Millisecond, openFailRate: 1.0},
		{id: 503, baseLatency: 5 * time.Millisecond, openFailRate: 1.0},
	}
	m := runSim(t, simConfig{
		Name: "BreakerTripsOnAllOpenFail", Afes: afes,
		PoolMin: 1, PoolMax: 10, Picker: "least-inflight",
		Duration: 5 * time.Second, Workers: 8, Qps: 40,
		CallTimeout:    3 * time.Second,
		StartPoolLoops: true,
	})
	m.PrintReport(t, "BreakerTripsOnAllOpenFail")
	t.Logf("  ANALYSIS: expect ErrConsecutiveFailures or DeadlineExceeded in the error breakdown; a hung run means the breaker isn't reaching parked waiters")
}

// 6. Breaker recovery after flip. Start all-fail, then flip to
//    zero-fail; verify traffic resumes.
func TestAfeLbSim_BreakerRecoveryAfterFlip(t *testing.T) {
	afes := []simAfe{
		{id: 601, baseLatency: 5 * time.Millisecond, openFailRate: 1.0},
		{id: 602, baseLatency: 5 * time.Millisecond, openFailRate: 1.0},
		{id: 603, baseLatency: 5 * time.Millisecond, openFailRate: 1.0},
	}
	flipped := atomic.Bool{}
	m := runSim(t, simConfig{
		Name: "BreakerRecoveryAfterFlip", Afes: afes,
		PoolMin: 1, PoolMax: 10, Picker: "least-inflight",
		Duration: 8 * time.Second, Workers: 8, Qps: 40,
		CallTimeout:    3 * time.Second,
		StartPoolLoops: true,
		OnMidRun: func(t testing.TB, elapsed time.Duration, s *simServer, p *SessionPoolImpl) {
			if elapsed > 4*time.Second && flipped.CompareAndSwap(false, true) {
				t.Logf("  MID-RUN flip: clearing openFailRate on all AFEs @ %s", elapsed.Round(time.Millisecond))
				s.SetAfeConfig(601, simAfe{id: 601, baseLatency: 5 * time.Millisecond})
				s.SetAfeConfig(602, simAfe{id: 602, baseLatency: 5 * time.Millisecond})
				s.SetAfeConfig(603, simAfe{id: 603, baseLatency: 5 * time.Millisecond})
			}
		},
	})
	m.PrintReport(t, "BreakerRecoveryAfterFlip")
	t.Logf("  ANALYSIS: total OKs should be substantially > 0 after flip; if all requests errored, recovery failed")
}

// 7. QPS ramp up — 10 -> 1000 QPS over 10s.
func TestAfeLbSim_QpsRampUp(t *testing.T) {
	afes := []simAfe{
		{id: 701, baseLatency: 5 * time.Millisecond, jitter: 1 * time.Millisecond},
		{id: 702, baseLatency: 5 * time.Millisecond, jitter: 1 * time.Millisecond},
		{id: 703, baseLatency: 5 * time.Millisecond, jitter: 1 * time.Millisecond},
		{id: 704, baseLatency: 5 * time.Millisecond, jitter: 1 * time.Millisecond},
		{id: 705, baseLatency: 5 * time.Millisecond, jitter: 1 * time.Millisecond},
	}
	m := runSim(t, simConfig{
		Name: "QpsRampUp", Afes: afes,
		PoolMin: 1, PoolMax: 60, Picker: "least-latency", SubsetSize: 2,
		Duration: 10 * time.Second, Workers: 32,
		QpsRamp: []qpsPoint{
			{At: 0 * time.Second, Qps: 10},
			{At: 2 * time.Second, Qps: 100},
			{At: 5 * time.Second, Qps: 500},
			{At: 8 * time.Second, Qps: 1000},
		},
		StartPoolLoops: true,
	})
	m.PrintReport(t, "QpsRampUp")
}

// 8. QPS ramp down — inverse of 7. Session count should NOT actively
//    shrink (passive shrink design).
func TestAfeLbSim_QpsRampDown(t *testing.T) {
	afes := []simAfe{
		{id: 801, baseLatency: 5 * time.Millisecond, jitter: 1 * time.Millisecond},
		{id: 802, baseLatency: 5 * time.Millisecond, jitter: 1 * time.Millisecond},
		{id: 803, baseLatency: 5 * time.Millisecond, jitter: 1 * time.Millisecond},
		{id: 804, baseLatency: 5 * time.Millisecond, jitter: 1 * time.Millisecond},
		{id: 805, baseLatency: 5 * time.Millisecond, jitter: 1 * time.Millisecond},
	}
	m := runSim(t, simConfig{
		Name: "QpsRampDown", Afes: afes,
		PoolMin: 1, PoolMax: 60, Picker: "least-latency", SubsetSize: 2,
		Duration: 10 * time.Second, Workers: 32,
		QpsRamp: []qpsPoint{
			{At: 0 * time.Second, Qps: 1000},
			{At: 3 * time.Second, Qps: 500},
			{At: 6 * time.Second, Qps: 100},
			{At: 8 * time.Second, Qps: 10},
		},
		StartPoolLoops: true,
	})
	m.PrintReport(t, "QpsRampDown")
	t.Logf("  ANALYSIS: sizer.Decide's scale-down branch should be advisory (delta<0 but pool doesn't proactively shrink)")
}

// 9. Sustained max QPS — pool cap = 10, QPS way above capacity. Should
//    show stable queue depth, no oscillation.
func TestAfeLbSim_SustainedMaxQps(t *testing.T) {
	afes := []simAfe{
		{id: 901, baseLatency: 20 * time.Millisecond, jitter: 3 * time.Millisecond},
		{id: 902, baseLatency: 20 * time.Millisecond, jitter: 3 * time.Millisecond},
		{id: 903, baseLatency: 20 * time.Millisecond, jitter: 3 * time.Millisecond},
		{id: 904, baseLatency: 20 * time.Millisecond, jitter: 3 * time.Millisecond},
		{id: 905, baseLatency: 20 * time.Millisecond, jitter: 3 * time.Millisecond},
	}
	m := runSim(t, simConfig{
		Name: "SustainedMaxQps", Afes: afes,
		PoolMin: 1, PoolMax: 10, Picker: "least-inflight", SubsetSize: 2,
		Duration: 8 * time.Second, Workers: 64, Qps: 5000,
		CallTimeout:    2 * time.Second,
		StartPoolLoops: true,
	})
	m.PrintReport(t, "SustainedMaxQps")
	t.Logf("  ANALYSIS: expect a healthy chunk of DeadlineExceeded (saturated pool) but no crash; queue must not grow unboundedly")
}

// 10. Cold-start under load — pool min=0 max=100, hammered from t=0.
//     Reports latency floor + how quickly OKs start arriving.
func TestAfeLbSim_ColdStartUnderLoad(t *testing.T) {
	afes := []simAfe{
		{id: 1001, baseLatency: 10 * time.Millisecond},
		{id: 1002, baseLatency: 10 * time.Millisecond},
		{id: 1003, baseLatency: 10 * time.Millisecond},
	}
	m := runSim(t, simConfig{
		Name: "ColdStartUnderLoad", Afes: afes,
		PoolMin: 0, PoolMax: 100, Picker: "least-inflight",
		Duration: 5 * time.Second, Workers: 32, Qps: 500,
		CallTimeout:    3 * time.Second,
		StartPoolLoops: true,
	})
	m.PrintReport(t, "ColdStartUnderLoad")
	if len(m.latencySamples) > 0 {
		t.Logf("  ANALYSIS: latency p50 should approach ~10ms once sessions warm; a very high p50 means the pool didn't warm inside the run")
	}
}

// Silence unused-import warnings when a helper isn't invoked by every
// scenario (io is used by scenario helpers in the chaos file).
var _ = io.EOF
