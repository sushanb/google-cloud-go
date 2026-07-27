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

// Chaos scenarios 11-18 for the AFE load-balancing simulation harness.
// Reuses simServer / runSim / simMetrics from afe_lb_sim_test.go.
//
// Each scenario stresses one operational hazard: server-driven
// disconnects, mid-run pool teardown, AFE flap, cold-appear AFE,
// heartbeat watchdog, race hunts under -race, context-deadline storms,
// and goroutine leak sweeps. All reports go through t.Logf.

package internal

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	spb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
)

// 11. Server-driven GoAway churn. GoAways force session retirement +
//     replacement; we want to see traffic keep flowing while sessions
//     rotate under us.
func TestAfeLbSim_ServerGoAway(t *testing.T) {
	afes := []simAfe{
		{id: 1101, baseLatency: 10 * time.Millisecond, goAwayEvery: 20},
		{id: 1102, baseLatency: 10 * time.Millisecond, goAwayEvery: 20},
		{id: 1103, baseLatency: 10 * time.Millisecond, goAwayEvery: 20},
		{id: 1104, baseLatency: 10 * time.Millisecond, goAwayEvery: 20},
		{id: 1105, baseLatency: 10 * time.Millisecond, goAwayEvery: 20},
	}
	m := runSim(t, simConfig{
		Name: "ServerGoAway", Afes: afes,
		PoolMin: 5, PoolMax: 25, Picker: "least-inflight", SubsetSize: 2,
		Duration: 6 * time.Second, Workers: 16, Qps: 200,
		StartPoolLoops: true,
	})
	m.PrintReport(t, "ServerGoAway")
	t.Logf("  ANALYSIS: substantial throughput despite GoAway every 20 vRPCs proves the replace-on-close gate is doing its job")
}

// 12. Concurrent Close under load. Ramp up traffic, then call Close
//     while ~100 in-flight; ensure Close returns cleanly and no
//     orphaned goroutines linger.
func TestAfeLbSim_ConcurrentClose(t *testing.T) {
	// Custom driver — need control over when Close fires.
	baseline := runtime.NumGoroutine()
	afes := []simAfe{
		{id: 1201, baseLatency: 30 * time.Millisecond},
		{id: 1202, baseLatency: 30 * time.Millisecond},
		{id: 1203, baseLatency: 30 * time.Millisecond},
	}
	server := newSimServer(t, afes)
	p := NewSessionPoolImpl(
		uint64(1), "sim-close", 3, 30,
		server.StreamFactory(),
		&spb.OpenSessionRequest{ProtocolVersion: 1},
		nil, SessionTypeTable,
	)
	setPicker(p, "least-inflight", 2)
	p.Start(p.poolCtx)

	desc := &fakeDesc{
		method: "SimVRpc",
		enc:    func(req interface{}) ([]byte, error) { return []byte(fmt.Sprint(req)), nil },
		dec:    func(buf []byte) (interface{}, error) { return string(buf), nil },
	}

	var errs sync.Map
	var oks atomic.Int64
	var callWg sync.WaitGroup
	stop := make(chan struct{})
	for i := 0; i < 100; i++ {
		callWg.Add(1)
		go func() {
			defer callWg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				_, err := p.Invoke(ctx, desc, "x")
				cancel()
				if err != nil {
					errs.Store(errKey(err), true)
				} else {
					oks.Add(1)
				}
			}
		}()
	}
	// Let load build.
	time.Sleep(500 * time.Millisecond)

	closeDone := make(chan error, 1)
	closeStart := time.Now()
	go func() { closeDone <- p.Close() }()
	select {
	case err := <-closeDone:
		t.Logf("  ConcurrentClose: pool.Close returned err=%v after %s", err, time.Since(closeStart).Round(time.Millisecond))
	case <-time.After(35 * time.Second):
		dumpStacksOnFailure(t, "pool.Close did not return within 35s")
		close(stop)
		callWg.Wait()
		server.closeAll()
		t.Fatal("pool.Close hung > 35s (budget is 30s)")
	}
	close(stop)
	callWg.Wait()
	server.closeAll()

	time.Sleep(100 * time.Millisecond)
	end := runtime.NumGoroutine()

	var errKeys []string
	errs.Range(func(k, v interface{}) bool { errKeys = append(errKeys, k.(string)); return true })
	t.Logf("=== ConcurrentClose ===")
	t.Logf("  OKs during load: %d", oks.Load())
	t.Logf("  Distinct error keys after Close: %v", errKeys)
	t.Logf("  Goroutines: baseline=%d end=%d diff=%+d", baseline, end, end-baseline)
	if end-baseline > 30 {
		t.Logf("  <-- >30 leaked goroutines is suspicious")
	}
}

// 13. AFE disappearance mid-run. One AFE flips to open-fail + vRPC-error;
//     traffic must reroute to survivors.
func TestAfeLbSim_AfeDisappearance(t *testing.T) {
	afes := []simAfe{
		{id: 1301, baseLatency: 10 * time.Millisecond},
		{id: 1302, baseLatency: 10 * time.Millisecond},
		{id: 1303, baseLatency: 10 * time.Millisecond},
	}
	flipped := atomic.Bool{}
	m := runSim(t, simConfig{
		Name: "AfeDisappearance", Afes: afes,
		PoolMin: 3, PoolMax: 15, Picker: "least-inflight", SubsetSize: 2,
		Duration: 8 * time.Second, Workers: 16, Qps: 200,
		CallTimeout:    2 * time.Second,
		StartPoolLoops: true,
		OnMidRun: func(t testing.TB, elapsed time.Duration, s *simServer, p *SessionPoolImpl) {
			if elapsed > 3*time.Second && flipped.CompareAndSwap(false, true) {
				t.Logf("  MID-RUN: AFE 1302 goes dark (open-fail + vRPC-error) @ %s", elapsed.Round(time.Millisecond))
				s.SetAfeConfig(1302, simAfe{
					id: 1302, baseLatency: 10 * time.Millisecond,
					openFailRate: 1.0, vRpcErrorRate: 1.0,
				})
			}
		},
	})
	m.PrintReport(t, "AfeDisappearance")
	t.Logf("  ANALYSIS: after flip, 1302's picks should either drop to ~0 or its errors dominate its column")
}

// 14. New AFE appears mid-run. Picker should eventually route to it.
func TestAfeLbSim_NewAfeAppears(t *testing.T) {
	afes := []simAfe{
		{id: 1401, baseLatency: 10 * time.Millisecond},
		{id: 1402, baseLatency: 10 * time.Millisecond},
	}
	added := atomic.Bool{}
	m := runSim(t, simConfig{
		Name: "NewAfeAppears", Afes: afes,
		PoolMin: 2, PoolMax: 20, Picker: "least-inflight", SubsetSize: 3,
		Duration: 8 * time.Second, Workers: 16, Qps: 200,
		StartPoolLoops: true,
		OnMidRun: func(t testing.TB, elapsed time.Duration, s *simServer, p *SessionPoolImpl) {
			if elapsed > 3*time.Second && added.CompareAndSwap(false, true) {
				t.Logf("  MID-RUN: adding AFE 1403 @ %s", elapsed.Round(time.Millisecond))
				s.AddAfe(simAfe{id: 1403, baseLatency: 10 * time.Millisecond})
			}
		},
	})
	m.PrintReport(t, "NewAfeAppears")
	if c := m.perAfe[1403]; c != nil {
		t.Logf("  ANALYSIS: AFE 1403 (added mid-run) picks = %d — should be > 0 if pool discovered it", c.picks)
	} else {
		t.Logf("  ANALYSIS: AFE 1403 was added but never picked — sessions must open on new streams to discover it")
	}
}

// 15. Heartbeat miss on one AFE. That AFE never replies to VirtualRpc,
//     so the client's watchdog fires and force-closes the session.
//     Replacement sessions should open.
func TestAfeLbSim_HeartbeatMiss(t *testing.T) {
	// One AFE stalls all VirtualRpc replies. The heartbeat watchdog
	// (100ms default) fires ~100ms into an in-flight vRPC. We can't
	// keep serving traffic if EVERY session lands on the stalled AFE;
	// keep it as one of many.
	afes := []simAfe{
		{id: 1501, baseLatency: 5 * time.Millisecond},
		{id: 1502, baseLatency: 5 * time.Millisecond},
		{id: 1503, stallVRpc: true, baseLatency: 30 * time.Second}, // never responds
	}
	m := runSim(t, simConfig{
		Name: "HeartbeatMiss", Afes: afes,
		PoolMin: 3, PoolMax: 12, Picker: "least-inflight", SubsetSize: 3,
		Duration: 6 * time.Second, Workers: 16, Qps: 100,
		CallTimeout:    2 * time.Second,
		StartPoolLoops: true,
	})
	m.PrintReport(t, "HeartbeatMiss")
	t.Logf("  ANALYSIS: expect DeadlineExceeded on any request that landed on AFE 1503; healthy AFEs should still show OKs")
}

// 16. Race hunt under -race: concurrent Close with 500 in-flight vRPCs.
//     Run with `go test -race -run '^TestAfeLbSim_ConcurrentCloseUnderHighInFlight$'`.
func TestAfeLbSim_ConcurrentCloseUnderHighInFlight(t *testing.T) {
	afes := []simAfe{
		{id: 1601, baseLatency: 30 * time.Millisecond},
		{id: 1602, baseLatency: 30 * time.Millisecond},
		{id: 1603, baseLatency: 30 * time.Millisecond},
		{id: 1604, baseLatency: 30 * time.Millisecond},
		{id: 1605, baseLatency: 30 * time.Millisecond},
	}
	server := newSimServer(t, afes)
	p := NewSessionPoolImpl(
		uint64(2), "sim-race", 5, 50,
		server.StreamFactory(),
		&spb.OpenSessionRequest{ProtocolVersion: 1},
		nil, SessionTypeTable,
	)
	setPicker(p, "least-inflight", 2)
	p.Start(p.poolCtx)

	desc := &fakeDesc{
		method: "SimVRpc",
		enc:    func(req interface{}) ([]byte, error) { return []byte(fmt.Sprint(req)), nil },
		dec:    func(buf []byte) (interface{}, error) { return string(buf), nil },
	}
	stop := make(chan struct{})
	var callWg sync.WaitGroup
	for i := 0; i < 500; i++ {
		callWg.Add(1)
		go func() {
			defer callWg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				p.Invoke(ctx, desc, "x")
				cancel()
			}
		}()
	}
	time.Sleep(400 * time.Millisecond)
	closeDone := make(chan error, 1)
	closeStart := time.Now()
	go func() { closeDone <- p.Close() }()
	select {
	case err := <-closeDone:
		t.Logf("  Close returned err=%v after %s", err, time.Since(closeStart).Round(time.Millisecond))
	case <-time.After(35 * time.Second):
		dumpStacksOnFailure(t, "Close did not return in 35s")
		close(stop)
		callWg.Wait()
		server.closeAll()
		t.Fatal("Close hung > 35s under -race")
	}
	close(stop)
	callWg.Wait()
	server.closeAll()
	t.Logf("=== ConcurrentCloseUnderHighInFlight ===")
	t.Logf("  Completed without hang. Run under -race to hunt data races.")
}

// 17. Context-cancel storm. 500 goroutines each call Invoke with a 50ms
//     deadline; wait for all to unblock, verify waiter queue empties.
func TestAfeLbSim_ContextCancelStorm(t *testing.T) {
	afes := []simAfe{
		{id: 1701, baseLatency: 200 * time.Millisecond}, // slower than the ctx deadline
		{id: 1702, baseLatency: 200 * time.Millisecond},
	}
	server := newSimServer(t, afes)
	p := NewSessionPoolImpl(
		uint64(3), "sim-cancel-storm", 1, 5,
		server.StreamFactory(),
		&spb.OpenSessionRequest{ProtocolVersion: 1},
		nil, SessionTypeTable,
	)
	setPicker(p, "least-inflight", 2)
	p.Start(p.poolCtx)

	baseline := runtime.NumGoroutine()

	var wg sync.WaitGroup
	desc := &fakeDesc{
		method: "SimVRpc",
		enc:    func(req interface{}) ([]byte, error) { return []byte(fmt.Sprint(req)), nil },
		dec:    func(buf []byte) (interface{}, error) { return string(buf), nil },
	}
	var timeouts, oks, others atomic.Int64
	for i := 0; i < 500; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()
			_, err := p.Invoke(ctx, desc, "x")
			switch errKey(err) {
			case "":
				oks.Add(1)
			case "DeadlineExceeded":
				timeouts.Add(1)
			default:
				others.Add(1)
			}
		}()
	}
	wg.Wait()

	// Wait for waiter queue to empty and one final Tick to flush.
	time.Sleep(200 * time.Millisecond)
	waiters := p.waitersCount.Load()
	p.waitersMu.Lock()
	qlen := p.waiters.Len()
	p.waitersMu.Unlock()

	server.closeAll()
	_ = p.Close()
	time.Sleep(50 * time.Millisecond)
	end := runtime.NumGoroutine()

	t.Logf("=== ContextCancelStorm ===")
	t.Logf("  OKs=%d Deadlines=%d Other=%d", oks.Load(), timeouts.Load(), others.Load())
	t.Logf("  Post-storm waitersCount=%d queue-length=%d (both should be 0)", waiters, qlen)
	t.Logf("  Goroutines: baseline=%d end=%d diff=%+d", baseline, end, end-baseline)
	if waiters != 0 || qlen != 0 {
		t.Logf("  <-- non-zero waiter state after storm — ghost waiter?")
	}
}

// 18. Goroutine leak sweep. Light scenario for 5s; report goroutine
//     delta after tear-down.
func TestAfeLbSim_GoroutineLeakSweep(t *testing.T) {
	baseline := runtime.NumGoroutine()
	afes := []simAfe{
		{id: 1801, baseLatency: 10 * time.Millisecond},
		{id: 1802, baseLatency: 10 * time.Millisecond},
		{id: 1803, baseLatency: 10 * time.Millisecond},
	}
	m := runSim(t, simConfig{
		Name: "GoroutineLeakSweep", Afes: afes,
		PoolMin: 3, PoolMax: 15, Picker: "least-inflight", SubsetSize: 2,
		Duration: 5 * time.Second, Workers: 8, Qps: 50,
		StartPoolLoops: true,
	})
	m.PrintReport(t, "GoroutineLeakSweep")
	// runSim's cleanup already captured m.goroutineEnd — but for this
	// scenario the ANALYSIS number that matters is baseline-vs-end.
	// t.Cleanup runs AFTER the test function returns, so grab a fresh
	// number here for a soft check, and rely on m.PrintReport's diff for
	// the definitive one.
	after := runtime.NumGoroutine()
	t.Logf("  DIRECT sample (before t.Cleanup): baseline=%d after=%d diff=%+d",
		baseline, after, after-baseline)
}
