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

package internal

import (
	"context"
	"testing"
	"time"

	spb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
)

// stubPoolStreamFactory returns a streamFactory that never gets dialled — the
// tests inject sessions manually so the factory should not run. If it does,
// the test should see the error.
func stubPoolStreamFactory(_ context.Context) (Stream, error) {
	return newFakeStream(), nil
}

// injectActiveSession builds a fakeStream-backed Session in StateReady,
// wraps it in a SessionHandle, and pushes it into pool.sessions (registering
// the createdAt time). This bypasses the real Start/handshake path so tests
// can exercise pool-level logic in milliseconds.
func injectActiveSession(t testing.TB, p *SessionPoolImpl, name string, createdAt time.Time) *SessionHandle {
	t.Helper()
	stream := newFakeStream()
	s := NewSession(name, stream, SessionHooks{
		OnStart:  p.OnStart,
		OnActive: p.OnActive,
		OnClose:  p.OnClose,
	}, SessionTypeTable)
	s.state.Store(int32(StateReady))

	sh := NewSessionHandle(s)
	p.mu.Lock()
	p.sessions = append(p.sessions, sh)
	p.sessionCreatedAt[sh] = createdAt
	p.mu.Unlock()
	// Register in the AFE-aware sessionList so CheckoutSession's two-tier
	// pick can find it. injectActiveSession skips the handshake so
	// PeerInfo stays nil — the handle lands in the AfeID=0 bucket, which
	// is fine for pool-level tests that don't care about AFE fanout.
	p.sl.OnSessionStarted(sh)
	return sh
}

func newTestPool(t testing.TB, min, max int) *SessionPoolImpl {
	t.Helper()
	return NewSessionPoolImpl(
		"test-pool",
		min,
		max,
		stubPoolStreamFactory,
		&spb.OpenSessionRequest{ProtocolVersion: 1},
		nil,
		SessionTypeTable,
	)
}

// TestSessionPool_Close_CompletesWithIdleSessions verifies that Close()
// returns promptly when sessions have nothing in flight, well within the 30s
// internal budget, and that poolCtx is cancelled after.
func TestSessionPool_Close_CompletesWithIdleSessions(t *testing.T) {
	p := newTestPool(t, 1, 10)

	// Three idle sessions registered with the pool. None have in-flight
	// VRPCs, so Session.Close should flip them to Closing and signal
	// quiescent immediately, letting the pool drain fast.
	for i := 0; i < 3; i++ {
		injectActiveSession(t, p, "idle", time.Now())
	}

	done := make(chan error, 1)
	go func() { done <- p.Close() }()

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Close returned err = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not return within 5s for idle sessions (budget is 30s)")
	}

	// poolCtx must be cancelled after Close.
	select {
	case <-p.poolCtx.Done():
	default:
		t.Error("poolCtx not cancelled after Close()")
	}
}

// TestSessionPool_Close_BoundedByTimeout is skipped: constructing a stuck
// session that ignores Close() requires real readLoop/Send plumbing that
// blocks unrecoverably, which is hard to do safely in a unit test. The 30s
// timeout path is exercised in integration tests.
func TestSessionPool_Close_BoundedByTimeout(t *testing.T) {
	t.Skip("requires a session that ignores Close — see integration tests")
}

// TestPerformScaling_NoLongerPrunesOverprovisioned verifies the passive-
// shrink design: PerformScaling with a scale-down delta must be a no-op
// — the pool shrinks only via OnClose's replace-on-death gate, not via a
// periodic prune. Regression guard against re-introducing the burst-then-
// lull oscillation that the earlier active-scale-down design produced.
func TestPerformScaling_NoLongerPrunesOverprovisioned(t *testing.T) {
	p := newTestPool(t, 1, 20)
	// 10 idle sessions, none in-flight → sizer will compute
	// desired ≈ minSessions (5-ish) and delta will be negative.
	// PerformScaling must observe the negative delta and NOT shrink
	// the pool — regression guard for the removed active-scale-down.
	for i := 0; i < 10; i++ {
		sh := injectActiveSession(t, p, "idle", time.Now().Add(-time.Hour))
		_ = sh
	}

	before := len(p.sessions)
	p.PerformScaling(context.Background())

	p.mu.Lock()
	after := len(p.sessions)
	p.mu.Unlock()

	if after != before {
		t.Errorf("PerformScaling pruned sessions: before=%d after=%d — scale-down must be advisory, not proactive", before, after)
	}
}

// TestSizer_ScaleDownIsAdvisory confirms the sizer still RETURNS a
// negative delta on overprovision (so ScalingHistory / callers can see
// the intent) but the calling site (PerformScaling) must not act on it.
// The paired assertion above proves the caller is well-behaved; this one
// pins the sizer contract.
func TestSizer_ScaleDownIsAdvisory(t *testing.T) {
	// InUse=1, Pending=0, Ready=10 → desired ≈ 2, immediate=10.
	// Expect delta = 2 - 10 = -8 with Branch = "scale-down".
	stats := &PoolStats{ReadyCount: 10, InUseCount: 1}
	sizer := NewPoolSizer(func() *PoolStats { return stats }, 1, 20, 0.5)
	d := sizer.Decide()
	if d.Branch != "scale-down" {
		t.Fatalf("Branch = %q, want scale-down", d.Branch)
	}
	if d.Delta >= 0 {
		t.Errorf("Delta = %d, want negative (advisory scale-down)", d.Delta)
	}
}

// TestSessionPool_Invoke_RecordsSlowCheckoutFailure pins the fix for the
// pool-exhaustion blind spot: if CheckoutSession errors out (typically ctx
// deadline while parked waiting on freeSignal), Invoke must still push a
// row into the slow-vRPC ring — otherwise the exact incident an operator
// opens sessionz to debug ("pool saturated, everything timing out") leaves
// no evidence in the debug UI.
func TestSessionPool_Invoke_RecordsSlowCheckoutFailure(t *testing.T) {
	// Dial that blocks until its own ctx (poolCtx, wrapped) fires. No
	// session ever lands, so CheckoutSession is forced to park on
	// freeSignal until the caller's ctx cancels.
	neverDialing := func(ctx context.Context) (Stream, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	p := NewSessionPoolImpl(
		"test-pool",
		0, 1,
		neverDialing,
		&spb.OpenSessionRequest{ProtocolVersion: 1},
		nil,
		SessionTypeTable,
	)
	defer p.Close()

	// Threshold well under the 50ms wait below so the record path fires
	// deterministically even on slow CI.
	p.m.slowVRpcThreshold = time.Millisecond

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := p.Invoke(ctx, newRoundTripDesc(), "hello")
	if err == nil {
		t.Fatal("Invoke on a pool that cannot dial succeeded; want ctx deadline error")
	}

	events := p.snapshotSlowVRpcs()
	if len(events) != 1 {
		t.Fatalf("snapshotSlowVRpcs len = %d, want 1 — checkout failure was not recorded", len(events))
	}
	ev := events[0]
	if ev.Method != "RoundTrip" {
		t.Errorf("Method = %q, want RoundTrip", ev.Method)
	}
	if ev.Success {
		t.Error("Success = true, want false (checkout failed)")
	}
	if ev.PoolWait <= 0 {
		t.Errorf("PoolWait = %v, want > 0", ev.PoolWait)
	}
	if ev.Latency != ev.PoolWait {
		t.Errorf("Latency = %v, PoolWait = %v — must match when all time was in checkout", ev.Latency, ev.PoolWait)
	}
	if ev.Session != "" {
		t.Errorf("Session = %q, want empty (no handle was ever returned)", ev.Session)
	}
	if ev.ErrCode != "DeadlineExceeded" {
		t.Errorf("ErrCode = %q, want DeadlineExceeded", ev.ErrCode)
	}
}

// TestRecordPickDecision_RingWrap verifies the O(1) circular-buffer
// implementation of pickHistory: pre-wrap events are in insertion order,
// post-wrap events preserve oldest-first ordering, and the ring keeps
// exactly maxPickHistory entries. Regression guard against the previous
// shift-based implementation that memmoved the whole buffer per record
// (~24µs p99 CheckoutSession regression at moderate QPS).
func TestRecordPickDecision_RingWrap(t *testing.T) {
	p := newTestPool(t, 1, 10)

	// Phase 1: fill up to cap-1 → snapshot should be insertion-ordered.
	for i := 0; i < maxPickHistory-1; i++ {
		p.recordPickDecision(PickDecision{Reason: "phase1", Winner: afeID(i + 1)}, "test")
	}
	snap := p.snapshotPickHistory()
	if len(snap) != maxPickHistory-1 {
		t.Fatalf("pre-wrap snapshot len = %d, want %d", len(snap), maxPickHistory-1)
	}
	if snap[0].Decision.Winner != afeID(1) || snap[len(snap)-1].Decision.Winner != afeID(maxPickHistory-1) {
		t.Errorf("pre-wrap ordering broken: first=%d last=%d",
			snap[0].Decision.Winner, snap[len(snap)-1].Decision.Winner)
	}

	// Phase 2: overshoot cap by 100 → ring wraps.
	for i := maxPickHistory - 1; i < maxPickHistory+100; i++ {
		p.recordPickDecision(PickDecision{Reason: "phase2", Winner: afeID(i + 1)}, "test")
	}
	snap = p.snapshotPickHistory()
	if len(snap) != maxPickHistory {
		t.Fatalf("post-wrap snapshot len = %d, want %d (ring must cap)", len(snap), maxPickHistory)
	}
	// After 100 overshoots, the oldest surviving Winner is 101; newest is
	// maxPickHistory+100.
	wantOldest := afeID(101)
	wantNewest := afeID(maxPickHistory + 100)
	if snap[0].Decision.Winner != wantOldest {
		t.Errorf("post-wrap oldest = %d, want %d", snap[0].Decision.Winner, wantOldest)
	}
	if snap[len(snap)-1].Decision.Winner != wantNewest {
		t.Errorf("post-wrap newest = %d, want %d", snap[len(snap)-1].Decision.Winner, wantNewest)
	}
	// Ordering must be monotonic (oldest-first).
	for i := 1; i < len(snap); i++ {
		if snap[i].Decision.Winner <= snap[i-1].Decision.Winner {
			t.Fatalf("ordering broken at i=%d: snap[i-1].Winner=%d snap[i].Winner=%d",
				i, snap[i-1].Decision.Winner, snap[i].Decision.Winner)
		}
	}
}

// --- core pool setters + hot-path helpers (session_pool.go) ----------------

func TestSetPoolID(t *testing.T) {
	p := newTestPool(t, 1, 10)
	if p.poolID != 0 {
		t.Errorf("initial poolID = %d, want 0", p.poolID)
	}
	p.SetPoolID(42)
	if p.poolID != 42 {
		t.Errorf("after SetPoolID(42), poolID = %d, want 42", p.poolID)
	}
}

func TestSetPoolShortName_FlattensSlashes(t *testing.T) {
	p := newTestPool(t, 1, 10)
	p.SetPoolShortName("projects/p/instances/i/tables/t")
	want := "projects_p_instances_i_tables_t"
	if p.poolShortName != want {
		t.Errorf("poolShortName = %q, want %q (slashes must flatten to underscores)", p.poolShortName, want)
	}
}

func TestSignalFree_NonBlockingWhenBufferFull(t *testing.T) {
	p := newTestPool(t, 1, 10)
	// Drain first so we know starting state.
	select {
	case <-p.freeSignal:
	default:
	}
	// First signal fills the cap-1 buffer.
	p.signalFree()
	// Second signal must be dropped, not block. Race the call against a
	// timeout — signalFree not returning within a beat is the failure.
	done := make(chan struct{})
	go func() {
		p.signalFree()
		close(done)
	}()
	select {
	case <-done:
		// expected
	case <-time.After(100 * time.Millisecond):
		t.Fatal("signalFree blocked with buffer full; should drop silently")
	}
	// Buffer should still hold exactly one wake-up.
	select {
	case <-p.freeSignal:
	default:
		t.Error("no wake-up available after two signalFree calls; first was lost")
	}
	select {
	case <-p.freeSignal:
		t.Error("second wake-up was somehow queued; buffer should have collapsed the duplicate")
	default:
	}
}

func TestStats_CountsReadyInUsePending(t *testing.T) {
	p := newTestPool(t, 1, 10)
	// Two ready sessions; one has outstanding=1 (in-use), other is idle.
	sh1 := injectActiveSession(t, p, "s1", time.Now())
	sh2 := injectActiveSession(t, p, "s2", time.Now())
	sh1.IncOutstanding()
	_ = sh2 // idle

	// Fake three parked waiters via waitersCount (the real path is inside
	// CheckoutSession's select; we bracket it directly here).
	p.waitersCount.Add(3)

	st := p.Stats()
	if st.ReadyCount != 2 {
		t.Errorf("ReadyCount = %d, want 2", st.ReadyCount)
	}
	if st.InUseCount != 1 {
		t.Errorf("InUseCount = %d, want 1", st.InUseCount)
	}
	if st.PendingCount != 3 {
		t.Errorf("PendingCount = %d, want 3 (must be waitersCount, not sum of outstanding)", st.PendingCount)
	}
	if st.StartingCount != 0 {
		t.Errorf("StartingCount = %d, want 0", st.StartingCount)
	}
}

func TestUpdateConfig_SwapsPickerAndBounds(t *testing.T) {
	p := newTestPool(t, 1, 10)
	// Default picker is LeastInFlight.
	if got := p.picker.Name(); got != "least-inflight" {
		t.Errorf("default picker = %q, want least-inflight", got)
	}

	// Switch to Random.
	p.UpdateConfig(&spb.SessionClientConfiguration_SessionPoolConfiguration{
		MinSessionCount: 4,
		MaxSessionCount: 40,
		LoadBalancingOptions: &spb.LoadBalancingOptions{
			LoadBalancingStrategy: &spb.LoadBalancingOptions_Random_{
				Random: &spb.LoadBalancingOptions_Random{},
			},
		},
	})
	if got := p.picker.Name(); got != "simple" {
		t.Errorf("after Random swap, picker = %q, want simple", got)
	}
	if p.minSessions != 4 || p.maxSessions != 40 {
		t.Errorf("min/max = %d/%d, want 4/40", p.minSessions, p.maxSessions)
	}

	// Switch to PeakEwma with an explicit subset size.
	p.UpdateConfig(&spb.SessionClientConfiguration_SessionPoolConfiguration{
		MinSessionCount: 1,
		MaxSessionCount: 10,
		LoadBalancingOptions: &spb.LoadBalancingOptions{
			LoadBalancingStrategy: &spb.LoadBalancingOptions_PeakEwma_{
				PeakEwma: &spb.LoadBalancingOptions_PeakEwma{RandomSubsetSize: 3},
			},
		},
	})
	if got := p.picker.Name(); got != "least-latency" {
		t.Errorf("after PeakEwma swap, picker = %q, want least-latency", got)
	}
	// listenerFires counter bumps once per UpdateConfig.
	if got := p.m.listenerFires.Load(); got != 2 {
		t.Errorf("listenerFires = %d, want 2 (one per UpdateConfig)", got)
	}
}

// TestUpdateConfig_HonorsRandomSubsetSize is a regression guard against the
// bug where UpdateConfig's LeastInFlight branch ignored its own
// RandomSubsetSize (only PeakEwma read it). Server-driven K must reach the
// picker for both K-choice strategies.
func TestUpdateConfig_HonorsRandomSubsetSize(t *testing.T) {
	cases := []struct {
		name string
		lbo  *spb.LoadBalancingOptions
		want int
	}{
		{
			name: "LeastInFlight with K=5",
			lbo: &spb.LoadBalancingOptions{
				LoadBalancingStrategy: &spb.LoadBalancingOptions_LeastInFlight_{
					LeastInFlight: &spb.LoadBalancingOptions_LeastInFlight{RandomSubsetSize: 5},
				},
			},
			want: 5,
		},
		{
			name: "PeakEwma with K=7",
			lbo: &spb.LoadBalancingOptions{
				LoadBalancingStrategy: &spb.LoadBalancingOptions_PeakEwma_{
					PeakEwma: &spb.LoadBalancingOptions_PeakEwma{RandomSubsetSize: 7},
				},
			},
			want: 7,
		},
		{
			name: "LeastInFlight with omitted K falls back to default",
			lbo: &spb.LoadBalancingOptions{
				LoadBalancingStrategy: &spb.LoadBalancingOptions_LeastInFlight_{
					LeastInFlight: &spb.LoadBalancingOptions_LeastInFlight{}, // K=0 → fallback
				},
			},
			want: defaultAfeRandomSubsetSize,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := newTestPool(t, 1, 10)
			p.UpdateConfig(&spb.SessionClientConfiguration_SessionPoolConfiguration{
				MinSessionCount:      1,
				MaxSessionCount:      10,
				LoadBalancingOptions: tc.lbo,
			})
			var gotK int
			switch pk := p.picker.(type) {
			case *LeastInFlightAfePicker:
				gotK = pk.RandomSubsetSize
			case *LeastLatencyAfePicker:
				gotK = pk.RandomSubsetSize
			default:
				t.Fatalf("unexpected picker type %T", p.picker)
			}
			if gotK != tc.want {
				t.Errorf("picker RandomSubsetSize = %d, want %d", gotK, tc.want)
			}
		})
	}
}

// TestPickerFromLoadBalancing_NilFallback verifies the constructor's
// bootstrap path — a nil LoadBalancingOptions gives Java's default
// (LeastInFlight with K=defaultAfeRandomSubsetSize).
func TestPickerFromLoadBalancing_NilFallback(t *testing.T) {
	picker := pickerFromLoadBalancing(nil)
	li, ok := picker.(*LeastInFlightAfePicker)
	if !ok {
		t.Fatalf("nil LBO → %T, want *LeastInFlightAfePicker", picker)
	}
	if li.RandomSubsetSize != defaultAfeRandomSubsetSize {
		t.Errorf("K = %d, want %d", li.RandomSubsetSize, defaultAfeRandomSubsetSize)
	}
}

func TestPruneDeadLocked_RemovesFromSessionsAndSL(t *testing.T) {
	p := newTestPool(t, 1, 10)
	sh := injectActiveSession(t, p, "s1", time.Now().Add(-time.Minute))
	// Flip the session's state to something other than Ready so
	// pruneDeadLocked treats it as dead.
	sh.session.state.Store(int32(StateClosed))

	p.mu.Lock()
	p.pruneDeadLocked([]*SessionHandle{sh})
	p.mu.Unlock()

	p.mu.Lock()
	stillIn := false
	for _, cur := range p.sessions {
		if cur == sh {
			stillIn = true
		}
	}
	p.mu.Unlock()
	if stillIn {
		t.Error("pruned session still present in p.sessions")
	}
	if got := p.snapshotCloseReasons()["DeadOnPick"]; got != 1 {
		t.Errorf("DeadOnPick count = %d, want 1", got)
	}
	if got := len(p.snapshotLifetimes()); got != 1 {
		t.Errorf("lifetimes len = %d, want 1 (createdAt was set)", got)
	}
}
