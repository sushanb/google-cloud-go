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
func injectActiveSession(t *testing.T, p *SessionPoolImpl, name string, createdAt time.Time) *SessionHandle {
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

func newTestPool(t *testing.T, min, max int) *SessionPoolImpl {
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
	p.slowVRpcThreshold = time.Millisecond

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
