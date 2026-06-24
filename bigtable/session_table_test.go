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

package bigtable

import (
	"context"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	btpb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
	btransport "cloud.google.com/go/bigtable/internal/transport"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

// fakeInvoker is a hand-rolled stub for the Invoker interface. It
// counts invocations and returns a configurable InvokeResult / err pair. A
// capture hook lets tests inspect the per-attempt context (for metrics-tracer
// assertions). Stats / sentAt are exposed so tests can exercise the
// session_table.go branches that pull serverLatency from Stats and
// clientBlockingLatency from (SentAt - attemptStart).
type fakeInvoker struct {
	mu          sync.Mutex
	calls       int32
	resp        interface{}
	clusterInfo *btpb.ClusterInformation
	stats       *btpb.SessionRequestStats
	sentAt      time.Time
	err         error
	// onCall, if non-nil, runs synchronously before the canned response is
	// returned. Useful for capturing the per-attempt ctx (and for stamping
	// per-attempt fields on the metrics tracer prior to the baseHandler's
	// post-Execute writes).
	onCall func(ctx context.Context)
}

func (f *fakeInvoker) Invoke(ctx context.Context, desc btransport.VRpcDescriptor, req interface{}) (btransport.InvokeResult, error) {
	atomic.AddInt32(&f.calls, 1)
	f.mu.Lock()
	hook := f.onCall
	f.mu.Unlock()
	if hook != nil {
		hook(ctx)
	}
	return btransport.InvokeResult{
		Response:    f.resp,
		ClusterInfo: f.clusterInfo,
		Stats:       f.stats,
		SentAt:      f.sentAt,
	}, f.err
}

func (f *fakeInvoker) callCount() int32 {
	return atomic.LoadInt32(&f.calls)
}

// newSessionTestTable returns a SessionTable wired up with the provided
// read/write pool fakes and a minimal *Table backing object. It uses a
// disabled metrics tracer factory so SessionTable's contextWithMetricsTracer
// path stays exercised but doesn't try to emit OTel metrics.
func newSessionTestTable(t *testing.T, readPool, writePool Invoker) *SessionTable {
	t.Helper()
	factory := &builtinMetricsTracerFactory{enabled: false, shutdown: func() {}}
	metricsFactory := func(ctx context.Context, isStreaming bool) *builtinMetricsTracer {
		mt := factory.createBuiltinMetricsTracer(ctx, "t", isStreaming)
		return &mt
	}
	st := &SessionTable{
		tableName:      "projects/P/instances/I/tables/t",
		md:             nil,
		metricsFactory: metricsFactory,
		readPool:       readPool,
		writePool:      writePool,
		readVRpcDesc:   btransport.READ_ROW,
		writeVRpcDesc:  btransport.MUTATE_ROW,
	}
	return st
}

// --- Apply: retry-idempotency ------------------------------------------------

// sessionUnavailableErr mirrors the unexported sessionErr produced by the
// transport's unavailable() helper: a codes.Unavailable status wrapping a
// sentinel cause. Tests use it to exercise the Apply carve-out that allows
// retries on errors which prove the request never reached the server, even
// for non-idempotent mutations.
type sessionUnavailableErr struct {
	sentinel error
	msg      string
}

func (e *sessionUnavailableErr) Error() string              { return e.msg }
func (e *sessionUnavailableErr) Unwrap() error              { return e.sentinel }
func (e *sessionUnavailableErr) GRPCStatus() *status.Status { return status.New(codes.Unavailable, e.msg) }

func TestSessionTable_Apply_ServerTimeMutationNotRetried(t *testing.T) {
	// Generic Unavailable (no safe-sentinel cause) must bubble up after a
	// single attempt for non-idempotent mutations — retrying could create
	// duplicate cells with different server-assigned timestamps.
	writePool := &fakeInvoker{
		err: status.Error(codes.Unavailable, "transient"),
	}
	st := newSessionTestTable(t, nil, writePool)

	m := NewMutation()
	m.Set("fam", "col", ServerTime, []byte{1})

	err := st.Apply(context.Background(), "row1", m)
	if err == nil {
		t.Fatal("Apply with ServerTime mutation succeeded; want Unavailable error to bubble up")
	}
	if got := writePool.callCount(); got != 1 {
		t.Errorf("Invoke called %d times; want exactly 1 (ServerTime mutations are non-idempotent and must not retry on generic Unavailable)", got)
	}
}

// TestSessionTable_Apply_ServerTimeMutationRetriesOnSafeSentinels pins the
// carve-out: ServerTime (non-idempotent) mutations must still retry when the
// error chain identifies one of the two sentinels that prove the request
// never executed on the server. Without this, transient session lifecycle
// events (state-not-active short-circuit, server GOAWAY) cause spurious
// write failures even though re-sending is provably safe.
func TestSessionTable_Apply_ServerTimeMutationRetriesOnSafeSentinels(t *testing.T) {
	cases := []struct {
		name     string
		sentinel error
	}{
		{"ErrSessionNotActive", btransport.ErrSessionNotActive},
		{"ErrUnavailableGoAway", btransport.ErrUnavailableGoAway},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			writePool := &fakeInvoker{
				err: &sessionUnavailableErr{sentinel: tc.sentinel, msg: "session sentinel: " + tc.name},
			}
			st := newSessionTestTable(t, nil, writePool)

			m := NewMutation()
			m.Set("fam", "col", ServerTime, []byte{1})

			err := st.Apply(context.Background(), "row1", m)
			if err == nil {
				t.Fatalf("Apply succeeded; want error after retries exhausted")
			}
			const wantAttempts = 10
			if got := writePool.callCount(); got != wantAttempts {
				t.Errorf("Invoke called %d times; want %d (ServerTime mutation must retry %s up to MaxAttempts)", got, wantAttempts, tc.name)
			}
		})
	}
}

func TestSessionTable_Apply_TimestampedMutationRetries(t *testing.T) {
	writePool := &fakeInvoker{
		err: status.Error(codes.Unavailable, "transient"),
	}
	st := newSessionTestTable(t, nil, writePool)

	m := NewMutation()
	m.Set("fam", "col", Timestamp(1234567890000000), []byte{1})

	err := st.Apply(context.Background(), "row1", m)
	if err == nil {
		t.Fatal("Apply succeeded; want Unavailable error after retries exhausted")
	}
	const wantAttempts = 10
	if got := writePool.callCount(); got != wantAttempts {
		t.Errorf("Invoke called %d times; want %d (MaxAttempts for idempotent mutations)", got, wantAttempts)
	}
}

func TestSessionTable_Apply_NilWritePoolReturnsError(t *testing.T) {
	// readPool present, writePool nil — Apply must not panic and must surface
	// a write-not-supported error.
	readPool := &fakeInvoker{}
	st := newSessionTestTable(t, readPool, nil)

	m := NewMutation()
	m.Set("fam", "col", Timestamp(1234567890000000), []byte{1})

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("Apply panicked with nil writePool: %v", r)
		}
	}()
	err := st.Apply(context.Background(), "row1", m)
	if err == nil {
		t.Fatal("Apply with nil writePool returned nil err; want a 'write operations not supported' error")
	}
	// Match the specific message Agent B chose.
	if msg := err.Error(); !strings.Contains(msg, "write operations not supported") {
		t.Errorf("error message = %q; want a 'write operations not supported' phrasing", msg)
	}
}

// --- ReadRow: cluster info recorded even on error ----------------------------

func TestSessionTable_ReadRow_ClusterInfoRecordedOnError(t *testing.T) {
	// The fake returns a ClusterInformation alongside an error on every
	// attempt. Capture the per-attempt ctx so we can inspect the metrics
	// tracer SessionTable installs on it. Retries are bounded by ctx
	// cancellation so this test stays fast — but with status.Internal the
	// retrying interceptor will exhaust MaxAttempts (10) before giving up.
	var capturedCtx context.Context
	var captureMu sync.Mutex

	readPool := &fakeInvoker{
		clusterInfo: &btpb.ClusterInformation{ClusterId: "c1", ZoneId: "z1"},
		err:         status.Error(codes.Internal, "boom"),
	}
	readPool.onCall = func(ctx context.Context) {
		captureMu.Lock()
		defer captureMu.Unlock()
		capturedCtx = ctx
	}

	st := newSessionTestTable(t, readPool, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		// Cancel after the first attempt has been captured so the test
		// stays bounded; the retrying interceptor will then return early
		// with ctx.Err() and our captured ctx survives.
		for {
			captureMu.Lock()
			ok := capturedCtx != nil
			captureMu.Unlock()
			if ok {
				cancel()
				return
			}
		}
	}()

	_, err := st.ReadRow(ctx, "row1")
	if err == nil {
		t.Fatal("ReadRow with always-error fake unexpectedly succeeded")
	}

	captureMu.Lock()
	defer captureMu.Unlock()
	if capturedCtx == nil {
		t.Fatal("fake pool was never called — cannot inspect attempt tracer")
	}
	mt := metricsTracerFromContext(capturedCtx)
	if mt == nil {
		t.Fatal("no metricsTracer on attempt ctx")
	}
	if got := mt.currOp.currAttempt.clusterID; got != "c1" {
		t.Errorf("attempt clusterID = %q; want %q (must be recorded even when attempt errors)", got, "c1")
	}
	if got := mt.currOp.currAttempt.zoneID; got != "z1" {
		t.Errorf("attempt zoneID = %q; want %q", got, "z1")
	}
}

// --- ReadRow: serverLatency / clientBlockingLatency from InvokeResult ------
//
// These tests pin the session-path metrics integration added in
// 17cb90b429. They go through the real ReadRow → retry interceptor →
// baseHandler path so the production code that copies result.SentAt and
// result.Stats.BackendLatency into the attemptTracer is exercised end-to-end
// against a fake pool.
//
// Note on tracer setup: newSessionTestTable installs a disabled
// builtinMetricsTracerFactory, so recordAttemptStart is a no-op and
// currAttempt.startTime stays zero. The session_table.go writes for
// clientBlockingLatency guard on `!startTime.IsZero()`, so the onCall hook
// stamps startTime directly on the attempt tracer before the baseHandler's
// post-Execute writes run. This mirrors what recordAttemptStart would do in
// production but avoids standing up an OTel meter.

func TestSessionTable_ServerLatencyFromStats(t *testing.T) {
	const wantBackend = 42 * time.Millisecond

	readPool := &fakeInvoker{
		resp:   &btpb.SessionReadRowResponse{Row: &btpb.Row{Key: []byte("row1")}},
		stats:  &btpb.SessionRequestStats{BackendLatency: durationpb.New(wantBackend)},
		sentAt: time.Now(),
	}
	var capturedCtx context.Context
	var captureMu sync.Mutex
	readPool.onCall = func(ctx context.Context) {
		captureMu.Lock()
		defer captureMu.Unlock()
		capturedCtx = ctx
		// Stamp a non-zero startTime so the production write of
		// clientBlockingLatency runs (we don't assert on it here, but it
		// keeps the code path consistent with production).
		if mt := metricsTracerFromContext(ctx); mt != nil {
			mt.currOp.currAttempt.setStartTime(time.Now())
		}
	}

	st := newSessionTestTable(t, readPool, nil)

	_, err := st.ReadRow(context.Background(), "row1")
	if err != nil {
		t.Fatalf("ReadRow returned unexpected err: %v", err)
	}

	captureMu.Lock()
	defer captureMu.Unlock()
	if capturedCtx == nil {
		t.Fatal("fake pool was never called — cannot inspect attempt tracer")
	}
	mt := metricsTracerFromContext(capturedCtx)
	if mt == nil {
		t.Fatal("no metricsTracer on attempt ctx")
	}
	// convertToMs returns float64 milliseconds; 42ms → 42.0.
	const wantMs = 42.0
	if got := mt.currOp.currAttempt.serverLatency; got != wantMs {
		t.Errorf("attempt serverLatency = %v ms, want %v ms (must be sourced from InvokeResult.Stats.BackendLatency)", got, wantMs)
	}
}

func TestSessionTable_ClientBlockingLatencyFromSentAt(t *testing.T) {
	const blocking = 100 * time.Millisecond

	// We need SentAt to be exactly attemptStart + blocking. Since the
	// production code computes clientBlockingLatency = SentAt - startTime,
	// stamp startTime in onCall and pre-set SentAt = startTime + blocking
	// just before the baseHandler reads it back. The cleanest way is to
	// have onCall both stamp startTime AND mutate sentAt to a known offset
	// from it — but sentAt is captured into the InvokeResult before the
	// hook runs (look at Invoke in fakeInvoker: the result
	// struct is built *after* hook returns). So if we set both in onCall,
	// both land in the result correctly.
	readPool := &fakeInvoker{
		resp: &btpb.SessionReadRowResponse{Row: &btpb.Row{Key: []byte("row1")}},
	}
	var capturedCtx context.Context
	var captureMu sync.Mutex
	readPool.onCall = func(ctx context.Context) {
		captureMu.Lock()
		defer captureMu.Unlock()
		capturedCtx = ctx
		mt := metricsTracerFromContext(ctx)
		if mt == nil {
			return
		}
		now := time.Now()
		mt.currOp.currAttempt.setStartTime(now)
		// Mutate the fake's sentAt under its own mu so the post-hook
		// InvokeResult build picks up the new value.
		readPool.mu.Lock()
		readPool.sentAt = now.Add(blocking)
		readPool.mu.Unlock()
	}

	st := newSessionTestTable(t, readPool, nil)

	_, err := st.ReadRow(context.Background(), "row1")
	if err != nil {
		t.Fatalf("ReadRow returned unexpected err: %v", err)
	}

	captureMu.Lock()
	defer captureMu.Unlock()
	mt := metricsTracerFromContext(capturedCtx)
	if mt == nil {
		t.Fatal("no metricsTracer on attempt ctx")
	}
	// Want ≈ 100ms. convertToMs returns float64 millis (nanos/1e6) so this
	// is exact for whole-millisecond inputs constructed from time.Add.
	const wantMs = 100.0
	got := mt.currOp.currAttempt.clientBlockingLatency
	if math.Abs(got-wantMs) > 0.5 {
		t.Errorf("attempt clientBlockingLatency = %v ms, want ≈%v ms (must be (SentAt - attemptStart) in ms)", got, wantMs)
	}
}

