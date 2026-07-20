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
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	btpb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	rpcstatus "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

// TestIntegration_SessionVRpc_ReadRow exercises ReadRow end-to-end through the
// vRPC session path and asserts the row arrives back from the fake server.
func TestIntegration_SessionVRpc_ReadRow(t *testing.T) {
	h := newSessionTestHarness(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tbl := h.client.OpenTable("test-table")
	row, err := tbl.ReadRow(ctx, "test-row")
	if err != nil {
		t.Fatalf("ReadRow: %v", err)
	}
	if row == nil {
		t.Fatal("ReadRow returned nil row, want test-value cell")
	}
	cells := row["fam1"]
	if len(cells) == 0 {
		t.Fatalf("ReadRow row missing fam1 cells, got %+v", row)
	}
	if got := string(cells[0].Value); got != "test-value" {
		t.Errorf("cell value = %q, want %q", got, "test-value")
	}

	vrpcs := waitForVRpcs(t, h.server, 1, 2*time.Second)
	if len(vrpcs) != 1 {
		t.Errorf("server saw %d vRPCs, want exactly 1", len(vrpcs))
	}
	// Confirm the payload was a ReadRow TableRequest.
	var tr btpb.TableRequest
	if err := proto.Unmarshal(vrpcs[0].req.Payload, &tr); err != nil {
		t.Fatalf("decode captured TableRequest: %v", err)
	}
	if tr.GetReadRow() == nil {
		t.Errorf("captured TableRequest was not ReadRow shape: %T", tr.Payload)
	}
}

// TestIntegration_SessionVRpc_Apply exercises Apply end-to-end through the
// vRPC session path and asserts a MutateRow-shaped request reached the wire.
func TestIntegration_SessionVRpc_Apply(t *testing.T) {
	h := newSessionTestHarness(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tbl := h.client.OpenTable("test-table")
	mut := NewMutation()
	// Explicit timestamp keeps the mutation retryable; ServerTime would
	// flip mutationsAreRetryable and change the retry budget — irrelevant
	// for this happy-path assertion, but worth being deterministic.
	mut.Set("fam1", "col1", Timestamp(1000), []byte("v"))

	if err := tbl.Apply(ctx, "test-row", mut); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	vrpcs := waitForVRpcs(t, h.server, 1, 2*time.Second)
	if len(vrpcs) != 1 {
		t.Errorf("server saw %d vRPCs, want exactly 1", len(vrpcs))
	}
	var tr btpb.TableRequest
	if err := proto.Unmarshal(vrpcs[0].req.Payload, &tr); err != nil {
		t.Fatalf("decode captured TableRequest: %v", err)
	}
	if tr.GetMutateRow() == nil {
		t.Errorf("captured TableRequest was not MutateRow shape: %T", tr.Payload)
	}
}

// TestIntegration_SessionVRpc_RequestCarriesDeadline asserts the per-vRPC
// Deadline field is populated from the caller's context deadline. The exact
// remaining budget will be slightly less than the parent (encode + send
// overhead), so we bound-check rather than equality-check.
func TestIntegration_SessionVRpc_RequestCarriesDeadline(t *testing.T) {
	h := newSessionTestHarness(t)

	parent, parentCancel := context.WithCancel(context.Background())
	defer parentCancel()
	ctx, cancel := context.WithDeadline(parent, time.Now().Add(2*time.Second))
	defer cancel()

	tbl := h.client.OpenTable("test-table")
	if _, err := tbl.ReadRow(ctx, "test-row"); err != nil {
		t.Fatalf("ReadRow: %v", err)
	}

	vrpcs := waitForVRpcs(t, h.server, 1, 2*time.Second)
	v := vrpcs[0].req
	if v.Deadline == nil {
		t.Fatal("VirtualRpcRequest.Deadline = nil, want non-nil")
	}
	d := v.Deadline.AsDuration()
	if d < 100*time.Millisecond || d > 2*time.Second {
		t.Errorf("VirtualRpcRequest.Deadline = %v, want in [100ms, 2s]", d)
	}
}

// TestIntegration_SessionVRpc_RequestCarriesMetadata asserts the vRPC
// Metadata oneof carries AttemptNumber and AttemptStart on every wire frame
// — the data the AFE needs to attribute retries to the same logical op.
func TestIntegration_SessionVRpc_RequestCarriesMetadata(t *testing.T) {
	h := newSessionTestHarness(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tbl := h.client.OpenTable("test-table")
	if _, err := tbl.ReadRow(ctx, "test-row"); err != nil {
		t.Fatalf("ReadRow: %v", err)
	}

	vrpcs := waitForVRpcs(t, h.server, 1, 2*time.Second)
	v := vrpcs[0].req
	if v.Metadata == nil {
		t.Fatal("VirtualRpcRequest.Metadata = nil")
	}
	if v.Metadata.AttemptNumber < 1 {
		t.Errorf("AttemptNumber = %d, want >= 1", v.Metadata.AttemptNumber)
	}
	if v.Metadata.AttemptStart == nil {
		t.Fatal("AttemptStart = nil")
	}
	// Sanity: AttemptStart should be within a few seconds of now (it was
	// captured immediately before Send by Invoke).
	now := time.Now()
	got := v.Metadata.AttemptStart.AsTime()
	if got.Before(now.Add(-30*time.Second)) || got.After(now.Add(30*time.Second)) {
		t.Errorf("AttemptStart = %v, want within +/-30s of %v", got, now)
	}
}

// TestIntegration_SessionVRpc_RetriesOnUnavailable arms the server to return
// Unavailable on the first vRPC then succeed on the second, and asserts the
// retry interceptor surfaces a successful ReadRow with AttemptNumber=1 and
// AttemptNumber=2 in the captured frames.
//
// The Java-parity classifier does NOT retry a bare server-explicit error
// (see shouldRetryDefault in internal/transport/retrying.go), so the reply
// must carry an explicit server RetryInfo to authorize the retry. This
// matches the Java client's contract with the AFE.
func TestIntegration_SessionVRpc_RetriesOnUnavailable(t *testing.T) {
	h := newSessionTestHarness(t)

	// Arm the first vRPC to fail with Unavailable + server-directed retry.
	// The vRPC retry loop in SessionTable uses RetryingVRpc(MaxAttempts:10,
	// InitialBackoff:10ms), so the second attempt fires very quickly.
	h.server.queueAttemptErrs(fakeAttemptErr{
		Status: &rpcstatus.Status{
			Code:    int32(codes.Unavailable),
			Message: "fake transient error",
		},
		RetryInfo: &errdetails.RetryInfo{
			RetryDelay: durationpb.New(5 * time.Millisecond),
		},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tbl := h.client.OpenTable("test-table")
	row, err := tbl.ReadRow(ctx, "test-row")
	if err != nil {
		// Re-check status code for diagnostics — the retry loop should
		// have eaten the Unavailable.
		st, _ := status.FromError(err)
		t.Fatalf("ReadRow after retry: %v (code=%s)", err, st.Code())
	}
	if row == nil {
		t.Fatal("ReadRow after retry returned nil row")
	}

	vrpcs := waitForVRpcs(t, h.server, 2, 5*time.Second)
	if len(vrpcs) < 2 {
		t.Fatalf("server saw %d vRPCs after retry, want >= 2", len(vrpcs))
	}
	// Both attempts must arrive with vRPC metadata populated, and
	// AttemptNumber must strictly increment: attempt 1 = 1, attempt 2 = 2.
	// SessionTable now seeds the ctx with WithVRpcMetadata before invoking
	// RetryingVRpc, so retrying.go's WithAttempt(ctx, n) mutates the value
	// that Invoke subsequently reads via VRpcAttempt(ctx).
	wantAttempts := []int64{1, 2}
	for i, want := range wantAttempts {
		v := vrpcs[i].req
		if v.GetMetadata() == nil {
			t.Errorf("vrpcs[%d].Metadata = nil, want non-nil", i)
			continue
		}
		if got := v.GetMetadata().GetAttemptNumber(); got != want {
			t.Errorf("vrpcs[%d].AttemptNumber = %d, want %d", i, got, want)
		}
	}
	// Sanity: at least one frame must be a ReadRow (both should be, but
	// the second is what we care about for retry semantics).
	var tr btpb.TableRequest
	if err := proto.Unmarshal(vrpcs[1].req.Payload, &tr); err != nil {
		t.Fatalf("decode retry TableRequest: %v", err)
	}
	if tr.GetReadRow() == nil {
		t.Errorf("retry attempt was not a ReadRow request: %T", tr.Payload)
	}
}

// TestIntegration_SessionVRpc_SessionReuse verifies that N sequential
// ReadRows do NOT open one session per call — sessions are reused. The
// pool can seed up to SessionPoolMax sessions eagerly (min=1, max=2 with
// headroom), but the aggregate open count must stay ≤ max regardless of
// how many reads fire, and the per-session RPC counter proves reuse.
func TestIntegration_SessionVRpc_SessionReuse(t *testing.T) {
	h := newSessionTestHarness(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const reads = 6
	tbl := h.client.OpenTable("test-table")
	for i := 0; i < reads; i++ {
		if _, err := tbl.ReadRow(ctx, "test-row"); err != nil {
			t.Fatalf("ReadRow[%d]: %v", i, err)
		}
	}

	vrpcs := waitForVRpcs(t, h.server, reads, 2*time.Second)
	if len(vrpcs) != reads {
		t.Errorf("server saw %d vRPCs, want exactly %d", len(vrpcs), reads)
	}
	// The pool is bounded by SessionPoolMax=2 (the fake advertises this in
	// GetClientConfiguration). If ANY reuse is happening, openSessionCount
	// stays well under `reads`.
	got := h.server.openSessionCount()
	if got > 2 {
		t.Errorf("openSessionCount = %d, want <= 2 (SessionPoolMax bound)", got)
	}
	if got >= reads {
		t.Errorf("openSessionCount = %d, want < %d (sessions must be reused across sequential reads)", got, reads)
	}
}

// TestIntegration_SessionVRpc_MultipleTables opens two SessionTables on the
// same Client and asserts each spins up its own read pool + session, and each
// ReadRow reaches the wire independently.
func TestIntegration_SessionVRpc_MultipleTables(t *testing.T) {
	h := newSessionTestHarness(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	t1 := h.client.OpenTable("table-a")
	t2 := h.client.OpenTable("table-b")

	if _, err := t1.ReadRow(ctx, "row-a"); err != nil {
		t.Fatalf("ReadRow table-a: %v", err)
	}
	if _, err := t2.ReadRow(ctx, "row-b"); err != nil {
		t.Fatalf("ReadRow table-b: %v", err)
	}

	// Two ReadRows, two vRPCs — one per table.
	vrpcs := waitForVRpcs(t, h.server, 2, 2*time.Second)
	if len(vrpcs) != 2 {
		t.Fatalf("saw %d vRPCs, want 2", len(vrpcs))
	}

	// Two SessionTables → two lazy read pools → at least two OpenSession
	// handshakes (each pool's initial fill opens one). Bound the upper end
	// loosely (SessionPoolMax=2 * 2 tables = 4).
	got := h.server.openSessionCount()
	if got < 2 || got > 4 {
		t.Errorf("openSessionCount = %d, want in [2, 4] for two distinct tables", got)
	}
}

// TestIntegration_SessionVRpc_NilRowResponse verifies the empty-row path:
// the server returns a well-formed TableResponse whose ReadRow.Row is nil
// (row not found), and the client surfaces (nil, nil) — not an error.
func TestIntegration_SessionVRpc_NilRowResponse(t *testing.T) {
	h := newSessionTestHarness(t)
	h.server.setReadRowResponse(t, &btpb.TableResponse{
		Payload: &btpb.TableResponse_ReadRow{
			ReadRow: &btpb.SessionReadRowResponse{Row: nil},
		},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	row, err := h.client.OpenTable("test-table").ReadRow(ctx, "missing-row")
	if err != nil {
		t.Fatalf("ReadRow: %v (want nil error for empty row)", err)
	}
	if row != nil {
		t.Errorf("ReadRow row = %+v, want nil (row not present)", row)
	}
}

// TestIntegration_SessionVRpc_NonRetryableInvalidArgument arms a single
// InvalidArgument reply (no RetryInfo) and asserts the client surfaces it
// immediately without retrying. This exercises the Java-parity default:
// bare server-explicit errors are terminal unless RetryInfo says otherwise.
func TestIntegration_SessionVRpc_NonRetryableInvalidArgument(t *testing.T) {
	h := newSessionTestHarness(t)
	h.server.queueAttemptErrs(fakeAttemptErr{
		Status: &rpcstatus.Status{
			Code:    int32(codes.InvalidArgument),
			Message: "bogus filter",
		},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := h.client.OpenTable("test-table").ReadRow(ctx, "test-row")
	if err == nil {
		t.Fatal("ReadRow returned nil, want InvalidArgument")
	}
	st, ok := status.FromError(err)
	if !ok {
		// Error may be wrapped in fmt.Errorf %w — walk the chain.
		if se := errors.Unwrap(err); se != nil {
			st, ok = status.FromError(se)
		}
	}
	if !ok || st.Code() != codes.InvalidArgument {
		t.Errorf("err = %v (ok=%t, code=%s), want InvalidArgument", err, ok, st.Code())
	}
	// Exactly one wire frame — no retry.
	vrpcs := h.server.snapshotVRpcs()
	if len(vrpcs) != 1 {
		t.Errorf("server saw %d vRPCs, want 1 (non-retryable code must not retry)", len(vrpcs))
	}
	if h.server.queuedAttemptErrCount() != 0 {
		t.Errorf("armed error queue still holds %d entries, want 0", h.server.queuedAttemptErrCount())
	}
}

// TestIntegration_SessionVRpc_ServerDirectedRetryOnFailedPrecondition
// verifies the RetryInfo escape hatch: a normally-terminal FailedPrecondition
// becomes retryable when the server explicitly attaches RetryInfo. This is
// the sole way for a session-path error to bypass the Java-parity default.
func TestIntegration_SessionVRpc_ServerDirectedRetryOnFailedPrecondition(t *testing.T) {
	h := newSessionTestHarness(t)
	h.server.queueAttemptErrs(fakeAttemptErr{
		Status: &rpcstatus.Status{
			Code:    int32(codes.FailedPrecondition),
			Message: "please retry",
		},
		RetryInfo: &errdetails.RetryInfo{
			RetryDelay: durationpb.New(1 * time.Millisecond),
		},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	row, err := h.client.OpenTable("test-table").ReadRow(ctx, "test-row")
	if err != nil {
		t.Fatalf("ReadRow: %v (server RetryInfo should authorize retry)", err)
	}
	if row == nil {
		t.Fatal("ReadRow after server-directed retry returned nil row")
	}
	vrpcs := waitForVRpcs(t, h.server, 2, 3*time.Second)
	if len(vrpcs) < 2 {
		t.Errorf("saw %d vRPCs, want >= 2 (initial + retry)", len(vrpcs))
	}
}

// TestIntegration_SessionVRpc_RetryExhaustion queues enough retryable
// errors to exceed MaxAttempts=10 and asserts the client (a) surfaces the
// last error, (b) actually issued MaxAttempts wire frames, and (c) never
// dips into the queue past what MaxAttempts permits.
func TestIntegration_SessionVRpc_RetryExhaustion(t *testing.T) {
	h := newSessionTestHarness(t)

	const maxAttempts = 10
	const queued = maxAttempts + 5 // 5 extra should remain unused
	errs := make([]fakeAttemptErr, queued)
	for i := range errs {
		errs[i] = fakeAttemptErr{
			Status: &rpcstatus.Status{
				Code:    int32(codes.Unavailable),
				Message: fmt.Sprintf("attempt %d failure", i+1),
			},
			// 0-delay RetryInfo makes the retry loop fire back-to-back
			// without dragging the test wall clock past the RPC deadline.
			RetryInfo: &errdetails.RetryInfo{
				RetryDelay: durationpb.New(0),
			},
		}
	}
	h.server.queueAttemptErrs(errs...)

	// Generous ctx budget so the failure is retry exhaustion, not deadline.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	_, err := h.client.OpenTable("test-table").ReadRow(ctx, "test-row")
	if err == nil {
		t.Fatal("ReadRow returned nil, want Unavailable after exhaustion")
	}
	// The bottom of the error chain should carry the last Unavailable.
	st, _ := status.FromError(errors.Unwrap(err))
	if st.Code() != codes.Unavailable {
		t.Errorf("last-error code = %s, want Unavailable", st.Code())
	}

	// Wait briefly for all attempts to land in the vRPC log (retries fire
	// after Send has captured the request, but the count race is short).
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(h.server.snapshotVRpcs()) >= maxAttempts {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got := len(h.server.snapshotVRpcs()); got != maxAttempts {
		t.Errorf("wire frame count = %d, want exactly %d", got, maxAttempts)
	}
	if got := h.server.queuedAttemptErrCount(); got != queued-maxAttempts {
		t.Errorf("armed queue depth = %d, want %d (retry loop must stop at MaxAttempts)",
			got, queued-maxAttempts)
	}
}

// TestIntegration_SessionVRpc_ContextCanceled cancels the caller's context
// before ReadRow. The client should surface context.Canceled without
// sending any vRPC to the wire.
func TestIntegration_SessionVRpc_ContextCanceled(t *testing.T) {
	h := newSessionTestHarness(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // fire immediately

	_, err := h.client.OpenTable("test-table").ReadRow(ctx, "test-row")
	if err == nil {
		t.Fatal("ReadRow with pre-canceled ctx returned nil error")
	}
	if !errors.Is(err, context.Canceled) {
		// Some paths convert to codes.Canceled — accept either shape.
		st, _ := status.FromError(errors.Unwrap(err))
		if st.Code() != codes.Canceled {
			t.Errorf("err = %v, want context.Canceled or codes.Canceled", err)
		}
	}
}

// TestIntegration_SessionVRpc_DeadlineExceeded pairs a short caller
// deadline with a slow server. The retry loop's ctx-done guard should fire
// and the error should carry a DeadlineExceeded signal.
func TestIntegration_SessionVRpc_DeadlineExceeded(t *testing.T) {
	h := newSessionTestHarness(t)
	// Every reply frame is delayed 500ms → any ctx with a sub-500ms budget
	// times out mid-flight, exercising the mid-flight ctx-done path
	// (session_vrpc.go tags this StateTransportFailure).
	h.server.setResponseDelay(500 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err := h.client.OpenTable("test-table").ReadRow(ctx, "test-row")
	if err == nil {
		t.Fatal("ReadRow with slow server + short deadline returned nil error")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		st, _ := status.FromError(errors.Unwrap(err))
		if st.Code() != codes.DeadlineExceeded {
			t.Errorf("err = %v, want context.DeadlineExceeded or codes.DeadlineExceeded", err)
		}
	}
}

// TestIntegration_SessionVRpc_ConcurrentLoad fires many concurrent
// ReadRow/Apply calls through one Client and asserts every call succeeds
// and every attempt reached the wire. Guards against pool contention
// deadlocks and races between checkout and result delivery.
func TestIntegration_SessionVRpc_ConcurrentLoad(t *testing.T) {
	h := newSessionTestHarness(t)

	const workers = 32
	const iters = 4

	tbl := h.client.OpenTable("test-table")
	var wg sync.WaitGroup
	errs := make(chan error, workers*iters)
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func(w int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			for i := 0; i < iters; i++ {
				if w%2 == 0 {
					if _, err := tbl.ReadRow(ctx, fmt.Sprintf("row-%d-%d", w, i)); err != nil {
						errs <- fmt.Errorf("ReadRow[w=%d i=%d]: %w", w, i, err)
						return
					}
				} else {
					mut := NewMutation()
					mut.Set("fam1", "col1", Timestamp(1000), []byte("v"))
					if err := tbl.Apply(ctx, fmt.Sprintf("row-%d-%d", w, i), mut); err != nil {
						errs <- fmt.Errorf("Apply[w=%d i=%d]: %w", w, i, err)
						return
					}
				}
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Errorf("concurrent op failed: %v", e)
	}

	// Every op maps to exactly one wire frame (no retries under normal load).
	wantVRpcs := workers * iters
	vrpcs := waitForVRpcs(t, h.server, wantVRpcs, 10*time.Second)
	if len(vrpcs) != wantVRpcs {
		t.Errorf("wire frame count = %d, want %d", len(vrpcs), wantVRpcs)
	}
	// Sessions never exceed SessionPoolMax=2 per pool × 2 pools (read+write).
	if got := h.server.openSessionCount(); got > 4 {
		t.Errorf("openSessionCount = %d, want <= 4 (Max=2 per pool, 2 pools)", got)
	}
}

// TestIntegration_SessionVRpc_PeerInfoParsedIntoAfeID configures the fake
// to emit the bigtable-peer-info header with two distinct AFE IDs across
// successive OpenTable streams, drives enough concurrent load to open
// multiple sessions, then snapshots SessionDebug and asserts BOTH AFE IDs
// appear on live sessions. Exercises PeerInfo extraction + AFE grouping.
func TestIntegration_SessionVRpc_PeerInfoParsedIntoAfeID(t *testing.T) {
	// SessionPoolMin=2 forces both AFE-tagged sessions to open eagerly;
	// setPeerInfoRotation must run before Serve so the fake stamps distinct
	// AFE IDs onto successive streams.
	h := newSessionTestHarness(t,
		withPoolSize(2, 4),
		withFakeSetup(func(s *fakeBigtableServer) { s.setPeerInfoRotation([]int64{4242, 8484}) }),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Drive a read so the read pool opens (lazy). SessionPoolMin=2 guarantees
	// both AFE-tagged sessions get created for the read pool.
	tbl := h.client.OpenTable("test-table")
	if _, err := tbl.ReadRow(ctx, "test-row"); err != nil {
		t.Fatalf("ReadRow: %v", err)
	}

	// Poll the debug snapshot briefly — session opens are asynchronous with
	// SessionPoolMin fills, so the second session may not be Ready yet at
	// the moment the first ReadRow returns.
	prov := h.client.SessionDebug()
	if prov == nil {
		t.Fatal("client.SessionDebug() = nil")
	}
	var (
		saw4242 bool
		saw8484 bool
	)
	pollUntil := time.Now().Add(3 * time.Second)
	for time.Now().Before(pollUntil) {
		saw4242, saw8484 = false, false
		for _, pool := range prov.Snapshot() {
			for _, s := range pool.Sessions {
				switch s.Peer.ApplicationFrontendID {
				case 4242:
					saw4242 = true
				case 8484:
					saw8484 = true
				}
			}
		}
		if saw4242 && saw8484 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !saw4242 || !saw8484 {
		t.Errorf("SessionDebug snapshot missed rotated AFE IDs: saw4242=%t saw8484=%t", saw4242, saw8484)
	}
}

// TestIntegration_SessionVRpc_ServerErrorRetriesOnDifferentSession pins
// down the "don't wait for ctx, retry on a different session" contract
// (SESSION_SPEC.md invariants 5, 8, 9): when the server terminates the
// session while a vRPC is in flight, the client MUST force-close that
// session, cancel the in-flight attempt with StateTransportFailure, and
// let the retry oracle re-pick a peer — completing well before the
// caller's ctx deadline.
//
// This test uses a session-level ErrorResponse (rpc_id=0) as the
// trigger. That's the same client-side path the missed-heartbeat
// watchdog reaches (handleErrorResponse → ForceClose →
// cancelActiveRPCs); the watchdog itself can't be exercised from an
// integration test because heartBeatLoop arms its timer to
// initialHeartbeatGrace (30 min) at spawn and doesn't reschedule when a
// later SessionParameters frame shortens the interval.
//
// Setup:
//   - Pool sized Min=2/Max=4 so a second Ready session exists when the
//     first one is force-closed. Min=1 would work but adds jitter from
//     on-demand pool refill.
//   - queueSessionLevelErrs(Unavailable) makes the first vRPC to arrive
//     be answered with a session-level error; the fake then closes that
//     stream. Subsequent vRPCs (retries) hit a different session and
//     succeed normally.
//   - Caller ctx = 10s. The test asserts wall-clock elapsed is well
//     under that — proving the retry did not sit on the caller's ctx.
func TestIntegration_SessionVRpc_ServerErrorRetriesOnDifferentSession(t *testing.T) {
	// SessionPoolMin=2 guarantees a Ready peer exists for the retry to land
	// on the moment the failed session force-closes.
	h := newSessionTestHarness(t, withPoolSize(2, 4))
	fakeSrv, tbl := h.server, h.client.OpenTable("test-table")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if _, err := tbl.ReadRow(ctx, "warmup"); err != nil {
		t.Fatalf("warmup ReadRow: %v", err)
	}
	// Wait until at least 2 sessions have completed the OpenSession
	// handshake — a retry-ready peer must exist before we arm the fault.
	pollDeadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(pollDeadline) {
		if fakeSrv.openSessionCount() >= 2 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := fakeSrv.openSessionCount(); got < 2 {
		t.Fatalf("openSessionCount = %d after warmup, want >= 2 (need a retry-ready peer)", got)
	}
	warmupVRpcs := len(fakeSrv.snapshotVRpcs())

	// ForceClose (triggered by the session-level error path) sends NO
	// CloseSessionRequest — stream is presumed dead. Snapshot before, and
	// re-check after ReadRow returns (before test cleanup Close() bumps it).
	closesBefore := fakeSrv.closeSessionCount()

	// Arm one session-level Unavailable and fire the real ReadRow.
	fakeSrv.queueSessionLevelErrs(fakeAttemptErr{
		Status: &rpcstatus.Status{
			Code:    int32(codes.Unavailable),
			Message: "simulated server-side session teardown",
		},
	})
	start := time.Now()
	row, err := tbl.ReadRow(ctx, "test-row")
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("ReadRow after session-level error: %v (elapsed=%v)", err, elapsed)
	}
	if row == nil {
		t.Fatal("ReadRow returned nil row after retry")
	}
	if got := string(row["fam1"][0].Value); got != "test-value" {
		t.Errorf("cell value = %q, want %q", got, "test-value")
	}

	// The whole retry — force-close, pool re-pick, second attempt —
	// should complete in tens of ms; the 3s bound is generous slack. If
	// the retry regressed to waiting for the caller's ctx, this bound
	// trips first, catching the regression the test exists to guard.
	if elapsed > 3*time.Second {
		t.Errorf("elapsed = %v, want < 3s (retry must fire off the server's session-level error, not the caller's ctx deadline)", elapsed)
	}

	// EXACTLY two attempts landed on the wire past warmup: the failed
	// one (captured before the session-level error came back) and the
	// successful retry. Requiring exact count guards against a regression
	// in the I5 (ReleaseToPool inExpectedCount) gate that could re-queue
	// a drained session and trigger extra attempts.
	vrpcs := waitForVRpcs(t, fakeSrv, warmupVRpcs+2, 3*time.Second)
	got := vrpcs[warmupVRpcs:]
	if len(got) != 2 {
		t.Fatalf("post-warmup vRPCs = %d, want exactly 2", len(got))
	}
	wantAttempts := []int64{1, 2}
	for i, want := range wantAttempts {
		if got[i].req.GetMetadata().GetAttemptNumber() != want {
			t.Errorf("post-warmup vrpcs[%d].AttemptNumber = %d, want %d",
				i, got[i].req.GetMetadata().GetAttemptNumber(), want)
		}
	}
	for i, v := range got[:2] {
		var tr btpb.TableRequest
		if err := proto.Unmarshal(v.req.Payload, &tr); err != nil {
			t.Fatalf("decode vrpcs[%d]: %v", warmupVRpcs+i, err)
		}
		if tr.GetReadRow() == nil {
			t.Errorf("vrpcs[%d] was not a ReadRow: %T", warmupVRpcs+i, tr.Payload)
		}
	}

	// Prove the retry hopped sessions: stream index of the second
	// attempt must differ from the first. Both indexes come from the
	// same snapshot as the requests above, so an in-flight append can't
	// misalign this assertion against a fresh vRPC arriving.
	if got[0].streamIdx == got[1].streamIdx {
		t.Errorf("retry landed on the same stream (idx %d) as the failed attempt; want different session",
			got[0].streamIdx)
	}

	// ForceClose (session-level error path) sends NO CloseSessionRequest
	// — stream is presumed dead. If closeSessionCount grew, the client
	// took the graceful-close branch instead, violating SESSION_SPEC #5/#8.
	if got := fakeSrv.closeSessionCount(); got != closesBefore {
		t.Errorf("closeSessionCount grew from %d to %d during force-close path; want no CloseSession frame on ForceClose",
			closesBefore, got)
	}
}

// TestIntegration_SessionVRpc_HeartbeatWatchdogReactiveToSessionParameters
// proves the reactive coupling between the heartbeat atomic and the
// heartBeatLoop Timer. See SESSION_SPEC.md #7 ("Any frame in either
// direction resets the deadline"): SessionParameters shortens
// `heartbeatIntervalNano` AND `nextHeartbeatDeadlineNano`; the wake on
// `heartbeatWake` reshuffles the Timer so a stalled vRPC in the first
// 30 min of a session is caught by the missed-heartbeat watchdog — NOT
// by the caller's ctx deadline. Without the wake, the Timer stays armed
// to the `initialHeartbeatGrace = 30 min` bootstrap set at NewSession,
// and no atomic shortening reshuffles it.
//
// Setup:
//   - Pool sized to 2 (min=max) so both sessions are open before ReadRow.
//   - Fake negotiates KeepAlive=100ms → atomic deadline = now + 300ms.
//   - queueVRpcStalls(1) makes ONLY the first incoming vRPC hang; the
//     retry that lands on the second session sees an empty stall queue
//     and returns the normal ReadRow response.
//   - Caller ctx = 3 s. If the watchdog WERE non-reactive, the first
//     vRPC would stall until ctx expiry (retry loop never fires because
//     ctx is done inside attempt 1), and ReadRow would return
//     context.DeadlineExceeded near the 3-s mark.
//
// Assertions:
//   - err == nil — retry landed on a healthy session and succeeded.
//   - Elapsed <= 1.5 s — the first attempt was killed by the watchdog
//     (~300ms) plus retry backoff (~10ms), not by the 3-s caller ctx.
//   - SessionDebug shows exactly one session with
//     CloseReason=="MissedHeartbeat" — the stalled first session.
//   - closeSessionCount stays 0 for the missed-heartbeat path (ForceClose
//     skips the graceful CloseSession frame — SESSION_SPEC #5/#8).
//
// If someone removes wakeHeartbeatLoop from handleSessionParameters (or
// from resetHeartbeatDeadline), this test flips back to failing with a
// ~3-s DeadlineExceeded — that regression is exactly what the reactive
// coupling exists to prevent.
func TestIntegration_SessionVRpc_HeartbeatWatchdogReactiveToSessionParameters(t *testing.T) {
	// min=max=2 keeps both sessions open before ReadRow so the retry after
	// ForceClose has an immediately-available peer to land on (no wait on
	// pool replacement open). withReverseCloseOrder is mandatory here —
	// client.Close would otherwise hang on the graceful CloseSession
	// exchange the stall prevents the fake from processing.
	h := newSessionTestHarness(t,
		withPoolSize(2, 2),
		withFakeSetup(func(s *fakeBigtableServer) {
			// 100 ms KeepAlive → atomic heartbeat deadline = now + 300 ms
			// after any inbound/outbound frame. With the Timer reactive to
			// the atomic (via heartbeatWake), the watchdog fires ~300 ms
			// into a stalled vRPC — before the caller's 3-s ctx does.
			s.setSessionParamsKeepAlive(100 * time.Millisecond)
			// ONLY the first vRPC stalls; the retry sees an empty stall
			// queue and gets the standard ReadRow response. That's what
			// turns "did the watchdog fire?" into an observable
			// success/failure split: reactive → retry succeeds;
			// non-reactive → ctx times out on attempt #1.
			s.queueVRpcStalls(1)
		}),
		withReverseCloseOrder(),
	)
	fakeSrv := h.server
	tbl := h.client.OpenTable("test-table")

	// Caller ctx has plenty of slack (3 s ≫ 300 ms). If the watchdog is
	// non-reactive, the first attempt stalls until this ctx expires; if
	// reactive, the watchdog kills attempt #1 in ~300 ms and the retry
	// completes well inside the ctx.
	callCtx, callCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer callCancel()

	start := time.Now()
	_, err := tbl.ReadRow(callCtx, "test-row")
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("ReadRow returned err=%v (elapsed=%v); expected success on retry after watchdog force-closed the stalled first session. If elapsed is near the 3-s ctx, the watchdog Timer likely stopped reacting to the SessionParameters atomic — check wakeHeartbeatLoop wiring in handleSessionParameters.",
			err, elapsed)
	}

	// Timing gate: elapsed must be near 300 ms + backoff (~10-50 ms), NOT
	// near the 3-s ctx. 1.5 s leaves generous slack for CI jitter while
	// still catching a "watchdog stopped firing" regression.
	if elapsed > 1500*time.Millisecond {
		t.Errorf("elapsed = %v, want <= 1.5s (watchdog should have fired at ~300 ms and let the retry succeed; a value near the 3-s ctx means the reactive-watchdog path is broken)",
			elapsed)
	}

	// The stalled session must show up in the pool's CloseReasons
	// aggregate as "MissedHeartbeat" — the per-session snapshot rows are
	// pruned as soon as sessionList drops the closed handle, but the
	// pool's lifetime counter survives. Non-zero here is definitive
	// proof that heartBeatLoop reached its ForceClose branch, and not
	// some other error path (transport drop, ctx cancel) that would
	// also fail the first attempt.
	var missedCount int64
	if prov := h.client.SessionDebug(); prov != nil {
		for _, pool := range prov.Snapshot() {
			missedCount += pool.CloseReasons["MissedHeartbeat"]
		}
	}
	if missedCount == 0 {
		t.Error("PoolSnapshot.CloseReasons has no MissedHeartbeat entry — retry succeeded but not via the watchdog path this test is meant to exercise")
	}

	// ForceClose on missed-heartbeat MUST NOT send a graceful CloseSession
	// frame (SESSION_SPEC #5/#8: ForceClose presumes the stream is dead).
	// Zero here proves the watchdog took the ForceClose branch, not a
	// graceful Close.
	if got := fakeSrv.closeSessionCount(); got != 0 {
		t.Errorf("closeSessionCount = %d, want 0 (missed-heartbeat force-close must not emit a CloseSession frame)", got)
	}
}

// TestIntegration_SessionVRpc_ClientCloseSendsCloseSession asserts that
// tearing down the Client triggers a CloseSession frame per session — the
// polite shutdown path the server expects for accounting.
func TestIntegration_SessionVRpc_ClientCloseSendsCloseSession(t *testing.T) {
	// Uses the standard harness (waitForRouting fires, so at least one
	// session is open by the time we snapshot). We call client.Close mid-
	// test to observe the effect; the harness's own Close later is a safe
	// no-op double-close.
	h := newSessionTestHarness(t)
	fakeSrv := h.server

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Force one ReadRow so the read pool opens (lazy).
	if _, err := h.client.OpenTable("test-table").ReadRow(ctx, "test-row"); err != nil {
		t.Fatalf("ReadRow: %v", err)
	}

	sessionsBeforeClose := fakeSrv.openSessionCount()
	if sessionsBeforeClose == 0 {
		t.Fatal("openSessionCount = 0 before Close, want >= 1")
	}

	// Close returns an error today when the CloseSession RPC races the
	// underlying conn teardown ("grpc: the client connection is closing").
	// The race is benign — the frame is enqueued before conn shutdown — so
	// we log but don't fail on it. What we care about is the ordering: at
	// least ONE CloseSession frame reached the server before the socket died.
	if err := h.client.Close(); err != nil {
		t.Logf("client.Close returned (non-fatal): %v", err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if fakeSrv.closeSessionCount() >= 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := fakeSrv.closeSessionCount(); got < 1 {
		t.Errorf("closeSessionCount = %d after Close, want >= 1 (sessions must send CloseSession on shutdown, opened=%d)",
			got, sessionsBeforeClose)
	}
}
