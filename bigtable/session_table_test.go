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
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	btpb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
	btransport "cloud.google.com/go/bigtable/internal/transport"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// fakeExecuteVRpcer is a hand-rolled stub for the ExecuteVRpcer interface. It
// counts invocations and returns a configurable (resp, clusterInfo, err)
// triple. A capture hook lets tests inspect the per-attempt context (for
// metrics-tracer assertions).
type fakeExecuteVRpcer struct {
	mu          sync.Mutex
	calls       int32
	resp        interface{}
	clusterInfo *btpb.ClusterInformation
	err         error
	// onCall, if non-nil, runs synchronously before the canned response is
	// returned. Useful for capturing the per-attempt ctx.
	onCall func(ctx context.Context)
}

func (f *fakeExecuteVRpcer) ExecuteVRpc(ctx context.Context, desc btransport.VRpcDescriptor, req interface{}) (interface{}, *btpb.ClusterInformation, error) {
	atomic.AddInt32(&f.calls, 1)
	f.mu.Lock()
	hook := f.onCall
	f.mu.Unlock()
	if hook != nil {
		hook(ctx)
	}
	return f.resp, f.clusterInfo, f.err
}

func (f *fakeExecuteVRpcer) callCount() int32 {
	return atomic.LoadInt32(&f.calls)
}

// newSessionTestTable returns a SessionTable wired up with the provided
// read/write pool fakes and a minimal *Table backing object. It uses a
// disabled metrics tracer factory so SessionTable's contextWithMetricsTracer
// path stays exercised but doesn't try to emit OTel metrics.
func newSessionTestTable(t *testing.T, readPool, writePool ExecuteVRpcer) *SessionTable {
	t.Helper()
	classic := &Table{
		c: &Client{
			project:              "P",
			instance:             "I",
			metricsTracerFactory: &builtinMetricsTracerFactory{enabled: false, shutdown: func() {}},
		},
		table: "t",
	}
	st := &SessionTable{
		tableName:     "projects/P/instances/I/tables/t",
		classic:       classic,
		readPool:      readPool,
		writePool:     writePool,
		readVRpcDesc:  btransport.READ_ROW,
		writeVRpcDesc: btransport.MUTATE_ROW,
	}
	return st
}

// --- Apply: retry-idempotency ------------------------------------------------

func TestSessionTable_Apply_ServerTimeMutationNotRetried(t *testing.T) {
	writePool := &fakeExecuteVRpcer{
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
		t.Errorf("ExecuteVRpc called %d times; want exactly 1 (ServerTime mutations are non-idempotent and must not retry)", got)
	}
}

func TestSessionTable_Apply_TimestampedMutationRetries(t *testing.T) {
	writePool := &fakeExecuteVRpcer{
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
		t.Errorf("ExecuteVRpc called %d times; want %d (MaxAttempts for idempotent mutations)", got, wantAttempts)
	}
}

func TestSessionTable_Apply_NilWritePoolReturnsError(t *testing.T) {
	// readPool present, writePool nil — Apply must not panic and must surface
	// a write-not-supported error.
	readPool := &fakeExecuteVRpcer{}
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

	readPool := &fakeExecuteVRpcer{
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

