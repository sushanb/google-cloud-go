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

package session

import (
	"context"
	"sync"
	"testing"

	btpb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
	"cloud.google.com/go/bigtable/internal/metrics"
	btransport "cloud.google.com/go/bigtable/internal/transport"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// fakeInvoker satisfies the Invoker interface. The first errBefore
// calls return err; every call after that returns result. Counts total
// calls under a mutex so tests can assert attempt fan-out.
type fakeInvoker struct {
	mu        sync.Mutex
	calls     int
	errBefore int
	err       error
	result    btransport.InvokeResult
}

func (f *fakeInvoker) Invoke(_ context.Context, _ btransport.VRpcDescriptor, _ interface{}) (btransport.InvokeResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.calls <= f.errBefore {
		return btransport.InvokeResult{}, f.err
	}
	return f.result, nil
}

func (f *fakeInvoker) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// newTestTable wires a sessionTable to the given invokers, a
// ManualReader-backed MeterProvider, and stub VRpc descriptors. The
// returned reader can be Collect()ed to inspect emitted histograms.
func newTestTable(t *testing.T, readInv, writeInv Invoker) (*sessionTable, *sdkmetric.ManualReader) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	factory, err := metrics.NewFactoryForTest("test-project", "test-instance", "test-profile", mp)
	if err != nil {
		t.Fatalf("metrics.NewFactoryForTest: %v", err)
	}
	openRead := func() (Invoker, error) { return readInv, nil }
	openWrite := func() (Invoker, error) { return writeInv, nil }
	if readInv == nil {
		openRead = nil
	}
	if writeInv == nil {
		openWrite = nil
	}
	tbl := newSessionTable(
		"projects/p/instances/i/tables/test-table",
		openRead,
		openWrite,
		&btransport.VRpcDescriptorImpl{MethodName: "test.ReadRow"},
		&btransport.VRpcDescriptorImpl{MethodName: "test.MutateRow"},
		nil,
		factory,
	)
	return tbl, reader
}

// sumHistogramSamples returns the total sample count across every data
// point of the named metric. Zero (and false) if the metric is absent.
func sumHistogramSamples(t *testing.T, reader *sdkmetric.ManualReader, name string) (uint64, bool) {
	t.Helper()
	rm := metricdata.ResourceMetrics{}
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("reader.Collect: %v", err)
	}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			hist, ok := m.Data.(metricdata.Histogram[float64])
			if !ok {
				t.Fatalf("metric %q data is %T, want Histogram[float64]", name, m.Data)
			}
			var total uint64
			for _, dp := range hist.DataPoints {
				total += dp.Count
			}
			return total, true
		}
	}
	return 0, false
}

func assertSamples(t *testing.T, reader *sdkmetric.ManualReader, name string, want uint64) {
	t.Helper()
	got, ok := sumHistogramSamples(t, reader, name)
	if !ok {
		t.Errorf("metric %q not emitted; want %d sample(s)", name, want)
		return
	}
	if got != want {
		t.Errorf("metric %q sample count = %d, want %d", name, got, want)
	}
}

func TestSessionTableReadRow_RecordsAttemptAndOperation(t *testing.T) {
	inv := &fakeInvoker{result: btransport.InvokeResult{Response: &btpb.SessionReadRowResponse{}}}
	tbl, reader := newTestTable(t, inv, nil)

	if _, err := tbl.ReadRow(context.Background(), &btpb.SessionReadRowRequest{Key: []byte("row1")}); err != nil {
		t.Fatalf("ReadRow: %v", err)
	}
	if got := inv.callCount(); got != 1 {
		t.Fatalf("invoker calls = %d, want 1", got)
	}
	assertSamples(t, reader, "attempt_latencies", 1)
	assertSamples(t, reader, "attempt_latencies2", 1)
	assertSamples(t, reader, "operation_latencies", 1)
}

func TestSessionTableMutateRow_RecordsAttemptAndOperation(t *testing.T) {
	inv := &fakeInvoker{result: btransport.InvokeResult{Response: &btpb.SessionMutateRowResponse{}}}
	tbl, reader := newTestTable(t, nil, inv)

	req := &btpb.SessionMutateRowRequest{
		Key: []byte("row1"),
		Mutations: []*btpb.Mutation{{
			Mutation: &btpb.Mutation_SetCell_{SetCell: &btpb.Mutation_SetCell{
				FamilyName:      "cf",
				ColumnQualifier: []byte("q"),
				TimestampMicros: 1_000_000,
				Value:           []byte("v"),
			}},
		}},
	}
	if _, err := tbl.MutateRow(context.Background(), req); err != nil {
		t.Fatalf("MutateRow: %v", err)
	}
	if got := inv.callCount(); got != 1 {
		t.Fatalf("invoker calls = %d, want 1", got)
	}
	assertSamples(t, reader, "attempt_latencies", 1)
	assertSamples(t, reader, "attempt_latencies2", 1)
	assertSamples(t, reader, "operation_latencies", 1)
}

// TestSessionTableReadRow_RetriesRecordAttemptPerAttempt drives the
// retry loop three times (two transport failures, then success) and
// asserts each attempt emits a fresh attempt_latencies / attempt_latencies2
// sample while operation_latencies is recorded exactly once for the
// operation as a whole. This is what the previous version of the code
// silently violated — attempts on the session path were never recorded.
func TestSessionTableReadRow_RetriesRecordAttemptPerAttempt(t *testing.T) {
	retriable := btransport.TagErr(btransport.StateUncommitted, status.Error(codes.Unavailable, "test"))
	inv := &fakeInvoker{
		errBefore: 2,
		err:       retriable,
		result:    btransport.InvokeResult{Response: &btpb.SessionReadRowResponse{}},
	}
	tbl, reader := newTestTable(t, inv, nil)

	if _, err := tbl.ReadRow(context.Background(), &btpb.SessionReadRowRequest{Key: []byte("row1")}); err != nil {
		t.Fatalf("ReadRow: %v", err)
	}
	if got := inv.callCount(); got != 3 {
		t.Fatalf("invoker calls = %d, want 3", got)
	}
	assertSamples(t, reader, "attempt_latencies", 3)
	assertSamples(t, reader, "attempt_latencies2", 3)
	assertSamples(t, reader, "operation_latencies", 1)
}
