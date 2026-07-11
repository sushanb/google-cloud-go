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

	spb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
)

// benchDesc is a zero-alloc VRpcDescriptor for the benchmark hot path.
// Encode returns a shared byte slice; Decode returns a nil interface.
type benchDesc struct{ payload []byte }

func (b *benchDesc) Method() string                         { return "BenchmarkVRpc" }
func (b *benchDesc) Encode(interface{}) ([]byte, error)     { return b.payload, nil }
func (b *benchDesc) Decode([]byte) (interface{}, error)     { return nil, nil }

// autoResponder wires a fakeStream so every VRpc Send is immediately
// answered by handleVRPCResponse on a helper goroutine. That closes the
// Invoke loop deterministically without a real gRPC bidi. Reused by the
// sequential and parallel benchmarks.
func autoResponder(s *Session, stream *fakeStream) {
	stream.sendFn = func(req *spb.SessionRequest) error {
		vr := req.GetVirtualRpc()
		if vr == nil {
			return nil
		}
		rpcID := vr.RpcId
		go s.handleVRPCResponse(&spb.VirtualRpcResponse{
			RpcId:   rpcID,
			Payload: nil,
		})
		return nil
	}
}

// setupBenchSession returns a Ready session with an autoresponder wired.
// Test-side Cleanup handles teardown (shuts the syncCtx runner).
func setupBenchSession(b *testing.B) (*Session, *fakeStream, *benchDesc) {
	b.Helper()
	stream := newFakeStream()
	s := NewSession("bench-session", stream, SessionHooks{}, SessionTypeTable)
	s.state.Store(int32(StateReady))
	b.Cleanup(func() {
		s.syncC.Shutdown()
		stream.Close()
	})
	autoResponder(s, stream)
	return s, stream, &benchDesc{payload: []byte("bench")}
}

// BenchmarkInvokeMicro exercises one full round of Invoke against a
// Ready session with a synthetic in-process responder. The path we care
// about — state check, Encode, syncCtx (POC) or plain CAS (baseline),
// SendVRpc, awaitInvokeResult — is what benchstat compares between
// branches.
func BenchmarkInvokeMicro(b *testing.B) {
	b.Run("sequential", func(b *testing.B) {
		s, _, desc := setupBenchSession(b)
		ctx := context.Background()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := s.Invoke(ctx, desc, nil); err != nil {
				b.Fatalf("iter %d: %v", i, err)
			}
		}
	})

	// Parallel variant intentionally over-subscribes the single Session
	// (multiPlexingLimit=1), so most goroutines observe a filled slot and
	// bail out with StateUncommitted. This surfaces syncCtx / CAS
	// contention rather than steady-state throughput. Useful for A/B
	// comparison of the reject path, but do NOT read the ns/op as a
	// realistic Invoke cost.
	b.Run("parallel-contended", func(b *testing.B) {
		s, _, desc := setupBenchSession(b)
		ctx := context.Background()
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				// Ignore per-iter err: contended CAS-fail returns
				// ErrSessionNotActive, which is expected and what we're
				// measuring.
				_, _ = s.Invoke(ctx, desc, nil)
			}
		})
	})
}

// BenchmarkSyncCtxOverhead measures the primitive alone (no session
// state involved) so the syncCtx CPU cost can be attributed cleanly
// from Invoke's overall cost.
func BenchmarkSyncCtxOverhead(b *testing.B) {
	sc := newSyncCtx()
	b.Cleanup(sc.Shutdown)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sc.ExecuteSync(func() {})
	}
}
