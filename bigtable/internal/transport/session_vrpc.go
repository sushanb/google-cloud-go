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
	"fmt"
	"time"

	spb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// InvokeResult carries the full set of outputs from a single Invoke call.
//
// Fields:
//   - Response: decoded vRPC payload (typed per VRpcDescriptor.Decode); nil on error.
//   - ClusterInfo: server-reported routing/cluster identity; may be set on
//     both success and error paths if the server included it.
//   - Stats: server-reported per-request statistics (notably BackendLatency);
//     nil if the server did not populate Stats on the success frame.
//   - SentAt: local monotonic timestamp captured immediately before the vRPC
//     frame was handed to the bidi Send. Used downstream to derive
//     client-side blocking latency (sentAt - attemptStart).
//
// ErrorResponse.RetryInfo from the server is plumbed via the returned error
// using gRPC status details — callers can extract it with
// status.FromError(err).Details() and type-asserting to *errdetails.RetryInfo
// (this is exactly how RetryingVRpc already consumes it).
type InvokeResult struct {
	Response    interface{}
	ClusterInfo *spb.ClusterInformation
	Stats       *spb.SessionRequestStats
	SentAt      time.Time
	// RpcIDOnSession is the per-session monotonic id of this call
	// (1, 2, 3, …). Distinguishes warm-up vRPCs (small id) from
	// established-session vRPCs.
	RpcIDOnSession int64
	// TransportLatency is the time between the vRPC frame being handed
	// to the bidi Send and the response (or server-side error) arriving
	// on the stream. Approximates network RTT + server queue + Backend;
	// (TransportLatency - BackendLatency) surfaces "everything except
	// server processing". Zero when Invoke returned before a Recv event
	// (context cancellation or pre-Send failure).
	TransportLatency time.Duration
}

// Invoke executes a single virtual RPC on this session and returns every
// observable output of the roundtrip — decoded response, cluster info,
// server-reported Stats, and the local SentAt timestamp — so callers can
// populate metrics (client_blocking_latency, server_backend_latency) and
// respect server-supplied retry hints without losing data on the way out of
// the transport.
//
// The caller MUST have exclusive access to this Session for the duration
// of the call — the pool guarantees this via CheckoutSession's per-session
// idle-slot gate. The single-in-flight invariant is enforced with a
// CompareAndSwap on activeRPC; a failing CAS means the caller bypassed
// the pool gate and is a programming error, not a runtime condition. This
// replaces a golang.org/x/sync/semaphore that added two channel ops per
// call on the hot path.
func (s *Session) Invoke(ctx context.Context, desc VRpcDescriptor, req interface{}) (result InvokeResult, err error) {
	startTime := time.Now()

	if st := State(s.state.Load()); st != StateReady {
		return InvokeResult{}, tagErr(StateUncommitted,
			unavailable(ErrSessionNotActive, "session is not active (state: %v)", st))
	}

	reqBytes, err := desc.Encode(req)
	if err != nil {
		return InvokeResult{}, tagErr(StateUncommitted, fmt.Errorf("encode vRPC request: %w", err))
	}

	rpcID := s.nextRPCID.Add(1)
	result.RpcIDOnSession = rpcID
	rpc := &vrpcImpl{
		id:         rpcID,
		method:     desc.Method(),
		resultChan: make(chan vrpcResult, 1),
	}

	// Claim the single in-flight slot. See method doc — a losing CAS is
	// a caller-side serialization bug, not a runtime backoff condition.
	if !s.activeRPC.CompareAndSwap(nil, rpc) {
		return InvokeResult{}, tagErr(StateUncommitted,
			unavailable(ErrSessionNotActive,
				"concurrent Invoke on session %q: multiPlexingLimit=1 violated", s.logName))
	}
	defer func() {
		s.activeRPC.CompareAndSwap(rpc, nil)
		// Order matters: clear the slot first, THEN check state. Close()
		// transitions to StateClosing first and only then observes
		// activeRPC — so at least one side signals. signalQuiescent is
		// once-guarded, so a double-signal is harmless.
		if State(s.state.Load()) == StateClosing {
			s.signalQuiescent()
		}
	}()

	// Reset the heartbeat deadline whenever we send an outbound frame: the
	// server's keepalive clock is implicitly reset by our activity.
	s.resetHeartbeatDeadline()

	attempt := int64(VRpcAttempt(ctx))
	if attempt == 0 {
		// Calls bypassing the retry interceptor have no attempt set in the
		// context; treat them as the first attempt.
		attempt = 1
	}
	if attempt > 1 {
		s.retries.Add(1)
		// Capture WHY this is a retry — the previous attempt's error
		// was stashed in ctx by RetryingVRpc. Without this the
		// per-session Retries counter is opaque ("we retried, but
		// why?"); with it sessionz surfaces a "retry" event tagged
		// with the prior gRPC code + message.
		if prev := PrevAttemptErr(ctx); prev != nil {
			prevCode := status.Code(prev).String()
			s.debugf("retry attempt=%d method=%s prev_code=%s prev_err=%v",
				attempt, desc.Method(), prevCode, prev)
			s.recordEvent("retry", "attempt=%d method=%s prev_code=%s prev_err=%v",
				attempt, desc.Method(), prevCode, prev)
		}
	}
	virtRpc := &spb.VirtualRpcRequest{
		RpcId:   rpcID,
		Payload: reqBytes,
		Metadata: &spb.VirtualRpcRequest_Metadata{
			AttemptNumber: attempt,
			AttemptStart:  timestamppb.New(startTime),
		},
	}
	ctxDeadline, hasDeadline := ctx.Deadline()
	if hasDeadline {
		if remaining := time.Until(ctxDeadline); remaining > 0 {
			virtRpc.Deadline = durationpb.New(remaining)
		}
	}
	sessionReq := &spb.SessionRequest{
		Payload: &spb.SessionRequest_VirtualRpc{
			VirtualRpc: virtRpc,
		},
	}
	// Capture SentAt immediately before the frame is handed to Send so
	// downstream metrics can compute client-side blocking latency as
	// (SentAt - attemptStart) without double-counting encode/setup overhead.
	sentAt := time.Now()
	result.SentAt = sentAt
	if err := s.Send(sessionReq); err != nil {
		return result, tagErr(StateTransportFailure, fmt.Errorf("send vRPC request: %w", err))
	}

	select {
	case <-ctx.Done():
		stillActive := s.activeRPC.Load() == rpc
		sessState := State(s.state.Load())
		waited := time.Since(sentAt)
		peer := s.peerInfoSummary()
		s.debugf("vRPC %s rpc_id=%d ctx.Done waited=%v err=%v session_state=%v still_in_flight=%v %s",
			desc.Method(), rpcID, waited, ctx.Err(), sessState, stillActive, peer)
		s.recordEvent("ctx-done", "method=%s rpc_id=%d waited=%v err=%v session_state=%v still_in_flight=%v %s",
			desc.Method(), rpcID, waited, ctx.Err(), sessState, stillActive, peer)
		return result, tagErr(StateTransportFailure, ctx.Err())
	case res := <-rpc.resultChan:
		result.TransportLatency = time.Since(sentAt)
		result.ClusterInfo = res.clusterInfo
		if res.clusterInfo != nil {
			s.recordCluster(res.clusterInfo.GetClusterId())
		}
		if res.err != nil {
			// res.err arrives from two sources: cancelActiveRPCs (session
			// died mid-call — always TransportFailure) and
			// handleVRPCErrorResponse (real server ErrorResponse frame —
			// always ServerResult). Both source sites now tag with the
			// correct AttemptState via tagErr before writing to resultChan,
			// so this path just forwards the tagged err as-is.
			return result, res.err
		}
		if res.resp.RpcId != rpcID {
			return result, tagErr(StateServerResult,
				fmt.Errorf("internal: response RpcId %d does not match request RpcId %d", res.resp.RpcId, rpcID))
		}
		respMsg, decodeErr := desc.Decode(res.resp.Payload)
		if decodeErr != nil {
			return result, tagErr(StateServerResult, fmt.Errorf("decode vRPC response: %w", decodeErr))
		}
		result.Response = respMsg
		result.Stats = res.resp.Stats
		if res.resp.Stats != nil && res.resp.Stats.BackendLatency != nil {
			s.recordLatency(res.resp.Stats.BackendLatency.AsDuration())
		}
		return result, nil
	}
}

// handleVRPCResponse delivers a server VirtualRpcResponse to the waiting
// Invoke caller, if any.
func (s *Session) handleVRPCResponse(resp *spb.VirtualRpcResponse) {
	rpc := s.activeRPC.Load()
	if rpc == nil || rpc.id != resp.RpcId {
		s.debugf("dropping VirtualRpcResponse for unknown rpc_id=%d", resp.RpcId)
		return
	}
	s.okRpcs.Add(1)
	s.deliver(rpc, vrpcResult{resp: resp, clusterInfo: resp.ClusterInfo})
}

// handleVRPCErrorResponse routes per-vRPC errors to the waiting caller.
// Session-level errors (rpc_id == 0) are handled in handleSessionResponse.
func (s *Session) handleVRPCErrorResponse(errResp *spb.ErrorResponse) {
	rpc := s.activeRPC.Load()
	if rpc == nil || rpc.id != errResp.RpcId {
		s.debugf("dropping ErrorResponse for unknown rpc_id=%d", errResp.RpcId)
		return
	}
	s.errorRpcs.Add(1)

	var goErr error
	if errResp.Status != nil {
		st := status.FromProto(errResp.Status)
		// If the server attached RetryInfo to the ErrorResponse envelope,
		// pack it into the status details so downstream consumers
		// (notably RetryingVRpc) can recover it via status.FromError(err).
		// .Details() — the same path they already use for inline retry
		// hints. WithDetails returns a fresh *Status on success; on the
		// rare failure (e.g. anypb marshal) we fall back to the bare
		// status so the error path still propagates the server's code.
		if errResp.RetryInfo != nil {
			if withDetails, derr := st.WithDetails(errResp.RetryInfo); derr == nil {
				st = withDetails
			}
		}
		goErr = st.Err()
	} else {
		goErr = fmt.Errorf("unknown vRPC error (rpc_id=%d)", errResp.RpcId)
	}
	// Real server ErrorResponse frame → ServerResult. Retry decision at
	// the interceptor gates on the underlying gRPC code + any RetryInfo.
	s.deliver(rpc, vrpcResult{err: tagErr(StateServerResult, goErr), clusterInfo: errResp.ClusterInfo})
}

// deliver writes a result onto the RPC's buffered (cap 1) channel. The
// non-blocking send protects against duplicate server frames for the same
// rpc_id; the first wins, subsequent ones are dropped.
func (s *Session) deliver(rpc *vrpcImpl, res vrpcResult) {
	select {
	case rpc.resultChan <- res:
	default:
		s.debugf("duplicate result for rpc_id=%d (%s) dropped", rpc.id, rpc.method)
	}
}

// cancelActiveRPCs cancels the in-flight vRPC (if any) with the given
// error. With multiPlexingLimit=1 there is at most one such vRPC.
// Clear-then-deliver so a racing handleVRPCResponse can't double-deliver
// on the same slot.
func (s *Session) cancelActiveRPCs(err error) {
	rpc := s.activeRPC.Load()
	if rpc == nil {
		return
	}
	if !s.activeRPC.CompareAndSwap(rpc, nil) {
		// Concurrent completion cleared the slot; the caller already
		// received a result. Nothing to cancel.
		return
	}
	// Session-side cancellation: session died / GoAway / heartbeat missed
	// / benign shutdown while an RPC was in-flight. Server may or may not
	// have processed — TransportFailure classification lets idempotent ops
	// retry and prevents non-idempotent ones from double-applying.
	s.deliver(rpc, vrpcResult{err: tagErr(StateTransportFailure, err)})
}
