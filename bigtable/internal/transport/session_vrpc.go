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
)

// ExecuteVRpc executes a single virtual RPC over the session stream and
// blocks for its response. Calls are serialized by vrpcSem; concurrent callers
// queue behind the in-flight RPC until the semaphore is released.
func (s *Session) ExecuteVRpc(ctx context.Context, desc VRpcDescriptor, req interface{}) (resp interface{}, clusterInfo *spb.ClusterInformation, err error) {
	if err := s.vrpcSem.Acquire(ctx, multiPlexingLimit); err != nil {
		return nil, nil, err
	}
	defer s.vrpcSem.Release(multiPlexingLimit)

	startTime := time.Now()
	defer func() {
		s.tracer.recordOperation(ctx, startTime, desc.Method(), err)
	}()

	s.mu.Lock()
	if s.state != StateActive {
		st := s.state
		s.mu.Unlock()
		return nil, nil, unavailable(ErrSessionNotActive, "session is not active (state: %v)", st)
	}
	s.mu.Unlock()

	reqBytes, err := desc.Encode(req)
	if err != nil {
		return nil, nil, fmt.Errorf("encode vRPC request: %w", err)
	}

	rpcID := s.nextRPCID.Add(1)
	rpc := &vrpcImpl{
		id:         rpcID,
		method:     desc.Method(),
		resultChan: make(chan vrpcResult, 1),
	}

	s.mu.Lock()
	s.activeRPCs[rpcID] = rpc
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.activeRPCs, rpcID)
		s.mu.Unlock()
	}()

	// Reset the heartbeat deadline whenever we send an outbound frame: the
	// server's keepalive clock is implicitly reset by our activity.
	s.resetHeartbeatDeadline()

	sessionReq := &spb.SessionRequest{
		Payload: &spb.SessionRequest_VirtualRpc{
			VirtualRpc: &spb.VirtualRpcRequest{
				RpcId:   rpcID,
				Payload: reqBytes,
			},
		},
	}
	if err := s.Send(sessionReq); err != nil {
		return nil, nil, fmt.Errorf("send vRPC request: %w", err)
	}

	select {
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	case res := <-rpc.resultChan:
		if res.err != nil {
			return nil, res.clusterInfo, res.err
		}
		if res.resp.RpcId != rpcID {
			return nil, res.clusterInfo, fmt.Errorf("internal: response RpcId %d does not match request RpcId %d", res.resp.RpcId, rpcID)
		}
		respMsg, decodeErr := desc.Decode(res.resp.Payload)
		if decodeErr != nil {
			return nil, res.clusterInfo, fmt.Errorf("decode vRPC response: %w", decodeErr)
		}
		return respMsg, res.clusterInfo, nil
	}
}

// handleVRPCResponse delivers a server VirtualRpcResponse to the waiting
// ExecuteVRpc caller, if any.
func (s *Session) handleVRPCResponse(resp *spb.VirtualRpcResponse) {
	s.mu.Lock()
	rpc, ok := s.activeRPCs[resp.RpcId]
	if ok {
		s.hasOkRpcs = true
	}
	s.mu.Unlock()

	if !ok {
		s.debugf("dropping VirtualRpcResponse for unknown rpc_id=%d", resp.RpcId)
		return
	}
	s.deliver(rpc, vrpcResult{resp: resp, clusterInfo: resp.ClusterInfo})
}

// handleVRPCErrorResponse routes per-vRPC errors to the waiting caller.
// Session-level errors (rpc_id == 0) are handled in handleSessionResponse.
func (s *Session) handleVRPCErrorResponse(errResp *spb.ErrorResponse) {
	s.mu.Lock()
	rpc, ok := s.activeRPCs[errResp.RpcId]
	if ok {
		s.hasErrorRpcs = true
	}
	s.mu.Unlock()

	if !ok {
		s.debugf("dropping ErrorResponse for unknown rpc_id=%d", errResp.RpcId)
		return
	}

	var goErr error
	if errResp.Status != nil {
		goErr = status.FromProto(errResp.Status).Err()
	} else {
		goErr = fmt.Errorf("unknown vRPC error (rpc_id=%d)", errResp.RpcId)
	}
	s.deliver(rpc, vrpcResult{err: goErr, clusterInfo: errResp.ClusterInfo})
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

// cancelActiveRPCs removes and notifies every in-flight RPC matching filter
// (or all, if filter is nil) with the given error.
func (s *Session) cancelActiveRPCs(err error, filter func(rpcID int64) bool) {
	s.mu.Lock()
	cancelled := make([]*vrpcImpl, 0, len(s.activeRPCs))
	for id, rpc := range s.activeRPCs {
		if filter == nil || filter(id) {
			cancelled = append(cancelled, rpc)
			delete(s.activeRPCs, id)
		}
	}
	s.mu.Unlock()

	for _, rpc := range cancelled {
		s.deliver(rpc, vrpcResult{err: err})
	}
}
