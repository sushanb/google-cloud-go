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
	"encoding/base64"
	"fmt"
	"time"

	spb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
	"google.golang.org/protobuf/proto"
)

const peerInfoHeaderKey = "bigtable-peer-info"

// Start opens the session by sending OpenSessionRequest, then launches the
// read and heartbeat loops. ctx governs the loops; cancelling it forces the
// session closed. Unblocking the underlying Recv requires the caller to also
// cancel the stream's context.
func (s *Session) Start(ctx context.Context, req *spb.OpenSessionRequest) error {
	s.mu.Lock()
	if s.state != StateNew {
		st := s.state
		s.mu.Unlock()
		return fmt.Errorf("session already started or closed (state: %v)", st)
	}
	s.state = StateStarting
	s.lastStateChange = time.Now()
	s.mu.Unlock()

	openReq := &spb.SessionRequest{
		Payload: &spb.SessionRequest_OpenSession{OpenSession: req},
	}
	if err := s.Send(openReq); err != nil {
		s.ForceClose(&spb.CloseSessionRequest{
			Reason:      spb.CloseSessionRequest_CLOSE_SESSION_REASON_ERROR,
			Description: "failed to send open session request",
		})
		return fmt.Errorf("send open session request: %w", err)
	}

	go s.readLoop(ctx)
	go s.heartBeatLoop(ctx)

	if s.listener != nil {
		s.listener.OnStart(ctx)
	}
	return nil
}

// ForceClose immediately transitions the session to StateClosed and cancels
// every in-flight RPC. It is safe to call multiple times; only the first call
// fires listener/tracer callbacks.
func (s *Session) ForceClose(req *spb.CloseSessionRequest) {
	s.mu.Lock()
	if s.state == StateClosed {
		s.mu.Unlock()
		return
	}
	s.state = StateClosed
	s.lastStateChange = time.Now()
	s.mu.Unlock()

	desc := "session force closed"
	if req != nil && req.Description != "" {
		desc = fmt.Sprintf("session force closed: %s", req.Description)
	}
	cause := closeReasonToCause(req)
	s.cancelActiveRPCs(unavailable(cause, "%s", desc), nil)

	s.notifyClosed(nil)
}

// notifyClosed fires tracer.recordClose and listener.OnClose exactly once over
// the lifetime of a Session, regardless of how many code paths race to close.
func (s *Session) notifyClosed(streamErr error) {
	s.mu.Lock()
	if s.closeNotified {
		s.mu.Unlock()
		return
	}
	s.closeNotified = true
	s.mu.Unlock()

	s.tracer.recordClose(context.Background())
	if s.listener != nil {
		s.listener.OnClose(s, streamErr)
	}
}

// Close requests a graceful shutdown: it transitions to StateClosing, waits
// for in-flight RPCs to drain (or for ctx to fire), sends CloseSessionRequest,
// and transitions to StateWaitServerClose. The server's EOF eventually drives
// handleClose, which moves to StateClosed.
func (s *Session) Close(ctx context.Context, req *spb.CloseSessionRequest) error {
	s.mu.Lock()
	switch s.state {
	case StateClosed, StateClosing, StateWaitServerClose:
		s.mu.Unlock()
		return nil
	}
	s.state = StateClosing
	s.lastStateChange = time.Now()
	s.mu.Unlock()

	ticker := time.NewTicker(closeDrainPollPeriod)
	defer ticker.Stop()
	for {
		s.mu.Lock()
		state := s.state
		active := len(s.activeRPCs)
		s.mu.Unlock()
		// If a concurrent ForceClose flipped us to StateClosed during the drain,
		// stop trying to advance to WaitServerClose; the close is already done.
		if state == StateClosed {
			return nil
		}
		if active == 0 {
			break
		}
		select {
		case <-ctx.Done():
			// Propagate ctx error as the close cause; ignore req-derived cause
			// because the user's req describes intent, not the actual failure.
			s.ForceClose(nil)
			return ctx.Err()
		case <-ticker.C:
		}
	}

	// Only advance to WaitServerClose if we are still in Closing; ForceClose
	// may have raced us in between the drain check and this transition.
	s.mu.Lock()
	if s.state != StateClosing {
		s.mu.Unlock()
		return nil
	}
	s.state = StateWaitServerClose
	s.lastStateChange = time.Now()
	s.mu.Unlock()

	closeReq := &spb.SessionRequest{
		Payload: &spb.SessionRequest_CloseSession{CloseSession: req},
	}
	if err := s.Send(closeReq); err != nil {
		s.ForceClose(nil)
		return fmt.Errorf("send close session request: %w", err)
	}
	return nil
}

// Send writes a SessionRequest under sendMu so concurrent producers don't
// corrupt the underlying stream.
func (s *Session) Send(req *spb.SessionRequest) error {
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	return s.stream.Send(req)
}

// readLoop drives the inbound side of the stream until Recv returns an error.
func (s *Session) readLoop(ctx context.Context) {
	// Extract peer info from the header metadata asynchronously so we don't
	// block reads on the header arriving.
	go func() {
		headerMD, err := s.stream.Header()
		if err != nil {
			s.debugf("stream Header() failed: %v", err)
			return
		}
		s.peerInfoExtracter(headerMD.Get(peerInfoHeaderKey))
	}()

	// Supervisor: if ctx is cancelled, mark the session closed so callers
	// observe state immediately. Unblocking Recv() requires the underlying
	// stream's context to be cancelled by the caller.
	readLoopDone := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			s.ForceClose(&spb.CloseSessionRequest{
				Reason:      spb.CloseSessionRequest_CLOSE_SESSION_REASON_USER,
				Description: "client context cancelled",
			})
		case <-readLoopDone:
		}
	}()
	defer close(readLoopDone)

	for {
		resp, err := s.stream.Recv()
		if err != nil {
			s.handleClose(err)
			return
		}
		s.handleSessionResponse(resp)
	}
}

// handleSessionResponse dispatches every SessionResponse oneof variant.
// Receiving any recognized frame resets the heartbeat watchdog; unknown
// frames do NOT, so a misbehaving server cannot keep the watchdog satisfied
// with junk payloads.
func (s *Session) handleSessionResponse(resp *spb.SessionResponse) {
	switch p := resp.GetPayload().(type) {
	case *spb.SessionResponse_OpenSession:
		s.handleOpenSession(p.OpenSession)
	case *spb.SessionResponse_VirtualRpc:
		s.handleVRPCResponse(p.VirtualRpc)
	case *spb.SessionResponse_Error:
		s.handleErrorResponse(p.Error)
	case *spb.SessionResponse_SessionParameters:
		s.handleSessionParameters(p.SessionParameters)
	case *spb.SessionResponse_Heartbeat:
		// No-op; the deadline reset below is the only effect.
	case *spb.SessionResponse_GoAway:
		s.handleGoAway(p.GoAway)
	case *spb.SessionResponse_SessionRefreshConfig:
		s.handleSessionRefreshConfig(p.SessionRefreshConfig)
	default:
		s.debugf("received SessionResponse with unknown payload type %T", p)
		return
	}
	s.resetHeartbeatDeadline()
}

// handleOpenSession transitions Starting -> Active and signals listeners.
func (s *Session) handleOpenSession(_ *spb.OpenSessionResponse) {
	s.mu.Lock()
	starting := s.state == StateStarting
	if starting {
		s.state = StateActive
		s.lastStateChange = time.Now()
		close(s.handshakeDone)
	}
	s.mu.Unlock()

	if !starting {
		return
	}
	s.tracer.recordOpen(context.Background(), nil)
	if s.listener != nil {
		s.listener.OnActive(s)
	}
}

// handleErrorResponse splits per-RPC errors (rpc_id != 0) from session-level
// errors (rpc_id == 0). Session-level errors force-close the session;
// ForceClose then cancels all in-flight RPCs with ErrUnavailableSessionError.
func (s *Session) handleErrorResponse(errResp *spb.ErrorResponse) {
	if errResp.GetRpcId() != 0 {
		s.handleVRPCErrorResponse(errResp)
		return
	}
	desc := "server reported session-level error"
	if errResp.Status != nil && errResp.Status.Message != "" {
		desc = fmt.Sprintf("server session error: %s", errResp.Status.Message)
	}
	s.debugf("%s", desc)
	s.ForceClose(&spb.CloseSessionRequest{
		Reason:      spb.CloseSessionRequest_CLOSE_SESSION_REASON_ERROR,
		Description: desc,
	})
}

// handleSessionParameters updates the heartbeat interval negotiated by the
// server and immediately recomputes the watchdog deadline against the new
// interval.
func (s *Session) handleSessionParameters(params *spb.SessionParametersResponse) {
	if params.KeepAlive == nil {
		return
	}
	interval := params.KeepAlive.AsDuration()
	if interval <= 0 {
		return
	}
	s.mu.Lock()
	s.heartbeatInterval = interval
	s.nextHeartbeatDeadline = time.Now().Add(3 * interval)
	s.mu.Unlock()
}

// handleSessionRefreshConfig stores the server-provided refresh configuration
// for later use by reconnection logic.
func (s *Session) handleSessionRefreshConfig(cfg *spb.SessionRefreshConfig) {
	s.mu.Lock()
	s.refreshConfig = cfg
	s.mu.Unlock()
	s.debugf("stored SessionRefreshConfig (optimized_open=%t, metadata_entries=%d)",
		cfg.GetOptimizedOpenRequest() != nil, len(cfg.GetMetadata()))
}

// handleGoAway transitions to StateClosing and cancels every RPC with an id
// greater than the last admitted one.
func (s *Session) handleGoAway(goAway *spb.GoAwayResponse) {
	s.mu.Lock()
	s.state = StateClosing
	s.lastStateChange = time.Now()
	s.mu.Unlock()

	lastAdmitted := goAway.GetLastRpcIdAdmitted()
	s.debugf("received GOAWAY reason=%q description=%q last_rpc_id_admitted=%d",
		goAway.GetReason(), goAway.GetDescription(), lastAdmitted)

	err := unavailable(ErrUnavailableGoAway,
		"vRPC not admitted before GOAWAY (last_admitted=%d)", lastAdmitted)
	s.cancelActiveRPCs(err, func(id int64) bool { return id > lastAdmitted })
}

// handleClose is invoked when Recv returns an error. It transitions to
// StateClosed and cancels every remaining in-flight RPC.
func (s *Session) handleClose(err error) {
	s.mu.Lock()
	if s.state == StateClosed {
		s.mu.Unlock()
		return
	}
	wasStarting := s.state == StateStarting || s.state == StateNew
	s.state = StateClosed
	s.lastStateChange = time.Now()
	if wasStarting {
		s.handshakeErr = err
		select {
		case <-s.handshakeDone:
		default:
			close(s.handshakeDone)
		}
	}
	s.mu.Unlock()

	s.cancelActiveRPCs(unavailable(err, "session closed: %v", err), nil)
	s.notifyClosed(err)
}

// resetHeartbeatDeadline pushes out the watchdog to (3 * heartbeatInterval)
// from now. The 3x multiplier follows the protocol guidance of tolerating two
// missed heartbeats.
func (s *Session) resetHeartbeatDeadline() {
	s.mu.Lock()
	s.nextHeartbeatDeadline = time.Now().Add(3 * s.heartbeatInterval)
	s.mu.Unlock()
}

// heartBeatLoop polls at heartbeatPollPeriod and force-closes the session if
// nextHeartbeatDeadline has elapsed.
func (s *Session) heartBeatLoop(ctx context.Context) {
	ticker := time.NewTicker(heartbeatPollPeriod)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.mu.Lock()
			if s.state == StateClosed {
				s.mu.Unlock()
				return
			}
			missed := time.Now().After(s.nextHeartbeatDeadline)
			s.mu.Unlock()
			if missed {
				s.ForceClose(&spb.CloseSessionRequest{
					Reason:      spb.CloseSessionRequest_CLOSE_SESSION_REASON_MISSED_HEARTBEAT,
					Description: "client terminated session due to missed server heartbeats",
				})
				return
			}
		}
	}
}

// peerInfoExtracter parses the base64-encoded peer info header and caches
// the decoded PeerInfo on the session.
func (s *Session) peerInfoExtracter(peerInfoData []string) {
	if len(peerInfoData) == 0 {
		return
	}
	encodings := []*base64.Encoding{
		base64.RawURLEncoding,
		base64.StdEncoding,
		base64.RawStdEncoding,
	}
	var decoded []byte
	var lastErr error
	for _, enc := range encodings {
		d, err := enc.DecodeString(peerInfoData[0])
		if err == nil {
			decoded = d
			lastErr = nil
			break
		}
		lastErr = err
	}
	if lastErr != nil {
		s.debugf("decode base64 PeerInfo failed: %v", lastErr)
		return
	}
	var peerInfo spb.PeerInfo
	if err := proto.Unmarshal(decoded, &peerInfo); err != nil {
		s.debugf("unmarshal PeerInfo proto failed: %v", err)
		return
	}
	s.mu.Lock()
	s.peerInfo = &peerInfo
	logName := s.logName
	s.mu.Unlock()
	s.tracer.setPeerInfo(&peerInfo, logName)
	s.debugf("parsed PeerInfo: transport_type=%s afe=%s",
		peerInfo.GetTransportType(), peerInfo.GetApplicationFrontendSubzone())
}

// closeReasonToCause maps a CloseSessionRequest reason to a sentinel error,
// or nil when the close is benign (user-initiated, downsize, unset). Callers
// of unavailable(nil, …) get a codes.Unavailable error without a wrapped
// sentinel, so errors.Is against the session sentinels returns false — which
// is the correct signal for "this was an expected shutdown."
func closeReasonToCause(req *spb.CloseSessionRequest) error {
	if req == nil {
		return nil
	}
	switch req.Reason {
	case spb.CloseSessionRequest_CLOSE_SESSION_REASON_MISSED_HEARTBEAT:
		return ErrUnavailableHeartBeatMissed
	case spb.CloseSessionRequest_CLOSE_SESSION_REASON_GOAWAY:
		return ErrUnavailableGoAway
	case spb.CloseSessionRequest_CLOSE_SESSION_REASON_ERROR:
		return ErrUnavailableSessionError
	default:
		return nil
	}
}
