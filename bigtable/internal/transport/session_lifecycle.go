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
	"errors"
	"fmt"
	"io"
	"time"

	spb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const peerInfoHeaderKey = "bigtable-peer-info"

// Start opens the session by sending OpenSessionRequest, then launches the
// read and heartbeat loops. ctx governs the loops; cancelling it forces the
// session closed. Unblocking the underlying Recv requires the caller to also
// cancel the stream's context.
func (s *Session) Start(ctx context.Context, req *spb.OpenSessionRequest) error {
	if prev, ok := s.transitionTo(StateStarting, isState(StateNew)); !ok {
		return fmt.Errorf("session already started or closed (state: %v)", prev)
	}

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

	s.hooks.onStart(ctx)
	return nil
}

// ForceClose immediately transitions the session to StateClosed and cancels
// every in-flight RPC. It is safe to call multiple times; only the first call
// fires listener/tracer callbacks.
func (s *Session) ForceClose(req *spb.CloseSessionRequest) {
	if _, ok := s.transitionTo(StateClosed, notState(StateClosed)); !ok {
		return
	}

	s.setCloseReason(closeReasonLabel(req))
	desc := "session force closed"
	if req != nil && req.Description != "" {
		desc = "session force closed: " + req.Description
	}
	s.cancelActiveRPCs(unavailable(closeReasonToCause(req), "%s", desc), nil)
	s.signalQuiescent()
	s.notifyClosed(nil)
}

// notifyClosed fires tracer.recordClose and listener.OnClose exactly once over
// the lifetime of a Session.
func (s *Session) notifyClosed(streamErr error) {
	s.closeOnce.Do(func() {
		s.tracer.recordClose(context.Background())
		s.hooks.onClose(s, streamErr)
	})
}

// Close requests a graceful shutdown:
//  1. Transitions to StateClosing (no-op if already Closing — callers like
//     handleGoAway invoke this after the state was advanced upstream).
//  2. Waits for in-flight RPCs to drain (or for ctx to fire).
//  3. Sends CloseSessionRequest to the server.
//  4. Transitions to StateWaitServerClose.
//  5. The server's EOF eventually drives handleClose → StateClosed.
//
// A pool-side monitor (see SessionPoolImpl.startStuckSessionMonitor)
// force-closes sessions stuck in StateWaitServerClose past a grace period
// so an unresponsive server can't leak Closing sessions indefinitely.
func (s *Session) Close(ctx context.Context, req *spb.CloseSessionRequest) error {
	// Allow being called from Closing too — handleGoAway already
	// transitioned and now wants the drain + send + WaitServerClose dance.
	s.transitionTo(StateClosing, isState(StateNew, StateStarting, StateActive))
	st := s.State()
	if st != StateClosing {
		// Already past Closing (WaitServerClose / Closed); nothing to do.
		return nil
	}
	// Record the intended reason now — the eventual handleClose (driven by
	// the server's EOF) would otherwise stamp "StreamEnd" over it and the
	// downstream OnClose hook would see the wrong label. setCloseReason is
	// CompareAndSwap-once so callers like handleGoAway that stamped earlier
	// win over this one.
	s.setCloseReason(closeReasonLabel(req))

	// Wait for active RPCs to drain. The quiescent channel is closed by the
	// last RPC's cleanup defer when it sees state==Closing && empty, or by
	// ForceClose if it races us.
	s.mu.Lock()
	empty := len(s.activeRPCs) == 0
	s.mu.Unlock()
	if empty {
		s.signalQuiescent()
	} else {
		select {
		case <-s.quiescent:
		case <-ctx.Done():
			s.ForceClose(nil)
			return ctx.Err()
		}
	}

	// If ForceClose raced us during the drain, our work is done.
	if s.State() == StateClosed {
		return nil
	}

	closeReq := &spb.SessionRequest{
		Payload: &spb.SessionRequest_CloseSession{CloseSession: req},
	}
	if err := s.Send(closeReq); err != nil {
		s.ForceClose(nil)
		return fmt.Errorf("send close session request: %w", err)
	}
	// Advance to WaitServerClose so the pool monitor can see we're waiting
	// on the server. handleClose accepts StateWaitServerClose → Closed.
	s.transitionTo(StateWaitServerClose, isState(StateClosing))
	return nil
}

// Send writes a SessionRequest under sendMu so concurrent producers don't
// corrupt the underlying stream.
func (s *Session) Send(req *spb.SessionRequest) error {
	s.sendMu.Lock()
	err := s.stream.Send(req)
	s.sendMu.Unlock()
	if err == nil {
		s.msgsSent.Add(1)
		s.msgsSentByType[classifyReq(req)].Add(1)
	}
	return err
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
		s.msgsRecv.Add(1)
		s.msgsRecvByType[classifyResp(resp)].Add(1)
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
		// Server emits Heartbeats during long-running VRPCs; the deadline
		// reset below is what keeps the watchdog from firing on them.
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
	if _, ok := s.transitionTo(StateActive, isState(StateStarting)); !ok {
		return
	}
	s.tracer.recordOpen(context.Background(), nil)
	s.hooks.onActive(s)
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

// handleGoAway processes a server-initiated GoAway:
//  1. Transitions to StateClosing (no backwards motion from terminal states).
//  2. Stamps "GoAway" as the close reason so it survives the eventual
//     handleClose stamp.
//  3. Cancels every in-flight RPC whose id exceeds lastAdmitted — the server
//     has promised never to ack those.
//  4. Spawns a goroutine that drives the session through Closing →
//     WaitServerClose → Closed via s.Close, so the lifecycle completes even
//     when the server forgets to follow up with a stream EOF.
//
// A late-arriving GOAWAY from an already terminal session is ignored.
func (s *Session) handleGoAway(goAway *spb.GoAwayResponse) {
	if _, ok := s.transitionTo(StateClosing, notState(StateClosing, StateWaitServerClose, StateClosed)); !ok {
		return
	}
	s.setCloseReason("GoAway")

	lastAdmitted := goAway.GetLastRpcIdAdmitted()
	s.debugf("received GOAWAY reason=%q description=%q last_rpc_id_admitted=%d",
		goAway.GetReason(), goAway.GetDescription(), lastAdmitted)

	err := unavailable(ErrUnavailableGoAway,
		"vRPC not admitted before GOAWAY (last_admitted=%d)", lastAdmitted)
	s.cancelActiveRPCs(err, func(id int64) bool { return id > lastAdmitted })

	// Drive the lifecycle to completion off the readLoop. s.Close drains
	// the remaining admitted RPCs (or returns on ctx-timeout via ForceClose),
	// sends CloseSession, transitions to WaitServerClose, and then the
	// pool's stuck-session monitor or the server's EOF moves us to Closed.
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = s.Close(ctx, &spb.CloseSessionRequest{
			Reason:      spb.CloseSessionRequest_CLOSE_SESSION_REASON_GOAWAY,
			Description: "client teardown after server GOAWAY",
		})
	}()
}

// handleClose is invoked when Recv returns an error. It transitions to
// StateClosed (from any non-terminal state — including WaitServerClose
// when the server's EOF arrives after a CloseSession we sent) and cancels
// every remaining in-flight RPC.
//
// The close reason is derived from the Recv error if no more-specific
// reason was recorded earlier — see streamEndReason. setCloseReason is
// CompareAndSwap-once, so a GoAway / MissedHeartbeat / Error stamp from
// upstream always wins; the categorized StreamEnd label only sticks when
// the stream ended without any other path classifying it first.
func (s *Session) handleClose(err error) {
	if _, ok := s.transitionTo(StateClosed, notState(StateClosed)); !ok {
		return
	}
	reason := streamEndReason(err)
	s.setCloseReason(reason)
	s.mu.Lock()
	inFlight := len(s.activeRPCs)
	s.mu.Unlock()
	age := time.Duration(0)
	if openedAt := s.OpenedAt(); !openedAt.IsZero() {
		age = time.Since(openedAt)
	}
	lastRPC := s.nextRPCID.Load()
	fmt.Printf(">>> SESSION %s handleClose reason=%s age=%v in_flight=%d last_rpc_id=%d raw_err=%v <<<\n",
		s.logName, reason, age, inFlight, lastRPC, err)
	s.recordEvent("close", "reason=%s age=%v in_flight=%d last_rpc_id=%d raw_err=%v",
		reason, age, inFlight, lastRPC, err)
	s.cancelActiveRPCs(unavailable(err, "session closed: %v", err), nil)
	s.signalQuiescent()
	s.notifyClosed(err)
}

// streamEndReason classifies the Recv error that ended the stream. The
// returned label is what shows up in sessionz's Close-reasons breakdown
// when no upstream path stamped a more specific reason (GoAway,
// MissedHeartbeat, Error, etc.).
//
// Categories the operator typically cares about:
//
//	StreamEnd:EOF              — server closed the stream cleanly with
//	                              io.EOF (graceful shutdown from server's
//	                              side that didn't go through GoAway)
//	StreamEnd:Canceled         — local ctx cancel (pool teardown,
//	                              client app exit) or grpc CANCELED
//	StreamEnd:DeadlineExceeded — ctx deadline or grpc DEADLINE_EXCEEDED
//	StreamEnd:Unavailable      — transport-level break (TCP drop,
//	                              connection recycler killed the channel,
//	                              load balancer evicted the backend)
//	StreamEnd:Internal         — server INTERNAL error
//	StreamEnd:{Code}           — any other gRPC status code (verbatim)
//	StreamEnd:Other            — no recognizable category (extremely rare)
//	StreamEnd                  — err was nil (shouldn't happen since Recv
//	                              only returns on error)
func streamEndReason(err error) string {
	if err == nil {
		return "StreamEnd"
	}
	if errors.Is(err, io.EOF) {
		return "StreamEnd:EOF"
	}
	if errors.Is(err, context.Canceled) {
		return "StreamEnd:Canceled"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "StreamEnd:DeadlineExceeded"
	}
	if st, ok := status.FromError(err); ok {
		return "StreamEnd:" + st.Code().String()
	}
	return "StreamEnd:Other"
}

// resetHeartbeatDeadline pushes out the watchdog to (3 * heartbeatInterval)
// from now. The 3x multiplier follows the protocol guidance of tolerating two
// missed heartbeats.
func (s *Session) resetHeartbeatDeadline() {
	s.mu.Lock()
	s.nextHeartbeatDeadline = time.Now().Add(3 * s.heartbeatInterval)
	s.mu.Unlock()
}

// heartBeatLoop watches the session's heartbeat deadline using a single Timer
// that re-arms itself when a frame extends the deadline. The watchdog is
// only enforced while at least one VRPC is in flight: the server emits
// Heartbeats during long-running VRPCs, so an idle session legitimately
// receives no heartbeats and must not be torn down.
func (s *Session) heartBeatLoop(ctx context.Context) {
	s.mu.Lock()
	deadline := s.nextHeartbeatDeadline
	s.mu.Unlock()

	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			s.mu.Lock()
			if s.state == StateClosed {
				s.mu.Unlock()
				return
			}
			active := len(s.activeRPCs)
			remaining := time.Until(s.nextHeartbeatDeadline)
			interval := s.heartbeatInterval
			// last-frame age = (deadline - now) inverted into "how long
			// since the last frame extended us" = 3*interval - remaining.
			lastFrameAge := 3*interval - remaining
			s.mu.Unlock()

			if active == 0 {
				// Idle session: no heartbeats are expected. Re-check after
				// one interval so a freshly-started VRPC is picked up.
				timer.Reset(interval)
				continue
			}
			if remaining > 0 {
				// Deadline was pushed out while we were sleeping; re-arm.
				// Log so we can see heartbeats (or other frames) keeping
				// the session alive while specific in-flight vRPCs stall —
				// that's the "case 1" signal vs. half-dead Recv (case 2).
				fmt.Printf(">>> SESSION %s heartbeat tick alive in_flight=%d last_frame_age=%v remaining=%v interval=%v <<<\n",
					s.logName, active, lastFrameAge, remaining, interval)
				// Only record when last_frame_age has crossed one interval —
				// otherwise every healthy session would spam the UI ring
				// buffer ~3x/second and drown out close/missed events.
				if lastFrameAge >= interval {
					s.recordEvent("hb-alive", "in_flight=%d last_frame_age=%v remaining=%v interval=%v",
						active, lastFrameAge, remaining, interval)
				}
				timer.Reset(remaining)
				continue
			}
			// active > 0 and deadline elapsed — half-dead stream (no frames
			// arriving while we have in-flight work). Log before ForceClose
			// so we have a definitive marker even if downstream cancel races.
			fmt.Printf(">>> SESSION %s heartbeat MISSED — forcing close in_flight=%d last_frame_age=%v interval=%v <<<\n",
				s.logName, active, lastFrameAge, interval)
			s.recordEvent("hb-missed", "in_flight=%d last_frame_age=%v interval=%v",
				active, lastFrameAge, interval)
			s.ForceClose(&spb.CloseSessionRequest{
				Reason:      spb.CloseSessionRequest_CLOSE_SESSION_REASON_MISSED_HEARTBEAT,
				Description: "client terminated session due to missed server heartbeats",
			})
			return
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

// closeReasonLabel maps a CloseSessionRequest reason to a short human-
// readable category string for the debug UI. Empty when req is nil.
func closeReasonLabel(req *spb.CloseSessionRequest) string {
	if req == nil {
		return ""
	}
	switch req.Reason {
	case spb.CloseSessionRequest_CLOSE_SESSION_REASON_MISSED_HEARTBEAT:
		return "MissedHeartbeat"
	case spb.CloseSessionRequest_CLOSE_SESSION_REASON_GOAWAY:
		return "GoAway"
	case spb.CloseSessionRequest_CLOSE_SESSION_REASON_ERROR:
		return "Error"
	case spb.CloseSessionRequest_CLOSE_SESSION_REASON_USER:
		return "User"
	case spb.CloseSessionRequest_CLOSE_SESSION_REASON_DOWNSIZE:
		return "Downsize"
	default:
		return "Other"
	}
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
