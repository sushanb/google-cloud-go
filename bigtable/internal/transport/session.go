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
	"errors"
	"fmt"
	"log"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	spb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
	btopt "cloud.google.com/go/bigtable/internal/option"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// multiPlexingLimit caps the number of vRPCs in flight on a single session
// stream. The current protocol does not support multiplexing, so the value
// must stay at 1; raising it requires a negotiated server-side change.
const multiPlexingLimit = 1

// Default heartbeat tunables. The interval is replaced once the server sends a
// SessionParametersResponse; the initial deadline is generous enough to span
// the first handshake.
const (
	defaultHeartbeatInterval = 10 * time.Second
	initialHeartbeatGrace    = 30 * time.Minute
)

// Sentinel session-level error codes. They are wrapped in a gRPC Unavailable
// status (so existing retry plumbing sees codes.Unavailable) while errors.Is
// lets callers distinguish the underlying cause.
var (
	// ErrSessionNotActive is returned by Invoke pre-Send when the session
	// is not yet active or has begun shutting down. Errors carrying this
	// cause are delivered tagged StateUncommitted (never left the client)
	// so RetryingVRpc retries them regardless of Idempotent.
	ErrSessionNotActive = errors.New("bigtable: session not active")
	// ErrUnavailableHeartBeatMissed indicates the session was torn down because
	// the server stopped sending heartbeats within the negotiated window.
	// Delivered via cancelActiveRPCs and tagged StateTransportFailure.
	ErrUnavailableHeartBeatMissed = errors.New("bigtable: session unavailable: server heartbeat missed")
	// ErrUnavailableGoAway indicates the server sent GOAWAY. The session
	// transitions to Closing (pool stops handing it out) but any in-flight
	// vRPC keeps running — if the server sends the response before dropping
	// the stream, the RPC completes cleanly (Java parity). Only if the
	// stream actually terminates without a response does the RPC get failed
	// via handleClose → cancelActiveRPCs (tagged StateTransportFailure).
	ErrUnavailableGoAway = errors.New("bigtable: session unavailable: server sent GOAWAY")
	// ErrUnavailableSessionError indicates the server reported a fatal
	// session-level error (an ErrorResponse with rpc_id == 0). Delivered
	// via cancelActiveRPCs and tagged StateTransportFailure.
	ErrUnavailableSessionError = errors.New("bigtable: session unavailable: server reported session error")
)

// State represents the lifecycle state of a Session. Sessions move strictly
// forward through the values; once StateClosed is reached the session is
// terminal.
type State int

const (
	// StateNew indicates the session is newly created and not yet active.
	StateNew State = iota
	// StateStarting indicates the session is dialing and handshaking.
	StateStarting
	// StateReady indicates the session is active and ready for RPCs.
	StateReady
	// StateClosing indicates the client has decided to close the session
	// and is draining outstanding vRPCs before sending CloseSession.
	StateClosing
	// StateWaitServerClose indicates the client has sent CloseSession and
	// is waiting for the server's EOF/trailers to confirm teardown. The
	// pool's stuck-session monitor force-closes sessions parked here too
	// long (server lost / silent).
	StateWaitServerClose
	// StateClosed indicates the session is closed.
	StateClosed
)

// String returns a human-readable name for the state.
func (s State) String() string {
	switch s {
	case StateNew:
		return "New"
	case StateStarting:
		return "Starting"
	case StateReady:
		return "Ready"
	case StateClosing:
		return "Closing"
	case StateWaitServerClose:
		return "WaitServerClose"
	case StateClosed:
		return "Closed"
	default:
		return "Unknown"
	}
}

// Stream is the bidirectional gRPC stream a Session multiplexes over.
type Stream interface {
	Send(*spb.SessionRequest) error
	Recv() (*spb.SessionResponse, error)
	Header() (metadata.MD, error)
	// Context is the stream's context. After Header() returns, peer info
	// (remote TCP addr) is available via peer.FromContext — sessionz uses
	// it to link a slow vRPC row to the specific conn in tcpz.
	Context() context.Context
}

// SessionHooks contains optional callbacks invoked at points in a session's
// lifecycle. Any field may be nil; the session calls only the non-nil hooks.
// Hooks must not block; dispatch long work to a goroutine. This follows the
// net/http/httptrace.ClientTrace pattern.
type SessionHooks struct {
	OnStart  func(ctx context.Context)
	OnActive func(s *Session)
	OnClose  func(s *Session, err error)
}

func (h SessionHooks) onStart(ctx context.Context) {
	if h.OnStart != nil {
		h.OnStart(ctx)
	}
}

func (h SessionHooks) onActive(s *Session) {
	if h.OnActive != nil {
		h.OnActive(s)
	}
}

func (h SessionHooks) onClose(s *Session, err error) {
	if h.OnClose != nil {
		h.OnClose(s, err)
	}
}

// vrpcResult is the single value delivered to Invoke through resultChan.
type vrpcResult struct {
	resp        *spb.VirtualRpcResponse
	clusterInfo *spb.ClusterInformation
	err         error
}

// vrpcImpl tracks the state of an in-flight virtual RPC.
//
// sentAt / deadline / attempt are populated by Session.Invoke immediately
// before the wire Send so the flightz debug page can render a live
// snapshot of every in-flight vRPC without allocating or locking on the
// hot path. All three values are already computed by Invoke to fill the
// VirtualRpcRequest envelope; storing them here just re-uses those
// stamps.
type vrpcImpl struct {
	id         int64
	method     string
	resultChan chan vrpcResult
	sentAt     time.Time // wall-clock stamp captured right before s.Send(sessionReq)
	deadline   time.Time // ctx.Deadline() at Invoke entry; zero if the ctx has no deadline
	attempt    int32     // VRpcAttempt(ctx) at Invoke entry (1 = first try, 2+ = retry)
}

// Session manages the lifecycle of a Bigtable Session and routes vRPCs over
// its bidirectional Stream.
//
// All fields formerly guarded by an internal mu are now atomics — the hot
// path (Invoke, State(), the picker) no longer takes a per-session mutex,
// removing four lock/unlock pairs and the cross-goroutine cache-line
// ping-pong the picker used to trigger by calling State() per session
// under lock. sendMu still guards concurrent Send() writers since
// grpc.ClientStream.Send is not safe for concurrent use.
type Session struct {
	// nextRPCID is mutated exclusively via atomic ops; using atomic.Int64
	// guarantees 8-byte alignment on 32-bit platforms without struct layout
	// constraints.
	nextRPCID atomic.Int64

	sendMu sync.Mutex

	logger      *log.Logger
	logName     string
	stream      Stream
	hooks       SessionHooks
	sessionType SessionType
	tracer      *sessionTracer

	// state is the session's lifecycle position (State constants). Read
	// with State(); mutate through transitionTo. int32 rather than
	// State-typed atomic because atomic.Int32 is stdlib and we control
	// the marshal.
	state atomic.Int32
	// lastStateChangeNano stamps each successful transitionTo with
	// time.Now().UnixNano(). Approximate ordering across transitions is
	// enough for the debug UI.
	lastStateChangeNano atomic.Int64
	// closeOnce serializes hooks.OnClose and tracer.recordClose so they
	// fire exactly once even if multiple paths race to close the session.
	closeOnce sync.Once

	// activeRPC holds the single in-flight vRPC (multiPlexingLimit=1).
	// Replaces a map[int64]*vrpcImpl guarded by mu. Invoke sets it via
	// CompareAndSwap(nil, rpc) — the CAS is the pool-serialization
	// invariant check made explicit: if it fails, someone bypassed the
	// pool's per-session checkout gate. Load is used from the response
	// dispatch paths and the ctx-done/close paths; a load returning nil
	// means "no rpc in flight" and a load returning a vrpc means "match
	// on id before delivering".
	activeRPC atomic.Pointer[vrpcImpl]

	// heartbeatIntervalNano is the server-negotiated keep-alive
	// interval (in ns). Stored atomically because handleSessionParameters
	// mutates it from the readLoop while the vRPC hot path reads it via
	// resetHeartbeatDeadline.
	heartbeatIntervalNano atomic.Int64
	// nextHeartbeatDeadlineNano is the wall-clock deadline (UnixNano) the
	// heartbeat watchdog compares against. Every outbound frame + every
	// inbound frame extends it via resetHeartbeatDeadline.
	nextHeartbeatDeadlineNano atomic.Int64

	// quiescent is closed when the in-flight vRPC (if any) has drained
	// after the session entered StateClosing, or when ForceClose runs.
	// Close() waits on it to drain in-flight RPCs without polling.
	quiescent     chan struct{}
	quiescentOnce sync.Once

	// peerInfo is populated by peerInfoExtracter from the stream header,
	// synchronously in handleOpenSession before hooks.onActive fires.
	// atomic.Pointer so peerInfoSummary / snapshot / the ctx-done event
	// recorder can read it lock-free on the hot path.
	peerInfo atomic.Pointer[spb.PeerInfo]
	// remoteAddr is the TCP remote (AFE) socket address in "ip:port" form,
	// captured from grpc peer.FromContext once the stream Header returns.
	// atomic.Pointer so the vRPC hot path can read it lock-free to stamp
	// slow-vRPC rows for the sessionz→tcpz cross-link.
	remoteAddr atomic.Pointer[string]
	// refreshConfig is stored once when the server sends
	// SessionRefreshConfig. atomic.Pointer to keep the getter allocation-
	// and lock-free.
	refreshConfig atomic.Pointer[spb.SessionRefreshConfig]

	// okRpcs / errorRpcs are bumped lock-free from the vRPC dispatch paths so
	// the debug UI can read a numeric total without locking. The booleans
	// HasOkRpcs / HasErrorRpcs remain as thin wrappers over these counters
	// for back-compat with existing callers.
	okRpcs    atomic.Int64
	errorRpcs atomic.Int64
	// msgsSent / msgsRecv count every Send / Recv frame on the bidi stream.
	// They cover every session-level frame type — OpenSession, vRPC,
	// CloseSession, Heartbeat — not just successful vRPCs.
	msgsSent atomic.Int64
	msgsRecv atomic.Int64
	// Per-frame-type breakdown of the above. Indexed by reqMsgType /
	// respMsgType (see session_msgtype.go). Lock-free reads and writes via
	// the array of atomics; total length is small (≤ 8) so the field stays
	// cache-resident.
	msgsSentByType [numReqMsgTypes]atomic.Int64
	msgsRecvByType [numRespMsgTypes]atomic.Int64

	// retries counts vRPCs that arrived with VRpcAttempt>1 — i.e. the retry
	// interceptor re-issued them on this session. Paired with errorRpcs it
	// distinguishes "errored once and recovered" from "errored terminally."
	retries atomic.Int64

	// closeReason is set exactly once via setCloseReason() at session
	// teardown so SessionPoolImpl.OnClose can attribute the close to a
	// category (Heartbeat / GoAway / Error / User / Downsize). Empty string
	// when the close cause isn't classified.
	closeReason atomic.Pointer[string]

	// poolCloseRecorded is the once-flag the owning SessionPoolImpl
	// consults so its sessionsClosed / CloseReasons counters bump exactly
	// once per session — regardless of which removal path arrives first
	// (proactive prune, CheckoutSession dead-detect, or the eventual
	// hooks.OnClose driven by the server EOF).
	poolCloseRecorded atomic.Bool

	// latencyMu guards latencySamples — a tiny ring buffer of the last
	// latencyWindow server-reported BackendLatency values, used to compute
	// p50/p95/p99 in the debug UI. Reservoir is sized so a snapshot copy
	// is cheap (a few hundred floats).
	latencyMu      sync.Mutex
	latencySamples []time.Duration
	latencyNext    int // next write index when ring is full

	// clusterCounts tallies per-ClusterInformation.ClusterId responses.
	// Lock-free reads via sync.Map; values are *atomic.Int64. The set of
	// cluster ids is small (≤ pool cardinality) so iteration is cheap.
	clusterCounts sync.Map

	// channelIndex is set once at construction to the BigtableChannelPool
	// connEntry index the session's bidi stream was placed on. -1 when
	// the underlying pool isn't a BigtableChannelPool (e.g. test setups
	// using option.WithGRPCConn) or when the pick wasn't observed.
	channelIndex atomic.Int32

	// eventsMu guards the per-session debug-event ring buffer surfaced in
	// sessionz. Sized small (maxSessionEvents) so the snapshot copy stays
	// cheap and the UI render isn't drowned by a runaway session.
	eventsMu sync.Mutex
	events   []SessionEvent
}

// SessionEvent is one entry in a session's per-session debug ring buffer.
// Surfaced through SessionSnapshot.RecentEvents and merged across all
// sessions into PoolSnapshot.RecentEvents for the sessionz UI.
//
// Kinds in use:
//
//	"close"     — stream tear-down handled by handleClose; Message carries
//	              reason, age, in-flight count, last rpc id, raw err.
//	"hb-missed" — heartbeat watchdog fired ForceClose; Message carries
//	              in-flight count and last-frame age.
//	"hb-alive"  — heartbeat tick observed in-flight RPC(s) while a recent
//	              frame had already pushed the deadline; useful for spotting
//	              "server kept stream alive but lost specific vRPC response"
//	              stalls. Suppressed (not recorded) unless lastFrameAge is
//	              at least one heartbeat interval to avoid log noise.
//	"ctx-done"  — Session.Invoke's per-attempt wait was killed by the
//	              caller's context (deadline or cancel); Message carries
//	              method, rpc id, time waited, ctx err, session state.
type SessionEvent struct {
	At      time.Time
	Kind    string
	Message string
}

const maxSessionEvents = 64

// recordEvent appends a SessionEvent to the per-session ring buffer. Cheap
// (single mutex + bounded slice); safe to call from any goroutine including
// readLoop and heartBeatLoop.
func (s *Session) recordEvent(kind, format string, args ...interface{}) {
	ev := SessionEvent{
		At:      time.Now(),
		Kind:    kind,
		Message: fmt.Sprintf(format, args...),
	}
	s.eventsMu.Lock()
	if len(s.events) >= maxSessionEvents {
		copy(s.events, s.events[1:])
		s.events = s.events[:len(s.events)-1]
	}
	s.events = append(s.events, ev)
	s.eventsMu.Unlock()
}

// snapshotEvents returns a copy of the session's debug-event ring buffer
// (oldest first). Allocation-friendly: single mutex acquire over a
// bounded-size slice.
func (s *Session) snapshotEvents() []SessionEvent {
	s.eventsMu.Lock()
	out := make([]SessionEvent, len(s.events))
	copy(out, s.events)
	s.eventsMu.Unlock()
	return out
}

// peerInfoSummary renders the session's PeerInfo as a compact single line
// suitable for inclusion in log messages and debug events. Returns
// "peer=unknown" when the bidi stream header hasn't been parsed yet (the
// session has not received the asynchronous PeerInfo metadata). Format
// keeps the ids in hex so they line up with what sessionz renders.
func (s *Session) peerInfoSummary() string {
	p := s.peerInfo.Load()
	if p == nil {
		return "peer=unknown"
	}
	return fmt.Sprintf("peer={afe=%x/%s/%s gfe=%x transport=%s}",
		p.GetApplicationFrontendId(),
		p.GetApplicationFrontendRegion(),
		p.GetApplicationFrontendSubzone(),
		p.GetGoogleFrontendId(),
		p.GetTransportType())
}

const latencyWindow = 256

// recordLatency appends a server-reported BackendLatency sample to the ring
// buffer. Called from Invoke whenever the response includes Stats. Cheap —
// single mutex acquire over a fixed-size slice.
func (s *Session) recordLatency(d time.Duration) {
	if d <= 0 {
		return
	}
	s.latencyMu.Lock()
	if len(s.latencySamples) < latencyWindow {
		s.latencySamples = append(s.latencySamples, d)
	} else {
		s.latencySamples[s.latencyNext] = d
		s.latencyNext = (s.latencyNext + 1) % latencyWindow
	}
	s.latencyMu.Unlock()
}

// snapshotLatencies returns a sorted copy of the current samples so callers
// can compute percentiles or iterate without holding the lock.
func (s *Session) snapshotLatencies() []time.Duration {
	s.latencyMu.Lock()
	out := make([]time.Duration, len(s.latencySamples))
	copy(out, s.latencySamples)
	s.latencyMu.Unlock()
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// percentile returns the p-th percentile (0-100) of the sorted slice. Uses
// nearest-rank — small N, linear interpolation isn't worth the complexity.
func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	if p <= 0 {
		return sorted[0]
	}
	if p >= 100 {
		return sorted[len(sorted)-1]
	}
	idx := int(float64(len(sorted)-1) * p / 100)
	return sorted[idx]
}

// recordCluster increments the per-cluster response counter.
func (s *Session) recordCluster(id string) {
	if id == "" {
		return
	}
	c, _ := s.clusterCounts.LoadOrStore(id, new(atomic.Int64))
	c.(*atomic.Int64).Add(1)
}

// snapshotClusters returns the per-cluster response counts as a flat map.
func (s *Session) snapshotClusters() map[string]int64 {
	out := map[string]int64{}
	s.clusterCounts.Range(func(k, v interface{}) bool {
		out[k.(string)] = v.(*atomic.Int64).Load()
		return true
	})
	return out
}

// SetChannelIndex records the BigtableChannelPool connEntry index the
// session's stream landed on. Called once at session construction.
func (s *Session) SetChannelIndex(idx int) {
	s.channelIndex.Store(int32(idx))
}

// ChannelIndex returns the per-pool channel index, or -1 if unset.
func (s *Session) ChannelIndex() int {
	return int(s.channelIndex.Load())
}

// setCloseReason records the reason for the session's terminal close. Only
// the first call sticks; subsequent calls (which happen if multiple teardown
// paths race) are ignored so the OnClose callback sees a stable value.
func (s *Session) setCloseReason(reason string) {
	if reason == "" {
		return
	}
	s.closeReason.CompareAndSwap(nil, &reason)
}

// CloseReason returns the recorded close reason, or "" if none was set.
func (s *Session) CloseReason() string {
	if p := s.closeReason.Load(); p != nil {
		return *p
	}
	return ""
}

// SampleUptime records the session's current alive time into the
// session.uptime histogram. Intended for periodic invocation by the pool
// heartbeat over still-active sessions; no-op if the session hasn't
// finished opening.
func (s *Session) SampleUptime(ctx context.Context) {
	s.tracer.sampleUptime(ctx)
}

// RecordTransportOverhead emits a per-vRPC transport-overhead sample —
// (stream − backend), i.e. wire + AFE + client-decode time excluding
// server processing — to the transport_latencies histogram. Caller has
// already computed the delta and validated it's positive; this is a
// no-op only if the metric isn't registered.
func (s *Session) RecordTransportOverhead(ctx context.Context, method string, overhead time.Duration) {
	s.tracer.recordTransportOverhead(ctx, method, overhead)
}

// Retries returns the number of vRPCs Invoke processed with AttemptNumber > 1.
func (s *Session) Retries() int64 { return s.retries.Load() }

// OpenedAt returns when the session reached StateReady (zero until then).
// Used by callers that want to compute a session's age for diagnostics.
func (s *Session) OpenedAt() time.Time {
	return s.tracer.openedAtSnapshot()
}

// SessionOption configures a Session at construction time.
type SessionOption func(*Session)

// WithSessionLogger attaches a logger for diagnostic output. Without it, the
// session logs nothing.
func WithSessionLogger(logger *log.Logger) SessionOption {
	return func(s *Session) { s.logger = logger }
}

// WithSessionPoolName stamps the pool-scoped name used for the session_name
// label on session lifecycle metrics. Matches java-bigtable's per-pool
// SessionPoolInfo name — cardinality stays bounded by the number of pools
// per process, not per session. Omit for tests / standalone sessions; the
// label will be empty.
func WithSessionPoolName(name string) SessionOption {
	return func(s *Session) { s.tracer.setPoolName(name) }
}

// NewSession constructs a Session bound to the given stream. Pass a zero-value
// SessionHooks if you don't need lifecycle callbacks.
func NewSession(logName string, stream Stream, hooks SessionHooks, sessionType SessionType, opts ...SessionOption) *Session {
	s := &Session{
		logName:     logName,
		stream:      stream,
		hooks:       hooks,
		quiescent:   make(chan struct{}),
		tracer:      newSessionTracer(sessionType),
		sessionType: sessionType,
	}
	s.state.Store(int32(StateNew))
	s.lastStateChangeNano.Store(time.Now().UnixNano())
	s.heartbeatIntervalNano.Store(int64(defaultHeartbeatInterval))
	s.nextHeartbeatDeadlineNano.Store(time.Now().Add(initialHeartbeatGrace).UnixNano())
	s.channelIndex.Store(-1)
	for _, o := range opts {
		o(s)
	}
	return s
}

// LogName returns the session's diagnostic identifier. Set once at
// construction; safe to read lock-free.
func (s *Session) LogName() string {
	return s.logName
}

// State returns the current state.
func (s *Session) State() State {
	return State(s.state.Load())
}

// RemoteAddr returns the TCP remote (AFE) socket address in "ip:port" form,
// or "" if the stream Header hasn't been observed yet or gRPC didn't
// populate peer info. Used by sessionz to link a slow-vRPC row to the
// specific conn in tcpz.
func (s *Session) RemoteAddr() string {
	if p := s.remoteAddr.Load(); p != nil {
		return *p
	}
	return ""
}

// PeerInfo returns the peer info, or nil if it has not been parsed yet.
func (s *Session) PeerInfo() *spb.PeerInfo {
	return s.peerInfo.Load()
}

// afeID identifies the AFE (Application Front End) a session is pinned to,
// derived from PeerInfo.ApplicationFrontendId. The zero value is the
// sentinel for "unknown" — used before PeerInfo is populated or when the
// server did not send the bigtable-peer-info header. Java-parity: mirrors
// the AutoValue AfeId in SessionList.java, which wraps the same signed
// 64-bit long. Underlying type matches the proto (int64) to avoid
// sign-conversion surprises.
type afeID int64

// AfeID returns the AFE identifier for this session, or 0 if PeerInfo is
// nil (header absent or session pre-Active). Stable for the session's
// lifetime — PeerInfo is populated once, synchronously with the transition
// to StateReady (see handleOpenSession).
func (s *Session) AfeID() afeID {
	if p := s.peerInfo.Load(); p != nil {
		return afeID(p.GetApplicationFrontendId())
	}
	return 0
}

// RefreshConfig returns the server-provided refresh configuration, or nil if
// the server has not sent one.
func (s *Session) RefreshConfig() *spb.SessionRefreshConfig {
	return s.refreshConfig.Load()
}

// HasOkRpcs reports whether the session served at least one successful vRPC.
func (s *Session) HasOkRpcs() bool {
	return s.okRpcs.Load() > 0
}

// HasErrorRpcs reports whether the session served at least one failed vRPC.
func (s *Session) HasErrorRpcs() bool {
	return s.errorRpcs.Load() > 0
}

// OkRpcs returns the total number of successful vRPC responses delivered on
// this session.
func (s *Session) OkRpcs() int64 {
	return s.okRpcs.Load()
}

// ErrorRpcs returns the total number of failed vRPC responses delivered on
// this session.
func (s *Session) ErrorRpcs() int64 {
	return s.errorRpcs.Load()
}

// MsgsSent returns the total number of frames sent on this session's bidi
// stream (every Send, regardless of payload type).
func (s *Session) MsgsSent() int64 {
	return s.msgsSent.Load()
}

// MsgsRecv returns the total number of frames received on this session's
// bidi stream (every successful Recv).
func (s *Session) MsgsRecv() int64 {
	return s.msgsRecv.Load()
}

// debugf logs at debug level if a logger has been attached.
func (s *Session) debugf(format string, args ...interface{}) {
	if s.logger == nil {
		return
	}
	btopt.Debugf(s.logger, "bigtable_session %s: "+format, append([]interface{}{s.logName}, args...)...)
}

// transitionTo sets the session state to `to` iff ok(currentState) returns
// true. Returns the previous state and whether the transition was applied.
// Retries on CAS failure so a losing racer with a still-valid current state
// still transitions; the predicate is re-evaluated after each spurious loss.
func (s *Session) transitionTo(to State, ok func(State) bool) (prev State, applied bool) {
	for {
		prev = State(s.state.Load())
		if !ok(prev) {
			return prev, false
		}
		if s.state.CompareAndSwap(int32(prev), int32(to)) {
			s.lastStateChangeNano.Store(time.Now().UnixNano())
			return prev, true
		}
	}
}

// isState returns a predicate matching any of `allowed`.
func isState(allowed ...State) func(State) bool {
	return func(s State) bool {
		for _, a := range allowed {
			if s == a {
				return true
			}
		}
		return false
	}
}

// notState returns a predicate matching any state NOT in `forbidden`.
func notState(forbidden ...State) func(State) bool {
	return func(s State) bool {
		for _, f := range forbidden {
			if s == f {
				return false
			}
		}
		return true
	}
}

// signalQuiescent closes the quiescent channel exactly once.
func (s *Session) signalQuiescent() {
	s.quiescentOnce.Do(func() { close(s.quiescent) })
}

// sessionErr couples a gRPC Unavailable status with a sentinel cause so that
// both status.Code(err) and errors.Is(err, sentinel) work.
type sessionErr struct {
	st    *status.Status
	cause error
}

func (e *sessionErr) Error() string              { return e.st.Err().Error() }
func (e *sessionErr) Unwrap() error              { return e.cause }
func (e *sessionErr) GRPCStatus() *status.Status { return e.st }

// unavailable builds a sessionErr from a sentinel cause and human-readable
// detail. The returned error carries codes.Unavailable.
func unavailable(cause error, format string, args ...interface{}) error {
	return &sessionErr{
		st:    status.Newf(codes.Unavailable, format, args...),
		cause: cause,
	}
}
