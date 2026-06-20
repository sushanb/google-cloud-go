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
	"log"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	spb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
	btopt "cloud.google.com/go/bigtable/internal/option"
	"golang.org/x/sync/semaphore"
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
	// ErrSessionNotActive is returned by Invoke when the session is not yet
	// active or has begun shutting down.
	ErrSessionNotActive = errors.New("bigtable: session not active")
	// ErrUnavailableHeartBeatMissed indicates the session was torn down because
	// the server stopped sending heartbeats within the negotiated window.
	ErrUnavailableHeartBeatMissed = errors.New("bigtable: session unavailable: server heartbeat missed")
	// ErrUnavailableGoAway indicates the server sent a GOAWAY and cancelled
	// vRPCs that had not yet been admitted.
	ErrUnavailableGoAway = errors.New("bigtable: session unavailable: server sent GOAWAY")
	// ErrUnavailableSessionError indicates the server reported a fatal
	// session-level error (an ErrorResponse with rpc_id == 0).
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
	// StateActive indicates the session is active and ready for RPCs.
	StateActive
	// StateClosing indicates the session is draining and shutting down. It
	// covers both the pre-CloseSession drain and the post-CloseSession wait
	// for the server's EOF.
	StateClosing
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
	case StateActive:
		return "Active"
	case StateClosing:
		return "Closing"
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
type vrpcImpl struct {
	id         int64
	method     string
	resultChan chan vrpcResult
}

// Session manages the lifecycle of a Bigtable Session and routes vRPCs over
// its bidirectional Stream.
type Session struct {
	// nextRPCID is mutated exclusively via atomic ops; using atomic.Int64
	// guarantees 8-byte alignment on 32-bit platforms without struct layout
	// constraints.
	nextRPCID atomic.Int64

	mu     sync.Mutex
	sendMu sync.Mutex

	logger      *log.Logger
	logName     string
	stream      Stream
	hooks       SessionHooks
	sessionType SessionType
	tracer      *sessionTracer
	vrpcSem     *semaphore.Weighted

	state           State
	lastStateChange time.Time
	// closeOnce serializes hooks.OnClose and tracer.recordClose so they
	// fire exactly once even if multiple paths race to close the session.
	closeOnce sync.Once

	activeRPCs map[int64]*vrpcImpl

	heartbeatInterval     time.Duration
	nextHeartbeatDeadline time.Time

	// quiescent is closed when no vRPCs remain in activeRPCs after the
	// session enters StateClosing, or when ForceClose runs. Close() waits on
	// it to drain in-flight RPCs without polling.
	quiescent     chan struct{}
	quiescentOnce sync.Once

	peerInfo      *spb.PeerInfo
	refreshConfig *spb.SessionRefreshConfig

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

// Retries returns the number of vRPCs Invoke processed with AttemptNumber > 1.
func (s *Session) Retries() int64 { return s.retries.Load() }

// SessionOption configures a Session at construction time.
type SessionOption func(*Session)

// WithSessionLogger attaches a logger for diagnostic output. Without it, the
// session logs nothing.
func WithSessionLogger(logger *log.Logger) SessionOption {
	return func(s *Session) { s.logger = logger }
}

// NewSession constructs a Session bound to the given stream. Pass a zero-value
// SessionHooks if you don't need lifecycle callbacks.
func NewSession(logName string, stream Stream, hooks SessionHooks, sessionType SessionType, opts ...SessionOption) *Session {
	s := &Session{
		state:                 StateNew,
		lastStateChange:       time.Now(),
		logName:               logName,
		stream:                stream,
		hooks:                 hooks,
		activeRPCs:            make(map[int64]*vrpcImpl),
		heartbeatInterval:     defaultHeartbeatInterval,
		nextHeartbeatDeadline: time.Now().Add(initialHeartbeatGrace),
		quiescent:             make(chan struct{}),
		tracer:                newSessionTracer(sessionType),
		sessionType:           sessionType,
		vrpcSem:               semaphore.NewWeighted(multiPlexingLimit),
	}
	s.channelIndex.Store(-1)
	for _, o := range opts {
		o(s)
	}
	return s
}

// LogName returns the session's diagnostic identifier.
func (s *Session) LogName() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.logName
}

// State returns the current state.
func (s *Session) State() State {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state
}

// PeerInfo returns the peer info, or nil if it has not been parsed yet.
func (s *Session) PeerInfo() *spb.PeerInfo {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.peerInfo
}

// RefreshConfig returns the server-provided refresh configuration, or nil if
// the server has not sent one.
func (s *Session) RefreshConfig() *spb.SessionRefreshConfig {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.refreshConfig
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
// All callers using this helper share the same lock/timestamp/transition
// pattern, so adding/removing transitions does not duplicate that bookkeeping.
func (s *Session) transitionTo(to State, ok func(State) bool) (prev State, applied bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	prev = s.state
	if !ok(prev) {
		return prev, false
	}
	s.state = to
	s.lastStateChange = time.Now()
	return prev, true
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
