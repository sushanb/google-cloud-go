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
	"reflect"
	"sort"
	"sync/atomic"
	"time"

	spb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// openRequestMarshaler formats decoded OpenSessionRequest payloads for the
// debug UI: multiline JSON, snake_case names, omit empties.
var openRequestMarshaler = protojson.MarshalOptions{
	Multiline:       true,
	Indent:          "  ",
	UseProtoNames:   true,
	EmitUnpopulated: false,
}

// buildOpenRequestSnapshot decodes the OpenSessionRequest's Payload into
// the message type indicated by sessionType and renders it (plus the
// feature-flags wrapper) as protojson for the debug UI. Returns nil when
// the pool has no template request (rare — only happens in tests that
// inject a session directly).
func buildOpenRequestSnapshot(req *spb.OpenSessionRequest, sessionType SessionType) *OpenRequestSnapshot {
	if req == nil {
		return nil
	}
	out := &OpenRequestSnapshot{ProtocolVersion: req.ProtocolVersion}

	var inner proto.Message
	switch sessionType {
	case SessionTypeTable:
		out.PayloadType = "OpenTableRequest"
		inner = &spb.OpenTableRequest{}
	case SessionTypeAuthorizedView:
		out.PayloadType = "OpenAuthorizedViewRequest"
		inner = &spb.OpenAuthorizedViewRequest{}
	case SessionTypeMaterializedView:
		out.PayloadType = "OpenMaterializedViewRequest"
		inner = &spb.OpenMaterializedViewRequest{}
	default:
		out.PayloadType = "unknown"
	}

	if inner != nil && len(req.Payload) > 0 {
		if err := proto.Unmarshal(req.Payload, inner); err == nil {
			if b, mErr := openRequestMarshaler.Marshal(inner); mErr == nil {
				out.PayloadJSON = string(b)
			}
		}
	}
	if req.Flags != nil {
		if b, err := openRequestMarshaler.Marshal(req.Flags); err == nil {
			out.FlagsJSON = string(b)
		}
	}
	return out
}

// SessionSnapshot is an immutable, allocation-friendly snapshot of one
// Session's debugging state. All fields are value types so the snapshot can
// be marshaled (JSON / template) without re-acquiring any session lock.
type SessionSnapshot struct {
	LogName           string
	State             string
	SessionType       string
	LastStateChange   time.Time
	OkRpcs            int64
	ErrorRpcs         int64
	Retries           int64
	MsgsSent          int64
	MsgsRecv          int64
	// MsgsSentByType / MsgsRecvByType break the totals above down by the
	// SessionRequest / SessionResponse oneof payload type. Keys come from the
	// reqMsgType / respMsgType String() methods (e.g. "VirtualRpc", "Heartbeat").
	// Buckets with a zero count are omitted to keep the rendered cell short.
	MsgsSentByType    map[string]int64
	MsgsRecvByType    map[string]int64
	ActiveRpcs        int
	CloseReason       string
	// LatencyP50/95/99 are computed from the server-reported BackendLatency
	// values cached on the session (last 256 samples). Zero when the session
	// hasn't seen any responses with Stats populated yet.
	LatencyP50  time.Duration
	LatencyP95  time.Duration
	LatencyP99  time.Duration
	LatencyN    int // number of samples in the window
	// ClusterCounts is the per-ClusterInformation.ClusterId response tally
	// (e.g. {"cluster-c1": 412, "cluster-c2": 198}). Empty until the server
	// has attached ClusterInformation to at least one response.
	ClusterCounts map[string]int64
	// ChannelIndex is the BigtableChannelPool connEntry index the session's
	// bidi stream landed on, or -1 when unknown (non-Bigtable channel pool).
	ChannelIndex int
	HeartbeatInterval time.Duration
	NextHeartbeat     time.Time
	HasRefreshConfig  bool
	Peer              PeerInfoSnapshot
	Handle            SessionHandleSnapshot
}

// PeerInfoSnapshot is a JSON-friendly mirror of the relevant fields of
// spb.PeerInfo. Empty fields indicate the server has not yet sent peer info
// (which arrives asynchronously via the response header).
type PeerInfoSnapshot struct {
	TransportType              string
	GoogleFrontendID           int64
	ApplicationFrontendID      int64
	ApplicationFrontendRegion  string
	ApplicationFrontendSubzone string
}

// SessionHandleSnapshot captures the per-handle pool bookkeeping.
type SessionHandleSnapshot struct {
	Outstanding  int64
	EwmaLatency  time.Duration
	LastActivity time.Time
	Picks        int64
}

// PoolSnapshot is a snapshot of one SessionPoolImpl, including every session
// currently in the pool. Sessions are listed in their pool order; callers may
// re-sort as they wish.
type PoolSnapshot struct {
	Name           string
	SessionType    string
	MinSessions    int
	MaxSessions    int
	PickerType     string
	ReadyCount     int
	StartingCount  int
	InUseCount     int
	PendingCount   int
	TotalSessions  int
	Sessions       []SessionSnapshot
	CapturedAt     time.Time
	// Lifecycle aggregates surfaced via PoolSnapshot.
	SessionsOpened int64
	SessionsClosed int64
	CloseReasons   map[string]int64
	ListenerFires  int64
	Throttler      ThrottlerSnapshot
	ScalingHistory []ScalingEvent
	// OpenRequest captures the OpenSessionRequest template used to handshake
	// every session in this pool — protocol version, feature flags, and the
	// decoded inner payload (OpenTable / OpenAuthorizedView /
	// OpenMaterializedView). All sessions in a given pool share this exact
	// request, so it's surfaced per-pool rather than per-session.
	OpenRequest *OpenRequestSnapshot
	// Pool-level aggregates derived from the per-session snapshots and
	// non-session pool state.
	ClusterCounts map[string]int64
	// StateCounts is the per-state population summary across every session
	// currently in the pool: e.g. {"Active": 5, "Closing": 1,
	// "WaitServerClose": 2}. Lets the debug UI render "how many are
	// healthy right now" without rescanning each row. Keys come from
	// State.String() so they line up with the per-session State column.
	StateCounts map[string]int
	LatencyP50  time.Duration
	LatencyP95  time.Duration
	LatencyP99  time.Duration
	LatencyN    int
	SlowVRpcs      []SlowVRpcEvent
	TimeSeries     []TimeSeriesSample
	// Session-lifetime distribution (built from the pool's lifetime ring
	// buffer of completed sessions). LifetimeHistogram is the bucket-label
	// → count list in the order defined by LifetimeBuckets; percentile
	// fields are computed over the same window.
	LifetimeHistogram []LifetimeBucketCount
	LifetimeP50       time.Duration
	LifetimeP95       time.Duration
	LifetimeP99       time.Duration
	LifetimeN         int
}

// LifetimeBucketCount is one bar in the session-lifetime histogram.
type LifetimeBucketCount struct {
	Label string
	Count int
}

// OpenRequestSnapshot is the JSON-friendly form of the OpenSessionRequest.
// PayloadType names the inner-message kind ("OpenTable",
// "OpenAuthorizedView", "OpenMaterializedView", or "unknown"); PayloadJSON
// is the inner message rendered via protojson — that's the field that
// answers "what was this session opened for?". FlagsJSON renders the
// FeatureFlags proto so operators can see what the client asked for.
type OpenRequestSnapshot struct {
	ProtocolVersion int64
	PayloadType     string
	PayloadJSON     string
	FlagsJSON       string
}

// Snapshot returns a debug-friendly snapshot of the session. It acquires
// s.mu once for the mutable fields and reads the atomic counters lock-free.
// It is safe to call concurrently with Invoke.
func (s *Session) Snapshot() SessionSnapshot {
	s.mu.Lock()
	state := s.state
	logName := s.logName
	lastChange := s.lastStateChange
	hbInterval := s.heartbeatInterval
	nextHb := s.nextHeartbeatDeadline
	activeRpcs := len(s.activeRPCs)
	peer := s.peerInfo
	hasRefresh := s.refreshConfig != nil
	sessionType := s.sessionType
	s.mu.Unlock()

	sortedLat := s.snapshotLatencies()
	return SessionSnapshot{
		LogName:           logName,
		State:             state.String(),
		SessionType:       sessionType.String(),
		LastStateChange:   lastChange,
		OkRpcs:            s.okRpcs.Load(),
		ErrorRpcs:         s.errorRpcs.Load(),
		Retries:           s.retries.Load(),
		MsgsSent:          s.msgsSent.Load(),
		MsgsRecv:          s.msgsRecv.Load(),
		MsgsSentByType:    sentByType(s),
		MsgsRecvByType:    recvByType(s),
		ActiveRpcs:        activeRpcs,
		CloseReason:       s.CloseReason(),
		ClusterCounts:     s.snapshotClusters(),
		ChannelIndex:      s.ChannelIndex(),
		LatencyP50:        percentile(sortedLat, 50),
		LatencyP95:        percentile(sortedLat, 95),
		LatencyP99:        percentile(sortedLat, 99),
		LatencyN:          len(sortedLat),
		HeartbeatInterval: hbInterval,
		NextHeartbeat:     nextHb,
		HasRefreshConfig:  hasRefresh,
		Peer:              peerInfoToSnapshot(peer),
	}
}

func sentByType(s *Session) map[string]int64 {
	var out map[string]int64
	for i := reqMsgType(0); i < numReqMsgTypes; i++ {
		if v := s.msgsSentByType[i].Load(); v > 0 {
			if out == nil {
				out = make(map[string]int64, 2)
			}
			out[i.String()] = v
		}
	}
	return out
}

func recvByType(s *Session) map[string]int64 {
	var out map[string]int64
	for i := respMsgType(0); i < numRespMsgTypes; i++ {
		if v := s.msgsRecvByType[i].Load(); v > 0 {
			if out == nil {
				out = make(map[string]int64, 2)
			}
			out[i.String()] = v
		}
	}
	return out
}

func peerInfoToSnapshot(p *spb.PeerInfo) PeerInfoSnapshot {
	if p == nil {
		return PeerInfoSnapshot{}
	}
	return PeerInfoSnapshot{
		TransportType:              p.GetTransportType().String(),
		GoogleFrontendID:           p.GetGoogleFrontendId(),
		ApplicationFrontendID:      p.GetApplicationFrontendId(),
		ApplicationFrontendRegion:  p.GetApplicationFrontendRegion(),
		ApplicationFrontendSubzone: p.GetApplicationFrontendSubzone(),
	}
}

// Snapshot returns a snapshot of the per-handle pool bookkeeping using the
// existing atomics — no lock taken.
func (h *SessionHandle) Snapshot() SessionHandleSnapshot {
	var ewma time.Duration
	if h.ewma != nil {
		ewma = time.Duration(h.ewma.Value())
	}
	return SessionHandleSnapshot{
		Outstanding:  atomic.LoadInt64(&h.outstanding),
		EwmaLatency:  ewma,
		LastActivity: h.GetLastActivity(),
		Picks:        atomic.LoadInt64(&h.picks),
	}
}

// PoolSnapshot returns a snapshot of the pool plus every session currently in
// it. Holds p.mu only long enough to copy out the slice header; per-session
// snapshots are taken after the pool lock is released so per-session locks
// cannot back up under p.mu.
func (p *SessionPoolImpl) PoolSnapshot() PoolSnapshot {
	p.mu.Lock()
	name := p.poolName
	min := p.minSessions
	max := p.maxSessions
	sessionType := p.sessionType
	startingCount := len(p.startingSessions)
	pickerType := "unknown"
	if p.picker != nil {
		pickerType = reflect.TypeOf(p.picker).Elem().Name()
	}
	handles := make([]*SessionHandle, len(p.sessions))
	copy(handles, p.sessions)
	p.mu.Unlock()

	var throttler ThrottlerSnapshot
	if p.budget != nil {
		throttler = p.budget.Snapshot()
	}
	snap := PoolSnapshot{
		Name:           name,
		SessionType:    sessionType.String(),
		MinSessions:    min,
		MaxSessions:    max,
		PickerType:     pickerType,
		StartingCount:  startingCount,
		TotalSessions:  len(handles),
		Sessions:       make([]SessionSnapshot, 0, len(handles)),
		CapturedAt:     time.Now(),
		SessionsOpened: p.sessionsOpened.Load(),
		SessionsClosed: p.sessionsClosed.Load(),
		CloseReasons:   p.snapshotCloseReasons(),
		ListenerFires:  p.listenerFires.Load(),
		Throttler:      throttler,
		ScalingHistory: p.snapshotScalingHistory(),
		OpenRequest:    buildOpenRequestSnapshot(p.openSessionRequest, sessionType),
		SlowVRpcs:      p.snapshotSlowVRpcs(),
		TimeSeries:     p.snapshotTimeSeries(),
	}

	// Pool-level aggregates: combine cluster counts + latency samples
	// across every session. Latency samples are merged from per-session
	// ring buffers; pool percentiles are computed on the combined window.
	aggregatedClusters := map[string]int64{}
	stateCounts := map[string]int{}
	var combinedLatencies []time.Duration

	for _, sh := range handles {
		if sh == nil || sh.session == nil {
			continue
		}
		s := sh.session.Snapshot()
		s.Handle = sh.Snapshot()
		snap.Sessions = append(snap.Sessions, s)

		stateCounts[s.State]++
		if s.State == StateActive.String() {
			snap.ReadyCount++
		}
		if s.Handle.Outstanding > 0 {
			snap.InUseCount++
			snap.PendingCount += int(s.Handle.Outstanding)
		}
		for k, v := range s.ClusterCounts {
			aggregatedClusters[k] += v
		}
		combinedLatencies = append(combinedLatencies, sh.session.snapshotLatencies()...)
	}
	if len(stateCounts) > 0 {
		snap.StateCounts = stateCounts
	}
	if len(aggregatedClusters) > 0 {
		snap.ClusterCounts = aggregatedClusters
	}
	if len(combinedLatencies) > 0 {
		sort.Slice(combinedLatencies, func(i, j int) bool {
			return combinedLatencies[i] < combinedLatencies[j]
		})
		snap.LatencyP50 = percentile(combinedLatencies, 50)
		snap.LatencyP95 = percentile(combinedLatencies, 95)
		snap.LatencyP99 = percentile(combinedLatencies, 99)
		snap.LatencyN = len(combinedLatencies)
	}

	// Lifetime distribution: render whichever buckets actually have data
	// — but always emit them in canonical bucket order so the UI's bar
	// labels line up across pools.
	lifetimes := p.snapshotLifetimes()
	if len(lifetimes) > 0 {
		snap.LifetimeN = len(lifetimes)
		buckets := make([]LifetimeBucketCount, len(LifetimeBuckets))
		for i, b := range LifetimeBuckets {
			buckets[i].Label = b.Label
		}
		for _, d := range lifetimes {
			for i, b := range LifetimeBuckets {
				if d < b.Max {
					buckets[i].Count++
					break
				}
			}
		}
		snap.LifetimeHistogram = buckets
		sort.Slice(lifetimes, func(i, j int) bool { return lifetimes[i] < lifetimes[j] })
		snap.LifetimeP50 = percentile(lifetimes, 50)
		snap.LifetimeP95 = percentile(lifetimes, 95)
		snap.LifetimeP99 = percentile(lifetimes, 99)
	}
	return snap
}
