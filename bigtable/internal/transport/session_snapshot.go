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
	"sync/atomic"
	"time"

	spb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
)

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
	MsgsSent          int64
	MsgsRecv          int64
	// MsgsSentByType / MsgsRecvByType break the totals above down by the
	// SessionRequest / SessionResponse oneof payload type. Keys come from the
	// reqMsgType / respMsgType String() methods (e.g. "VirtualRpc", "Heartbeat").
	// Buckets with a zero count are omitted to keep the rendered cell short.
	MsgsSentByType    map[string]int64
	MsgsRecvByType    map[string]int64
	ActiveRpcs        int
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
}

// PoolSnapshot is a snapshot of one SessionPoolImpl, including every session
// currently in the pool. Sessions are listed in their pool order; callers may
// re-sort as they wish.
type PoolSnapshot struct {
	Name          string
	SessionType   string
	MinSessions   int
	MaxSessions   int
	PickerType    string
	ReadyCount    int
	StartingCount int
	InUseCount    int
	PendingCount  int
	TotalSessions int
	Sessions      []SessionSnapshot
	CapturedAt    time.Time
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

	return SessionSnapshot{
		LogName:           logName,
		State:             state.String(),
		SessionType:       sessionType.String(),
		LastStateChange:   lastChange,
		OkRpcs:            s.okRpcs.Load(),
		ErrorRpcs:         s.errorRpcs.Load(),
		MsgsSent:          s.msgsSent.Load(),
		MsgsRecv:          s.msgsRecv.Load(),
		MsgsSentByType:    sentByType(s),
		MsgsRecvByType:    recvByType(s),
		ActiveRpcs:        activeRpcs,
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

	snap := PoolSnapshot{
		Name:          name,
		SessionType:   sessionType.String(),
		MinSessions:   min,
		MaxSessions:   max,
		PickerType:    pickerType,
		StartingCount: startingCount,
		TotalSessions: len(handles),
		Sessions:      make([]SessionSnapshot, 0, len(handles)),
		CapturedAt:    time.Now(),
	}

	for _, sh := range handles {
		if sh == nil || sh.session == nil {
			continue
		}
		s := sh.session.Snapshot()
		s.Handle = sh.Snapshot()
		snap.Sessions = append(snap.Sessions, s)

		if s.State == StateActive.String() {
			snap.ReadyCount++
		}
		if s.Handle.Outstanding > 0 {
			snap.InUseCount++
			snap.PendingCount += int(s.Handle.Outstanding)
		}
	}
	return snap
}
