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
	"sync/atomic"
	"time"
)

// SessionHandle wraps a Session with the counters the pool needs to
// account for it. Picking has moved to the two-tier AFE-aware flow (see
// afe_picker.go + session_list.go); this type no longer tracks
// per-session PeakEwma or a pool wake-up signal — the pool drives the
// wake-up centrally from Invoke's defer.
type SessionHandle struct {
	session      *Session
	outstanding  int64
	lastActivity int64 // UnixNano timestamp of the last completed call
	picks        int64 // Number of times the picker has picked this handle.
}

// Picks returns the number of times this handle has been picked by the pool's
// picker. Bumped exactly once per successful CheckoutSession.
func (h *SessionHandle) Picks() int64 {
	return atomic.LoadInt64(&h.picks)
}

// NewSessionHandle creates a new SessionHandle wrapping a Session.
func NewSessionHandle(session *Session) *SessionHandle {
	return &SessionHandle{session: session}
}

// IncOutstanding increments outstanding calls.
func (h *SessionHandle) IncOutstanding() {
	atomic.AddInt64(&h.outstanding, 1)
}

// DecOutstanding decrements outstanding calls and stamps lastActivity.
// The pool wakes waiters and returns the session to its AFE queue from
// Invoke's defer, so this method no longer signals directly.
func (h *SessionHandle) DecOutstanding() {
	atomic.AddInt64(&h.outstanding, -1)
	atomic.StoreInt64(&h.lastActivity, time.Now().UnixNano())
}

// GetLastActivity returns the time of the last activity.
func (h *SessionHandle) GetLastActivity() time.Time {
	nano := atomic.LoadInt64(&h.lastActivity)
	if nano == 0 {
		return time.Time{}
	}
	return time.Unix(0, nano)
}
