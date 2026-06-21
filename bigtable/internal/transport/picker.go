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
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// RoundRobinPicker picks sessions in a round-robin sequence.
type RoundRobinPicker struct {
	mu       sync.Mutex
	sessions []*SessionHandle
	next     uint32
}

// NewRoundRobinPicker creates a new RoundRobinPicker.
func NewRoundRobinPicker(sessions []*SessionHandle) *RoundRobinPicker {
	return &RoundRobinPicker{
		sessions: sessions,
	}
}

// PickSession selects the next idle Active session in round-robin order.
// Returns nil when every session is busy or non-Active — callers (e.g.
// CheckoutSession) treat nil as "park on freeSignal". Skipping busy
// sessions instead of returning them keeps the multiPlexingLimit=1
// invariant downstream and prevents head-of-slice starvation when one
// cohort of sessions handles all traffic.
func (p *RoundRobinPicker) PickSession() *SessionHandle {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := uint32(len(p.sessions))
	if n == 0 {
		return nil
	}
	start := p.next
	for i := uint32(0); i < n; i++ {
		idx := (start + i) % n
		sh := p.sessions[idx]
		if sh == nil || sh.session == nil {
			continue
		}
		if sh.session.State() != StateActive {
			continue
		}
		if atomic.LoadInt64(&sh.outstanding) != 0 {
			continue
		}
		p.next = idx + 1
		return sh
	}
	return nil
}

// PeakEwmaPicker picks sessions based on outstanding request count and EWMA latency.
type PeakEwmaPicker struct {
	mu               sync.Mutex
	sessions         []*SessionHandle
	randomSubsetSize int
	rng              *rand.Rand
}

// NewPeakEwmaPicker creates a new PeakEwmaPicker.
func NewPeakEwmaPicker(sessions []*SessionHandle, randomSubsetSize int) *PeakEwmaPicker {
	if randomSubsetSize <= 0 {
		randomSubsetSize = 2 // Default to 2-choice randomized selection
	}
	return &PeakEwmaPicker{
		sessions:         sessions,
		randomSubsetSize: randomSubsetSize,
		rng:              rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// PickSession selects an idle Active session via Peak EWMA least-cost over
// a randomized K-choice subset. Returns nil when no idle Active session
// exists. Filtering to idle here keeps multiPlexingLimit=1 enforced at the
// pool boundary and prevents the picker from returning a busy session
// that the caller would have to reject anyway.
func (p *PeakEwmaPicker) PickSession() *SessionHandle {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.sessions) == 0 {
		return nil
	}

	// Build the idle-Active set first; K-choice draws come from this
	// pool so a busy/closing session can never win the cost comparison.
	idle := make([]*SessionHandle, 0, len(p.sessions))
	for _, sh := range p.sessions {
		if sh == nil || sh.session == nil {
			continue
		}
		if sh.session.State() != StateActive {
			continue
		}
		if atomic.LoadInt64(&sh.outstanding) != 0 {
			continue
		}
		idle = append(idle, sh)
	}
	if len(idle) == 0 {
		return nil
	}

	subsetSize := p.randomSubsetSize
	if subsetSize >= len(idle) {
		return p.pickMinCost(idle)
	}
	choices := make([]*SessionHandle, subsetSize)
	for i := 0; i < subsetSize; i++ {
		choices[i] = idle[p.rng.Intn(len(idle))]
	}
	return p.pickMinCost(choices)
}

func (p *PeakEwmaPicker) pickMinCost(choices []*SessionHandle) *SessionHandle {
	var minSH *SessionHandle
	minCost := -1.0

	for _, sh := range choices {
		cost := p.getSessionCost(sh)
		if minCost < 0 || cost < minCost {
			minCost = cost
			minSH = sh
		}
	}
	return minSH
}

func (p *PeakEwmaPicker) getSessionCost(sh *SessionHandle) float64 {
	outstanding := atomic.LoadInt64(&sh.outstanding)
	val := 1.0
	if sh.ewma != nil {
		ewmaVal := sh.ewma.Value()
		if ewmaVal > 0 {
			val = ewmaVal
		}
	}
	return float64(outstanding+1) * val
}

// SessionHandle wraps a Session.
type SessionHandle struct {
	session      *Session
	outstanding  int64
	ewma         *PeakEwma
	lastActivity int64 // UnixNano timestamp of the last completed call
	picks        int64 // Number of times the picker has picked this handle.
	// freeSignal is the owning pool's "a session became idle" channel.
	// DecOutstanding does a non-blocking send when outstanding drops
	// to 0 so any worker parked on CheckoutSession wakes up and
	// re-scans. nil when the handle isn't pool-owned (test setups).
	freeSignal chan<- struct{}
}

// Picks returns the number of times this handle has been picked by the pool's
// picker. Bumped exactly once per successful CheckoutSession.
func (h *SessionHandle) Picks() int64 {
	return atomic.LoadInt64(&h.picks)
}

// NewSessionHandle creates a new SessionHandle wrapping a Session.
func NewSessionHandle(session *Session) *SessionHandle {
	return &SessionHandle{
		session: session,
		ewma:    NewPeakEwma(10 * time.Second),
	}
}

// SetFreeSignal connects this handle's DecOutstanding to the pool's
// "a session is now idle" wake-up channel. Called by SessionPoolImpl
// when adding the handle in OnActive.
func (h *SessionHandle) SetFreeSignal(c chan<- struct{}) {
	h.freeSignal = c
}

// IncOutstanding increments outstanding calls.
func (h *SessionHandle) IncOutstanding() {
	atomic.AddInt64(&h.outstanding, 1)
}

// DecOutstanding decrements outstanding calls and updates EWMA latency + lastActivity timestamp.
// When outstanding drops to 0, signals the pool that this session is
// now idle so any worker waiting at CheckoutSession can grab it.
func (h *SessionHandle) DecOutstanding(latency time.Duration) {
	newCount := atomic.AddInt64(&h.outstanding, -1)
	if h.ewma != nil && latency > 0 {
		h.ewma.Update(latency)
	}
	atomic.StoreInt64(&h.lastActivity, time.Now().UnixNano())
	if newCount == 0 && h.freeSignal != nil {
		select {
		case h.freeSignal <- struct{}{}:
		default:
		}
	}
}

// GetLastActivity returns the time of the last activity.
func (h *SessionHandle) GetLastActivity() time.Time {
	nano := atomic.LoadInt64(&h.lastActivity)
	if nano == 0 {
		return time.Time{}
	}
	return time.Unix(0, nano)
}

// Picker defines the interface for picking a session from a pool.
type Picker interface {
	PickSession() *SessionHandle
}

// LeastInFlightPicker picks the session with the lowest outstanding
// vRPC count, breaking ties by lower last-activity (oldest first so
// load spreads). With multiPlexingLimit=1 this collapses to "pick any
// outstanding == 0 session" — the right semantic for the
// LoadBalancingOptions_LeastInFlight_ config the server sends.
type LeastInFlightPicker struct {
	mu       sync.Mutex
	sessions []*SessionHandle
}

// NewLeastInFlightPicker creates a new LeastInFlightPicker.
func NewLeastInFlightPicker(sessions []*SessionHandle) *LeastInFlightPicker {
	return &LeastInFlightPicker{sessions: sessions}
}

// PickSession scans for an idle Active session, tie-breaking on
// lastActivity so the longest-idle session wins. Returns nil when no
// idle Active session exists. Two important properties:
//
//  1. We do NOT break on the first outstanding==0 we find. Returning the
//     first match starves later slots in p.sessions — the wake-up
//     pattern (cap-1 freeSignal → ONE waiter wakes → scans → finds the
//     just-freed session in the first cohort → returns) means later-
//     activated sessions can sit at Picks=0 indefinitely while
//     PendingCount stays high.
//  2. The lastActivity tie-break favors NEVER-PICKED sessions (whose
//     lastActivity is zero) and sessions idle for longer, distributing
//     traffic across every member of the pool rather than letting one
//     cohort handle everything.
//
// All non-Active and busy sessions are filtered out, keeping the
// multiPlexingLimit=1 invariant at the picker boundary.
func (p *LeastInFlightPicker) PickSession() *SessionHandle {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.sessions) == 0 {
		return nil
	}
	var best *SessionHandle
	bestActivity := int64(math.MaxInt64)
	for _, sh := range p.sessions {
		if sh == nil || sh.session == nil {
			continue
		}
		if sh.session.State() != StateActive {
			continue
		}
		if atomic.LoadInt64(&sh.outstanding) != 0 {
			continue
		}
		activity := atomic.LoadInt64(&sh.lastActivity)
		if activity < bestActivity {
			best = sh
			bestActivity = activity
		}
	}
	return best
}

// RandomPicker picks a session randomly from a list of sessions.
type RandomPicker struct {
	mu       sync.Mutex
	sessions []*SessionHandle
	rng      *rand.Rand
}

// NewRandomPicker creates a new RandomPicker.
func NewRandomPicker(sessions []*SessionHandle) *RandomPicker {
	return &RandomPicker{
		sessions: sessions,
		rng:      rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// PickSession selects an idle Active session uniformly at random.
// Returns nil when no idle Active session exists. Filtering to idle
// keeps multiPlexingLimit=1 enforced at the picker boundary.
func (p *RandomPicker) PickSession() *SessionHandle {
	p.mu.Lock()
	defer p.mu.Unlock()
	idle := make([]*SessionHandle, 0, len(p.sessions))
	for _, sh := range p.sessions {
		if sh == nil || sh.session == nil {
			continue
		}
		if sh.session.State() != StateActive {
			continue
		}
		if atomic.LoadInt64(&sh.outstanding) != 0 {
			continue
		}
		idle = append(idle, sh)
	}
	if len(idle) == 0 {
		return nil
	}
	return idle[p.rng.Intn(len(idle))]
}
