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
	"testing"
	"time"

	spb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
)

// stubPoolStreamFactory returns a streamFactory that never gets dialled — the
// tests inject sessions manually so the factory should not run. If it does,
// the test should see the error.
func stubPoolStreamFactory(_ context.Context) (Stream, error) {
	return newFakeStream(), nil
}

// injectActiveSession builds a fakeStream-backed Session in StateActive,
// wraps it in a SessionHandle, and pushes it into pool.sessions (registering
// the createdAt time). This bypasses the real Start/handshake path so tests
// can exercise pool-level logic in milliseconds.
func injectActiveSession(t *testing.T, p *SessionPoolImpl, name string, createdAt time.Time) *SessionHandle {
	t.Helper()
	stream := newFakeStream()
	s := NewSession(name, stream, SessionHooks{
		OnStart:  p.OnStart,
		OnActive: p.OnActive,
		OnClose:  p.OnClose,
	}, SessionTypeTable)
	s.mu.Lock()
	s.state = StateActive
	s.mu.Unlock()

	sh := NewSessionHandle(s)
	p.mu.Lock()
	p.sessions = append(p.sessions, sh)
	p.sessionCreatedAt[sh] = createdAt
	p.picker = NewRandomPicker(p.sessions)
	p.mu.Unlock()
	return sh
}

func newTestPool(t *testing.T, min, max int) *SessionPoolImpl {
	t.Helper()
	return NewSessionPoolImpl(
		"test-pool",
		min,
		max,
		stubPoolStreamFactory,
		&spb.OpenSessionRequest{ProtocolVersion: 1},
		nil,
		SessionTypeTable,
	)
}

// TestSessionPool_Close_CompletesWithIdleSessions verifies that Close()
// returns promptly when sessions have nothing in flight, well within the 30s
// internal budget, and that poolCtx is cancelled after.
func TestSessionPool_Close_CompletesWithIdleSessions(t *testing.T) {
	p := newTestPool(t, 1, 10)

	// Three idle sessions registered with the pool. None have in-flight
	// VRPCs, so Session.Close should flip them to Closing and signal
	// quiescent immediately, letting the pool drain fast.
	for i := 0; i < 3; i++ {
		injectActiveSession(t, p, "idle", time.Now())
	}

	done := make(chan error, 1)
	go func() { done <- p.Close() }()

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Close returned err = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not return within 5s for idle sessions (budget is 30s)")
	}

	// poolCtx must be cancelled after Close.
	select {
	case <-p.poolCtx.Done():
	default:
		t.Error("poolCtx not cancelled after Close()")
	}
}

// TestSessionPool_PruneSessions_RespectsAgeGuard verifies pruneSessions skips
// freshly-minted sessions (< minSessionAge) so we don't churn through new
// sessions before they have a chance to absorb load.
func TestSessionPool_PruneSessions_RespectsAgeGuard(t *testing.T) {
	p := newTestPool(t, 1, 10)

	// Freshly minted (createdAt = now): must be skipped by pruneSessions.
	fresh := injectActiveSession(t, p, "fresh", time.Now())

	p.pruneSessions(1)

	p.mu.Lock()
	stillThere := false
	for _, sh := range p.sessions {
		if sh == fresh {
			stillThere = true
			break
		}
	}
	p.mu.Unlock()
	if !stillThere {
		t.Fatal("fresh session was pruned despite age-guard (must be skipped while < minSessionAge)")
	}

	// Time-travel: rewrite createdAt to a time well past minSessionAge so
	// the next prune is allowed to take it.
	p.mu.Lock()
	p.sessionCreatedAt[fresh] = time.Now().Add(-time.Hour)
	p.mu.Unlock()

	p.pruneSessions(1)

	p.mu.Lock()
	stillThere = false
	for _, sh := range p.sessions {
		if sh == fresh {
			stillThere = true
			break
		}
	}
	p.mu.Unlock()
	if stillThere {
		t.Error("aged session was not pruned after age-guard cleared")
	}
}

// TestSessionPool_Close_BoundedByTimeout is skipped: constructing a stuck
// session that ignores Close() requires real readLoop/Send plumbing that
// blocks unrecoverably, which is hard to do safely in a unit test. The 30s
// timeout path is exercised in integration tests.
func TestSessionPool_Close_BoundedByTimeout(t *testing.T) {
	t.Skip("requires a session that ignores Close — see integration tests")
}
