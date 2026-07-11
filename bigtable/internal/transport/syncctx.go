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
	"sync"
)

// syncCtx serializes execution of session-state mutations onto a single
// goroutine. Every function passed to Execute or ExecuteSync runs strictly
// after the previous one returns; the serializer removes the possibility of
// concurrent state transitions on a single Session without a per-session
// mutex on the read path.
//
// One syncCtx per Session. It owns Session.state transitions, activeRPC
// mutations, and sessionList membership flips. Read paths (State(),
// activeVRPC(), PeerInfo()) stay lock-free atomics — they observe whatever
// the serializer most recently published.
//
// Shape is intentionally close to google.golang.org/grpc/internal/grpcsync
// CallbackSerializer so future contributors can transfer intuition. Java
// parity: io.grpc.SynchronizationContext.
type syncCtx struct {
	callbacks    chan func()
	done         chan struct{}
	shutdown     chan struct{}
	shutdownOnce sync.Once
}

// newSyncCtx spins up a fresh serializer with its runner goroutine.
// The runner runs until Shutdown drains the queue and exits.
func newSyncCtx() *syncCtx {
	sc := &syncCtx{
		callbacks: make(chan func(), 64),
		done:      make(chan struct{}),
		shutdown:  make(chan struct{}),
	}
	go sc.run()
	return sc
}

// Execute schedules fn to run on the serializer goroutine and returns
// immediately. Returns false if the serializer has already been shut down
// and fn was not queued.
//
// Execute is safe to call from ANY goroutine, including from inside a
// callback previously scheduled on this syncCtx (nested Execute is a
// standard "post work to run next tick" pattern).
func (sc *syncCtx) Execute(fn func()) bool {
	select {
	case <-sc.shutdown:
		return false
	default:
	}
	select {
	case sc.callbacks <- fn:
		return true
	case <-sc.shutdown:
		return false
	}
}

// ExecuteSync schedules fn and blocks until fn has returned. Returns false
// if the serializer is already shut down and fn was not executed.
//
// Panics inside fn are captured and re-panicked on the calling goroutine,
// so a single bad callback cannot deadlock the serializer.
//
// MUST NOT be called from inside a syncCtx callback — doing so deadlocks
// (the runner is busy executing the outer fn and can never pick up the
// inner). Use Execute for post-a-follow-up-tick semantics instead.
func (sc *syncCtx) ExecuteSync(fn func()) bool {
	done := make(chan struct{})
	var panicVal any
	wrapper := func() {
		defer close(done)
		defer func() { panicVal = recover() }()
		fn()
	}
	if !sc.Execute(wrapper) {
		return false
	}
	<-done
	if panicVal != nil {
		panic(panicVal)
	}
	return true
}

// Shutdown stops the runner after draining any callbacks already queued.
// Blocks until the runner goroutine has exited. Safe to call multiple
// times; subsequent calls just wait for the same shutdown to complete.
//
// Callbacks scheduled AFTER Shutdown returns are rejected (Execute /
// ExecuteSync return false). Callbacks queued before Shutdown was
// observed still run before the runner exits — this preserves the
// invariant that a successfully scheduled callback always executes.
func (sc *syncCtx) Shutdown() {
	sc.shutdownOnce.Do(func() {
		close(sc.shutdown)
	})
	<-sc.done
}

// run is the serializer goroutine. Pulls callbacks in FIFO order and
// invokes them one at a time. On shutdown, drains any remaining queued
// callbacks so ExecuteSync callers that raced Shutdown don't hang on a
// done channel that never closes.
func (sc *syncCtx) run() {
	defer close(sc.done)
	for {
		select {
		case fn := <-sc.callbacks:
			fn()
		case <-sc.shutdown:
			for {
				select {
				case fn := <-sc.callbacks:
					fn()
				default:
					return
				}
			}
		}
	}
}
