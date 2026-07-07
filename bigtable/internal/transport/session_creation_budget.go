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
	"time"
)

// SessionThrottler manages the pacing and rate-limiting for creating new sessions.
type SessionThrottler interface {
	// Acquire blocks until a session creation token is available or ctx is done.
	Acquire(ctx context.Context) error
	// Release returns a token back to the throttler, registering a penalty duration hold-off on failure.
	Release(success bool)
	// Snapshot exposes the throttler's current state for the debug UI:
	// InUse is the number of tokens currently held by in-flight creations,
	// Capacity is the total semaphore size, PenaltyDuration is the hold-off
	// applied to a failed-creation token before it returns to the pool.
	Snapshot() ThrottlerSnapshot
}

// ThrottlerSnapshot is the budget state surfaced to the debug UI.
type ThrottlerSnapshot struct {
	InUse           int
	Capacity        int
	PenaltyDuration time.Duration
}

// AdaptiveSessionThrottler implements a concurrency governor with adaptive failure penalties using a channel semaphore.
type AdaptiveSessionThrottler struct {
	sem             chan struct{}
	penaltyDuration time.Duration
}

// NewAdaptiveSessionThrottler creates a new SessionThrottler implemented by AdaptiveSessionThrottler.
func NewAdaptiveSessionThrottler(maxConcurrent int, penaltyDuration time.Duration) SessionThrottler {
	return &AdaptiveSessionThrottler{
		sem:             make(chan struct{}, maxConcurrent),
		penaltyDuration: penaltyDuration,
	}
}

// Acquire blocks context-safely until a session creation token is available or ctx is done.
func (b *AdaptiveSessionThrottler) Acquire(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case b.sem <- struct{}{}:
		return nil
	}
}

// Release releases a token back to the throttler, registering a penalty duration hold-off on failure.
func (b *AdaptiveSessionThrottler) Release(success bool) {
	if success {
		<-b.sem
	} else {
		// Hold the token for b.penaltyDuration before releasing it to protect the backend AFE
		time.AfterFunc(b.penaltyDuration, func() {
			<-b.sem
		})
	}
}

// Snapshot returns the throttler's current state. len(sem) is the number of
// in-flight creations holding a token; cap(sem) is the configured ceiling.
func (b *AdaptiveSessionThrottler) Snapshot() ThrottlerSnapshot {
	return ThrottlerSnapshot{
		InUse:           len(b.sem),
		Capacity:        cap(b.sem),
		PenaltyDuration: b.penaltyDuration,
	}
}
