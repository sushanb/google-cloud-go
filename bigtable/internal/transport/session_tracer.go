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
	"fmt"
	"sync"
	"time"

	spb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"google.golang.org/grpc/status"
)

// Session-scoped histograms. They are registered once at process startup via
// InitializeSessionMetrics and shared across every session.
var (
	sessionDurations     metric.Float64Histogram
	sessionOpenLatencies metric.Float64Histogram
	sessionUptime        metric.Float64Histogram

	sessionMetricsOnce sync.Once
	sessionMetricsErr  error
)

// InitializeSessionMetrics registers the session histograms against the given
// meter provider. It runs at most once for the lifetime of the process;
// subsequent calls (with any provider, including nil) return the result of
// the first call. Passing nil on the first call leaves the histograms unset
// and returns nil.
func InitializeSessionMetrics(meterProvider metric.MeterProvider) error {
	sessionMetricsOnce.Do(func() {
		if meterProvider == nil {
			return
		}
		meter := meterProvider.Meter(clientMeterName)

		var err error
		if sessionDurations, err = meter.Float64Histogram(
			"session.durations",
			metric.WithDescription("Duration of operations within a session"),
			metric.WithUnit("ms"),
		); err != nil {
			sessionMetricsErr = fmt.Errorf("create session.durations histogram: %w", err)
			return
		}
		if sessionOpenLatencies, err = meter.Float64Histogram(
			"session.open_latencies",
			metric.WithDescription("Latency to open a session"),
			metric.WithUnit("ms"),
		); err != nil {
			sessionMetricsErr = fmt.Errorf("create session.open_latencies histogram: %w", err)
			return
		}
		if sessionUptime, err = meter.Float64Histogram(
			"session.uptime",
			metric.WithDescription("Total lifetime of a session"),
			metric.WithUnit("ms"),
		); err != nil {
			sessionMetricsErr = fmt.Errorf("create session.uptime histogram: %w", err)
			return
		}
	})
	return sessionMetricsErr
}

// sessionTracer tracks and records metrics for a Session's lifecycle and
// individual operations.
type sessionTracer struct {
	mu          sync.Mutex
	startTime   time.Time
	openedAt    time.Time
	peerInfo    *spb.PeerInfo
	sessionName string
	sessionType SessionType
}

// newSessionTracer starts the "open" timer.
func newSessionTracer(sessionType SessionType) *sessionTracer {
	return &sessionTracer{
		startTime:   time.Now(),
		sessionType: sessionType,
	}
}

// openedAtSnapshot returns the cached open timestamp under the lock so
// callers don't read a torn value.
func (t *sessionTracer) openedAtSnapshot() time.Time {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.openedAt
}

func (t *sessionTracer) setPeerInfo(peerInfo *spb.PeerInfo, sessionName string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.peerInfo = peerInfo
	t.sessionName = sessionName
}

// snapshot captures the fields we need under the lock so that we can do
// allocating work (string formatting, attribute builds) without holding it.
type tracerSnapshot struct {
	openedAt      time.Time
	transportType string
	afeLocation   string
	sessionName   string
}

func (t *sessionTracer) snapshot() tracerSnapshot {
	t.mu.Lock()
	defer t.mu.Unlock()
	snap := tracerSnapshot{
		openedAt:      t.openedAt,
		sessionName:   t.sessionName,
		transportType: "unknown",
	}
	if t.peerInfo != nil {
		if tt := t.peerInfo.GetTransportType().String(); tt != "" {
			snap.transportType = tt
		}
		snap.afeLocation = t.peerInfo.GetApplicationFrontendSubzone()
	}
	return snap
}

// recordOpen records the latency to open the session and stamps openedAt for
// uptime accounting.
func (t *sessionTracer) recordOpen(ctx context.Context, err error) {
	t.mu.Lock()
	t.openedAt = time.Now()
	startedAt := t.startTime
	t.mu.Unlock()

	if sessionOpenLatencies == nil {
		return
	}
	snap := t.snapshot()
	statusStr := "OK"
	if err != nil {
		statusStr = status.Code(err).String()
	}
	sessionOpenLatencies.Record(ctx, msSince(startedAt), metric.WithAttributes(
		attribute.String("transport_type", snap.transportType),
		attribute.String("status", statusStr),
		attribute.String("session_type", t.sessionType.String()),
		attribute.String("afe_location", snap.afeLocation),
		attribute.String("session_name", snap.sessionName),
	))
}

// recordClose records the total uptime when the session closes.
func (t *sessionTracer) recordClose(ctx context.Context) {
	if sessionUptime == nil {
		return
	}
	snap := t.snapshot()
	if snap.openedAt.IsZero() {
		return
	}
	sessionUptime.Record(ctx, msSince(snap.openedAt), metric.WithAttributes(
		attribute.String("transport_type", snap.transportType),
		attribute.String("session_type", t.sessionType.String()),
		attribute.Bool("ready", true),
		attribute.String("afe_location", snap.afeLocation),
		attribute.String("session_name", snap.sessionName),
	))
}

// recordOperation records the execution duration of a single virtual RPC.
func (t *sessionTracer) recordOperation(ctx context.Context, opStartTime time.Time, method string, err error) {
	if sessionDurations == nil {
		return
	}
	snap := t.snapshot()
	statusStr := "OK"
	if err != nil {
		statusStr = status.Code(err).String()
	}
	sessionDurations.Record(ctx, msSince(opStartTime), metric.WithAttributes(
		attribute.String("transport_type", snap.transportType),
		attribute.String("status", statusStr),
		attribute.String("vrpcs", method),
		attribute.String("session_type", t.sessionType.String()),
		attribute.String("closing_reason", ""),
		attribute.Bool("ready", true),
		attribute.String("afe_location", snap.afeLocation),
		attribute.String("session_name", snap.sessionName),
	))
}

func msSince(t time.Time) float64 {
	return float64(time.Since(t)) / float64(time.Millisecond)
}
