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

// Package session provides the internal vRPC-session connectivity tier used
// by both the classic bigtable.Client and the accelerator. A SessionClient is
// per (project, instance, appProfile). It owns a channel pool and vends
// per-table SessionTableApi instances.
//
// Callers (bigtable.Client, accelerator.AcceleratorChannel) are responsible
// for caching per-table SessionTableApi instances — NewSessionTable does not
// cache.
package session

import (
	"context"

	btransport "cloud.google.com/go/bigtable/internal/transport"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	"google.golang.org/api/option"
)

// SessionClient is the per-instance connectivity tier for the vRPC session
// transport.
type SessionClient interface {
	// NewSessionTable constructs a fresh SessionTableApi for tableName.
	// Does NOT cache — the consumer (bigtable.Client / AcceleratorChannel) is
	// responsible for caching per-table instances to amortize pool creation.
	NewSessionTable(tableName string) SessionTableApi

	// MeterProvider returns the OTel meter provider derived from the metrics
	// factory injected at construction time. Callers can register additional
	// instruments on it (for example, the accelerator's python-to-wire hop
	// histograms). Returns a no-op provider when no metrics factory was
	// injected — registrations are silently dropped.
	MeterProvider() metric.MeterProvider

	// Close closes the underlying channel pool. SessionTableApi instances
	// previously vended become unusable.
	Close() error
}

// NewSessionClient dials Bigtable and constructs a SessionClient bound to the
// given resource scope. configManager is optional — when non-nil, each
// SessionPoolImpl created by this client registers a listener so the server
// can push pool sizing / channel-selection configuration.
//
// TODO: Replace the simple single-connection dial with a per-AFE channel pool
// driven by ClientConfigurationManager — see SESSION_CLIENT_REFACTOR.md
// ("Why a separate channel pool"). For now we dial a single connection so the
// rest of the structure can be exercised end-to-end.
//
// TODO (Phase 4): Accept a *metrics.BuiltinMetricsTracerFactory parameter and
// thread it through to per-pool / per-tracer construction. For now metrics
// recording is a no-op on the session path; MeterProvider() returns a no-op
// provider.
func NewSessionClient(
	ctx context.Context,
	project, instance, appProfile string,
	configManager *btransport.ClientConfigurationManager,
	opts ...option.ClientOption,
) (SessionClient, error) {
	conn, btClient, err := dialBigtable(ctx, opts...)
	if err != nil {
		return nil, err
	}
	return &sessionClient{
		project:        project,
		instance:       instance,
		appProfile:     appProfile,
		conn:           conn,
		btClient:       btClient,
		featureFlagsMD: buildFeatureFlagsMD(),
		meterProvider:  noop.NewMeterProvider(),
		configManager:  configManager,
	}, nil
}
