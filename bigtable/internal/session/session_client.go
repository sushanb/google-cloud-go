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
// by the classic bigtable.Client. A SessionClient is per (project, instance,
// appProfile). It owns a channel pool, a ClientConfigurationManager for
// server-driven pool sizing / traffic-split updates, and vends per-table
// SessionTableApi instances.
//
// Callers (bigtable.Client) are responsible for caching per-table
// SessionTableApi instances — NewSessionTable does not cache.
package session

import (
	"context"
	"fmt"
	"net/url"

	btransport "cloud.google.com/go/bigtable/internal/transport"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	"google.golang.org/api/option"
	"google.golang.org/grpc/metadata"
)

// SessionClient is the per-instance connectivity tier for the vRPC session
// transport.
type SessionClient interface {
	// NewSessionTable constructs a fresh SessionTableApi for tableName.
	// Does NOT cache — the consumer (bigtable.Client) is responsible for
	// caching per-table instances to amortize pool creation.
	NewSessionTable(tableName string) SessionTableApi

	// MeterProvider returns the OTel meter provider derived from the metrics
	// factory injected at construction time. Callers can register additional
	// instruments on it. Returns a no-op provider when no metrics factory was
	// injected — registrations are silently dropped.
	MeterProvider() metric.MeterProvider

	// OnSessionLoad registers a listener for SessionLoad updates (0.0 =
	// all-classic, 1.0 = all-session) pushed by the embedded
	// ClientConfigurationManager. Fires once at registration with the CCM's
	// current value (defaults to 0 before any successful poll) and again on
	// each subsequent poll where the new value differs from the previous
	// fire — a poll that returns an unchanged SessionLoad does not refire.
	// Returns an unregister thunk.
	OnSessionLoad(listener func(load float64)) (unregister func())

	// Close closes the embedded ClientConfigurationManager (stopping further
	// listener callbacks) and the underlying channel pool. SessionTableApi
	// instances previously vended become unusable.
	Close() error
}

// NewSessionClient dials Bigtable, constructs a ClientConfigurationManager
// bound to the dialed connection, and returns a SessionClient that owns
// both. Close cascades: CCM is closed first (so no late listener callbacks
// fire against pools that are about to tear down), then the gRPC connection.
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
	opts ...option.ClientOption,
) (SessionClient, error) {
	conn, btClient, err := dialBigtable(ctx, opts...)
	if err != nil {
		return nil, err
	}

	featureFlagsMD := buildFeatureFlagsMD()
	instanceName := fmt.Sprintf("projects/%s/instances/%s", project, instance)
	configMD := metadata.Join(metadata.Pairs(
		resourcePrefixHeader, instanceName,
		requestParamsHeader, fmt.Sprintf("name=%s&app_profile_id=%s",
			url.QueryEscape(instanceName), url.QueryEscape(appProfile)),
	), featureFlagsMD)
	configManager := btransport.NewClientConfigurationManager(btClient, instanceName, appProfile, configMD, nil)
	configManager.Start(ctx)

	return &sessionClient{
		project:        project,
		instance:       instance,
		appProfile:     appProfile,
		conn:           conn,
		btClient:       btClient,
		featureFlagsMD: featureFlagsMD,
		meterProvider:  noop.NewMeterProvider(),
		configManager:  configManager,
	}, nil
}
