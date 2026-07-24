// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package internal

import (
	"context"
	"log"

	bigtablepb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
	btopt "cloud.google.com/go/bigtable/internal/option"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// getClientConfigDirectAccessChecker probes Direct Access compatibility by
// dialing once and issuing GetClientConfiguration + ALTS inspection. It is
// the session-pool sibling of pingAndWarmDirectAccessChecker — same
// dial/adopt/report shape, same investigation on failure — but uses
// GetClientConfiguration (the same RPC ClientConfigurationManager polls)
// as the probe RPC, since session-based clients don't run PingAndWarm.
type getClientConfigDirectAccessChecker struct {
	dialer          func() (*BigtableConn, error)
	instanceName    string
	appProfileID    string
	configMD        metadata.MD
	daEligibleGauge metric.Int64Gauge
	logger          *log.Logger
}

// NewGetClientConfigDirectAccessChecker constructs the session-pool
// DirectAccessChecker. A nil meterProvider produces a checker that silently
// skips metric reporting. configMD should be the same instance-scoped
// metadata that ClientConfigurationManager attaches to its polls
// (resource-prefix + request-params + feature-flag headers).
func NewGetClientConfigDirectAccessChecker(
	dialer func() (*BigtableConn, error),
	instanceName, appProfileID string,
	configMD metadata.MD,
	meterProvider metric.MeterProvider,
	logger *log.Logger,
) DirectAccessChecker {
	return &getClientConfigDirectAccessChecker{
		dialer:          dialer,
		instanceName:    instanceName,
		appProfileID:    appProfileID,
		configMD:        configMD,
		daEligibleGauge: newDirectAccessEligibleGauge(meterProvider, logger),
		logger:          logger,
	}
}

// Dialer returns the configured direct-access dialer.
func (c *getClientConfigDirectAccessChecker) Dialer() func() (*BigtableConn, error) {
	return c.dialer
}

// CheckCompatibility opens a single probe connection, calls
// GetClientConfiguration, and decides whether Direct Access is usable. On
// compatible: the probed connection is returned so the pool can adopt it
// as its first connection. On incompatible: the probe connection is
// closed and an async investigation begins.
func (c *getClientConfigDirectAccessChecker) CheckCompatibility(ctx context.Context) (*BigtableConn, bool) {
	conn, err := c.dialer()
	if err != nil {
		btopt.Debugf(c.logger, "bigtable_direct_access: dial failed: %v", err)
		return nil, false
	}

	stub := bigtablepb.NewBigtableClient(conn)
	_, _, _, err = FetchClientConfigurationOnce(ctx, stub, c.instanceName, c.appProfileID, c.configMD)
	if err != nil {
		// PermissionDenied is expected on probes whose bootstrap credentials
		// lack GetClientConfiguration access; fall through to the ALTS
		// check rather than failing fast — mirrors pingAndWarm's stance.
		if status.Code(err) != codes.PermissionDenied {
			btopt.Debugf(c.logger, "bigtable_direct_access: GetClientConfiguration failed during compatibility check: %v", err)
			conn.Close()
			go c.investigateFailure(err)
			return nil, false
		}
		btopt.Debugf(c.logger, "bigtable_direct_access: GetClientConfiguration failed with PermissionDenied, continuing to ALTS check: %v", err)
	}

	if conn.isALTSConn.Load() {
		btopt.Debugf(c.logger, "bigtable_direct_access: GetClientConfiguration compatibility check succeeded (ALTS conn, ip_preference=%s)", conn.ipProtocol())
		c.reportSuccess(ctx, conn.ipProtocol())
		return conn, true
	}

	conn.Close()
	go c.investigateFailure(err)
	return nil, false
}

// reportSuccess records a direct_access/compatible=1 reading.
func (c *getClientConfigDirectAccessChecker) reportSuccess(ctx context.Context, ipPreference string) {
	if c.daEligibleGauge == nil {
		return
	}
	c.daEligibleGauge.Record(ctx, 1, metric.WithAttributes(
		attribute.String("ip_preference", ipPreference),
		attribute.String("reason", ""),
	))
}

// reportFailure records a direct_access/compatible=0 reading with the given
// reason tag.
func (c *getClientConfigDirectAccessChecker) reportFailure(reason string) {
	if c.daEligibleGauge == nil {
		return
	}
	c.daEligibleGauge.Record(context.Background(), 0, metric.WithAttributes(
		attribute.String("ip_preference", ""),
		attribute.String("reason", reason),
	))
}

// investigateFailure runs the shared GCE-precondition walk with a
// GetClientConfiguration probe for the isolated-endpoint step.
func (c *getClientConfigDirectAccessChecker) investigateFailure(originalErr error) {
	investigateDirectAccessFailure(c.logger, c.reportFailure, c.probe, originalErr)
}

// probe issues a single GetClientConfiguration against the given conn —
// used by the isolated-endpoint step of the failure investigation.
func (c *getClientConfigDirectAccessChecker) probe(ctx context.Context, conn *BigtableConn) error {
	stub := bigtablepb.NewBigtableClient(conn)
	_, _, _, err := FetchClientConfigurationOnce(ctx, stub, c.instanceName, c.appProfileID, c.configMD)
	return err
}
