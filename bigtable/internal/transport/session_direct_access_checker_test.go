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
	"errors"
	"testing"
	"time"

	btopt "cloud.google.com/go/bigtable/internal/option"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// TestGetClientConfigDirectAccessChecker mirrors TestDirectAccessLogic's
// coverage of the pingAndWarm checker for the GetClientConfiguration-based
// session checker: adopt on ALTS+OK, drop on non-ALTS, dial-fail fallback,
// RPC-error fallback, PermissionDenied falls through to ALTS.
func TestGetClientConfigDirectAccessChecker(t *testing.T) {
	ctx := context.Background()
	fake := &fakeService{}
	addr := setupTestServer(t, fake)
	baseDialFunc := func() (*BigtableConn, error) { return dialBigtableserver(addr) }

	configMD := metadata.Pairs(
		"x-goog-request-params", "name=projects/test-project/instances/test-instance",
	)

	t.Run("Adopts_ALTS_And_OK", func(t *testing.T) {
		fake.reset()
		var daConn *BigtableConn
		daDialCalled := false
		daDial := func() (*BigtableConn, error) {
			c, err := baseDialFunc()
			if err != nil {
				return nil, err
			}
			if !daDialCalled {
				// Capture only on the checker's initial probe — the pool may
				// call the dialer again for remaining pool slots.
				daConn = c
				daDialCalled = true
			}
			c.isALTSConn.Store(true) // poor man's DirectPath
			return c, nil
		}

		poolSize := 3
		opts := append(poolOpts(), WithDirectAccessChecker(
			NewGetClientConfigDirectAccessChecker(daDial, testInstanceName, testAppProfile, configMD, nil, nil)))
		pool, err := NewBigtableChannelPool(ctx, poolSize, btopt.RoundRobin, baseDialFunc, time.Now(), opts...)
		if err != nil {
			t.Fatalf("Failed to create pool: %v", err)
		}
		defer pool.Close()

		if got := fake.getGetConfigCount(); got == 0 {
			t.Error("GetClientConfiguration was not invoked by the checker")
		}
		conns := pool.getConns()
		if len(conns) != poolSize {
			t.Fatalf("pool size got %d, want %d", len(conns), poolSize)
		}
		if conns[0].conn != daConn {
			t.Error("pool did not reuse the DA connection as the first entry")
		}
		if !conns[0].isALTSUsed() {
			t.Error("reused connection does not report ALTS usage")
		}
		// Verify configMD metadata reached the server.
		md := fake.getGetConfigMetadata()
		if got := md.Get("x-goog-request-params"); len(got) == 0 {
			t.Errorf("configMD not propagated: x-goog-request-params missing from server-side metadata")
		}
	})

	t.Run("DropsNonALTS", func(t *testing.T) {
		fake.reset()
		var daConn *BigtableConn
		daDial := func() (*BigtableConn, error) {
			c, err := baseDialFunc()
			if err != nil {
				return nil, err
			}
			daConn = c
			c.isALTSConn.Store(false)
			return c, nil
		}

		poolSize := 2
		opts := append(poolOpts(), WithDirectAccessChecker(
			NewGetClientConfigDirectAccessChecker(daDial, testInstanceName, testAppProfile, configMD, nil, nil)))
		pool, err := NewBigtableChannelPool(ctx, poolSize, btopt.RoundRobin, baseDialFunc, time.Now(), opts...)
		if err != nil {
			t.Fatalf("Failed to create pool: %v", err)
		}
		defer pool.Close()

		conns := pool.getConns()
		if conns[0].conn == daConn {
			t.Error("pool reused non-ALTS DA connection")
		}
		if !isConnClosed(daConn.ClientConn) {
			t.Error("discarded non-ALTS DA connection was not closed")
		}
	})

	t.Run("DialFail_Fallback", func(t *testing.T) {
		fake.reset()
		daDial := func() (*BigtableConn, error) {
			return nil, errors.New("da dial failed")
		}

		poolSize := 1
		opts := append(poolOpts(), WithDirectAccessChecker(
			NewGetClientConfigDirectAccessChecker(daDial, testInstanceName, testAppProfile, configMD, nil, nil)))
		pool, err := NewBigtableChannelPool(ctx, poolSize, btopt.RoundRobin, baseDialFunc, time.Now(), opts...)
		if err != nil {
			t.Fatalf("Failed to create pool: %v", err)
		}
		defer pool.Close()

		if pool.Num() != 1 {
			t.Errorf("pool size got %d, want 1 (fallback)", pool.Num())
		}
		if got := fake.getGetConfigCount(); got != 0 {
			t.Errorf("GetClientConfiguration called %d times despite dial failure, want 0", got)
		}
	})

	t.Run("GetConfigError_Fallback", func(t *testing.T) {
		fake.reset()
		fake.setGetConfigErr(status.Error(codes.Internal, "simulated getconfig failure"))

		var daConn *BigtableConn
		daDial := func() (*BigtableConn, error) {
			c, err := baseDialFunc()
			if err != nil {
				return nil, err
			}
			daConn = c
			c.isALTSConn.Store(true) // even ALTS shouldn't rescue a non-PermDenied error
			return c, nil
		}

		poolSize := 1
		opts := append(poolOpts(), WithDirectAccessChecker(
			NewGetClientConfigDirectAccessChecker(daDial, testInstanceName, testAppProfile, configMD, nil, nil)))
		pool, err := NewBigtableChannelPool(ctx, poolSize, btopt.RoundRobin, baseDialFunc, time.Now(), opts...)
		if err != nil {
			t.Fatalf("Failed to create pool: %v", err)
		}
		defer pool.Close()

		if pool.getConns()[0].conn == daConn {
			t.Error("pool reused DA connection despite GetClientConfiguration failure")
		}
		if !isConnClosed(daConn.ClientConn) {
			t.Error("failed-probe DA connection was not closed")
		}
	})

	t.Run("PermissionDenied_ChecksALTS", func(t *testing.T) {
		fake.reset()
		fake.setGetConfigErr(status.Error(codes.PermissionDenied, "simulated permission denied"))

		var daConn *BigtableConn
		daDial := func() (*BigtableConn, error) {
			c, err := baseDialFunc()
			if err != nil {
				return nil, err
			}
			daConn = c
			c.isALTSConn.Store(true)
			return c, nil
		}

		poolSize := 1
		opts := append(poolOpts(), WithDirectAccessChecker(
			NewGetClientConfigDirectAccessChecker(daDial, testInstanceName, testAppProfile, configMD, nil, nil)))
		pool, err := NewBigtableChannelPool(ctx, poolSize, btopt.RoundRobin, baseDialFunc, time.Now(), opts...)
		if err != nil {
			t.Fatalf("Failed to create pool: %v", err)
		}
		defer pool.Close()

		conns := pool.getConns()
		if len(conns) == 0 {
			t.Fatalf("pool has no connections")
		}
		// PermissionDenied is not a disqualifier — ALTS check should still adopt.
		if conns[0].conn != daConn {
			t.Error("pool did not reuse DA connection despite ALTS+PermDenied-only failure")
		}
	})
}
