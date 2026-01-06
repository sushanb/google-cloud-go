// Copyright 2025 Google LLC
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
	"sync/atomic"
	"testing"
	"time"

	"cloud.google.com/go/bigtable/internal/option"
	btopt "cloud.google.com/go/bigtable/internal/option"
)

func TestConnHealthStateAddProbeResult(t *testing.T) {
	chs := &connHealthState{}
	config := btopt.DefaultHealthCheckConfig()
	chs.addProbeResult(true, config.WindowDuration)
	if len(chs.probeHistory) != 1 || !chs.probeHistory[0].successful || chs.successfulProbes != 1 || chs.failedProbes != 0 {
		t.Errorf("Add successful probe failed: %+v", chs)
	}
	chs.addProbeResult(false, config.WindowDuration)
	if len(chs.probeHistory) != 2 || chs.probeHistory[1].successful || chs.successfulProbes != 1 || chs.failedProbes != 1 {
		t.Errorf("Add failed probe failed: %+v", chs)
	}
}

func TestConnHealthStatePruneHistory(t *testing.T) {
	chs := &connHealthState{}
	config := btopt.DefaultHealthCheckConfig()
	now := time.Now()
	chs.mu.Lock()
	chs.probeHistory = []probeResult{
		{t: now.Add(-config.WindowDuration - time.Second), successful: true},
		{t: now.Add(-config.WindowDuration + time.Millisecond), successful: false},
	}
	chs.successfulProbes = 1
	chs.failedProbes = 1
	chs.mu.Unlock()

	chs.addProbeResult(true, config.WindowDuration) // This triggers prune

	chs.mu.Lock()
	defer chs.mu.Unlock()
	if len(chs.probeHistory) != 2 || chs.successfulProbes != 1 || chs.failedProbes != 1 {
		t.Errorf("Prune failed, history length %d, successful %d, failed %d", len(chs.probeHistory), chs.successfulProbes, chs.failedProbes)
	}
}

func TestChannelHealthMonitor_Stop(t *testing.T) {
	t.Run("Enabled", func(t *testing.T) {
		config := btopt.DefaultHealthCheckConfig()
		if !config.Enabled {
			t.Fatal("DefaultHealthCheckConfig.Enabled should be true for this test")
		}
		chm := NewChannelHealthMonitor(config, nil)
		chm.Stop()
		chm.Stop() // The sync.Once should prevent a panic on double close
		select {
		case <-chm.done:
		default:
			t.Errorf("chm.done not closed after Stop()")
		}
	})

	t.Run("Disabled", func(t *testing.T) {
		config := btopt.DefaultHealthCheckConfig()
		config.Enabled = false
		chm := NewChannelHealthMonitor(config, nil)
		chm.Stop()
		select {
		case <-chm.done:
			t.Errorf("chm.done was closed, but monitor was disabled")
		default:
		}
	})
}

func TestRunProbesWhenContextDone(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fake := &fakeService{}
	addr := setupTestServer(t, fake)
	dialFunc := func() (*BigtableConn, error) { return dialBigtableserver(addr) }
	pool, err := NewBigtableChannelPool(ctx, 2, btopt.RoundRobin, dialFunc, time.Now())
	hcConfig := option.DefaultHealthCheckConfig()
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}
	defer pool.Close()

	probeCtx, cancelProbe := context.WithCancel(ctx)
	cancelProbe()

	pool.runProbes(probeCtx, hcConfig)

	conns := pool.getConns()
	for i, entry := range conns {
		entry.health.mu.Lock()
		if len(entry.health.probeHistory) != 1 || entry.health.probeHistory[0].successful {
			t.Errorf("Entry %d: Expected 1 failed probe due to context done, got %+v", i, entry.health.probeHistory)
		}
		entry.health.mu.Unlock()
	}
}

func TestConnHealthStateIsHealthy(t *testing.T) {
	config := btopt.HealthCheckConfig{MinProbesForEval: 3, FailurePercentThresh: 50}
	tests := []struct {
		name       string
		results    []bool
		isHealthy  bool
		numSuccess int
		numFailed  int
	}{
		{"NotEnoughProbes", []bool{true, false}, true, 1, 1},
		{"Healthy", []bool{true, true, false}, true, 2, 1},
		{"Unhealthy", []bool{true, false, false, false}, false, 1, 3},
		{"JustUnhealthy", []bool{true, true, false, false, false}, false, 2, 3},
		{"AllSuccessful", []bool{true, true, true}, true, 3, 0},
		{"AllFailed", []bool{false, false, false}, false, 0, 3},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			chs := &connHealthState{}
			for _, r := range tc.results {
				chs.addProbeResult(r, time.Minute)
			}

			if got := chs.isHealthy(config.MinProbesForEval, config.FailurePercentThresh); got != tc.isHealthy {
				t.Errorf("isHealthy() got %v, want %v", got, tc.isHealthy)
			}
			if chs.successfulProbes != tc.numSuccess || chs.failedProbes != tc.numFailed {
				t.Errorf("counts got success=%d, failed=%d; want success=%d, failed=%d", chs.successfulProbes, chs.failedProbes, tc.numSuccess, tc.numFailed)
			}
		})
	}
}

func TestDetectAndEvictUnhealthy(t *testing.T) {
	ctx := context.Background()
	const poolSize = 10
	testConfig := btopt.HealthCheckConfig{
		Enabled:                  true,
		ProbeInterval:            30 * time.Second,
		ProbeTimeout:             1 * time.Second,
		WindowDuration:           5 * time.Minute,
		MinProbesForEval:         5,
		FailurePercentThresh:     20,
		PoolwideBadThreshPercent: 50,
		MinEvictionInterval:      0, // Allow immediate eviction for test
	}

	fake := &fakeService{}
	addr := setupTestServer(t, fake)
	dialFunc := func() (*BigtableConn, error) { return dialBigtableserver(addr) }

	setupHealth := func(entry *connEntry, successful, failed int) {
		entry.health.mu.Lock()
		defer entry.health.mu.Unlock()
		entry.health.successfulProbes, entry.health.failedProbes = successful, failed
		for i := 0; i < successful+failed; i++ {
			entry.health.probeHistory = append(entry.health.probeHistory, probeResult{t: time.Now()})
		}
	}

	t.Run("EvictOneUnhealthy", func(t *testing.T) {
		pool, err := NewBigtableChannelPool(ctx, poolSize, btopt.RoundRobin, dialFunc, time.Now())
		if err != nil {
			t.Fatalf("Failed to create pool: %v", err)
		}
		defer pool.Close()

		chm := NewChannelHealthMonitor(testConfig, pool)
		chm.Start(ctx)

		unhealthyIdx := 3
		conns := pool.getConns()
		for _, entry := range conns {
			setupHealth(entry, 10, 0) // Healthy
		}
		setupHealth(conns[unhealthyIdx], 7, 3) // 30% failure > 20% thresh -> Unhealthy
		pool.conns.Store(&conns)

		oldConn := pool.getConns()[unhealthyIdx].conn
		if !pool.detectAndEvictUnhealthy(testConfig, chm.AllowEviction, chm.RecordEviction) {
			t.Fatal("Connection was not evicted")
		}
		time.Sleep(50 * time.Millisecond) // Allow replacement goroutine to run
		if pool.getConns()[unhealthyIdx].conn == oldConn {
			t.Errorf("Connection at index %d was not replaced", unhealthyIdx)
		}
	})

	t.Run("CircuitBreakerTooManyUnhealthy", func(t *testing.T) {
		pool, err := NewBigtableChannelPool(ctx, poolSize, btopt.RoundRobin, dialFunc, time.Now())
		if err != nil {
			t.Fatalf("Failed to create pool: %v", err)
		}
		defer pool.Close()

		chm := NewChannelHealthMonitor(testConfig, pool)
		chm.Start(ctx)

		conns := pool.getConns()
		for i, entry := range conns {
			if i < 6 { // 60% unhealthy > 50% PoolwideBadThreshPercent
				setupHealth(entry, 5, 5)
			} else {
				setupHealth(entry, 10, 0)
			}
		}
		pool.conns.Store(&conns)
		if pool.detectAndEvictUnhealthy(testConfig, chm.AllowEviction, chm.RecordEviction) {
			t.Error("Connection was evicted when circuit breaker should have tripped")
		}
	})
}

func TestHealthCheckerIntegration(t *testing.T) {
	ctx := context.Background()
	// Shorten times for testing
	testHCConfig := btopt.HealthCheckConfig{
		Enabled:                  true,
		ProbeInterval:            50 * time.Millisecond,
		ProbeTimeout:             1 * time.Second, // Keep timeout reasonable
		WindowDuration:           500 * time.Millisecond,
		MinProbesForEval:         2,
		FailurePercentThresh:     40,
		PoolwideBadThreshPercent: 70, // Or as needed
		MinEvictionInterval:      100 * time.Millisecond,
	}
	fake1, fake2 := &fakeService{}, &fakeService{}
	addr1, addr2 := setupTestServer(t, fake1), setupTestServer(t, fake2)
	dialOpts := []string{addr1, addr2}
	var dialIdx int32

	dialFunc := func() (*BigtableConn, error) {
		idx := atomic.AddInt32(&dialIdx, 1) - 1
		addr := dialOpts[idx%2]
		if idx >= 2 { // Replacements always go to addr2
			addr = addr2
		}
		return dialBigtableserver(addr)
	}

	pool, err := NewBigtableChannelPool(ctx, 2, btopt.RoundRobin, dialFunc, time.Now())
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}
	defer pool.Close()

	chm := NewChannelHealthMonitor(testHCConfig, pool)
	// for signalling
	chm.evictionDone = make(chan struct{}, 1)
	chm.Start(ctx)
	chm.Start(ctx)

	time.Sleep(2 * testHCConfig.WindowDuration) // Let initial probes run

	fake1.setPingErr(errors.New("server1 unhealthy")) // Make conn 0 fail;

	// Wait for the monitor to signal that an eviction has occurred
	select {
	case <-chm.evictionDone:
		// Eviction triggered successfully
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for eviction signal")
	}

	conns := pool.getConns()
	if len(conns) > 0 && conns[0].conn.ClientConn.Target() != addr2 {
		t.Errorf("Connection 0 target was %s, expected %s", conns[0].conn.ClientConn.Target(), addr2)
	}
	if len(pool.getConns()) > 1 && pool.getConns()[1].conn.ClientConn.Target() != addr2 {
		t.Errorf("Connection 1 target changed unexpectedly")
	}
}
