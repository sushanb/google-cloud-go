package internal

import (
	"context"
	"sync"
	"time"

	btopt "cloud.google.com/go/bigtable/internal/option"
)

// ChannelHealthMonitor manages the overall health checking process for a pool of connections.
type ChannelHealthMonitor struct {
	config           btopt.HealthCheckConfig
	pool             *BigtableChannelPool
	ticker           *time.Ticker
	done             chan struct{}
	stopOnce         sync.Once  // Add sync.Once
	evictionMu       sync.Mutex // Guards lastEvictionTime
	lastEvictionTime time.Time
	evictionDone     chan struct{} // Notification for test

}

// NewChannelHealthMonitor creates a new ChannelHealthMonitor.
func NewChannelHealthMonitor(config btopt.HealthCheckConfig, pool *BigtableChannelPool) *ChannelHealthMonitor {
	return &ChannelHealthMonitor{
		config: config,
		pool:   pool,
		done:   make(chan struct{}),
	}
}

// Start begins the periodic health checking loop. It takes functions to probe all connections
// and to evict unhealthy ones.
func (chm *ChannelHealthMonitor) Start(ctx context.Context) {
	if !chm.config.Enabled {
		return
	}
	chm.ticker = time.NewTicker(chm.config.ProbeInterval)
	go func() {
		defer chm.ticker.Stop()
		for {
			select {
			case <-chm.ticker.C:
				chm.pool.runProbes(ctx, chm.config)
				// Check if the eviction method returned true
				if chm.pool.detectAndEvictUnhealthy(chm.config, chm.AllowEviction, chm.RecordEviction) {
					select {
					case chm.evictionDone <- struct{}{}:
					default: // Don't block if the channel is full or nil
					}
				}
			case <-chm.done:
				return
			case <-ctx.Done():
				return
			}
		}
	}()
}

// Stop terminates the health checking loop.
func (chm *ChannelHealthMonitor) Stop() {
	if chm.config.Enabled {
		chm.stopOnce.Do(func() {
			close(chm.done)
		})
	}
}

// AllowEviction checks if enough time has passed since the last eviction.
func (chm *ChannelHealthMonitor) AllowEviction() bool {
	chm.evictionMu.Lock()
	defer chm.evictionMu.Unlock()
	return time.Since(chm.lastEvictionTime) >= chm.config.MinEvictionInterval
}

// RecordEviction updates the last eviction time to the current time.
func (chm *ChannelHealthMonitor) RecordEviction() {
	chm.evictionMu.Lock()
	defer chm.evictionMu.Unlock()
	chm.lastEvictionTime = time.Now()
}
