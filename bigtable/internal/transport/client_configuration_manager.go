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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	bigtablepb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
	btopt "cloud.google.com/go/bigtable/internal/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/durationpb"
)

const MinPollingInterval = 1 * time.Minute

// clientConfig holds configuration for the client.
type clientConfig struct {
	Polling          pollingConfig
	Session          sessionConfig
	HasSessionConfig bool
}

// Clone returns a deep copy of the clientConfig.
func (c clientConfig) Clone() clientConfig {
	// return by value so it copies but future proofing
	return c
}

type pollingConfig struct {
	PollingInterval  time.Duration
	ValidityDuration time.Duration
	MaxRpcRetryCount int
}

type sessionConfig struct {
	SessionLoad float64
	ChannelPool channelPoolConfig
	SessionPool sessionPoolConfig
}

type channelPoolConfig struct {
	MinServerCount             int
	MaxServerCount             int
	PerServerSessionCount      int
	DirectAccessCheckInterval  time.Duration
	DirectAccessErrorThreshold float32
}

type sessionPoolConfig struct {
	Headroom                           float32
	MinSessionCount                    int
	MaxSessionCount                    int
	NewSessionCreationBudget           int
	NewSessionCreationPenalty          time.Duration
	ConsecutiveSessionFailureThreshold int
	NewSessionQueueLength              int
	LoadBalancing                      loadBalancingOptions
}

type loadBalancingStrategy int

const (
	StrategyLeastInFlight loadBalancingStrategy = iota
	StrategyRandom
	StrategyPeakEwma
)

type loadBalancingOptions struct {
	Strategy         loadBalancingStrategy
	RandomSubsetSize int
}

// configListener is a callback function for configuration changes.
type configListener func(newConfig clientConfig, seq int64)

// ClientConfigurationManager manages the dynamic client configuration for Bigtable.
// It periodically polls for client configuration updates via GetClientConfiguration RPCs.
type ClientConfigurationManager struct {
	// done is closed when the manager is closed to signal the background polling goroutine to exit.
	done          chan struct{}
	client        bigtablepb.BigtableClient
	instanceName  string
	appProfileId  string
	metadata      metadata.MD
	defaultConfig clientConfig
	logger        *log.Logger

	// closeOnce ensures Close()'s teardown logic runs exactly once, so a second
	// Close() call cannot panic by re-closing m.done.
	closeOnce sync.Once
	// closed is set to true at the very start of Close()'s teardown, BEFORE
	// m.done is closed. poll() consults it via isClosed() to suppress listener
	// invocations against pools that may already be tearing down.
	closed atomic.Bool
	// pollsWG tracks in-flight poll() invocations spawned by Start() and
	// pollingLoop(). Close() waits on it so callers can rely on no listener
	// callbacks firing after Close() returns.
	pollsWG sync.WaitGroup

	mu            sync.RWMutex
	currentConfig clientConfig
	// configSeq is a monotonically increasing sequence number incremented every time the configuration changes.
	configSeq      int64
	validUntil     time.Time
	listeners      map[int]configListener
	nextListenerID int
}

var defaultClientConfig = clientConfig{
	Polling: pollingConfig{
		PollingInterval:  300 * time.Second,
		ValidityDuration: 100 * 365 * 24 * time.Hour, // Safe representation of 10,000 years.
		MaxRpcRetryCount: 5,
	},
	Session: sessionConfig{
		SessionLoad: 0,
		ChannelPool: channelPoolConfig{
			MinServerCount:             2,
			MaxServerCount:             25,
			PerServerSessionCount:      10,
			DirectAccessCheckInterval:  60 * time.Second,
			DirectAccessErrorThreshold: 0.8,
		},
		SessionPool: sessionPoolConfig{
			Headroom:                           0.5,
			MinSessionCount:                    5,
			MaxSessionCount:                    400,
			NewSessionCreationBudget:           50,
			NewSessionCreationPenalty:          60 * time.Second,
			ConsecutiveSessionFailureThreshold: 10,
			NewSessionQueueLength:              10,
			LoadBalancing: loadBalancingOptions{
				Strategy:         StrategyLeastInFlight,
				RandomSubsetSize: 0,
			},
		},
	},
	HasSessionConfig: true,
}

// NewClientConfigurationManager creates a new ClientConfigurationManager.
func NewClientConfigurationManager(
	client bigtablepb.BigtableClient,
	instanceName string,
	appProfileId string,
	md metadata.MD,
	logger *log.Logger,
) *ClientConfigurationManager {
	done := make(chan struct{})

	return &ClientConfigurationManager{
		done:          done,
		client:        client,
		instanceName:  instanceName,
		appProfileId:  appProfileId,
		metadata:      md,
		defaultConfig: defaultClientConfig,
		currentConfig: defaultClientConfig,
		validUntil:    time.Now().Add(time.Hour * 24 * 365 * 100), //  default far in future
		listeners:     make(map[int]configListener),
		logger:        logger,
	}
}

// Start begins the polling process.
func (m *ClientConfigurationManager) Start(ctx context.Context) {
	btopt.Debugf(m.logger, "bigtable: starting client configuration manager for instance %q, app profile %q", m.instanceName, m.appProfileId)
	// We need a context for the initial poll.
	m.pollsWG.Add(1)
	go func() {
		defer m.pollsWG.Done()
		pollCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		m.poll(pollCtx)
	}()

	// Start background polling
	go m.pollingLoop(ctx)
}

// Close stops the polling process.
//
// Close is safe to call multiple times; only the first call performs teardown.
// It also waits for any in-flight poll() invocations spawned by Start() and
// pollingLoop() to return before it itself returns. Combined with the closed
// gate that poll() consults before firing listeners, this guarantees no
// listener callbacks fire after Close() returns — so SessionPools registered
// via AddSessionPoolListener can be Close'd immediately after this returns
// without racing against a late configuration callback.
func (m *ClientConfigurationManager) Close() {
	m.closeOnce.Do(func() {
		btopt.Debugf(m.logger, "bigtable: closing client configuration manager")
		// Set closed BEFORE closing m.done so any poll() that observes the
		// done channel after this point also observes closed == true.
		m.closed.Store(true)
		close(m.done)
		// Wait for in-flight polls (including their listener callbacks, which
		// are short-circuited by isClosed()) to finish before returning.
		m.pollsWG.Wait()
	})
}

// isClosed reports whether Close() has begun teardown. poll() consults this
// before invoking listeners so callbacks do not fire after Close() returns.
func (m *ClientConfigurationManager) isClosed() bool {
	return m.closed.Load()
}

// getConfig returns the current configuration.
func (m *ClientConfigurationManager) getConfig() clientConfig {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.currentConfig.Clone()
}

// addListener adds a listener for configuration changes.
func (m *ClientConfigurationManager) addListener(listener configListener) func() {
	m.mu.Lock()
	id := m.nextListenerID
	m.nextListenerID++
	if m.listeners == nil {
		m.listeners = make(map[int]configListener)
	}
	m.listeners[id] = listener

	cfg := m.currentConfig.Clone()
	seq := m.configSeq
	m.mu.Unlock()

	btopt.Debugf(m.logger, "bigtable: adding configuration listener (id: %d)", id)
	listener(cfg, seq)

	return func() {
		m.mu.Lock()
		delete(m.listeners, id)
		m.mu.Unlock()
	}
}

// AddSessionPoolListener registers a callback that receives raw SessionPoolConfiguration updates.
//
// Forwards EVERY field of the parsed sessionPoolConfig — not just the size
// bounds. Before this, Headroom, NewSessionQueueLength, and the entire
// LoadBalancingOptions oneof were silently dropped, so the pool ran with
// the constructor defaults no matter what the control plane sent.
func (m *ClientConfigurationManager) AddSessionPoolListener(listener func(*bigtablepb.SessionClientConfiguration_SessionPoolConfiguration)) func() {
	return m.addListener(func(cfg clientConfig, seq int64) {
		sp := cfg.Session.SessionPool
		spCfg := &bigtablepb.SessionClientConfiguration_SessionPoolConfiguration{
			Headroom:                           sp.Headroom,
			MinSessionCount:                    int32(sp.MinSessionCount),
			MaxSessionCount:                    int32(sp.MaxSessionCount),
			NewSessionCreationBudget:           int32(sp.NewSessionCreationBudget),
			ConsecutiveSessionFailureThreshold: int32(sp.ConsecutiveSessionFailureThreshold),
			NewSessionQueueLength:              int32(sp.NewSessionQueueLength),
		}
		if sp.NewSessionCreationPenalty > 0 {
			spCfg.NewSessionCreationPenalty = durationpb.New(sp.NewSessionCreationPenalty)
		}
		spCfg.LoadBalancingOptions = encodeLoadBalancing(sp.LoadBalancing)
		listener(spCfg)
	})
}

// encodeLoadBalancing converts the parsed loadBalancingOptions back into the
// proto LoadBalancingOptions oneof so downstream consumers (PoolSizer,
// SessionPoolImpl.UpdateConfig) see the same value the control plane sent.
func encodeLoadBalancing(lb loadBalancingOptions) *bigtablepb.LoadBalancingOptions {
	switch lb.Strategy {
	case StrategyRandom:
		return &bigtablepb.LoadBalancingOptions{
			LoadBalancingStrategy: &bigtablepb.LoadBalancingOptions_Random_{
				Random: &bigtablepb.LoadBalancingOptions_Random{},
			},
		}
	case StrategyLeastInFlight:
		return &bigtablepb.LoadBalancingOptions{
			LoadBalancingStrategy: &bigtablepb.LoadBalancingOptions_LeastInFlight_{
				LeastInFlight: &bigtablepb.LoadBalancingOptions_LeastInFlight{
					RandomSubsetSize: int64(lb.RandomSubsetSize),
				},
			},
		}
	case StrategyPeakEwma:
		return &bigtablepb.LoadBalancingOptions{
			LoadBalancingStrategy: &bigtablepb.LoadBalancingOptions_PeakEwma_{
				PeakEwma: &bigtablepb.LoadBalancingOptions_PeakEwma{
					RandomSubsetSize: int64(lb.RandomSubsetSize),
				},
			},
		}
	}
	return nil
}

// AddSessionLoadListener registers a callback that receives the server-driven
// SessionLoad value (0.0 = all-classic, 1.0 = all-session) on every
// configuration update from a successful poll. Returns an unregister thunk.
//
// Unlike AddSessionPoolListener, this skips the immediate registration-time
// fire that would otherwise deliver the default config (seq=0). Callers rely
// on the bootstrap value they passed to NewDiverter remaining in effect until
// the control plane actually responds — firing with seq=0's default
// SessionLoad=0 would silently clobber that bootstrap.
func (m *ClientConfigurationManager) AddSessionLoadListener(listener func(load float64)) func() {
	return m.addListener(func(cfg clientConfig, seq int64) {
		if seq == 0 {
			return
		}
		listener(cfg.Session.SessionLoad)
	})
}

// pollingLoop continuously polls the Bigtable control plane at the configured interval.
// It enforces a minimum interval (MinPollingInterval) to protect the control plane from DDoSes.
func (m *ClientConfigurationManager) pollingLoop(parentCtx context.Context) {
	ctx, cancel := context.WithCancel(parentCtx)
	defer cancel()

	go func() {
		select {
		case <-m.done:
			cancel()
		case <-ctx.Done():
		}
	}()

	for {
		m.mu.RLock()
		cfg := m.currentConfig
		m.mu.RUnlock()

		interval := cfg.Polling.PollingInterval
		if interval < MinPollingInterval {
			interval = MinPollingInterval
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
			// Poll with a 5-second timeout per RPC attempt. Track it with
			// pollsWG so Close() can wait for in-flight polls (and their
			// listener callbacks) to finish before returning.
			m.pollsWG.Add(1)
			pollCtx, pollCancel := context.WithTimeout(ctx, 5*time.Second)
			m.poll(pollCtx)
			pollCancel()
			m.pollsWG.Done()
		}
	}
}

// poll queries the GetClientConfiguration API and triggers registered listeners with the new configuration.
// If the poll fails and the previous configuration's validity has expired, it falls back to the default config.
func (m *ClientConfigurationManager) poll(ctx context.Context) {
	btopt.Debugf(m.logger, "bigtable: polling client configuration...")
	req := &bigtablepb.GetClientConfigurationRequest{
		InstanceName: m.instanceName,
		AppProfileId: m.appProfileId,
	}

	ctx = metadata.NewOutgoingContext(ctx, m.metadata)

	var resp *bigtablepb.ClientConfiguration
	var err error

	m.mu.RLock()
	maxRetries := m.currentConfig.Polling.MaxRpcRetryCount
	m.mu.RUnlock()

	// Retry with randomized exponential backoff using seconds
	for i := 0; i <= maxRetries; i++ {
		var header, trailer metadata.MD
		rpcStart := time.Now()
		resp, err = m.client.GetClientConfiguration(ctx, req, grpc.Header(&header), grpc.Trailer(&trailer))
		rpcDuration := time.Since(rpcStart)
		if err == nil {
			btopt.Debugf(m.logger, "bigtable: GetClientConfiguration RPC attempt %d completed successfully in %v", i, rpcDuration)
			break
		}
		btopt.Debugf(m.logger, "bigtable: GetClientConfiguration RPC attempt %d failed in %v: %v", i, rpcDuration, err)
		if i < maxRetries {
			delay := time.Duration(rand.Intn(1<<i)) * time.Second
			select {
			case <-ctx.Done():
				return
			case <-time.After(delay):
			}
		}
	}

	if err != nil {
		btopt.Debugf(m.logger, "bigtable: failed to poll client configuration: %v", err)
		m.mu.Lock()
		var listeners []configListener
		var cfgToNotify clientConfig
		var seq int64
		// Fall back to default configuration if validity window has expired
		if time.Now().After(m.validUntil) {
			btopt.Debugf(m.logger, "bigtable: client configuration validity window expired, falling back to default config")
			m.currentConfig = m.defaultConfig
			m.configSeq++
			seq = m.configSeq
			listeners = make([]configListener, 0, len(m.listeners))
			for _, l := range m.listeners {
				listeners = append(listeners, l)
			}
			cfgToNotify = m.currentConfig
		}
		m.mu.Unlock()

		// Suppress listener fires after Close() has begun teardown — the
		// SessionPools they target may already be Close'd.
		if listeners != nil && !m.isClosed() {
			for _, l := range listeners {
				l(cfgToNotify.Clone(), seq)
			}
		}
		return
	}

	parsedResp := parseConfig(resp, m.defaultConfig)
	btopt.Debugf(m.logger, "bigtable: successfully polled new client configuration. validityDuration: %v", parsedResp.Polling.ValidityDuration)

	m.mu.Lock()
	m.currentConfig = parsedResp
	m.configSeq++
	seq := m.configSeq
	m.validUntil = time.Now().Add(parsedResp.Polling.ValidityDuration)

	listeners := make([]configListener, 0, len(m.listeners))
	for _, l := range m.listeners {
		listeners = append(listeners, l)
	}
	cfgToNotify := m.currentConfig
	m.mu.Unlock()

	// Suppress listener fires after Close() has begun teardown — the
	// SessionPools they target may already be Close'd.
	if m.isClosed() {
		return
	}
	for _, l := range listeners {
		l(cfgToNotify.Clone(), seq)
	}
}

// parseConfig converts the protobuf ClientConfiguration message into the internal clientConfig structure,
// validating bounds such as MinPollingInterval and capping validity duration to prevent integer overflows.
func parseConfig(protoCfg *bigtablepb.ClientConfiguration, defaultCfg clientConfig) clientConfig {
	res := defaultCfg

	if protoCfg == nil {
		return res
	}

	if p := protoCfg.GetPollingConfiguration(); p != nil {
		res.Polling = parsePollingConfig(p, res.Polling)
	}

	if protoCfg.SessionConfiguration != nil {
		s := protoCfg.SessionConfiguration
		res.Session = parseSessionConfig(s, res.Session)
		res.HasSessionConfig = s.SessionLoad > 0
	} else {
		res.HasSessionConfig = false
	}

	return res
}

func parsePollingConfig(p *bigtablepb.ClientConfiguration_PollingConfiguration, defaultCfg pollingConfig) pollingConfig {
	res := defaultCfg
	if p == nil {
		return res
	}
	if p.PollingInterval != nil {
		res.PollingInterval = p.PollingInterval.AsDuration()
	}
	if res.PollingInterval < MinPollingInterval {
		res.PollingInterval = MinPollingInterval
	}
	if p.ValidityDuration != nil {
		res.ValidityDuration = p.ValidityDuration.AsDuration()
		if res.ValidityDuration > 100*365*24*time.Hour {
			res.ValidityDuration = 100 * 365 * 24 * time.Hour
		}
	}
	res.MaxRpcRetryCount = int(p.MaxRpcRetryCount)
	return res
}

func parseSessionConfig(s *bigtablepb.SessionClientConfiguration, defaultCfg sessionConfig) sessionConfig {
	res := defaultCfg
	if s == nil {
		return res
	}
	res.SessionLoad = float64(s.SessionLoad)
	if s.ChannelConfiguration != nil {
		res.ChannelPool = parseChannelPoolConfig(s.ChannelConfiguration, res.ChannelPool)
	}
	if s.SessionPoolConfiguration != nil {
		res.SessionPool = parseSessionPoolConfig(s.SessionPoolConfiguration, res.SessionPool)
	}
	return res
}

func parseChannelPoolConfig(cc *bigtablepb.SessionClientConfiguration_ChannelPoolConfiguration, defaultCfg channelPoolConfig) channelPoolConfig {
	res := defaultCfg
	if cc == nil {
		return res
	}
	res.MinServerCount = int(cc.MinServerCount)
	res.MaxServerCount = int(cc.MaxServerCount)
	res.PerServerSessionCount = int(cc.PerServerSessionCount)
	if fallback := cc.GetDirectAccessWithFallback(); fallback != nil {
		if fallback.CheckInterval != nil {
			res.DirectAccessCheckInterval = fallback.CheckInterval.AsDuration()
		}
		res.DirectAccessErrorThreshold = fallback.ErrorRateThreshold
	}
	return res
}

func parseSessionPoolConfig(sp *bigtablepb.SessionClientConfiguration_SessionPoolConfiguration, defaultCfg sessionPoolConfig) sessionPoolConfig {
	res := defaultCfg
	if sp == nil {
		return res
	}
	res.Headroom = sp.Headroom
	res.MinSessionCount = int(sp.MinSessionCount)
	res.MaxSessionCount = int(sp.MaxSessionCount)
	res.NewSessionCreationBudget = int(sp.NewSessionCreationBudget)
	if sp.NewSessionCreationPenalty != nil {
		res.NewSessionCreationPenalty = sp.NewSessionCreationPenalty.AsDuration()
	}
	res.ConsecutiveSessionFailureThreshold = int(sp.ConsecutiveSessionFailureThreshold)
	res.NewSessionQueueLength = int(sp.NewSessionQueueLength)

	if sp.LoadBalancingOptions != nil {
		lbo := sp.LoadBalancingOptions
		switch opt := lbo.LoadBalancingStrategy.(type) {
		case *bigtablepb.LoadBalancingOptions_Random_:
			res.LoadBalancing.Strategy = StrategyRandom
		case *bigtablepb.LoadBalancingOptions_LeastInFlight_:
			res.LoadBalancing.Strategy = StrategyLeastInFlight
			if opt.LeastInFlight != nil {
				res.LoadBalancing.RandomSubsetSize = int(opt.LeastInFlight.RandomSubsetSize)
			}
		case *bigtablepb.LoadBalancingOptions_PeakEwma_:
			res.LoadBalancing.Strategy = StrategyPeakEwma
			if opt.PeakEwma != nil {
				res.LoadBalancing.RandomSubsetSize = int(opt.PeakEwma.RandomSubsetSize)
			}
		}
	}
	return res
}
