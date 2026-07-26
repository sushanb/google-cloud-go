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

// debug_tracer.go — cheap counter for "this branch shouldn't reach"
// sites in the session pool, session, and configuration manager. Every
// emission is one atomic add plus one OTel Int64Counter increment; safe
// to sprinkle freely on cold paths.
//
// # How to use
//
// Add a tag string constant to the catalog block below, then call one
// of the four entry points at the site.
//
// Observation — a branch that shouldn't happen but recovers cleanly.
// Adds the tag counter alongside whatever the branch already does (log,
// return, drop). No behavior change:
//
//	default:
//	    recordDebugTag(tagSessionUnknownResponse)
//	    s.debugf("received SessionResponse with unknown payload type %T", p)
//	    return
//
// Observation at a non-default level — rare; use only when the site
// really means "this is worse than a Warn but not an assert failure":
//
//	recordDebugTagAt(lvl.Error, tagSessionVRPCIDMismatch)
//
// Precondition — an invariant the caller relies on. Both forms return
// the predicate result: use as an `if !` guard so the site records +
// logs + bails in one line. Neither panics — the counter (and any err
// the caller wants to return) is the observable signal.
//
// Format-free when the tag name is enough:
//
//	if !assertDebugTag(rpc != nil, tagSessionVRPCNil) {
//	    return
//	}
//
// With formatted diagnostic context when the site has state values / ids
// worth capturing:
//
//	if !assertDebugTagf(state == StateReady || state == StateClosing,
//	    tagSessionVRPCResponseWrongState,
//	    "vRPC response for rpc_id=%d arrived in state %s", resp.RpcId, state) {
//	    return
//	}
//
// # Style rules
//
//   - Tag names are literal constants declared in the catalog below.
//     Never fmt.Sprintf into a name — dynamic context belongs in the log
//     message alongside, not the metric attribute.
//   - Emission is additive to whatever the branch already does — do
//     not delete the existing log / err / return.
//   - Default to recordDebugTag; reach for recordDebugTagAt only when
//     a non-Warn level is genuinely warranted.
//   - lvl.Error is reserved for assertDebugTag / assertDebugTagf
//     failures + the handful of explicit "this is really wrong"
//     observations. Everything else is lvl.Warn (recordDebugTag).
//   - Adding a site = one new const in the catalog block + one call.
package internal

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	btopt "cloud.google.com/go/bigtable/internal/option"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// debugLevel gates whether a debug tag emission is admitted. Only two
// levels today — no site needs finer granularity.
type debugLevel int32

// lvl exposes the debug levels as a small namespace so call sites read
// as `lvl.Warn` / `lvl.Error`. Naming `lvl` (not `tag`) intentionally —
// the tag catalog constants below use the `tag` prefix, so a namespace
// literally named `tag` would collide visually at call sites like
// `recordDebugTagAt(tag.Error, tagSessionXxx)`. Warn is the default for
// observations; Error is used by assertDebugTag failures and rare
// explicit invariant violations.
var lvl = struct {
	Warn  debugLevel
	Error debugLevel
}{
	Warn:  1,
	Error: 2,
}

// debugTagCounterName is the OTel instrument name. It is deliberately
// SHORT — the Cloud Monitoring exporter prepends the
// `bigtable.googleapis.com/internal/client/` namespace itself, so
// using the fully-qualified name here would double-prefix (and
// normalize dots to slashes in the second half), producing e.g.
// `bigtable.googleapis.com/internal/client/bigtable/googleapis/com/internal/client/debug_tags`
// which Cloud Monitoring rejects as an unknown type.
const debugTagCounterName = "debug_tags"

// debugTagAttrKey is the single OTel attribute under which the tag
// string travels. Bounded cardinality — each tag is a literal constant
// declared below.
const debugTagAttrKey = "tag"

// Debug-tag catalog. Every recordDebugTag / assertDebugTag call site in
// the transport package passes one of these constants — never an inline
// literal. Adding a tag = adding a const here. Names use snake_case on
// the wire and camelCase in code. Grep for a tag on either the const
// name or the string literal and you find the definition + the sole
// emission site (usually one, at most a few).
const (
	// Session-lifecycle observations.
	tagSessionUnknownResponse       = "session_unknown_response"
	tagSessionOpenWrongState        = "session_open_wrong_state"
	tagSessionGoawayAfterClose      = "session_goaway_after_close"
	tagSessionGoawayBeforeStart     = "session_goaway_before_start"
	tagSessionAbnormalClose         = "session_abnormal_close"
	tagSessionHeartbeatMissed       = "session_heartbeat_missed"
	tagSessionForceCloseNeverStarted = "session_force_close_never_started"
	tagSessionCloseNoReason         = "session_close_no_reason"
	// tagSessionReadLoopPanic fires when readLoop's deferred recover
	// catches a panic from handleSessionResponse (or any downstream
	// handler). Session is force-closed with REASON_ERROR carrying the
	// panic value in the description.
	tagSessionReadLoopPanic = "session_read_loop_panic"

	// vRPC dispatch observations.
	//
	// tagSessionVRPCNil / tagSessionVRPCErrorNil / tagSessionVRPCIDMismatch
	// are all unreachable in production under the slotMu slot lifecycle —
	// the slot is only cleared by response handlers or session teardown,
	// both of which serialize via drainSlot. They survive as canary
	// counters for wire corruption / bookkeeping bugs and as pins for
	// tests that inject responses out-of-band.
	tagSessionVRPCNil                = "session_vrpc_nil"
	tagSessionVRPCErrorNil           = "session_vrpc_error_nil"
	tagSessionVRPCIDMismatch         = "session_vrpc_id_mismatch"
	tagSessionVRPCResponseWrongState = "session_vrpc_response_wrong_state"
	// tagSessionVRPCCancelledDrained fires when a server response
	// arrives for a slot the caller already abandoned via ctx.Done
	// (markCancelled set currentCancel before drainSlot ran).
	// Bookkeeping-only signal — no user impact — but a rising rate
	// under steady load usually means tail-latency spikes are pushing
	// more callers onto the ctx.Done branch.
	tagSessionVRPCCancelledDrained = "session_vrpc_cancelled_drained"

	// tagSessionInvokeStateChangedAfterClaim fires when Session.Invoke
	// observes state != Ready AFTER a successful claimSlot — meaning
	// a GOAWAY (or Close / ForceClose / heartbeat-miss) transitioned
	// the session during the encode window between the initial state
	// check and claimSlot. Observation-only signal ahead of the
	// short-circuit fix: a nonzero rate correlating with the server's
	// GOAWAY cadence (e.g. 5-min per session in benchmarks) confirms
	// the "DeadlineExceeded every N minutes" symptom is driven by
	// frames being sent on a draining session.
	tagSessionInvokeStateChangedAfterClaim = "session_invoke_state_changed_after_claim"

	// TagSessionAttemptNilClusterInfo fires when a session-path attempt
	// completes with no ClusterInformation on the InvokeResult — either
	// the attempt failed with a transport error (no server response) or
	// the server response omitted ClusterInformation. Downstream, the
	// attempt's cluster_id label defaults to <unspecified> because
	// stampAttempt has nothing to stamp AND session path has no per-vRPC
	// gRPC headers for ExtractLocation to fall back on. Observation-only
	// signal to validate whether this pathway drives the reported
	// mismatch between attempt_latencies2 (filtered on real cluster) and
	// connectivity_error_count (labeled <unspecified>).
	TagSessionAttemptNilClusterInfo DebugTag = "session_attempt_nil_cluster_info"

	// Experimental checkpoint tags — decompose the 402-per-minute
	// nil-ClusterInfo mystery reported on final_configuration. Three
	// checkpoints along the code path a caller's InvokeResult traverses:
	//
	//   A. processResult's `ci := res.ClusterInfo()` — did the delivery
	//      onto rpc.resultChan carry a non-nil ClusterInfo? Split by
	//      cause so we can tell "cancelActiveRPCs delivered res.err"
	//      from "server error frame with nil ClusterInfo" from "server
	//      success frame with nil ClusterInfo" (the last is a server
	//      contract violation).
	//
	//   B. processResult after `result.ClusterInfo = ci` — sanity check
	//      that the assignment stuck. Should equal the sum of the three
	//      A-tags. A divergence means a data race on InvokeResult.
	//
	//   C. SessionPoolImpl.Invoke right after `result, invokeErr =
	//      sh.session.Invoke(...)`. Catches every path that produces a
	//      caller-visible InvokeResult — INCLUDING Session.Invoke early
	//      returns (encode fail, claimSlot lost, Send fail, state != Ready)
	//      that never reach processResult. C > (A_res_err + A_res_errresp
	//      + A_server_omitted) tells us how many nils come from those
	//      pre-processResult paths.
	tagVRpcPRClusterInfoNilResErr           = "session_pr_ci_nil_res_err"
	tagVRpcPRClusterInfoNilResErrResp       = "session_pr_ci_nil_res_errresp"
	tagVRpcPRClusterInfoNilServerOmitted    = "session_pr_ci_nil_server_omitted"
	tagVRpcPRResultClusterInfoNilAfterAssign = "session_pr_result_ci_nil_after_assign"
	tagVRpcPoolPostInvokeResultClusterInfoNil = "session_pool_postinvoke_result_ci_nil"

	// tagVRpcPoolCheckoutFailedCINil fires on SessionPoolImpl.Invoke's
	// early return at session_pool.go:444 — CheckoutSession returned an
	// error, we return InvokeResult{} with nil ClusterInfo, and never
	// reach checkpoint C. Closes the accounting: expected to explain
	// (session_attempt_nil_cluster_info) − (postinvoke_result_ci_nil).
	// The three CheckoutSession failure exits (pool closed at :229,
	// ctx.Done while parked at :275, drainWaitersWithErr poisoned wake
	// at :282) all funnel through this single site.
	tagVRpcPoolCheckoutFailedCINil = "session_pool_checkout_failed_ci_nil"

	// TagSessionAttemptEmptyClusterID fires when ClusterInformation is
	// present on the InvokeResult but ClusterId is empty — a server
	// contract violation (server should always populate ClusterId on
	// vRPC responses per CLIENT_SIDE_METRICS_SPEC #1). Companion to
	// TagSessionAttemptNilClusterInfo; distinct so ops can tell
	// "server didn't respond" from "server responded without cluster".
	TagSessionAttemptEmptyClusterID DebugTag = "session_attempt_empty_cluster_id"

	// Pool-scoped anomalies.
	tagSessionPoolStuckSessionSwept          = "session_pool_stuck_session_swept"
	tagSessionPoolDrainTimeout               = "session_pool_drain_timeout"
	tagSessionPoolCreateFailed               = "session_pool_create_failed"
	tagSessionPoolPickLostRace               = "session_pool_pick_lost_race"
	tagSessionPoolConsecutiveFailuresTripped = "session_pool_consecutive_failures_tripped"

	// sessionList bookkeeping violations.
	//
	// tagSessionListRefcountUnderflow fires when OnSessionClosed would
	// decrement an afeHandle's refCount below zero. Under I4/I6 this is
	// unreachable — every decrement is preceded by an OnSessionStarted
	// increment and the handleToAfe map delete guards against a
	// double-close reaching the decrement. A non-zero count here means
	// bookkeeping drifted (missed OnSessionStarted, mis-paired hook
	// ordering, or a force-close bypass) and should be investigated.
	tagSessionListRefcountUnderflow = "session_list_refcount_underflow"

	// Client configuration polling.
	tagClientConfigPollFailed     = "client_config_poll_failed"
	tagClientConfigPollCtxExpired = "client_config_poll_ctx_expired"
)

var (
	// debugTagCounter is the OTel Int64Counter registered inside
	// InitializeSessionMetrics. Nil until initialization runs (or if
	// initialization was called with a nil meter provider) — every
	// emission path nil-checks it, so the tracer is safe to call before
	// InitializeSessionMetrics or in tests that don't wire OTel at all.
	debugTagCounter metric.Int64Counter

	// debugTagLevelFloor is the runtime-configurable floor. Emissions
	// with level < floor are dropped without incrementing either the
	// OTel counter or the in-memory map. Defaults to Warn so no site is
	// silent by default. Set via setDebugTagLevelFloor.
	debugTagLevelFloor atomic.Int32

	// debugTagCountsMu guards debugTagStats. Contention is negligible —
	// emissions on cold paths only, and readers (tests / debugview page)
	// are rare.
	debugTagCountsMu sync.RWMutex
	// debugTagStats is the in-process view of every tag seen since
	// process start: count + first-seen + last-seen. Kept alongside the
	// OTel counter so tests and the /debugtagsz/ page can read state
	// without an OTel exporter wired up. `firstSeen` is stamped once at
	// first emission; `lastSeen` updates on every emission.
	debugTagStats = map[string]*tagStat{}
)

// tagStat holds the per-tag counters + timestamps behind debugTagStats.
// All fields are atomics so the emission hot path stays lock-free once
// the tag's entry exists in the map (map lookup under RLock, then atomic
// bumps).
type tagStat struct {
	count     atomic.Int64 // total emissions since process start
	firstSeen atomic.Int64 // unix-nano of first emission; write-once
	lastSeen  atomic.Int64 // unix-nano of most-recent emission
}

// DebugTagSnapshot is one row of the DebugTags output — a single tag's
// count plus the timestamps of its first and most-recent emissions.
// Exported for consumption by the debugview /debugtagsz/ page.
type DebugTagSnapshot struct {
	Name      string
	Count     int64
	FirstSeen time.Time
	LastSeen  time.Time
}

func init() {
	debugTagLevelFloor.Store(int32(lvl.Warn))
}

// registerDebugTagCounter is invoked exactly once from
// InitializeSessionMetrics after the meter provider is validated
// non-nil. Split out so the session_tracer init path stays readable.
func registerDebugTagCounter(meter metric.Meter) error {
	c, err := meter.Int64Counter(
		debugTagCounterName,
		metric.WithDescription("Count of unexpected events tagged by call site."),
	)
	if err != nil {
		return fmt.Errorf("create debug_tags counter: %w", err)
	}
	debugTagCounter = c
	return nil
}

// setDebugTagLevelFloor overrides the emission floor. Any tag whose
// level is below the floor is dropped. Intended for future wiring from
// TelemetryConfiguration.debug_tag_level; safe to call from anywhere.
func setDebugTagLevelFloor(l debugLevel) {
	debugTagLevelFloor.Store(int32(l))
}

// recordDebugTag increments the debug_tags counter for `name` at the
// default level (lvl.Warn). Use recordDebugTagAt when the emission
// genuinely warrants a different level. `name` MUST be a stable literal
// from the tag catalog above — never format values into it (dynamic
// context belongs in the log line alongside, not the metric attribute).
//
// Safe to call before InitializeSessionMetrics: the OTel counter path
// is nil-checked, so only the in-memory map increments in that window.
func recordDebugTag(name string) {
	recordDebugTagAt(lvl.Warn, name)
}

// DebugTag is the typed form for tag names exposed across package
// boundaries. Callers must pass a catalog constant (e.g., a
// TagSessionAttemptNilClusterInfo below) rather than a raw string —
// the type prevents arbitrary literals from drifting off the catalog.
type DebugTag string

// RecordDebugTag is the exported form for other packages under
// bigtable/internal that need to fire tags from their own layer
// (e.g., internal/session's stampAttempt observing missing
// ClusterInformation on session-path attempts). Same semantics as
// recordDebugTag; the DebugTag typing forces callers to use a catalog
// constant rather than an ad-hoc string.
func RecordDebugTag(t DebugTag) {
	recordDebugTag(string(t))
}

// recordDebugTagAt is the level-explicit form of recordDebugTag. Prefer
// recordDebugTag for observations (which are always Warn); reach for
// recordDebugTagAt only when a site needs to name a non-Warn level
// (e.g. an inline invariant violation outside an assertDebugTag).
func recordDebugTagAt(level debugLevel, name string) {
	if int32(level) < debugTagLevelFloor.Load() {
		return
	}
	bumpDebugTagCountLocked(name)
	if debugTagCounter != nil {
		debugTagCounter.Add(context.Background(), 1,
			metric.WithAttributes(attribute.String(debugTagAttrKey, name)))
	}
}

// assertDebugTag returns whether `expr` holds. When it doesn't, it
// records an lvl.Error tag and logs "debug-tag assertion failed [name]".
// Use assertDebugTagf when the site has diagnostic context worth
// attaching to the log line. Does NOT panic — the counter + log is the
// observable signal; the caller decides whether to bail
// (`if !assertDebugTag(...) { return }`), drop the message, or continue.
func assertDebugTag(expr bool, name string) bool {
	if expr {
		return true
	}
	recordDebugTagAt(lvl.Error, name)
	btopt.Debugf(nil, "bigtable: debug-tag assertion failed [%s]", name)
	return false
}

// assertDebugTagf is the format-string form of assertDebugTag. Use it
// when the site has diagnostic context (state values, ids, timing)
// worth attaching to the log line. Counter increment is identical to
// assertDebugTag — the format only affects the log message.
func assertDebugTagf(expr bool, name, format string, args ...interface{}) bool {
	if expr {
		return true
	}
	recordDebugTagAt(lvl.Error, name)
	btopt.Debugf(nil, "bigtable: debug-tag assertion failed [%s]: "+format, append([]interface{}{name}, args...)...)
	return false
}

// bumpDebugTagCountLocked increments the in-memory count for `name` and
// stamps the emission timestamps. First-emission case creates the entry
// under the write lock; subsequent emissions hit the RLock fast path
// and only touch atomics on the stat.
func bumpDebugTagCountLocked(name string) {
	now := time.Now().UnixNano()
	debugTagCountsMu.RLock()
	s, ok := debugTagStats[name]
	debugTagCountsMu.RUnlock()
	if ok {
		s.count.Add(1)
		s.lastSeen.Store(now)
		return
	}
	debugTagCountsMu.Lock()
	if s, ok = debugTagStats[name]; !ok {
		s = &tagStat{}
		s.firstSeen.Store(now)
		debugTagStats[name] = s
	}
	s.count.Add(1)
	s.lastSeen.Store(now)
	debugTagCountsMu.Unlock()
}

// DebugTags returns a snapshot of every tag emitted since process
// start, sorted by LastSeen descending (most-recently-fired first).
// The number of distinct tags is bounded by the catalog above (~17
// entries today), so callers can render or serialize the whole slice
// without paging.
func DebugTags() []DebugTagSnapshot {
	debugTagCountsMu.RLock()
	defer debugTagCountsMu.RUnlock()
	out := make([]DebugTagSnapshot, 0, len(debugTagStats))
	for name, s := range debugTagStats {
		out = append(out, DebugTagSnapshot{
			Name:      name,
			Count:     s.count.Load(),
			FirstSeen: time.Unix(0, s.firstSeen.Load()),
			LastSeen:  time.Unix(0, s.lastSeen.Load()),
		})
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].LastSeen.After(out[j].LastSeen)
	})
	return out
}

// snapshotDebugTagCounts returns a bare name→count map. Kept as a
// convenience for tests that only care about counts; new callers should
// use DebugTags for the richer view.
func snapshotDebugTagCounts() map[string]int64 {
	debugTagCountsMu.RLock()
	defer debugTagCountsMu.RUnlock()
	out := make(map[string]int64, len(debugTagStats))
	for name, s := range debugTagStats {
		out[name] = s.count.Load()
	}
	return out
}

// resetDebugTagCountsForTest wipes the in-memory map so a test can
// assert on a specific tag's count without cross-test contamination.
// Exported (with the _ForTest suffix) so tests in other packages under
// the transport tree can reuse it. Never call outside tests.
func resetDebugTagCountsForTest() {
	debugTagCountsMu.Lock()
	debugTagStats = map[string]*tagStat{}
	debugTagCountsMu.Unlock()
}
