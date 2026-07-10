# Session Spec — Test Coverage Map

Companion to `SESSION_SPEC.md` (behavior) and `SESSION_COMPONENT_SPEC.md` (boundaries). This file tracks which specs are enforced by tests, on which branch, and which spec items still need coverage. Update in the same PR that adds/moves a test.

**Branch legend.** `primitives` = `bigtable-session-primitives` (this branch, only the Session primitives). `sessionz` = `feat/bigtable-sessionz-debug` (full Session + pool + shim + z-pages).

**Status legend.** ✓ = enforced. ~ = partially covered / smoke only. ✗ = not yet tested. ▲ = not directly testable at runtime (linter/reviewer catch).

---

## Coverage on `bigtable-session-primitives` (current branch)

| Spec item | Rule | Test | Status |
|---|---|---|---|
| #1 | State machine is strictly forward, 6 states, monotonic ordinals | `session_state_test.go::TestState_OrdinalsPinned`, `::TestState_String` | ✓ |
| #9 (a) | `AttemptState` has exactly three values with pinned meanings | `attempt_outcome_test.go::TestAttemptState_String` | ✓ |
| #9 (b) | Zero value = `ServerResult` = do-not-retry (fail-closed) | `attempt_outcome_test.go::TestClassifyErr_Cases` (untagged branch) | ✓ |
| #9 (c) | `tagErr(state, nil)` returns nil (composability) | `attempt_outcome_test.go::TestTagErr_NilPassesThrough` | ✓ |
| #9 (d) | `tagErr` is transparent to `errors.Is` / `errors.Unwrap` | `attempt_outcome_test.go::TestTagErr_WrapsAndUnwraps` | ✓ |
| #9 (e) | `ClassifyErr` finds the tag through arbitrary `fmt.Errorf("%w")` wrapping | `attempt_outcome_test.go::TestClassifyErr_FindsThroughWrapper` | ✓ |

All other spec items (#2–#8, #10–#14) require code that is not on this branch. See the checklist below.

---

## Coverage on `feat/bigtable-sessionz-debug` (as of 2026-07-10, batch 1)

Newly added in commit series after the base survey (see `SESSION_COMPONENT_SPEC.md` Part A snapshot date). All added tests were verified against the spec by the `session-reviewer` self-check pattern before commit.

| Spec | Test added | File | Notes |
|---|---|---|---|
| #5 | `TestForceClose_SendsNoCloseSessionRequest` | `bigtable/internal/transport/session_test.go` | Asserts ForceClose sends no CloseSessionRequest frame. |
| #5 | `TestClose_IdempotentAcrossNCalls` | `bigtable/internal/transport/session_test.go` | 5 repeat Close calls: nil returns, no extra frames, ≤1 OnClose. |
| #11 | `TestSessionTable_MatView_MutateRowReturnsErrWriteNotSupported` | `bigtable/internal/session/table_test.go` | MatView write → `ErrWriteNotSupported`; read still works. |
| #11 | `TestSessionTable_ReadAndWritePoolsDoNotShareSessions` | `bigtable/internal/session/table_test.go` | Read invoker gets ReadRow only; write invoker gets MutateRow only. |
| #11 | `TestLazyPool_NilPoolAndNilOpenReturnNilNil` | `bigtable/internal/session/lazy_pool_test.go` | Nil-receiver + nil-open contract. |
| #11 | `TestLazyPool_FailedOpenNotCached` | `bigtable/internal/session/lazy_pool_test.go` | 4 failures + 1 success ⇒ 5 open() calls (not 1). |
| #13 | `TestManagerPoll_MinPollingIntervalClamp` | `bigtable/internal/transport/client_configuration_manager_test.go` | Server 30s → clamped to `MinPollingInterval` (1min). |
| #14 | `TestDiverter_UseSession_LoadZero_Counters` | `bigtable/internal/transport/diverter_test.go` | Extends fast-path 0 with `SessionPicks`/`ClassicPicks` assertion. |
| #14 | `TestDiverter_UseSession_LoadOne_Counters` | `bigtable/internal/transport/diverter_test.go` | Extends fast-path 1 with counter assertion. |
| #14 | `TestTableShim_SampleRowKeys_AlwaysClassic` | `bigtable/table_shim_test.go` | Non-vRPC method breadth. |
| #14 | `TestTableShim_ApplyBulk_AlwaysClassic` | `bigtable/table_shim_test.go` | Non-vRPC method breadth. |
| #14 | `TestTableShim_ApplyReadModifyWrite_AlwaysClassic` | `bigtable/table_shim_test.go` | Non-vRPC method breadth. |
| #14 | `TestTableShim_NilSession_AllMethodsFallBackToClassic` | `bigtable/table_shim_test.go` | Nil-safety across all 6 TableAPI methods. |
| #14 | `TestTableShim_SessionErrorNotRetriedOnClassic` | `bigtable/table_shim_test.go` | Cross-cite to #9: shim never retries session error on classic. |

**Still MISSING on this branch** (from the base survey — none yet started; require heavy fixtures):
- #2: `TestSession_InvokeConcurrent_RejectsSecond`, `TestSession_ResponseRpcIdMismatch_Dropped`
- #4: `TestSession_HooksFireExactlyOnce_UnderConcurrentClose`, `TestSession_HookOrdering`
- #5: `TestSession_CloseReasonCASOnce`
- #7: `TestSession_Heartbeat_ResetByAnyFrame` (partial gaps around request-Send / response-Recv reset)
- #8: `TestSession_MissedHeartbeat_SequenceIsExact` (partial — needs counter + ring + hook assertions)
- #10: `TestSession_ConcurrentInvokeAndClose_NoRace`, `TestPool_LockOrder_NoReentrantDeadlock`
- #11: `TestSessionTable_ReadAndWritePools_DoNotShareSessions` was added; distinctness partial gap resolved
- #12: `TestSessionClient_SharedChannelPool`, `TestSessionClient_Close_UnwindsAllPools`
- **All #15 and #16 tests** — see the newly-added sections below.

The added tests raise coverage from ~8 COVERED (survey baseline) to **~22 COVERED / PARTIAL** on this branch. The remaining ~15 pending items are the hard-fixture cluster: fake clocks, concurrent races, ast/lock-order source checks, source-provenance separation. Batch them into a follow-up when someone can dedicate time to the fixtures.

---

## Pending tests for `feat/bigtable-sessionz-debug`

Add these when Session + pool + shim code lands. Each row lists the minimum assertion that would enforce the spec item.

### Session-level (#2–#8, #10)

| Spec | Test to add | Notes |
|---|---|---|
| #2 | `TestSession_InvokeConcurrent_RejectsSecond` — call `Invoke` twice concurrently, assert second returns a `TagErr(StateUncommitted, ...)` — `CompareAndSwap` MUST refuse. | Use a slow fake stream so the first Invoke is still in flight when the second lands. |
| #2 | `TestSession_ResponseRpcIdMismatch_Dropped` — send an OpenSessionResponse, invoke, then have the fake stream reply with a response whose `rpc_id` doesn't match — assert the response is dropped (Invoke times out on ctx). | Verifies the correlation guard. |
| #3 | `TestSession_PeerInfoPopulatedBeforeOnActive` — provide a header via the fake stream; assert `Session.AfeID()` is non-zero from inside the `OnActive` hook. | Synchronous parse invariant. |
| #4 | `TestSession_HooksFireExactlyOnce_UnderConcurrentClose` — call `Close`, `ForceClose`, and `handleGoAway` concurrently N times; assert `OnStart`/`OnActive`/`OnClosing`/`OnClose` each fired exactly once. | Enforces `sync.Once` guards. |
| #4 | `TestSession_HookOrdering` — record hook fire order in a slice; assert `[OnStart, OnActive, OnClosing, OnClose]`. | Ordering invariant. |
| #5 | `TestSession_CloseIdempotent` — call `Close` N times, no panic; second-and-later calls return nil. | Idempotency. |
| #5 | `TestSession_ForceClose_SendsNoCloseSessionRequest` — spy on the fake stream's Send calls after `ForceClose`; assert no `CloseSessionRequest` was sent. | ForceClose semantics. |
| #5 | `TestSession_CloseReasonCASOnce` — trigger GoAway, then have Recv return an error; assert `closeReason == "GoAway"`, not `"StreamEnd:..."`. | CAS-once ordering. |
| #6 | `TestSession_GoAway_DoesNotCancelInFlight` — invoke a slow op, trigger GoAway, arrange the fake stream to deliver the response after GoAway; assert Invoke returns the response, not a cancel error. | Non-idempotent-Apply safety. |
| #7 | `TestSession_Heartbeat_UnarmedWhenIdle` — start a session, do not invoke anything, advance a fake clock by `3 × heartbeatInterval`; assert session is still Ready and no ForceClose fired. Requires clock injection. | Idle sessions must not be torn down. |
| #7 | `TestSession_Heartbeat_ResetByAnyFrame` — start a slow invoke, fake stream sends a heartbeat frame just under the deadline; assert no miss. Repeat with a request Send, response Recv, unknown-frame (assert unknown does NOT reset). | Frame-type table. |
| #8 | `TestSession_MissedHeartbeat_SequenceIsExact` — arrange the miss; assert the exact sequence: `tagSessionHeartbeatMissed` counter incremented, `hb-missed` event in ring buffer, `ForceClose` with `CLOSE_SESSION_REASON_MISSED_HEARTBEAT`, in-flight vRPC returns `ErrUnavailableHeartBeatMissed` tagged `StateTransportFailure`, `OnClosing`/`OnClose` fire once each. | The single most load-bearing test in the suite. |
| #10 | `TestSession_ConcurrentInvokeAndClose_NoRace` — with `-race`, run 1000× `parallel(Invoke, Close, ForceClose, handleGoAway)`. | Concurrency discipline. |
| #10 | `TestPool_LockOrder_NoReentrantDeadlock` — verify any method reading `p.picker.Name()` inside `CheckoutSession` receives it as a parameter (source-level assertion via `go/ast`) or via atomic snapshot. | Static rather than runtime; deadlock would show as test hang. |

### Pool-topology + routing (#11–#14)

| Spec | Test to add | Notes |
|---|---|---|
| #11 | `TestSessionClient_OpenMaterializedView_HasNoWritePool` — `sc.OpenMaterializedView("v").MutateRow(...)` MUST return `ErrWriteNotSupported`. | Contract check. |
| #11 | `TestLazyPool_FailedOpenNotCached` — first `openRead()` returns err; second call MUST call `openRead` again (not cached). | Fault tolerance. |
| #11 | `TestSessionTable_ReadAndWritePools_DoNotShareSessions` — mock the Invoker returned by each lazyPool; assert `ReadRow` invokes the READ Invoker and `Apply` invokes the WRITE Invoker; the two invokers are distinct instances. | Direction isolation. |
| #12 | `TestSessionClient_SharedChannelPool` — spy on the channel pool factory; construct 3 tables + 1 AV + 1 MatView; assert the factory was called exactly once. | Single-channel-pool guarantee. |
| #12 | `TestSessionClient_Close_UnwindsAllPools` — construct N tables; call `Close`; assert every `SessionPoolImpl.Close()` was called; assert channel pool closed after the pools. | Ordered teardown. |
| #13 | `TestClientConfigurationManager_MinPollingIntervalClamp` — server response says `PollingInterval: 30s`; assert manager uses `MinPollingInterval` (1 min) instead. | Rate-limit floor. |
| #13 | `TestClientConfigurationManager_UpdatePropagatesToPool` — register a fake pool as listener; poll returns new `MaxSessionCount`; assert `UpdateConfig` was called with the new value AND with a `configSeq` higher than the previous. | Monotonic + propagation. |
| #13 | `TestClientConfigurationManager_CloseHappensBeforeListenerSilence` — Close the manager; assert no listener callback fires after `Close()` returns (start a poll, race Close, use `sync.Cond` to detect). | Teardown safety. |
| #13 | `TestClientConfigurationManager_PollFailureFallsBackAfterValidity` — force poll errors; before `ValidityDuration` assert last-good config is used; after expiry assert bootstrap defaults resume. | Poll-failure semantics. |
| #14 | `TestDiverter_UseSession_LoadZero` — `NewDiverter(0)`; 1000 calls to `UseSession()` all return false; only `classicPicks` increments. | Fast-path 0. |
| #14 | `TestDiverter_UseSession_LoadOne` — `NewDiverter(1)`; 1000 calls all return true; only `sessionPicks` increments. | Fast-path 1. |
| #14 | `TestDiverter_UseSession_ApproximateRatio` — `NewDiverter(0.5)`; 10000 calls; assert `sessionPicks/total` in `[0.45, 0.55]`. | Stochastic behavior. |
| #14 | `TestTableShim_NilSession_FallsBackToClassic` — construct shim with `session=nil`; call every `TableAPI` method; assert only classic was invoked. | Nil-safety. |
| #14 | `TestTableShim_Apply_Conditional_AlwaysClassic` — mock both paths; call `Apply` with a conditional mutation; assert only classic; repeat with `Diverter` load=1; still only classic. | Method-shape gate. |
| #14 | `TestTableShim_ReadRows_AlwaysClassic` (similar for `SampleRowKeys`, `ApplyBulk`, `ApplyReadModifyWrite`). | Non-vRPC methods. |
| #14 | `TestTableShim_Errors_NotRetriedOnClassic` — session path returns a `StateTransportFailure` on a non-idempotent Apply; assert shim returns the error, does NOT retry on classic. | Retry-oracle safety. |

### Debug non-blocking (#15)

| Spec | Test to add | Notes |
|---|---|---|
| #15 | `TestSessionPoolImpl_Snapshot_DoesNotBlockCheckoutSession` — spin a goroutine hammering `CheckoutSession`/`ReleaseSession` at high concurrency, snapshot the pool 100x from another goroutine, assert p95 checkout latency does not degrade materially. | Runtime hot-path guard. |
| #15 | `TestSessionDebugProvider_Snapshot_ReturnsValueNotView` — mutate pool state after taking a snapshot, assert the snapshot is unchanged (byte-for-byte or deep-equal). | Immutability guard. |
| #15 | `TestSessionPoolImpl_Snapshot_UsesRLockNotLock` — with `-race`, run parallel snapshots concurrently; MUST NOT serialize. Consider a `go/ast` check that snapshot methods use `RLock` not `Lock`. | Reader-parallelism guard. |
| #15 | `TestSessionEventRing_BoundedIteration` — push >ring-cap events to a session's debug ring, snapshot, assert length == cap and oldest entry evicted. | Bounded-work guard. |
| #15 | `TestZPage_HungPoolDoesNotHangHandler` — construct a pool that never releases `p.mu`; snapshot MUST return within a bounded deadline (via TryLock or atomic fallback), not block forever. | Liveness-weapon guard. |

### Metrics fields per attempt (#16)

| Spec | Test to add | Notes |
|---|---|---|
| #16 | `TestClassicPath_StampsClusterAndZoneFromResponseParams` — inject a `x-goog-cbt-cookie-*` header with a marshaled `ResponseParams{ClusterId, ZoneId}`; assert emitted `attempt_latencies` carries those labels. | Classic source-of-truth guard. |
| #16 | `TestSessionPath_StampsClusterAndZoneFromInvokeResult` — fake Invoker returns `InvokeResult{ClusterInfo: &spb.ClusterInformation{ClusterId: "c", ZoneId: "z"}}`; assert emitted `attempt_latencies` carries those labels. Confirms `sessionTable.stampAttempt` wires them. | Session source-of-truth guard. |
| #16 | `TestSessionPath_MissingClusterInfo_DoesNotPanicOrLeakStale` — `InvokeResult.ClusterInfo == nil`; assert emit still fires (possibly with empty labels), no panic, no stale labels from a previous attempt. | Nil-guard. |
| #16 | `TestSessionPath_TransportPeerFixedAcrossAttempts` — same session, N retries; assert `transport_zone` / `transport_subzone` labels are byte-identical across all N attempts (session invariant vs classic). | Session-invariant guard. |
| #16 | `TestClassicPath_TransportPeerCanVaryAcrossAttempts` — force two attempts on different subchannels; assert `transport_*` labels MAY differ (regression guard against accidental sticky-peer logic). | Classic-variability guard. |
| #16 | `TestMetricsTracer_NoCrossPathLeakage` — set up both paths; verify classic-side attempts NEVER call into `InvokeResult.ClusterInfo` and session-side attempts NEVER unmarshal `ResponseParams`. Structural (ast) check. | Source-distinction guard per spec #16 last bullet. |

### Component/boundary rules (SESSION_COMPONENT_SPEC.md Part B)

These are grep-able assertions, best expressed as `go vet` analyzers or shell-scripted tests. Add as a single `TestBoundaries` that shells out.

| Rule | Assertion | Test approach |
|---|---|---|
| B1 | `internal/session/**` MUST NOT reference `bigtable.Row`/`Mutation`/`Filter`/etc. | `git grep` in a `TestBoundary_SessionPackage_ProtoOnly` that fails on any hit. |
| B2 | `internal/transport/**` MUST NOT import `internal/session`. | `git grep` on the import path. |
| B3 | `debugview/**` MUST NOT import concrete pool/session types (only interfaces + DTOs). | `go/ast` walk over imports; whitelist DTO+interface names. |
| B6 | Only `ClientConfigurationManager` calls `Diverter.SetSessionLoad` or `SessionPoolImpl.UpdateConfig` outside tests. | `git grep` with excludes. |
| B8 | Lock order pool.mu → sl.mu (no methods holding sl.mu that then take pool.mu). | Manual review + reviewer agent; hard to test at runtime without a lock-order-inversion detector. |
| Part C | No duplicate ownership. | Enforced by structural review, not test. |

Items marked ▲ in the top table (like #1's monotonic-forward guarantee at the transition level) are best enforced by the `session-reviewer` agent — no code-only test can verify "this state machine never has a backward edge" without simulating every transition.

---

## Adding a new invariant

1. Add it to `SESSION_SPEC.md` (behavioral) or `SESSION_COMPONENT_SPEC.md` (boundary).
2. Add its test row to this file — either coverage row (if the test exists) or pending row (if not).
3. If pending, file the test as part of the same PR that adds the invariant (or open a follow-up issue and link it here).
4. If the invariant is not testable at runtime, mark ▲ and note which reviewer agent catches it.
