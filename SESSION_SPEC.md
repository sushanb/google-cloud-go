# Session — Behavioral Spec

**Scope.** This file governs the **runtime behavior** of code under `bigtable/internal/transport/session*.go`, `bigtable/internal/session/**`, `bigtable/table_shim.go`, and any `bigtable/session_*.go`. Any change to those files MUST be checked against the 14 invariants below. Non-Session bigtable code (classic data path, admin, firestore/spanner/storage/etc.) is out of scope.

**Companion doc.** `SESSION_COMPONENT_SPEC.md` covers the **component topology and layering rules** (which package owns what, what MUST NOT import what, where types live). Read that file too before any structural refactor — it's what prevents "muddling the logic of one thing into another thing." This file (`SESSION_SPEC.md`) is about how each layer must *behave*; the component doc is about *where each layer lives*. Both apply.

**How to use.** Read top-to-bottom before editing session code. When a change would violate an invariant, either (a) it is a bug — fix it, or (b) the spec is stale — update the spec in the same PR with justification, and confirm with mutianf on human-reviewer PRs.

**Java parity.** Where the two clients differ, both sides are cited so drift is visible. Deviations from Java parity require an explicit note in the invariant.

**Layer split.** #1–#10 are Session-level invariants (one Session's behavior). #11–#14 are pool-topology and routing invariants (how the client composes many Sessions). Do not mix them.

---

## Session-level invariants (#1–#10)

### 1. State machine is strictly forward, 6 states
`New → Starting → Ready → Closing → WaitServerClose → Closed` (Go, `session.go:74-116`) / `NEW → STARTING → READY → CLOSING → WAIT_SERVER_CLOSE → CLOSED` (Java, `Session.java:37-50`). Monotonic-by-phase; `Invoke` / `startRpc` MUST reject anything but Ready with a retryable-tagged error (Go: `ErrSessionNotActive` tagged `StateUncommitted`; Java: `INTERNAL` on a second concurrent `startRpc`, `SessionImpl.java:410-436`).

### 2. One in-flight vRPC per session — enforced, not advisory
Multiplex limit is **exactly 1** (`multiPlexingLimit=1`, `session.go:31`). Go enforces via `activeRPC.CompareAndSwap(nil, rpc)`; a failed CAS is a **caller (pool) bug**, not backpressure. Java's `SessionImpl.startRpc` returns `INTERNAL` on a concurrent call. Every vRPC carries a monotonic `rpcId` from `nextRPCID` / `nextRpcId`; a response whose `rpc_id` does not match the outstanding call is **dropped**, not delivered.

### 3. PeerInfo (AFE ID) MUST be resolved synchronously before Ready is announced
Go parses `bigtable-peer-info` (URL-safe base64 proto) inline in `handleOpenSession` and populates `peerInfo`/`AfeID` **before** `OnActive` fires (`session_lifecycle.go:266-289, 571-587`). Java's `SessionHandle.onSessionStarted` extracts `AfeId.extract(peerInfo)` before the session enters `afesWithReadySessions` (`SessionList.java:163, 376-378`). Zero AFE ID is a legal "unknown" bucket — still routable, doesn't count toward fanout.

### 4. Lifecycle hooks fire in a fixed order, exactly once each
`OnStart → OnActive → OnClosing → OnClose` (Go, `sync.Once`-guarded, `session.go:130-158`); `onReady → onGoAway? → onClose` (Java, dispatched on `sessionSyncContext`). `OnClosing`/`onGoAway` fires on **the first** exit from Ready via any path (`Close`, `ForceClose`, `handleGoAway`, `handleClose`) — subsequent triggers are no-ops.

### 5. Close is idempotent; graceful waits for in-flight vRPC; ForceClose does not
`Close`/`ForceClose`/`handleClose`/`handleGoAway` MUST be safe to call any number of times from any goroutine/thread. Graceful: Closing → drain via `quiescent`/in-flight-completion → Send `CloseSessionRequest` → `WaitServerClose`. `ForceClose`: jump past Closing, send **no** `CloseSessionRequest`, cancel active RPCs. **Close-reason is CAS-once** (`GoAway` / `MissedHeartbeat` / `Error` must beat a late `StreamEnd:*` classification).

### 6. GOAWAY does NOT cancel the in-flight vRPC
Server may still deliver the response — matters for non-idempotent `Apply`. GOAWAY only: (a) Ready→Closing, (b) fires `OnClosing`/`onGoAway` so the pool stops routing new work, (c) schedules an off-loop `Close` driver with a bounded deadline (30s in Go). Drain uses the same in-flight-completion path as normal `Close`. Java + Go match (`SessionImpl.java:689-716`, `session_lifecycle.go:353-402`).

### 7. Heartbeat is armed only while a vRPC is in flight
- Enforced **only while `activeRPC != nil`** — server emits heartbeats *during* long-running vRPCs; idle sessions legitimately receive none and must not be torn down (`session_lifecycle.go:522-528`). Java idle path pushes `nextHeartbeat` to `FUTURE_TIME` (30 min) (`SessionImpl.java:440-443`).
- Deadline = **`3 × heartbeatInterval`** (default 10 s ⇒ 30 s). Rationale: protocol tolerates **two** missed heartbeats before declaring the stream half-dead.
- **Any frame in either direction resets the deadline** — request Send, response Recv, heartbeat frame, `SessionRefreshConfig`, error responses. **Unknown frame types explicitly do NOT reset** (`session_lifecycle.go:233-263`) — otherwise a rogue future frame type would mask a broken stream.
- Reset is 2 atomic loads + 1 atomic store on the hot path; no mutex.

### 8. Missed-heartbeat sequence is fixed and observable
On miss, in this exact order (`session_lifecycle.go:558-572`):
1. `recordDebugTag(tagSessionHeartbeatMissed)` — fires the debug-tag counter.
2. `debugf("heartbeat MISSED — forcing close in_flight=%d last_frame_age=%v ...")` — deterministic log marker **before** ForceClose so it isn't lost to a downstream cancel race.
3. `recordEvent("hb-missed", ...)` — appended to the session's event ring buffer (surfaces on `sessionz`).
4. `ForceClose(&CloseSessionRequest{Reason: CLOSE_SESSION_REASON_MISSED_HEARTBEAT, Description: "client terminated session due to missed server heartbeats"})`.
5. `heartBeatLoop` returns; does not respawn.

ForceClose semantics on this path: transition **directly to `WaitServerClose`, skipping `Closing`**; send **no** `CloseSessionRequest` (stream is presumed dead); `cancelActiveRPCs` delivers `ErrUnavailableHeartBeatMissed` (wrapped `codes.Unavailable`), **tagged `StateTransportFailure`** (`session_vrpc.go:378`) — server may or may not have processed the request, so retry is safe only for idempotent ops. `OnClosing`/`OnClose` still fire exactly once each. `"MissedHeartbeat"` wins the close-reason CAS over any late `"StreamEnd:*"`. Java parity: `SessionImpl.java:440-443, 460-490, 605, 674, 855`.

### 9. Server is the oracle for retry — client never invents retryability from raw gRPC codes
Retry decisions consume a **three-value classification** (Go `AttemptState` in `attempt_outcome.go:28-46`; Java `VRpc.VRpcResult.State`), NOT the underlying gRPC code:

| State | Meaning | Retry rule |
|---|---|---|
| `Uncommitted` | Never left the client (encode fail, session Closing, pool rejected, ctx dead before Send) | Retry unconditionally |
| `TransportFailure` | Handed to transport, no server response observed (Send err, Recv err, ctx cancel mid-flight, heartbeat miss) | Retry **only** if idempotent |
| `ServerResult` | Server returned a definitive `ErrorResponse` (or decode err on delivered response) | Retry **only** if server attached `RetryInfo`, or code is in the narrowed always-retryable set (`RetryingVRpc.java:290-311`) |

Additional invariants:
- **Zero value = `ServerResult` = do-not-retry.** Untagged errors are fail-closed by design. Any retryable path MUST call `tagErr(...)` explicitly (`attempt_outcome.go:28-42`).
- **`RetryInfo` is plumbed on the error, not out-of-band.** Session packs the server's `errdetails.RetryInfo` into `status.Status` via `status.WithDetails` (`session_vrpc.go:320-335`); downstream extracts via `status.FromError(err).Details()`. No side-channel.
- **`RetryInfo.retryDelay` is honored only if it fits within the remaining deadline** — server-suggested delays that would exhaust the caller's deadline are ignored, not clipped (`RetryingVRpc.java:290-298`).
- **Retries capped at 3 attempts total** (`RetryingVRpc.java:305-311`), independent of server `RetryInfo`.
- **`Rejected` is terminal** (pool closing, wrong state, no ready AFEs) — the caller sees it, not RetryingVRpc.
- **GOAWAY does NOT re-classify an in-flight vRPC** — retry oracle fires only on the vRPC's terminal outcome (see #6).

Reference Go tag sites (`session_vrpc.go`): encode err → `Uncommitted:88-94`; session not Ready → `Uncommitted:108`; Send err → `TransportFailure:129`; ctx.Done during Recv → `TransportFailure:213`; server `ErrorResponse` → `ServerResult:225`; response decode err → `ServerResult:240`; `cancelActiveRPCs` (close/goaway/heartbeat) → `TransportFailure:378`.

### 10. Concurrency discipline is model-specific but non-negotiable
- **Go:** all Session mutable state is `atomic.*` (`state`, `activeRPC`, `peerInfo`, `refreshConfig`, `heartbeat*Nano`, `nextRPCID`, `poolHandle`). The **only** mutex is `sendMu` — `grpc.ClientStream.Send` is not concurrent-safe. `deliver` is cap-1 non-blocking (duplicate deliveries dropped; first wins). Lock ordering: **never take `pool.mu` while holding `sl.mu`**.
- **Java:** every mutation runs inside `sessionSyncContext` (io.grpc `SynchronizationContext`); public entrypoints trampoline via `execute(...)`; handlers assert `throwIfNotInThisSynchronizationContext()`. Uncaught exceptions invoke `abortFromUncaughtException` — re-entrancy-guarded, force-closes the stream, calls `notifyTerminalClose` exactly once.

---

## Pool-topology & configuration invariants (#11–#14)

### 11. Every resource has two session pools — read and write — except MaterializedView, which has read only
- `SessionClient.OpenTable(name)` → `sessionTable{readPool, writePool}` with descriptors `READ_ROW` / `MUTATE_ROW` (`client.go:369`).
- `SessionClient.OpenAuthorizedView(view)` → `sessionTable{readPool, writePool}` with descriptors `READ_ROW_AUTH_VIEW` / `MUTATE_ROW_AUTH_VIEW` (`client.go:397`).
- `SessionClient.OpenMaterializedView(view)` → `sessionTable{readPool, writePool=nil}` with `READ_ROW_MAT_VIEW`; **MatView is read-only by contract** (`client.go:403-415`). `MutateRow` on a MatView MUST return `ErrWriteNotSupported` (`table.go:29-31, 154`).
- Pools are **`lazyPool`** — opened on first `ReadRow` / `Apply`, not at `OpenTable`. Failed opens are **not cached** (a transient `proto.Marshal` failure MUST NOT strand the table); the next call retries.
- **Read and write pools do not share sessions.** Each pool holds its own set of Sessions typed for its direction (via `SessionType` on the underlying `Session`), each running its own OpenSession bidi stream. This is what keeps the multiplex=1 rule (spec #2) from starving cross-direction traffic.
- Behavioral shift vs classic path: marshal failures on session payloads surface on first `ReadRow`/`Apply`, not at `OpenTable`. Callers that expected eager failure will see it later.

### 12. All Session Pools on a `SessionClient` share one channel pool
- `sessionClient` owns exactly one `ChannelPool` (`client.go:126-160`), one gRPC stub bound to that pool, and one `ClientConfigurationManager` polling through the same stub.
- Every `*SessionPoolImpl` constructed by that `SessionClient` — regardless of resource (table/AV/MatView) or direction (read/write) — dials via that single channel pool. Session traffic is fanned out **inside** the pool (across sub-channels sized by `ChannelPool.MinServerCount` / `MaxServerCount` / `PerServerSessionCount`), not per-pool.
- The session channel pool is **distinct from the classic (data-plane) channel pool**. Mixed-mode `bigtable.Client` holds both: the classic pool warmed with pingAndWarm, and the session pool warmed with `getClientConfigDirectAccessChecker` (no priming). The `Diverter` (see #14) is what routes each user call to one or the other.
- Standalone `NewSessionClient` skips the classic pool entirely — one channel pool per client, one stub, one config manager. All session traffic — vRPC, `GetClientConfiguration` polls, `OpenSession` streams — goes through it.
- Teardown: `SessionClient.Close()` closes every `SessionPoolImpl`, then the shared channel pool, then the metrics factory. `backgroundCancel` unwinds every per-pool goroutine parented on the client's background ctx.

### 13. `GetClientConfiguration` is the authoritative source for pool load and shape
Every knob that governs a session pool's **size, admission, load-balancing, and traffic share** MUST come from the server-driven `ClientConfiguration`, not from hardcoded client defaults (defaults exist only as bootstrap fallback until the first successful poll). The polled `clientConfig` (`client_configuration_manager.go:37-89`) carries:

| Field | Governs |
|---|---|
| `Session.SessionLoad` (float64, 0.0–1.0) | Fraction of traffic the `Diverter` sends via session pools vs classic. Emitted to callers via `SessionLoadListener` → `Diverter.SetSessionLoad`. |
| `Session.ChannelPool.MinServerCount` / `MaxServerCount` | Channel-pool sizing bounds. |
| `Session.ChannelPool.PerServerSessionCount` | Target sessions per sub-channel; drives the pool's steady-state session count. |
| `Session.ChannelPool.DirectAccessCheckInterval` / `DirectAccessErrorThreshold` | DirectAccess health probe cadence + trip threshold. |
| `Session.SessionPool.Headroom` (0.0–1.0) | Slack above in-use count that `PoolSizer` maintains; drives scale-up decisions. |
| `Session.SessionPool.MinSessionCount` / `MaxSessionCount` | Hard bounds on the pool's active session count. |
| `Session.SessionPool.NewSessionCreationBudget` / `NewSessionCreationPenalty` | Concurrency gate + failure back-off on new `OpenSession` calls (feeds `AdaptiveSessionThrottler`). |
| `Session.SessionPool.ConsecutiveSessionFailureThreshold` | Trip threshold for the creation-budget circuit breaker. |
| `Session.SessionPool.NewSessionQueueLength` | Waiter queue depth in `CheckoutSession`. |
| `Session.SessionPool.LoadBalancing.Strategy` ∈ {`LeastInFlight`, `Random`, `PeakEwma`} + `RandomSubsetSize` | Picker choice (`AfePicker` impl) and P2C sample width. |
| `Polling.PollingInterval` / `ValidityDuration` / `MaxRpcRetryCount` | Cadence + retry policy of the config poll itself. Interval is **clamped to `MinPollingInterval = 1 min`** — server cannot ask the client to poll faster (`client_configuration_manager.go:33`). |

Additional invariants:
- **Config changes MUST reshape live pools, not just future ones.** `ClientConfigurationManager` fires registered `configListener` callbacks with a monotonic `configSeq`; each pool's `SessionPoolImpl.UpdateConfig` reshapes the picker, sizer, budget, and load-balancing strategy in place. New sessions come and go per the new bounds; existing in-flight vRPCs are undisturbed.
- **`configSeq` is monotonic** — listeners MUST ignore any callback with a `seq` older than the last one they processed (out-of-order delivery from the polling loop is possible under Close-race).
- **Close happens-before listener silence.** `ClientConfigurationManager.Close()` sets `closed.Store(true)` **before** closing `done`, waits `pollsWG`, and thereby guarantees no listener callback fires after `Close()` returns — pools tearing down cannot race an inbound `UpdateConfig`.
- **On poll failure**, the last successfully-fetched config remains authoritative until `ValidityDuration` expires; after expiry, the client MUST fall back to bootstrap defaults, not to an arbitrarily stale config. `lastResponse` / `lastErr` / `pollHistory` are kept verbatim for the `configz` debug page.
- **`SessionLoad` is the only knob that couples session and classic paths.** All other fields shape session-side infra only. This is what makes mixed-mode safe: turning `SessionLoad` to 0 quiesces session traffic without touching classic pool sizing.

### 14. Traffic split between session and classic (unary) pools is a two-piece routing tier: `Diverter` (policy) + `TableShim` (mechanism)

**`Diverter` (`internal/transport/diverter.go`) — policy layer.**
- Stores one `sessionLoad` (float64, 0.0–1.0) as `atomic.Uint64` of `math.Float64bits(load)`. Updated by `SetSessionLoad(load)` — the ONLY writer is `ClientConfigurationManager` via `SessionLoadListener` (spec #13). Manual overrides for tests/staging use the same setter.
- `UseSession()` decides per call: `load<=0` → false; `load>=1` → true; otherwise `rand.Float64() <= load`. Every call increments either `sessionPicks` or `classicPicks` (atomic counters) so `configz`/`sessionz` can show **actual** ratio vs configured — the two diverge during rollouts and are the ground-truth signal.
- Diverter is stateless per-call and **has no memory of a specific row/key** — the split is stochastic across calls, not sticky. A single logical operation on the same row may land on classic once and session next time. Any invariant that assumes the same connection for two consecutive calls MUST NOT rely on this layer.
- Snapshot: `DiverterSnapshot{SessionLoad, SessionPicks, ClassicPicks}` — surfaced under the `SessionDebugProvider.Diverter()` method (#12 debug provider iface).

**`TableShim` (`bigtable/table_shim.go`) — mechanism layer.**
- Implements the public `TableAPI` — this is what user code holds when running mixed-mode. Owns `(classic TableAPI, session SessionTableApi, diverter *Diverter)`. Any of `session` / `diverter` may be nil → **shim degrades to classic-only** silently. This is the fallback contract when session support is not enabled or the pool failed to open.
- Per-call routing rule: `if !t.useSession() { classic } else { session }`. `useSession()` = `session != nil && diverter != nil && diverter.UseSession()`.
- Owns **all proto ↔ `bigtable.Row` conversion** at the boundary — the `internal/session` package stays proto-native (never sees `bigtable.Row`, `Mutation`, `Filter`, etc.). This is how the two data planes stay decoupled.
- Method routability is **fixed by shape**, not by config — the shim MUST NOT attempt to route an operation whose vRPC equivalent doesn't exist. Enforced today as:

| `TableAPI` method | Routable? | Why |
|---|---|---|
| `ReadRow` | via Diverter | `SessionReadRow` vRPC exists |
| `Apply` (non-conditional) | via Diverter | `SessionMutateRow` vRPC exists |
| `Apply` (conditional, `m.isConditional`) | **always classic** | `CheckAndMutateRow` has no vRPC equivalent |
| `ReadRows` | always classic | streaming reads not in vRPC surface |
| `SampleRowKeys` | always classic | no vRPC equivalent |
| `ApplyBulk` | always classic | no vRPC equivalent |
| `ApplyReadModifyWrite` | always classic | no vRPC equivalent |

- **Read/write direction determines which of the two session pools is engaged** (spec #11): `ReadRow` → session table's read pool; `Apply` → write pool. The Diverter's decision precedes the pool split — it says "session vs classic", then the direction picks read-pool vs write-pool inside the session side.
- Response-side plumbing lives in the shim, not the session package: `WithFullReadStats` callbacks fire from `TableShim.ReadRow` after `protoRowToRow` converts the response.
- Errors from either side are surfaced to the caller as-is — the shim does NOT retry a session-side failure on classic. That would violate the retry-oracle contract (spec #9): a session `TransportFailure` on a non-idempotent `Apply` is not automatically safe to re-run on classic.

### 15. Debug views (all `/-z` pages) MUST NOT block hot-path latencies

Every debug view — `sessionz`, `afez`, `flightz`, `loadz`, `channelz`, `configz`, `tcpz`, `debugtagsz` — is a **passive observer** of session/pool state. It MUST NOT be able to slow down a real vRPC, session Send/Recv, pool checkout/return, or heartbeat.

**Concrete rules:**

- **Snapshot returns a value, not a live view.** Every `SessionDebugProvider.Snapshot()`, `LoadBalancingSnapshots()`, `Diverter()`, `Snapshot()` on any provider MUST copy the state it needs under its lock and release the lock before returning. The z-page handler holds no lock while writing the HTTP response body (`debugview/sessionz.go:100-104`, `debugview/afez.go:104`, `debugview/loadz.go:125`, `debugview/configz.go:61`).
- **Snapshots take at most an RLock, never a write lock, on any mutex held by the hot path.** `SessionPoolImpl.mu` and `sessionList.mu` are hot-path mutexes (acquired by `CheckoutSession`, `ReleaseSession`, `RecordVRpcOutcome`, `AddSession`). A snapshot method that takes the write side would serialize the hot path against a random HTTP request.
- **No lock on `Session` is held across HTTP write.** `Session`'s ring buffer (`sessionDebug.events`) and slow-vRPC log MUST be snapshotted with a short RLock or via `atomic.*` reads, then rendered lock-free.
- **Bounded work per request.** Ring buffers are fixed-size (session event ring, `pickHistory` at 500, `pollHistory` capped). No z-page may iterate an unbounded structure — if a snapshot would exceed a bound, it MUST truncate and mark the response with a `truncated=true` flag.
- **Metric emission is separate from snapshot rendering.** OTel histograms/counters are recorded synchronously on the hot path by the tracer (`internal/metrics/tracer.go`) — that's the source of truth. Z-pages read those metrics via the OTel SDK's own async collection path; they MUST NOT compute derived statistics inline that would require iterating live pool state a second time.
- **Auto-refresh cadence has a floor.** Each z-page's client-side refresh MUST be ≥ 2s (currently 2–10s per view). No page may poll faster; a 100ms refresh over 20 pools would burn measurable CPU on snapshot construction.
- **Provider dependency is via interface, not concrete type.** Per SESSION_COMPONENT_SPEC.md B3, debug code takes `SessionDebugProvider` (and siblings), never `*SessionPoolImpl`. This is what lets snapshot semantics evolve without breaking the debug UI, and it's what makes the "no hot-path lock" invariant enforceable — the interface says "return a snapshot," not "give me a pointer to internal state."
- **A hung z-page MUST NOT hang the process.** All snapshot methods MUST complete in bounded time even if the target pool is deadlocked (i.e. tryLock semantics or best-effort read of atomics). A z-page that blocks on `p.mu` forever, waiting on a stuck checkout, turns a debug URL into a liveness weapon.

**How the mixed-mode Client stays honest.** `mixedModeSessionDebug` (`bigtable/session_debug.go:66`) MUST NOT synthesize snapshots by reaching into `*Client` internals. It composes the underlying `SessionClient`'s provider with the classic-path diverter snapshot, both accessed through interfaces. Any new mixed-mode observability field goes on the provider interface, not on `*Client`.

### 16. `cluster_id` / `zone_id` / `transport peer` MUST be populated per-attempt on BOTH the classic (unary) and session (vRPC) paths, from path-specific sources

Client-side metrics (attribute labels on `attempt_latencies`, `attempt_latencies2`, `operation_latencies`) MUST carry the same set of routing/identity attributes regardless of which data path served the call. Cross-path dashboards would otherwise be unusable — half the traffic would be missing `cluster_id`. But the *source* of each field differs by path, and that difference is architectural, not accidental.

**Fields both paths MUST populate per attempt:**
- `cluster_id` — Bigtable cluster that served the request.
- `zone_id` — GCP zone of that cluster.
- `client_uid` — stable client identity (from the metrics factory).
- Transport peer labels — `transport_type`, `transport_region`, `transport_zone`, `transport_subzone` (from the AFE/backend PeerInfo).

**Where each field comes from — classic (unary) path:**
- **`cluster_id` + `zone_id`:** from the response's `ResponseParams` proto, packed into gRPC headers (`x-goog-cbt-cookie-*` routing cookies) and trailers by the server. Extracted per-attempt in `bigtable/internal/metrics/util.go:96-102` via `proto.Unmarshal` of the header/trailer bytes into a `btpb.ResponseParams`. Populated on the tracer's `attempt` state in `onAttemptCompletion`.
- **Transport peer labels:** from the gRPC connection's grpc.Peer info — a per-attempt observation, potentially different across attempts even in one operation.
- **Retry cookies:** the same `x-goog-cbt-cookie-routing-cookie` header is round-tripped as-is on the next attempt to preserve routing stickiness; that flow is separate from the metrics stamp but shares the same header.

**Where each field comes from — session (vRPC) path:**
- **`cluster_id` + `zone_id`:** from the vRPC response's typed `ClusterInformation` field, plumbed via `InvokeResult.ClusterInfo` (`session_vrpc.go:44-46, 216-219`). Stamped per-attempt on the tracer in `sessionTable.stampAttempt` → `att.SetClusterID(result.ClusterInfo.ClusterId)` / `att.SetZoneID(result.ClusterInfo.ZoneId)` (`internal/session/table.go:238-240`). **No `ResponseParams` unmarshal** — the server sends the cluster identity as a typed field on the vRPC response frame, not as an opaque header blob.
- **Transport peer labels:** from `InvokeResult.PeerInfo` (`session_vrpc.go`), which is a pointer to the owning `Session.peerInfo` — **the same PeerInfo every attempt on the same session sees**, because it was parsed once from the `bigtable-peer-info` header at session open (spec #3). This is a real semantic difference from classic: on classic, transport peer can vary per attempt; on session, it's fixed for the session's lifetime.
- **Retry cookies:** vRPC does not use `x-goog-cbt-cookie-routing-cookie`. Retry routing on session is a property of picker + AFE selection (spec #5's removed AFE items, preserved in SESSION_COMPONENT_SPEC.md Part C), not a header round-trip.

**Consequences worth calling out:**

- **A classic-path retry may hop clusters.** The `cluster_id` on attempt N-1 and attempt N may differ (routing cookie updates between attempts). Dashboards MUST NOT assume `cluster_id` is invariant across an operation.
- **A session-path retry within the same session cannot hop backends.** All attempts on session S have identical `transport_*` labels. Retries that need a different backend must be checked out onto a different session by RetryingVRpc — an observable, dashboard-visible event.
- **`ClusterInfo` may be nil on a session response.** Server MAY omit it on some responses (typically errors); the metric stamp gracefully skips (`session/table.go:238` guards `if result.ClusterInfo != nil`). Classic path has the same nil-guard on `ResponseParams` unmarshal.
- **BOTH paths MUST NOT leak session-only sources into classic tracers, or vice versa.** The metrics tracer is a shared type (`internal/metrics/tracer.go`) but the *stamp sites* live in path-specific code — classic stamps from headers/trailers in the unary interceptor; session stamps from `InvokeResult` in `sessionTable.ReadRow`/`MutateRow`. Do not add a "grab ClusterInfo from wherever" utility that both call; it would collapse the source distinction and hide protocol bugs.

Ownership matrix additions live in SESSION_COMPONENT_SPEC.md Part C.

### 17. Session pool scaling is server-driven and MUST NOT overwhelm the control plane

Pool size respects `MinSessionCount` / `MaxSessionCount` and `Headroom` from `GetClientConfiguration` (spec #13), and rate-limits `OpenSession` creation via `NewSessionCreationBudget` + `NewSessionCreationPenalty` back-off + `ConsecutiveSessionFailureThreshold` circuit breaker — so a bad server response cannot trigger a session-creation storm. Scale-down is passive: closed sessions are simply not replaced when the pool has slack above headroom (Java-parity replace-on-close; supersedes any active-reaper approach).

---

**See `SESSION_COMPONENT_SPEC.md`** for the component reference map and boundary/layering rules that prevent one component's logic from muddling into another's.
