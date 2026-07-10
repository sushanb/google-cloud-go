# Session — Behavioral Spec

**Scope.** This file governs the **runtime behavior** of a single `Session`'s lifecycle: code under `bigtable/internal/transport/session*.go` (`session.go`, `session_lifecycle.go`, `session_vrpc.go`, `session_state.go`, etc.). It covers the state machine, one-in-flight-vRPC rule, PeerInfo timing, hook ordering, close/GOAWAY semantics, heartbeat, retry oracle, and concurrency discipline. Any change to those files MUST be checked against the 10 invariants below.

**Sibling behavioral specs.** `SESSION_CLIENT_SPEC.md` (SessionClient topology, channel pool, config, OpenSession envelope, 4 invariants) · `SESSION_POOL_SPEC.md` (pool topology, picking, routing, scaling, debug non-blocking, 5 invariants) · `CLIENT_SIDE_METRICS_SPEC.md` (per-attempt metrics field provenance).

**Component/boundary spec.** `SESSION_COMPONENT_SPEC.md` — layering, ownership, import direction. Read it before any structural refactor.

**How to use.** Read top-to-bottom before editing files in scope. Cross-references to other specs use `<FILE>.md #N`-style anchors. When a change spans layers (e.g., a Session-lifecycle change that also touches pool routing), verify against every spec in scope.

**Java parity.** Where the two clients differ, both sides are cited so drift is visible. Deviations from Java parity require an explicit note in the invariant.

---

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

### 6. GOAWAY does NOT cancel the in-flight vRPC; `LastRpcIdAdmitted` bounds what may be retried

Server-initiated `GoAwayResponse` (`apiv2/bigtablepb/session.pb.go:2698-2761`) carries `{reason, description, last_rpc_id_admitted}`. On receipt, `handleGoAway` (`session_lifecycle.go:331-392`) does exactly the following, in order:

1. **Precondition assert.** `preState >= StateStarting`; a GOAWAY on a still-NEW session is a protocol oddity — recorded via `tagSessionGoawayBeforeStart` and the frame is dropped without advancing state.
2. **Ready → Closing** via `transitionTo(StateClosing, notState(Closing, WaitServerClose, Closed))`. A late GOAWAY on an already-terminal session (Closing / WaitServerClose / Closed) is a no-op — recorded via `tagSessionGoawayAfterClose` for observability but otherwise ignored (races a local teardown; harmless).
3. **`OnClosing` / `onGoAway` fires immediately** so the pool pulls the session out of `sessionList` routing structures. This is up to `waitServerCloseGrace` (30s) earlier than the actual stream close — the whole point of GOAWAY is *early* removal from routing, not synchronous teardown.
4. **`"GoAway"` wins the close-reason CAS** (`setCloseReason("GoAway")`) — beats any later `StreamEnd:*` classification that `handleClose` would otherwise stamp when the stream actually EOFs.
5. **`Reason` and `Description` from the payload are logged** via `debugf("received GOAWAY reason=%q description=%q", ...)` and recorded on the session event ring buffer so they surface on `sessionz`.
6. **In-flight vRPC is NOT canceled.** Java parity: `SessionImpl.java:689-716` — `handleGoAwayResponse` leaves `currentRpc` alone. If the server sends the vRPC response before dropping the stream, the RPC completes successfully. Only when the stream actually terminates does `handleClose → cancelActiveRPCs` fail it with `TransportFailure`. This grace period is what makes GOAWAY on server graceful drains safe for non-idempotent `Apply`: the previous behavior (fail-fast on GOAWAY) turned successful server-side commits into client-visible transport failures.
7. **Off-loop `Close` driver** — `handleGoAway` spawns a goroutine that calls `s.Close(ctx, CloseSessionRequest{Reason: CLOSE_SESSION_REASON_GOAWAY, Description: "client teardown after server GOAWAY"})` under a **30s bounded** ctx. `Close` drains via `quiescent` (or `ForceClose`s at deadline) before sending `CloseSession` — the in-flight vRPC gets its full chance without extra scheduling here.

**`LastRpcIdAdmitted` retry oracle.** The server's guarantee is: *any vRPC with `rpc_id <= last_rpc_id_admitted` will receive its response on this stream before disconnect; anything beyond that boundary must be retried on a fresh session.* The client MUST treat a `TransportFailure` on a vRPC with `rpc_id > LastRpcIdAdmitted` as retryable per the #9 oracle (subject to idempotency), regardless of the underlying gRPC code. The transport is responsible for funneling that retry back through `RetryingVRpc` — it MUST NOT surface as terminal to the caller.

**Session `Close` reason on GOAWAY-driven teardown.** The `CloseSessionRequest` we send server-side stamps `CLOSE_SESSION_REASON_GOAWAY` (Go) / `CLOSE_SESSION_REASON_GOAWAY` with description `"Server sent GO_AWAY_" + reason` (Java, `SessionImpl.java:706-711`). Description prefix differs; enum value matches.

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

**See `SESSION_COMPONENT_SPEC.md`** for the component reference map and boundary/layering rules that prevent one component's logic from muddling into another's.
