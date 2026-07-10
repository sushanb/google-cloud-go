---
marp: true
theme: default
paginate: true
size: 16:9
header: 'Bigtable Session subsystem — spec-driven review'
style: |
  section { font-size: 20px; }
  h1 { color: #1a73e8; }
  h2 { color: #202124; border-bottom: 2px solid #dadce0; padding-bottom: 6px; }
  code { background: #f1f3f4; padding: 1px 4px; border-radius: 3px; font-size: 0.9em; }
  pre { background: #f8f9fa; border-left: 3px solid #1a73e8; font-size: 15px; }
  table { font-size: 0.8em; }
  th { background: #e8f0fe; }
  blockquote { border-left: 4px solid #34a853; background: #f1f8f4; padding: 8px 14px; margin: 6px 0; font-size: 0.9em; }
---

# How we use SPECs on the Session subsystem

<br>

**The problem.** 36k-line diff on `bigtable/`. Session lifecycle + AFE picker + Diverter + z-pages + metrics all in flight. Every review is "does this look right?" — no shared axis, no way to prove a change *preserved* an existing rule.

**The fix.** Write the rules down. Split by concern. Enforce with subagents. Wire a hook so nobody forgets.

<br>

### Five spec files at repo root

| File | Invariants | Governs |
|---|---|---|
| `SESSION_SPEC.md` | 10 | One Session's lifecycle (state machine, GOAWAY, heartbeat, retry oracle) |
| `SESSION_CLIENT_SPEC.md` | 4 | `Client`↔`SessionClient`↔pools topology, config, OpenSession envelope |
| `SESSION_POOL_SPEC.md` | 5 | Pool per resource, picker discipline, Diverter+TableShim, scaling, debug non-blocking |
| `CLIENT_SIDE_METRICS_SPEC.md` | 1 (room to grow) | Per-attempt `cluster_id`/`zone_id`/peer sourcing |
| `SESSION_COMPONENT_SPEC.md` | Part B + Part C | Layering, imports, ownership matrix |

### The workflow

1. **Edit** a session-scope file → **PostToolUse hook fires** with a reminder.
2. Before commit, **invoke both reviewer agents in parallel**: `session-reviewer` (behavioral) + `session-component-review` (boundaries).
3. Each returns a table: `Spec | # | Verdict (PASS / VIOLATION / AMBIGUOUS) | Evidence`.
4. On VIOLATION → fix the code, OR (if the rule is stale) update the spec **in the same PR** with justification.
5. On PASS → commit.

Specs are **living** — updated in the same PR as the code they govern. Java-parity notes make drift visible. `file:line` citations make claims grep-checkable.

---

## Example — `SESSION_SPEC.md #6` (GOAWAY + `LastRpcIdAdmitted`)

**Shape.** Heading = the rule. Body = order-of-ops + Java-parity + consequences. Reviewers cite by *file + number*.

> ### 6. GOAWAY does NOT cancel the in-flight vRPC; `LastRpcIdAdmitted` bounds what may be retried
>
> `GoAwayResponse` carries `{reason, description, last_rpc_id_admitted}`. On receipt, `handleGoAway` (`session_lifecycle.go:331-392`) does **exactly** the following, in order:
>
> 1. **Precondition assert.** `preState >= StateStarting`; GOAWAY on NEW is a protocol oddity → drop.
> 2. **Ready → Closing.** Late GOAWAY on terminal session is a no-op (tag `tagSessionGoawayAfterClose`).
> 3. **`OnClosing` / `onGoAway` fires** so the pool pulls this session out of routing structures.
> 4. **`"GoAway"` wins the close-reason CAS** — beats any later `StreamEnd:*` classification.
> 5. **In-flight vRPC is NOT canceled.** Java parity: `SessionImpl.java:689-716`. Server may still deliver the response — critical for non-idempotent `Apply`.
> 6. **Off-loop `Close` driver** under 30s bounded ctx; drains via `quiescent` before `CloseSession`.
>
> **`LastRpcIdAdmitted` retry oracle.** vRPCs with `rpc_id > LastRpcIdAdmitted` that fail on stream drop MUST be retried on a fresh session per `#9` (subject to idempotency). Transport funnels through `RetryingVRpc`; MUST NOT surface as terminal.

**Why this shape works.** A reviewer grepping `handleGoAway` in the diff can check each numbered step against the code. The Java-parity cite (`SessionImpl.java:689-716`) makes divergence visible. Cross-ref `#9` is unambiguous (same file, retry oracle invariant).

**Anti-pattern this prevents:** "GOAWAY cancels the in-flight" — the pre-#6 behavior. A one-line "fix" to cancel on GOAWAY would flip a non-idempotent Apply from success to `TransportFailure`. The spec makes that regression a VIOLATION with a rule-cite, not a "hmm, seems fine" review comment.

---

## `SESSION_COMPONENT_SPEC.md` — 7-layer topology (Part A snapshot)

Part A is the *descriptive* layer map — drifts with code. Part B (12 boundary MUST-rules) and Part C (ownership matrix) are the *prescriptive* durable specs.

### Layers, bottom-up

| # | Layer | Location | Key types |
|---|---|---|---|
| 1 | **Transport primitives** | `bigtable/internal/transport/` | `Stream`, `State`, `AttemptState`, `reqMsgType`/`respMsgType` |
| 2 | **Session** | `bigtable/internal/transport/session*.go` | `Session`, `SessionHooks`, `afeID`, `SessionType` |
| 3 | **Pool** | `bigtable/internal/transport/session_pool*.go` | `SessionPoolImpl`, `sessionList`, `afeHandle`, `SessionHandle`, `PeakEwma`, `PoolSizer`, `SessionThrottler` |
| 3.5 | **Picker** (part of Pool) | `afe_picker.go`, `picker.go` | `AfePicker` iface + `Simple`/`LeastInFlight`/`LeastLatency` impls, `PickDecision` |
| 4 | **Session client + tables** | `bigtable/internal/session/` | `SessionClient`, `SessionTableApi`, `lazyPool`, `ClientConfigurationManager` |
| 5 | **Routing shim + diverter** | `bigtable/table_shim.go`, `internal/transport/diverter.go` | `TableShim`, `Diverter` (Go-only, mixed-mode) |
| 6 | **Public bigtable API** | `bigtable/` | `Client`, `Table`, `Mutation`, `Row`, `Filter`, `ReadOption` |
| 7 | **Observability tier** | `bigtable/debugview/`, `*_snapshot.go`, `debug_api.go` | `SessionDebugProvider` iface, snapshot DTOs, 7 z-pages |

### Boundary rules (Part B highlights — grep-checkable)

| Rule | What it forbids | Grep check |
|---|---|---|
| **B1** | `internal/session/` importing public bigtable types | `git grep -l 'bigtable\.\(Row\|Mutation\|Filter\)' bigtable/internal/session/` MUST be empty |
| **B2** | `internal/transport/` importing `internal/session/` | `git grep -l '.../internal/session' bigtable/internal/transport/` MUST be empty |
| **B3** | z-pages importing concrete pool/session types (must use interfaces) | Any new `debugview/*` import of `*SessionPoolImpl` |
| **B6** | Anyone other than `ClientConfigurationManager` calling `SetSessionLoad` / `UpdateConfig` | `git grep -n 'SetSessionLoad\|UpdateConfig' -- ':!*_test.go' ':!*_configuration_manager.go'` |
| **B8** | Lock inversion: `sl.mu` before `pool.mu` | Manual review of any new lock acquisition pair |

### Two Go-only structural quirks worth remembering

- **Lazy per-op pools.** `SessionTable` holds two `*lazyPool`; opened on first `ReadRow`/`Apply`. Java eagerly builds per-resource pools.
- **Mixed-mode + z-pages.** `TableShim` / `Diverter` / `debugview/*` have no Java analog. Java is session-only OR classic-only, not both.

---

## Three prompts for three change shapes

The spec + hook pattern scales across change size. What varies is *how much reviewing* happens — the mechanism is identical.

### 1. Simple fix (one line, one file)

> "Fix the off-by-one in `session_lifecycle.go:558` — the missed-heartbeat log should print `in_flight` **before** calling ForceClose, not after (it's getting lost to the cancel race)."

**Flow:** Edit fires the hook once → reminder → `session-reviewer` reads all 4 specs → checks only `SESSION_SPEC.md #8` (missed-heartbeat sequence, which mandates step 2 = log-before-ForceClose) → single-row report `SESSION | 8 | PASS | session_lifecycle.go:558`. `session-component-review` finds nothing touched — empty report. Commit.

### 2. Multi-file fix (spans layers)

> "Server started sending `LastRpcIdAdmitted` in GOAWAY. Wire it into the retry path so vRPCs past the boundary get retried on a fresh session instead of surfacing as `TransportFailure`."

**Flow:** Edits touch `session_lifecycle.go` (parse the field), `session_vrpc.go` / `retrying.go` (retry classification), maybe `session_snapshot.go` (surface on `sessionz`). Hook fires per edit. Before commit → both agents in parallel:

- `session-reviewer` checks `SESSION_SPEC.md #6` (GOAWAY steps preserved?), `#9` (retry oracle honors the boundary?), `SESSION_POOL_SPEC.md #2` (picker unaffected?). Reports 3 rows.
- `session-component-review` checks B2 (didn't accidentally import `internal/session` from transport?), Part C (retry classification stays owned by `attempt_outcome.go`?). Reports 2 rows.

**Both PASS** → commit. **Either VIOLATION** → fix, re-run.

### 3. New feature (adds a new invariant)

> "Add server-driven `SessionRefreshConfig.OptimizedOpenRequest` — server can hand the client a pre-optimized replacement handshake to use on session reconnect."

**Flow:** Feature crosses `SESSION_CLIENT_SPEC.md #4` (OpenSessionRequest envelope) — the "payload bytes are immutable per pool" rule needs a new exception for the refresh path. Before writing code: **update `SESSION_CLIENT_SPEC.md #4` first**, add the paragraph explaining the exception, then write the code to match. Reviewers now enforce the *new* rule, not the old one.

**Anti-pattern this prevents:** shipping code that quietly contradicts a spec. Spec-first for new invariants forces the design conversation before the diff exists.

---

## PostToolUse hook — makes forgetting expensive

Hooks are shell commands, not tool calls — they can't invoke subagents directly. What they *can* do is fail loudly on relevant edits and tell Claude exactly what to run next.

```json
{
  "hooks": {
    "PostToolUse": [{
      "matcher": "Edit|Write|MultiEdit",
      "hooks": [{
        "type": "command",
        "command": "jq -r '.tool_input.file_path // empty' | grep -qE 'bigtable/(internal/(transport/(session|afe_picker|diverter)|session/)|table_shim|debugview/|session_[a-z]+\\.go)|(SESSION_(SPEC|CLIENT_SPEC|POOL_SPEC|COMPONENT_SPEC)|CLIENT_SIDE_METRICS_SPEC)\\.md' && { printf 'Session subsystem file just edited. Before committing:\\n  1. Invoke `session-reviewer` (behavioral — 4 spec files)\\n  2. Invoke `session-component-review` (boundaries)\\nRun in parallel. If either reports VIOLATION, fix before committing.\\n' >&2; exit 2; } || exit 0"
      }]
    }]
  }
}
```

### How it works, mechanically

1. **Trigger:** every `Edit` / `Write` / `MultiEdit` fires the hook.
2. **Match:** `jq` pulls `file_path` from tool input; `grep -qE` runs the session-scope regex — matches Go files under `session*.go`, `afe_picker.go`, `diverter.go`, `debugview/`, `table_shim.go`, plus all 5 spec `.md` files.
3. **On match:** `exit 2` + stderr message. `exit 2` is the *blocking* code — Claude sees the message as a required-attention notice on the next turn.
4. **On no match:** `exit 0`, silent — hook fired but nothing to say.

### Batching behavior

Hook fires *per edit* — Claude sees the reminder repeatedly during a multi-file change. The message text says **"before committing this batch"** so Claude runs the two agents *once*, in parallel, right before `git commit` — not after every keystroke. Enforcement without per-edit cost.

### What this guarantees

- You cannot silently ship a Session-touching change without at least being told the two reviewers exist.
- The regex is narrow (session subsystem only) — no false positives on unrelated `bigtable/bigtable.go` or `spanner/` edits.
- Skipping the reviewers is now a *deliberate* choice, not an oversight.
- Spec-file edits also trigger the hook — reminding to pair the spec change with code (or vice versa).
