# Client-Side Metrics — Behavioral Spec

**Scope.** This file governs the **runtime behavior** of per-attempt metric stamping across both data paths: `bigtable/internal/metrics/**` (tracer + attribute plumbing + OTel adapters), the classic-path stamp sites in the unary interceptor chain (`bigtable/gax_wrapper.go`, `bigtable/bigtable.go`), and the session-path stamp sites in `bigtable/internal/session/table.go` (`sessionTable.stampAttempt`) driven by `InvokeResult` from `bigtable/internal/transport/session_vrpc.go`. It covers what labels every attempt-latencies / operation-latencies emission MUST carry, where each label comes from on each path, and why those sources are architecturally distinct. Any change to those files MUST be checked against the 1 invariant below.

**Sibling behavioral specs.** `SESSION_SPEC.md` (per-Session lifecycle) · `SESSION_CLIENT_SPEC.md` (SessionClient topology + config + OpenSession envelope) · `SESSION_POOL_SPEC.md` (pool topology, picking, routing, scaling, debug non-blocking).

**Component/boundary spec.** `SESSION_COMPONENT_SPEC.md` — layering, ownership, import direction. See especially Part C ownership matrix for who writes each attribute.

**How to use.** Read top-to-bottom before editing files in scope. Cross-references to other specs use `<FILE>.md #N`-style anchors.

**Room to grow.** This file starts with 1 invariant (routing/identity attributes per attempt), but is deliberately scoped to hold future metrics rules — `client_uid` stability, method-label discipline, latency-histogram bucket contract, exemplar rules, cardinality budget, per-attempt vs per-operation aggregation. Add new invariants as #2, #3, … as they are decided. Do NOT invent invariants that aren't already understood — reserve the numbering.

**Java parity.** Where the two clients differ, both sides are cited. Deviations require an explicit note in the invariant.

---

### 1. `cluster_id` / `zone_id` / `transport peer` MUST be populated per-attempt on BOTH the classic (unary) and session (vRPC) paths, from path-specific sources

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
- **Transport peer labels:** from `InvokeResult.PeerInfo` (`session_vrpc.go`), which is a pointer to the owning `Session.peerInfo` — **the same PeerInfo every attempt on the same session sees**, because it was parsed once from the `bigtable-peer-info` header at session open (`SESSION_SPEC.md #3`). This is a real semantic difference from classic: on classic, transport peer can vary per attempt; on session, it's fixed for the session's lifetime.
- **Retry cookies:** vRPC does not use `x-goog-cbt-cookie-routing-cookie`. Retry routing on session is a property of picker + AFE selection (`SESSION_POOL_SPEC.md #2`), not a header round-trip.

**Consequences worth calling out:**

- **A classic-path retry may hop clusters.** The `cluster_id` on attempt N-1 and attempt N may differ (routing cookie updates between attempts). Dashboards MUST NOT assume `cluster_id` is invariant across an operation.
- **A session-path retry within the same session cannot hop backends.** All attempts on session S have identical `transport_*` labels. Retries that need a different backend must be checked out onto a different session by `RetryingVRpc` — an observable, dashboard-visible event.
- **`ClusterInfo` may be nil on a session response.** Server MAY omit it on some responses (typically errors); the metric stamp gracefully skips (`session/table.go:238` guards `if result.ClusterInfo != nil`). Classic path has the same nil-guard on `ResponseParams` unmarshal.
- **BOTH paths MUST NOT leak session-only sources into classic tracers, or vice versa.** The metrics tracer is a shared type (`internal/metrics/tracer.go`) but the *stamp sites* live in path-specific code — classic stamps from headers/trailers in the unary interceptor; session stamps from `InvokeResult` in `sessionTable.ReadRow`/`MutateRow`. Do not add a "grab ClusterInfo from wherever" utility that both call; it would collapse the source distinction and hide protocol bugs.

Ownership matrix additions live in `SESSION_COMPONENT_SPEC.md` Part C.

---

**See `SESSION_COMPONENT_SPEC.md`** for the component reference map and boundary/layering rules that prevent one component's logic from muddling into another's.
