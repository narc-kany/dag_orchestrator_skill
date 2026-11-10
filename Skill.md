---
name: dag_orchestrator_skill
version: 1.0.0
description: >-
  High-assurance enterprise multi-agent DAG orchestrator. Compiles compound multi-hop analytical queries into a
  validated DAG, enforces token and tail-latency budgets, dispatches subtasks over MCP to heterogeneous engines,
  isolates non-blocking shadow-mode SLM telemetry and synthesizes conflict-free answers grounded in
  cryptographically verified provenance under a strict 4-tier epistemic hierarchy.
owner: Sivanesh S.
data_classification: Tier 1 (Highly Confidential / Enterprise IP)
approval_status: Pending Architecture Review Board (ARB) Audit
production_connection: prohibited_until_arb_closed
slo:
  e2e_latency_ms: { p50: 1400, p95: 3000, p99: 4200 }
  routing_coverage_min: 0.75
  selective_accuracy_min: 0.85
  ece_max: 0.10
  tau_abstain: 0.80
mcp:
  tools: [tool_core_compute, tool_policy_ast, tool_hybrid_rag, tool_slm_intent_shadow]
  resources:
    - orchestrator://reference/guidelines
    - orchestrator://reference/prompts/task_decomposition
    - orchestrator://reference/prompts/conflict_synthesis
triggers:
  route_when: [compound_multi_hop_analysis, conflict_synthesis, multi_engine_routing, dag_compilation]
  never_route_when: [core_policy_mutation, epistemic_ground_truth_bypass, unverified_rule_execution, shadow_output_disclosure]
---

# `dag_orchestrator_skill`: High-Assurance Enterprise Multi-Agent DAG Orchestrator

> **GOVERNANCE NOTICE - PRODUCTION CONNECTION PROHIBITED.** This skill orchestrates Tier 1 enterprise IP (policy
> corpus, risk and exposure telemetry, routing intelligence). Its approval status is **Pending Architecture Review
> Board (ARB) Audit**: it MUST NOT be connected to production compute, policy, retrieval or SLM engines until the
> ARB audit is closed **and** this banner is removed by the Owner. The reference build runs entirely against an
> in-process reference transport and a synthetic 550-identity corpus.

## 1. Metadata and governance

### 1.1 Manifest

| Field | Value |
|---|---|
| Skill name | `dag_orchestrator_skill` |
| **Owner** | **Sivanesh S.** |
| **Version** | **1.0.0** ([Semantic Versioning 2.0.0](https://semver.org)) |
| **Data Classification** | **Tier 1 (Highly Confidential / Enterprise IP)** |
| **Approval Status** | **Pending Architecture Review Board (ARB) Audit** |
| **Production connection** | **Prohibited until the ARB audit is closed** |
| Runtime | Python >= 3.11 (`asyncio.TaskGroup`), Pydantic v2; tests: pytest 8, pytest-asyncio 1.0 |
| Machine-readable manifest | `skill.json` (SLA budgets, tool endpoints, breaker / limiter settings, fallback topology, MCP resource registry) |

**Versioning policy.** MAJOR: breaking change to `DAGPlanSchema`, `OrchestratorSynthesis`, error contracts or tier
precedence. MINOR: new tool, new corpus tier, new guardrail class, new MCP resource. PATCH: bug fix with no schema
change. Every release MUST pass the full chaos suite (section 7) and the blocking gates (section 8) before tagging.

**ARB exit criteria** (all required before the banner may be removed): (1) ARB sign-off recorded by the Owner;
(2) every "Reference build" status in section 3 moved from Reference to Integrated and re-tested; (3) `pytest
evals/red_team_modes.py` green on the integrated build; (4) the residual-risk table (section 11) reviewed with no open
High item.

### 1.2 Architectural invariants (hard guardrails)

| # | Invariant | Enforced by |
|---|---|---|
| I1 | **Core immutability.** The skill never modifies `CoreComputeEngine`, `BaseWorkspaceContainer`, the canonical `PolicyRuleAST` grammar or the 50 Protected Baseline Rules. Engines are only *called* through MCP; no write tool exists | architecture; `MCPTransport` exposes calls only |
| I2 | **Corpus boundary.** Exactly **550 identities**: `1-50` `PROTECTED_50`, `51-250` `ACTIVE_200`, `251-550` `CANDIDATE_300`. Any id outside `1..550`, or any chunk lacking **both** a verified `postgres_doc_id` and a matching `vector_chunk_sha256`, is unverified commentary and is quarantined | `ProvenanceRecord` validators; `ProvenanceGate` in `tools/mcp_router.py` |
| I3 | **SLA split.** 3000 ms total = **2600 ms** execution ceiling + **400 ms** synthesis reserve | `compile_dag(budget_ms=2600)`; per-wave `asyncio.timeout` |
| I4 | **Shadow isolation.** `tool_slm_intent_shadow` is fire-and-forget with a hard 1500 ms ceiling; output goes only to `slm_shadow_telemetry` and never into a response | `MCPClientRouter.fork_shadow`; `ShadowAuditor` |
| I5 | **Epistemic precedence.** Tier 1 > Tier 2 > Tier 3 > Tier 4, never overridden by a lower tier | `EpistemicReconciler` |

### 1.3 Tier 1 (Enterprise IP) handling controls

| Control | Requirement | Enforced by |
|---|---|---|
| Least privilege | Engines are reached only via the router; the LLM host holds no engine credential or network path | router seam, egress allow-list (section 3.6) |
| Provenance | Every asserted policy carries a verified `ProvenanceRecord`; spoofed or out-of-bounds items are quarantined and recorded as `SecurityAnomaly` | `ProvenanceGate`, `provenance_ledger` |
| Non-disclosure of shadow data | SLM routes, confidence and latency never appear in any response | `ShadowAuditor` sink; `test_scenario5_*` |
| Auditability | Every synthesis returns `state_fingerprint`, provenance ledger, anomalies, conflict resolutions and degraded services | `OrchestratorSynthesis` |
| Immutability of contracts | All schemas are `frozen=True, extra="forbid"` | `tools/mcp_schemas.py` |

### 1.4 Service-level objectives (hard gates)

| SLO | Target | Enforcing check |
|---|---|---|
| End-to-end latency p50 | $p_{50} \le 1.4\,\text{s}$ | `test_sla_percentiles_under_concurrent_load` |
| End-to-end latency **p95** | $p_{95} \le 3.0\,\text{s}$ | `test_sla_percentiles_under_concurrent_load` |
| End-to-end latency p99 | $p_{99} \le 4.2\,\text{s}$ | `test_sla_percentiles_under_concurrent_load` |
| Deterministic routing coverage | $\ge 75\,\%$ | `test_calibration_quality_gates` |
| Selective accuracy (committed answers) | $\ge 85\,\%$ | `test_calibration_quality_gates` |
| Expected Calibration Error ($M=10$ bins) | $\mathrm{ECE} \le 0.10$ | `test_calibration_quality_gates` |
| Abstention threshold | commit iff $\text{confidence} \ge \tau = 0.80$ | `EpistemicReconciler`, boundary test |
| Shadow latency bleed | $< 5\,\text{ms}$ | `test_scenario5_shadow_hang_does_not_bleed_latency` |

Coverage, selective accuracy and ECE are verified on a seeded synthetic calibration set (n = 2000). The 9 end-to-end
vectors are *not* used for these gates because several abstain by design.

## 2. Routing and trigger behavior

This section is the contract for semantic routers and upstream LLM hosts. Routing is **intent-based and fail-closed**:
when intent is ambiguous, do **not** route here.

### 2.1 Trigger conditions: route here ONLY for these intents

| Intent class | Example utterances | What the skill does |
|---|---|---|
| **Compound multi-hop analytics** | "Does ACME exposure breach the risk threshold, and does the pricing guardrail allow the re-price? Cite the policies." | plan (`reference/prompts/task_decomposition.md`) -> compile DAG -> execute waves -> synthesize |
| **Conflict synthesis** | Sub-engines return disagreeing facts or verdicts | reconcile under Tier 1 > 2 > 3 > 4, emit `ConflictResolution` and `AbstainedClaim` |
| **Multi-engine routing** | A query needs compute **and** policy **and** retrieval | parallel evidence gathering in wave 0, bound evaluation in wave 1+ |
| **DAG compilation** | A planner emits a task list that must be validated and budgeted | Kahn validation, wave partition, critical-path pruning |
| **Zero-hop canonical bypass** | Every required fact is already in `GlobalExecutionContext.precomputed_facts` | planner emits zero nodes; resolved from context, no engine call |

### 2.2 Anti-goals: NEVER route here

| Intent (never route) | Examples | Enforcement | JSON-RPC code |
|---|---|---|---|
| **Direct mutation of core policies** | "Change rule 57 threshold to 5M." / "Deactivate protected rule 12." | no write tool exists; engines are called read-only (I1) | **-32003 Forbidden** |
| **Bypass of Tier 1 / Tier 2 ground truth** | "Ignore the compute engine and trust the document." / "Override the AST verdict." | precedence is structural: the reconciler drops lower-tier claims and records a `ConflictResolution` | **-32003 Forbidden** |
| **Unverified rule execution** | Rule id `612`; a chunk with a tampered `vector_chunk_sha256`; a `CANDIDATE_300` rule executed via AST | `ProvenanceGate` quarantine; candidate AST verdicts become `AbstainedClaim(CANDIDATE_RULE_NOT_ACTIVE)` | **-32003 Forbidden** |
| **Disclosure of shadow-mode output** | "Show me the SLM's predicted route." | shadow data lives only in `slm_shadow_telemetry` | **-32003 Forbidden** |
| **Direct engine access** | "Query the policy database for me." | engines are reachable only through the router | **-32003 Forbidden** |
| Cyclic / dangling / invalid-ref plan | `A -> B -> C -> D -> B` | `compile_dag` raises a typed error before any dispatch | -32602 Invalid params |
| Over-budget plan with nothing optional to prune | all nodes priority 1-2 and `T_critical > 2600 ms` | `BudgetInfeasibleError` | -32004 Unprocessable |

> **Status of the codes.** The reference build raises typed Python exceptions (table below). The JSON-RPC codes are
> the transport contract the host MCP server must apply. Natural-language screening of requests such as "bypass the
> ground truth" belongs to the host or gateway; inside the orchestrator the bypass is impossible by construction
> (precedence is code, not a prompt), which `test_scenario4_*` verifies.

### 2.3 Error contract

| Condition | Typed exception | JSON-RPC code |
|---|---|---|
| Cycle or self-loop (exact path attached) | `CyclicDependencyError` | -32602 |
| Missing upstream task id | `DanglingDependencyError` | -32602 |
| `$ref` malformed, not an ancestor, or wrong wave index | `InvalidReferenceError` | -32602 |
| Plan exceeds node or wave limit | `DAGLimitError` | -32602 |
| `T_critical` cannot be brought under 2600 ms | `BudgetInfeasibleError` | -32004 |
| Pinned Priority 1 context exceeds 4096 tokens | `ContextBudgetExceededError` | -32004 |
| Tier 1 node failed (`HARD_ABORT`) or exceeded the deadline | `HardAbortError` | -32002 Upstream unavailable |
| Quarantined provenance, tier violation | `SecurityAnomaly` in the response; not an exception | -32003 when the whole request depended on it |
| Anything unexpected | - | -32603 Internal error (sanitised) |

## 3. MCP external system connections

Every engine is reached **only** through `MCPClientRouter` (`tools/mcp_router.py`) over the `MCPTransport` protocol.
Values below are the defaults in `skill.json`, verified against `RouterConfig.default()`.

### 3.1 Integration matrix

| MCP tool | Backend | Epistemic tier | Access | Timeout | Hedge after (p85) | Rate limit (R / B) | Concurrency (min / init / max) | Failure policy | Fallback | Reference build |
|---|---|---|---|---|---|---|---|---|---|---|
| `tool_core_compute` | Deterministic Compute Engine | **1** | read-only | 800 ms | 450 ms | 200/s, 64 | 2 / 6 / 16 | `HARD_ABORT` | none (abort) | in-process handler |
| `tool_policy_ast` | Protected Symbolic Policy AST engine | **2** | read-only evaluation | 600 ms | 400 ms | 200/s, 64 | 2 / 6 / 16 | `DEGRADE_WITH_FALLBACK` | none (node reported degraded) | in-process predicate evaluator |
| `tool_hybrid_rag` | Qdrant (vectors) + PostgreSQL (documents) | **3** | read-only retrieval | 900 ms | 750 ms | 50/s, 20 | 1 / 4 / 12 | `DEGRADE_WITH_FALLBACK` | `tool_policy_ast` over `PROTECTED_50` | in-process chunk replay + `CorpusRegistry.synthetic()` |
| `tool_slm_intent_shadow` | local SLM / vLLM | none (shadow) | non-blocking, isolated | **1500 ms hard** | none | 100/s, 32 | 1 / 4 / 8 | `IGNORE_OPTIONAL` | none | in-process echo handler |

Common resilience settings (per tool): circuit breaker trips on 5 consecutive failures, a failure rate above 40 % over a
30 s window (minimum 10 samples) or $L_{\text{EWMA}} > 2200\,\text{ms}$ ($\alpha = 0.3$); recovery uses full-jitter
backoff $t = \mathrm{uniform}(0, \min(15\,\text{s}, 0.5\,\text{s} \cdot 2^{n}))$ and HALF_OPEN probes. The adaptive limiter
applies Vegas/AIMD with $\gamma_{low} = 0.15$, $\gamma_{high} = 0.35$, $\beta = 0.75$.

### 3.2 `tool_core_compute`: Deterministic Compute Engine (Tier 1)

| Aspect | Specification |
|---|---|
| Data | Read-only transactional state and time-series metrics: real-time exposure, aggregations, quantitative risk and financial calculations |
| Authority | **Ground truth.** Overrides every other tier unconditionally; its facts are pinned (Priority 1) in the context window |
| Failure | `HARD_ABORT`: there is no substitute for Tier 1 state, so an outage aborts the orchestration (`HardAbortError`) instead of degrading |
| Idempotency | Idempotent; eligible for hedging and memoisation by `state_fingerprint` |

### 3.3 `tool_policy_ast`: Protected Symbolic Policy ASTs (Tier 2)

| Aspect | Specification |
|---|---|
| Data | Deterministic boolean/symbolic evaluation of the **50 Protected Baseline Rules** (`PROTECTED_50`) and the **200 active identities** (`ACTIVE_200`) with predicates of the form `{"binding", "op", "value"}` |
| Authority | Overrides unstructured retrieval; a Tier 3 claim that disagrees with a Tier 2 verdict on the same rule is dropped and recorded as a `ConflictResolution` |
| Candidate rules | A `CANDIDATE_300` rule evaluated here is **not executable**: it becomes `AbstainedClaim(CANDIDATE_RULE_NOT_ACTIVE)` |
| Provenance | Each evaluation carries `postgres_doc_id` + `vector_chunk_sha256`; the gate compares the digest to the registered corpus digest |
| Degrade path | Also the fallback target for `tool_hybrid_rag`, restricted to `PROTECTED_50` baseline rules |

### 3.4 `tool_hybrid_rag`: Vector / Graph retrieval (Tier 3)

| Aspect | Specification |
|---|---|
| Backends | **Qdrant** for chunk vectors and the `vector_chunk_sha256` digest; **PostgreSQL** for `postgres_doc_id` and the canonical document |
| Corpus | The **7 Enterprise Policy Families**: Regulatory & Statutory Compliance; Quantitative Risk & Exposure Thresholds; SLA & Operational Resilience Governance; Identity, Security & Access Control; Financial Controls & Pricing Guardrails; Data Lineage, Retention & Privacy; Incident Escalation & Remediation Workflows |
| Provenance requirement | A chunk is assertable **only** with a verified `postgres_doc_id` **and** a matching `vector_chunk_sha256`. Neither alone is sufficient |
| Abstention | `confidence < 0.80` becomes an `AbstainedClaim`; exactly `0.80` commits |

Provenance gate, in order (first failure quarantines the chunk and records a `SecurityAnomaly`):

| Step | Check | Anomaly kind |
|---|---|---|
| 1 | `rule_identity_id` is an integer | `SCHEMA_VIOLATION` |
| 2 | `1 <= rule_identity_id <= 550` | `OUT_OF_BOUNDS_RULE_IDENTITY` |
| 3 | `postgres_doc_id` present | `MISSING_DOC_ID` |
| 4 | `vector_chunk_sha256` is 64 lowercase hex | `MALFORMED_DIGEST` |
| 5 | `SHA-256(chunk text)` equals the claimed digest | `HASH_MISMATCH` |
| 6 | doc id is registered for that rule identity | `UNREGISTERED_DOC` |
| 7 | digest equals the registered corpus digest | `DIGEST_MISMATCH` |
| 8 | claimed tier and policy family match the registry | `TIER_MISMATCH` / `FAMILY_MISMATCH` |

### 3.5 `tool_slm_intent_shadow`: Shadow-mode SLM

| Aspect | Specification |
|---|---|
| Backend | A local SLM served by vLLM (isolated network segment; no access to canonical state) |
| Mode | **Non-blocking fire-and-forget**: forked with `asyncio.create_task`, never awaited by the critical path, protected by `asyncio.shield` with a hard **1500 ms** timeout |
| Output | Route prediction, confidence, abstention flag and latency go **only** to `slm_shadow_telemetry` (`ShadowAuditor`): agreement rate, latency delta, fault counts, ECE bins |
| Isolation | Faults (`RuntimeError`, timeout, malformed response, cancellation) are recorded, never raised; the shadow call bypasses breaker and limiter |

### 3.6 Fallback topology and egress allow-list

| Failed tool | Fallback | Result status |
|---|---|---|
| `tool_hybrid_rag` (timeout, error, breaker OPEN) | `tool_policy_ast` over `node.payload.baseline_rules` with `scope: PROTECTED_50` | `degraded`; `tool_hybrid_rag` listed in `degraded_services` |
| `tool_policy_ast` | none | `failed` / `skipped` per policy; degraded service reported |
| `tool_core_compute` | none | `HardAbortError` |
| `tool_slm_intent_shadow` | none | fault recorded in `slm_shadow_telemetry` |

| From | To | Purpose |
|---|---|---|
| Orchestrator | Compute engine, policy AST engine, Qdrant, PostgreSQL, local vLLM | the only permitted egress |
| Orchestrator | everything else | **denied** |

## 4. Execution pipeline

| Stage | Behavior | Module |
|---|---|---|
| 1. Fingerprint | SHA-256 over normalised, key-sorted `tenant_id`, `entity_id`, `as_of_timestamp` (UTC), `policy_version`, `query_signature` | `scripts/state_manager.py` |
| 2. Shadow fork | `tool_slm_intent_shadow` forked, never awaited | `tools/mcp_router.py` |
| 3. Zero-hop | Empty plan resolves from `GlobalExecutionContext`; missing required keys become `AbstainedClaim(MISSING_CANONICAL_FACT)` | `tools/parallel_executor.py` |
| 4. Compile | Kahn's algorithm, wave partition $\text{wave}(u) = 1 + \max_{v \in \text{deps}(u)} \text{wave}(v)$, $T_{\text{critical}} = \sum_w \max_{u \in W_w} \hat{L}(u)$, budget pruning to $\le 2600\,\text{ms}$ | `scripts/dag_compiler.py` |
| 5. Execute waves | One `asyncio.TaskGroup` per wave; node wrappers catch `Exception` only; `$ref` resolved between waves; memoised by fingerprint; global 2600 ms deadline | `tools/parallel_executor.py` |
| 6. Dispatch | token bucket -> breaker -> adaptive slot -> hedged call under timeout -> provenance gate | `tools/mcp_router.py`, `scripts/circuit_breaker.py`, `scripts/concurrency_controller.py` |
| 7. Reconcile | Tier 1 > 2 > 3 > 4; contradictions resolved in favour of the higher tier; abstain below $\tau = 0.80$ | `EpistemicReconciler` |
| 8. Compress | Tiered 0/1 knapsack into $W_{\max} = 4096$ tokens: P1 pinned, P2 structurally compressed then DP-selected, P3 FIFO-evicted | `scripts/state_manager.py` |
| 9. Emit | Validated `OrchestratorSynthesis` with `TelemetrySummary` (p50/p95/p99, slack, SLA flag) | `tools/mcp_schemas.py`, `scripts/telemetry_logger.py` |

Budget pruning removes or downgrades only nodes with `priority >= 3` (never `HARD_ABORT`) whose descendants are all
optional: first to the cached fast path (`fast_path_latency_ms`), otherwise removal.

## 5. Reference folder contract

All operational doctrine and prompt engineering live **only** in `reference/`. `Skill.md` points at these files; it
does not restate them.

| File | Role | MCP resource URI (read-only) |
|---|---|---|
| `reference/guidelines.md` | **4-Tier Epistemic Hierarchy** doctrine: invariants, tier precedence, abstention triggers, zero-hop and diamond-DAG decomposition, failure policy, context budget | `orchestrator://reference/guidelines` |
| `reference/prompts/task_decomposition.md` | System prompt of the DAG planner: `DAGPlanSchema` contract, `$ref` rules, priority and budget rules, 3 few-shot DAGs (zero-hop, diamond, over-budget) | `orchestrator://reference/prompts/task_decomposition` |
| `reference/prompts/conflict_synthesis.md` | System prompt of the epistemic synthesizer: precedence procedure, mandatory provenance citations, abstention at confidence < 0.80 | `orchestrator://reference/prompts/conflict_synthesis` |

### 5.1 Exposure as MCP resources

| Property | Contract |
|---|---|
| Registry | `skill.json` -> `mcp_resources` maps each URI to its file with `mime_type: text/markdown` and `read_only: true` |
| Access | Read-only. No write path and no resource template; URIs resolve only to the three registered files |
| Serving | The host MCP server loads the registry and serves `resources/read`. **The reference build ships the registry but no MCP server process** (see section 11) |

```json
{ "jsonrpc": "2.0", "id": 4, "method": "resources/read", "params": { "uri": "orchestrator://reference/guidelines" } }
```

### 5.2 Authority and change control

| Rule | Detail |
|---|---|
| Precedence | If `reference/guidelines.md` and a code comment disagree, the doctrine states the intended behavior and the code is the bug |
| Prompt coupling | The planner prompt must stay consistent with `SubTaskNode` and `DAGPlanSchema`; a schema change is a MAJOR release and updates the prompt in the same change |
| Release gate | Editing any `reference/` file is a MINOR or PATCH release and requires `pytest evals/red_team_modes.py` green |

## 6. Data contracts (`tools/mcp_schemas.py`)

All models use `ConfigDict(frozen=True, extra="forbid")`.

| Model | Key fields and constraints |
|---|---|
| `ProvenanceRecord` | `postgres_doc_id: int`, `vector_chunk_sha256` (64-hex), `corpus_tier` in {`PROTECTED_50`, `ACTIVE_200`, `CANDIDATE_300`}, `policy_family_id` 1-7, `rule_identity_id` 1-550; the tier must match the id range |
| `SubTaskNode` | `task_id`, `tool: MCPToolType`, `dependencies`, `timeout_ms`, `idempotent`, `priority` 1-5, `failure_policy`, `estimated_latency_ms`, `payload`, optional `fast_path_latency_ms`; self- and duplicate dependencies rejected |
| `DAGPlanSchema` | `request_id`, `zero_hop`, `rationale`, `nodes`; unique ids; a zero-hop plan has no nodes |
| `ToolResult` / `FallbackEnvelope` | status `ok` / `degraded` / `skipped` / `failed`, verified provenance, anomalies, typed fallback record |
| `AbstainedClaim` | `claim_id`, `reason`, `confidence`, `rule_identity_id`, tier 4 |
| `OrchestratorSynthesis` | `request_id`, `state_fingerprint`, `canonical_facts`, `triggered_policies`, `provenance_ledger`, `abstained_claims`, `degraded_services`, `telemetry_summary`, plus `conflict_resolutions` and `security_anomalies` |

`$ref` grammar: `$ref:$.waves.<wave_idx>.<task_id>.output[.<json_path>][|default=<json>]`. The target must be an
ancestor and `<wave_idx>` must equal its compiled wave. A pruned or failed upstream resolves to its `|default=` value.

## 7. Cybersecurity, red-team and chaos test vectors

The five scenarios below are the minimum adversarial acceptance suite, implemented in `evals/red_team_modes.py`
(`pytest evals/red_team_modes.py -v`). Timing assertions use medians and explicit margins so they hold on Windows
timer granularity (about 15 ms).

### Scenario 1: Cyclic dependency and diamond deadlock injection
- **Attack.** A planner (or an attacker controlling it) emits `A -> B -> C -> D -> B`, a self-loop, a dangling dependency, a `$ref` to a sibling, or a `$ref` with the wrong wave index.
- **Expected behavior.** `CyclicDependencyError` with the exact path `B -> C -> D -> B` in **under 2 ms**; `DanglingDependencyError` / `InvalidReferenceError` for the others. Nothing is dispatched (zero engine calls). Over-budget plans prune only priority >= 3 nodes; if nothing optional remains, `BudgetInfeasibleError`.
- **Controls.** Kahn's algorithm with residual-graph cycle extraction; `$ref` ancestor and wave validation; priority-safe pruning.
- **Tests.** `test_scenario1_cycle_detected_under_2ms`, `test_scenario1_ref_must_target_a_valid_ancestor`, `test_scenario1_budget_pruning_is_priority_safe`.

### Scenario 2: The 60-subtask context bomb (token and concurrency flood)
- **Attack.** An adversarial query spawns 60 independent evaluations (6 entities x 10 historical windows) plus 300 turns of history to exhaust the context window and the connection pool.
- **Expected behavior.** Active coroutines are bounded by the adaptive limiter (`peak_in_flight <= 8` in the test configuration, no pool starvation); all 60 results succeed; the context compresses to **$\le 4096$ tokens** with **100 % of Priority 1 facts retained**; history is evicted FIFO (the survivors are a contiguous newest suffix). Total time under 3000 ms.
- **Controls.** Token bucket, Vegas/AIMD limiter, tiered 0/1 knapsack, `ContextBudgetExceededError` if pinned content alone cannot fit.
- **Tests.** `test_scenario2_context_bomb_is_bounded_and_compressed`, `test_scenario2_compressor_preserves_priority1_within_window`.

### Scenario 3: Vector RAG brownout and tail-latency straggler (5.0 s delay)
- **Attack.** `tool_hybrid_rag` (primary **and** replica) is delayed by 5000 ms.
- **Expected behavior.** Hedged requests fire at the 750 ms p85 threshold; the 900 ms per-node timeout cuts execution off cleanly; five consecutive failures trip the breaker to **OPEN**; later requests short-circuit instantly and fall back to the **50 Protected Baseline AST rules**. Total orchestration latency **under 3000 ms**; no zombie tasks; `tool_hybrid_rag` reported in `degraded_services`.
- **Controls.** `execute_with_hedge` (loser cancelled and drained, never counted as a failure), per-node timeout, EWMA/consecutive-failure breaker with full-jitter backoff, fallback topology.
- **Tests.** `test_scenario3_rag_brownout_trips_breaker_and_falls_back`, `test_scenario3_single_request_with_5s_straggler_stays_within_sla`, `test_scenario3_ewma_latency_trips_breaker_and_half_open_recovers`, `test_hedged_request_wins_against_a_straggling_primary`, `test_non_idempotent_calls_are_never_hedged`.

### Scenario 4: Epistemic contradiction and spoofed provenance attack
- **Attack.** A rogue RAG payload contradicts Tier 1 compute metrics and carries a **tampered `vector_chunk_sha256`**, tampered chunk text, a foreign `postgres_doc_id`, or an out-of-bounds `rule_identity_id` (612).
- **Expected behavior.** Every spoofed chunk is quarantined (`HASH_MISMATCH`, `UNREGISTERED_DOC`, `OUT_OF_BOUNDS_RULE_IDENTITY`) and recorded as a `SecurityAnomaly`; none reaches `triggered_policies` or `provenance_ledger`. A *verified* chunk that contradicts Tier 1 is dropped with a `ConflictResolution` (winning tier 1). Tier 1 facts and Tier 2 verdicts stand.
- **Controls.** `ProvenanceGate` (recomputed SHA-256, registry digest, corpus bounds), `EpistemicReconciler`.
- **Tests.** `test_scenario4_spoofed_provenance_is_quarantined`; vectors `case_4_*` and `case_6_*` in `test_cases.json`.

### Scenario 5: Shadow-mode crash and latency isolation
- **Attack.** `tool_slm_intent_shadow` raises an unhandled `RuntimeError`, or hangs for 10 s.
- **Expected behavior.** **Zero user-facing impact** (the synthesis is identical to a run without the shadow, and contains no shadow fields); **zero main-path latency bleed** (fork under 5 ms, median overhead under 5 ms); the fault (`RuntimeError`, or `timeout` at the 1500 ms ceiling) is recorded in `slm_shadow_telemetry`; no leaked tasks after drain.
- **Controls.** `fork_shadow` (create_task + shield + hard timeout, inner call cancelled and drained), `ShadowAuditor` never raises.
- **Tests.** `test_scenario5_shadow_crash_has_zero_user_impact`, `test_scenario5_shadow_hang_does_not_bleed_latency`.

### Vector coverage summary

| Scenario | Primary control | Release-blocking gate condition |
|---|---|---|
| 1 Cycle / deadlock | Kahn validation | any cyclic plan accepted, or detection >= 2 ms |
| 2 Context bomb | limiter + knapsack | tokens > 4096, any Priority 1 fact lost, concurrency above the limit |
| 3 RAG brownout | hedge + timeout + breaker + fallback | latency >= 3000 ms, breaker not OPEN, no baseline fallback |
| 4 Spoofed provenance | provenance gate + precedence | any spoofed identity in policies or ledger, Tier 1 overridden |
| 5 Shadow fault | isolation | any user-visible change, bleed >= 5 ms, fault not recorded |

### 7.1 End-to-end vectors (`evals/test_cases.json`)

| Vector | Verifies |
|---|---|
| `case_1_zero_hop_canonical_bypass` | no engine dispatched; facts from context |
| `case_2_three_wave_diamond_families_1_2_5` | 3 waves, `$ref` across waves, Policy Families 1, 2 and 5 covered |
| `case_3_over_budget_pruning` | $T_{\text{critical}}$ 3450 ms -> 2400 ms; priority 5 node pruned, priority 4 node downgraded |
| `case_4_tier1_compute_vs_tier3_rag_contradiction` | Tier 1 wins, `ConflictResolution` recorded |
| `case_5_candidate_rule_low_confidence_abstention` | `CANDIDATE_300` rule at 0.68 -> `AbstainedClaim` |
| `case_6_out_of_bounds_rule_identity_612` | identity 612 quarantined, legitimate chunk honoured |
| `case_7_cyclic_plan_rejected` | exact cycle path, zero dispatches |
| `case_8_core_compute_outage_hard_abort` | `HardAbortError`, no degradation of Tier 1 |
| `case_9_optional_node_failure_uses_ref_default` | optional failure falls back to its declared default and reports the degraded service |

## 8. Evaluation and release gates

Composite score $S = 0.25 D_1 + 0.25 D_2 + 0.20 D_3 + 0.15 D_4 + 0.15 D_5$ (`evals/rubric.md`). Releasable only if
$S \ge 90$ **and** every blocking gate passes; a gate failure caps $S$ at 59.

| Dimension | Weight | Evidence |
|---|---|---|
| D1 DAG topological and routing optimality | 25 % | wave speed-up $S_{\text{wave}} = \sum \hat{L}(u) / T_{\text{critical}}$, zero redundant invocations, 100 % cycle rejection |
| D2 Hard SLA latency compliance | 25 % | p50/p95/p99, pruning accuracy, shadow bleed |
| D3 Epistemic and provenance integrity | 20 % | precedence, zero leakage of identities > 550, 100 % SHA-256 verification |
| D4 Selective accuracy and calibration | 15 % | coverage, selective accuracy, ECE, Brier score |
| D5 Chaos and fault resilience | 15 % | zero unhandled exceptions, zero zombie tasks |

| Blocking gate | Condition |
|---|---|
| G1 | cyclic or dangling plans always rejected with the exact path |
| G2 | $p_{95} \le 3000\,\text{ms}$ and $p_{99} \le 4200\,\text{ms}$ under load |
| G3 | no unverified or out-of-bounds identity in `triggered_policies` or `provenance_ledger` |
| G4 | shadow latency bleed $< 5\,\text{ms}$, no shadow field in any response |
| G5 | zero unhandled exceptions in the chaos suite |
| G6 | Tier 1 values never overridden |

## 9. Configuration (`skill.json`)

| Key | Content |
|---|---|
| `sla`, `quality_gates` | 3000 / 2600 / 400 ms split; p50/p95/p99; coverage, accuracy, ECE, `tau_abstain` |
| `corpus` | 550-identity map, 7 policy families |
| `dag_limits` | `max_nodes` 64, `max_waves` 6, pruning threshold priority 3 |
| `context` | `w_max_tokens` 4096, tokenizer `ceil(chars/4)` |
| `shadow` | tool, 1500 ms timeout, sink `slm_shadow_telemetry`, `user_visible: false` |
| `tools` | per-tool endpoint, tier, timeout, hedge threshold, rate limit, concurrency, breaker |
| `fallback_topology` | RAG -> AST (`PROTECTED_50`) and the degrade flags |
| `prompts`, `doctrine`, `mcp_resources` | paths under `reference/` and the MCP resource registry |

`RouterConfig.from_skill_json("skill.json")` must equal `RouterConfig.default()` (`test_skill_manifest_matches_router_defaults`).

## 10. Package layout

```
dag_orchestrator_skill/
|-- Skill.md                            this manifest (governance, routing, MCP matrix, red-team vectors)
|-- skill.json                          machine-readable manifest: SLA, tools, limits, fallback, MCP resource registry
|-- reference/                          operational doctrine and prompt engineering (read-only MCP resources)
|   |-- guidelines.md                   4-tier epistemic hierarchy doctrine          -> orchestrator://reference/guidelines
|   `-- prompts/
|       |-- task_decomposition.md       DAG planner system prompt                    -> orchestrator://reference/prompts/task_decomposition
|       `-- conflict_synthesis.md       epistemic synthesizer system prompt          -> orchestrator://reference/prompts/conflict_synthesis
|-- scripts/                            distributed-systems infrastructure
|   |-- __init__.py
|   |-- dag_compiler.py                 Kahn wave compiler, cycle detector, critical-path pruner
|   |-- circuit_breaker.py              3-state breaker, EWMA latency drift, full-jitter backoff
|   |-- concurrency_controller.py       Vegas/AIMD limiter, token bucket, hedged requests
|   |-- state_manager.py                SHA-256 fingerprint, $ref resolver, knapsack token compressor
|   `-- telemetry_logger.py             ring buffer percentiles, trace spans, ECE/Brier, shadow auditor
|-- tools/                              contracts and execution
|   |-- __init__.py
|   |-- mcp_schemas.py                  frozen Pydantic v2 contracts and provenance models
|   |-- mcp_router.py                   MCP client router, provenance gate, shadow fork, reference transport
|   `-- parallel_executor.py            wave executor, epistemic reconciler, synthesis
|-- evals/                              grading and adversarial suite
|   |-- rubric.md                       weighted rubric and blocking gates
|   |-- test_cases.json                 9 end-to-end vectors
|   |-- judges.py                       CI/CD harness: rubric scoring -> report.md, red-team gate, exit codes 0/1/2
|   `-- red_team_modes.py               pytest/asyncio chaos and red-team suite (29 tests)
`-- features/
    `-- arch-delegate-ai-subtask.md     original build plan
```

### Quick start

```bash
pip install pydantic pytest pytest-asyncio            # no other dependencies
pytest evals/red_team_modes.py -v                     # chaos scenarios, vectors, SLA and calibration gates
python evals/judges.py --offline --red-team            # CI gate: exit 0 pass, 1 functional failure, 2 security failure
```

## 11. Known limitations and residual risk (for the ARB audit)

| Item | Severity | Status / mitigation |
|---|---|---|
| Production connection prohibited | - | **Pending ARB Audit**: not authorised for production engines or data |
| Engines are an in-process reference transport; Qdrant, PostgreSQL and vLLM are the production contract | High | Implement `MCPTransport` for the real clients and re-run the suite on the integrated build |
| Corpus is synthetic (`CorpusRegistry.synthetic()`), 550 deterministic rules | High | Load the real registry of doc ids and digests; re-validate the provenance gate |
| Digest covers chunk text only, not the `claims` metadata | High | Include claim fields in the digested text in the production corpus |
| `reference/` resources are declared in `skill.json` but no MCP server process ships in this build | Medium | The host MCP server must implement `resources/read` against the registry |
| JSON-RPC codes are a transport contract; the build raises typed Python exceptions | Medium | Map exceptions to codes in the host server (section 2.3) |
| No natural-language screening of "bypass the ground truth" requests | Medium | Precedence is structural; add gateway screening before production |
| Shadow `canonical_latency_ms` is the elapsed time when the shadow finishes | Low | Documented approximation in `slm_shadow_telemetry` |
| Tokenizer is `ceil(chars/4)` | Low | Pluggable `Tokenizer`; use the production model tokenizer |
| Timing assertions depend on host timer granularity (Windows about 15 ms) | Low | Medians and margins used; run on target hardware before release |
