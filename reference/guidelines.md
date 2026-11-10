# `dag_orchestrator_skill` — Master Operational Doctrine

This document is the deterministic operating doctrine for the orchestration skill. It is normative:
the code in `scripts/` and `tools/` implements it, and `evals/` verifies it.

## 0. Architectural invariants (hard guardrails)

1. **Core immutability.** The orchestrator never modifies `CoreComputeEngine`, `BaseWorkspaceContainer`
   (UI state store), the canonical `PolicyRuleAST` grammar, or the **50 Protected Baseline Compliance Rules**.
   It only *calls* them through MCP tools.
2. **Corpus boundary.** The verified policy corpus is exactly **550 identities**:
   `1–50` `PROTECTED_50`, `51–250` `ACTIVE_200`, `251–550` `CANDIDATE_300`
   (200 active + 300 staging = 500, plus 50 protected baseline).
   Any `rule_identity_id` outside `1..550`, or any retrieved chunk lacking **both** a verified
   `postgres_doc_id` **and** a matching `vector_chunk_sha256`, is *unverified commentary*: it is quarantined,
   logged as a `SecurityAnomaly`, and never reaches deterministic execution.
3. **SLA gates.** End-to-end: p50 ≤ 1400 ms, p95 ≤ 3000 ms, p99 ≤ 4200 ms. Budget split:
   3000 ms total = 2600 ms execution ceiling + 400 ms synthesis reserve.
   Deterministic routing coverage ≥ 75 %, selective accuracy ≥ 85 %, ECE ≤ 0.10.
4. **Shadow isolation.** `tool_slm_intent_shadow` is fire-and-forget with a hard 1500 ms ceiling. Its predictions,
   latency and confidence go **only** to `slm_shadow_telemetry`. It must never block the critical path,
   mutate canonical state, or appear in a user-visible response.

## 1. The 4-tier epistemic hierarchy

When sub-agents return conflicting data or partial failures, precedence is strict: **Tier 1 > Tier 2 > Tier 3 > Tier 4**.

| Tier | Source | Authority | Conflict behaviour |
|------|--------|-----------|--------------------|
| 1 | `tool_core_compute` (and pre-computed `GlobalExecutionContext` facts) | Authoritative transactional state, quantitative risk/financial calculations, exact time-series aggregations | Overrides every other tier unconditionally |
| 2 | `tool_policy_ast` evaluating the 50 Protected Baseline Rules and the 200 Active identities | Deterministic boolean/symbolic AST evaluation | Overrides unstructured text retrieval |
| 3 | `tool_hybrid_rag` passages with verified `postgres_doc_id` **and** matching `vector_chunk_sha256` | Cryptographically verified evidence | Yields to Tiers 1 and 2; must carry provenance |
| 4 | Abstention / unverified boundary | Nothing is asserted | Emit a structured `AbstainedClaim` |

### Tier 3 policy families

Every Tier 3 passage is categorised under one of the 7 Enterprise Policy Families:

1. Regulatory & Statutory Compliance
2. Quantitative Risk & Exposure Thresholds
3. SLA & Operational Resilience Governance
4. Identity, Security & Access Control
5. Financial Controls & Pricing Guardrails
6. Data Lineage, Retention & Privacy
7. Incident Escalation & Remediation Workflows

### Tier 4 triggers (abstention)

An `AbstainedClaim` replaces the claim whenever any of the following holds. The orchestrator never
extrapolates or hallucinates a substitute value.

* `confidence < τ_abstain = 0.80` (strict inequality; exactly `0.80` commits).
* The claim comes from a `CANDIDATE_300` rule evaluated through `tool_policy_ast` (staging rules are not executable).
* Provenance is corrupted (hash mismatch, unregistered `postgres_doc_id`, digest mismatch).
* `rule_identity_id` is outside the 550-identity boundary.

### Resolution procedure

1. Verify provenance of every Tier 2/3 item (router gate). Failures → quarantine + anomaly. Never promoted.
2. Collect Tier 1 facts. They are never overwritten.
3. Evaluate Tier 2 verdicts. Staging-tier or low-confidence verdicts → Tier 4.
4. For each Tier 3 claim: if confidence < 0.80 → Tier 4. If it contradicts a Tier 1 fact (numeric tolerance
   `|a-b| ≤ max(1e-9, 1e-6·|a|)`) or disagrees with a Tier 2 verdict on the same rule → record a
   `ConflictResolution` and **drop the lower tier**. Otherwise it may be asserted, with citation.
5. Every asserted policy carries a `ProvenanceRecord`; every record is deduplicated into `provenance_ledger`.

## 2. Decomposition doctrine

### 2.1 Zero-hop canonical bypass

If every fact required by the query is already present in `GlobalExecutionContext.precomputed_facts` for the
current `state_fingerprint` (`tenant_id`, `entity_id`, `as_of_timestamp`, `policy_version`, `query_signature`),
the planner emits **zero nodes**. The executor resolves from context in O(1): no RAG, no AST, no compute. Heavy
execution on already-known deterministic facts is a rubric violation (redundant tool invocation).

### 2.2 Multi-wave diamond DAGs

```
W0:  tool_core_compute (real-time exposure)   ||   tool_hybrid_rag (Policy Family rules)
            \                                      /
W1:          tool_policy_ast  (bound symbolic predicates; $ref inputs from W0 outputs)
                               |
W2:          Deterministic Conflict Synthesizer (Tier 1 > 2 > 3 > 4; not a tool node)
```

* `wave(u) = 0` if `deps(u) = ∅`, else `1 + max wave(v)` over dependencies.
* A node may only `$ref` outputs of its **ancestors**, and the wave index in the `$ref` must match the
  ancestor's compiled wave: `$ref:$.waves.<wave_idx>.<task_id>.output.<json_path>`.
* Independent evidence gathering is **always** parallel in the same wave. Sequential chains are only for true data dependence.

### 2.3 Budget and pruning

`T_critical = Σ_w max_{u∈W_w} L̂(u)`. If `T_critical > 2600 ms` the compiler iteratively
(a) downgrades optional nodes to their cached fast path, or (b) prunes them, in order of lowest value
(highest priority number, then largest latency), only for nodes with `priority ≥ 3` whose descendants are all
optional too. Priority 1–2 nodes and `HARD_ABORT` nodes are never pruned. If the budget cannot be met,
compilation fails with `BudgetInfeasibleError` rather than silently breaching the SLA.

## 3. Failure policy

| Policy | On failure |
|--------|-----------|
| `HARD_ABORT` | The whole orchestration aborts (`HardAbortError`). Used for Tier 1 compute that has no substitute. |
| `DEGRADE_WITH_FALLBACK` | Route through `fallback_topology` (e.g. `tool_hybrid_rag` → `tool_policy_ast` over the 50 protected baseline rules), mark the service degraded. |
| `IGNORE_OPTIONAL` | Record a skipped result; downstream `$ref`s use their `\|default=` value. |

Open circuit breakers short-circuit instantly to a typed `FallbackEnvelope`. Hedge-loser cancellation is
never counted as a breaker failure.

## 4. Context budget (W_max = 4096 tokens)

* **Priority 1 (pinned, never truncated):** Tier 1 telemetry, active entity state, protected-rule outputs. If they alone exceed the window the request fails loudly (`ContextBudgetExceededError`).
* **Priority 2 (structural compression):** Tier 3 passages are stripped of prose and markdown; symbolic predicates, numeric thresholds and provenance hashes are preserved verbatim, then selected by 0/1 knapsack.
* **Priority 3 (FIFO eviction first):** historical turns, oldest evicted first.

## 5. Observability

Every span is timed with `time.perf_counter_ns()`. Latency ring buffers (capacity 10 000) feed p50/p90/p95/p99
and `Slack_ms = 3000 − T_elapsed_ms`. Calibration is tracked with 10-bin ECE and Brier score. Shadow
comparisons go to `slm_shadow_telemetry` only.
