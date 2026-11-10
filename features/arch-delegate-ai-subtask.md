# Plan: `dag_orchestrator_skill` orchestration skill (15 files)

## Context
Greenfield build (`C:\Users\user\Documents\dag_orchestrator_skill` is empty). The user wants the full
orchestration skill for an enterprise multi-agent decision platform: DAG compilation, SLA budgets,
resilient MCP dispatch, isolated SLM shadow telemetry, provenance-gated epistemic synthesis, plus an
eval rubric, test vectors and an executable chaos suite. Toolchain verified: Python 3.11.9,
pydantic 2.11.7, pytest 8.4.0, pytest-asyncio 1.0.0 (`asyncio.TaskGroup` available). No extra deps.

## Key design decisions
- **15 deliverable files + 2 empty support files.** Verified that `scripts` already resolves to a
  site-packages namespace portion (`pywin32\win32\scripts`), so `scripts/__init__.py` and
  `tools/__init__.py` (empty) are added to make the local regular packages win. `red_team_modes.py`
  inserts the repo root at `sys.path[0]`; no pyproject needed.
- **pytest-asyncio 1.0 strict mode:** explicit `@pytest.mark.asyncio`, async fixtures via
  `@pytest_asyncio.fixture`, no `event_loop` fixture; fixture teardown calls `router.drain_shadow()`
  so the 10 s hanging shadow task leaves no pending-task warnings.
- **Transport abstraction, no fake "real" servers.** `mcp_router.py` defines an `MCPTransport`
  `Protocol` (`async call(tool, payload, timeout_ms) -> dict`) plus `InProcessMCPTransport`, a
  deterministic reference transport with injectable latency/fault hooks (used by evals). A real MCP SDK
  client plugs in behind the same Protocol; the immutable `CoreComputeEngine` is only ever called, never
  modified.
- **Corpus identity map (550):** 1–50 `PROTECTED_50`, 51–250 `ACTIVE_200`, 251–550 `CANDIDATE_300`.
  `ProvenanceRecord` validator rejects id/tier mismatches; router additionally recomputes
  `sha256(normalized chunk text)` and checks `postgres_doc_id` against a `CorpusRegistry`.
  Failures → quarantine + `security_anomalies` entry, never Tier 3.
- **Out-of-bounds ids (e.g. 612) must not crash.** Raw sub-agent payloads are validated by the
  `ProvenanceGate` *before* constructing `ProvenanceRecord`, so `rule_identity_id > 550` becomes a
  quarantined `SecurityAnomaly`, not an unhandled `ValidationError`.
- **TaskGroup semantics:** each node wrapper catches `Exception` (never `BaseException`) and converts
  it per `FailurePolicy`, so one DEGRADE failure doesn't cancel siblings; `CancelledError` propagates.
  HARD_ABORT is raised after the group exits. Hedge-loser cancellation is not a breaker failure.
- **Scenario 3 mechanics (fixed now):** RAG node timeout 900 ms; breaker trips via consecutive
  failures (N_fail=5) over warm-up brownout requests in the test, plus the EWMA path is covered by a
  separate unit-style assertion with a seeded EWMA (first observation seeds it). The final request asserts:
  breaker `OPEN` → immediate `FallbackEnvelope` → `policy_ast` PROTECTED_50 used, total < 3000 ms.
  The hedge replica is also browned out in this scenario (so failures register); a separate test
  covers the hedge-wins path where the replica answers fast.
- **Scenario 2 sizing:** P1 facts stored as compact canonical JSON (60 entities × short records,
  budgeted explicitly ≈ 1.5k tokens), 600 P2/P3 items fill the rest; assert P1 100% retained,
  total ≤ 4096, and `limiter.peak_in_flight ≤ C_max`.
- **Quality gates are not asserted on `test_cases.json`** (6–8 vectors, some abstain by design).
  The vector test asserts per-case expected behavior only; coverage/selective-accuracy/ECE gates get
  a dedicated test over a seeded synthetic calibration dataset (n = 2000).
- **Token estimation:** pluggable `Tokenizer` protocol, default deterministic `ceil(chars/4)` (no tiktoken dep).
- **Dependency direction:** `tools/mcp_schemas.py` is the leaf; `scripts/*` import only schemas;
  `tools/mcp_router.py` → scripts; `tools/parallel_executor.py` → router + scripts.

## File-by-file contents (build order)
1. **`skill.json`**: semver `1.0.0`, id `dag_orchestrator_skill`, I/O schema refs (`DAGPlanSchema`,
   `OrchestratorSynthesis`), SLA (`total 3000 / exec 2600 / synthesis 400`, p50/p95/p99 1400/3000/4200),
   quality gates (coverage ≥0.75, selective acc ≥0.85, ECE ≤0.10, τ_abstain 0.80), 4 tool endpoints with
   tier, timeout, idempotency, rate limits, breaker params; fallback topology (`hybrid_rag → policy_ast
   PROTECTED_50`), DAG limits (max nodes 64, max waves 6), W_max 4096, shadow timeout 1500.
2. **`guidelines.md`**: invariants, 4-tier hierarchy with precedence table, 7 policy families,
   abstention rules, zero-hop bypass doctrine, diamond W0→W1→W2 doctrine, pruning & failure policies.
3. **`prompts/task_decomposition.md`**: planner system prompt, inline JSON Schema for `DAGPlanSchema`,
   rules (no cycles, `$ref` only to ancestors, priority 1–5, idempotent flags), 3 full few-shot
   examples (zero-hop, diamond, over-budget pruned).
4. **`prompts/conflict_synthesis.md`**: Tier 1>2>3>4 resolution procedure, mandatory
   `postgres_doc_id`+`vector_chunk_sha256` citations, abstention at conf < 0.80, output schema + example.
5. **`tools/mcp_schemas.py`**: frozen/extra-forbid Pydantic v2 models: `MCPToolType`, `FailurePolicy`,
   `CorpusTier`, `EpistemicTier`, `ProvenanceRecord` (64-hex regex, id↔tier validator), `SubTaskNode`
   (self-dependency validator), `DAGPlanSchema`, `ToolResult`, `FallbackEnvelope`, `WaveExecutionResult`,
   `CanonicalFact`, `TriggeredPolicy`, `AbstainedClaim`, `SecurityAnomaly`, `TelemetrySummary`,
   `OrchestratorSynthesis`; constants for corpus bounds.
6. **`scripts/dag_compiler.py`**: Kahn's algorithm; `CyclicDependencyError` (exact cycle path via DFS
   on residual nodes), `DanglingDependencyError`, `InvalidReferenceError` (`$ref` to non-ancestor);
   wave assignment by recurrence; `T_critical`; iterative pruning (priority ≥3, `IGNORE_OPTIONAL`, no
   critical descendants; first try cached fast-path downgrade, then removal; lowest-value first) until
   ≤2600 ms or `BudgetInfeasibleError`; returns `CompiledDAG` (waves, T_critical, pruned/downgraded ids,
   speedup ratio).
7. **`scripts/circuit_breaker.py`**: `asyncio.Lock` FSM CLOSED/OPEN/HALF_OPEN; consecutive-failure,
   30 s sliding failure-rate (min-sample guard), EWMA latency trip (α configurable, 2200 ms);
   full-jitter backoff for OPEN→HALF_OPEN probe windows; `call(coro_factory, fallback)` returns
   `ToolResult` or typed `FallbackEnvelope`; injectable clock/RNG for deterministic tests; registry per tool.
   Timeouts record the timeout ceiling as observed latency so a 5 s straggler cut at its timeout still
   pushes EWMA up.
8. **`scripts/concurrency_controller.py`**: `AdaptiveConcurrencyLimiter` (Vegas q_ratio + AIMD, β 0.75,
   resizable semaphore built on `asyncio.Condition`, tracks peak in-flight), `TokenBucket`
   (`monotonic_ns`, async `acquire`), `execute_with_hedge` (p85 delay, `asyncio.wait FIRST_COMPLETED`,
   cancel + await loser, idempotent-only).
9. **`scripts/state_manager.py`**: canonical SHA-256 fingerprint (key-sorted, normalized JSON,
   UTC-normalized timestamps); `MemoCache` (TTL, fingerprint-keyed, in-flight dedupe via futures);
   `$ref` resolver (`$ref:$.waves.<i>.<task>.output.<path>`, `|default=` support for pruned nodes,
   recursive over dict/list); `KnapsackTokenCompressor` (P1 pinned, raises if P1 alone > W_max;
   P2 structural compression regex-strip prose/markdown keeping predicates, numbers, hashes then 0/1 DP
   knapsack; P3 FIFO eviction of old turns).
10. **`scripts/telemetry_logger.py`**: preallocated `array('d')` ring buffer (single-writer, lock-free
    in the asyncio sense), p50/p90/p95/p99 via nearest-rank, slack; `async_trace_span` usable as
    decorator and async context manager (`perf_counter_ns`); `CalibrationTracker` (10-bin ECE, Brier,
    coverage, selective accuracy); `ShadowAuditor.log_shadow_comparison()` writing to an
    `slm_shadow_telemetry` sink (agreement rate, latency deltas, faults). It never returns into the main path.
11. **`tools/mcp_router.py`**: `MCPTransport` Protocol, `InProcessMCPTransport` (simulated tool
    behaviours + fault injection), `CorpusRegistry`, `ProvenanceGate`, `MCPClientRouter.dispatch(node)`:
    token bucket → adaptive limiter → breaker → (hedge if idempotent) → timeout → provenance gate;
    fallback mapping from skill.json; `fork_shadow()` via `asyncio.create_task` + `asyncio.wait_for(1500)`
    with all exceptions swallowed into telemetry, strong task refs kept in a set, `drain_shadow()` for shutdown.
12. **`tools/parallel_executor.py`**: `ParallelDAGExecutor.run(request, plan)`: fingerprint + zero-hop
    bypass from `GlobalExecutionContext`; compile; per-wave `asyncio.TaskGroup` with per-node failure
    policy handling (HARD_ABORT raises after group, DEGRADE → fallback, IGNORE → skipped); `$ref`
    interpolation between waves; global 2600 ms execution deadline; `EpistemicReconciler` (Tier 1 >
    2 > 3 > 4, numeric contradiction detection with tolerance, abstention < 0.80); knapsack compression;
    emits validated `OrchestratorSynthesis`.
13. **`evals/rubric.md`**: 5 weighted dimensions (25/25/20/15/15) with explicit scoring formulas,
    blocking gates, and a composite score equation.
14. **`evals/test_cases.json`**: ≥6 vectors (cases 1–6 as specified, plus a few extra: hedge win,
    HARD_ABORT on core compute), each with input plan, injected faults, expected synthesis assertions.
15. **`evals/red_team_modes.py`**: fixtures building router/executor over `InProcessMCPTransport`;
    5 scenario tests (cycle <2 ms; 60-subtask flood bounded concurrency + ≤4096 tokens with 100% P1
    retained; 5 s RAG brownout → breaker OPEN + fallback + <3000 ms; spoofed hash / id 612 quarantined,
    Tier 1 upheld; shadow crash/hang → identical output, <5 ms overhead measured by median over runs,
    fault recorded). Plus a data-driven test that runs every vector in `test_cases.json`.

## Verification
- `python -m py_compile` on all `.py` files; `json.load` on both JSON files.
- `pytest evals/red_team_modes.py -v`: all scenarios + test_cases vectors must pass; run twice to
  check timing-based assertions aren't flaky on Windows (timing thresholds use medians/margins).
- Grep for `TODO` / bare `...` / placeholder `pass` → none.
