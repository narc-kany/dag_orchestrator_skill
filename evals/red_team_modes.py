"""Chaos-engineering and adversarial suite for dag_orchestrator_skill.

Run with:  pytest evals/red_team_modes.py -v

Five hostile scenarios (cycle injection, 60-subtask context bomb, RAG brownout, spoofed provenance, shadow crash/hang)
plus data-driven test vectors (evals/test_cases.json), SLA/calibration gates and component-level resilience tests.
Timing assertions use medians and explicit margins so they hold on Windows timer granularity (about 15 ms).
"""

from __future__ import annotations

import asyncio
import copy
import json
import random
import statistics
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
import pytest_asyncio

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.circuit_breaker import BreakerConfig, BreakerState, CircuitBreaker  # noqa: E402
from scripts.concurrency_controller import (  # noqa: E402
    AdaptiveConcurrencyLimiter,
    TokenBucket,
    execute_with_hedge,
)
from scripts.dag_compiler import (  # noqa: E402
    BudgetInfeasibleError,
    CyclicDependencyError,
    DanglingDependencyError,
    InvalidReferenceError,
    compile_dag,
)
from scripts.state_manager import (  # noqa: E402
    ContextItem,
    KnapsackTokenCompressor,
    MemoCache,
    state_fingerprint,
)
from scripts.telemetry_logger import CalibrationTracker, LatencyRingBuffer  # noqa: E402
from tools.mcp_router import (  # noqa: E402
    CorpusRegistry,
    HardAbortError,
    InProcessMCPTransport,
    MCPClientRouter,
    RouterConfig,
    build_synthetic_chunk,
)
from tools.mcp_schemas import (  # noqa: E402
    DAGPlanSchema,
    FailurePolicy,
    MCPToolType,
    SubTaskNode,
    corpus_tier_for,
)
from tools.parallel_executor import OrchestrationRequest, ParallelDAGExecutor  # noqa: E402

CORE, AST, RAG, SLM = (
    MCPToolType.CORE_COMPUTE,
    MCPToolType.POLICY_AST,
    MCPToolType.HYBRID_RAG,
    MCPToolType.SLM_INTENT_SHADOW,
)
VECTORS = json.loads((ROOT / "evals" / "test_cases.json").read_text(encoding="utf-8"))
ERRORS: dict[str, type[Exception]] = {"RuntimeError": RuntimeError, "TimeoutError": TimeoutError, "ValueError": ValueError}


class MaxJitter:
    """Deterministic full-jitter RNG: always draws the upper bound of the backoff window."""

    def uniform(self, a: float, b: float) -> float:
        return b


@dataclass
class Stack:
    transport: InProcessMCPTransport
    router: MCPClientRouter
    executor: ParallelDAGExecutor


@pytest.fixture(scope="module")
def registry() -> CorpusRegistry:
    return CorpusRegistry.synthetic()


@pytest_asyncio.fixture
async def make_stack(registry: CorpusRegistry):
    created: list[MCPClientRouter] = []

    def _make(config: RouterConfig | None = None, *, rng: Any = None, enable_shadow: bool = True) -> Stack:
        transport = InProcessMCPTransport(registry)
        router = MCPClientRouter(transport, registry, config=config or RouterConfig.default(), rng=rng)
        executor = ParallelDAGExecutor(router, memo=MemoCache(ttl_s=0.0), enable_shadow=enable_shadow)
        created.append(router)
        return Stack(transport, router, executor)

    yield _make
    for router in created:
        await router.drain_shadow()


def node(
    task_id: str,
    tool: MCPToolType,
    deps: tuple[str, ...] = (),
    *,
    priority: int = 1,
    policy: FailurePolicy = FailurePolicy.DEGRADE_WITH_FALLBACK,
    payload: dict[str, Any] | None = None,
    est: int = 100,
    timeout: int = 900,
) -> SubTaskNode:
    return SubTaskNode(
        task_id=task_id, tool=tool, dependencies=list(deps), timeout_ms=timeout, idempotent=True,
        priority=priority, failure_policy=policy, estimated_latency_ms=est, payload=payload or {},
    )


def request(rid: str = "rt", **kwargs: Any) -> OrchestrationRequest:
    base: dict[str, Any] = dict(
        request_id=rid, query="exposure and policy check", tenant_id="tenant-acme", entity_id="ACME",
        as_of_timestamp="2026-01-15T12:00:00Z", policy_version="2026.01",
    )
    base.update(kwargs)
    return OrchestrationRequest(**base)


def stray_tasks() -> list[asyncio.Task[Any]]:
    current = asyncio.current_task()
    return [
        t for t in asyncio.all_tasks()
        if t is not current and not t.done() and not t.get_name().startswith("slm-shadow")
    ]


def rule(rid: int, binding: str, op: str, value: Any) -> dict[str, Any]:
    return {"rule_identity_id": rid, "predicate": {"binding": binding, "op": op, "value": value}}


# ======================================================================================
# Scenario 1 - cyclic dependency and diamond deadlock injection
# ======================================================================================


@pytest.mark.asyncio
async def test_scenario1_cycle_detected_under_2ms(make_stack: Any) -> None:
    cyclic = [
        node("A", CORE),
        node("B", CORE, ("A", "D")),
        node("C", CORE, ("B",)),
        node("D", CORE, ("C",)),
    ]
    compile_dag([node("W", CORE), node("X", CORE, ("W",))])  # warm-up so import and regex costs are excluded
    started = time.perf_counter_ns()
    with pytest.raises(CyclicDependencyError) as info:
        compile_dag(cyclic)
    elapsed_ms = (time.perf_counter_ns() - started) / 1e6
    assert elapsed_ms < 2.0, f"cycle detection took {elapsed_ms:.3f} ms"
    assert info.value.cycle == ("B", "C", "D", "B")

    # Self-loop (bypassing model validation) and dangling dependency are rejected deterministically too.
    self_loop = SubTaskNode.model_construct(**{**node("S", CORE).model_dump(), "dependencies": ["S"]})
    with pytest.raises(CyclicDependencyError) as loop_info:
        compile_dag([self_loop])
    assert loop_info.value.cycle == ("S", "S")
    with pytest.raises(DanglingDependencyError):
        compile_dag([node("P", CORE, ("missing",))])

    # The executor aborts before dispatching anything.
    stack = make_stack()
    with pytest.raises(CyclicDependencyError):
        await stack.executor.run(request(), cyclic)
    assert stack.transport.calls(CORE) == 0


def test_scenario1_ref_must_target_a_valid_ancestor() -> None:
    ref = "$ref:$.waves.0.A.output.facts.x"
    with pytest.raises(InvalidReferenceError):  # sibling, not an ancestor
        compile_dag([node("A", CORE), node("B", AST, payload={"bindings": {"x": ref}})])
    with pytest.raises(InvalidReferenceError):  # wrong wave index
        compile_dag([node("A", CORE), node("B", AST, ("A",), payload={"bindings": {"x": "$ref:$.waves.3.A.output.facts.x"}})])
    compiled = compile_dag([node("A", CORE), node("B", AST, ("A",), payload={"bindings": {"x": ref}})])
    assert compiled.wave_of == {"A": 0, "B": 1}


def test_scenario1_budget_pruning_is_priority_safe() -> None:
    optional = FailurePolicy.IGNORE_OPTIONAL
    compiled = compile_dag(
        [
            node("core", CORE, est=650, policy=FailurePolicy.HARD_ABORT),
            node("opt", RAG, priority=5, policy=optional, est=3000, timeout=3500),
        ]
    )
    assert compiled.pruned == ("opt",) and compiled.t_critical_ms == 650
    with pytest.raises(BudgetInfeasibleError):  # nothing optional left to prune
        compile_dag([node("a", CORE, est=2000), node("b", AST, ("a",), est=1500)])


# ======================================================================================
# Scenario 2 - the 60-subtask context bomb
# ======================================================================================


@pytest.mark.asyncio
async def test_scenario2_context_bomb_is_bounded_and_compressed(make_stack: Any) -> None:
    config = RouterConfig.default().with_tool(
        CORE, c_min=1, c_init=4, c_max=8, rate_per_s=5000.0, burst=200, hedge_after_ms=None
    )
    stack = make_stack(config)
    stack.transport.set_latency(CORE, 15.0)
    rng = random.Random(42)
    nodes: list[SubTaskNode] = []
    for entity in range(6):
        for window in range(10):
            facts = {f"exp_{entity}_{window}": round(rng.uniform(1e5, 9e6), 2), f"var_{entity}_{window}": round(rng.uniform(1e3, 9e5), 2)}
            nodes.append(
                node(f"c{entity}_{window}", CORE, policy=FailurePolicy.HARD_ABORT, payload={"facts": facts}, est=50, timeout=800)
            )
    assert len(nodes) == 60
    history = tuple(f"turn {i}: " + "background discussion " * 20 for i in range(300))
    started = time.perf_counter()
    report = await stack.executor.run_detailed(request("bomb", history=history), nodes)
    elapsed_ms = (time.perf_counter() - started) * 1000
    synthesis = report.synthesis

    assert elapsed_ms < 3000
    assert all(r.status == "ok" for r in report.waves[0].results) and len(report.waves[0].results) == 60
    assert len(synthesis.canonical_facts) == 120  # 100 percent of Tier 1 facts survive
    limiter = stack.router.limiter(CORE)
    assert 1 < limiter.peak_in_flight <= 8
    assert stack.transport.peak_active_calls <= 8
    assert synthesis.telemetry_summary.context_tokens <= 4096
    assert synthesis.telemetry_summary.context_items_dropped > 0  # history was evicted
    assert not stray_tasks()


def test_scenario2_compressor_preserves_priority1_within_window() -> None:
    p1 = [ContextItem(f"p1:{i}", 1, json.dumps({"k": i, "exp": 1234567.89 + i, "var": 98765.4321}, separators=(",", ":"))) for i in range(60)]
    sha = "a" * 64
    p2 = [
        ContextItem(
            f"p2:{i}", 2,
            "# Heading\nSure, here is some friendly prose that carries no policy meaning at all. "
            f"**IF** exposure_usd > {100000 * (i + 1)} THEN escalate; limit = {i}.5 percent.\n- bullet filler words",
            relevance=(i % 10) / 10.0, provenance_tag=f"[doc={i} sha={sha}]",
        )
        for i in range(400)
    ]
    p3 = [ContextItem(f"p3:{i}", 3, "chatty history " * 30, turn_index=i) for i in range(200)]
    compressor = KnapsackTokenCompressor(4096)
    result = compressor.compress(p1 + p2 + p3)
    assert result.tokens_used <= 4096
    assert set(result.pinned_ids) == {i.item_id for i in p1}  # 100 percent of Priority 1 retained
    assert result.compressed_ids  # structural compression ran on Priority 2
    kept_p2 = [i for i in result.kept if i.priority == 2]
    assert kept_p2 and all(sha in i.render() and "Sure," not in i.content for i in kept_p2)
    kept_p3 = [int(i.item_id.split(":")[1]) for i in result.kept if i.priority == 3]
    assert kept_p3 == sorted(kept_p3)
    if kept_p3:  # FIFO eviction: survivors are a contiguous newest suffix
        assert kept_p3[-1] == 199 and kept_p3 == list(range(kept_p3[0], 200))


# ======================================================================================
# Scenario 3 - vector RAG brownout and tail-latency straggler (5.0 s)
# ======================================================================================


def _brownout_config() -> RouterConfig:
    breaker = BreakerConfig(base_backoff_s=10.0, max_backoff_s=15.0)
    return RouterConfig.default().with_tool(RAG, breaker=breaker, c_min=1, c_init=8, c_max=12)


def _rag_node(task_id: str = "rag") -> SubTaskNode:
    payload = {"query": "risk families", "family_ids": [1, 2, 5], "baseline_rules": [rule(7, "exposure_usd", ">", 1_000_000)], "bindings": {"exposure_usd": 4_200_000}}
    return node(task_id, RAG, payload=payload, est=800)


@pytest.mark.asyncio
async def test_scenario3_rag_brownout_trips_breaker_and_falls_back(make_stack: Any) -> None:
    stack = make_stack(_brownout_config(), rng=MaxJitter())
    stack.transport.set_latency(RAG, 5000.0, 5000.0)  # primary and replica are both browned out
    breaker = stack.router.breaker(RAG)

    started = time.perf_counter()
    warmups = await asyncio.gather(*[stack.router.dispatch(_rag_node(f"w{i}")) for i in range(5)])
    warmup_ms = (time.perf_counter() - started) * 1000
    assert warmup_ms < 3000  # per-node timeout (900 ms) cut the 5 s stragglers off
    assert all(r.status == "degraded" and r.tool is AST for r in warmups)
    assert stack.transport.calls(RAG, replica=True) >= 5  # hedges fired at the 750 ms p85 threshold
    assert breaker.state is BreakerState.OPEN
    assert breaker.snapshot().last_trip_reason == "consecutive_failures>=5"
    assert not stray_tasks()  # hedge losers and timed-out calls were cancelled and drained

    plan = [
        node("core", CORE, policy=FailurePolicy.HARD_ABORT, payload={"facts": {"exposure_usd": 4_200_000}}, est=600),
        _rag_node("rag"),
        node("ast", AST, ("core", "rag"), payload={"rules": [rule(121, "exposure_usd", ">", 1_000_000)], "bindings": {"exposure_usd": "$ref:$.waves.0.core.output.facts.exposure_usd"}}, est=500),
    ]
    started = time.perf_counter()
    synthesis = await stack.executor.run(request("brownout"), plan)
    total_ms = (time.perf_counter() - started) * 1000
    assert total_ms < 3000 and synthesis.telemetry_summary.total_latency_ms < 3000
    assert breaker.state is BreakerState.OPEN
    assert "tool_hybrid_rag" in synthesis.degraded_services
    rule_ids = {p.rule_identity_id: p.corpus_tier for p in synthesis.triggered_policies}
    assert rule_ids[7] == "PROTECTED_50"  # protected baseline rule evaluated via the fallback topology
    assert rule_ids[121] == "ACTIVE_200"
    assert not synthesis.security_anomalies


@pytest.mark.asyncio
async def test_scenario3_single_request_with_5s_straggler_stays_within_sla(make_stack: Any) -> None:
    stack = make_stack(_brownout_config(), rng=MaxJitter())
    stack.transport.set_latency(RAG, 5000.0, 5000.0)
    plan = [node("core", CORE, policy=FailurePolicy.HARD_ABORT, payload={"facts": {"exposure_usd": 4_200_000}}, est=600), _rag_node("rag")]
    synthesis = await stack.executor.run(request("straggler"), plan)
    assert synthesis.telemetry_summary.total_latency_ms < 3000
    assert synthesis.telemetry_summary.sla_met
    assert "tool_hybrid_rag" in synthesis.degraded_services
    assert [p.rule_identity_id for p in synthesis.triggered_policies] == [7]


def test_scenario3_ewma_latency_trips_breaker_and_half_open_recovers() -> None:
    async def scenario() -> None:
        now = [0.0]
        breaker = CircuitBreaker("tool_hybrid_rag", BreakerConfig(base_backoff_s=1.0, max_backoff_s=2.0), clock=lambda: now[0], rng=MaxJitter())
        for _ in range(2):
            await breaker.record_success(3000.0)
        assert breaker.state is BreakerState.CLOSED  # EWMA needs a minimum sample count
        await breaker.record_success(3000.0)
        assert breaker.state is BreakerState.OPEN
        assert (breaker.snapshot().last_trip_reason or "").startswith("ewma_latency=3000ms")
        assert not (await breaker.admit()).allowed  # short-circuits while OPEN
        now[0] = 1.5  # past the full-jitter window (upper bound 1.0 s)
        probe = await breaker.admit()
        assert probe.allowed and probe.probe and breaker.state is BreakerState.HALF_OPEN
        assert not (await breaker.admit()).allowed  # only one concurrent probe
        await breaker.record_failure(probe=True)
        assert breaker.state is BreakerState.OPEN and breaker.snapshot().attempt == 2
        now[0] = 10.0
        for _ in range(2):
            adm = await breaker.admit()
            await breaker.record_success(10.0, probe=adm.probe)
        assert breaker.state is BreakerState.CLOSED

    asyncio.run(scenario())


@pytest.mark.asyncio
async def test_hedged_request_wins_against_a_straggling_primary(make_stack: Any) -> None:
    config = RouterConfig.default().with_tool(RAG, hedge_after_ms=100.0)
    stack = make_stack(config)
    stack.transport.set_latency(RAG, 700.0, 5.0)
    started = time.perf_counter()
    result = await stack.router.dispatch(node("rag", RAG, payload={"chunks": []}))
    elapsed_ms = (time.perf_counter() - started) * 1000
    assert result.status == "ok" and stack.router.hedge_wins == 1
    assert elapsed_ms < 450
    assert stack.router.breaker(RAG).state is BreakerState.CLOSED  # cancelled loser is not a failure
    assert not stray_tasks()


@pytest.mark.asyncio
async def test_non_idempotent_calls_are_never_hedged() -> None:
    calls = {"hedge": 0}

    async def primary() -> str:
        await asyncio.sleep(0.05)
        return "primary"

    async def hedge() -> str:
        calls["hedge"] += 1
        return "hedge"

    outcome = await execute_with_hedge(primary, hedge, idempotent=False, hedge_after_ms=1.0)
    assert outcome.value == "primary" and not outcome.hedged and calls["hedge"] == 0


# ======================================================================================
# Scenario 4 - epistemic contradiction and spoofed provenance attack
# ======================================================================================


@pytest.mark.asyncio
async def test_scenario4_spoofed_provenance_is_quarantined(make_stack: Any, registry: CorpusRegistry) -> None:
    stack = make_stack()
    lie = {"exposure_usd": 1_000_000}
    chunks = [
        build_synthetic_chunk(registry, 70, claims=lie, confidence=0.99, rule_verdict=True, tamper="sha"),
        build_synthetic_chunk(registry, 71, claims=lie, confidence=0.99, rule_verdict=True, tamper="text"),
        build_synthetic_chunk(registry, 72, claims=lie, confidence=0.99, rule_verdict=True, tamper="doc"),
        build_synthetic_chunk(registry, 612, claims=lie, confidence=0.99, rule_verdict=True),
        build_synthetic_chunk(registry, 73, claims={"exposure_usd": 5}, confidence=0.95, rule_verdict=True),  # valid but contradicts Tier 1
    ]
    plan = [
        node("core", CORE, policy=FailurePolicy.HARD_ABORT, payload={"facts": {"exposure_usd": 4_200_000}}, est=600),
        node("rag", RAG, payload={"chunks": chunks, "baseline_rules": [], "bindings": {}}, est=800),
        node("ast", AST, ("core", "rag"), payload={"rules": [rule(121, "exposure_usd", ">", 1_000_000)], "bindings": {"exposure_usd": "$ref:$.waves.0.core.output.facts.exposure_usd"}}, est=500),
    ]
    synthesis = await stack.executor.run(request("spoof"), plan)

    assert {f.key: f.value for f in synthesis.canonical_facts} == {"exposure_usd": 4_200_000}  # Tier 1 upheld
    assert [p.rule_identity_id for p in synthesis.triggered_policies] == [121]  # Tier 2 upheld, nothing spoofed asserted
    kinds = sorted(a.kind for a in synthesis.security_anomalies)
    assert kinds == ["HASH_MISMATCH", "HASH_MISMATCH", "OUT_OF_BOUNDS_RULE_IDENTITY", "UNREGISTERED_DOC"]
    assert {a.rule_identity_id for a in synthesis.security_anomalies} == {70, 71, 72, 612}
    assert {r.postgres_doc_id for r in synthesis.provenance_ledger} == {10_121}
    assert all(r.rule_identity_id <= 550 for r in synthesis.provenance_ledger)
    resolutions = [(c.key, c.winning_tier, c.losing_tier, c.losing_rule_identity_id) for c in synthesis.conflict_resolutions]
    assert resolutions == [("exposure_usd", 1, 3, 73)]


# ======================================================================================
# Scenario 5 - shadow-mode crash and latency isolation
# ======================================================================================


def _shadow_plan(registry: CorpusRegistry) -> list[SubTaskNode]:
    chunk = build_synthetic_chunk(registry, 57, confidence=0.93, rule_verdict=True)
    return [
        node("core", CORE, policy=FailurePolicy.HARD_ABORT, payload={"facts": {"exposure_usd": 4_200_000}}, est=300),
        node("rag", RAG, payload={"chunks": [chunk], "baseline_rules": [], "bindings": {}}, est=300),
        node("ast", AST, ("core", "rag"), payload={"rules": [rule(121, "exposure_usd", ">", 1_000_000)], "bindings": {"exposure_usd": "$ref:$.waves.0.core.output.facts.exposure_usd"}}, est=300),
    ]


def _user_visible(synthesis: Any) -> dict[str, Any]:
    return synthesis.model_dump(exclude={"telemetry_summary"})


@pytest.mark.asyncio
async def test_scenario5_shadow_crash_has_zero_user_impact(make_stack: Any, registry: CorpusRegistry) -> None:
    baseline = make_stack(enable_shadow=False)
    crashing = make_stack()
    crashing.transport.set_failure(SLM, RuntimeError("shadow exploded"))
    plan = _shadow_plan(registry)

    expected = await baseline.executor.run(request("iso-1"), plan)
    actual = await crashing.executor.run(request("iso-1"), plan)
    assert _user_visible(actual) == _user_visible(expected)
    assert "shadow" not in actual.model_dump_json().lower() and "slm" not in actual.model_dump_json().lower()

    await crashing.router.drain_shadow(wait_s=0.5)
    summary = crashing.router.auditor.summary()
    assert summary["fault_counts"] == {"RuntimeError": 1}
    assert summary["internal_errors"] == 0
    record = crashing.router.auditor.records()[0]
    assert record.fault == "RuntimeError" and record.shadow_route is None and not record.agree
    assert crashing.transport.calls(SLM) == 1
    assert not stray_tasks()


@pytest.mark.asyncio
async def test_scenario5_shadow_hang_does_not_bleed_latency(make_stack: Any, registry: CorpusRegistry) -> None:
    hanging = make_stack()
    quiet = make_stack(enable_shadow=False)
    hanging.transport.set_latency(SLM, 10_000.0)  # the shadow call hangs for 10 s
    plan = _shadow_plan(registry)

    started = time.perf_counter_ns()
    hanging.router.fork_shadow(request_id="probe", query="q", canonical_route="deterministic")
    fork_ms = (time.perf_counter_ns() - started) / 1e6
    assert fork_ms < 5.0  # forking is a task spawn, nothing more

    async def timed(stack: Stack, index: int) -> float:
        begin = time.perf_counter_ns()
        await stack.executor.run(request(f"hang-{index}"), plan)
        return (time.perf_counter_ns() - begin) / 1e6

    for i in range(3):  # warm-up
        await timed(hanging, i)
        await timed(quiet, i)
    with_shadow = [await timed(hanging, i) for i in range(20)]
    without = [await timed(quiet, i) for i in range(20)]
    overhead_ms = abs(statistics.median(with_shadow) - statistics.median(without))
    assert overhead_ms < 5.0, f"shadow latency bleed {overhead_ms:.3f} ms"
    assert statistics.median(with_shadow) < 100.0  # nowhere near the 10 s hang

    assert hanging.router.shadow_inflight > 0
    await hanging.router.drain_shadow(wait_s=2.5)  # hard 1500 ms ceiling fires, faults are recorded
    summary = hanging.router.auditor.summary()
    assert summary["fault_counts"].get("timeout", 0) >= 20
    assert summary["internal_errors"] == 0 and hanging.router.shadow_inflight == 0
    assert not stray_tasks()


# ======================================================================================
# Data-driven end-to-end vectors (evals/test_cases.json)
# ======================================================================================


def _materialize_plan(case: dict[str, Any], registry: CorpusRegistry) -> DAGPlanSchema:
    plan = copy.deepcopy(case["plan"])
    for spec_node in plan["nodes"]:
        specs = case.get("chunk_specs", {}).get(spec_node["task_id"])
        if specs is not None:
            spec_node["payload"]["chunks"] = [
                build_synthetic_chunk(
                    registry, s["rule_identity_id"], claims=s.get("claims"), confidence=s.get("confidence", 0.95),
                    rule_verdict=s.get("rule_verdict"), tamper=s.get("tamper"),
                )
                for s in specs
            ]
    return DAGPlanSchema.model_validate(plan)


def _vector_request(case: dict[str, Any]) -> OrchestrationRequest:
    merged = {**VECTORS["defaults"]["request"], **case["request"]}
    merged["required_fact_keys"] = tuple(merged.get("required_fact_keys", ()))
    return OrchestrationRequest(**merged)


def _check_synthesis(report: Any, expected: dict[str, Any]) -> None:
    s = report.synthesis
    if "zero_hop" in expected:
        assert s.telemetry_summary.zero_hop is expected["zero_hop"]
    if "waves" in expected:
        assert s.telemetry_summary.waves == expected["waves"]
    if "canonical_facts" in expected:
        assert {f.key: f.value for f in s.canonical_facts} == expected["canonical_facts"]
    if "triggered_rule_ids" in expected:
        assert sorted(p.rule_identity_id for p in s.triggered_policies) == expected["triggered_rule_ids"]
    if "policy_families_covered" in expected:
        assert set(expected["policy_families_covered"]) <= {p.policy_family_id for p in s.triggered_policies}
    if "abstained_claim_ids" in expected:
        assert [a.claim_id for a in s.abstained_claims] == expected["abstained_claim_ids"]
    for item in expected.get("abstained", []):
        assert any(a.claim_id == item["claim_id"] and a.reason == item["reason"] for a in s.abstained_claims)
    if "anomaly_kinds" in expected:
        assert sorted(a.kind for a in s.security_anomalies) == sorted(expected["anomaly_kinds"])
    if "conflicts" in expected:
        got = [{"key": c.key, "winning_tier": c.winning_tier, "losing_tier": c.losing_tier} for c in s.conflict_resolutions]
        assert got == expected["conflicts"]
    if "degraded_services" in expected:
        assert s.degraded_services == expected["degraded_services"]
    if "ledger_size" in expected:
        assert len(s.provenance_ledger) == expected["ledger_size"]
    for rid in expected.get("forbidden_rule_ids", []):
        assert all(p.rule_identity_id != rid for p in s.triggered_policies)
        assert all(r.rule_identity_id != rid for r in s.provenance_ledger)
    if "t_critical_before_ms" in expected:
        assert report.compiled is not None
        assert report.compiled.original_t_critical_ms == expected["t_critical_before_ms"]
        assert report.compiled.t_critical_ms == expected["t_critical_after_ms"] <= 2600
        assert list(report.compiled.pruned) == expected["pruned_nodes"]
        assert list(report.compiled.downgraded) == expected["downgraded_nodes"]
    assert s.telemetry_summary.total_latency_ms < 3000


@pytest.mark.asyncio
@pytest.mark.parametrize("case", VECTORS["cases"], ids=[c["id"] for c in VECTORS["cases"]])
async def test_vectors(case: dict[str, Any], make_stack: Any, registry: CorpusRegistry) -> None:
    stack = make_stack()
    for failure in case.get("failures", []):
        stack.transport.set_failure(MCPToolType(failure["tool"]), ERRORS[failure["error"]]("injected"), replica=failure["replica"])
    plan = _materialize_plan(case, registry)
    req = _vector_request(case)
    expected = case["expected"]

    if "error" in expected:
        error_type = {"CyclicDependencyError": CyclicDependencyError, "HardAbortError": HardAbortError}[expected["error"]]
        with pytest.raises(error_type) as info:
            await stack.executor.run(req, plan)
        if "cycle" in expected:
            assert list(info.value.cycle) == expected["cycle"]
    else:
        _check_synthesis(await stack.executor.run_detailed(req, plan), expected)

    for tool_name, count in expected.get("tool_calls", {}).items():
        tool = MCPToolType(tool_name)
        assert stack.transport.calls(tool) + stack.transport.calls(tool, replica=True) == count


# ======================================================================================
# SLA, calibration and component-level gates
# ======================================================================================


@pytest.mark.asyncio
async def test_sla_percentiles_under_concurrent_load(make_stack: Any, registry: CorpusRegistry) -> None:
    config = RouterConfig.default()
    for tool in (CORE, AST, RAG):  # provision capacity for the load; rate limiting itself is covered separately
        config = config.with_tool(tool, rate_per_s=5000.0, burst=500, c_min=4, c_init=16, c_max=32)
    stack = make_stack(config)
    stack.transport.set_latency(CORE, 5.0)
    stack.transport.set_latency(RAG, 8.0)
    plan = _shadow_plan(registry)
    results = await asyncio.gather(*[stack.executor.run(request(f"load-{i}", query=f"q{i}"), plan) for i in range(200)])
    assert all(r.telemetry_summary.sla_met for r in results)
    assert all(not r.degraded_services for r in results)
    buffer = stack.executor.hub.buffer("orchestration.total")
    assert len(buffer) == 200
    assert buffer.percentile(50) <= 1400 and buffer.percentile(95) <= 3000 and buffer.percentile(99) <= 4200
    await stack.router.drain_shadow(wait_s=1.0)


def test_calibration_quality_gates() -> None:
    rng = random.Random(20260110)
    tracker = CalibrationTracker(tau=0.80, bins=10)
    for _ in range(2000):
        confident = rng.random() < 0.82
        conf = rng.uniform(0.82, 0.99) if confident else rng.uniform(0.30, 0.79)
        tracker.add(conf, rng.random() < conf)  # well-calibrated by construction
    report = tracker.report()
    assert report["coverage"] >= 0.75
    assert report["selective_accuracy"] >= 0.85
    assert report["ece"] <= 0.10
    boundary = CalibrationTracker(tau=0.80)
    boundary.add(0.80, True)
    boundary.add(0.7999, True)
    assert boundary.coverage == 0.5  # exactly 0.80 commits, below it abstains


def test_ring_buffer_percentiles_and_wraparound() -> None:
    ring = LatencyRingBuffer(capacity=100)
    for value in range(1, 251):
        ring.record(float(value))
    assert len(ring) == 100 and ring.total_recorded == 250
    assert ring.percentile(50) == 200.0 and ring.percentile(99) == 249.0 and ring.snapshot()["max"] == 250.0


def test_skill_manifest_matches_router_defaults() -> None:
    assert RouterConfig.from_skill_json(ROOT / "skill.json") == RouterConfig.default()
    manifest = json.loads((ROOT / "skill.json").read_text(encoding="utf-8"))
    sla = manifest["sla"]
    assert sla["total_budget_ms"] == sla["execution_ceiling_ms"] + sla["synthesis_reserve_ms"]


def test_corpus_boundary_is_550_identities(registry: CorpusRegistry) -> None:
    tiers = [corpus_tier_for(i) for i in range(1, 551)]
    assert len(registry) == 550
    assert (tiers.count("PROTECTED_50"), tiers.count("ACTIVE_200"), tiers.count("CANDIDATE_300")) == (50, 200, 300)
    for bad in (0, 551, 612, -3):
        with pytest.raises(ValueError):
            corpus_tier_for(bad)


def test_state_fingerprint_is_canonical() -> None:
    a = state_fingerprint(tenant_id=" t ", entity_id="e", as_of_timestamp="2026-01-01T00:00:00Z", policy_version="v1", query_signature="a  b")
    b = state_fingerprint(tenant_id="t", entity_id="e", as_of_timestamp="2026-01-01T01:00:00+01:00", policy_version="v1", query_signature="a b")
    c = state_fingerprint(tenant_id="t", entity_id="e", as_of_timestamp="2026-01-01T00:00:01Z", policy_version="v1", query_signature="a b")
    assert a == b and a != c and len(a) == 64


@pytest.mark.asyncio
async def test_adaptive_limiter_and_token_bucket_behaviour() -> None:
    limiter = AdaptiveConcurrencyLimiter("t", c_min=2, c_max=8, c_init=8)
    await limiter.acquire()
    await limiter.release(10.0, timed_out=True)
    assert limiter.limit == 6  # floor(8 * 0.75)
    clock = [0]
    bucket = TokenBucket(100.0, 5.0, clock_ns=lambda: clock[0])
    assert all(bucket.try_acquire() for _ in range(5)) and not bucket.try_acquire()
    clock[0] += 10_000_000  # 10 ms at 100 tokens per second refills one token
    assert bucket.try_acquire() and not bucket.try_acquire()
