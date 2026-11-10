"""CI/CD evaluation harness for dag_orchestrator_skill.

Bridges the functional rubric (evals/test_cases.json, evals/rubric.md) and the adversarial chaos suite
(evals/red_team_modes.py).

    python evals/judges.py --offline               # functional rubric, deterministic heuristic judge
    python evals/judges.py --offline --red-team    # plus the chaos suite as a release-blocking security gate

Functional evaluation executes every vector against the real orchestrator (in-process reference transport), derives
check results per rubric dimension and scores each dimension 1-5:

    score_d = 1 + 4 * (checks passed / checks total)        (5 when the dimension has no applicable check)

Weights follow evals/rubric.md: D1 DAG optimality 0.25, D2 SLA latency 0.25, D3 epistemic integrity 0.20,
D4 calibration 0.15, D5 resilience 0.15. A case passes when its weighted score and every dimension are >= 4.0 and no individual check failed.

Fail-closed exit codes (security overrides functional):

    0  every functional case, SLA probe, calibration gate and (if requested) the red-team gate passed
    1  functional rubric failure: case below 4.0, mean below 4.0, p50/p95/p99 SLA missed, calibration gate missed
    2  security gate failure: any red-team test failed or was skipped, the suite could not run, a cyclic plan was
       accepted or deadlocked, an out-of-bounds or unverified identity leaked, or a Tier 1 value was overridden
"""

from __future__ import annotations

import argparse
import asyncio
import copy
import os
import random
import sys
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.state_manager import MemoCache  # noqa: E402
from scripts.telemetry_logger import CalibrationTracker  # noqa: E402
from tools.mcp_router import (  # noqa: E402
    CorpusRegistry,
    InProcessMCPTransport,
    MCPClientRouter,
    RouterConfig,
    build_synthetic_chunk,
)
from tools.mcp_schemas import TAU_ABSTAIN, DAGPlanSchema, MCPToolType  # noqa: E402
from tools.parallel_executor import (  # noqa: E402
    ExecutionReport,
    OrchestrationRequest,
    ParallelDAGExecutor,
)

EXIT_OK = 0
EXIT_FUNCTIONAL_FAILURE = 1
EXIT_SECURITY_FAILURE = 2

DIMENSIONS: tuple[str, ...] = ("dag_optimality", "sla_latency", "epistemic_integrity", "calibration", "resilience")
DIMENSION_LABELS: dict[str, str] = {
    "dag_optimality": "D1 DAG",
    "sla_latency": "D2 SLA",
    "epistemic_integrity": "D3 Epistemic",
    "calibration": "D4 Calibration",
    "resilience": "D5 Resilience",
}
WEIGHTS: dict[str, float] = {
    "dag_optimality": 0.25,
    "sla_latency": 0.25,
    "epistemic_integrity": 0.20,
    "calibration": 0.15,
    "resilience": 0.15,
}
PASS_THRESHOLD = 4.0
SLA_MS: dict[str, float] = {"p50": 1400.0, "p95": 3000.0, "p99": 4200.0}
COVERAGE_MIN, SELECTIVE_ACCURACY_MIN, ECE_MAX = 0.75, 0.85, 0.10
CASE_TIMEOUT_S = 10.0
MAX_RULE_IDENTITY = 550
ENGINE_TOOLS = (MCPToolType.CORE_COMPUTE, MCPToolType.POLICY_AST, MCPToolType.HYBRID_RAG)
INJECTABLE_ERRORS: dict[str, type[Exception]] = {
    "RuntimeError": RuntimeError,
    "TimeoutError": TimeoutError,
    "ValueError": ValueError,
}


@dataclass(frozen=True)
class CheckResult:
    """One verifiable expectation, attributed to a rubric dimension."""

    dimension: str
    name: str
    passed: bool
    detail: str = ""


@dataclass
class CaseOutcome:
    """Everything observed while executing one vector."""

    case_id: str
    description: str
    expected: dict[str, Any]
    report: ExecutionReport | None = None
    error: BaseException | None = None
    deadlocked: bool = False
    latency_ms: float = 0.0
    call_counts: dict[str, int] = field(default_factory=dict)
    node_count: int = 0
    stray_task_count: int = 0
    checks: list[CheckResult] = field(default_factory=list)
    breaches: list[str] = field(default_factory=list)
    scores: dict[str, float] = field(default_factory=dict)
    overall: float = 0.0

    @property
    def passed(self) -> bool:
        """Fail closed: weighted score and every dimension >= 4.0 AND every individual check passed."""
        return (
            self.overall >= PASS_THRESHOLD
            and all(v >= PASS_THRESHOLD for v in self.scores.values())
            and all(c.passed for c in self.checks)
        )


# ======================================================================================
# Vector loading and execution
# ======================================================================================


def load_vectors(path: Path) -> dict[str, Any]:
    """Load and minimally validate evals/test_cases.json."""
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data.get("cases"), list) or not data["cases"]:
        raise ValueError(f"{path} contains no cases")
    return data


def materialize_plan(case: dict[str, Any], registry: CorpusRegistry) -> DAGPlanSchema:
    """Build the plan, expanding chunk_specs into verified-corpus chunks inside the named nodes."""
    plan = copy.deepcopy(case["plan"])
    for node in plan["nodes"]:
        specs = case.get("chunk_specs", {}).get(node["task_id"])
        if specs is not None:
            node["payload"]["chunks"] = [
                build_synthetic_chunk(
                    registry,
                    s["rule_identity_id"],
                    claims=s.get("claims"),
                    confidence=s.get("confidence", 0.95),
                    rule_verdict=s.get("rule_verdict"),
                    tamper=s.get("tamper"),
                )
                for s in specs
            ]
    return DAGPlanSchema.model_validate(plan)


def build_request(case: dict[str, Any], defaults: dict[str, Any], *, suffix: str = "") -> OrchestrationRequest:
    merged = {**defaults.get("request", {}), **case["request"]}
    merged["required_fact_keys"] = tuple(merged.get("required_fact_keys", ()))
    if suffix:
        merged["query"] = f"{merged['query']} {suffix}"
        merged["request_id"] = f"{merged['request_id']}{suffix}"
    return OrchestrationRequest(**merged)


def new_stack(
    registry: CorpusRegistry, config: RouterConfig | None = None
) -> tuple[InProcessMCPTransport, MCPClientRouter, ParallelDAGExecutor]:
    """Fresh transport, router and executor (memoisation disabled so every dispatch is observable)."""
    transport = InProcessMCPTransport(registry)
    router = MCPClientRouter(transport, registry, config=config or RouterConfig.default())
    executor = ParallelDAGExecutor(router, memo=MemoCache(ttl_s=0.0))
    return transport, router, executor


def stray_tasks() -> list[asyncio.Task[Any]]:
    """Unfinished tasks other than the caller (shadow tasks are drained before this is called)."""
    current = asyncio.current_task()
    return [t for t in asyncio.all_tasks() if t is not current and not t.done()]


async def run_case(case: dict[str, Any], vectors: dict[str, Any], registry: CorpusRegistry) -> CaseOutcome:
    """Execute one vector end to end and record what happened (never raises for orchestrator errors)."""
    outcome = CaseOutcome(case_id=case["id"], description=case.get("description", ""), expected=case["expected"])
    transport, router, executor = new_stack(registry)
    for failure in case.get("failures", []):
        error_type = INJECTABLE_ERRORS[failure["error"]]
        transport.set_failure(MCPToolType(failure["tool"]), error_type("injected"), replica=failure["replica"])
    plan = materialize_plan(case, registry)
    outcome.node_count = len(plan.nodes)
    request = build_request(case, vectors.get("defaults", {}))

    started = asyncio.get_running_loop().time()
    try:
        outcome.report = await asyncio.wait_for(executor.run_detailed(request, plan), timeout=CASE_TIMEOUT_S)
    except asyncio.TimeoutError:
        outcome.deadlocked = True
    except Exception as exc:  # recorded and judged, not propagated
        outcome.error = exc
    outcome.latency_ms = (asyncio.get_running_loop().time() - started) * 1000.0

    await router.drain_shadow()
    outcome.stray_task_count = len(stray_tasks())
    outcome.call_counts = {
        tool.value: transport.calls(tool) + transport.calls(tool, replica=True) for tool in ENGINE_TOOLS
    }
    return outcome


# ======================================================================================
# Expectation checks (each check is attributed to one rubric dimension)
# ======================================================================================


def _add(outcome: CaseOutcome, dimension: str, name: str, passed: bool, detail: str = "") -> None:
    outcome.checks.append(CheckResult(dimension, name, bool(passed), detail))


def _verified(registry: CorpusRegistry, rule_id: int, doc_id: int, digest: str) -> bool:
    """True iff the identity is inside the 550 boundary and doc id and digest match the corpus registry."""
    entry = registry.by_rule(rule_id)
    return (
        1 <= rule_id <= MAX_RULE_IDENTITY
        and entry is not None
        and entry.postgres_doc_id == doc_id
        and entry.vector_chunk_sha256 == digest
    )


def check_outcome(outcome: CaseOutcome, registry: CorpusRegistry) -> None:
    """Populate outcome.checks and outcome.breaches from the observed behaviour."""
    expected = outcome.expected
    _add(outcome, "resilience", "no leaked tasks after drain", outcome.stray_task_count == 0, f"{outcome.stray_task_count} stray")
    for label, limit in (("p50", SLA_MS["p50"]), ("p95", SLA_MS["p95"]), ("p99", SLA_MS["p99"])):
        _add(outcome, "sla_latency", f"case latency <= {label} budget {limit:.0f} ms", outcome.latency_ms <= limit, f"{outcome.latency_ms:.1f} ms")
    if outcome.deadlocked:
        outcome.breaches.append(f"{outcome.case_id}: execution did not terminate within {CASE_TIMEOUT_S:.0f}s (deadlock)")

    if "error" in expected:
        _check_expected_error(outcome, expected)
    elif outcome.error is not None or outcome.deadlocked:
        reason = type(outcome.error).__name__ if outcome.error else "deadlock"
        _add(outcome, "resilience", "completes without an unhandled error", False, reason)
    elif outcome.report is not None:
        _check_synthesis(outcome, expected, registry)

    for tool_name, count in expected.get("tool_calls", {}).items():
        got = outcome.call_counts.get(tool_name, -1)
        _add(outcome, "dag_optimality", f"{tool_name} dispatched {count}x", got == count, f"got {got}")


def _check_expected_error(outcome: CaseOutcome, expected: dict[str, Any]) -> None:
    wanted = expected["error"]
    got = type(outcome.error).__name__ if outcome.error is not None else None
    dimension = "dag_optimality" if wanted == "CyclicDependencyError" else "resilience"
    _add(outcome, dimension, f"raises {wanted}", got == wanted, f"got {got}")
    if "cycle" in expected:
        cycle = list(getattr(outcome.error, "cycle", ()))
        _add(outcome, "dag_optimality", "exact cycle path reported", cycle == expected["cycle"], f"got {cycle}")
    if outcome.error is None and not outcome.deadlocked:
        if wanted == "CyclicDependencyError":
            outcome.breaches.append(f"{outcome.case_id}: cyclic plan was accepted")
        else:
            outcome.breaches.append(f"{outcome.case_id}: {wanted} expected but the run completed (Tier 1 failure was absorbed)")


def _check_synthesis(outcome: CaseOutcome, expected: dict[str, Any], registry: CorpusRegistry) -> None:
    assert outcome.report is not None
    s = outcome.report.synthesis
    compiled = outcome.report.compiled

    # D1: topology and routing optimality
    if "zero_hop" in expected:
        _add(outcome, "dag_optimality", "zero-hop flag", s.telemetry_summary.zero_hop is expected["zero_hop"])
    if "waves" in expected:
        _add(outcome, "dag_optimality", f"{expected['waves']} waves", s.telemetry_summary.waves == expected["waves"], f"got {s.telemetry_summary.waves}")
    if compiled is not None:
        fallbacks = sum(1 for w in outcome.report.waves for r in w.results if r.fallback is not None and r.fallback.fallback_tool is not None)
        allowed = compiled.node_count + fallbacks
        issued = sum(outcome.call_counts.values())
        _add(outcome, "dag_optimality", "no redundant invocations", issued <= allowed, f"{issued} calls for {allowed} allowed")
    else:
        issued = sum(outcome.call_counts.values())
        _add(outcome, "dag_optimality", "no engine call for a zero-hop plan", issued == 0, f"{issued} calls")

    # D2: critical-path budget and pruning accuracy
    if compiled is not None:
        _add(outcome, "sla_latency", "T_critical <= 2600 ms after pruning", compiled.t_critical_ms <= 2600, f"{compiled.t_critical_ms:.0f} ms")
    if "t_critical_before_ms" in expected and compiled is not None:
        _add(outcome, "sla_latency", "T_critical before pruning", compiled.original_t_critical_ms == expected["t_critical_before_ms"], f"{compiled.original_t_critical_ms:.0f}")
        _add(outcome, "sla_latency", "T_critical after pruning", compiled.t_critical_ms == expected["t_critical_after_ms"], f"{compiled.t_critical_ms:.0f}")
        _add(outcome, "sla_latency", "pruned nodes", list(compiled.pruned) == expected["pruned_nodes"], f"{list(compiled.pruned)}")
        _add(outcome, "sla_latency", "downgraded nodes", list(compiled.downgraded) == expected["downgraded_nodes"], f"{list(compiled.downgraded)}")
    _check_epistemic(outcome, expected, registry)
    _check_calibration(outcome, expected)


def _check_epistemic(outcome: CaseOutcome, expected: dict[str, Any], registry: CorpusRegistry) -> None:
    """D3: precedence, boundary, provenance verification, conflicts, anomalies."""
    assert outcome.report is not None
    s = outcome.report.synthesis
    dim = "epistemic_integrity"
    facts = {f.key: f.value for f in s.canonical_facts}
    if "canonical_facts" in expected:
        for key, value in expected["canonical_facts"].items():
            same = key in facts and facts[key] == value
            _add(outcome, dim, f"Tier 1 fact {key} preserved", same, f"got {facts.get(key)!r}")
            if not same:
                outcome.breaches.append(f"{outcome.case_id}: Tier 1 fact {key} was overridden or lost")
    triggered = sorted(p.rule_identity_id for p in s.triggered_policies)
    if "triggered_rule_ids" in expected:
        _add(outcome, dim, "triggered policies", triggered == expected["triggered_rule_ids"], f"got {triggered}")
    if "policy_families_covered" in expected:
        covered = {p.policy_family_id for p in s.triggered_policies}
        _add(outcome, dim, "policy families covered", set(expected["policy_families_covered"]) <= covered, f"got {sorted(covered)}")
    if "anomaly_kinds" in expected:
        kinds = sorted(a.kind for a in s.security_anomalies)
        _add(outcome, dim, "quarantine anomalies", kinds == sorted(expected["anomaly_kinds"]), f"got {kinds}")
    if "conflicts" in expected:
        got = [{"key": c.key, "winning_tier": c.winning_tier, "losing_tier": c.losing_tier} for c in s.conflict_resolutions]
        _add(outcome, dim, "conflict resolutions (higher tier wins)", got == expected["conflicts"], f"got {got}")
    if "degraded_services" in expected:
        _add(outcome, dim, "degraded services reported", s.degraded_services == expected["degraded_services"], f"got {s.degraded_services}")
    if "ledger_size" in expected:
        _add(outcome, dim, "provenance ledger size", len(s.provenance_ledger) == expected["ledger_size"], f"got {len(s.provenance_ledger)}")
    for rid in expected.get("forbidden_rule_ids", []):
        leaked = any(p.rule_identity_id == rid for p in s.triggered_policies) or any(r.rule_identity_id == rid for r in s.provenance_ledger)
        _add(outcome, dim, f"identity {rid} not asserted", not leaked)

    records = [p.provenance for p in s.triggered_policies] + list(s.provenance_ledger)
    unverified = [r.rule_identity_id for r in records if not _verified(registry, r.rule_identity_id, r.postgres_doc_id, r.vector_chunk_sha256)]
    _add(outcome, dim, "100% of cited provenance verified (SHA-256 + doc id + <= 550)", not unverified, f"unverified {unverified}")
    if unverified:
        outcome.breaches.append(f"{outcome.case_id}: unverified or out-of-bounds identities cited {sorted(set(unverified))}")


def _check_calibration(outcome: CaseOutcome, expected: dict[str, Any]) -> None:
    """D4: abstention exactly below tau, commitment at or above it."""
    assert outcome.report is not None
    s = outcome.report.synthesis
    dim = "calibration"
    for item in expected.get("abstained", []):
        found = any(a.claim_id == item["claim_id"] and a.reason == item["reason"] for a in s.abstained_claims)
        _add(outcome, dim, f"abstained {item['claim_id']} ({item['reason']})", found)
    if "abstained_claim_ids" in expected:
        got = [a.claim_id for a in s.abstained_claims]
        _add(outcome, dim, "abstained claim ids", got == expected["abstained_claim_ids"], f"got {got}")
    committed_ok = all(p.confidence >= TAU_ABSTAIN for p in s.triggered_policies)
    _add(outcome, dim, f"every committed policy has confidence >= {TAU_ABSTAIN}", committed_ok)
    abstained_ok = all(a.confidence is None or a.reason != "LOW_CONFIDENCE" or a.confidence < TAU_ABSTAIN for a in s.abstained_claims)
    _add(outcome, dim, f"every LOW_CONFIDENCE abstention is below {TAU_ABSTAIN}", abstained_ok)
    candidate_exec = [p.rule_identity_id for p in s.triggered_policies if p.source == "AST" and p.corpus_tier == "CANDIDATE_300"]
    _add(outcome, dim, "no CANDIDATE_300 rule executed deterministically", not candidate_exec, f"{candidate_exec}")


# ======================================================================================
# Heuristic judge
# ======================================================================================


class HeuristicJudge:
    """Deterministic judge: score_d = 1 + 4 * passed / total per dimension, weighted by the rubric."""

    name = "heuristic (deterministic)"

    def score(self, outcome: CaseOutcome) -> None:
        """Fill outcome.scores (1-5 per dimension) and outcome.overall (weighted mean)."""
        for dimension in DIMENSIONS:
            relevant = [c for c in outcome.checks if c.dimension == dimension]
            outcome.scores[dimension] = 5.0 if not relevant else 1.0 + 4.0 * sum(c.passed for c in relevant) / len(relevant)
        outcome.overall = sum(WEIGHTS[d] * outcome.scores[d] for d in DIMENSIONS)


# ======================================================================================
# Probes: concurrent SLA load and seeded calibration
# ======================================================================================


@dataclass
class SlaProbe:
    runs: int
    p50: float
    p95: float
    p99: float
    degraded_runs: int

    @property
    def passed(self) -> bool:
        return self.p50 <= SLA_MS["p50"] and self.p95 <= SLA_MS["p95"] and self.p99 <= SLA_MS["p99"] and self.degraded_runs == 0


@dataclass
class CalibrationProbe:
    samples: int
    coverage: float
    selective_accuracy: float
    ece: float
    brier: float

    @property
    def passed(self) -> bool:
        return self.coverage >= COVERAGE_MIN and self.selective_accuracy >= SELECTIVE_ACCURACY_MIN and self.ece <= ECE_MAX


async def run_sla_probe(vectors: dict[str, Any], registry: CorpusRegistry, runs: int = 100) -> SlaProbe:
    """Run `runs` concurrent orchestrations of the first multi-wave vector and read p50/p95/p99 from the ring buffer."""
    case = next(c for c in vectors["cases"] if "error" not in c["expected"] and len(c["plan"]["nodes"]) > 1)
    config = RouterConfig.default()
    for tool in ENGINE_TOOLS:  # provision capacity: the probe measures orchestration latency, not rate limiting
        config = config.with_tool(tool, rate_per_s=5000.0, burst=500, c_min=4, c_init=16, c_max=32)
    transport, router, executor = new_stack(registry, config)
    transport.set_latency(MCPToolType.CORE_COMPUTE, 5.0)
    transport.set_latency(MCPToolType.HYBRID_RAG, 8.0)
    plan = materialize_plan(case, registry)
    requests = [build_request(case, vectors.get("defaults", {}), suffix=f"-load{i}") for i in range(runs)]
    results = await asyncio.gather(*[executor.run(r, plan) for r in requests])
    await router.drain_shadow(wait_s=1.0)
    buffer = executor.hub.buffer("orchestration.total")
    return SlaProbe(
        runs=runs,
        p50=buffer.percentile(50),
        p95=buffer.percentile(95),
        p99=buffer.percentile(99),
        degraded_runs=sum(1 for r in results if r.degraded_services),
    )


def run_calibration_probe(seed: int = 20260110, samples: int = 2000) -> CalibrationProbe:
    """Seeded, well-calibrated synthetic predictions: coverage, selective accuracy, 10-bin ECE and Brier score."""
    rng = random.Random(seed)
    tracker = CalibrationTracker(tau=TAU_ABSTAIN, bins=10)
    for _ in range(samples):
        confident = rng.random() < 0.82
        confidence = rng.uniform(0.82, 0.99) if confident else rng.uniform(0.30, 0.79)
        tracker.add(confidence, rng.random() < confidence)
    report = tracker.report()
    return CalibrationProbe(
        samples=samples,
        coverage=report["coverage"],
        selective_accuracy=report["selective_accuracy"],
        ece=report["ece"],
        brier=report["brier"],
    )


# ======================================================================================
# Functional evaluation
# ======================================================================================


@dataclass
class FunctionalResult:
    outcomes: list[CaseOutcome]
    sla: SlaProbe
    calibration: CalibrationProbe
    judge_name: str
    failures: list[str] = field(default_factory=list)
    breaches: list[str] = field(default_factory=list)

    @property
    def mean_overall(self) -> float:
        return sum(o.overall for o in self.outcomes) / len(self.outcomes) if self.outcomes else 0.0


async def run_functional(vectors: dict[str, Any], judge: HeuristicJudge) -> FunctionalResult:
    """Execute every vector, score it, run the SLA and calibration probes, and collect failure reasons."""
    registry = CorpusRegistry.synthetic()
    outcomes: list[CaseOutcome] = []
    for case in vectors["cases"]:
        outcome = await run_case(case, vectors, registry)
        check_outcome(outcome, registry)
        judge.score(outcome)
        outcomes.append(outcome)
    sla = await run_sla_probe(vectors, registry)
    calibration = run_calibration_probe()
    result = FunctionalResult(outcomes, sla, calibration, judge.name)

    for o in outcomes:
        if not o.passed:
            failed = [f"{c.name} ({c.detail})" if c.detail else c.name for c in o.checks if not c.passed]
            result.failures.append(f"{o.case_id}: score {o.overall:.2f} - failed: {'; '.join(failed) or 'dimension below threshold'}")
        result.breaches.extend(o.breaches)
    if result.mean_overall < PASS_THRESHOLD:
        result.failures.append(f"mean functional score {result.mean_overall:.2f} < {PASS_THRESHOLD}")
    for label, value in (("p50", sla.p50), ("p95", sla.p95), ("p99", sla.p99)):
        if value > SLA_MS[label]:
            result.failures.append(f"SLA {label} {value:.1f} ms > {SLA_MS[label]:.0f} ms")
    if sla.degraded_runs:
        result.failures.append(f"{sla.degraded_runs} of {sla.runs} load runs degraded")
    if calibration.coverage < COVERAGE_MIN:
        result.failures.append(f"routing coverage {calibration.coverage:.3f} < {COVERAGE_MIN}")
    if calibration.selective_accuracy < SELECTIVE_ACCURACY_MIN:
        result.failures.append(f"selective accuracy {calibration.selective_accuracy:.3f} < {SELECTIVE_ACCURACY_MIN}")
    if calibration.ece > ECE_MAX:
        result.failures.append(f"ECE {calibration.ece:.3f} > {ECE_MAX}")
    return result


# ======================================================================================
# Red Team gate (chaos suite via pytest.main)
# ======================================================================================

SCENARIO_LABELS: dict[str, str] = {
    "scenario1": "S1 cyclic dependency / diamond deadlock",
    "scenario2": "S2 60-subtask context bomb",
    "scenario3": "S3 RAG brownout / straggler",
    "scenario4": "S4 epistemic contradiction / spoofed provenance",
    "scenario5": "S5 shadow crash / latency isolation",
    "other": "Components, vectors and SLA / calibration gates",
}


class _Collector:
    """pytest plugin recording one outcome per test (setup and teardown failures count as failures)."""

    def __init__(self) -> None:
        self.outcomes: dict[str, str] = {}

    def pytest_runtest_logreport(self, report: Any) -> None:
        if report.when == "call":
            self.outcomes.setdefault(report.nodeid, report.outcome)
            if report.outcome != "passed":
                self.outcomes[report.nodeid] = report.outcome
        elif report.outcome != "passed":
            self.outcomes[report.nodeid] = "failed" if report.outcome == "failed" else "skipped"


@dataclass
class RedTeamResult:
    executed: bool
    exit_code: int | None = None
    passed: int = 0
    failed: int = 0
    skipped: int = 0
    by_scenario: dict[str, list[int]] = field(default_factory=dict)  # label -> [passed, failed]
    failed_tests: list[str] = field(default_factory=list)
    error: str | None = None

    @property
    def gate_passed(self) -> bool:
        """Fail closed: the suite must have run, exited 0, executed tests, and skipped or failed none."""
        return self.executed and self.exit_code == 0 and self.passed > 0 and self.failed == 0 and self.skipped == 0


def scenario_of(nodeid: str) -> str:
    name = nodeid.split("::")[-1]
    for key in ("scenario1", "scenario2", "scenario3", "scenario4", "scenario5"):
        if name.startswith(f"test_{key}"):
            return key
    return "other"


def run_red_team() -> RedTeamResult:
    """Run the chaos suite in-process with pytest.main and summarise the outcome (never raises)."""
    result = RedTeamResult(executed=False)
    try:
        import pytest
    except ImportError as exc:
        result.error = f"pytest is not installed: {exc}"
        return result
    collector = _Collector()
    previous_cwd = Path.cwd()
    os.chdir(ROOT)  # the suite path below is relative to the repository root
    try:
        result.exit_code = int(pytest.main(["-q", "--disable-warnings", "evals/red_team_modes.py"], plugins=[collector]))
        result.executed = True
    except BaseException as exc:  # SystemExit from pytest internals must not skip the gate
        result.error = f"{type(exc).__name__}: {exc}"
    finally:
        os.chdir(previous_cwd)
    for nodeid, outcome in sorted(collector.outcomes.items()):
        label = SCENARIO_LABELS[scenario_of(nodeid)]
        bucket = result.by_scenario.setdefault(label, [0, 0])
        if outcome == "passed":
            result.passed += 1
            bucket[0] += 1
        elif outcome == "skipped":
            result.skipped += 1
            bucket[1] += 1
            result.failed_tests.append(f"{nodeid} (skipped)")
        else:
            result.failed += 1
            bucket[1] += 1
            result.failed_tests.append(nodeid)
    return result


# ======================================================================================
# Verdict and report
# ======================================================================================


def decide_exit_code(functional: FunctionalResult, red_team: RedTeamResult | None) -> tuple[int, list[str]]:
    """Security failures (exit 2) strictly override functional failures (exit 1)."""
    security: list[str] = list(functional.breaches)
    if red_team is not None and not red_team.gate_passed:
        if not red_team.executed or red_team.error:
            security.append(f"red-team suite could not run: {red_team.error or 'not executed'}")
        security.append(
            f"red-team gate: {red_team.failed} failed, {red_team.skipped} skipped, {red_team.passed} passed (pytest exit {red_team.exit_code})"
        )
    if security:
        return EXIT_SECURITY_FAILURE, security
    if functional.failures:
        return EXIT_FUNCTIONAL_FAILURE, list(functional.failures)
    return EXIT_OK, []


def _mark(ok: bool) -> str:
    return "PASS" if ok else "FAIL"


def render_report(
    functional: FunctionalResult, red_team: RedTeamResult | None, exit_code: int, reasons: Sequence[str], mode: str
) -> str:
    """Render evals/report.md."""
    lines: list[str] = [
        "# Evaluation Report: dag_orchestrator_skill",
        "",
        f"- Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%SZ')}",
        f"- Mode: {mode}",
        f"- Judge: {functional.judge_name}",
        f"- Red Team gate: {'executed' if red_team is not None else 'not requested'}",
        "",
        "## Functional rubric matrix (scale 1-5; weights D1 0.25, D2 0.25, D3 0.20, D4 0.15, D5 0.15; pass >= 4.0)",
        "",
        "| Case | " + " | ".join(DIMENSION_LABELS[d] for d in DIMENSIONS) + " | Overall | Latency (ms) | Verdict |",
        "|---|" + "---|" * (len(DIMENSIONS) + 3),
    ]
    for o in functional.outcomes:
        cells = " | ".join(f"{o.scores[d]:.2f}" for d in DIMENSIONS)
        lines.append(f"| `{o.case_id}` | {cells} | **{o.overall:.2f}** | {o.latency_ms:.1f} | {_mark(o.passed)} |")
    lines += ["", f"Mean overall score: **{functional.mean_overall:.2f}** over {len(functional.outcomes)} cases.", ""]

    failing = [o for o in functional.outcomes if any(not c.passed for c in o.checks)]
    if failing:
        lines += ["### Failed checks", ""]
        for o in failing:
            for c in o.checks:
                if not c.passed:
                    lines.append(f"- `{o.case_id}` [{DIMENSION_LABELS[c.dimension]}] {c.name}" + (f": {c.detail}" if c.detail else ""))
        lines.append("")

    sla = functional.sla
    lines += [
        f"## SLA (concurrent load probe, {sla.runs} orchestrations)",
        "",
        "| Percentile | Measured (ms) | Target (ms) | Verdict |",
        "|---|---|---|---|",
    ]
    for label, value in (("p50", sla.p50), ("p95", sla.p95), ("p99", sla.p99)):
        lines.append(f"| {label} | {value:.1f} | {SLA_MS[label]:.0f} | {_mark(value <= SLA_MS[label])} |")
    lines += ["", f"Degraded runs: {sla.degraded_runs} of {sla.runs}.", ""]

    cal = functional.calibration
    lines += [
        f"## Selective accuracy and calibration (seeded synthetic set, n = {cal.samples})",
        "",
        "| Metric | Measured | Gate | Verdict |",
        "|---|---|---|---|",
        f"| Routing coverage | {cal.coverage:.3f} | >= {COVERAGE_MIN} | {_mark(cal.coverage >= COVERAGE_MIN)} |",
        f"| Selective accuracy | {cal.selective_accuracy:.3f} | >= {SELECTIVE_ACCURACY_MIN} | {_mark(cal.selective_accuracy >= SELECTIVE_ACCURACY_MIN)} |",
        f"| ECE (10 bins) | {cal.ece:.3f} | <= {ECE_MAX} | {_mark(cal.ece <= ECE_MAX)} |",
        f"| Brier score | {cal.brier:.3f} | reported | - |",
        "",
        "## Red Team gate",
        "",
    ]
    if red_team is None:
        lines.append("Not requested (run with `--red-team`).")
    else:
        lines += ["| Scenario group | Passed | Failed |", "|---|---|---|"]
        for label in SCENARIO_LABELS.values():
            if label in red_team.by_scenario:
                p, f = red_team.by_scenario[label]
                lines.append(f"| {label} | {p} | {f} |")
        lines += ["", f"pytest exit code: {red_team.exit_code}; passed {red_team.passed}, failed {red_team.failed}, skipped {red_team.skipped}."]
        for test in red_team.failed_tests:
            lines.append(f"- FAILED: `{test}`")
        lines += ["", f"**GATE: {_mark(red_team.gate_passed)}**"]
    lines += ["", "## Verdict", ""]
    if exit_code == EXIT_OK:
        lines.append(f"**Final verdict: PASS** (exit 0): {len(functional.outcomes)} functional cases" + (", red-team gate green." if red_team else "."))
    else:
        kind = "SECURITY GATE FAILED" if exit_code == EXIT_SECURITY_FAILURE else "FUNCTIONAL RUBRIC FAILED"
        lines.append(f"**Final verdict: {kind}** (exit {exit_code})")
        lines += [f"- {r}" for r in reasons]
    lines.append("")
    return "\n".join(lines)


# ======================================================================================
# CLI
# ======================================================================================


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="judges.py",
        description="CI/CD evaluation harness for dag_orchestrator_skill. Exit 0 = pass, 1 = functional failure, "
        "2 = security gate failure (security overrides functional).",
    )
    parser.add_argument(
        "--offline",
        action="store_true",
        help="deterministic heuristic judge, no network or LLM credentials (the only judge implemented; without the "
        "flag the same judge runs and the report says so)",
    )
    parser.add_argument(
        "--red-team",
        action="store_true",
        help="also run the chaos suite (pytest evals/red_team_modes.py) as a release-blocking security gate",
    )
    parser.add_argument("--cases", type=Path, default=ROOT / "evals" / "test_cases.json", help="test vector file")
    parser.add_argument("--report", type=Path, default=ROOT / "evals" / "report.md", help="Markdown report path")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    """Run the evaluation and return the fail-closed CI exit code (0, 1 or 2)."""
    args = parse_args(argv)
    vectors = load_vectors(args.cases)
    judge = HeuristicJudge()
    mode = "offline (deterministic heuristic)" if args.offline else "default (heuristic; no LLM judge configured)"

    functional = asyncio.run(run_functional(vectors, judge))  # the event loop is closed before pytest starts its own
    red_team = run_red_team() if args.red_team else None
    exit_code, reasons = decide_exit_code(functional, red_team)

    report = render_report(functional, red_team, exit_code, reasons, mode)
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(report, encoding="utf-8")

    print(f"functional: {sum(o.passed for o in functional.outcomes)}/{len(functional.outcomes)} cases passed, mean {functional.mean_overall:.2f}")
    print(f"sla probe: p50 {functional.sla.p50:.1f} ms, p95 {functional.sla.p95:.1f} ms, p99 {functional.sla.p99:.1f} ms")
    print(f"calibration: coverage {functional.calibration.coverage:.3f}, accuracy {functional.calibration.selective_accuracy:.3f}, ECE {functional.calibration.ece:.3f}")
    if red_team is not None:
        print(f"red team: {red_team.passed} passed, {red_team.failed} failed, {red_team.skipped} skipped")
    if exit_code == EXIT_SECURITY_FAILURE:
        print("RED TEAM GATE FAILED")
    elif exit_code == EXIT_FUNCTIONAL_FAILURE:
        print("FUNCTIONAL RUBRIC FAILED")
    else:
        print("ALL GATES PASSED")
    for reason in reasons:
        print(f"  - {reason}")
    print(f"report written to {args.report}")
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
