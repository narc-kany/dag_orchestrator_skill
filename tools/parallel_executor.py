"""Wave-based DAG execution engine and the deterministic epistemic reconciler.

ParallelDAGExecutor compiles the plan (scripts.dag_compiler), executes it wave by wave with asyncio.TaskGroup,
interpolates upstream outputs into downstream payloads (scripts.state_manager), compresses accumulated state into
the 4096-token window, reconciles conflicts with strict Tier 1 > 2 > 3 > 4 precedence and emits a validated
OrchestratorSynthesis. The SLM shadow is forked at the start and never awaited.
"""

from __future__ import annotations

import asyncio
import math
import time
from dataclasses import dataclass, field
from typing import Any, Mapping, Sequence

from scripts.dag_compiler import CompiledDAG, compile_dag
from scripts.state_manager import (
    CompressionResult,
    ContextItem,
    KnapsackTokenCompressor,
    MemoCache,
    RefContext,
    UnresolvedReferenceError,
    canonical_json,
    resolve_refs,
    state_fingerprint,
    subtask_key,
)
from scripts.telemetry_logger import TOTAL_BUDGET_MS, TelemetryHub, async_trace_span, slack_ms
from tools.mcp_router import HardAbortError, MCPClientRouter
from tools.mcp_schemas import (
    TAU_ABSTAIN,
    AbstainedClaim,
    CanonicalFact,
    ConflictResolution,
    DAGPlanSchema,
    FailurePolicy,
    MCPToolType,
    OrchestratorSynthesis,
    ProvenanceRecord,
    SecurityAnomaly,
    SubTaskNode,
    TelemetrySummary,
    ToolResult,
    TriggeredPolicy,
    WaveExecutionResult,
)


@dataclass(frozen=True)
class OrchestrationRequest:
    """Inputs of one orchestration, including the GlobalExecutionContext facts."""

    request_id: str
    query: str
    tenant_id: str
    entity_id: str
    as_of_timestamp: str
    policy_version: str
    precomputed_facts: Mapping[str, Any] = field(default_factory=dict)
    required_fact_keys: tuple[str, ...] = ()
    history: tuple[str, ...] = ()
    canonical_route: str = "deterministic"


@dataclass(frozen=True)
class ReconciliationOutcome:
    facts: list[CanonicalFact]
    policies: list[TriggeredPolicy]
    abstained: list[AbstainedClaim]
    conflicts: list[ConflictResolution]
    ledger: list[ProvenanceRecord]
    anomalies: list[SecurityAnomaly]
    degraded: list[str]


def values_equal(a: Any, b: Any) -> bool:
    """Numeric tolerance |a-b| <= max(1e-9, 1e-6 * |a|); exact equality otherwise."""
    numeric = (int, float)
    if isinstance(a, numeric) and isinstance(b, numeric) and not isinstance(a, bool) and not isinstance(b, bool):
        return math.isclose(a, b, rel_tol=1e-6, abs_tol=1e-9)
    return bool(a == b)


def _record_of(item: Mapping[str, Any]) -> ProvenanceRecord:
    return ProvenanceRecord(
        postgres_doc_id=item["postgres_doc_id"],
        vector_chunk_sha256=item["vector_chunk_sha256"],
        corpus_tier=item["corpus_tier"],
        policy_family_id=item["policy_family_id"],
        rule_identity_id=item["rule_identity_id"],
    )


class EpistemicReconciler:
    """Deterministic Tier 1 > 2 > 3 > 4 reconciliation of verified tool results."""

    def __init__(self, tau: float = TAU_ABSTAIN) -> None:
        self.tau = tau

    def reconcile(
        self,
        request: OrchestrationRequest,
        results: Sequence[ToolResult],
        *,
        zero_hop: bool = False,
    ) -> ReconciliationOutcome:
        ordered = sorted(results, key=lambda r: (r.task_id, r.tool.value))
        anomalies = [a for r in ordered for a in r.anomalies]
        usable = [r for r in ordered if r.status in ("ok", "degraded")]

        # Tier 1: context facts, then live compute (fresher, same tier).
        facts: dict[str, CanonicalFact] = {
            k: CanonicalFact(key=k, value=v, source_task_id="context") for k, v in request.precomputed_facts.items()
        }
        for r in usable:
            if r.tool is MCPToolType.CORE_COMPUTE:
                for key, value in r.output.get("facts", {}).items():
                    facts[key] = CanonicalFact(key=key, value=value, source_task_id=r.task_id)

        abstained: list[AbstainedClaim] = []
        if zero_hop:
            for key in request.required_fact_keys:
                if key not in facts:
                    abstained.append(AbstainedClaim(claim_id=f"fact:{key}", reason="MISSING_CANONICAL_FACT"))

        policies: dict[tuple[int, str], TriggeredPolicy] = {}
        ast_verdicts: dict[int, bool] = {}
        self._tier2(usable, abstained, policies, ast_verdicts)
        conflicts = self._tier3(usable, facts, abstained, policies, ast_verdicts)

        ordered_policies = [policies[k] for k in sorted(policies)]
        ledger: dict[tuple[int, str], ProvenanceRecord] = {}
        for policy in ordered_policies:
            ledger.setdefault(policy.provenance.key, policy.provenance)
        degraded: set[str] = set()
        for r in ordered:
            if r.status != "ok":
                degraded.add(r.fallback.failed_tool.value if r.fallback is not None else r.tool.value)
        return ReconciliationOutcome(
            facts=[facts[k] for k in sorted(facts)],
            policies=ordered_policies,
            abstained=sorted(abstained, key=lambda a: a.claim_id),
            conflicts=conflicts,
            ledger=[ledger[k] for k in sorted(ledger)],
            anomalies=anomalies,
            degraded=sorted(degraded),
        )

    def _tier2(
        self,
        usable: Sequence[ToolResult],
        abstained: list[AbstainedClaim],
        policies: dict[tuple[int, str], TriggeredPolicy],
        ast_verdicts: dict[int, bool],
    ) -> None:
        """Symbolic evaluations of protected and active identities; staging or low confidence goes to Tier 4."""
        for r in usable:
            if r.tool is not MCPToolType.POLICY_AST:
                continue
            for ev in r.output.get("evaluations", []):
                rid = ev["rule_identity_id"]
                confidence = float(ev.get("confidence", 1.0))
                if ev["corpus_tier"] == "CANDIDATE_300":
                    abstained.append(
                        AbstainedClaim(claim_id=f"ast:{rid}", reason="CANDIDATE_RULE_NOT_ACTIVE", confidence=confidence, rule_identity_id=rid)
                    )
                    continue
                if confidence < self.tau:
                    abstained.append(
                        AbstainedClaim(claim_id=f"ast:{rid}", reason="LOW_CONFIDENCE", confidence=confidence, rule_identity_id=rid)
                    )
                    continue
                verdict = bool(ev["verdict"])
                ast_verdicts[rid] = verdict
                if verdict:
                    policies[(rid, "AST")] = TriggeredPolicy(
                        rule_identity_id=rid,
                        corpus_tier=ev["corpus_tier"],
                        policy_family_id=ev["policy_family_id"],
                        source="AST",
                        confidence=confidence,
                        epistemic_tier=2,
                        provenance=_record_of(ev),
                    )

    def _tier3(
        self,
        usable: Sequence[ToolResult],
        facts: Mapping[str, CanonicalFact],
        abstained: list[AbstainedClaim],
        policies: dict[tuple[int, str], TriggeredPolicy],
        ast_verdicts: Mapping[int, bool],
    ) -> list[ConflictResolution]:
        """Verified RAG chunks: abstain below tau, yield to Tier 1 facts and Tier 2 verdicts, otherwise assert."""
        conflicts: list[ConflictResolution] = []
        for r in usable:
            if r.tool is not MCPToolType.HYBRID_RAG:
                continue
            for chunk in r.output.get("chunks", []):
                rid = chunk["rule_identity_id"]
                confidence = float(chunk["confidence"])
                if confidence < self.tau:
                    abstained.append(
                        AbstainedClaim(claim_id=f"rag:{rid}", reason="LOW_CONFIDENCE", confidence=confidence, rule_identity_id=rid)
                    )
                    continue
                record = _record_of(chunk)
                dropped = False
                claims = chunk.get("claims") or {}
                for key in sorted(claims):
                    if key in facts and not values_equal(facts[key].value, claims[key]):
                        conflicts.append(
                            ConflictResolution(
                                key=key,
                                winning_tier=1,
                                winning_value=facts[key].value,
                                losing_tier=3,
                                losing_value=claims[key],
                                losing_rule_identity_id=rid,
                                losing_provenance=record,
                            )
                        )
                        dropped = True
                verdict = chunk.get("rule_verdict")
                if verdict is not None and rid in ast_verdicts and bool(verdict) != ast_verdicts[rid]:
                    conflicts.append(
                        ConflictResolution(
                            key=f"rule:{rid}",
                            winning_tier=2,
                            winning_value=ast_verdicts[rid],
                            losing_tier=3,
                            losing_value=bool(verdict),
                            losing_rule_identity_id=rid,
                            losing_provenance=record,
                        )
                    )
                    dropped = True
                if dropped:
                    continue
                if verdict is True and rid not in ast_verdicts:
                    policies[(rid, "RAG")] = TriggeredPolicy(
                        rule_identity_id=rid,
                        corpus_tier=chunk["corpus_tier"],
                        policy_family_id=chunk["policy_family_id"],
                        source="RAG",
                        confidence=confidence,
                        epistemic_tier=3,
                        provenance=record,
                    )
        return conflicts


@dataclass(frozen=True)
class ExecutionReport:
    """Synthesis plus execution internals (for evaluation and debugging)."""

    synthesis: OrchestratorSynthesis
    waves: list[WaveExecutionResult]
    compiled: CompiledDAG | None


class ParallelDAGExecutor:
    """Compile, execute wave by wave, reconcile and synthesize."""

    def __init__(
        self,
        router: MCPClientRouter,
        *,
        memo: MemoCache | None = None,
        compressor: KnapsackTokenCompressor | None = None,
        reconciler: EpistemicReconciler | None = None,
        hub: TelemetryHub | None = None,
        total_budget_ms: float = TOTAL_BUDGET_MS,
        exec_budget_ms: float = 2600.0,
        max_nodes: int = 64,
        enable_shadow: bool = True,
    ) -> None:
        self.router = router
        self.memo = memo if memo is not None else MemoCache()
        self.compressor = compressor or KnapsackTokenCompressor()
        self.reconciler = reconciler or EpistemicReconciler()
        self.hub = hub if hub is not None else TelemetryHub()
        self.total_budget_ms = total_budget_ms
        self.exec_budget_ms = exec_budget_ms
        self.max_nodes = max_nodes
        self.enable_shadow = enable_shadow

    async def run(self, request: OrchestrationRequest, plan: DAGPlanSchema | Sequence[SubTaskNode]) -> OrchestratorSynthesis:
        """Execute ``plan`` for ``request`` and return the validated synthesis."""
        return (await self.run_detailed(request, plan)).synthesis

    async def run_detailed(
        self, request: OrchestrationRequest, plan: DAGPlanSchema | Sequence[SubTaskNode]
    ) -> ExecutionReport:
        """Like run, but also returns per-wave results and the compiled DAG.

        Raises:
            HardAbortError: a HARD_ABORT node failed.
            DAGCompilationError: the plan is cyclic, dangling, has bad refs or cannot meet the budget.
            ContextBudgetExceededError: pinned priority 1 context alone exceeds the token window.
        """
        # The span only measures; the latency is recorded once, explicitly, so percentiles include this run.
        async with async_trace_span("orchestration.span", hub=TelemetryHub(capacity=1), budget_ms=self.total_budget_ms) as span:
            fingerprint = state_fingerprint(
                tenant_id=request.tenant_id,
                entity_id=request.entity_id,
                as_of_timestamp=request.as_of_timestamp,
                policy_version=request.policy_version,
                query_signature=request.query,
            )
            if self.enable_shadow:
                self.router.fork_shadow(
                    request_id=request.request_id,
                    query=request.query,
                    canonical_route=request.canonical_route,
                    canonical_latency_ms=lambda: span.elapsed_ms,
                )
            nodes = list(plan.nodes) if isinstance(plan, DAGPlanSchema) else list(plan)
            zero_hop = not nodes
            compiled: CompiledDAG | None = None
            results: list[ToolResult] = []
            waves: list[WaveExecutionResult] = []
            if not zero_hop:
                compiled = compile_dag(nodes, budget_ms=self.exec_budget_ms, max_nodes=self.max_nodes)
                results, waves = await self._execute(compiled, fingerprint)

            outcome = self.reconciler.reconcile(request, results, zero_hop=zero_hop)
            compression = self._compress(request, results, outcome)
            total_ms = span.elapsed_ms
            self.hub.record("orchestration.total", total_ms)
            buffer = self.hub.buffer("orchestration.total")
            telemetry = TelemetrySummary(
                total_latency_ms=total_ms,
                p50_ms=buffer.percentile(50),
                p95_ms=buffer.percentile(95),
                p99_ms=buffer.percentile(99),
                slack_ms=slack_ms(total_ms, self.total_budget_ms),
                sla_met=total_ms <= self.total_budget_ms,
                waves=len(compiled.waves) if compiled else 0,
                critical_path_ms=compiled.t_critical_ms if compiled else 0.0,
                pruned_nodes=list(compiled.pruned) if compiled else [],
                downgraded_nodes=list(compiled.downgraded) if compiled else [],
                context_tokens=compression.tokens_used,
                context_items_dropped=len(compression.dropped_ids),
                zero_hop=zero_hop,
            )
            synthesis = OrchestratorSynthesis(
                request_id=request.request_id,
                state_fingerprint=fingerprint,
                canonical_facts=outcome.facts,
                triggered_policies=outcome.policies,
                provenance_ledger=outcome.ledger,
                abstained_claims=outcome.abstained,
                degraded_services=outcome.degraded,
                telemetry_summary=telemetry,
                conflict_resolutions=outcome.conflicts,
                security_anomalies=outcome.anomalies,
            )
            return ExecutionReport(synthesis=synthesis, waves=waves, compiled=compiled)

    # ------------------------------------------------------------------ wave execution
    async def _execute(self, compiled: CompiledDAG, fingerprint: str) -> tuple[list[ToolResult], list[WaveExecutionResult]]:
        exec_started = time.perf_counter_ns()
        outputs: dict[str, Mapping[str, Any]] = {}
        all_results: list[ToolResult] = []
        waves: list[WaveExecutionResult] = []
        for index, wave in enumerate(compiled.waves):
            wave_started = time.perf_counter_ns()
            remaining_ms = self.exec_budget_ms - (wave_started - exec_started) / 1e6
            results: dict[str, ToolResult] = {}
            aborts: list[HardAbortError] = []
            if remaining_ms > 0:
                context = RefContext(outputs=outputs, wave_of=compiled.wave_of)
                try:
                    async with asyncio.timeout(remaining_ms / 1000.0):
                        async with asyncio.TaskGroup() as group:
                            for node in wave:
                                group.create_task(self._run_node(node, context, fingerprint, results, aborts))
                except TimeoutError:
                    remaining_ms = 0.0  # wave deadline reached; unfinished nodes are resolved below per failure policy
            skipped: list[str] = []
            for node in wave:
                if node.task_id not in results and not any(a.task_id == node.task_id for a in aborts):
                    skipped.append(node.task_id)
                    self._record_failure(node, "execution_deadline_exceeded", results, aborts)
            if aborts:
                raise aborts[0]
            for node in wave:
                result = results[node.task_id]
                self.hub.record(f"tool.{result.tool.value}", result.latency_ms)
                if result.status in ("ok", "degraded"):
                    outputs[node.task_id] = result.output
                all_results.append(result)
            waves.append(
                WaveExecutionResult(
                    wave_index=index,
                    results=[results[n.task_id] for n in wave],
                    skipped_task_ids=skipped,
                    elapsed_ms=(time.perf_counter_ns() - wave_started) / 1e6,
                )
            )
        return all_results, waves

    async def _run_node(
        self,
        node: SubTaskNode,
        context: RefContext,
        fingerprint: str,
        results: dict[str, ToolResult],
        aborts: list[HardAbortError],
    ) -> None:
        """Run one node. Catches Exception (never BaseException) so siblings are not cancelled."""
        try:
            payload = resolve_refs(node.payload, context)
            results[node.task_id] = await self._dispatch(node, payload, fingerprint)
        except HardAbortError as exc:
            aborts.append(exc)
        except UnresolvedReferenceError as exc:
            self._record_failure(node, f"unresolved reference: {exc}", results, aborts)
        except Exception as exc:
            self._record_failure(node, f"{type(exc).__name__}: {exc}", results, aborts)

    @staticmethod
    def _record_failure(
        node: SubTaskNode, reason: str, results: dict[str, ToolResult], aborts: list[HardAbortError]
    ) -> None:
        if node.failure_policy is FailurePolicy.HARD_ABORT:
            aborts.append(HardAbortError(node.task_id, node.tool, reason))
            return
        results[node.task_id] = ToolResult(
            task_id=node.task_id,
            tool=node.tool,
            status="skipped" if node.failure_policy is FailurePolicy.IGNORE_OPTIONAL else "failed",
            error=reason,
        )

    async def _dispatch(self, node: SubTaskNode, payload: Mapping[str, Any], fingerprint: str) -> ToolResult:
        """Dispatch with memoisation of identical idempotent subtasks under the same state fingerprint."""
        if not node.idempotent:
            return await self.router.dispatch(node, payload)
        key = subtask_key(fingerprint, node.tool.value, {"payload": payload, "use_cache": node.use_cache})
        result, from_cache = await self.memo.get_or_compute(
            key, lambda: self.router.dispatch(node, payload), cache_if=lambda r: r.status == "ok"
        )
        if from_cache:
            return result.model_copy(update={"task_id": node.task_id, "from_cache": True, "latency_ms": 0.0})
        return result

    # ------------------------------------------------------------------ context compression
    def _compress(
        self, request: OrchestrationRequest, results: Sequence[ToolResult], outcome: ReconciliationOutcome
    ) -> CompressionResult:
        items: list[ContextItem] = []
        if request.precomputed_facts:
            items.append(ContextItem("p1:context", 1, canonical_json(dict(request.precomputed_facts))))
        for r in results:
            if r.status not in ("ok", "degraded"):
                continue
            if r.tool is MCPToolType.CORE_COMPUTE:
                items.append(ContextItem(f"p1:{r.task_id}", 1, canonical_json(r.output.get("facts", {}))))
            elif r.tool is MCPToolType.POLICY_AST:
                for ev in r.output.get("evaluations", []):
                    if ev["corpus_tier"] == "PROTECTED_50":
                        items.append(
                            ContextItem(f"p1:{r.task_id}:r{ev['rule_identity_id']}", 1, f"r{ev['rule_identity_id']}={int(bool(ev['verdict']))}")
                        )
            elif r.tool is MCPToolType.HYBRID_RAG:
                for chunk in r.output.get("chunks", []):
                    tag = f"[doc={chunk['postgres_doc_id']} sha={chunk['vector_chunk_sha256']}]"
                    items.append(
                        ContextItem(
                            f"p2:{r.task_id}:{chunk['rule_identity_id']}",
                            2,
                            str(chunk.get("text", "")),
                            relevance=float(chunk["confidence"]),
                            provenance_tag=tag,
                        )
                    )
        for turn, text in enumerate(request.history):
            items.append(ContextItem(f"p3:h{turn}", 3, text, turn_index=turn))
        return self.compressor.compress(items)
