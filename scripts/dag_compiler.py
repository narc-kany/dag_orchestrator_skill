"""Topological wave compiler, cycle detector and critical-path pruner.

Implements:

* Kahn's algorithm validation of a ``SubTaskNode`` list (cycle detection with the exact cycle path,
  dangling dependency detection, ``$ref`` ancestor validation).
* Wave partitioning with ``wave(u) = 0`` if ``deps(u)`` is empty else ``1 + max(wave(v) for v in deps(u))``.
* ``T_critical = sum_w max_{u in W_w} L(u)`` and iterative budget pruning when ``T_critical`` exceeds the
  execution ceiling (default 2600 ms, leaving 400 ms of the 3000 ms SLA for synthesis).
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping, Sequence

from tools.mcp_schemas import REF_PATTERN, FailurePolicy, SubTaskNode

DEFAULT_EXECUTION_BUDGET_MS: int = 2600
DEFAULT_MAX_NODES: int = 64
PRUNE_PRIORITY_THRESHOLD: int = 3


class DAGCompilationError(Exception):
    """Base class of all deterministic compilation failures."""


class CyclicDependencyError(DAGCompilationError):
    """Raised when the dependency graph contains a cycle or self-loop."""

    def __init__(self, cycle: Sequence[str]) -> None:
        self.cycle: tuple[str, ...] = tuple(cycle)
        super().__init__("cyclic dependency: " + " -> ".join(self.cycle))


class DanglingDependencyError(DAGCompilationError):
    """Raised when a node depends on a task id that does not exist."""

    def __init__(self, task_id: str, missing: str) -> None:
        self.task_id = task_id
        self.missing = missing
        super().__init__(f"node {task_id!r} depends on unknown task {missing!r}")


class InvalidReferenceError(DAGCompilationError):
    """Raised when a ``$ref`` binding does not point at a valid upstream ancestor."""

    def __init__(self, task_id: str, ref: str, reason: str) -> None:
        self.task_id = task_id
        self.ref = ref
        super().__init__(f"node {task_id!r} has invalid $ref {ref!r}: {reason}")


class BudgetInfeasibleError(DAGCompilationError):
    """Raised when pruning cannot bring ``T_critical`` under the budget."""

    def __init__(self, t_critical_ms: float, budget_ms: float) -> None:
        self.t_critical_ms = t_critical_ms
        self.budget_ms = budget_ms
        super().__init__(
            f"critical path {t_critical_ms:.0f}ms exceeds budget {budget_ms:.0f}ms and no prunable nodes remain"
        )


class DAGLimitError(DAGCompilationError):
    """Raised when the plan exceeds the configured node limit."""


@dataclass(frozen=True)
class CompiledDAG:
    """Result of compilation."""

    waves: tuple[tuple[SubTaskNode, ...], ...]
    wave_of: Mapping[str, int]
    t_critical_ms: float
    original_t_critical_ms: float
    pruned: tuple[str, ...] = ()
    downgraded: tuple[str, ...] = ()
    total_latency_ms: float = 0.0

    @property
    def speedup_ratio(self) -> float:
        """``S_wave = sum(L(u)) / T_critical`` (1.0 for an empty DAG)."""
        if self.t_critical_ms <= 0:
            return 1.0
        return self.total_latency_ms / self.t_critical_ms

    @property
    def node_count(self) -> int:
        return sum(len(w) for w in self.waves)


def iter_refs(value: Any) -> Iterable[str]:
    """Yield every ``$ref:`` string found anywhere inside a JSON-like value."""
    if isinstance(value, str):
        if value.startswith("$ref:"):
            yield value
    elif isinstance(value, Mapping):
        for item in value.values():
            yield from iter_refs(item)
    elif isinstance(value, (list, tuple)):
        for item in value:
            yield from iter_refs(item)


def _find_cycle(residual: Mapping[str, Sequence[str]]) -> list[str]:
    """Return one exact cycle ``u -> v -> ... -> u`` in prerequisite-to-dependent order.

    ``residual`` maps each node left over by Kahn's algorithm to its dependencies. Every residual node has at
    least one residual dependency, so following dependencies from any residual node must revisit a node.
    """
    start = min(residual)
    chain: list[str] = []
    seen: dict[str, int] = {}
    current = start
    while current not in seen:
        seen[current] = len(chain)
        chain.append(current)
        deps = sorted(d for d in residual[current] if d in residual)
        current = deps[0]
    loop = chain[seen[current]:] + [current]  # follows "depends on" edges
    loop.reverse()  # prerequisite -> dependent order
    return loop


def t_critical(waves: Sequence[Sequence[SubTaskNode]]) -> float:
    """``T_critical = sum_w max_{u in W_w} L(u)``."""
    return float(sum(max((n.estimated_latency_ms for n in wave), default=0) for wave in waves))


def _layer(nodes: Sequence[SubTaskNode]) -> tuple[list[list[SubTaskNode]], dict[str, int]]:
    """Layered Kahn algorithm. Returns ``(waves, wave_of)`` or raises a deterministic error."""
    by_id: dict[str, SubTaskNode] = {}
    for node in nodes:
        if node.task_id in by_id:
            raise DAGCompilationError(f"duplicate task_id {node.task_id!r}")
        by_id[node.task_id] = node
    dependents: dict[str, list[str]] = {tid: [] for tid in by_id}
    indegree: dict[str, int] = {}
    for node in nodes:
        for dep in node.dependencies:
            if dep not in by_id:
                raise DanglingDependencyError(node.task_id, dep)
            dependents[dep].append(node.task_id)
        indegree[node.task_id] = len(node.dependencies)

    ready: list[str] = sorted(tid for tid, deg in indegree.items() if deg == 0)
    waves: list[list[SubTaskNode]] = []
    wave_of: dict[str, int] = {}
    processed = 0
    while ready:
        index = len(waves)
        waves.append([by_id[tid] for tid in ready])
        following: list[str] = []
        for tid in ready:
            wave_of[tid] = index
            processed += 1
            for child in dependents[tid]:
                indegree[child] -= 1
                if indegree[child] == 0:
                    following.append(child)
        ready = sorted(following)

    if processed != len(by_id):
        residual = {tid: by_id[tid].dependencies for tid in by_id if tid not in wave_of}
        raise CyclicDependencyError(_find_cycle(residual))
    return waves, wave_of


def _ancestors(waves: Sequence[Sequence[SubTaskNode]]) -> dict[str, set[str]]:
    """Transitive dependency closure per node, computed in wave order."""
    ancestors: dict[str, set[str]] = {}
    for wave in waves:
        for node in wave:
            closure: set[str] = set()
            for dep in node.dependencies:
                closure.add(dep)
                closure |= ancestors[dep]
            ancestors[node.task_id] = closure
    return ancestors


def _validate_refs(
    nodes: Sequence[SubTaskNode],
    waves: Sequence[Sequence[SubTaskNode]],
    wave_of: Mapping[str, int],
) -> None:
    """Every ref must name an ancestor and carry the exact compiled wave index of that ancestor."""
    ancestors = _ancestors(waves)
    for node in nodes:
        for ref in iter_refs(node.payload):
            match = REF_PATTERN.match(ref)
            if match is None:
                raise InvalidReferenceError(node.task_id, ref, "malformed reference")
            target = match.group("task")
            wave_text = match.group("wave")
            if target not in wave_of:
                raise InvalidReferenceError(node.task_id, ref, f"unknown task {target!r}")
            if target not in ancestors[node.task_id]:
                raise InvalidReferenceError(node.task_id, ref, f"{target!r} is not an ancestor")
            if int(wave_text) != wave_of[target]:
                raise InvalidReferenceError(
                    node.task_id, ref, f"wave index {wave_text} != compiled wave {wave_of[target]}"
                )


def _is_optional(node: SubTaskNode) -> bool:
    return node.priority >= PRUNE_PRIORITY_THRESHOLD and node.failure_policy != FailurePolicy.HARD_ABORT


def _descendants(nodes: Mapping[str, SubTaskNode], root: str) -> set[str]:
    children: dict[str, list[str]] = {tid: [] for tid in nodes}
    for node in nodes.values():
        for dep in node.dependencies:
            children[dep].append(node.task_id)
    found: set[str] = set()
    queue: deque[str] = deque(children[root])
    while queue:
        tid = queue.popleft()
        if tid in found:
            continue
        found.add(tid)
        queue.extend(children[tid])
    return found


def _apply_action(
    current: Mapping[str, SubTaskNode], node: SubTaskNode
) -> tuple[dict[str, SubTaskNode], list[str], list[str]]:
    """Downgrade ``node`` to its cached fast path if possible, otherwise remove it and its descendants."""
    updated = dict(current)
    if (
        node.fast_path_latency_ms is not None
        and not node.use_cache
        and node.fast_path_latency_ms < node.estimated_latency_ms
    ):
        updated[node.task_id] = node.model_copy(
            update={"estimated_latency_ms": node.fast_path_latency_ms, "use_cache": True}
        )
        return updated, [], [node.task_id]
    removed = sorted({node.task_id} | _descendants(current, node.task_id))
    for tid in removed:
        del updated[tid]
    return updated, removed, []


def _prune_to_budget(
    nodes: Sequence[SubTaskNode], budget_ms: float
) -> tuple[list[SubTaskNode], list[str], list[str]]:
    """Iteratively downgrade or prune optional nodes until ``T_critical <= budget_ms``.

    Candidates are nodes with ``priority >= 3`` (never ``HARD_ABORT``) whose descendants are all optional too,
    so no critical node ever loses an upstream dependency. Candidates are ordered by lowest value first
    (highest priority number, then largest latency). The first action that strictly lowers ``T_critical``
    is applied; if none does, the lowest-value action is applied anyway. Progress is guaranteed because every
    step downgrades or removes at least one node.
    """
    current: dict[str, SubTaskNode] = {n.task_id: n for n in nodes}
    pruned: list[str] = []
    downgraded: list[str] = []
    while True:
        waves, _ = _layer(list(current.values()))
        total = t_critical(waves)
        if total <= budget_ms:
            break
        candidates = [
            n
            for n in current.values()
            if _is_optional(n) and all(_is_optional(current[d]) for d in _descendants(current, n.task_id))
        ]
        if not candidates:
            raise BudgetInfeasibleError(total, budget_ms)
        candidates.sort(key=lambda n: (-n.priority, -n.estimated_latency_ms, n.task_id))
        chosen: tuple[dict[str, SubTaskNode], list[str], list[str]] | None = None
        for candidate in candidates:
            trial = _apply_action(current, candidate)
            trial_waves, _ = _layer(list(trial[0].values()))
            if t_critical(trial_waves) < total:
                chosen = trial
                break
        if chosen is None:
            chosen = _apply_action(current, candidates[0])
        current, removed, lowered = chosen
        pruned.extend(removed)
        downgraded.extend(lowered)
    ordered = [current[n.task_id] for n in nodes if n.task_id in current]
    return ordered, pruned, downgraded


def compile_dag(
    nodes: Sequence[SubTaskNode],
    *,
    budget_ms: float = DEFAULT_EXECUTION_BUDGET_MS,
    max_nodes: int = DEFAULT_MAX_NODES,
    max_waves: int = 6,
    prune: bool = True,
) -> CompiledDAG:
    """Validate, wave-partition and (if necessary) prune a decomposed task list.

    Raises:
        DAGLimitError: more than ``max_nodes`` nodes or more than ``max_waves`` waves.
        DanglingDependencyError: a dependency id does not exist.
        CyclicDependencyError: the graph has a cycle (exact path attached).
        InvalidReferenceError: a ref is malformed, not an ancestor or has a wrong wave index.
        BudgetInfeasibleError: pruning cannot satisfy ``budget_ms``.
    """
    if len(nodes) > max_nodes:
        raise DAGLimitError(f"{len(nodes)} nodes exceed the limit of {max_nodes}")
    waves, wave_of = _layer(nodes)
    _validate_refs(nodes, waves, wave_of)
    original = t_critical(waves)

    pruned: list[str] = []
    downgraded: list[str] = []
    final_nodes: list[SubTaskNode] = list(nodes)
    if prune and original > budget_ms:
        final_nodes, pruned, downgraded = _prune_to_budget(nodes, budget_ms)
        waves, wave_of = _layer(final_nodes)

    if len(waves) > max_waves:
        raise DAGLimitError(f"{len(waves)} waves exceed the limit of {max_waves}")
    return CompiledDAG(
        waves=tuple(tuple(w) for w in waves),
        wave_of=dict(wave_of),
        t_critical_ms=t_critical(waves),
        original_t_critical_ms=original,
        pruned=tuple(pruned),
        downgraded=tuple(downgraded),
        total_latency_ms=float(sum(n.estimated_latency_ms for n in final_nodes)),
    )
