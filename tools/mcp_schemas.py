"""Strict, immutable Pydantic v2 data contracts for the ``dag_orchestrator_skill`` skill.

This module is the dependency leaf of the repository: every other module imports its contracts from here and
it imports nothing from the repository. All models are ``frozen=True, extra="forbid"``.

Corpus identity map (550 identities):

* ``1..50``    -> ``PROTECTED_50``   (protected baseline compliance rules)
* ``51..250``  -> ``ACTIVE_200``     (active production rules)
* ``251..550`` -> ``CANDIDATE_300``  (staging candidate rules)
"""

from __future__ import annotations

import re
from enum import Enum, IntEnum
from typing import Annotated, Any, Final, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

PROTECTED_RANGE: Final[tuple[int, int]] = (1, 50)
ACTIVE_RANGE: Final[tuple[int, int]] = (51, 250)
CANDIDATE_RANGE: Final[tuple[int, int]] = (251, 550)
MAX_RULE_IDENTITY: Final[int] = 550
POLICY_FAMILY_COUNT: Final[int] = 7
TAU_ABSTAIN: Final[float] = 0.80

SHA256_HEX_RE: Final[re.Pattern[str]] = re.compile(r"^[0-9a-f]{64}$")

#: ``$ref:$.waves.<wave_idx>.<task_id>.output[.<json_path>][|default=<json>]``
REF_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"^\$ref:\$\.waves\.(?P<wave>\d+)\.(?P<task>[A-Za-z0-9_\-]+)\.output"
    r"(?:\.(?P<path>[^|]+))?(?:\|default=(?P<default>.*))?$",
    re.DOTALL,
)

CorpusTier = Literal["PROTECTED_50", "ACTIVE_200", "CANDIDATE_300"]


def corpus_tier_for(rule_identity_id: int) -> CorpusTier:
    """Return the corpus tier of a rule identity.

    Raises:
        ValueError: if the identity is outside ``1..550`` or not an int.
    """
    if isinstance(rule_identity_id, bool) or not isinstance(rule_identity_id, int):
        raise ValueError(f"rule_identity_id must be an int, got {type(rule_identity_id).__name__}")
    if PROTECTED_RANGE[0] <= rule_identity_id <= PROTECTED_RANGE[1]:
        return "PROTECTED_50"
    if ACTIVE_RANGE[0] <= rule_identity_id <= ACTIVE_RANGE[1]:
        return "ACTIVE_200"
    if CANDIDATE_RANGE[0] <= rule_identity_id <= CANDIDATE_RANGE[1]:
        return "CANDIDATE_300"
    raise ValueError(f"rule_identity_id {rule_identity_id} is outside the 550-identity corpus boundary")


class MCPToolType(str, Enum):
    """The four MCP tool endpoints orchestrated by the skill."""

    CORE_COMPUTE = "tool_core_compute"
    POLICY_AST = "tool_policy_ast"
    HYBRID_RAG = "tool_hybrid_rag"
    SLM_INTENT_SHADOW = "tool_slm_intent_shadow"


class FailurePolicy(str, Enum):
    """What the orchestrator does when a node fails."""

    HARD_ABORT = "HARD_ABORT"
    DEGRADE_WITH_FALLBACK = "DEGRADE_WITH_FALLBACK"
    IGNORE_OPTIONAL = "IGNORE_OPTIONAL"


class EpistemicTier(IntEnum):
    """Epistemic precedence: lower value wins."""

    GROUND_TRUTH = 1
    SYMBOLIC_POLICY = 2
    VERIFIED_RAG = 3
    ABSTENTION = 4


class _Frozen(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class ProvenanceRecord(_Frozen):
    """Cryptographic provenance for one policy rule / retrieved chunk."""

    postgres_doc_id: int
    vector_chunk_sha256: str
    corpus_tier: CorpusTier
    policy_family_id: Annotated[int, Field(ge=1, le=POLICY_FAMILY_COUNT)]
    rule_identity_id: Annotated[int, Field(ge=1, le=MAX_RULE_IDENTITY)]

    @field_validator("vector_chunk_sha256")
    @classmethod
    def _validate_sha(cls, value: str) -> str:
        if not SHA256_HEX_RE.fullmatch(value):
            raise ValueError("vector_chunk_sha256 must be 64 lowercase hex characters")
        return value

    @model_validator(mode="after")
    def _validate_tier(self) -> "ProvenanceRecord":
        expected = corpus_tier_for(self.rule_identity_id)
        if expected != self.corpus_tier:
            raise ValueError(
                f"rule_identity_id {self.rule_identity_id} belongs to {expected}, not {self.corpus_tier}"
            )
        return self

    @property
    def key(self) -> tuple[int, str]:
        """Dedup key for the provenance ledger."""
        return (self.postgres_doc_id, self.vector_chunk_sha256)


class SubTaskNode(_Frozen):
    """A single node of the decomposed DAG."""

    task_id: Annotated[str, Field(pattern=r"^[A-Za-z0-9_\-]+$", min_length=1, max_length=96)]
    tool: MCPToolType
    dependencies: list[str] = Field(default_factory=list)
    timeout_ms: Annotated[int, Field(gt=0, le=10_000)]
    idempotent: bool
    priority: Annotated[int, Field(ge=1, le=5)]
    failure_policy: FailurePolicy
    estimated_latency_ms: Annotated[int, Field(ge=0, le=60_000)]
    payload: dict[str, Any] = Field(default_factory=dict)
    #: Latency of the cached fast path; enables "downgrade" instead of "prune" under budget pressure.
    fast_path_latency_ms: Annotated[int, Field(ge=0, le=60_000)] | None = None
    #: Set by the compiler when the node was downgraded to its cached fast path.
    use_cache: bool = False

    @model_validator(mode="after")
    def _validate_dependencies(self) -> "SubTaskNode":
        if self.task_id in self.dependencies:
            raise ValueError(f"node {self.task_id} depends on itself")
        if len(set(self.dependencies)) != len(self.dependencies):
            raise ValueError(f"node {self.task_id} lists duplicate dependencies")
        return self


class DAGPlanSchema(_Frozen):
    """Planner output: the decomposed DAG for one request."""

    request_id: str
    zero_hop: bool = False
    rationale: str = ""
    nodes: list[SubTaskNode] = Field(default_factory=list)

    @model_validator(mode="after")
    def _validate_unique_ids(self) -> "DAGPlanSchema":
        ids = [n.task_id for n in self.nodes]
        if len(set(ids)) != len(ids):
            raise ValueError("duplicate task_id in plan")
        if self.zero_hop and self.nodes:
            raise ValueError("zero_hop plans must contain no nodes")
        return self


class SecurityAnomaly(_Frozen):
    """A quarantined item: spoofed hash, out-of-bounds identity, schema violation, ..."""

    kind: str
    task_id: str
    detail: str
    rule_identity_id: int | None = None
    postgres_doc_id: int | None = None


class FallbackEnvelope(_Frozen):
    """Typed short-circuit result returned by an open breaker or a failed degrade path."""

    task_id: str
    failed_tool: MCPToolType
    fallback_tool: MCPToolType | None
    reason: str
    payload: dict[str, Any] = Field(default_factory=dict)
    degraded: bool = True


class ToolResult(_Frozen):
    """Outcome of dispatching one node."""

    task_id: str
    tool: MCPToolType
    status: Literal["ok", "degraded", "skipped", "failed"]
    output: dict[str, Any] = Field(default_factory=dict)
    latency_ms: float = 0.0
    from_cache: bool = False
    error: str | None = None
    provenance: list[ProvenanceRecord] = Field(default_factory=list)
    anomalies: list[SecurityAnomaly] = Field(default_factory=list)
    fallback: FallbackEnvelope | None = None


class WaveExecutionResult(_Frozen):
    """Results of one execution wave."""

    wave_index: int
    results: list[ToolResult]
    skipped_task_ids: list[str] = Field(default_factory=list)
    elapsed_ms: float


class CanonicalFact(_Frozen):
    key: str
    value: Any
    source_task_id: str
    tier: Literal[1] = 1


class TriggeredPolicy(_Frozen):
    rule_identity_id: Annotated[int, Field(ge=1, le=MAX_RULE_IDENTITY)]
    corpus_tier: CorpusTier
    policy_family_id: Annotated[int, Field(ge=1, le=POLICY_FAMILY_COUNT)]
    source: Literal["AST", "RAG"]
    confidence: Annotated[float, Field(ge=0.0, le=1.0)]
    epistemic_tier: Literal[2, 3]
    provenance: ProvenanceRecord


class AbstainedClaim(_Frozen):
    claim_id: str
    reason: str
    confidence: Annotated[float, Field(ge=0.0, le=1.0)] | None = None
    rule_identity_id: int | None = None
    epistemic_tier: Literal[4] = 4


class ConflictResolution(_Frozen):
    key: str
    winning_tier: Literal[1, 2]
    winning_value: Any
    losing_tier: Literal[2, 3]
    losing_value: Any
    losing_rule_identity_id: int | None = None
    losing_provenance: ProvenanceRecord | None = None


class TelemetrySummary(_Frozen):
    total_latency_ms: float
    p50_ms: float
    p95_ms: float
    p99_ms: float
    slack_ms: float
    sla_met: bool
    waves: int
    critical_path_ms: float
    pruned_nodes: list[str] = Field(default_factory=list)
    downgraded_nodes: list[str] = Field(default_factory=list)
    context_tokens: int = 0
    context_items_dropped: int = 0
    zero_hop: bool = False


class OrchestratorSynthesis(_Frozen):
    """Final output contract of the skill."""

    request_id: str
    state_fingerprint: Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
    canonical_facts: list[CanonicalFact]
    triggered_policies: list[TriggeredPolicy]
    provenance_ledger: list[ProvenanceRecord]
    abstained_claims: list[AbstainedClaim]
    degraded_services: list[str]
    telemetry_summary: TelemetrySummary
    conflict_resolutions: list[ConflictResolution] = Field(default_factory=list)
    security_anomalies: list[SecurityAnomaly] = Field(default_factory=list)
