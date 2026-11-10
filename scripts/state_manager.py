"""Canonical fingerprinting, memoisation, inter-wave ``$ref`` resolution and the knapsack token compressor.

* :func:`state_fingerprint` - deterministic SHA-256 over normalised, key-sorted request parameters
  (``tenant_id``, ``entity_id``, ``as_of_timestamp``, ``policy_version``, ``query_signature``).
* :class:`MemoCache` - TTL cache keyed by fingerprint with in-flight de-duplication.
* :func:`resolve_refs` - resolves ``$ref:$.waves.<wave_idx>.<task_id>.output.<json_path>`` placeholders
  (with optional ``|default=<json>``) before a child wave is dispatched.
* :class:`KnapsackTokenCompressor` - enforces ``W_max`` (4096) tokens by solving a tiered bounded knapsack:

  ``max sum(v_i x_i)  s.t.  sum(w_i x_i) <= W_max,  x_i in {0, 1}``

  Priority 1 items are pinned (infinite value, never truncated). Priority 2 items are structurally compressed and
  selected by 0/1 knapsack DP. Priority 3 items are evicted FIFO (oldest first) and only fill the leftover room.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import math
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Mapping, Protocol, Sequence, TypeVar

from tools.mcp_schemas import REF_PATTERN

T = TypeVar("T")

W_MAX_TOKENS: int = 4096


class StateError(Exception):
    """Base class for state manager errors."""


class UnresolvedReferenceError(StateError):
    """A ``$ref`` could not be resolved and carried no default."""


class ContextBudgetExceededError(StateError):
    """Priority 1 (pinned) content alone exceeds the token window."""


# --------------------------------------------------------------------------------------
# Canonical fingerprint
# --------------------------------------------------------------------------------------


def _normalise_timestamp(value: str | datetime) -> str:
    """Normalise a timestamp to UTC ISO-8601 with microsecond precision (naive values are taken as UTC)."""
    if isinstance(value, datetime):
        parsed = value
    else:
        text = value.strip()
        if text.endswith(("Z", "z")):
            text = text[:-1] + "+00:00"
        parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).isoformat(timespec="microseconds")


def canonical_json(value: Any) -> str:
    """Key-sorted, whitespace-free, ASCII-safe JSON used for hashing."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, default=str)


def state_fingerprint(
    *,
    tenant_id: str,
    entity_id: str,
    as_of_timestamp: str | datetime,
    policy_version: str,
    query_signature: str,
) -> str:
    """SHA-256 hex digest over the normalised request parameters."""
    normalised = {
        "tenant_id": tenant_id.strip(),
        "entity_id": entity_id.strip(),
        "as_of_timestamp": _normalise_timestamp(as_of_timestamp),
        "policy_version": policy_version.strip(),
        "query_signature": " ".join(query_signature.split()),
    }
    return hashlib.sha256(canonical_json(normalised).encode("utf-8")).hexdigest()


def subtask_key(fingerprint: str, tool: str, payload: Mapping[str, Any]) -> str:
    """Memoisation key for one subtask under one state fingerprint."""
    return hashlib.sha256(canonical_json([fingerprint, tool, payload]).encode("utf-8")).hexdigest()


# --------------------------------------------------------------------------------------
# Memo cache with in-flight de-duplication
# --------------------------------------------------------------------------------------


class MemoCache:
    """TTL memo cache. Concurrent callers for the same key share one computation."""

    def __init__(self, ttl_s: float = 30.0, *, max_entries: int = 4096, clock: Callable[[], float] = time.monotonic) -> None:
        self.ttl_s = ttl_s
        self.max_entries = max_entries
        self._clock = clock
        self._entries: dict[str, tuple[float, Any]] = {}
        self._inflight: dict[str, asyncio.Future[Any]] = {}
        self.hits = 0
        self.misses = 0

    def get(self, key: str) -> tuple[bool, Any]:
        entry = self._entries.get(key)
        if entry is None:
            return False, None
        expires, value = entry
        if self._clock() >= expires:
            del self._entries[key]
            return False, None
        return True, value

    def put(self, key: str, value: Any) -> None:
        if len(self._entries) >= self.max_entries:
            oldest = min(self._entries, key=lambda k: self._entries[k][0])
            del self._entries[oldest]
        self._entries[key] = (self._clock() + self.ttl_s, value)

    async def get_or_compute(
        self,
        key: str,
        factory: Callable[[], Awaitable[T]],
        *,
        cache_if: Callable[[T], bool] = lambda _value: True,
    ) -> tuple[T, bool]:
        """Return ``(value, from_cache)``. Failed or non-cacheable results are not stored."""
        found, value = self.get(key)
        if found:
            self.hits += 1
            return value, True
        pending = self._inflight.get(key)
        if pending is not None:
            self.hits += 1
            return await asyncio.shield(pending), True
        self.misses += 1
        future: asyncio.Future[Any] = asyncio.get_running_loop().create_future()
        future.add_done_callback(lambda f: f.exception() if not f.cancelled() else None)
        self._inflight[key] = future
        try:
            result = await factory()
        except asyncio.CancelledError:
            future.set_exception(StateError("memo leader cancelled"))
            raise
        except Exception as exc:
            future.set_exception(exc)
            raise
        else:
            if cache_if(result):
                self.put(key, result)
            future.set_result(result)
            return result, False
        finally:
            self._inflight.pop(key, None)


# --------------------------------------------------------------------------------------
# Inter-wave $ref resolution
# --------------------------------------------------------------------------------------


@dataclass(frozen=True)
class RefContext:
    """Upstream state visible to ``$ref`` resolution.

    ``outputs`` maps ``task_id`` to its output dict and only contains nodes that produced usable output; pruned,
    skipped or failed nodes are absent, which makes their refs fall back to ``|default=``.
    """

    outputs: Mapping[str, Mapping[str, Any]]
    wave_of: Mapping[str, int]


def _walk_path(value: Any, path: str) -> Any:
    current = value
    for part in path.split("."):
        if isinstance(current, Mapping):
            if part not in current:
                raise KeyError(part)
            current = current[part]
        elif isinstance(current, (list, tuple)):
            try:
                current = current[int(part)]
            except (ValueError, IndexError) as exc:
                raise KeyError(part) from exc
        else:
            raise KeyError(part)
    return current


def _parse_default(text: str) -> Any:
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return text


def resolve_ref(ref: str, context: RefContext) -> Any:
    """Resolve one ``$ref`` string to a typed value (or its default)."""
    match = REF_PATTERN.match(ref)
    if match is None:
        raise UnresolvedReferenceError(f"malformed reference: {ref}")
    task = match.group("task")
    path = match.group("path")
    default = match.group("default")
    declared_wave = int(match.group("wave"))
    actual_wave = context.wave_of.get(task)
    if actual_wave is not None and actual_wave != declared_wave:
        raise UnresolvedReferenceError(f"reference {ref} declares wave {declared_wave}, task is in wave {actual_wave}")
    output = context.outputs.get(task)
    if output is None:
        if default is not None:
            return _parse_default(default)
        raise UnresolvedReferenceError(f"upstream task {task} produced no output for {ref}")
    if path is None:
        return dict(output)
    try:
        return _walk_path(output, path)
    except KeyError as exc:
        if default is not None:
            return _parse_default(default)
        raise UnresolvedReferenceError(f"path {path} missing in output of {task}") from exc


def resolve_refs(payload: Any, context: RefContext) -> Any:
    """Recursively resolve every ``$ref`` placeholder inside a JSON-like payload (returns a new structure)."""
    if isinstance(payload, str):
        return resolve_ref(payload, context) if payload.startswith("$ref:") else payload
    if isinstance(payload, Mapping):
        return {key: resolve_refs(item, context) for key, item in payload.items()}
    if isinstance(payload, (list, tuple)):
        return [resolve_refs(item, context) for item in payload]
    return payload


# --------------------------------------------------------------------------------------
# Tokenizer and structural compression
# --------------------------------------------------------------------------------------


class Tokenizer(Protocol):
    def count(self, text: str) -> int: ...


class CharTokenizer:
    """Deterministic estimator ``ceil(chars / 4)``."""

    def count(self, text: str) -> int:
        return math.ceil(len(text) / 4) if text else 0


_MD_PREFIX = re.compile(r"^\s{0,3}(?:#{1,6}\s+|[-*+]\s+|\d+\.\s+|>\s+)")
_MD_LINK = re.compile(r"\[([^\]]*)\]\([^)]*\)")
_MD_EMPHASIS = re.compile(r"(\*\*|__|`{1,3})")
_FILLER = re.compile(
    r"^(?:sure|certainly|of course|please note|note that|in summary|to summarize|as per|"
    r"according to the document|it is important to note that|basically)[,:]?\s*",
    re.IGNORECASE,
)
_HASH64 = re.compile(r"\b[0-9a-fA-F]{64}\b")
_SIGNAL = re.compile(
    r"(\d|[<>=]|!=|\b(?:AND|OR|NOT|IF|THEN|ELSE|IMPLIES|MUST|SHALL)\b|postgres_doc_id|rule_|\$ref)"
)
_SENTENCE_SPLIT = re.compile(r"(?<=[.;!?])\s+")


def structural_compress(text: str) -> str:
    """Strip markdown and conversational prose, keeping predicates, numeric thresholds and hashes verbatim."""
    kept: list[str] = []
    for line in text.splitlines():
        cleaned = _MD_PREFIX.sub("", line)
        cleaned = _MD_LINK.sub(r"\1", cleaned)
        cleaned = _MD_EMPHASIS.sub("", cleaned).strip()
        if not cleaned:
            continue
        for sentence in _SENTENCE_SPLIT.split(cleaned):
            sentence = _FILLER.sub("", sentence).strip()
            if sentence and (_SIGNAL.search(sentence) or _HASH64.search(sentence)):
                kept.append(sentence)
    return " ".join(" ".join(kept).split())


# --------------------------------------------------------------------------------------
# Tiered knapsack compressor
# --------------------------------------------------------------------------------------


@dataclass(frozen=True)
class ContextItem:
    """One unit of context competing for the token window.

    Attributes:
        priority: 1 pinned, 2 structurally compressible evidence, 3 evictable history.
        relevance: knapsack value base for priority 2 (0.0 - 1.0).
        turn_index: chronological index for priority 3 (larger is newer).
        provenance_tag: preserved verbatim (never compressed) and appended after the content.
    """

    item_id: str
    priority: int
    content: str
    relevance: float = 0.5
    turn_index: int | None = None
    provenance_tag: str = ""

    def render(self) -> str:
        return f"{self.content} {self.provenance_tag}".strip() if self.provenance_tag else self.content


@dataclass(frozen=True)
class CompressionResult:
    kept: tuple[ContextItem, ...]
    dropped_ids: tuple[str, ...]
    compressed_ids: tuple[str, ...]
    tokens_used: int
    p1_tokens: int
    w_max: int

    @property
    def pinned_ids(self) -> tuple[str, ...]:
        return tuple(i.item_id for i in self.kept if i.priority == 1)


def knapsack_select(weights: Sequence[int], values: Sequence[int], capacity: int, *, max_cells: int = 400_000) -> list[int]:
    """0/1 knapsack ``max sum(v_i x_i)  s.t.  sum(w_i x_i) <= capacity``. Returns the chosen indices.

    Weights are quantised upward (ceil) to a granule ``g`` chosen so the DP table has at most ``max_cells`` cells;
    rounding weights up and the capacity down keeps every returned selection feasible for the true weights.
    """
    count = len(weights)
    if count == 0 or capacity <= 0:
        return []
    granule = max(1, math.ceil(count * capacity / max_cells))
    qweights = [-(-w // granule) for w in weights]
    cap = capacity // granule
    best = [0] * (cap + 1)
    take = [bytearray(cap + 1) for _ in range(count)]
    for i in range(count):
        w, v, row = qweights[i], values[i], take[i]
        if w > cap:
            continue
        for c in range(cap, w - 1, -1):
            candidate = best[c - w] + v
            if candidate > best[c]:
                best[c] = candidate
                row[c] = 1
    chosen: list[int] = []
    c = cap
    for i in range(count - 1, -1, -1):
        if take[i][c]:
            chosen.append(i)
            c -= qweights[i]
    chosen.reverse()
    return chosen


class KnapsackTokenCompressor:
    """Fits prioritised context into ``W_max`` tokens."""

    def __init__(self, w_max: int = W_MAX_TOKENS, tokenizer: Tokenizer | None = None) -> None:
        if w_max <= 0:
            raise ValueError("w_max must be positive")
        self.w_max = w_max
        self.tokenizer: Tokenizer = tokenizer or CharTokenizer()

    def compress(self, items: Sequence[ContextItem], w_max: int | None = None) -> CompressionResult:
        """Select and compress ``items`` so the total token cost is at most ``w_max``.

        Raises:
            ContextBudgetExceededError: pinned priority 1 content alone exceeds the window.
            ValueError: an item has a priority outside 1..3.
        """
        window = w_max if w_max is not None else self.w_max
        count = self.tokenizer.count
        for item in items:
            if item.priority not in (1, 2, 3):
                raise ValueError(f"item {item.item_id} has invalid priority {item.priority}")
        pinned = [i for i in items if i.priority == 1]
        evidence = [i for i in items if i.priority == 2]
        history = [i for i in items if i.priority == 3]

        p1_tokens = sum(count(i.render()) for i in pinned)
        if p1_tokens > window:
            raise ContextBudgetExceededError(f"pinned priority-1 context needs {p1_tokens} tokens > window {window}")
        remaining = window - p1_tokens

        compressed_ids: list[str] = []
        shrunk: list[ContextItem] = []
        for item in evidence:
            reduced = structural_compress(item.content)
            if reduced != item.content:
                compressed_ids.append(item.item_id)
            shrunk.append(replace_content(item, reduced))
        weights = [count(i.render()) for i in shrunk]
        dropped: list[str] = []
        if sum(weights) <= remaining:
            chosen_idx = list(range(len(shrunk)))
        else:
            values = [int(max(0.0, min(1.0, i.relevance)) * 1000) + 1 for i in shrunk]
            chosen_idx = knapsack_select(weights, values, remaining)
        chosen_set = set(chosen_idx)
        kept_evidence = [shrunk[i] for i in range(len(shrunk)) if i in chosen_set]
        dropped.extend(shrunk[i].item_id for i in range(len(shrunk)) if i not in chosen_set)
        p2_tokens = sum(weights[i] for i in chosen_idx)
        remaining -= p2_tokens

        ordered_history = sorted(
            enumerate(history), key=lambda pair: (pair[1].turn_index if pair[1].turn_index is not None else pair[0])
        )
        kept_history: list[ContextItem] = []
        p3_tokens = 0
        evicting = False
        for _, item in reversed(ordered_history):  # newest first: FIFO eviction removes the oldest
            cost = count(item.render())
            if not evicting and cost <= remaining - p3_tokens:
                kept_history.append(item)
                p3_tokens += cost
            else:
                evicting = True
                dropped.append(item.item_id)
        kept_history.reverse()

        kept = tuple(pinned) + tuple(kept_evidence) + tuple(kept_history)
        return CompressionResult(
            kept=kept,
            dropped_ids=tuple(dropped),
            compressed_ids=tuple(compressed_ids),
            tokens_used=p1_tokens + p2_tokens + p3_tokens,
            p1_tokens=p1_tokens,
            w_max=window,
        )


def replace_content(item: ContextItem, content: str) -> ContextItem:
    """Return a copy of ``item`` with new content (the provenance tag is untouched)."""
    return ContextItem(
        item_id=item.item_id,
        priority=item.priority,
        content=content,
        relevance=item.relevance,
        turn_index=item.turn_index,
        provenance_tag=item.provenance_tag,
    )
