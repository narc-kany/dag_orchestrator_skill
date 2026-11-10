"""Latency ring buffer, tracing spans, calibration metrics and the SLM shadow auditor.

* :class:`LatencyRingBuffer` - bounded, pre-allocated ring (capacity 10 000) of execution latencies. A single
  writer (the asyncio event loop) records without locks; the record path never awaits, so it is atomic with
  respect to other coroutines. Percentiles use nearest-rank.
* :class:`async_trace_span` - both a decorator and an async context manager, timed with ``perf_counter_ns``.
* :func:`expected_calibration_error` - M-bin ECE: ``sum_m |B_m|/N * |acc(B_m) - conf(B_m)|``; plus Brier score.
* :class:`ShadowAuditor` - writes ``tool_slm_intent_shadow`` comparisons to ``slm_shadow_telemetry`` only. It never
  returns data into the main path and never raises.
"""

from __future__ import annotations

import functools
import math
import time
from array import array
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Deque, Sequence, TypeVar

T = TypeVar("T")

RING_CAPACITY: int = 10_000
TOTAL_BUDGET_MS: float = 3000.0
SHADOW_SINK_NAME: str = "slm_shadow_telemetry"


# --------------------------------------------------------------------------------------
# Ring buffer
# --------------------------------------------------------------------------------------


class LatencyRingBuffer:
    """Pre-allocated circular buffer of latencies in milliseconds."""

    def __init__(self, capacity: int = RING_CAPACITY) -> None:
        if capacity <= 0:
            raise ValueError("capacity must be positive")
        self.capacity = capacity
        self._data = array("d", bytes(8 * capacity))
        self._head = 0
        self._size = 0
        self._total_recorded = 0

    def record(self, latency_ms: float) -> None:
        self._data[self._head] = latency_ms
        self._head = (self._head + 1) % self.capacity
        if self._size < self.capacity:
            self._size += 1
        self._total_recorded += 1

    def __len__(self) -> int:
        return self._size

    @property
    def total_recorded(self) -> int:
        return self._total_recorded

    def _values(self) -> list[float]:
        if self._size < self.capacity:
            return list(self._data[: self._size])
        return list(self._data)

    def percentile(self, p: float) -> float:
        """Nearest-rank percentile ``p`` in (0, 100]; 0.0 when empty."""
        if not 0.0 < p <= 100.0:
            raise ValueError("p must be in (0, 100]")
        values = self._values()
        if not values:
            return 0.0
        values.sort()
        rank = max(1, math.ceil(p / 100.0 * len(values)))
        return values[rank - 1]

    def snapshot(self) -> dict[str, float]:
        values = self._values()
        if not values:
            return {"count": 0.0, "mean": 0.0, "max": 0.0, "p50": 0.0, "p90": 0.0, "p95": 0.0, "p99": 0.0}
        values.sort()
        n = len(values)

        def nr(p: float) -> float:
            return values[max(1, math.ceil(p / 100.0 * n)) - 1]

        return {
            "count": float(n),
            "mean": sum(values) / n,
            "max": values[-1],
            "p50": nr(50),
            "p90": nr(90),
            "p95": nr(95),
            "p99": nr(99),
        }


def slack_ms(elapsed_ms: float, budget_ms: float = TOTAL_BUDGET_MS) -> float:
    """``Slack_ms = budget - T_elapsed``."""
    return budget_ms - elapsed_ms


class TelemetryHub:
    """Named collection of ring buffers."""

    def __init__(self, capacity: int = RING_CAPACITY) -> None:
        self.capacity = capacity
        self._buffers: dict[str, LatencyRingBuffer] = {}

    def buffer(self, name: str) -> LatencyRingBuffer:
        buf = self._buffers.get(name)
        if buf is None:
            buf = LatencyRingBuffer(self.capacity)
            self._buffers[name] = buf
        return buf

    def record(self, name: str, latency_ms: float) -> None:
        self.buffer(name).record(latency_ms)

    def names(self) -> list[str]:
        return sorted(self._buffers)


DEFAULT_HUB = TelemetryHub()


# --------------------------------------------------------------------------------------
# Trace span (decorator + async context manager)
# --------------------------------------------------------------------------------------


class async_trace_span:  # noqa: N801 - public API name mandated by the specification
    """Time a block or an async function with ``time.perf_counter_ns``.

    As a context manager::

        async with async_trace_span("orchestration.total") as span:
            ...
            remaining = span.slack_ms

    As a decorator::

        @async_trace_span("tool.rag")
        async def fetch(...): ...

    The latency is recorded into the hub ring buffer named ``name`` even if the body raises.
    """

    def __init__(
        self,
        name: str,
        *,
        hub: TelemetryHub | None = None,
        budget_ms: float = TOTAL_BUDGET_MS,
    ) -> None:
        self.name = name
        self.hub = hub if hub is not None else DEFAULT_HUB
        self.budget_ms = budget_ms
        self._start_ns: int | None = None
        self._end_ns: int | None = None
        self.error: BaseException | None = None

    @property
    def elapsed_ms(self) -> float:
        """Elapsed milliseconds so far (final value once the span has exited)."""
        if self._start_ns is None:
            return 0.0
        end = self._end_ns if self._end_ns is not None else time.perf_counter_ns()
        return (end - self._start_ns) / 1e6

    @property
    def slack_ms(self) -> float:
        return slack_ms(self.elapsed_ms, self.budget_ms)

    async def __aenter__(self) -> "async_trace_span":
        self._start_ns = time.perf_counter_ns()
        self._end_ns = None
        self.error = None
        return self

    async def __aexit__(self, exc_type: Any, exc: BaseException | None, tb: Any) -> None:
        self._end_ns = time.perf_counter_ns()
        self.error = exc
        self.hub.record(self.name, self.elapsed_ms)

    def __call__(self, func: Callable[..., Awaitable[T]]) -> Callable[..., Awaitable[T]]:
        name, hub, budget = self.name, self.hub, self.budget_ms

        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> T:
            async with async_trace_span(name, hub=hub, budget_ms=budget):
                return await func(*args, **kwargs)

        return wrapper


# --------------------------------------------------------------------------------------
# Calibration metrics
# --------------------------------------------------------------------------------------


@dataclass(frozen=True)
class CalibrationBin:
    index: int
    lower: float
    upper: float
    count: int
    accuracy: float
    confidence: float


def _check_inputs(confidences: Sequence[float], correct: Sequence[bool], bins: int) -> None:
    if len(confidences) != len(correct):
        raise ValueError("confidences and correct must have the same length")
    if bins <= 0:
        raise ValueError("bins must be positive")
    for c in confidences:
        if not 0.0 <= c <= 1.0:
            raise ValueError(f"confidence {c} outside [0, 1]")


def calibration_bins(confidences: Sequence[float], correct: Sequence[bool], bins: int = 10) -> list[CalibrationBin]:
    """Partition predictions into ``bins`` equal-width confidence bins (the top bin includes 1.0)."""
    _check_inputs(confidences, correct, bins)
    counts = [0] * bins
    hits = [0] * bins
    conf_sum = [0.0] * bins
    for conf, ok in zip(confidences, correct):
        idx = min(bins - 1, int(conf * bins))
        counts[idx] += 1
        hits[idx] += 1 if ok else 0
        conf_sum[idx] += conf
    result: list[CalibrationBin] = []
    for m in range(bins):
        n = counts[m]
        result.append(
            CalibrationBin(
                index=m,
                lower=m / bins,
                upper=(m + 1) / bins,
                count=n,
                accuracy=(hits[m] / n) if n else 0.0,
                confidence=(conf_sum[m] / n) if n else 0.0,
            )
        )
    return result


def expected_calibration_error(confidences: Sequence[float], correct: Sequence[bool], bins: int = 10) -> float:
    """``ECE = sum_m |B_m| / N * |acc(B_m) - conf(B_m)|`` (0.0 for no data)."""
    total = len(confidences)
    if total == 0:
        return 0.0
    return sum(b.count / total * abs(b.accuracy - b.confidence) for b in calibration_bins(confidences, correct, bins) if b.count)


def brier_score(confidences: Sequence[float], correct: Sequence[bool]) -> float:
    """Mean squared error between confidence and the 0/1 outcome (0.0 for no data)."""
    _check_inputs(confidences, correct, 1)
    if not confidences:
        return 0.0
    return sum((c - (1.0 if ok else 0.0)) ** 2 for c, ok in zip(confidences, correct)) / len(confidences)


class CalibrationTracker:
    """Accumulates (confidence, correct) pairs and reports the production quality gates."""

    def __init__(self, tau: float = 0.80, bins: int = 10) -> None:
        self.tau = tau
        self.bins = bins
        self._confidences: list[float] = []
        self._correct: list[bool] = []

    def add(self, confidence: float, correct: bool) -> None:
        if not 0.0 <= confidence <= 1.0:
            raise ValueError(f"confidence {confidence} outside [0, 1]")
        self._confidences.append(confidence)
        self._correct.append(bool(correct))

    def __len__(self) -> int:
        return len(self._confidences)

    def _committed(self) -> tuple[list[float], list[bool]]:
        pairs = [(c, k) for c, k in zip(self._confidences, self._correct) if c >= self.tau]
        return [p[0] for p in pairs], [p[1] for p in pairs]

    @property
    def coverage(self) -> float:
        """Fraction of predictions committed (confidence >= tau)."""
        return len(self._committed()[0]) / len(self._confidences) if self._confidences else 0.0

    @property
    def selective_accuracy(self) -> float:
        """Accuracy over committed predictions only."""
        confs, oks = self._committed()
        return sum(oks) / len(oks) if oks else 0.0

    def ece(self, *, committed_only: bool = True) -> float:
        if committed_only:
            confs, oks = self._committed()
            return expected_calibration_error(confs, oks, self.bins)
        return expected_calibration_error(self._confidences, self._correct, self.bins)

    def brier(self) -> float:
        return brier_score(self._confidences, self._correct)

    def report(self) -> dict[str, float]:
        return {
            "n": float(len(self)),
            "coverage": self.coverage,
            "selective_accuracy": self.selective_accuracy,
            "ece": self.ece(),
            "ece_all": self.ece(committed_only=False),
            "brier": self.brier(),
        }


@dataclass(frozen=True)
class ShadowRecord:
    request_id: str
    timestamp_ns: int
    canonical_route: str
    shadow_route: str | None
    shadow_confidence: float | None
    agree: bool
    abstained: bool
    shadow_latency_ms: float
    canonical_latency_ms: float
    latency_delta_ms: float
    calibration_bin: int | None
    fault: str | None


class ShadowAuditor:
    """Sink for tool_slm_intent_shadow telemetry (slm_shadow_telemetry).

    Nothing here is ever returned to the orchestration path, and log_shadow_comparison never raises.
    """

    def __init__(self, capacity: int = RING_CAPACITY, *, bins: int = 10, tau: float = 0.80) -> None:
        self.sink_name = SHADOW_SINK_NAME
        self.slm_shadow_telemetry: Deque[ShadowRecord] = deque(maxlen=capacity)
        self.bins = bins
        self._calibration = CalibrationTracker(tau=tau, bins=bins)
        self.internal_errors = 0

    def log_shadow_comparison(
        self,
        *,
        request_id: str,
        canonical_route: str,
        shadow_route: str | None,
        shadow_confidence: float | None,
        shadow_latency_ms: float,
        canonical_latency_ms: float,
        abstained: bool = False,
        fault: str | None = None,
    ) -> None:
        """Record one comparison between the SLM shadow prediction and the canonical deterministic routing."""
        try:
            agree = fault is None and not abstained and shadow_route == canonical_route
            bin_index: int | None = None
            if fault is None and shadow_confidence is not None and 0.0 <= shadow_confidence <= 1.0:
                bin_index = min(self.bins - 1, int(shadow_confidence * self.bins))
                self._calibration.add(shadow_confidence, agree)
            self.slm_shadow_telemetry.append(
                ShadowRecord(
                    request_id=request_id,
                    timestamp_ns=time.time_ns(),
                    canonical_route=canonical_route,
                    shadow_route=shadow_route,
                    shadow_confidence=shadow_confidence,
                    agree=agree,
                    abstained=abstained,
                    shadow_latency_ms=shadow_latency_ms,
                    canonical_latency_ms=canonical_latency_ms,
                    latency_delta_ms=shadow_latency_ms - canonical_latency_ms,
                    calibration_bin=bin_index,
                    fault=fault,
                )
            )
        except Exception:
            # Telemetry must never affect the caller.
            self.internal_errors += 1

    def records(self) -> list[ShadowRecord]:
        return list(self.slm_shadow_telemetry)

    def summary(self) -> dict[str, Any]:
        records = self.records()
        faults: dict[str, int] = {}
        for rec in records:
            if rec.fault is not None:
                faults[rec.fault] = faults.get(rec.fault, 0) + 1
        healthy = [r for r in records if r.fault is None]
        return {
            "sink": self.sink_name,
            "records": len(records),
            "fault_counts": faults,
            "agreement_rate": (sum(1 for r in healthy if r.agree) / len(healthy)) if healthy else 0.0,
            "abstention_rate": (sum(1 for r in healthy if r.abstained) / len(healthy)) if healthy else 0.0,
            "mean_latency_delta_ms": (sum(r.latency_delta_ms for r in healthy) / len(healthy)) if healthy else 0.0,
            "ece": self._calibration.ece(committed_only=False),
            "brier": self._calibration.brier(),
            "internal_errors": self.internal_errors,
        }
