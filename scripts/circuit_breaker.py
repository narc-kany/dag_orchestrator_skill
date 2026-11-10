"""EWMA latency-aware three-state circuit breaker (CLOSED / OPEN / HALF_OPEN) with full-jitter backoff.

One :class:`CircuitBreaker` guards one MCP tool endpoint. State transitions are serialised by an
``asyncio.Lock``. The breaker trips from CLOSED to OPEN when any of the following holds:

* consecutive failures ``>= consecutive_failures`` (default 5);
* sliding-window failure rate ``> failure_rate_threshold`` (default 40 percent over 30 s, with a sample floor);
* ``L_ewma > ewma_trip_ms`` (default 2200 ms) where

  ``L_ewma(t) = alpha * L_observed(t) + (1 - alpha) * L_ewma(t - 1)``  (seeded with the first observation).

While OPEN the breaker short-circuits immediately. After a full-jitter backoff
``t_sleep = uniform(0, min(T_max, T_base * 2**attempt))`` it admits probe calls in HALF_OPEN. A successful probe
streak closes the breaker; a failed probe re-opens it with a larger backoff window.

Cancellation is never a failure: a cancelled call (for example a hedge loser) releases its probe slot and
does not affect the failure statistics.
"""

from __future__ import annotations

import asyncio
import random
import time
from collections import deque
from dataclasses import dataclass
from enum import Enum
from typing import Awaitable, Callable, Protocol, TypeVar

from tools.mcp_schemas import FallbackEnvelope

T = TypeVar("T")


class BreakerState(str, Enum):
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"


class SupportsUniform(Protocol):
    def uniform(self, a: float, b: float) -> float: ...


@dataclass(frozen=True)
class BreakerConfig:
    """Tunable breaker parameters."""

    consecutive_failures: int = 5
    failure_rate_threshold: float = 0.40
    window_s: float = 30.0
    min_samples: int = 10
    ewma_alpha: float = 0.3
    ewma_trip_ms: float = 2200.0
    ewma_min_samples: int = 3
    base_backoff_s: float = 0.5
    max_backoff_s: float = 15.0
    half_open_max_probes: int = 1
    half_open_successes_to_close: int = 2

    def __post_init__(self) -> None:
        if not 0.0 < self.ewma_alpha < 1.0:
            raise ValueError("ewma_alpha must be in (0, 1)")
        if self.consecutive_failures < 1 or self.half_open_max_probes < 1:
            raise ValueError("consecutive_failures and half_open_max_probes must be >= 1")


@dataclass(frozen=True)
class BreakerSnapshot:
    name: str
    state: BreakerState
    ewma_ms: float | None
    consecutive_failures: int
    window_failure_rate: float
    window_samples: int
    trip_count: int
    attempt: int
    reopen_in_s: float
    last_trip_reason: str | None


class CircuitBreaker:
    """Async-safe three-state circuit breaker for one tool endpoint."""

    def __init__(
        self,
        name: str,
        config: BreakerConfig | None = None,
        *,
        clock: Callable[[], float] = time.monotonic,
        rng: SupportsUniform | None = None,
    ) -> None:
        self.name = name
        self.config = config or BreakerConfig()
        self._clock = clock
        self._rng: SupportsUniform = rng if rng is not None else random.Random()
        self._lock = asyncio.Lock()
        self._state = BreakerState.CLOSED
        self._ewma: float | None = None
        self._ewma_samples = 0
        self._consecutive = 0
        self._window: deque[tuple[float, bool]] = deque()
        self._reopen_at = 0.0
        self._attempt = 0
        self._probes_in_flight = 0
        self._probe_successes = 0
        self._trip_count = 0
        self._last_trip_reason: str | None = None

    # ------------------------------------------------------------------ introspection
    @property
    def state(self) -> BreakerState:
        return self._state

    @property
    def ewma_ms(self) -> float | None:
        return self._ewma

    def snapshot(self) -> BreakerSnapshot:
        now = self._clock()
        self._evict(now)
        total = len(self._window)
        failures = sum(1 for _, ok in self._window if not ok)
        return BreakerSnapshot(
            name=self.name,
            state=self._state,
            ewma_ms=self._ewma,
            consecutive_failures=self._consecutive,
            window_failure_rate=(failures / total) if total else 0.0,
            window_samples=total,
            trip_count=self._trip_count,
            attempt=self._attempt,
            reopen_in_s=max(0.0, self._reopen_at - now) if self._state is BreakerState.OPEN else 0.0,
            last_trip_reason=self._last_trip_reason,
        )

    # ------------------------------------------------------------------ internals (lock held)
    def _evict(self, now: float) -> None:
        cutoff = now - self.config.window_s
        while self._window and self._window[0][0] < cutoff:
            self._window.popleft()

    def _observe_latency(self, latency_ms: float) -> None:
        alpha = self.config.ewma_alpha
        if self._ewma is None:
            self._ewma = latency_ms
        else:
            self._ewma = alpha * latency_ms + (1.0 - alpha) * self._ewma
        self._ewma_samples += 1

    def _trip(self, reason: str, now: float) -> None:
        window = min(self.config.max_backoff_s, self.config.base_backoff_s * (2 ** self._attempt))
        sleep_s = self._rng.uniform(0.0, window)
        self._state = BreakerState.OPEN
        self._reopen_at = now + sleep_s
        self._attempt += 1
        self._trip_count += 1
        self._last_trip_reason = reason
        self._probes_in_flight = 0
        self._probe_successes = 0

    def _close(self) -> None:
        self._state = BreakerState.CLOSED
        self._attempt = 0
        self._consecutive = 0
        self._window.clear()
        self._ewma = None
        self._ewma_samples = 0
        self._probes_in_flight = 0
        self._probe_successes = 0

    def _should_trip(self, now: float) -> str | None:
        cfg = self.config
        if self._consecutive >= cfg.consecutive_failures:
            return f"consecutive_failures>={cfg.consecutive_failures}"
        self._evict(now)
        total = len(self._window)
        if total >= cfg.min_samples:
            rate = sum(1 for _, ok in self._window if not ok) / total
            if rate > cfg.failure_rate_threshold:
                return f"failure_rate={rate:.2f}>{cfg.failure_rate_threshold:.2f}"
        if (
            self._ewma is not None
            and self._ewma_samples >= cfg.ewma_min_samples
            and self._ewma > cfg.ewma_trip_ms
        ):
            return f"ewma_latency={self._ewma:.0f}ms>{cfg.ewma_trip_ms:.0f}ms"
        return None

    # ------------------------------------------------------------------ public API
    async def admit(self) -> "Admission":
        """Decide whether a call may proceed. Handles the OPEN -> HALF_OPEN transition."""
        async with self._lock:
            now = self._clock()
            if self._state is BreakerState.OPEN:
                if now < self._reopen_at:
                    return Admission(allowed=False, probe=False)
                self._state = BreakerState.HALF_OPEN
                self._probes_in_flight = 0
                self._probe_successes = 0
            if self._state is BreakerState.HALF_OPEN:
                if self._probes_in_flight >= self.config.half_open_max_probes:
                    return Admission(allowed=False, probe=False)
                self._probes_in_flight += 1
                return Admission(allowed=True, probe=True)
            return Admission(allowed=True, probe=False)

    async def record_success(self, latency_ms: float, *, probe: bool = False) -> None:
        async with self._lock:
            now = self._clock()
            if probe:
                if self._state is BreakerState.HALF_OPEN:
                    self._probes_in_flight = max(0, self._probes_in_flight - 1)
                    self._probe_successes += 1
                    if self._probe_successes >= self.config.half_open_successes_to_close:
                        self._close()
                return
            if self._state is not BreakerState.CLOSED:
                return
            self._consecutive = 0
            self._window.append((now, True))
            self._observe_latency(latency_ms)
            reason = self._should_trip(now)
            if reason is not None:
                self._trip(reason, now)

    async def record_failure(self, latency_ms: float | None = None, *, probe: bool = False) -> None:
        async with self._lock:
            now = self._clock()
            if probe:
                if self._state is BreakerState.HALF_OPEN:
                    self._trip("half_open_probe_failed", now)
                return
            if self._state is not BreakerState.CLOSED:
                return
            self._consecutive += 1
            self._window.append((now, False))
            if latency_ms is not None:
                self._observe_latency(latency_ms)
            reason = self._should_trip(now)
            if reason is not None:
                self._trip(reason, now)

    async def release_probe(self) -> None:
        """Release a HALF_OPEN probe slot without recording success or failure (cancellation)."""
        async with self._lock:
            if self._state is BreakerState.HALF_OPEN and self._probes_in_flight > 0:
                self._probes_in_flight -= 1

    async def call(
        self,
        factory: Callable[[], Awaitable[T]],
        *,
        fallback_factory: Callable[[str], FallbackEnvelope],
    ) -> T | FallbackEnvelope:
        """Run ``factory`` under breaker protection.

        Returns the factory result, or a :class:`FallbackEnvelope` immediately when the breaker is OPEN.
        Exceptions raised by ``factory`` are recorded as failures and re-raised; ``CancelledError`` is never
        counted as a failure.
        """
        admission = await self.admit()
        if not admission.allowed:
            return fallback_factory(f"circuit_open:{self.name}")
        started = time.perf_counter_ns()
        try:
            result = await factory()
        except asyncio.CancelledError:
            if admission.probe:
                await asyncio.shield(self.release_probe())
            raise
        except Exception:
            await self.record_failure((time.perf_counter_ns() - started) / 1e6, probe=admission.probe)
            raise
        await self.record_success((time.perf_counter_ns() - started) / 1e6, probe=admission.probe)
        return result


@dataclass(frozen=True)
class Admission:
    """Result of :meth:`CircuitBreaker.admit`."""

    allowed: bool
    probe: bool


class BreakerRegistry:
    """Lazily creates one breaker per tool name."""

    def __init__(
        self,
        configs: dict[str, BreakerConfig] | None = None,
        *,
        default: BreakerConfig | None = None,
        clock: Callable[[], float] = time.monotonic,
        rng: SupportsUniform | None = None,
    ) -> None:
        self._configs = dict(configs or {})
        self._default = default or BreakerConfig()
        self._clock = clock
        self._rng = rng
        self._breakers: dict[str, CircuitBreaker] = {}

    def get(self, name: str) -> CircuitBreaker:
        breaker = self._breakers.get(name)
        if breaker is None:
            breaker = CircuitBreaker(
                name, self._configs.get(name, self._default), clock=self._clock, rng=self._rng
            )
            self._breakers[name] = breaker
        return breaker

    def snapshots(self) -> dict[str, BreakerSnapshot]:
        return {name: b.snapshot() for name, b in self._breakers.items()}
