"""AIMD / TCP-Vegas adaptive concurrency limiter, token-bucket rate limiter and hedged execution.

Adaptive limiter
    ``q_ratio = (L_ewma - L_min_observed) / L_ewma``. If ``q_ratio < gamma_low`` (0.15) and utilisation is above
    70 percent the limit grows ``C <- min(C_max, C + 1)``. If ``q_ratio > gamma_high`` (0.35) or a timeout occurred
    it shrinks ``C <- max(C_min, floor(C * beta))`` with ``beta = 0.75``.

Token bucket
    ``R`` tokens per second with capacity ``B``, driven by ``time.monotonic_ns``.

Hedged execution
    For idempotent read tools, if the primary does not resolve within its p85 threshold a secondary request is
    started against a warm replica or cache. The first successful result wins and the loser is cancelled and
    drained so no zombie coroutine survives.
"""

from __future__ import annotations

import asyncio
import math
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import AsyncIterator, Awaitable, Callable, Generic, Literal, TypeVar

T = TypeVar("T")


class SlotHandle:
    """Handed to the body of ``limiter.slot()``; lets the caller flag a timeout."""

    __slots__ = ("timed_out", "failed")

    def __init__(self) -> None:
        self.timed_out = False
        self.failed = False

    def mark_timeout(self) -> None:
        self.timed_out = True


class AdaptiveConcurrencyLimiter:
    """Vegas/AIMD adaptive semaphore with a resizable limit."""

    def __init__(
        self,
        name: str,
        *,
        c_min: int = 1,
        c_max: int = 16,
        c_init: int = 4,
        gamma_low: float = 0.15,
        gamma_high: float = 0.35,
        beta: float = 0.75,
        alpha: float = 0.2,
        utilisation_threshold: float = 0.70,
        adjust_every: int = 4,
    ) -> None:
        if not 1 <= c_min <= c_init <= c_max:
            raise ValueError("require 1 <= c_min <= c_init <= c_max")
        if not 0.0 < beta < 1.0 or not 0.0 < alpha < 1.0:
            raise ValueError("alpha and beta must be in (0, 1)")
        if not 0.0 <= gamma_low < gamma_high <= 1.0:
            raise ValueError("require 0 <= gamma_low < gamma_high <= 1")
        self.name = name
        self.c_min = c_min
        self.c_max = c_max
        self.gamma_low = gamma_low
        self.gamma_high = gamma_high
        self.beta = beta
        self.alpha = alpha
        self.utilisation_threshold = utilisation_threshold
        self.adjust_every = max(1, adjust_every)
        self._limit = c_init
        self._in_flight = 0
        self._peak = 0
        self._ewma: float | None = None
        self._min_observed: float | None = None
        self._completions = 0
        self._peak_util_window = 0.0
        self._cond = asyncio.Condition()

    # ------------------------------------------------------------------ introspection
    @property
    def limit(self) -> int:
        return self._limit

    @property
    def in_flight(self) -> int:
        return self._in_flight

    @property
    def peak_in_flight(self) -> int:
        """Highest number of simultaneously held slots ever observed."""
        return self._peak

    @property
    def ewma_ms(self) -> float | None:
        return self._ewma

    @property
    def min_observed_ms(self) -> float | None:
        return self._min_observed

    def q_ratio(self) -> float:
        """Queueing-delay gradient ``(L_ewma - L_min) / L_ewma`` (0.0 until data exists)."""
        if self._ewma is None or self._min_observed is None or self._ewma <= 0.0:
            return 0.0
        return max(0.0, (self._ewma - self._min_observed) / self._ewma)

    # ------------------------------------------------------------------ slot management
    async def acquire(self) -> None:
        async with self._cond:
            await self._cond.wait_for(lambda: self._in_flight < self._limit)
            self._in_flight += 1
            if self._in_flight > self._peak:
                self._peak = self._in_flight

    async def release(self, latency_ms: float | None, *, timed_out: bool = False) -> None:
        """Release a slot and feed the observed latency into the Vegas/AIMD controller."""
        async with self._cond:
            utilisation = self._in_flight / self._limit if self._limit else 1.0
            self._peak_util_window = max(self._peak_util_window, utilisation)
            self._in_flight = max(0, self._in_flight - 1)
            if latency_ms is not None and latency_ms >= 0.0:
                self._observe(latency_ms)
            self._completions += 1
            if timed_out:
                self._decrease()
                self._reset_window()
            elif self._completions % self.adjust_every == 0:
                self._adjust()
                self._reset_window()
            self._cond.notify_all()

    @asynccontextmanager
    async def slot(self) -> AsyncIterator[SlotHandle]:
        """Acquire a slot for the body. Latency is measured with ``perf_counter_ns``."""
        await self.acquire()
        handle = SlotHandle()
        started = time.perf_counter_ns()
        try:
            yield handle
        except asyncio.CancelledError:
            await asyncio.shield(self.release(None))
            raise
        except BaseException:
            handle.failed = True
            await asyncio.shield(
                self.release((time.perf_counter_ns() - started) / 1e6, timed_out=handle.timed_out)
            )
            raise
        else:
            await asyncio.shield(
                self.release((time.perf_counter_ns() - started) / 1e6, timed_out=handle.timed_out)
            )

    # ------------------------------------------------------------------ controller (condition held)
    def _observe(self, latency_ms: float) -> None:
        if self._ewma is None:
            self._ewma = latency_ms
        else:
            self._ewma = self.alpha * latency_ms + (1.0 - self.alpha) * self._ewma
        if self._min_observed is None or latency_ms < self._min_observed:
            self._min_observed = latency_ms

    def _reset_window(self) -> None:
        self._peak_util_window = 0.0

    def _increase(self) -> None:
        self._limit = min(self.c_max, self._limit + 1)

    def _decrease(self) -> None:
        self._limit = max(self.c_min, math.floor(self._limit * self.beta))

    def _adjust(self) -> None:
        q = self.q_ratio()
        if q > self.gamma_high:
            self._decrease()
        elif q < self.gamma_low and self._peak_util_window > self.utilisation_threshold:
            self._increase()


class RateLimitExceeded(Exception):
    """Raised when a token cannot be obtained within ``max_wait_s``."""


class TokenBucket:
    """Token bucket with burst capacity ``B`` and sustained rate ``R`` tokens per second.

    Time is read from ``time.monotonic_ns`` (or an injected nanosecond clock). Refill and consumption never
    await, so they are atomic with respect to other coroutines; waiters are served FIFO through a lock.
    """

    def __init__(
        self,
        rate_per_s: float,
        capacity: float,
        *,
        clock_ns: Callable[[], int] = time.monotonic_ns,
    ) -> None:
        if rate_per_s <= 0.0 or capacity <= 0.0:
            raise ValueError("rate_per_s and capacity must be positive")
        self.rate_per_s = float(rate_per_s)
        self.capacity = float(capacity)
        self._clock_ns = clock_ns
        self._tokens = float(capacity)
        self._last_ns = clock_ns()
        self._lock = asyncio.Lock()

    def _refill(self) -> None:
        now = self._clock_ns()
        elapsed_ns = max(0, now - self._last_ns)
        self._last_ns = now
        self._tokens = min(self.capacity, self._tokens + elapsed_ns * self.rate_per_s / 1e9)

    @property
    def tokens(self) -> float:
        self._refill()
        return self._tokens

    def try_acquire(self, tokens: float = 1.0) -> bool:
        """Take ``tokens`` if available right now."""
        if tokens > self.capacity:
            raise ValueError("requested tokens exceed bucket capacity")
        self._refill()
        if self._tokens >= tokens:
            self._tokens -= tokens
            return True
        return False

    def wait_time_s(self, tokens: float = 1.0) -> float:
        """Seconds until ``tokens`` will be available (0.0 if available now)."""
        self._refill()
        deficit = tokens - self._tokens
        return 0.0 if deficit <= 0.0 else deficit / self.rate_per_s

    async def acquire(self, tokens: float = 1.0, *, max_wait_s: float | None = None) -> None:
        """Wait (FIFO) until ``tokens`` are available.

        Raises:
            RateLimitExceeded: if the required wait exceeds ``max_wait_s``.
        """
        async with self._lock:
            deadline_ns = None if max_wait_s is None else self._clock_ns() + int(max_wait_s * 1e9)
            while True:
                if self.try_acquire(tokens):
                    return
                wait_s = self.wait_time_s(tokens)
                if deadline_ns is not None and self._clock_ns() + int(wait_s * 1e9) > deadline_ns:
                    raise RateLimitExceeded(f"rate limit wait {wait_s:.3f}s exceeds {max_wait_s}s")
                await asyncio.sleep(wait_s)


@dataclass(frozen=True)
class HedgeOutcome(Generic[T]):
    """Result of :func:`execute_with_hedge`."""

    value: T
    winner: Literal["primary", "hedge"]
    hedged: bool
    elapsed_ms: float


async def execute_with_hedge(
    primary: Callable[[], Awaitable[T]],
    hedge: Callable[[], Awaitable[T]],
    *,
    idempotent: bool,
    hedge_after_ms: float | None,
) -> HedgeOutcome[T]:
    """Run ``primary``; if it has not resolved within ``hedge_after_ms``, race a ``hedge`` request against it.

    Only idempotent calls are hedged. The first successful result wins; if the first finisher failed the
    other is awaited. Every losing task is cancelled and drained before returning (also when this coroutine
    itself is cancelled), so no zombie task survives. Loser cancellation is internal and never raised.
    """
    started = time.perf_counter_ns()

    def elapsed() -> float:
        return (time.perf_counter_ns() - started) / 1e6

    if not idempotent or hedge_after_ms is None:
        value = await primary()
        return HedgeOutcome(value, "primary", False, elapsed())

    tasks: dict[asyncio.Future[T], Literal["primary", "hedge"]] = {}
    errors: dict[str, BaseException] = {}
    primary_task: asyncio.Future[T] = asyncio.ensure_future(primary())
    tasks[primary_task] = "primary"
    try:
        done, _ = await asyncio.wait({primary_task}, timeout=max(0.0, hedge_after_ms) / 1000.0)
        if primary_task in done:
            return HedgeOutcome(primary_task.result(), "primary", False, elapsed())
        hedge_task: asyncio.Future[T] = asyncio.ensure_future(hedge())
        tasks[hedge_task] = "hedge"
        pending: set[asyncio.Future[T]] = {primary_task, hedge_task}
        while pending:
            done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                if task.cancelled():
                    continue
                error = task.exception()
                if error is None:
                    return HedgeOutcome(task.result(), tasks[task], True, elapsed())
                errors[tasks[task]] = error
        raise errors.get("primary") or errors["hedge"]
    finally:
        losers = [t for t in tasks if not t.done()]
        for task in losers:
            task.cancel()
        if losers:
            await asyncio.gather(*losers, return_exceptions=True)
        for task in tasks:
            if task.done() and not task.cancelled():
                task.exception()
