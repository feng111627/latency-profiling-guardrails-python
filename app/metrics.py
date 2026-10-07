"""Per-route latency metrics with a sliding time window.

Statistics are kept as raw samples inside a per-route deque. Old samples
expire naturally: every read and every write evicts entries older than
``window_seconds`` (wall clock), so a rolled-over window never contains
stale data. All mutating/reading operations hold one lock, which makes the
store safe under concurrent requests regardless of the worker model
(asyncio tasks mixed with threadpool endpoints).

Outcome taxonomy
----------------
- ``success``  : handler ran and produced a response
- ``error``    : handler raised an unexpected exception (HTTP 5xx)
- ``timeout``  : the local slow dependency exceeded its budget (HTTP 504)
- ``rejected`` : overload guard refused the request before the handler ran

Only *success* samples feed the advertised latency quantiles. Failed
samples are counted (and exposed per-outcome) but can never inflate the
success figures.
"""
from __future__ import annotations

import threading
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Iterable

QUANTILES = (0.5, 0.9, 0.95, 0.99)
OUTCOMES = ("success", "error", "timeout", "rejected")
SEGMENTS = ("total_ms", "queue_ms", "dependency_ms", "self_ms")


@dataclass(frozen=True)
class Sample:
    timestamp: float
    total_ms: float
    queue_ms: float
    dependency_ms: float
    self_ms: float
    outcome: str  # one of OUTCOMES
    reject_reason: str | None = None


@dataclass
class OutcomeCounts:
    total: int = 0
    success: int = 0
    error: int = 0
    timeout: int = 0
    rejected: int = 0


@dataclass
class RouteStats:
    route: str
    window_seconds: float
    counts: OutcomeCounts = field(default_factory=OutcomeCounts)
    reject_reasons: dict[str, int] = field(default_factory=dict)
    # Latency quantiles of *successful* requests, in milliseconds.
    success_latency_ms: dict[str, float | None] = field(default_factory=dict)
    # Mean segment split for successful requests, in milliseconds.
    breakdown_ms: dict[str, float] = field(default_factory=dict)
    # Quantiles of total latency split per outcome (debugging aid).
    total_latency_by_outcome_ms: dict[str, dict[str, float | None]] = field(
        default_factory=dict
    )


def percentile(sorted_values: Iterable[float], q: float) -> float | None:
    """Nearest-rank percentile of an already ascending sequence.

    Returns ``None`` for an empty sequence. ``q`` is clamped to [0, 1].
    """
    values = list(sorted_values)
    if not values:
        return None
    if q <= 0:
        return float(values[0])
    if q >= 1:
        return float(values[-1])
    rank = q * len(values)
    index = int(rank)
    if index == rank and index > 0:
        index -= 1
    return float(values[min(index, len(values) - 1)])


def _quantile_block(values: list[float]) -> dict[str, float | None]:
    values.sort()
    return {f"p{int(q * 100)}": percentile(values, q) for q in QUANTILES}


class SlidingWindowStats:
    """Thread-safe per-route sample store with time-based expiry."""

    def __init__(self, window_seconds: float = 60.0, clock=time.time) -> None:
        if window_seconds <= 0:
            raise ValueError("window_seconds must be positive")
        self.window_seconds = float(window_seconds)
        self._clock = clock
        self._samples: dict[str, deque[Sample]] = defaultdict(deque)
        self._lock = threading.Lock()

    def _evict(self, route: str, now: float) -> None:
        bucket = self._samples[route]
        horizon = now - self.window_seconds
        while bucket and bucket[0].timestamp < horizon:
            bucket.popleft()

    def record(self, route: str, sample: Sample) -> None:
        if sample.outcome not in OUTCOMES:
            raise ValueError(f"unknown outcome: {sample.outcome!r}")
        with self._lock:
            bucket = self._samples[route]
            bucket.append(sample)
            self._evict(route, self._clock())

    def snapshot(self, route: str, now: float | None = None) -> RouteStats:
        now = self._clock() if now is None else now
        with self._lock:
            self._evict(route, now)
            samples = list(self._samples.get(route, ()))

        counts = OutcomeCounts(total=len(samples))
        reject_reasons: dict[str, int] = defaultdict(int)
        success = [s for s in samples if s.outcome == "success"]
        for sample in samples:
            setattr(counts, sample.outcome, getattr(counts, sample.outcome) + 1)
            if sample.outcome == "rejected" and sample.reject_reason:
                reject_reasons[sample.reject_reason] += 1

        stats = RouteStats(
            route=route,
            window_seconds=self.window_seconds,
            counts=counts,
            reject_reasons=dict(reject_reasons),
            success_latency_ms=_quantile_block([s.total_ms for s in success]),
            breakdown_ms=(
                {
                    seg: round(sum(getattr(s, seg) for s in success) / len(success), 3)
                    for seg in SEGMENTS
                }
                if success
                else {seg: 0.0 for seg in SEGMENTS}
            ),
            total_latency_by_outcome_ms={
                outcome: _quantile_block(
                    [s.total_ms for s in samples if s.outcome == outcome]
                )
                for outcome in OUTCOMES
            },
        )
        return stats

    def routes(self) -> list[str]:
        with self._lock:
            return sorted(r for r, b in self._samples.items() if b)

    def all_snapshots(self, now: float | None = None) -> list[RouteStats]:
        with self._lock:
            names = list(self._samples.keys())
        return [self.snapshot(name, now=now) for name in names]

    def reset(self) -> None:
        with self._lock:
            self._samples.clear()
