"""Request timing recorder splitting latency into named segments.

Segment definitions (all measured with a monotonic clock, reported in ms):

- ``queue_ms``      : time spent waiting for the concurrency guard
                      (only present on the guarded expensive endpoint).
- ``dependency_ms`` : time spent blocked inside the local slow dependency.
- ``self_ms``       : handler's own processing, i.e.
                      ``total - queue - dependency``.
- ``total_ms``      : end-to-end request time, measured by the metrics
                      middleware from request start to response/exception.

A recorder is created per request and is not shared between requests.
The dependency segment uses an accumulator so multiple dependency calls
inside one request are summed; nested/overlapping uses are prevented.
"""
from __future__ import annotations

import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Iterator


@dataclass
class TimingRecorder:
    started_at: float
    queue_ms: float = 0.0
    dependency_ms: float = 0.0
    enqueued_at: float | None = None
    finished_at: float | None = None
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)
    _dep_active: bool = field(default=False, repr=False)
    _acquired_at: float | None = field(default=None, repr=False)

    def now(self) -> float:
        return time.perf_counter()

    def mark_enqueued(self) -> None:
        """Record that the request starts waiting for a concurrency slot."""
        if self.enqueued_at is None:
            self.enqueued_at = self.now()

    def mark_acquired(self) -> None:
        """Record that a concurrency slot was granted; closes the queue span."""
        self._acquired_at = self.now()
        if self.enqueued_at is not None:
            self.queue_ms = (self._acquired_at - self.enqueued_at) * 1000.0

    @contextmanager
    def dependency(self) -> Iterator[None]:
        """Measure one span spent waiting on the local slow dependency."""
        with self._lock:
            if self._dep_active:
                raise RuntimeError("overlapping dependency spans")
            self._dep_active = True
            start = self.now()
        try:
            yield
        finally:
            end = self.now()
            with self._lock:
                self.dependency_ms += (end - start) * 1000.0
                self._dep_active = False

    def finish(self, ended_at: float | None = None) -> dict[str, float]:
        """Close the recorder and return the segment breakdown in ms."""
        ended_at = self.now() if ended_at is None else ended_at
        self.finished_at = ended_at
        total_ms = (ended_at - self.started_at) * 1000.0
        self_ms = total_ms - self.queue_ms - self.dependency_ms
        return {
            "total_ms": round(max(total_ms, 0.0), 3),
            "queue_ms": round(max(self.queue_ms, 0.0), 3),
            "dependency_ms": round(max(self.dependency_ms, 0.0), 3),
            "self_ms": round(max(self_ms, 0.0), 3),
        }
