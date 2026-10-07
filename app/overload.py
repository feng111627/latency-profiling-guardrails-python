"""Concurrency limiting and overload rejection for expensive endpoints.

The guard is an async context manager with *fail-fast* semantics and an
optional bounded waiting queue:

1. If fewer than ``max_inflight`` requests are active: admitted immediately.
2. Otherwise, if the waiting queue already holds ``max_queue`` requests:
   rejected right away with reason ``queue_full`` (HTTP 503).
3. Otherwise the request joins the queue. If a slot is not granted within
   ``queue_timeout`` seconds: rejected with reason ``queue_timeout`` (503).
4. If the caller is cancelled while waiting, it leaves the queue without
   consuming a slot and the exception is re-raised (not swallowed).

Direct rejections at full inflight capacity carry reason
``concurrency_limit_reached``. Rejected requests are recorded by the
metrics middleware as ``rejected`` samples and never reach the handler.
"""
from __future__ import annotations

import asyncio
import contextlib


class OverloadError(Exception):
    """Request rejected by the overload guard (maps to HTTP 503)."""

    def __init__(self, reason: str, detail: str = "") -> None:
        super().__init__(detail or reason)
        self.reason = reason
        self.detail = detail


class ConcurrencyGuard:
    REASON_LIMIT = "concurrency_limit_reached"
    REASON_QUEUE_FULL = "queue_full"
    REASON_QUEUE_TIMEOUT = "queue_timeout"

    def __init__(
        self,
        max_inflight: int,
        max_queue: int = 0,
        queue_timeout: float | None = None,
    ) -> None:
        if max_inflight < 1:
            raise ValueError("max_inflight must be >= 1")
        if max_queue < 0:
            raise ValueError("max_queue must be >= 0")
        self.max_inflight = max_inflight
        self.max_queue = max_queue
        self.queue_timeout = queue_timeout
        self.inflight = 0
        self.waiting = 0
        self._cond = asyncio.Condition()

    @contextlib.asynccontextmanager
    async def __call__(self):
        async with self._cond:
            if self.inflight < self.max_inflight:
                self.inflight += 1
                granted = True
            elif self.max_queue == 0:
                raise OverloadError(
                    self.REASON_LIMIT,
                    f"inflight limit reached ({self.inflight}/{self.max_inflight})",
                )
            elif self.waiting >= self.max_queue:
                raise OverloadError(
                    self.REASON_QUEUE_FULL,
                    f"waiting queue full ({self.waiting}/{self.max_queue})",
                )
            else:
                self.waiting += 1
                granted = False
                try:
                    await asyncio.wait_for(
                        self._wait_slot(), self.queue_timeout
                    )
                except asyncio.TimeoutError:
                    self.waiting -= 1
                    raise OverloadError(
                        self.REASON_QUEUE_TIMEOUT,
                        f"no slot within {self.queue_timeout}s",
                    ) from None
                except BaseException:
                    # Cancellation (e.g. client disconnect) while queued:
                    # leave the queue untouched and propagate unchanged.
                    self.waiting -= 1
                    if granted:
                        self.inflight -= 1
                        self._cond.notify()
                    raise
                else:
                    granted = True
                    self.waiting -= 1
                    self.inflight += 1
        try:
            yield
        finally:
            async with self._cond:
                self.inflight -= 1
                self._cond.notify()

    async def _wait_slot(self) -> None:
        while self.inflight >= self.max_inflight:
            await self._cond.wait()
