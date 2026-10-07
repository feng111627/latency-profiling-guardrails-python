"""并发上限与过载拒绝。

策略：

- 用 ``asyncio.Semaphore(limit)`` 限制某昂贵接口同时执行的请求数。
- 信号量满时，新来的请求进入"排队"，最多等 ``queue_timeout`` 秒；
  等到令牌则正常执行（排队耗时计入指标），等不到则以
  ``RejectedError(QUEUE_TIMEOUT)`` 立即拒绝，不会无限堆积。
- 若拿令牌前发现"在途请求数（执行中 + 排队中）"已达
  ``limit + max_queue``，直接以 ``RejectedError(LIMIT_REACHED)`` 拒绝，
  连排队都不允许，避免队列无限增长拖垮服务。

拒绝原因通过 :class:`RejectedError` 向上抛出，由异常处理器转成可区分的
503 响应；超时候由 :class:`DependencyTimeoutError` 表示，二者在指标里
分别计 rejected / timeout，绝不计入 success。
"""
from __future__ import annotations

import asyncio
import enum
from typing import Optional

from .timing import RequestTiming


class RejectReason(str, enum.Enum):
    """过载拒绝原因（响应体中 reason 字段取此值）。"""

    LIMIT_REACHED = "limit_reached"
    QUEUE_TIMEOUT = "queue_timeout"


class RejectedError(Exception):
    """请求被过载保护拒绝。"""

    def __init__(self, reason: RejectReason, limit: int = 0, queue_timeout: float = 0.0) -> None:
        self.reason = reason
        self.limit = limit
        self.queue_timeout = queue_timeout
        super().__init__(f"rejected: {reason.value}")


class DependencyTimeoutError(Exception):
    """等待本地慢依赖超时。"""

    def __init__(self, elapsed: float = 0.0, timeout: float = 0.0) -> None:
        self.elapsed = elapsed
        self.timeout = timeout
        super().__init__(f"dependency timeout after {elapsed:.3f}s (limit {timeout:.3f}s)")


class ConcurrencyGuard:
    """带排队与超时的并发闸门。

    用法::

        async with guard.entry(timing):
            ...  # 受保护的处理

    ``limit`` 个并发在执行；另有最多 ``max_queue`` 个请求可排队等待
    ``queue_timeout`` 秒，超出/等不到即拒绝。
    """

    def __init__(self, limit: int, queue_timeout: float = 0.1, max_queue: int = 100) -> None:
        if limit < 1:
            raise ValueError("limit must be >= 1")
        self.limit = limit
        self.queue_timeout = queue_timeout
        self.max_queue = max_queue
        self._sem = asyncio.Semaphore(limit)
        self._inflight = 0  # 执行中 + 排队中
        self._state_lock = asyncio.Lock()

    @property
    def inflight(self) -> int:
        return self._inflight

    async def acquire(self, timing: Optional[RequestTiming] = None) -> None:
        async with self._state_lock:
            if self._inflight >= self.limit + self.max_queue:
                raise RejectedError(
                    RejectReason.LIMIT_REACHED,
                    limit=self.limit,
                    queue_timeout=self.queue_timeout,
                )
            self._inflight += 1
        queued = False
        try:
            if timing is not None:
                timing.queue_wait_start()
                queued = True
            try:
                await asyncio.wait_for(self._sem.acquire(), timeout=self.queue_timeout)
            except TimeoutError as exc:
                raise RejectedError(
                    RejectReason.QUEUE_TIMEOUT,
                    limit=self.limit,
                    queue_timeout=self.queue_timeout,
                ) from exc
            finally:
                if queued and timing is not None:
                    timing.queue_wait_end()
        except BaseException:
            async with self._state_lock:
                self._inflight -= 1
            raise

    def release(self) -> None:
        self._sem.release()
        # decrement 不需要 await：用 try acquire lock 会复杂化；
        # 直接在事件循环线程内同步调整即可（调用点均在同一 loop）。
        if self._inflight > 0:
            self._inflight -= 1

    class _Entry:
        def __init__(self, guard: "ConcurrencyGuard", timing: Optional[RequestTiming]) -> None:
            self._guard = guard
            self._timing = timing

        async def __aenter__(self) -> "ConcurrencyGuard._Entry":
            await self._guard.acquire(self._timing)
            return self

        async def __aexit__(self, exc_type, exc, tb) -> bool:
            self._guard.release()
            return False

    def entry(self, timing: Optional[RequestTiming] = None) -> "ConcurrencyGuard._Entry":
        return ConcurrencyGuard._Entry(self, timing)
