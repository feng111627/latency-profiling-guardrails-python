"""可开关的结果缓存 + 并发防击穿（single-flight）。

保证：

- TTL：写入时记录过期时间，读取时严格检查墙钟时间，过期条目绝不会返回；
  过期由读取自然触发，也可通过 :meth:`invalidate` 显式失效（按 key / 全部）。
- 可开关：``set_enabled(False)`` 后读请求一律回源（也不写缓存），
  再次打开后立即恢复；关闭期间旧缓存保留但不可见。
- 防击穿：同一 key 的并发请求只允许一个回源 loader 执行，其余等待同一个
  共享任务并拿到同一份结果（成功值或同一异常）。
- 失败不缓存：loader 抛异常时该 key 不留下任何缓存条目，后续请求正常重试，
  不会把错误结果缓存下来返回给后续调用。
- 返回值不被篡改：返回缓存对象的深拷贝，调用方对嵌套结构的修改不会
  污染缓存（约定 loader 返回可深拷贝的普通数据结构）。
"""
from __future__ import annotations

import asyncio
import copy
import time
from dataclasses import dataclass
from typing import Awaitable, Callable, Dict, Optional, TypeVar

T = TypeVar("T")


@dataclass
class CacheEntry:
    value: object
    expires_at: float


class SingleFlightCache:
    """按 key 的 TTL 结果缓存（asyncio 友好，事件循环内并发安全）。"""

    def __init__(
        self,
        ttl_seconds: float,
        enabled: bool = True,
        time_func: Callable[[], float] = time.monotonic,
    ) -> None:
        if ttl_seconds < 0:
            raise ValueError("ttl_seconds must be >= 0")
        self.ttl_seconds = float(ttl_seconds)
        self._enabled = enabled
        self._time = time_func
        self._entries: Dict[str, CacheEntry] = {}
        # 正在进行的回源任务：key -> Task（owner 与等待者共享）
        self._inflight: Dict[str, "asyncio.Task"] = {}

    @property
    def enabled(self) -> bool:
        return self._enabled

    def set_enabled(self, value: bool) -> None:
        self._enabled = bool(value)

    def _get_valid(self, key: str, now: float) -> Optional[object]:
        entry = self._entries.get(key)
        if entry is None:
            return None
        if entry.expires_at <= now:
            # 过期：立即移除，绝不返回旧值
            del self._entries[key]
            return None
        return entry.value

    async def get_or_set(
        self, key: str, loader: Callable[[], Awaitable[T]]
    ) -> tuple[T, bool]:
        """返回 ``(值, 是否命中缓存)``。

        - 缓存关闭或未命中：执行 loader，返回 (loader 结果, False)
        - 命中有效缓存：返回 (缓存值副本, True)，不执行 loader
        - loader 抛异常时原样透传，不缓存
        """
        if not self._enabled:
            return await loader(), False

        now = self._time()
        value = self._get_valid(key, now)
        if value is not None:
            return copy.deepcopy(value), True

        task = self._inflight.get(key)
        if task is None:
            # 本请求负责回源；回源任务被所有并发请求共享
            task = asyncio.ensure_future(loader())
            self._inflight[key] = task

            def _on_done(done: "asyncio.Task[T]") -> None:
                self._inflight.pop(key, None)
                if done.cancelled():
                    return
                exc = done.exception()
                if exc is None:
                    # 仅成功才写入，失败不留任何条目
                    self._entries[key] = CacheEntry(
                        value=done.result(),
                        expires_at=self._time() + self.ttl_seconds,
                    )

            task.add_done_callback(_on_done)

        # 无论是不是发起者，都等待同一个共享任务；
        # 异常原样透传，不被吞掉
        return copy.deepcopy(await task), False

    def invalidate(self, key: Optional[str] = None) -> int:
        """显式失效。key=None 清空全部；返回被失效的条目数。

        注意：正在进行的 single-flight 回源不取消，其成功结果会重新写入；
        需要"回源期间也拒绝旧读"时先 set_enabled(False)。
        """
        if key is None:
            n = len(self._entries)
            self._entries.clear()
            return n
        if key in self._entries:
            del self._entries[key]
            return 1
        return 0

    def size(self) -> int:
        return len(self._entries)
