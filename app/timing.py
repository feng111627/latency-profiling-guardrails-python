"""请求分阶段计时。

口径（单位：秒，基于 ``time.monotonic``）：

- total      : 请求端到端总耗时（进入中间件 -> 响应发出）
- queue      : 排队等待耗时（等待并发闸门令牌）
- dependency : 等待本地慢依赖的累计耗时（可能调用多次）
- self       : 自身处理耗时 = total - queue - dependency

``dependency`` 支持在一次请求内多次标记（累计），其余阶段为单段。
所有读取方法均为纯计算，无需加锁；计时对象本身只在单个请求的
任务上下文内被读写。
"""
from __future__ import annotations

from dataclasses import dataclass, field
import time


def _now() -> float:
    return time.monotonic()


@dataclass
class RequestTiming:
    """记录一次请求各阶段的起止时间戳（秒）。"""

    start_ts: float = 0.0
    end_ts: float = 0.0
    queue_start: float = 0.0
    queue_end: float = 0.0
    # dependency 可能有多段，直接累计耗时
    _dependency_elapsed: float = field(default=0.0, init=False, repr=False)
    dep_start: float = field(default=0.0, init=False)
    dep_end: float = field(default=0.0, init=False)

    def start(self) -> "RequestTiming":
        self.start_ts = _now()
        return self

    def stop(self) -> None:
        self.end_ts = _now()

    def queue_wait_start(self) -> None:
        self.queue_start = _now()

    def queue_wait_end(self) -> None:
        self.queue_end = _now()

    def dependency_start(self) -> None:
        self.dep_start = _now()

    def dependency_end(self) -> None:
        self.dep_end = _now()
        if self.dep_start:
            self._dependency_elapsed += max(0.0, self.dep_end - self.dep_start)
        # 复位起点，避免忘记 start 时重复累计
        self.dep_start = 0.0

    @property
    def total(self) -> float:
        end = self.end_ts or _now()
        return max(0.0, end - self.start_ts) if self.start_ts else 0.0

    @property
    def queue(self) -> float:
        if not self.queue_start:
            return 0.0
        end = self.queue_end or _now()
        return max(0.0, end - self.queue_start)

    @property
    def dependency(self) -> float:
        elapsed = self._dependency_elapsed
        if self.dep_start:  # 依赖尚未结束（进行中）
            elapsed += max(0.0, _now() - self.dep_start)
        return elapsed

    @property
    def self_time(self) -> float:
        return max(0.0, self.total - self.queue - self.dependency)
