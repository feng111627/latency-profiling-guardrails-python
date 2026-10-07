"""按路由的滚动窗口延迟指标。

口径说明：

- 窗口：以 ``time.monotonic`` 为时钟的 wall-clock 滑动窗口，窗口外的旧样本
  在每次写入/快照时惰性丢弃（不做后台清理，避免后台线程带来的不确定性）。
- 计数：count 为窗口内全部请求数；按结果拆为 success/error/timeout/rejected。
- 分位数：对窗口内样本做线性插值的最近序分位数（单位秒），提供三类：
  * total_success：仅成功请求（失败样本不算进成功统计）
  * total_all    ：全部请求（含失败/超时/拒绝）
  * phase        ：total/self/dependency/queue 四段各自的分位数（全样本，
    用于定位排队/慢依赖瓶颈；拒绝请求本身也可能携带排队耗时）
- 并发：每条路由一把锁，所有读写在锁内完成；快照对窗口做一次深拷贝，
  序列化输出，不与并发写相互串扰。
"""
from __future__ import annotations

import math
import threading
import time
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Literal, Tuple

Outcome = Literal["success", "error", "timeout", "rejected"]

_VALID_OUTCOMES = ("success", "error", "timeout", "rejected")
_DEFAULT_QUANTILES = (0.5, 0.95, 0.99)


@dataclass(frozen=True)
class Sample:
    """单次请求样本（耗时单位：秒）。"""

    ts: float
    total: float
    self_time: float
    dependency: float
    queue: float
    outcome: Outcome


@dataclass
class RouteStats:
    """单条路由在当前滚动窗口内的聚合结果。"""

    route: str
    window_seconds: float
    sample_count: int = 0
    counts: Dict[str, int] = field(default_factory=dict)
    quantiles_total_success: Dict[str, float] = field(default_factory=dict)
    quantiles_total_all: Dict[str, float] = field(default_factory=dict)
    quantiles_phase: Dict[str, Dict[str, float]] = field(default_factory=dict)

    @property
    def count(self) -> int:
        return self.sample_count

    def to_dict(self) -> dict:
        return {
            "route": self.route,
            "window_seconds": self.window_seconds,
            "sample_count": self.sample_count,
            "counts": self.counts,
            "quantiles": {
                "total_success_seconds": self.quantiles_total_success,
                "total_all_seconds": self.quantiles_total_all,
                "phase_seconds": self.quantiles_phase,
            },
        }


def percentile(sorted_values: List[float], q: float) -> float | None:
    """线性插值最近序分位数（类型 R 的 C=1 变体，与 numpy 默认一致）。"""
    if not sorted_values:
        return None
    if len(sorted_values) == 1:
        return sorted_values[0]
    pos = q * (len(sorted_values) - 1)
    lo = math.floor(pos)
    hi = math.ceil(pos)
    if lo == hi:
        return sorted_values[lo]
    frac = pos - lo
    return sorted_values[lo] * (1 - frac) + sorted_values[hi] * frac


def _quantile_map(values: List[float], quantiles: Tuple[float, ...]) -> Dict[str, float]:
    sv = sorted(values)
    out: Dict[str, float] = {}
    for q in quantiles:
        v = percentile(sv, q)
        if v is not None:
            out[f"p{int(round(q * 100))}"] = round(v, 6)
    return out


class _RouteBucket:
    __slots__ = ("samples", "lock")

    def __init__(self) -> None:
        self.samples: List[Sample] = []
        self.lock = threading.Lock()


class RollingWindowMetrics:
    """按路由维护的滚动窗口指标注册表（线程安全）。"""

    def __init__(
        self,
        window_seconds: float = 60.0,
        quantiles: List[float] | None = None,
        time_func: Callable[[], float] = time.monotonic,
    ) -> None:
        self.window_seconds = float(window_seconds)
        self.quantiles: Tuple[float, ...] = tuple(quantiles or _DEFAULT_QUANTILES)
        self._time = time_func
        self._buckets: Dict[str, _RouteBucket] = {}
        self._buckets_lock = threading.Lock()

    def _bucket(self, route: str) -> _RouteBucket:
        bucket = self._buckets.get(route)
        if bucket is None:
            # 双检，避免为同一路由创建多把锁导致样本分裂
            with self._buckets_lock:
                bucket = self._buckets.get(route)
                if bucket is None:
                    bucket = _RouteBucket()
                    self._buckets[route] = bucket
        return bucket

    def _evict(self, samples: List[Sample], now: float) -> None:
        """丢弃窗口外旧样本。样本按时间近似有序，用删除前缀的方式即可。"""
        cutoff = now - self.window_seconds
        idx = 0
        for s in samples:
            if s.ts >= cutoff:
                break
            idx += 1
        if idx:
            del samples[:idx]

    def record(self, route: str, sample: Sample) -> None:
        if sample.outcome not in _VALID_OUTCOMES:
            raise ValueError(f"invalid outcome: {sample.outcome!r}")
        bucket = self._bucket(route)
        with bucket.lock:
            bucket.samples.append(sample)
            self._evict(bucket.samples, self._time())

    def _aggregate(self, route: str, bucket: _RouteBucket, now: float) -> RouteStats:
        with bucket.lock:
            self._evict(bucket.samples, now)
            samples = list(bucket.samples)  # 快照后立刻释放锁

        counts = {name: 0 for name in _VALID_OUTCOMES}
        totals_success: List[float] = []
        totals_all: List[float] = []
        phases: Dict[str, List[float]] = {
            "total": [],
            "self": [],
            "dependency": [],
            "queue": [],
        }
        for s in samples:
            counts[s.outcome] += 1
            totals_all.append(s.total)
            phases["total"].append(s.total)
            phases["self"].append(s.self_time)
            phases["dependency"].append(s.dependency)
            phases["queue"].append(s.queue)
            if s.outcome == "success":
                totals_success.append(s.total)

        stats = RouteStats(route=route, window_seconds=self.window_seconds)
        stats.sample_count = len(samples)
        stats.counts = counts
        stats.quantiles_total_success = _quantile_map(totals_success, self.quantiles)
        stats.quantiles_total_all = _quantile_map(totals_all, self.quantiles)
        stats.quantiles_phase = {
            name: _quantile_map(values, self.quantiles)
            for name, values in phases.items()
            if values
        }
        return stats

    def snapshot(self, now: float | None = None) -> List[RouteStats]:
        current = now if now is not None else self._time()
        # 固定路由快照顺序，输出稳定
        with self._buckets_lock:
            items = sorted(self._buckets.items())
        return [self._aggregate(route, bucket, current) for route, bucket in items]

    def snapshot_dict(self, now: float | None = None) -> dict:
        return {
            "window_seconds": self.window_seconds,
            "quantiles": list(self.quantiles),
            "routes": [s.to_dict() for s in self.snapshot(now)],
        }
