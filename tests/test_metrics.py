"""Business coverage: per-route request counts, concurrency safety,
sliding-window expiry and success-only latency quantiles."""
from __future__ import annotations

import threading
import time

import pytest

from app.metrics import QUANTILES, Sample, SlidingWindowStats, percentile


def make_sample(now: float, total: float, outcome: str = "success",
                route: str = "/r", reject_reason: str | None = None) -> Sample:
    return Sample(
        timestamp=now, total_ms=total, queue_ms=0.0,
        dependency_ms=0.0, self_ms=total, outcome=outcome,
        reject_reason=reject_reason,
    )


@pytest.mark.business_point("分位数口径：空序列返回 None，p50/p90/p95/p99 近秩计算")
def test_percentile_nearest_rank():
    assert percentile([], 0.9) is None
    vals = [float(v) for v in range(1, 101)]  # 1..100
    assert percentile(vals, 0.5) == 50.0
    assert percentile(vals, 0.9) == 90.0
    assert percentile(vals, 0.95) == 95.0
    assert percentile(vals, 0.99) == 99.0
    assert percentile(vals, 0.0) == 1.0
    assert percentile(vals, 1.0) == 100.0
    assert list(QUANTILES) == [0.5, 0.9, 0.95, 0.99]


@pytest.mark.business_point("按路由统计：不同路由的请求量与成败计数互不串数据")
def test_counts_are_keyed_per_route(stats):
    now = time.time()
    stats.record("/a", make_sample(now, 10, "success", "/a"))
    stats.record("/a", make_sample(now, 20, "error", "/a"))
    stats.record("/b", make_sample(now, 30, "success", "/b"))

    a, b = stats.snapshot("/a", now), stats.snapshot("/b", now)
    assert a.counts.total == 2 and a.counts.success == 1 and a.counts.error == 1
    assert b.counts.total == 1 and b.counts.success == 1
    assert b.counts.error == 0


@pytest.mark.business_point("并发安全：多线程并发写入不丢样本、计数精确")
def test_concurrent_records_are_not_lost(stats):
    n_threads, per_thread = 16, 500

    def worker(tid):
        for i in range(per_thread):
            stats.record(
                "/heavy",
                make_sample(time.time(), float(i + 1),
                            "success" if i % 2 else "error"),
            )

    threads = [threading.Thread(target=worker, args=(t,))
               for t in range(n_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    snap = stats.snapshot("/heavy")
    expected = n_threads * per_thread
    assert snap.counts.total == expected
    assert snap.counts.success + snap.counts.error == expected
    assert snap.counts.success == n_threads * (per_thread // 2)


@pytest.mark.business_point("并发安全：多路由并发写入彼此不串")
def test_concurrent_records_do_not_cross_routes(stats):
    routes_list = [f"/r{i}" for i in range(8)]

    def worker(route):
        for _ in range(300):
            stats.record(route, make_sample(time.time(), 1.0, "success", route))

    threads = [threading.Thread(target=worker, args=(r,))
               for r in routes_list]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    for r in routes_list:
        assert stats.snapshot(r).counts.total == 300


@pytest.mark.business_point("滑动窗口：窗口滚动后旧样本自然过期，计数归零")
def test_samples_expire_with_window():
    clock = {"t": 1000.0}
    stats = SlidingWindowStats(window_seconds=1.0, clock=lambda: clock["t"])
    stats.record("/x", make_sample(clock["t"], 10))
    clock["t"] += 0.5
    stats.record("/x", make_sample(clock["t"], 20))
    # snapshot() honours an explicit "now" so boundaries are deterministic
    assert stats.snapshot("/x", now=1000.9).counts.total == 2
    assert stats.snapshot("/x", now=1001.4).counts.total == 1
    assert stats.snapshot("/x", now=1001.6).counts.total == 0
    # write path also evicts lazily using the same clock
    clock["t"] = 1003.0
    stats.record("/x", make_sample(clock["t"], 30))
    assert stats.snapshot("/x").counts.total == 1


@pytest.mark.business_point("分位数口径：只有成功样本进入分位数，失败样本不污染")
def test_quantiles_only_use_success_samples():
    stats = SlidingWindowStats(window_seconds=10.0)
    now = time.time()
    for v in range(1, 11):
        stats.record("/x", make_sample(now, float(v), "success"))
    # huge failing samples must not move success quantiles
    for _ in range(50):
        stats.record("/x", make_sample(now, 9_999.0, "error"))
    stats.record("/x", make_sample(now, 9_999.0, "timeout"))
    stats.record("/x", make_sample(now, 9_999.0, "rejected",
                                    reject_reason="queue_full"))

    snap = stats.snapshot("/x", now)
    assert snap.success_latency_ms["p50"] == 5.0
    assert snap.success_latency_ms["p99"] == 10.0
    assert snap.counts.success == 10
    assert snap.counts.error == 50
    assert snap.counts.timeout == 1
    assert snap.counts.rejected == 1
    # failure latency remains separately observable
    assert snap.total_latency_by_outcome_ms["error"]["p50"] == 9999.0


@pytest.mark.business_point("拒绝原因：reject_reason 按原因分桶计数")
def test_reject_reasons_are_counted(stats):
    now = time.time()
    stats.record("/x", make_sample(now, 1, "rejected", "/x", "queue_full"))
    stats.record("/x", make_sample(now, 1, "rejected", "/x", "queue_full"))
    stats.record("/x", make_sample(now, 1, "rejected", "/x",
                                    "concurrency_limit_reached"))
    snap = stats.snapshot("/x", now)
    assert snap.reject_reasons == {"queue_full": 2,
                                   "concurrency_limit_reached": 1}


@pytest.mark.business_point("重置：reset 后窗口清空")
def test_reset_clears_samples(stats):
    stats.record("/x", make_sample(time.time(), 1))
    stats.reset()
    assert stats.routes() == []
    assert stats.snapshot("/x").counts.total == 0
