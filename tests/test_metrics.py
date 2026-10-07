"""计时与滚动窗口指标单测。"""
from __future__ import annotations

import threading

import pytest

from app.metrics import RollingWindowMetrics, Sample, percentile
from app.timing import RequestTiming


@pytest.mark.business("分阶段耗时口径：total=queue+dependency+self")
def test_timing_phase_identity():
    import asyncio

    async def scenario():
        t = RequestTiming().start()
        await asyncio.sleep(0.01)
        t.queue_wait_start()
        await asyncio.sleep(0.01)
        t.queue_wait_end()
        t.dependency_start()
        await asyncio.sleep(0.02)
        t.dependency_end()
        t.stop()
        assert t.queue == pytest.approx(0.01, abs=0.01)
        assert t.dependency == pytest.approx(0.02, abs=0.01)
        assert t.total == pytest.approx(
            t.queue + t.dependency + t.self_time, abs=1e-9
        )

    asyncio.run(scenario())


@pytest.mark.business("分位数算法：空样本返回 None，边界与插值正确")
def test_percentile_values():
    assert percentile([], 0.95) is None
    assert percentile([0.5], 0.95) == 0.5
    assert pytest.approx(percentile([1, 2, 3, 4], 0.5)) == 2.5
    assert pytest.approx(percentile(list(range(1, 101)), 0.95)) == 95.05


@pytest.mark.business("并发统计不串不丢：多线程写同一/不同路由，样本一条不丢")
def test_concurrent_records_no_loss():
    # 注入固定时钟，避免样本 ts 与真实 monotonic 时钟比较导致误驱逐
    m = RollingWindowMetrics(window_seconds=60.0, time_func=lambda: 1001.0)

    def worker(route: str, n: int, fail_every: int):
        for j in range(n):
            outcome = "error" if j % fail_every == 0 else "success"
            m.record(
                route,
                Sample(
                    ts=1000.0 + j * 0.001,
                    total=0.01 + j * 0.0001,
                    self_time=0.005,
                    dependency=0.003,
                    queue=0.002,
                    outcome=outcome,
                ),
            )

    threads = []
    for r in range(4):
        threads.append(threading.Thread(target=worker, args=(f"/r{r}", 250, 7)))
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    stats = {s.route: s for s in m.snapshot(now=1001.0)}
    assert set(stats) == {"/r0", "/r1", "/r2", "/r3"}
    for s in stats.values():
        assert s.count == 250
        assert sum(s.counts.values()) == 250
        # 成功分位数只基于成功样本
        assert s.counts["success"] + s.counts["error"] == 250
        assert len(s.quantiles_total_success) == 3
        assert len(s.quantiles_total_all) == 3
        # 分阶段分位数齐全
        assert set(s.quantiles_phase) == {"total", "self", "dependency", "queue"}


@pytest.mark.business("失败/超时/拒绝样本不计入成功分位数")
def test_failures_excluded_from_success_quantiles():
    m = RollingWindowMetrics(window_seconds=60.0, time_func=lambda: 1.0)
    for outcome, total in [
        ("success", 0.01),
        ("success", 0.02),
        ("error", 5.0),
        ("timeout", 9.0),
        ("rejected", 8.0),
    ]:
        m.record(
            "/x",
            Sample(0.0, total, 0.001, 0.0, 0.0, outcome),
        )
    s = m.snapshot(now=1.0)[0]
    assert s.counts == {"success": 2, "error": 1, "timeout": 1, "rejected": 1}
    assert max(s.quantiles_total_success.values()) < 0.1
    assert max(s.quantiles_total_all.values()) > 1.0


@pytest.mark.business("滚动窗口过期：窗口滚动后旧样本自然消失")
def test_window_expiry():
    clock = [100.0]
    m = RollingWindowMetrics(window_seconds=10.0, time_func=lambda: clock[0])
    m.record("/a", Sample(100.0, 0.01, 0.01, 0.0, 0.0, "success"))
    m.record("/a", Sample(105.0, 0.02, 0.02, 0.0, 0.0, "success"))
    assert m.snapshot()[0].count == 2
    clock[0] = 110.5
    s = m.snapshot()[0]
    assert s.count == 1
    assert s.quantiles_total_all == {"p50": 0.02, "p95": 0.02, "p99": 0.02}
    clock[0] = 120.0
    assert m.snapshot()[0].count == 0


@pytest.mark.business("非法 outcome 被拒绝，防止统计口径被污染")
def test_invalid_outcome_rejected():
    m = RollingWindowMetrics(window_seconds=10.0)
    with pytest.raises(ValueError):
        m.record("/a", Sample(0.0, 0.1, 0.1, 0.0, 0.0, "bogus"))  # type: ignore[arg-type]
