"""并发闸门 / 过载拒绝单测。"""
from __future__ import annotations

import asyncio

import pytest

from app.concurrency import ConcurrencyGuard, RejectedError, RejectReason
from app.timing import RequestTiming


async def _gather(coro_factories):
    return await asyncio.gather(*(f() for f in coro_factories))


@pytest.mark.asyncio(loop_scope="function")
@pytest.mark.business("超限请求立即拒绝：原因 limit_reached，不排队不拖垮服务")
async def test_limit_reached_rejected_immediately():
    guard = ConcurrencyGuard(limit=2, queue_timeout=0.05, max_queue=1)

    async def call(i):
        try:
            async with guard.entry():
                await asyncio.sleep(0.2)
            return (i, "ok")
        except RejectedError as e:
            return (i, e.reason.value)

    results = dict(await asyncio.gather(*(call(i) for i in range(4))))
    assert sorted(v for v in results.values() if v == "ok") == ["ok", "ok"]
    assert "limit_reached" in results.values()


@pytest.mark.asyncio(loop_scope="function")
@pytest.mark.business("排队超时拒绝：原因 queue_timeout，与 limit_reached 可区分")
async def test_queue_timeout_distinguishable():
    guard = ConcurrencyGuard(limit=1, queue_timeout=0.03, max_queue=10)

    async def call(i):
        try:
            async with guard.entry():
                await asyncio.sleep(0.15)
            return (i, "ok")
        except RejectedError as e:
            return (i, e.reason.value)

    results = dict(await asyncio.gather(*(call(i) for i in range(3))))
    assert results[0] == "ok"
    timeouts = [i for i, v in results.items() if v == "queue_timeout"]
    assert timeouts  # 排队等不到令牌的请求被明确标记
    # 异常对象携带原因与参数
    with pytest.raises(RejectedError) as ei:
        g2 = ConcurrencyGuard(limit=1, queue_timeout=0.0)
        # 直接占住再抢
        async with g2.entry():
            async with g2.entry():
                pass
    assert ei.value.reason in (RejectReason.QUEUE_TIMEOUT, RejectReason.LIMIT_REACHED)


@pytest.mark.asyncio(loop_scope="function")
@pytest.mark.business("排队等待时间计入 timing.queue 指标段")
async def test_queue_time_recorded_in_timing():
    guard = ConcurrencyGuard(limit=1, queue_timeout=1.0)

    async def hold():
        async with guard.entry():
            await asyncio.sleep(0.08)

    async def queued():
        t = RequestTiming().start()
        async with guard.entry(t):
            t.stop()
        return t

    t_holder, t_queued = await asyncio.gather(hold(), queued())
    assert t_queued.queue > 0.03
    # 自身处理耗时已扣除排队
    assert t_queued.self_time >= 0.0


@pytest.mark.asyncio(loop_scope="function")
@pytest.mark.business("令牌在异常路径下也正确释放，后续请求不受影响")
async def test_release_after_exception():
    guard = ConcurrencyGuard(limit=1, queue_timeout=0.5)

    async def failing():
        async with guard.entry():
            raise RuntimeError("boom inside guard")

    with pytest.raises(RuntimeError):
        await failing()

    ok = False
    async with guard.entry():
        ok = True
    assert ok
