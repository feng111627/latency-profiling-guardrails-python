"""Business coverage: concurrency caps, differentiated rejection reasons,
queue timeout and clean cancellation while queued."""
from __future__ import annotations

import asyncio

import pytest

from app.overload import ConcurrencyGuard, OverloadError


async def hold(guard: ConcurrencyGuard, delay: float = 0.2) -> None:
    async with guard():
        await asyncio.sleep(delay)


@pytest.mark.business_point("并发上限：达到上限立即拒绝，原因 concurrency_limit_reached")
async def test_immediate_rejection_at_limit():
    guard = ConcurrencyGuard(max_inflight=1, max_queue=0)
    async with guard():
        with pytest.raises(OverloadError) as ei:
            async with guard():
                pass
    assert ei.value.reason == "concurrency_limit_reached"
    assert guard.inflight == 0


@pytest.mark.business_point("区分拒绝原因：队列满 -> queue_full，与并发上限区分")
async def test_queue_full_reason():
    guard = ConcurrencyGuard(max_inflight=1, max_queue=1, queue_timeout=5)
    results = await asyncio.gather(
        hold(guard), hold(guard), hold(guard), return_exceptions=True
    )
    reasons = sorted(getattr(r, "reason", "ok") for r in results)
    assert reasons == ["ok", "ok", "queue_full"]


@pytest.mark.business_point("排队等待超时：拿不到槽位时 queue_timeout 拒绝，不无限排队")
async def test_queue_timeout_reason():
    guard = ConcurrencyGuard(max_inflight=1, max_queue=1, queue_timeout=0.03)

    async def wait_for_slot():
        async with guard():
            pass

    results = await asyncio.gather(
        hold(guard, 0.15), wait_for_slot(), return_exceptions=True
    )
    assert not isinstance(results[0], Exception)
    assert isinstance(results[1], OverloadError)
    assert results[1].reason == "queue_timeout"
    assert guard.inflight == 0 and guard.waiting == 0


@pytest.mark.business_point("队列放行：槽位释放后排队请求获得处理")
async def test_queued_request_admitted_after_release():
    order = []
    guard = ConcurrencyGuard(max_inflight=1, max_queue=1, queue_timeout=1)

    async def named(tag, delay):
        async with guard():
            order.append(f"{tag}:in")
            await asyncio.sleep(delay)
            order.append(f"{tag}:out")

    await asyncio.gather(named("first", 0.05), named("second", 0.0))
    assert order == ["first:in", "first:out", "second:in", "second:out"]
    assert guard.inflight == 0 and guard.waiting == 0


@pytest.mark.business_point("并发上限实际生效：运行中在飞数不超过 max_inflight")
async def test_inflight_never_exceeds_cap():
    guard = ConcurrencyGuard(max_inflight=2, max_queue=10, queue_timeout=5)
    observed_max = 0

    async def worker():
        nonlocal observed_max
        async with guard():
            observed_max = max(observed_max, guard.inflight)
            await asyncio.sleep(0.01)

    await asyncio.gather(*(worker() for _ in range(12)))
    assert observed_max == 2
    assert guard.inflight == 0


@pytest.mark.business_point("排队中取消：取消传播不被吞，队列槽位干净回收")
async def test_cancelled_while_queued_is_propagated_and_cleaned():
    guard = ConcurrencyGuard(max_inflight=1, max_queue=5, queue_timeout=5)
    holder = asyncio.create_task(hold(guard, 1.0))
    await asyncio.sleep(0.02)
    waiter = asyncio.create_task(hold(guard, 1.0))
    await asyncio.sleep(0.02)
    assert guard.waiting == 1 and guard.inflight == 1

    waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter
    assert guard.waiting == 0 and guard.inflight == 1

    holder.cancel()
    await asyncio.gather(holder, return_exceptions=True)
    assert guard.inflight == 0 and guard.waiting == 0
    # guard is reusable afterwards
    async with guard():
        pass


@pytest.mark.business_point("非法参数：max_inflight/max_queue 不接受非法值")
def test_guard_validates_arguments():
    with pytest.raises(ValueError):
        ConcurrencyGuard(max_inflight=0)
    with pytest.raises(ValueError):
        ConcurrencyGuard(max_inflight=1, max_queue=-1)


@pytest.mark.business_point("HTTP 过载：超限请求返回 503 与可区分原因头，不拖垮服务")
async def test_http_overload_returns_503_with_reason(make_client):
    import httpx

    app, client = make_client(max_inflight=1, max_queue=0)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://t"
    ) as ac:
        async def call():
            return await ac.get("/expensive", params={"delay": 0.2})

        responses = await asyncio.gather(call(), call(), call())
        statuses = sorted(r.status_code for r in responses)
    assert statuses == [200, 503, 503]
    rejected = [r for r in responses if r.status_code == 503]
    assert all(r.headers["x-overload-reason"] == "concurrency_limit_reached"
               for r in rejected)
    assert all(r.json()["reason"] == "concurrency_limit_reached"
               for r in rejected)
