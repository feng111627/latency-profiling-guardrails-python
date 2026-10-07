"""可开关 TTL 缓存 + single-flight 防击穿单测。"""
from __future__ import annotations

import asyncio

import pytest

from app.cache import SingleFlightCache


@pytest.mark.business("缓存命中：TTL 内重复请求只回源一次")
async def test_cache_hit_avoids_reload():
    clock = [100.0]
    calls = []
    cache = SingleFlightCache(ttl_seconds=10.0, time_func=lambda: clock[0])

    async def loader():
        calls.append(1)
        return {"n": len(calls)}

    assert (await cache.get_or_set("k", loader)) == ({"n": 1}, False)
    assert (await cache.get_or_set("k", loader)) == ({"n": 1}, True)
    assert len(calls) == 1


@pytest.mark.business("TTL 过期：绝不返回过期旧值，触发重新回源")
async def test_ttl_expiry_returns_fresh_value():
    clock = [100.0]
    calls = []
    cache = SingleFlightCache(ttl_seconds=5.0, time_func=lambda: clock[0])

    async def loader():
        calls.append(1)
        return {"n": len(calls)}

    await cache.get_or_set("k", loader)
    clock[0] = 104.9
    assert (await cache.get_or_set("k", loader))[1] is True
    clock[0] = 105.0  # 到期瞬间视为过期
    value, hit = await cache.get_or_set("k", loader)
    assert value == {"n": 2} and hit is False


@pytest.mark.business("显式失效：按 key 与全部失效，失效后重新回源")
async def test_explicit_invalidation():
    cache = SingleFlightCache(ttl_seconds=60.0)

    async def loader_a():
        return "a"

    async def loader_b():
        return "b"

    await cache.get_or_set("a", loader_a)
    await cache.get_or_set("b", loader_b)
    assert cache.size() == 2
    assert cache.invalidate("a") == 1
    assert cache.invalidate("a") == 0
    assert cache.size() == 1
    assert cache.invalidate() == 1
    assert cache.size() == 0


@pytest.mark.business("防击穿：同 key 高并发只有一个回源，其余共享结果")
async def test_single_flight_no_thundering_herd():
    calls = 0
    cache = SingleFlightCache(ttl_seconds=60.0)

    async def loader():
        nonlocal calls
        calls += 1
        await asyncio.sleep(0.05)
        return {"loaded_by": calls}

    results = await asyncio.gather(*(cache.get_or_set("hot", loader) for _ in range(20)))
    assert calls == 1
    assert all(r == ({"loaded_by": 1}, False) for r in results)
    # 之后是命中
    assert (await cache.get_or_set("hot", loader))[1] is True
    assert calls == 1


@pytest.mark.business("失败不缓存：loader 异常后不留脏条目，后续请求可重试")
async def test_error_not_cached():
    attempts = 0
    cache = SingleFlightCache(ttl_seconds=60.0)

    async def flaky():
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError("dep down")
        return "recovered"

    with pytest.raises(RuntimeError):
        await cache.get_or_set("k", flaky)
    assert cache.size() == 0
    assert (await cache.get_or_set("k", flaky)) == ("recovered", False)
    assert attempts == 2


@pytest.mark.business("异常不吞且可区分：并发等待者拿到同一异常，不变成成功")
async def test_error_propagates_to_all_concurrent_waiters():
    cache = SingleFlightCache(ttl_seconds=60.0)

    async def boom():
        await asyncio.sleep(0.02)
        raise ValueError("explicit failure")

    results = await asyncio.gather(
        *(cache.get_or_set("k", boom) for _ in range(5)),
        return_exceptions=True,
    )
    assert len(results) == 5
    assert all(isinstance(r, ValueError) for r in results)
    assert cache.size() == 0


@pytest.mark.business("缓存开关：关闭时一律回源，重新打开后恢复且不返回错误结果")
async def test_enable_disable_switch():
    cache = SingleFlightCache(ttl_seconds=60.0)
    calls = 0

    async def loader():
        nonlocal calls
        calls += 1
        return {"n": calls}

    await cache.get_or_set("k", loader)
    cache.set_enabled(False)
    assert cache.enabled is False
    await cache.get_or_set("k", loader)
    await cache.get_or_set("k", loader)
    assert calls == 3  # 关闭期间每次回源
    cache.set_enabled(True)
    value, hit = await cache.get_or_set("k", loader)
    assert hit is True and value == {"n": 1}  # 旧的成功缓存仍可用
    assert calls == 3


@pytest.mark.business("返回值隔离：调用方篡改返回对象不污染缓存")
async def test_returned_value_isolation():
    cache = SingleFlightCache(ttl_seconds=60.0)

    async def loader():
        return {"items": [1, 2]}

    first, _ = await cache.get_or_set("k", loader)
    first["items"].append(3)
    second, hit = await cache.get_or_set("k", loader)
    assert hit is True
    assert second["items"] == [1, 2]
