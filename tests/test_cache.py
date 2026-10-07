"""Business coverage: cache hit/miss, TTL expiry, explicit invalidation,
single-flight stampede protection, toggle and no-cached-errors."""
from __future__ import annotations

import asyncio

import pytest


@pytest.mark.business_point("缓存开关：禁用时不读不写，producer 每次执行（BYPASS）")
async def test_disabled_cache_bypasses(fake_clock):
    from app.cache import ResultCache

    cache = ResultCache(ttl_seconds=10, enabled=False, clock=fake_clock)
    calls = []

    async def producer():
        calls.append(1)
        return "v"

    for _ in range(3):
        value, source = await cache.get_or_set("k", producer)
        assert value == "v" and source == "BYPASS"
    assert len(calls) == 3
    assert cache.status()["entries"] == 0


@pytest.mark.business_point("缓存命中：首次 MISS，后续命中 HIT，producer 只执行一次")
async def test_hit_after_miss(cache):
    calls = []

    async def producer():
        calls.append(1)
        await asyncio.sleep(0)
        return {"v": 1}

    v0, s0 = await cache.get_or_set("k", producer)
    v1, s1 = await cache.get_or_set("k", producer)
    assert (v0, s0) == ({"v": 1}, "MISS")
    assert (v1, s1) == ({"v": 1}, "HIT")
    assert len(calls) == 1


@pytest.mark.business_point("TTL 失效：过期条目不返回，触发重新计算（不返回过期结果）")
async def test_ttl_expiry_never_serves_stale(cache, fake_clock):
    calls = []

    async def producer_gen(tag):
        async def _p():
            calls.append(tag)
            return tag
        return _p

    await cache.get_or_set("k", await producer_gen("old"))
    assert cache.peek("k")[0] == "old"
    fake_clock.t += cache.ttl_seconds - 0.01
    assert cache.peek("k")[1] is True  # still fresh
    fake_clock.t += 0.02
    assert cache.peek("k") == (None, False)  # expired -> absent, not stale
    value, source = await cache.get_or_set("k", await producer_gen("new"))
    assert (value, source) == ("new", "MISS")


@pytest.mark.business_point("显式失效：invalidate(key) 后下次必须重算")
async def test_explicit_invalidation(cache):
    calls = []

    async def p1():
        calls.append("first")
        return "first"

    async def p2():
        calls.append("second")
        return "second"

    await cache.get_or_set("k", p1)
    assert cache.invalidate("k") == 1
    v, source = await cache.get_or_set("k", p2)
    assert v == "second" and source == "MISS"
    assert calls == ["first", "second"]


@pytest.mark.business_point("显式失效：invalidate() 全量清空，返回删除条数")
async def test_invalidate_all(cache):
    async def p():
        return 1

    await cache.get_or_set("a", p)
    await cache.get_or_set("b", p)
    assert cache.status()["entries"] == 2
    assert cache.invalidate() == 2
    assert cache.status()["entries"] == 0


@pytest.mark.business_point("防击穿：并发重复请求只触发一个 producer，其余 SHARED 同一结果")
async def test_singleflight_collapses_concurrent_misses(cache):
    calls = 0
    started = asyncio.Event()
    release = asyncio.Event()

    async def producer():
        nonlocal calls
        calls += 1
        started.set()
        await release.wait()
        return {"computed": True}

    tasks = [asyncio.create_task(cache.get_or_set("hot", producer))
             for _ in range(10)]
    await asyncio.wait_for(started.wait(), 1.0)
    await asyncio.sleep(0.02)  # let the other 9 join single-flight
    assert calls == 1
    release.set()
    results = await asyncio.gather(*tasks)

    sources = sorted(source for _, source in results)
    assert sources == ["MISS"] + ["SHARED"] * 9
    assert all(v == {"computed": True} for v, _ in results)
    # afterwards the value is stored and served as HIT
    v, hit = cache.peek("hot")
    assert hit and v == {"computed": True}


@pytest.mark.business_point("不同 key 不合并：不同缓存键各自计算")
async def test_singleflight_is_per_key(cache):
    async def make(tag):
        async def _p():
            await asyncio.sleep(0.01)
            return tag
        return _p

    results = await asyncio.gather(
        cache.get_or_set("k1", await make("a")),
        cache.get_or_set("k2", await make("b")),
    )
    assert {v for v, _ in results} == {"a", "b"}
    assert cache.status()["entries"] == 2


@pytest.mark.business_point("异常链路：producer 抛错时所有等待者收到同一异常且不写缓存")
async def test_producer_error_is_propagated_and_not_cached(cache):
    calls = 0

    async def boom():
        nonlocal calls
        calls += 1
        await asyncio.sleep(0)
        raise ValueError("dependency down")

    # concurrent callers all see the exception
    results = await asyncio.gather(
        *[cache.get_or_set("err", boom) for _ in range(4)],
        return_exceptions=True,
    )
    assert all(isinstance(r, ValueError) for r in results), results
    assert calls == 1  # single-flight still collapsed them
    # nothing stored: next call retries the producer
    assert cache.peek("err") == (None, False)
    with pytest.raises(ValueError):
        await cache.get_or_set("err", boom)
    assert calls == 2
    # other keys are unaffected
    assert cache.status()["inflight"] == 0


@pytest.mark.business_point("缓存开关：启用后写入；再关闭清空条目，旧结果不能复活")
async def test_toggle_clears_entries(cache, fake_clock):
    async def p():
        return "keep"

    await cache.get_or_set("k", p)
    assert cache.status()["entries"] == 1
    cache.set_enabled(False)
    assert cache.status()["entries"] == 0
    cache.set_enabled(True)
    assert cache.peek("k") == (None, False)
    v, source = await cache.get_or_set("k", p)
    assert (v, source) == ("keep", "MISS")


@pytest.mark.business_point("失效防复活：invalidate 时在途 producer 的结果不得写入旧世代")
async def test_invalidation_during_inflight_blocks_resurrection(cache):
    release = asyncio.Event()

    async def stale_producer():
        await release.wait()
        return "stale"

    task = asyncio.create_task(cache.get_or_set("k", stale_producer))
    await asyncio.sleep(0.02)  # producer is in flight
    removed = cache.invalidate("k")
    assert removed == 0  # nothing stored yet, but generation bumped
    release.set()
    value, source = await task
    assert value == "stale" and source == "MISS"
    # the in-flight result belongs to the old generation -> must not be served
    assert cache.peek("k") == (None, False)
