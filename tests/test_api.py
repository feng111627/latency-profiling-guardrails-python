"""Business coverage through real HTTP (ASGI): original behaviour,
end-to-end metrics, timeout/error exception chains, queue-time visibility,
and cache admin operations."""
from __future__ import annotations

import asyncio

import httpx
import pytest


async def get(app, path, **kw) -> httpx.Response:
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport,
                                 base_url="http://t") as client:
        return await client.get(path, **kw)


def route_stats(app, route: str) -> dict:
    for item in app.state.stats.all_snapshots():
        if item.route == route:
            return item
    raise AssertionError(f"no stats for {route}")


@pytest.mark.business_point("对外行为不变：GET / 依旧返回 {'Hello': 'World'}")
async def test_root_behaviour_preserved(app_factory):
    app = app_factory()
    r = await get(app, "/")
    assert r.status_code == 200
    assert r.json() == {"Hello": "World"}


@pytest.mark.business_point("端到端指标：成功请求按路由出现在 /metrics，含分位数与拆分")
async def test_metrics_report_end_to_end(app_factory):
    app = app_factory(dependency_delay=0.01)
    for _ in range(5):
        r = await get(app, "/expensive", params={"dep_timeout": 1})
        assert r.status_code == 200
    r = await get(app, "/metrics")
    assert r.status_code == 200
    body = r.json()
    exp = [x for x in body["routes"] if x["route"] == "/expensive"][0]
    assert exp["counts"]["success"] == 5
    assert exp["counts"]["error"] == 0
    q = exp["success_latency_ms"]
    assert set(q) == {"p50", "p90", "p95", "p99"}
    assert q["p50"] is not None
    # the slow dependency segment must be visible in the breakdown
    assert exp["breakdown_ms"]["dependency_ms"] >= 5


@pytest.mark.business_point("异常链路：依赖超时返回 504 且记为 timeout，不算成功")
async def test_dependency_timeout_is_504_timeout(app_factory):
    app = app_factory(dependency_delay=0.1)
    r = await get(app, "/expensive", params={"dep_timeout": 0.01})
    assert r.status_code == 504
    assert r.json()["error"] == "dependency_timeout"
    s = route_stats(app, "/expensive")
    assert s.counts.timeout == 1
    assert s.counts.success == 0


@pytest.mark.business_point("异常链路：依赖抛错返回 500 且记为 error，异常不被吞成成功")
async def test_dependency_error_is_500(app_factory):
    app = app_factory(dependency_fail=True)
    r = await get(app, "/expensive", params={"dep_timeout": 1})
    assert r.status_code == 500
    assert r.json()["error"] == "internal_server_error"
    s = route_stats(app, "/expensive")
    assert s.counts.error == 1
    assert s.counts.success == 0


@pytest.mark.business_point("异常日志：处理失败时异常带完整堆栈写入日志")
async def test_error_traceback_is_logged(app_factory, caplog):
    import logging

    app = app_factory(dependency_fail=True)
    with caplog.at_level(logging.ERROR, logger="app.metrics"):
        await get(app, "/expensive", params={"dep_timeout": 1})
    assert any("unhandled error" in rec.message for rec in caplog.records)
    assert any(rec.exc_info is not None for rec in caplog.records)


@pytest.mark.business_point("排队耗时可见：进入队列等待的请求 queue_ms > 0")
async def test_queue_time_is_measured(app_factory):
    app = app_factory(max_inflight=1, max_queue=1, queue_timeout=2.0)

    async def call(delay):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport,
                                     base_url="http://t") as c:
            return await c.get("/expensive", params={"delay": delay})

    results = await asyncio.gather(call(0.1), call(0.0))
    assert sorted(r.status_code for r in results) == [200, 200]
    s = route_stats(app, "/expensive")
    assert s.breakdown_ms["queue_ms"] > 0


@pytest.mark.business_point("缓存命中：开启缓存后第二次 HIT，且慢依赖只被调用一次")
async def test_cache_hit_over_http(app_factory):
    app = app_factory(cache_enabled=True, cache_ttl=10, dependency_delay=0.01)
    r1 = await get(app, "/expensive", params={"q": "x"})
    r2 = await get(app, "/expensive", params={"q": "x"})
    assert r1.headers["x-cache"] == "MISS"
    assert r2.headers["x-cache"] == "HIT"
    assert r1.json() == r2.json()
    assert app.state.dependency.calls == 1


@pytest.mark.business_point("缓存防击穿：并发相同请求只产生一次慢依赖调用")
async def test_no_cache_stampede_over_http(app_factory):
    app = app_factory(
        max_inflight=4, max_queue=4, queue_timeout=5.0,
        cache_enabled=True, cache_ttl=10, dependency_delay=0.05,
    )

    async def call():
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport,
                                     base_url="http://t") as c:
            return await c.get("/expensive", params={"q": "hot"})

    results = await asyncio.gather(*(call() for _ in range(6)))
    states = sorted(r.headers["x-cache"] for r in results)
    assert states.count("MISS") == 1
    assert states.count("SHARED") >= 1
    assert app.state.dependency.calls == 1


@pytest.mark.business_point("缓存失效：DELETE 后再次请求重新计算，不返回旧结果")
async def test_cache_invalidation_over_http(app_factory):
    app = app_factory(cache_enabled=True, cache_ttl=100)
    await get(app, "/expensive", params={"q": "u"})
    status = await get(app, "/admin/cache")
    assert status.json()["entries"] == 1

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://t"
    ) as c:
        deleted = await c.request("DELETE", "/admin/cache/expensive:u")
    assert deleted.json()["invalidated"] == 1
    r = await get(app, "/expensive", params={"q": "u"})
    assert r.headers["x-cache"] == "MISS"


@pytest.mark.business_point("缓存开关：POST /admin/cache/enabled=false 后全部绕过缓存")
async def test_cache_toggle_over_http(app_factory):
    app = app_factory(cache_enabled=True, cache_ttl=100)
    await get(app, "/expensive", params={"q": "t"})
    assert app.state.dependency.calls == 1
    await get(app, "/expensive", params={"q": "t"})
    assert app.state.dependency.calls == 1  # served from cache

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://t"
    ) as c:
        off = await c.post("/admin/cache/enabled", json={"enabled": False})
    assert off.json()["enabled"] is False
    r = await get(app, "/expensive", params={"q": "t"})
    assert r.headers["x-cache"] == "BYPASS"
    assert app.state.dependency.calls == 2


@pytest.mark.business_point("no-cache 头：强制重算，不返回缓存旧值")
async def test_force_refresh_header(app_factory):
    app = app_factory(cache_enabled=True, cache_ttl=100)
    await get(app, "/expensive", params={"q": "z"})
    r = await get(
        app, "/expensive", params={"q": "z"},
        headers={"x-cache-control": "no-cache"},
    )
    assert r.headers["x-cache"] == "MISS"


@pytest.mark.business_point("过载指标：被拒请求计入 rejected 并带原因，不影响后续成功请求")
async def test_overload_then_recovery(app_factory):
    app = app_factory(max_inflight=1, max_queue=0)

    async def call(delay):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport,
                                     base_url="http://t") as c:
            return await c.get("/expensive", params={"delay": delay})

    burst = await asyncio.gather(call(0.15), call(0.0), call(0.0))
    assert sorted(r.status_code for r in burst) == [200, 503, 503]

    # service recovered immediately after the load subsided
    ok = await call(0.0)
    assert ok.status_code == 200

    s = route_stats(app, "/expensive")
    assert s.counts.rejected == 2
    assert s.counts.success == 2
    assert s.reject_reasons["concurrency_limit_reached"] == 2


@pytest.mark.business_point("缓存命中不受过载限制：满载下缓存 HIT 仍立即返回")
async def test_cache_hits_bypass_overload(app_factory):
    app = app_factory(
        max_inflight=1, max_queue=0, cache_enabled=True, cache_ttl=100,
    )
    await get(app, "/expensive", params={"q": "c"})  # warm

    async def call(hold):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport,
                                     base_url="http://t") as c:
            if hold:
                return await c.get("/expensive",
                                   params={"q": "other", "delay": 0.15})
            return await c.get("/expensive", params={"q": "c"})

    results = await asyncio.gather(call(True), call(False))
    statuses = sorted(r.status_code for r in results)
    assert statuses == [200, 200], statuses
    cached = [r for r in results if r.headers["x-cache"] == "HIT"]
    assert len(cached) == 1 and "q=c" in str(cached[0].request.url)
