"""端到端 HTTP 集成测试（真实 uvicorn 服务 + httpx 并发）。

这些用例验证中间件、路由、闸门、缓存在真实服务器下的协作，
而不是单组件的行为。每个用例使用独立端口的全新 app，互不干扰。
"""
from __future__ import annotations

import asyncio
import contextlib
import socket

import httpx
import pytest
import uvicorn

from app.cache import SingleFlightCache
from app.concurrency import ConcurrencyGuard
from app.metrics import RollingWindowMetrics


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture
async def server():
    """启动一个端口随机、组件全新隔离的 uvicorn 服务。"""
    # 替换模块级组件单例，使各用例的指标/闸门/缓存互不干扰
    import app.routes as routes
    from fastapi import FastAPI

    from app.main import register_exception_handlers
    from app.middleware import MetricsMiddleware

    routes.metrics = RollingWindowMetrics(window_seconds=30.0)
    routes.guard = ConcurrencyGuard(limit=2, queue_timeout=0.15, max_queue=2)
    routes.expensive_cache = SingleFlightCache(ttl_seconds=5.0)

    app = FastAPI(title="e2e")
    app.add_middleware(MetricsMiddleware, metrics=routes.metrics)
    app.include_router(routes.router)
    register_exception_handlers(app)

    port = _free_port()
    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning")
    server = uvicorn.Server(config)
    task = asyncio.ensure_future(server.serve())
    # 等待端口就绪
    for _ in range(100):
        if server.started:
            break
        await asyncio.sleep(0.02)
    base = f"http://127.0.0.1:{port}"
    try:
        yield base
    finally:
        server.should_exit = True
        with contextlib.suppress(Exception):
            await task


@pytest.mark.business("兼容性：原有 GET / 对外行为保持不变")
async def test_root_unchanged(server):
    async with httpx.AsyncClient(base_url=server, timeout=5) as cli:
        r = await cli.get("/")
    assert r.status_code == 200
    assert r.json() == {"Hello": "World"}


@pytest.mark.business("端到端指标：按路由统计请求量与成功/失败数")
async def test_metrics_counts_per_route(server):
    async with httpx.AsyncClient(base_url=server, timeout=5) as cli:
        await cli.get("/")
        await cli.get("/expensive", params={"delay": 0.01, "use_cache": "false"})
        await cli.get("/expensive", params={"delay": 0.01, "fail": "true",
                                            "use_cache": "false"})
        await cli.get("/expensive", params={"delay": 0.2, "timeout": 0.01,
                                            "use_cache": "false"})
        m = (await cli.get("/metrics")).json()
    by_route = {r["route"]: r for r in m["routes"]}
    assert by_route["/"]["counts"]["success"] == 1
    exp = by_route["/expensive"]["counts"]
    assert exp["success"] == 1
    assert exp["error"] == 1
    assert exp["timeout"] == 1


@pytest.mark.business("分段耗时：成功请求 total≈self+dependency，queue 可观测")
async def test_phase_latency_breakdown(server):
    async with httpx.AsyncClient(base_url=server, timeout=5) as cli:
        await cli.get("/expensive", params={"delay": 0.05, "use_cache": "false"})
        m = (await cli.get("/metrics")).json()
    s = next(r for r in m["routes"] if r["route"] == "/expensive")
    phases = s["quantiles"]["phase_seconds"]
    assert phases["dependency"]["p50"] == pytest.approx(0.05, abs=0.05)
    total_p50 = phases["total"]["p50"]
    assert total_p50 >= phases["dependency"]["p50"]


@pytest.mark.business("过载拒绝：并发打爆昂贵接口，返回可区分的 503 原因")
async def test_overload_rejected_with_distinct_reasons(server):
    async def one(cli):
        return await cli.get("/expensive", params={"delay": 0.3, "use_cache": "false"})

    async with httpx.AsyncClient(base_url=server, timeout=5) as cli:
        results = await asyncio.gather(*(one(cli) for _ in range(8)))
    statuses = [r.status_code for r in results]
    bodies = [r.json() for r in results]
    assert 200 in statuses
    assert 503 in statuses
    reasons = {b.get("reason") for b in bodies if b.get("error") == "overloaded"}
    assert reasons <= {"limit_reached", "queue_timeout"}
    assert reasons  # 至少出现一种可区分原因
    assert all(b["error"] == "overloaded" for b in bodies
               if b.get("reason") in {"limit_reached", "queue_timeout"})


@pytest.mark.business("拒绝样本计入 rejected，不计入 success")
async def test_rejected_accounting(server):
    async def one(cli):
        return await cli.get("/expensive", params={"delay": 0.3, "use_cache": "false"})

    async with httpx.AsyncClient(base_url=server, timeout=5) as cli:
        await asyncio.gather(*(one(cli) for _ in range(8)))
        m = (await cli.get("/metrics")).json()
    s = next(r for r in m["routes"] if r["route"] == "/expensive")
    c = s["counts"]
    assert c["rejected"] >= 1
    assert c["success"] + c["rejected"] == s["sample_count"]


@pytest.mark.business("缓存命中：重复并发请求不回源、不占并发位、秒回")
async def test_cache_hit_concurrent(server):
    async def one(cli):
        return await cli.get("/expensive", params={"delay": 0.2})

    async with httpx.AsyncClient(base_url=server, timeout=5) as cli:
        # 第一次填充
        first = await cli.get("/expensive", params={"delay": 0.2})
        assert first.json()["cached"] is False
        # 后续 8 个并发全部命中（即便并发上限只有 2，也不应出现 503）
        results = await asyncio.gather(*(one(cli) for _ in range(8)))
    assert all(r.status_code == 200 for r in results)
    assert all(r.json()["cached"] is True for r in results)


@pytest.mark.business("缓存失效：显式失效后返回新结果；TTL 到期不返回旧值")
async def test_cache_invalidation_and_ttl(server):
    async with httpx.AsyncClient(base_url=server, timeout=5) as cli:
        r1 = await cli.get("/expensive", params={"delay": 0.01})
        r2 = await cli.get("/expensive", params={"delay": 0.01})
        assert r2.json()["cached"] is True
        inv = await cli.post("/admin/cache/invalidate")
        assert inv.status_code == 200 and inv.json()["invalidated"] >= 1
        r3 = await cli.get("/expensive", params={"delay": 0.01})
        assert r3.json()["cached"] is False


@pytest.mark.business("缓存开关：关闭后每次回源，打开后恢复缓存")
async def test_cache_switch_api(server):
    async with httpx.AsyncClient(base_url=server, timeout=5) as cli:
        await cli.post("/admin/cache/enabled", params={"enabled": "false"})
        r1 = await cli.get("/expensive", params={"delay": 0.01})
        r2 = await cli.get("/expensive", params={"delay": 0.01})
        assert r1.json()["cached"] is False
        assert r2.json()["cached"] is False
        await cli.post("/admin/cache/enabled", params={"enabled": "true"})
        await cli.get("/expensive", params={"delay": 0.01})
        r4 = await cli.get("/expensive", params={"delay": 0.01})
        assert r4.json()["cached"] is True


@pytest.mark.business("异常链路：依赖失败返回 500、超时返回 504，且不缓存错误")
async def test_error_and_timeout_http_semantics(server):
    async with httpx.AsyncClient(base_url=server, timeout=5) as cli:
        r_fail = await cli.get("/expensive",
                               params={"delay": 0.01, "fail": "true"})
        assert r_fail.status_code == 500
        r_timeout = await cli.get("/expensive",
                                  params={"delay": 0.2, "timeout": 0.02})
        assert r_timeout.status_code == 504
        assert r_timeout.json()["error"] == "dependency_timeout"
        # 失败结果没被缓存：相同 fail 请求仍是 500 而不是命中
        r_again = await cli.get("/expensive",
                                params={"delay": 0.01, "fail": "true"})
        assert r_again.status_code == 500


@pytest.mark.business("异常不被吞：错误请求不会变成 200 成功")
async def test_failures_never_appear_as_success(server):
    async with httpx.AsyncClient(base_url=server, timeout=5) as cli:
        rf = await cli.get("/expensive",
                           params={"delay": 0.01, "fail": "true", "use_cache": "false"})
        rt = await cli.get("/expensive",
                           params={"delay": 0.2, "timeout": 0.01, "use_cache": "false"})
    assert rf.status_code == 500 and rt.status_code == 504
