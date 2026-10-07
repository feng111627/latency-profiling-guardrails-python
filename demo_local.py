"""Local end-to-end verification without external dependencies.

Runs the real uvicorn ASGI app on 127.0.0.1, then exercises:
  1. original behaviour (GET /)
  2. normal expensive requests and /metrics quantiles
  3. a dependency timeout (504) and its metrics classification
  4. a concurrency burst and differentiated overload rejection (503)
  5. cache enable -> HIT, explicit invalidation -> MISS

Usage:
    uv run python demo_local.py
"""
from __future__ import annotations

import asyncio
import json
import logging

import httpx
import uvicorn

from app.main import create_app

# Failures are logged with full tracebacks by the metrics middleware; the
# demo already prints the classified status codes, so silence the stack lines.
logging.getLogger("app.metrics").setLevel(logging.CRITICAL)


async def hit(client: httpx.AsyncClient, label: str, **kw):
    r = await client.get("/expensive", **kw)
    reason = r.headers.get("x-overload-reason", "")
    cache = r.headers.get("x-cache", "")
    print(f"  {label:28s} -> HTTP {r.status_code} "
          f"{reason and 'reason=' + reason or '':28s} x-cache={cache}")
    return r


async def scenario(base: str) -> None:
    async with httpx.AsyncClient(base_url=base, timeout=5) as c:
        print("1) 原有行为:")
        r = await c.get("/")
        print("   GET / ->", r.status_code, r.json())
        assert r.json() == {"Hello": "World"}

        print("2) 正常请求 x 20（含约 20ms 慢依赖）:")
        for _ in range(20):
            await hit(c, "normal", params={"dep_timeout": 1})

        print("3) 依赖超时:")
        await hit(c, "dep_timeout=0.005", params={"dep_timeout": 0.005})

        print("4) 并发冲击（max_inflight=2, max_queue=1）:")
        burst = await asyncio.gather(*[
            hit(c, f"burst-{i}", params={"delay": 0.2}) for i in range(8)
        ])
        rejected = [r for r in burst if r.status_code == 503]
        reasons = {r.headers["x-overload-reason"] for r in rejected}
        print("   被拒:", len(rejected), "原因集合:", reasons)

        print("5) 缓存:")
        await c.post("/admin/cache/enabled", json={"enabled": True})
        await hit(c, "cache warm", params={"q": "demo"})
        await hit(c, "cache again", params={"q": "demo"})
        await c.request("DELETE", "/admin/cache/expensive:demo")
        await hit(c, "after invalidate", params={"q": "demo"})

        print("\n6) /metrics（窗口内 /expensive 汇总）:")
        m = (await c.get("/metrics")).json()
        for row in m["routes"]:
            if row["route"] == "/expensive":
                print(json.dumps(row, indent=2, ensure_ascii=False))


async def main() -> None:
    config = uvicorn.Config(
        create_app(max_inflight=2, max_queue=1, queue_timeout=0.1,
                   cache_ttl=30, dependency_delay=0.02),
        host="127.0.0.1", port=8765, log_level="warning",
    )
    server = uvicorn.Server(config)
    task = asyncio.create_task(server.serve())
    await asyncio.sleep(0.4)
    try:
        await scenario("http://127.0.0.1:8765")
    finally:
        server.should_exit = True
        await task


if __name__ == "__main__":
    asyncio.run(main())
