"""业务路由。

- GET /            : 原有接口，行为保持不变
- GET /health      : 健康检查
- GET /expensive   : 昂贵接口（并发上限 + 过载拒绝 + 可开关结果缓存），
                     内部调用模拟的本地慢依赖，便于观察分段耗时
- GET /metrics     : 导出当前滚动窗口指标
- POST /admin/...  : 缓存开关与显式失效
"""
from __future__ import annotations

from fastapi import APIRouter, Query, Request

from .cache import SingleFlightCache
from .concurrency import ConcurrencyGuard
from .deps import slow_local_call
from .metrics import RollingWindowMetrics

router = APIRouter()

# 默认：最多 3 个并发执行，超出排队 0.2 秒，排队槽位 20
metrics = RollingWindowMetrics(window_seconds=60.0)
guard = ConcurrencyGuard(limit=3, queue_timeout=0.2, max_queue=20)
expensive_cache = SingleFlightCache(ttl_seconds=5.0)


@router.get("/")
async def read_root() -> dict:
    return {"Hello": "World"}


@router.get("/health")
async def health() -> dict:
    return {"status": "ok"}


@router.get("/metrics")
async def get_metrics() -> dict:
    return expensive_cache_snapshot()


def expensive_cache_snapshot() -> dict:
    data = metrics.snapshot_dict()
    data["guard"] = {"limit": guard.limit, "inflight": guard.inflight}
    data["cache"] = {"enabled": expensive_cache.enabled, "size": expensive_cache.size()}
    return data


async def _do_expensive(request: Request, delay: float, fail: bool, timeout: float) -> dict:
    timing = request.state.timing
    # 慢依赖分段：等待本地依赖
    timing.dependency_start()
    try:
        result = await slow_local_call(
            delay, fail=fail, timeout=timeout if timeout > 0 else None
        )
    finally:
        timing.dependency_end()
    # self 段：结果封装
    return {"result": result, "delay": delay, "cached": False}


@router.get("/expensive")
async def expensive(
    request: Request,
    delay: float = Query(0.2, ge=0, le=30),
    fail: bool = False,
    timeout: float = Query(0.0, ge=0, description=">0 时作为慢依赖超时阈值"),
    use_cache: bool = True,
) -> dict:
    timing = request.state.timing
    cache_key = f"expensive:delay={delay}:fail={fail}:timeout={timeout}"

    async def loader() -> dict:
        # 缓存未命中才进入并发闸门：命中缓存的请求不占执行位
        async with guard.entry(timing):
            payload = await _do_expensive(request, delay, fail, timeout)
            return payload

    if use_cache and expensive_cache.enabled:
        payload, hit = await expensive_cache.get_or_set(cache_key, loader)
        return {**payload, "cached": hit}
    return await loader()


@router.post("/admin/cache/invalidate")
async def invalidate_cache(key: str | None = None) -> dict:
    removed = expensive_cache.invalidate(key)
    return {"invalidated": removed, "size": expensive_cache.size()}


@router.post("/admin/cache/enabled")
async def set_cache_enabled(enabled: bool) -> dict:
    expensive_cache.set_enabled(enabled)
    return {"enabled": expensive_cache.enabled}
