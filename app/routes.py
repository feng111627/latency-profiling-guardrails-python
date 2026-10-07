"""HTTP routes.

Public surface
---------------
- ``GET  /``                 : original service behaviour (untouched).
- ``GET  /health``           : liveness probe.
- ``GET  /expensive``        : guarded expensive endpoint (concurrency cap,
                               optional result cache). Query params:
                               ``q`` (cache key), ``delay`` (own processing
                               seconds), ``dep_timeout`` (dependency budget).
- ``GET  /metrics``          : per-route sliding-window statistics.

Admin surface (cache of /expensive)
------------------------------------
- ``GET    /admin/cache``      : cache status.
- ``POST   /admin/cache/enabled`` body {"enabled": bool}: turn on/off.
- ``DELETE /admin/cache/{key}`` and ``DELETE /admin/cache?all=true``:
                               explicit invalidation.
"""
from __future__ import annotations

import asyncio
import json

from fastapi import APIRouter, Header, Query, Response
from pydantic import BaseModel

from app.cache import ResultCache
from app.dependencies import SlowDependency
from app.metrics import SlidingWindowStats
from app.metrics_middleware import get_timing
from app.overload import ConcurrencyGuard

api_router = APIRouter()
admin_router = APIRouter(prefix="/admin")

_stats: SlidingWindowStats | None = None
_guard: ConcurrencyGuard | None = None
_cache: ResultCache | None = None
_dependency: SlowDependency | None = None


def configure(
    stats: SlidingWindowStats,
    guard: ConcurrencyGuard,
    cache: ResultCache,
    dependency: SlowDependency,
) -> None:
    global _stats, _guard, _cache, _dependency
    _stats, _guard, _cache, _dependency = stats, guard, cache, dependency


@api_router.get("/health")
def health() -> dict:
    return {"status": "ok"}


def _json(value: dict, cache_state: str) -> Response:
    return Response(
        content=json.dumps(value),
        media_type="application/json",
        headers={"x-cache": cache_state},
    )


@api_router.get("/expensive")
async def expensive(
    q: str = Query("default", description="cache key"),
    delay: float = Query(0.0, ge=0, description="own processing seconds"),
    dep_timeout: float = Query(0.5, gt=0, description="dependency budget s"),
    x_cache_control: str | None = Header(default=None,
                                          alias="x-cache-control"),
) -> Response:
    """Guarded expensive endpoint.

    A live cache hit is served before touching the concurrency guard, so
    cache hits cannot be rejected by overload and cost no backend slot.
    Otherwise the request queues for a slot; after admission it either finds
    a value cached by an earlier request (filled while queued), joins a
    single in-flight producer (``SHARED``, no stampede), or recomputes
    (``MISS``). ``x-cache-control: no-cache`` forces recompute; an expired
    entry is never served.
    """
    timing = get_timing()
    key = f"expensive:{q}"
    if x_cache_control and x_cache_control.lower() == "no-cache":
        _cache.invalidate(key)
    else:
        value, served = _cache.peek(key)
        if served:
            return _json(value, "HIT")

    timing.mark_enqueued()
    async with _guard():
        timing.mark_acquired()

        async def produce():
            with timing.dependency():
                dep_result = await _dependency.call(
                    payload=q, timeout=dep_timeout
                )
            if delay:
                await asyncio.sleep(delay)
            return {
                "q": q,
                "dependency": dep_result,
                "own_delay": delay,
            }

        value, source = await _cache.get_or_set(key, produce)
    return _json(value, source)


class CacheEnabledBody(BaseModel):
    enabled: bool


@admin_router.get("/cache")
def cache_status() -> dict:
    return _cache.status()


@admin_router.post("/cache/enabled")
def cache_set_enabled(body: CacheEnabledBody) -> dict:
    _cache.set_enabled(body.enabled)
    return _cache.status()


@admin_router.delete("/cache")
def cache_invalidate(all: bool = Query(False), key: str | None = None) -> dict:
    if all:
        removed = _cache.invalidate(None)
    elif key is not None:
        removed = _cache.invalidate(key)
    else:
        removed = 0
    return {"invalidated": removed, **_cache.status()}


@admin_router.delete("/cache/{key}")
def cache_invalidate_key(key: str) -> dict:
    removed = _cache.invalidate(key)
    return {"invalidated": removed, "key": key, **_cache.status()}


@api_router.get("/metrics")
def metrics() -> dict:
    snapshots = _stats.all_snapshots()
    return {
        "window_seconds": _stats.window_seconds,
        "routes": [
            {
                "route": s.route,
                "counts": s.counts.__dict__,
                "reject_reasons": s.reject_reasons,
                "success_latency_ms": s.success_latency_ms,
                "breakdown_ms": s.breakdown_ms,
                "total_latency_by_outcome_ms": s.total_latency_by_outcome_ms,
            }
            for s in snapshots
        ],
    }
