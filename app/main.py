"""Application factory wiring stats, guard, cache and routes together."""
from __future__ import annotations

from fastapi import FastAPI

from app.cache import ResultCache
from app.dependencies import SlowDependency
from app.metrics import SlidingWindowStats
from app.metrics_middleware import MetricsMiddleware
from app.overload import ConcurrencyGuard
from app import routes


def create_app(
    max_inflight: int = 2,
    max_queue: int = 1,
    queue_timeout: float = 0.05,
    cache_ttl: float = 5.0,
    cache_enabled: bool = False,
    window_seconds: float = 60.0,
    dependency_delay: float = 0.02,
    dependency_fail: bool = False,
) -> FastAPI:
    """Build the application.

    Defaults are intentionally small so overload behaviour is observable
    locally with a handful of concurrent requests. Nothing here changes the
    original ``GET /`` response.
    """
    app = FastAPI(title="latency-profiling-guardrails")

    stats = SlidingWindowStats(window_seconds=window_seconds)
    guard = ConcurrencyGuard(
        max_inflight=max_inflight,
        max_queue=max_queue,
        queue_timeout=queue_timeout,
    )
    cache = ResultCache(ttl_seconds=cache_ttl, enabled=cache_enabled)
    dependency = SlowDependency(
        base_delay=dependency_delay, fail=dependency_fail
    )

    routes.configure(stats, guard, cache, dependency)
    app.add_middleware(MetricsMiddleware, stats=stats)
    app.include_router(routes.api_router)
    app.include_router(routes.admin_router)

    # Expose components on app.state for tests / operational tooling.
    app.state.stats = stats
    app.state.guard = guard
    app.state.cache = cache
    app.state.dependency = dependency

    @app.get("/", include_in_schema=False)
    def read_root():
        return {"Hello": "World"}

    return app
