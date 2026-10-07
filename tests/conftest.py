"""Shared pytest fixtures."""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from app.cache import ResultCache
from app.main import create_app
from app.metrics import SlidingWindowStats
from app.overload import ConcurrencyGuard


@pytest.fixture
def stats() -> SlidingWindowStats:
    return SlidingWindowStats(window_seconds=60.0)


@pytest.fixture
def guard() -> ConcurrencyGuard:
    return ConcurrencyGuard(max_inflight=1, max_queue=1, queue_timeout=0.05)


@pytest.fixture
def fake_clock():
    """A controllable monotonic clock."""

    class Clock:
        def __init__(self) -> None:
            self.t = 1000.0

        def __call__(self) -> float:
            return self.t

    return Clock()


@pytest.fixture
def cache(fake_clock) -> ResultCache:
    return ResultCache(ttl_seconds=10.0, enabled=True, clock=fake_clock)


def make_app(**overrides) -> tuple:
    app = create_app(
        max_inflight=overrides.pop("max_inflight", 1),
        max_queue=overrides.pop("max_queue", 0),
        queue_timeout=overrides.pop("queue_timeout", 0.05),
        cache_ttl=overrides.pop("cache_ttl", 5.0),
        cache_enabled=overrides.pop("cache_enabled", False),
        window_seconds=overrides.pop("window_seconds", 60.0),
        dependency_delay=overrides.pop("dependency_delay", 0.02),
        **overrides,
    )
    return app, TestClient(app)


@pytest.fixture
def app_factory():
    """Return a factory ``(**overrides) -> FastAPI`` with small guard defaults."""

    def _factory(**overrides):
        overrides.setdefault("max_inflight", 1)
        overrides.setdefault("max_queue", 0)
        overrides.setdefault("queue_timeout", 0.05)
        app, _ = make_app(**overrides)
        return app

    return _factory


@pytest.fixture
def make_client():
    """Factory yielding managed (app, TestClient) pairs, closed on teardown."""
    created = []

    def _make(**kw):
        app, c = make_app(**kw)
        c.__enter__()
        created.append(c)
        return app, c

    yield _make
    for c in created:
        c.__exit__(None, None, None)
