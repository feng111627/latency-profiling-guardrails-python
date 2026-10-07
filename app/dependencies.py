"""Local slow dependency simulation.

The dependency is intentionally in-process so the whole system can be
verified locally without external services. It is fully controllable:

- ``base_delay``  : artificial latency added to every call;
- ``fail`` / ``fail_message``: when set, every call raises ``RuntimeError``
  *after* the delay, simulating a dependency-side failure;
- the caller passes a ``timeout`` budget. Exceeding it raises
  :class:`DependencyTimeoutError` (mapped by the API to HTTP 504 and
  classified as the ``timeout`` outcome, distinct from generic errors).

Calls are coroutines so they actually release the event loop while
"waiting", letting concurrency/queue behaviour be exercised honestly.
"""
from __future__ import annotations

import asyncio


class DependencyTimeoutError(Exception):
    """The local slow dependency did not respond in time (HTTP 504)."""

    def __init__(self, budget: float) -> None:
        super().__init__(f"dependency timeout after {budget:.3f}s")
        self.budget = budget


class SlowDependency:
    def __init__(
        self,
        base_delay: float = 0.0,
        fail: bool = False,
        fail_message: str = "slow dependency failed",
    ) -> None:
        self.base_delay = float(base_delay)
        self.fail = fail
        self.fail_message = fail_message
        self.calls = 0

    async def call(self, payload: str = "ping", timeout: float = 1.0) -> dict:
        self.calls += 1
        try:
            await asyncio.wait_for(asyncio.sleep(self.base_delay), timeout)
        except asyncio.TimeoutError:
            raise DependencyTimeoutError(timeout) from None
        if self.fail:
            raise RuntimeError(self.fail_message)
        return {"echo": payload, "served_by": "slow-dependency"}
