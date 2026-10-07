"""Toggleable result cache for expensive endpoints.

Guarantees
----------
- **Explicit expiry**: an entry is valid until ``ttl_seconds`` elapse
  (monotonic clock) *or* it is invalidated explicitly via
  :meth:`invalidate` / :meth:`set_enabled`. Expired entries are treated as
  absent and replaced; a stale value is never returned.
- **No cache stampede** ("并发重复请求不能击穿"): concurrent lookups for the
  same miss/expired key share one producer invocation via a single-flight
  future; the other callers await the same result instead of recomputing.
- **Errors are never cached**: if the producer raises, every waiter sees the
  exception and nothing is stored, so a transient failure cannot be memoized
  as a (wrong) successful result.
- **Toggleable**: when disabled, all reads bypass the cache and no entry is
  written. Disabling (or invalidating) bumps a generation counter, so a
  producer that was started before invalidation but finishes afterwards can
  never resurrect stale data.
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass


@dataclass
class CacheEntry:
    value: object
    expires_at: float
    generation: int


class ResultCache:
    def __init__(
        self,
        ttl_seconds: float,
        enabled: bool = False,
        clock=time.monotonic,
    ) -> None:
        if ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be positive")
        self.ttl_seconds = float(ttl_seconds)
        self.enabled = enabled
        self._clock = clock
        self._entries: dict[str, CacheEntry] = {}
        self._inflight: dict[str, asyncio.Future] = {}
        self._locks: dict[str, asyncio.Lock] = {}
        self._generation = 0
        self.hits = 0
        self.misses = 0
        self.bypassed = 0

    def _lock_for(self, key: str) -> asyncio.Lock:
        lock = self._locks.get(key)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[key] = lock
        return lock

    def _take(self, key: str) -> tuple[bool, object | None]:
        """Return (cached, value); expired entries are removed, never served."""
        entry = self._entries.get(key)
        if entry is None:
            return False, None
        if entry.expires_at <= self._clock() or entry.generation != self._generation:
            self._entries.pop(key, None)
            return False, None
        return True, entry.value

    def peek(self, key: str):
        """Return (value, True) for a live stored entry without computing.

        Unlike :meth:`get_or_set` this never creates single-flight state and
        never runs a producer, so cache hits need not acquire a concurrency
        slot. Expired/disabled caches behave as misses.
        """
        if not self.enabled:
            self.bypassed += 1
            return None, False
        hit, value = self._take(key)
        if hit:
            self.hits += 1
        return value, hit

    async def get_or_set(self, key: str, producer):
        """Return ``(value, source)`` where source is one of:

        - ``"HIT"``    : a live stored entry was served;
        - ``"SHARED"`` : the caller joined an in-flight producer (stampede
                         protection);
        - ``"MISS"``   : this caller produced the value itself;
        - ``"BYPASS"`` : cache disabled, producer ran.

        ``producer`` may be a coroutine function or return an awaitable.
        """
        if not self.enabled:
            self.bypassed += 1
            produced = producer()
            if asyncio.iscoroutine(produced):
                produced = await produced
            return produced, "BYPASS"

        async with self._lock_for(key):
            hit, value = self._take(key)
            if hit:
                self.hits += 1
                return value, "HIT"

            future = self._inflight.get(key)
            if future is None:
                future = asyncio.get_running_loop().create_future()
                self._inflight[key] = future
                owner = True
                owner_generation = self._generation
            else:
                owner = False

        if not owner:
            # Single-flight waiter: shares the owner's result or exception.
            self.misses += 1
            return await future, "SHARED"

        try:
            produced = producer()
            if asyncio.iscoroutine(produced):
                produced = await produced
        except BaseException as exc:
            # Do not cache errors; wake every waiter with the exception.
            async with self._lock_for(key):
                fut = self._inflight.pop(key, None)
            if fut is not None and not fut.done():
                fut.set_exception(exc)
                # Mark the shared future retrieved so asyncio does not emit
                # "exception never retrieved" when there were no waiters;
                # awaiting waiters still receive the same exception.
                fut.exception()
            raise

        async with self._lock_for(key):
            fut = self._inflight.pop(key, None)
            # If invalidate()/disable ran while the producer was in flight,
            # the result belongs to a dead generation: hand it to current
            # waiters but never store it, so stale data cannot resurrect.
            if self._generation == owner_generation:
                self._entries[key] = CacheEntry(
                    value=produced,
                    expires_at=self._clock() + self.ttl_seconds,
                    generation=owner_generation,
                )
        if fut is not None and not fut.done():
            fut.set_result(produced)
        self.misses += 1
        return produced, "MISS"

    def invalidate(self, key: str | None = None) -> int:
        """Invalidate one key, or all entries when ``key`` is None.

        Bumping the generation also invalidates any producer result that is
        still in flight, preventing stale resurrection.
        """
        if key is None:
            count = len(self._entries)
            self._entries.clear()
            self._generation += 1
            return count
        self._generation += 1
        return 1 if self._entries.pop(key, None) is not None else 0

    def set_enabled(self, enabled: bool) -> None:
        if not enabled and self.enabled:
            self._entries.clear()
            self._generation += 1
        self.enabled = enabled

    def status(self) -> dict:
        return {
            "enabled": self.enabled,
            "ttl_seconds": self.ttl_seconds,
            "entries": len(self._entries),
            "inflight": len(self._inflight),
            "generation": self._generation,
            "hits": self.hits,
            "misses": self.misses,
            "bypassed": self.bypassed,
        }
