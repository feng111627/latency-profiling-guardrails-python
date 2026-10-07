"""模拟的本地慢依赖。

用 ``asyncio.sleep`` 模拟"等待本地依赖"（如本地缓存/本地子进程/同机服务），
可注入 delay / 失败 / 超时。超时以 :class:`DependencyTimeoutError` 抛出，
依赖失败以普通异常抛出，二者在路由层被区分记录为 timeout / error。
"""
from __future__ import annotations

import asyncio
from typing import Optional

from .concurrency import DependencyTimeoutError


async def slow_local_call(
    delay: float,
    fail: bool = False,
    timeout: Optional[float] = None,
) -> str:
    """等待 ``delay`` 秒后返回结果。

    - ``timeout`` 不为 None 且 delay > timeout 时抛 DependencyTimeoutError
    - ``fail=True`` 时在等待结束后抛 RuntimeError（依赖自身报错）
    """
    try:
        if timeout is not None and delay > timeout:
            await asyncio.sleep(timeout)
            raise DependencyTimeoutError(elapsed=timeout, timeout=timeout)
        await asyncio.sleep(delay)
    except asyncio.CancelledError:
        raise
    if fail:
        raise RuntimeError("slow dependency failed")
    return f"slow-result-after-{delay:.3f}s"
