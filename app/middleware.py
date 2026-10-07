"""端到端指标采集 ASGI 中间件。

职责：

1. 为每个 HTTP 请求创建 :class:`RequestTiming` 并写入 ``scope["state"]``，
   路由内通过 ``request.state.timing`` 标记排队 / 慢依赖阶段。
2. 拦截响应起始帧拿到状态码，按状态码区分结果：
   2xx/3xx=success，503=rejected，504=timeout，其余（4xx/5xx）=error。
3. 路由抛出的未处理异常穿透本中间件时（最终由服务端错误中间件转 500），
   记录为 error 后原样重新抛出——不吞异常、不改变对外行为。
4. 以路由匹配结果（scope["route"]）作为指标维度；未匹配按 "unmatched" 计。
"""
from __future__ import annotations

from typing import Any

from .concurrency import DependencyTimeoutError, RejectedError
from .metrics import Outcome, RollingWindowMetrics, Sample
from .timing import RequestTiming


def _route_name(scope) -> str:
    route = scope.get("route")
    if route is not None:
        path = getattr(route, "path", None)
        if path:
            return path
    return scope.get("path", "unknown")


def outcome_from_status(status_code: int) -> Outcome:
    if 200 <= status_code < 400:
        return "success"
    if status_code == 503:
        return "rejected"
    if status_code == 504:
        return "timeout"
    return "error"


class MetricsMiddleware:
    def __init__(self, app: Any, metrics: RollingWindowMetrics) -> None:
        self.app = app
        self.metrics = metrics

    async def __call__(self, scope, receive, send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        timing = RequestTiming().start()
        # 路由处理器通过 request.state.timing 拿到该对象
        scope.setdefault("state", {})["timing"] = timing

        status_holder: dict = {"code": 0}

        async def send_wrapper(message):
            if message["type"] == "http.response.start":
                status_holder["code"] = int(message["status"])
            await send(message)

        forced_outcome: Outcome | None = None
        try:
            await self.app(scope, receive, send_wrapper)
        except RejectedError:
            forced_outcome = "rejected"
            raise
        except DependencyTimeoutError:
            forced_outcome = "timeout"
            raise
        except Exception:
            forced_outcome = "error"
            raise
        finally:
            timing.stop()
            code = status_holder["code"]
            outcome = forced_outcome or outcome_from_status(code)
            # 未拿到任何响应帧（连接中断等）按 error 计
            if code == 0 and forced_outcome is None:
                outcome = "error"
            self.metrics.record(
                _route_name(scope),
                Sample(
                    ts=timing.start_ts,
                    total=timing.total,
                    self_time=timing.self_time,
                    dependency=timing.dependency,
                    queue=timing.queue,
                    outcome=outcome,
                ),
            )
