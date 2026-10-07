"""应用组装入口。

对外行为：原有 GET / 行为不变；新增接口见 :mod:`app.routes`。
异常 -> HTTP 响应的映射在这里集中处理，指标中间件按最终结果记录。
"""
from __future__ import annotations

import os

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from .concurrency import DependencyTimeoutError, RejectedError
from .middleware import MetricsMiddleware
from . import routes
from .routes import router


def register_exception_handlers(app: FastAPI) -> FastAPI:
    """注册业务异常 -> HTTP 响应映射，便于测试复用。"""

    @app.exception_handler(RejectedError)
    async def _on_rejected(request: Request, exc: RejectedError) -> JSONResponse:
        return JSONResponse(
            status_code=503,
            content={
                "error": "overloaded",
                "reason": exc.reason.value,
                "limit": exc.limit,
                "queue_timeout_seconds": exc.queue_timeout,
            },
        )

    @app.exception_handler(DependencyTimeoutError)
    async def _on_timeout(request: Request, exc: DependencyTimeoutError) -> JSONResponse:
        return JSONResponse(
            status_code=504,
            content={
                "error": "dependency_timeout",
                "elapsed_seconds": exc.elapsed,
                "timeout_seconds": exc.timeout,
            },
        )

    # 未预期异常：不吞掉——转成 JSON 500，完整堆栈仍由 Starlette 服务端
    # 错误中间件（ServerErrorMiddleware）记录到日志；指标中间件统一计为
    # error，不会污染成功统计，也不会让失败结果变成 200。
    @app.exception_handler(Exception)
    async def _on_unexpected(request: Request, exc: Exception) -> JSONResponse:
        return JSONResponse(status_code=500, content={"error": "internal_error"})

    return app


def create_app() -> FastAPI:
    """用 app.routes 当前的组件单例组装应用。"""
    app = FastAPI(title="latency-profiling-guardrails")
    app.add_middleware(MetricsMiddleware, metrics=routes.metrics)
    app.include_router(router)
    register_exception_handlers(app)
    return app


app = create_app()


def main() -> None:
    port = int(os.environ.get("PORT", "8000"))
    uvicorn.run(app, host="127.0.0.1", port=port)


if __name__ == "__main__":
    main()
