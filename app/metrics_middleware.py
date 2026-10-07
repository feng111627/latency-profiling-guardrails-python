"""HTTP middleware: end-to-end timing, outcome classification, metrics.

For every request a :class:`~app.timing.TimingRecorder` is created (one per
request, never shared), exposed both as ``request.state.timing`` and via the
:data:`timing_var` context var for endpoints. The middleware:

1. wraps the response to capture its HTTP status;
2. catches exceptions raised downstream — they are logged with a full
   traceback (never silently swallowed) and converted to JSON error
   responses, so the externally observable behaviour stays consistent and
   the failing request is classified as ``error``/``timeout``/``rejected``;
3. records one :class:`~app.metrics.Sample` into the sliding window under the
   *route template* (e.g. ``/expensive`` rather than per-query-string URLs).

Outcome classification:

- 503 with an ``x-overload-reason`` header -> ``rejected`` (+reason)
- 504                                       -> ``timeout``
- other 5xx                                 -> ``error``
- everything else                           -> ``success``
"""
from __future__ import annotations

import contextvars
import json
import logging
import time

from starlette.types import ASGIApp, Message, Receive, Scope, Send

from app.metrics import Sample, SlidingWindowStats
from app.overload import OverloadError
from app.timing import TimingRecorder
from app.dependencies import DependencyTimeoutError

logger = logging.getLogger("app.metrics")

timing_var: contextvars.ContextVar[TimingRecorder] = contextvars.ContextVar(
    "timing"
)

REJECT_HEADER = "x-overload-reason"


def new_recorder() -> TimingRecorder:
    return TimingRecorder(started_at=time.perf_counter())


def get_timing() -> TimingRecorder | None:
    return timing_var.get(None)


def _route_name(scope: Scope) -> str:
    route = scope.get("route")
    path = getattr(route, "path", None)
    return path if isinstance(path, str) else scope.get("path", "unknown")


async def _json_response(
    send: Send,
    status: int,
    payload: dict,
    extra_headers: list[tuple[bytes, bytes]] | None = None,
) -> None:
    body = json.dumps(payload).encode("utf-8")
    headers = [
        (b"content-type", b"application/json"),
        (b"content-length", str(len(body)).encode()),
    ]
    if extra_headers:
        headers.extend(extra_headers)
    await send({"type": "http.response.start", "status": status,
                "headers": headers})
    await send({"type": "http.response.body", "body": body})


class MetricsMiddleware:
    def __init__(self, app: ASGIApp, stats: SlidingWindowStats) -> None:
        self.app = app
        self.stats = stats

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        recorder = new_recorder()
        token = timing_var.set(recorder)
        scope["state"] = dict(scope.get("state") or {})
        scope["state"]["timing"] = recorder
        status_box = {"status": 200}
        started_box = {"value": False}
        reject_reason_box: dict[str, str | None] = {"reason": None}

        async def send_wrapper(message: Message) -> None:
            if message["type"] == "http.response.start":
                started_box["value"] = True
                status_box["status"] = message["status"]
                for k, v in message.get("headers", []):
                    if k == REJECT_HEADER.encode():
                        reject_reason_box["reason"] = v.decode()
            await send(message)

        outcome = "success"
        try:
            await self.app(scope, receive, send_wrapper)
        except OverloadError as exc:
            outcome = "rejected"
            reject_reason_box["reason"] = exc.reason
            status_box["status"] = 503
            if not started_box["value"]:
                await _json_response(
                    send,
                    503,
                    {"error": "overloaded", "reason": exc.reason,
                     "detail": exc.detail},
                    [(REJECT_HEADER.encode(), exc.reason.encode())],
                )
        except DependencyTimeoutError as exc:
            outcome = "timeout"
            logger.exception("dependency timeout for %s", scope.get("path"))
            status_box["status"] = 504
            if not started_box["value"]:
                await _json_response(
                    send, 504,
                    {"error": "dependency_timeout", "detail": str(exc)},
                )
        except Exception:
            outcome = "error"
            # Full traceback in logs; the failure must never look like success.
            logger.exception("unhandled error for %s", scope.get("path"))
            status_box["status"] = 500
            if not started_box["value"]:
                await _json_response(send, 500,
                                     {"error": "internal_server_error"})
        finally:
            status = status_box["status"]
            if status == 503 and reject_reason_box["reason"]:
                outcome = "rejected"
            elif status == 504:
                outcome = "timeout"
            elif status >= 500:
                outcome = "error"
            segments = recorder.finish()
            self.stats.record(
                _route_name(scope),
                Sample(
                    timestamp=time.time(),
                    outcome=outcome,
                    reject_reason=reject_reason_box["reason"],
                    **segments,
                ),
            )
            timing_var.reset(token)
