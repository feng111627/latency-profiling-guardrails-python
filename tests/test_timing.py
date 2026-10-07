"""Business coverage: latency breakdown into queue / dependency / self."""
from __future__ import annotations

import asyncio

import pytest

from app.timing import TimingRecorder


@pytest.mark.business_point("耗时拆分：queue+dependency+self 约等于 total")
async def test_segment_sum_matches_total():
    r = TimingRecorder(started_at=0.0)
    base = r.now()
    r.started_at = base
    r.mark_enqueued()
    await asyncio.sleep(0.02)   # 20ms queue
    r.mark_acquired()
    with r.dependency():
        await asyncio.sleep(0.03)  # 30ms dependency
    seg = r.finish()

    assert seg["queue_ms"] == pytest.approx(20, abs=8)
    assert seg["dependency_ms"] == pytest.approx(30, abs=8)
    assert seg["total_ms"] == pytest.approx(
        seg["queue_ms"] + seg["dependency_ms"] + seg["self_ms"], abs=0.5
    )
    assert seg["self_ms"] >= 0


@pytest.mark.business_point("耗时拆分：多次依赖调用时间累加")
async def test_multiple_dependency_spans_accumulate():
    r = TimingRecorder(started_at=0.0)
    with r.dependency():
        await asyncio.sleep(0.01)
    with r.dependency():
        await asyncio.sleep(0.01)
    seg = r.finish()
    assert seg["dependency_ms"] == pytest.approx(20, abs=8)


@pytest.mark.business_point("耗时拆分：无排队无依赖时全部计入 self")
def test_pure_self_time():
    r = TimingRecorder(started_at=0.0)
    base = r.now()
    r.started_at = base
    seg = r.finish()
    assert seg["queue_ms"] == 0.0
    assert seg["dependency_ms"] == 0.0
    assert seg["self_ms"] == seg["total_ms"]


@pytest.mark.business_point("耗时拆分：嵌套/重叠的依赖段被拒绝，避免计时串段")
async def test_overlapping_dependency_spans_rejected():
    r = TimingRecorder(started_at=0.0)
    with pytest.raises(RuntimeError):
        with r.dependency():
            with r.dependency():
                pass
