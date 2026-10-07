"""测试日志钩子。

运行 pytest 时为每个用例打印：
- 覆盖的业务点（取自 @pytest.mark.business(...)，缺省取 docstring 首行）
- 用例结果（PASSED/FAILED/SKIPPED）与耗时
末尾输出业务点覆盖汇总，方便人工核对覆盖面。
"""
from __future__ import annotations

import pytest


def _business_point(item: pytest.Item) -> str:
    marker = item.get_closest_marker("business")
    if marker and marker.args:
        return marker.args[0]
    doc = getattr(item.function, "__doc__", None)
    if doc:
        return doc.strip().splitlines()[0].strip()
    return ""


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line("markers", "business(text): 该用例覆盖的业务点")


def pytest_collection_modifyitems(config, items) -> None:
    config._business_points = {it.nodeid: _business_point(it) for it in items}


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    report = outcome.get_result()
    if report.when == "call" or (report.when == "setup" and report.failed):
        point = item.config._business_points.get(item.nodeid, "")
        report._business_point = point


def pytest_report_teststatus(report, config):
    # verbose 行的后缀（-v 时展示在每条用例后），分类必须保持标准值，
    # 否则 reporter 的 passed/failed 统计会失效。
    if report.when != "call":
        return None
    point = getattr(report, "_business_point", "")
    verbose_suffix = f"{report.outcome.upper()} | 业务点: {point}"
    return report.outcome, report.outcome[0], verbose_suffix


def pytest_terminal_summary(terminalreporter, exitstatus, config) -> None:
    terminalreporter.write_sep("=", "业务点覆盖汇总")
    points = getattr(config, "_business_points", {})
    by_point: dict[str, list[str]] = {}
    for nodeid, point in points.items():
        by_point.setdefault(point or "(未标注)", []).append(nodeid)
    for point, nodeids in sorted(by_point.items()):
        terminalreporter.write_line(f"- {point}  [{len(nodeids)} 例]")
