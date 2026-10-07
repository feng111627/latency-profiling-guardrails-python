"""pytest plugin: log the business point each test covers and its result.

Every test is marked with ``@pytest.mark.business_point("...")``. During a
normal ``pytest`` run each case emits one line of the form::

    [coverage] PASSED   test_xxx :: 业务点描述

and after the session a numbered coverage summary lists every business
scenario exercised, so the test log alone shows what is covered and the
per-case outcome.
"""
from __future__ import annotations

import logging

import pytest

logger = logging.getLogger("business-coverage")


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers",
        "business_point(label): which business scenario this case covers",
    )
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("[coverage] %(message)s"))
    logger.setLevel(logging.INFO)
    if not any(getattr(h, "_business_handler", False) for h in logger.handlers):
        handler._business_handler = True
        logger.addHandler(handler)
    logger.propagate = False


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    report = outcome.get_result()
    marker = item.get_closest_marker("business_point")
    report.business_label = marker.args[0] if marker else None
    if report.when == "call" and report.business_label:
        logger.info("%-7s %s :: %s",
                    report.outcome.upper(), item.name, report.business_label)


def pytest_terminal_summary(terminalreporter, config):
    seen: list[tuple[str, str, str]] = []
    for reports in terminalreporter.stats.values():
        for report in reports:
            label = getattr(report, "business_label", None)
            if getattr(report, "when", None) == "call" and label:
                seen.append((report.nodeid, report.outcome, label))
    seen.sort()
    terminalreporter.write_sep("=", "业务点覆盖清单 (business coverage)")
    for i, (nodeid, outcome, label) in enumerate(seen, 1):
        terminalreporter.write_line(
            f"{i:2d}. [{outcome.upper():7s}] {label}  ({nodeid})"
        )
    missing = [
        report.nodeid
        for reports in terminalreporter.stats.values()
        for report in reports
        if getattr(report, "when", None) == "call"
        and getattr(report, "business_label", None) is None
    ]
    if missing:
        terminalreporter.write_line(
            f"缺少 business_point 标记的用例: {', '.join(missing)}",
            red=True,
        )
