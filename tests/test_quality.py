from datetime import datetime

import pyarrow as pa
import pytest
from pydantic import ValidationError

from agentic_lakehouse.quality import QualityConfig, TableRules, blocking, run_checks

# Three hourly rows of a rollup: one ISP missing, one rate out of range.
ROWS = pa.table(
    {
        "event_ts": [datetime(2026, 1, 1, 0), datetime(2026, 1, 1, 1), datetime(2026, 1, 1, 1)],
        "isp": ["comcast", None, "starlink"],
        "plays": [10, 4, 7],
        "sessions": [5, 6, 2],
        "rate": [0.1, 1.5, 0.2],
    }
)
WINDOW_END = datetime(2026, 1, 1, 2)


def check(spec: dict):
    [result] = run_checks(ROWS, TableRules(table="gold.t", checks=[spec]).checks, time_column="event_ts", window_end=WINDOW_END)
    return result


@pytest.mark.parametrize(
    ("spec", "passed", "detail"),
    [
        ({"type": "row_count", "min": 1}, True, "3 rows"),
        ({"type": "row_count", "max": 2}, False, "3 rows"),
        ({"type": "not_null", "columns": ["plays", "isp"]}, False, "1 row with NULL isp"),
        ({"type": "not_null", "columns": ["plays"]}, True, "no NULLs"),
        ({"type": "unique", "columns": ["event_ts"]}, False, "1 duplicated keys"),
        ({"type": "unique", "columns": ["event_ts", "isp"]}, True, "all unique"),
        ({"type": "range", "column": "rate", "min": 0, "max": 1}, False, "1 row outside"),
        ({"type": "range", "column": "plays", "min": 0}, True, "all within"),
        ({"type": "accepted_values", "column": "isp", "values": ["comcast"]}, False, "1 row with unexpected values, e.g. ['starlink']"),
        ({"type": "expression", "description": "A session has an event.", "sql": "sessions <= plays"}, False, "1 row violating: A session has an event."),
        ({"type": "freshness", "max_lag_hours": 1}, True, "newest bucket 2026-01-01 01:00, 1:00:00 before the window end"),
        ({"type": "freshness", "max_lag_hours": 0.5}, False, "newest bucket 2026-01-01 01:00, 1:00:00 before the window end"),
    ],
)
def test_checks(spec, passed, detail):
    result = check(spec)
    assert (result.passed, result.detail) == (passed, detail)


def test_null_values_are_left_to_not_null():
    # The NULL isp row neither passes nor fails accepted_values; "starlink" alone fails it.
    assert check({"type": "accepted_values", "column": "isp", "values": ["comcast", "starlink"]}).passed


def test_only_failed_error_checks_block():
    checks = [
        {"type": "row_count", "min": 1},
        {"type": "range", "column": "rate", "max": 1, "severity": "warn"},
        {"type": "not_null", "columns": ["isp"]},
    ]
    results = run_checks(ROWS, TableRules(table="gold.t", checks=checks).checks, time_column="event_ts", window_end=WINDOW_END)
    assert [r.check for r in blocking(results)] == ["not_null(isp)"]


def test_freshness_fails_without_rows():
    [result] = run_checks(
        ROWS.slice(0, 0),
        TableRules(table="gold.t", checks=[{"type": "freshness", "max_lag_hours": 1}]).checks,
        time_column="event_ts",
        window_end=WINDOW_END,
    )
    assert (result.passed, result.detail) == (False, "no rows")


@pytest.mark.parametrize(
    ("spec", "match"),
    [
        ({"type": "range", "column": "rate"}, "needs `min`, `max` or both"),
        ({"type": "expression", "description": "d", "sql": "sum(plays) > 0"}, "must not contain an aggregate function"),
        ({"type": "row_count", "minimum": 1}, "Extra inputs are not permitted"),
        ({"type": "nonsense"}, "does not match any of the expected tags"),
    ],
)
def test_bad_checks_are_rejected_when_config_loads(spec, match):
    with pytest.raises(ValidationError, match=match):
        TableRules(table="gold.t", checks=[spec])


def test_a_table_gets_one_rules_entry():
    entry = {"table": "gold.t", "checks": [{"type": "row_count", "min": 1}]}
    with pytest.raises(ValidationError, match="more than once: gold.t"):
        QualityConfig(version="1", rules=[entry, entry])
