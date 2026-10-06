"""The router on the real tiers: answers that span Pinot and Trino must match
DuckDB reading every seeded event.

Both tiers hold the whole seeded batch, so the placement here pretends Pinot
keeps only its second half and Iceberg was flushed 6 hours before the end. That
makes every routing path run: one tier, bucket-aligned split, partial
aggregate merge, and the fallback for metrics that can't be combined.
"""

from dataclasses import replace
from datetime import UTC, timedelta

import pytest

from core.compiler import MetricQuery, SqlCompiler
from core.ontology import Ontology
from core.router import Router

pytestmark = pytest.mark.integration

# Approximate quantiles are specified by rank error: a p99 must lie between the
# exact p97 and p99.99 of its group. (Relative value error is meaningless for
# small groups, where one data point separates p98 from p99.)
RANK_ERROR = 0.02


def exact_percentile_at(platform, engines, query, fraction):
    """DuckDB's exact p99_ttfb for every group of `query`, at another fraction."""
    raw = platform.ontology.model_dump()
    for metric in raw["models"][0]["metrics"]:
        if metric["name"] == "p99_ttfb":
            metric["percentile"] = fraction
    sql = SqlCompiler(Ontology.model_validate(raw)).compile(query, engine="duckdb", table="telemetry")
    return [row["p99_ttfb"] for row in engines["duckdb"].execute(sql).to_pylist()]


@pytest.fixture(scope="module")
def setup(platform, engines, window):
    start, end = window["start"].replace(tzinfo=UTC), window["end"].replace(tzinfo=UTC)
    router = Router.from_platform(platform)
    flushed = end - timedelta(hours=6)
    router.placements["telemetry"] = replace(
        router.placements["telemetry"], retention=(end - start) / 2, flushed_through=lambda: flushed
    )
    router.clock = lambda: end
    return router, start, end, flushed


CASES = {
    # name: (query fields, tiers that should run, covers up to the flush instead of the end)
    "recent only": ({"metrics": ["event_count", "p99_ttfb"], "dimensions": ["isp_provider"], "recent": True}, {"realtime"}, False),
    "merged totals": (
        {"metrics": ["event_count", "rebuffer_incidence_rate", "avg_bitrate_kbps"], "dimensions": ["isp_provider"]},
        {"historical", "realtime", "merge"},
        False,
    ),
    "hourly with percentile": (
        {"metrics": ["p99_ttfb", "unique_sessions"], "grain": "hour"},
        {"historical", "realtime", "merge"},
        False,
    ),
    "total percentile": ({"metrics": ["p99_ttfb", "event_count"], "dimensions": ["is_mobile_device"]}, {"historical"}, True),
}


@pytest.mark.parametrize("name", CASES)
def test_routed_answer_matches_all_events(setup, platform, engines, name):
    router, start, end, flushed = setup
    fields, tiers, truncated = CASES[name]
    fields = dict(fields)
    if fields.pop("recent", False):
        start = end - timedelta(hours=3)
    q = MetricQuery.model_validate({**fields, "start": start, "end": end})

    result = router.run(q)

    assert result.sql.keys() == tiers
    assert result.end == (flushed if truncated else end)
    covered = q.model_copy(update={"start": result.start, "end": result.end})
    expected = engines["duckdb"].execute(router.compiler.compile(covered, engine="duckdb", table="telemetry")).to_pylist()
    actual = result.table.to_pylist()
    assert len(actual) == len(expected)
    if "p99_ttfb" in q.metrics:
        low = exact_percentile_at(platform, engines, covered, 0.99 - RANK_ERROR)
        high = exact_percentile_at(platform, engines, covered, 0.9999)
        for row, lo, hi in zip(actual, low, high):
            assert lo <= row["p99_ttfb"] <= hi, (lo, row["p99_ttfb"], hi)
    for actual_row, expected_row in zip(actual, expected):
        for column, value in expected_row.items():
            if column == "p99_ttfb":
                continue
            elif isinstance(value, float):
                assert actual_row[column] == pytest.approx(value, rel=1e-9), column
            else:
                assert actual_row[column] == value, column
