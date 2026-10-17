"""The router on the real tiers: answers that span Pinot and Trino must match
DuckDB reading every seeded event.

Both tiers hold the whole seeded batch, so the placement here pretends Pinot
keeps only its second half and Iceberg was flushed 6 hours before the end. That
makes every routing path run: one tier, bucket-aligned split, partial
aggregate merge, the merge of percentiles and distinct counts by the real-time
tier's values, and the fallback past the router's limit on those values.
"""

from dataclasses import replace
from datetime import UTC, timedelta

import pytest
from compare import assert_same_result

from agentic_lakehouse.compiler import MetricQuery
from agentic_lakehouse.router import Router

pytestmark = pytest.mark.integration


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
    "total percentile": (  # Pinot's half here is 6 hours, so the limit is lifted
        {"metrics": ["p99_ttfb", "event_count"], "dimensions": ["is_mobile_device"], "max_realtime_values": 10**7},
        {"historical", "realtime", "realtime_values", "merge"},
        False,
    ),
    "total percentile, too many new values": (
        {"metrics": ["p99_ttfb", "event_count"], "dimensions": ["is_mobile_device"], "max_realtime_values": 0},
        {"historical"},
        True,
    ),
}


@pytest.mark.parametrize("name", CASES)
def test_routed_answer_matches_all_events(setup, platform, engines, name):
    router, start, end, flushed = setup
    fields, tiers, truncated = CASES[name]
    fields = dict(fields)
    if fields.pop("recent", False):
        start = end - timedelta(hours=3)
    limit = fields.pop("max_realtime_values", router.max_realtime_values)
    q = MetricQuery.model_validate({**fields, "start": start, "end": end})

    default, router.max_realtime_values = router.max_realtime_values, limit
    try:
        result = router.run(q)
    finally:
        router.max_realtime_values = default

    assert result.sql.keys() == tiers
    assert result.end == (flushed if truncated else end)
    covered = q.model_copy(update={"start": result.start, "end": result.end})
    expected = engines["duckdb"].execute(router.compiler.compile(covered, engine="duckdb", table="telemetry"))
    assert_same_result(platform.ontology, engines["duckdb"], covered, expected.to_pylist(), result.table.to_pylist())
