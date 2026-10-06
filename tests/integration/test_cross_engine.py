"""Same semantic query, same answer, on every engine.

Each query runs on DuckDB, Trino and Pinot over the telemetry batch that
`python run.py seed` loaded into both tiers, and the results must match. DuckDB
reads the Iceberg table's files directly, so all three see the same events.
"""

import pytest
from compare import assert_same_result

from core.compiler import MetricQuery, SqlCompiler
from core.ontology import Ontology

pytestmark = pytest.mark.integration

QUERIES = {
    "every metric by isp": {
        "metrics": [
            "event_count",
            "rebuffer_event_count",
            "rebuffer_incidence_rate",
            "unique_sessions",
            "avg_bitrate_kbps",
            "p99_ttfb",
        ],
        "dimensions": ["isp_provider"],
    },
    "hourly counts": {"metrics": ["event_count", "rebuffer_event_count"], "grain": "hour"},
    "daily rates by device class": {
        "metrics": ["rebuffer_incidence_rate", "unique_sessions"],
        "dimensions": ["is_mobile_device"],
        "grain": "day",
    },
    "filtered slice": {
        "metrics": ["unique_sessions", "avg_bitrate_kbps"],
        "dimensions": ["isp_provider", "is_mobile_device"],
        "filters": [
            {"dimension": "isp_provider", "op": "not_in", "value": ["frontier"]},
            {"dimension": "is_mobile_device", "op": "=", "value": True},
        ],
    },
    "worst isps first": {
        "metrics": ["rebuffer_incidence_rate"],
        "dimensions": ["isp_provider"],
        "order_by": [{"field": "rebuffer_incidence_rate", "descending": True}],
        "limit": 3,
    },
    "p99 by device class": {"metrics": ["p99_ttfb"], "dimensions": ["is_mobile_device"]},
    "no matching rows, grouped": {
        "metrics": ["event_count"],
        "dimensions": ["isp_provider"],
        "filters": [{"dimension": "isp_provider", "op": "=", "value": "nobody"}],
    },
    "no matching rows, total": {
        "metrics": ["event_count", "avg_bitrate_kbps"],
        "filters": [{"dimension": "isp_provider", "op": "=", "value": "nobody"}],
    },
}


@pytest.mark.parametrize("engine", ["trino", "pinot"])
@pytest.mark.parametrize("name", QUERIES)
def test_engine_agrees_with_duckdb(platform, engines, window, engine, name):
    query = MetricQuery.model_validate({**QUERIES[name], **window})
    compiler = SqlCompiler(platform.ontology)
    expected = engines["duckdb"].execute(compiler.compile(query, engine="duckdb", table="telemetry"))
    actual = engines[engine].execute(
        compiler.compile(query, engine=engine, table=platform.table_for("telemetry", engine))
    )
    assert actual.column_names == expected.column_names
    assert_same_result(platform.ontology, engines["duckdb"], query, expected.to_pylist(), actual.to_pylist())


def test_percentile_check_rejects_a_wrong_percentile(platform, engines, window):
    """The rank check isn't vacuous: the exact p99 passes, a p90 passed off as the p99 fails."""
    query = MetricQuery.model_validate({**QUERIES["p99 by device class"], **window})
    exact = engines["duckdb"].execute(SqlCompiler(platform.ontology).compile(query, engine="duckdb", table="telemetry"))
    raw = platform.ontology.model_dump()
    for metric in raw["models"][0]["metrics"]:
        if metric["name"] == "p99_ttfb":
            metric["percentile"] = 0.9
    p90 = engines["duckdb"].execute(
        SqlCompiler(Ontology.model_validate(raw)).compile(query, engine="duckdb", table="telemetry")
    )

    assert_same_result(platform.ontology, engines["duckdb"], query, exact.to_pylist(), exact.to_pylist())
    with pytest.raises(AssertionError, match="not within 1% of the 99% position"):
        assert_same_result(platform.ontology, engines["duckdb"], query, exact.to_pylist(), p90.to_pylist())
