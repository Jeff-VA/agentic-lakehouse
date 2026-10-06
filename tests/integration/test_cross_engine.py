"""Same semantic query, same answer, on every engine.

Each query runs on DuckDB, Trino and Pinot over the telemetry batch that
`python run.py seed` loaded into both tiers, and the results must match. DuckDB
reads the Iceberg table's files directly, so all three see the same events.
"""

import pytest

from core.compiler import MetricQuery, SqlCompiler
from core.ontology import AggregateMetric

pytestmark = pytest.mark.integration

# Trino's approx_percentile and Pinot's t-digest against DuckDB's exact quantile_cont.
PERCENTILE_TOLERANCE = 0.05

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
    percentiles = {
        m.name for m in platform.ontology.models[0].metrics if isinstance(m, AggregateMetric) and m.agg == "percentile"
    }
    expected_rows, actual_rows = expected.to_pylist(), actual.to_pylist()
    assert len(actual_rows) == len(expected_rows)
    for expected_row, actual_row in zip(expected_rows, actual_rows):
        for column, value in expected_row.items():
            if column in percentiles:
                assert actual_row[column] == pytest.approx(value, rel=PERCENTILE_TOLERANCE), column
            elif isinstance(value, float):
                assert actual_row[column] == pytest.approx(value, rel=1e-9), column
            else:
                assert actual_row[column] == value, column
