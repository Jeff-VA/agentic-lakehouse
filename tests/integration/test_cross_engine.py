"""Same semantic query, same answer, on every engine.

Each query runs on DuckDB, Trino and Pinot over the telemetry batch that
`python run.py seed` loaded into both tiers, and the results must match. DuckDB
reads the Iceberg table's files directly, so all three see the same events.
"""

import time
from datetime import timedelta

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


@pytest.fixture(scope="module")
def window(platform, engines):
    """The hour-aligned range holding the seeded batch, once Pinot has ingested all of it."""
    trino_table = platform.table_for("telemetry", "trino")
    [stats] = engines["trino"].execute(
        f"SELECT count(*) AS n, min(event_ts) AS first, max(event_ts) AS last FROM {trino_table}"
    ).to_pylist()
    if not stats["n"]:
        pytest.skip("the Iceberg table is empty; run: python run.py seed")
    pinot_table = platform.table_for("telemetry", "pinot")
    deadline = time.monotonic() + 30  # Pinot may still be consuming from Kafka
    while (pinot_rows := engines["pinot"].execute(f"SELECT COUNT(*) AS n FROM {pinot_table}")["n"][0].as_py()) != stats["n"]:
        if time.monotonic() > deadline:
            pytest.fail(
                f"Pinot holds {pinot_rows} events but Iceberg holds {stats['n']}; "
                "reload both: python run.py reset --yes && python run.py seed"
            )
        time.sleep(1)
    start = stats["first"].replace(minute=0, second=0, microsecond=0)
    return {"start": start, "end": stats["last"].replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)}


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
