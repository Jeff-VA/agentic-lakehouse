import os
from pathlib import Path

import duckdb
import pytest
from pydantic import ValidationError

from core.compiler import MetricQuery, QueryError, SqlCompiler
from core.ontology import Ontology, load_ontology

ROOT = Path(__file__).parents[1]
GOLDEN = Path(__file__).parent / "golden"

# Covers every aggregation, a filtered metric, a ratio and a computed dimension.
# The second model only exists to test cross-model queries.
ONTOLOGY = Ontology.model_validate(
    {
        "version": "1",
        "models": [
            {
                "name": "playback",
                "description": "One row per playback event.",
                "source": "playback",
                "time_dimension": "ts",
                "dimensions": [
                    {"name": "ts", "type": "timestamp"},
                    {"name": "isp", "type": "string"},
                    {"name": "is_mobile", "type": "boolean", "sql": "device IN ('ios', 'android')"},
                ],
                "metrics": [
                    {"name": "plays", "description": "Playback events.", "agg": "count"},
                    {
                        "name": "slow_plays",
                        "description": "Plays slower than 100 ms.",
                        "agg": "count",
                        "filters": ["latency_ms > 100"],
                    },
                    {
                        "name": "slow_rate",
                        "type": "ratio",
                        "description": "Share of plays slower than 100 ms.",
                        "numerator": "slow_plays",
                        "denominator": "plays",
                    },
                    {"name": "viewers", "description": "Distinct viewers.", "agg": "count_distinct", "sql": "user_id"},
                    {"name": "total_latency_ms", "description": "d", "agg": "sum", "sql": "latency_ms"},
                    {"name": "avg_latency_ms", "description": "d", "agg": "avg", "sql": "latency_ms"},
                    {"name": "min_latency_ms", "description": "d", "agg": "min", "sql": "latency_ms"},
                    {"name": "max_latency_ms", "description": "d", "agg": "max", "sql": "latency_ms"},
                    {
                        "name": "median_latency_ms",
                        "description": "d",
                        "agg": "percentile",
                        "percentile": 0.5,
                        "sql": "latency_ms",
                    },
                ],
            },
            {
                "name": "billing",
                "description": "One row per invoice.",
                "source": "invoices",
                "time_dimension": "billed_at",
                "dimensions": [{"name": "billed_at", "type": "timestamp"}],
                "metrics": [{"name": "revenue", "description": "Billed revenue.", "agg": "sum", "sql": "amount"}],
            },
        ],
    }
)

START, END = "2026-01-01T00:00:00", "2026-01-01T02:00:00"
ROWS = [
    # ts,                   isp,       device,    user_id, latency_ms
    ("2025-12-31 23:59:59", "verizon", "ios", "u9", 999),  # before START: excluded
    ("2026-01-01 00:10:00", "comcast", "ios", "u1", 50),
    ("2026-01-01 00:20:00", "comcast", "web", "u2", 150),
    ("2026-01-01 00:50:00", "verizon", "android", "u1", 200),
    ("2026-01-01 01:05:00", "comcast", "ios", "u3", 100),
    ("2026-01-01 01:30:00", "verizon", "web", "u2", 300),
    ("2026-01-01 02:00:00", "comcast", "tv", "u9", 999),  # at END: excluded, the range is half-open
]


@pytest.fixture(scope="module")
def run():
    """Compile a query for DuckDB, run it against ROWS and return the result rows."""
    con = duckdb.connect()
    con.execute("CREATE TABLE playback (ts TIMESTAMP, isp VARCHAR, device VARCHAR, user_id VARCHAR, latency_ms INTEGER)")
    con.executemany("INSERT INTO playback VALUES (?, ?, ?, ?, ?)", ROWS)
    compiler = SqlCompiler(ONTOLOGY)

    def run(**fields):
        query = MetricQuery.model_validate({"start": START, "end": END, **fields})
        return con.execute(compiler.compile(query, engine="duckdb", table="playback")).fetchall()

    return run


def compile_query(**fields):
    query = MetricQuery.model_validate({"start": START, "end": END, **fields})
    return SqlCompiler(ONTOLOGY).compile(query, engine="duckdb", table="playback")


def test_every_aggregation_by_dimension(run):
    rows = run(
        metrics=[
            "plays",
            "slow_plays",
            "slow_rate",
            "viewers",
            "total_latency_ms",
            "avg_latency_ms",
            "min_latency_ms",
            "max_latency_ms",
            "median_latency_ms",
        ],
        dimensions=["isp"],
    )
    assert rows == [
        ("comcast", 3, 1, pytest.approx(1 / 3), 3, 300, 100.0, 50, 150, 100.0),
        ("verizon", 2, 2, 1.0, 2, 500, 250.0, 200, 300, 250.0),
    ]


def test_grain_buckets_the_time_dimension(run):
    rows = run(metrics=["plays", "slow_plays"], grain="hour")
    assert [(ts.isoformat(), plays, slow) for ts, plays, slow in rows] == [
        ("2026-01-01T00:00:00", 3, 2),
        ("2026-01-01T01:00:00", 2, 1),
    ]


def test_computed_dimension(run):
    assert run(metrics=["plays"], dimensions=["is_mobile"]) == [(False, 2), (True, 3)]


@pytest.mark.parametrize(
    ("query_filter", "plays"),
    [
        ({"dimension": "isp", "op": "=", "value": "comcast"}, 3),
        ({"dimension": "isp", "op": "!=", "value": "comcast"}, 2),
        ({"dimension": "isp", "op": ">", "value": "d"}, 2),
        ({"dimension": "isp", "op": "in", "value": ["comcast", "att"]}, 3),
        ({"dimension": "isp", "op": "not_in", "value": ["comcast"]}, 2),
        ({"dimension": "is_mobile", "op": "=", "value": True}, 3),
        # Values are rendered by sqlglot as escaped literals, so this is just a string that matches nothing.
        ({"dimension": "isp", "op": "=", "value": "x' OR '1'='1"}, 0),
    ],
)
def test_filters(run, query_filter, plays):
    assert run(metrics=["plays"], filters=[query_filter]) == [(plays,)]


def test_aware_datetimes_are_converted_to_utc(run):
    # The same two hours as START/END, written in UTC-5.
    assert run(metrics=["plays"], start="2025-12-31T19:00:00-05:00", end="2025-12-31T21:00:00-05:00") == [(5,)]


def test_order_by_and_limit(run):
    rows = run(metrics=["plays"], dimensions=["isp"], order_by=[{"field": "plays", "descending": True}], limit=1)
    assert rows == [("comcast", 3)]


def test_compound_dimension_is_parenthesized_in_filters():
    sql = compile_query(metrics=["plays"], filters=[{"dimension": "is_mobile", "op": "in", "value": [True]}])
    assert "(device IN ('ios', 'android')) IN (TRUE)" in sql


@pytest.mark.parametrize(
    ("fields", "match"),
    [
        ({"metrics": ["playz"]}, "unknown metrics: playz. Available: avg_latency_ms"),
        ({"metrics": ["plays", "revenue"]}, r"different models \(billing, playback\)"),
        ({"metrics": ["plays"], "dimensions": ["country"]}, "unknown dimension 'country'.*Available: isp, is_mobile"),
        ({"metrics": ["plays"], "dimensions": ["ts"]}, "set `grain` to group by time"),
        ({"metrics": ["plays"], "filters": [{"dimension": "ts", "op": ">", "value": "x"}]}, "use start/end"),
        ({"metrics": ["plays"], "order_by": [{"field": "isp"}]}, "cannot order by 'isp'"),
    ],
)
def test_queries_that_do_not_match_the_ontology_are_rejected(fields, match):
    with pytest.raises(QueryError, match=match):
        compile_query(**fields)


@pytest.mark.parametrize(
    ("fields", "match"),
    [
        ({"metrics": ["plays"], "start": END, "end": START}, "start must be before end"),
        ({"metrics": ["plays", "plays"]}, "metrics requested more than once: plays"),
        ({"metrics": ["plays"], "filters": [{"dimension": "isp", "op": "in", "value": "comcast"}]}, "takes a list"),
        ({"metrics": ["plays"], "filters": [{"dimension": "isp", "op": "=", "value": ["a"]}]}, "takes a single value"),
        ({"metrics": ["plays"], "filters": [{"dimension": "isp", "op": "in", "value": []}]}, "at least one value"),
        ({"metrics": ["plays"], "limit": 0}, "greater than 0"),
    ],
)
def test_malformed_queries_are_rejected(fields, match):
    with pytest.raises(ValidationError, match=match):
        MetricQuery.model_validate({"start": START, "end": END, **fields})


# The sample config, compiled for every engine. The golden files show exactly
# what each engine receives; regenerate them with UPDATE_GOLDEN=1 after an
# intended change and review the diff.
SAMPLE_TABLES = {"duckdb": "telemetry", "trino": "iceberg.raw.telemetry", "pinot": "telemetry"}
SAMPLE_QUERY = MetricQuery.model_validate(
    {
        "metrics": ["p99_ttfb", "rebuffer_incidence_rate", "unique_sessions", "avg_bitrate_kbps"],
        "dimensions": ["isp_provider", "is_mobile_device"],
        "start": "2026-10-01T00:00:00Z",
        "end": "2026-10-02T00:00:00Z",
        "grain": "hour",
        "filters": [
            {"dimension": "isp_provider", "op": "in", "value": ["comcast", "verizon"]},
            {"dimension": "is_mobile_device", "op": "=", "value": True},
        ],
    }
)


@pytest.mark.parametrize("engine", SAMPLE_TABLES)
def test_sample_query_matches_golden_sql(engine):
    sql = SqlCompiler(load_ontology(ROOT / "config" / "ontology.yaml")).compile(
        SAMPLE_QUERY, engine=engine, table=SAMPLE_TABLES[engine]
    )
    golden = GOLDEN / f"sample_query.{engine}.sql"
    if os.environ.get("UPDATE_GOLDEN"):
        golden.write_text(sql + "\n")
    assert sql + "\n" == golden.read_text()


def test_sample_query_runs_on_duckdb():
    con = duckdb.connect()
    con.execute(
        "CREATE TABLE telemetry (event_ts TIMESTAMP, session_id VARCHAR, client_isp VARCHAR, client_device VARCHAR,"
        " request_latency_ms INTEGER, stream_start_event BOOLEAN, buffer_duration_ms INTEGER, bitrate_kbps INTEGER)"
    )
    con.execute((GOLDEN / "sample_query.duckdb.sql").read_text())
