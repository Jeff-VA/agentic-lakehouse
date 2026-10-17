"""Metrics from models over different datasets in one query: a drill-across. Each dataset's metrics are
asked for on their own and routed across its tiers like any query, and the answers are joined on the
dimensions the datasets share (and the time buckets)."""

from datetime import timedelta

import duckdb
import pytest
import yaml
from fastapi.testclient import TestClient

from agentic_lakehouse.api_gateway import Gateway, create_app
from agentic_lakehouse.compiler import QueryError, SqlCompiler
from agentic_lakehouse.engines import DuckDBEngine
from agentic_lakehouse.ontology import Ontology
from agentic_lakehouse.router import Placement, Router, Tier
from agentic_lakehouse.views import ViewError, parse_view
from test_compiler import ONTOLOGY
from test_router import EVERYTHING, FLUSHED, NOW, REALTIME_FROM, RETENTION, ROWS, T0, duckdb_with, query

# Support tickets, a second dataset sharing `isp` with playback: comcast, att and tmobile open them
# (verizon's users don't; tmobile has no plays), every 20 minutes over the same six hours.
TICKETS = [(T0 + timedelta(minutes=20 * i), ["comcast", "att", "tmobile"][i % 3], f"u{i % 5}", (i * 7) % 60 + 1) for i in range(18)]
_spec = ONTOLOGY.model_dump(exclude_defaults=True)
_spec["models"].append(
    {
        "name": "support",
        "description": "One row per support ticket.",
        "source": "tickets",
        "time_dimension": "ts",
        "dimensions": [
            {"name": "ts", "type": "timestamp"},
            {"name": "isp", "type": "string"},
            {"name": "queue", "type": "string", "sql": "CASE WHEN wait_min > 30 THEN 'slow' ELSE 'fast' END"},
            {"name": "is_mobile", "type": "string", "sql": "'unknown'"},  # a boolean in playback
        ],
        "metrics": [
            {"name": "tickets", "description": "Tickets opened.", "agg": "count"},
            {"name": "ticket_users", "description": "Users who opened one.", "agg": "count_distinct", "sql": "user_id"},
            {"name": "avg_wait_min", "description": "Average wait for an answer.", "agg": "avg", "sql": "wait_min"},
        ],
    }
)
COMPILER = SqlCompiler(Ontology.model_validate(_spec))
COUNTS = {"plays", "viewers", "tickets", "ticket_users"}  # what a dataset without the group answers 0 for


def tickets_with(rows) -> DuckDBEngine:
    con = duckdb.connect()
    con.execute("CREATE TABLE tickets (ts TIMESTAMP, isp VARCHAR, user_id VARCHAR, wait_min INTEGER)")
    if rows:
        con.executemany("INSERT INTO tickets VALUES (?, ?, ?, ?)", rows)
    return DuckDBEngine(con)


ALL_TICKETS = tickets_with(TICKETS)


def router(flushed=FLUSHED) -> Router:
    def placement(rows, engine_with, table):
        return Placement(
            realtime=Tier(engine_with([r for r in rows if r[0] >= REALTIME_FROM]), table),
            historical=Tier(engine_with([r for r in rows if r[0] < FLUSHED]), table),
            retention=RETENTION,
            flushed_through=lambda: flushed,
        )

    placements = {"playback": placement(ROWS, duckdb_with, "playback"), "tickets": placement(TICKETS, tickets_with, "tickets")}
    return Router(COMPILER, placements, clock=lambda: NOW)


def joined(q, start=None, end=None) -> list[dict]:
    """What the drill-across should answer: each dataset's metrics in one query over all its events,
    outer-joined on the query's dimensions and time buckets, then ordered and limited."""
    q = q.model_copy(update={"start": start or q.start, "end": end or q.end})
    keys = (["ts"] if q.grain else []) + q.dimensions
    parts = []
    for engine, table, model in ((EVERYTHING, "playback", "playback"), (ALL_TICKETS, "tickets", "support")):
        metrics = [m for m in q.metrics if COMPILER._model_of[m].name == model]
        if metrics:
            part = q.model_copy(update={"metrics": metrics, "order_by": [], "limit": None})
            parts.append((metrics, {tuple(r[k] for k in keys): r for r in engine.execute(COMPILER.compile(part, engine="duckdb", table=table)).to_pylist()}))
    rows = []
    for group in sorted({g for _, found in parts for g in found}, key=lambda g: tuple((v is None, v) for v in g)):
        row = dict(zip(keys, group))
        for metrics, found in parts:
            row |= {m: found[group][m] if group in found else (0 if m in COUNTS else None) for m in metrics}
        rows.append(row)
    for key in reversed(q.order_by):
        rows.sort(key=lambda r: (r[key.field] is None, r[key.field]), reverse=key.descending)
    return rows[: q.limit] if q.limit else rows


def assert_same(actual: list[dict], expected: list[dict]):
    assert [list(r) for r in actual] == [list(r) for r in expected]
    assert actual == [{k: pytest.approx(v) if isinstance(v, float) else v for k, v in r.items()} for r in expected]


def test_metrics_from_two_datasets_are_joined_on_the_dimension_they_share():
    q = query(0, 6, metrics=["plays", "viewers", "avg_latency_ms", "tickets", "ticket_users", "avg_wait_min"], dimensions=["isp"])
    result = router().run(q)
    assert not result.notes and (result.start, result.end) == (q.start, q.end)
    assert {label.split(": ")[0] for label in result.sql} == {"playback", "tickets", "join"}
    assert_same(result.table.to_pylist(), joined(q))
    by_isp = {r["isp"]: r for r in result.table.to_pylist()}
    assert (by_isp["tmobile"]["plays"], by_isp["tmobile"]["avg_latency_ms"]) == (0, None)  # tickets, no plays
    assert (by_isp["verizon"]["tickets"], by_isp["verizon"]["avg_wait_min"]) == (0, None)  # plays, no tickets


@pytest.mark.parametrize(
    "fields",
    [
        {"grain": "hour"},
        {"grain": "hour", "order_by": [{"field": "tickets", "descending": True}, {"field": "ts"}], "limit": 3},
        {},  # no shared dimension: each dataset's totals, side by side
        {"dimensions": ["isp"], "filters": [{"dimension": "isp", "op": "in", "value": ["comcast", "tmobile"]}]},
    ],
)
def test_drill_across_by_time_with_ordering_limits_and_filters(fields):
    q = query(0, 6, metrics=["plays", "slow_rate", "tickets"], **fields)
    assert_same(router().run(q).table.to_pylist(), joined(q))


def test_every_dataset_answers_for_the_same_range():
    """A monthly percentile stops at the last flush, so the tickets are counted up to it too: one
    row mustn't mix two ranges. The note says why."""
    q = query(0, 6, metrics=["median_latency_ms", "tickets"], grain="month")
    result = router().run(q)
    assert result.end == FLUSHED.replace(tzinfo=result.end.tzinfo)
    assert any("median_latency_ms can't be combined" in note for note in result.notes)
    assert_same(result.table.to_pylist(), joined(q, end=result.end))


@pytest.mark.parametrize(
    ("fields", "message"),
    [
        ({"dimensions": ["queue"]}, r"'queue' isn't a dimension of every dataset's models: playback hasn't one"),
        ({"dimensions": ["is_mobile"]}, r"'is_mobile' is a boolean in playback but a string in support"),
        ({"filters": [{"dimension": "queue", "op": "=", "value": "slow"}]}, r"'queue' isn't a dimension of every dataset's models"),
    ],
)
def test_a_dimension_the_datasets_dont_share_is_refused(fields, message):
    with pytest.raises(QueryError, match=message):
        router().run(query(0, 6, metrics=["plays", "tickets"], **fields))


def test_time_buckets_across_datasets_that_name_time_differently_are_refused():
    with pytest.raises(QueryError, match=r"by time: playback's is ts, billing's billed_at"):
        router().run(query(0, 6, metrics=["plays", "revenue"], grain="hour"))


def test_a_query_of_one_dataset_routes_as_before():
    q = query(0, 6, metrics=["plays", "viewers"], dimensions=["isp"])
    assert set(router().run(q).sql) == {"historical", "realtime", "realtime_keys", "merge"}


def test_the_api_answers_a_drill_across_with_its_time_column():
    with TestClient(create_app(Gateway(router()))) as client:
        body = client.post(
            "/query",
            json={"metrics": ["plays", "tickets"], "dimensions": ["isp"], "grain": "hour", "start": "2026-01-01T00:00:00", "end": "2026-01-01T06:00:00"},
        ).json()
    assert [(c["name"], c["role"]) for c in body["columns"]] == [("ts", "time"), ("isp", "dimension"), ("plays", "metric"), ("tickets", "metric")]
    assert len(body["rows"]) == len(joined(query(0, 6, metrics=["plays", "tickets"], dimensions=["isp"], grain="hour")))


# ---------------------------------------------------------------- FROM metrics: SQL across datasets

TABLES = {"raw.playback": {"ts": "TIMESTAMP", "isp": "VARCHAR", "device": "VARCHAR", "user_id": "VARCHAR", "latency_ms": "INTEGER"}}


def sql_view(sql: str, ontology=COMPILER.ontology):
    header = yaml.safe_dump({"description": "A test view.", "params": {"hours": {"type": "int", "default": 6}}})
    return parse_view("test", f"/*\n{header}*/\n{sql}", ontology, TABLES)


def test_a_view_from_metrics_asks_for_the_metrics_of_every_dataset():
    """FROM metrics names no model: it's the whole semantic layer, so the view's metrics may come from
    any dataset, and its reads say which models answer."""
    v = sql_view("SELECT isp, plays, viewers, tickets FROM metrics WHERE ts >= now() - INTERVAL :hours HOUR ORDER BY tickets DESC, isp")
    assert v.reads == ["playback", "support"]
    assert [(c.name, c.role) for c in v.columns] == [("isp", "dimension"), ("plays", "metric"), ("viewers", "metric"), ("tickets", "metric")]
    q = v.source.query({"hours": 6}, NOW)
    assert_same(router().run(q).table.to_pylist(), joined(q))


def test_a_view_from_metrics_buckets_every_dataset_by_time():
    v = sql_view("SELECT date_trunc('hour', ts) AS hour, plays, tickets FROM metrics WHERE ts >= now() - INTERVAL :hours HOUR")
    q = v.source.query({"hours": 6}, NOW)
    assert (q.grain, [(c.name, c.role) for c in v.columns][0]) == ("hour", ("hour", "time"))
    assert len(router().run(q).table) == len(joined(q))


def test_a_view_from_metrics_of_one_dataset_reads_it_like_a_view_from_its_model():
    from_metrics = sql_view("SELECT isp, plays, slow_rate FROM metrics WHERE ts >= now() - INTERVAL :hours HOUR")
    from_model = sql_view("SELECT isp, plays, slow_rate FROM playback WHERE ts >= now() - INTERVAL :hours HOUR")
    assert from_metrics.reads == ["playback"]
    assert from_metrics.source.query({"hours": 6}, NOW) == from_model.source.query({"hours": 6}, NOW)


def test_a_view_from_metrics_refuses_a_dimension_the_datasets_dont_share():
    with pytest.raises(ViewError, match=r"'queue' isn't a dimension of every dataset's models: playback hasn't one"):
        sql_view("SELECT queue, plays, tickets FROM metrics WHERE ts >= now() - INTERVAL :hours HOUR")


def test_a_model_named_metrics_is_still_read_by_from_metrics():
    """Additive: a project with a model called metrics keeps reading it."""
    spec = ONTOLOGY.model_dump(exclude_defaults=True)
    spec["models"][0]["name"] = "metrics"
    v = sql_view("SELECT isp, plays FROM metrics WHERE ts >= now() - INTERVAL :hours HOUR", Ontology.model_validate(spec))
    assert v.reads == ["metrics"]


def test_sql_from_metrics_runs_through_the_gateway():
    with TestClient(create_app(Gateway(router(), tables=TABLES))) as client:
        body = client.get("/sql", params={"q": "SELECT isp, plays, tickets FROM metrics WHERE ts >= now() - INTERVAL '6' HOUR"}).json()
    assert "error" not in body, body
    assert {r["isp"] for r in body["rows"]} == {"att", "comcast", "tmobile", "verizon"}
