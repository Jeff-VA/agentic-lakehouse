"""Views served by the gateway: GET /views, GET /views/{name}, and the MCP tools list_views and query_view."""

from datetime import datetime

import duckdb
import pytest
from fastapi.testclient import TestClient
from test_api_gateway import END, START, call_mcp, post_query
from test_router import router
from test_views import TABLES

from agentic_lakehouse.api_gateway import Gateway, TableState, Warehouse, create_app, create_mcp_server
from agentic_lakehouse.engines import DuckDBEngine
from agentic_lakehouse.views import load_views
from test_compiler import ONTOLOGY

VIEWS = {
    "plays_by_isp": """/*
description: Plays and the slow share per ISP over the last `hours`.
params:
  hours: {type: int, default: 6, min: 1, max: 24}
*/
SELECT isp, plays, slow_rate AS slow
FROM playback
WHERE ts >= now() - INTERVAL :hours HOUR
ORDER BY plays DESC, isp
""",
    "hourly": """/*
description: Hourly plays per ISP from the gold table.
params:
  min_plays: {type: int, default: 0}
*/
SELECT ts AS hour, isp, plays, slow_rate
FROM gold.hourly
WHERE plays >= :min_plays
ORDER BY hour, isp
""",
}


def warehouse(exists: bool = True) -> Warehouse:
    """DuckDB standing in for Trino: the gold table, complete through 04:00."""
    con = duckdb.connect()
    con.execute("CREATE SCHEMA gold; CREATE TABLE gold.hourly (ts TIMESTAMP, isp VARCHAR, plays BIGINT, slow_rate DOUBLE)")
    con.executemany(
        "INSERT INTO gold.hourly VALUES (?, ?, ?, ?)",
        [(datetime(2026, 1, 1, h), isp, plays, plays / 10) for h in range(4) for isp, plays in (("att", h), ("comcast", 5 + h))],
    )
    con.execute("INSERT INTO gold.hourly VALUES ('2026-01-01 04:00:00', 'cox', 9, 'NaN')")
    return Warehouse(DuckDBEngine(con), table_state=lambda table: TableState(exists=exists, complete_through=datetime(2026, 1, 1, 4)))


def gateway(tmp_path, **options) -> Gateway:
    for name, text in VIEWS.items():
        (tmp_path / f"{name}.sql").write_text(text)
    views = load_views(tmp_path, ONTOLOGY, TABLES)
    return Gateway(router(), views=views, warehouse=options.pop("warehouse", warehouse()), **options)


@pytest.fixture
def client(tmp_path):
    with TestClient(create_app(gateway(tmp_path))) as client:
        yield client


def test_views_are_listed_with_their_parameters_columns_and_sql(client):
    views = {v["name"]: v for v in client.get("/views").json()["views"]}
    assert views.keys() == {"hourly", "plays_by_isp"}
    model_view = views["plays_by_isp"]
    assert (model_view["source"], model_view["reads"]) == ("model", ["playback"])
    assert model_view["params"] == [
        {"name": "hours", "type": "int", "description": "", "required": False, "default": 6, "min": 1, "max": 24, "values": None}
    ]
    plain = {"format": None, "unit": "", "better": None, "grain": None}
    assert model_view["columns"] == [
        {"name": "isp", "type": "string", "label": "Isp", "role": "dimension"} | plain,
        {"name": "plays", "type": "number", "label": "Plays", "role": "metric"} | plain | {"format": "integer"},
        {"name": "slow", "type": "number", "label": "Slow", "role": "metric"} | plain | {"format": "number"},
    ]
    assert model_view["sql"].startswith("SELECT isp, plays, slow_rate AS slow")
    assert (views["hourly"]["source"], views["hourly"]["reads"]) == ("tables", ["gold.hourly"])


def test_a_model_view_answers_like_the_query_it_stands_for(client):
    body = client.get("/views/plays_by_isp").json()
    order = [{"field": "plays", "descending": True}, {"field": "isp"}]
    query = post_query(client, metrics=["plays", "slow_rate"], dimensions=["isp"], order_by=order).json()
    assert body["rows"] == [{"isp": r["isp"], "plays": r["plays"], "slow": r["slow_rate"]} for r in query["rows"]]
    assert body["coverage"] == {"start": START, "end": END}  # the router's clock is 06:00; 6 hours back
    assert (body["view"], body["params"], body["data_through"], body["truncated"]) == ("plays_by_isp", {"hours": 6}, None, False)
    assert client.get("/views/plays_by_isp", params={"hours": 1}).json()["coverage"]["start"] == "2026-01-01T05:00:00Z"


def test_a_table_view_runs_on_the_warehouse(client):
    body = client.get("/views/hourly", params={"min_plays": 7, "include_sql": True}).json()
    assert body["rows"] == [
        {"hour": "2026-01-01T02:00:00Z", "isp": "comcast", "plays": 7, "slow_rate": 0.7},
        {"hour": "2026-01-01T03:00:00Z", "isp": "comcast", "plays": 8, "slow_rate": 0.8},
        {"hour": "2026-01-01T04:00:00Z", "isp": "cox", "plays": 9, "slow_rate": None},  # NaN can't be JSON
    ]
    assert (body["coverage"], body["data_through"]) == (None, "2026-01-01T04:00:00Z")
    assert "WHERE plays >= 7" in body["sql"]["warehouse"]


@pytest.mark.parametrize(
    ("path", "status", "detail"),
    [
        ("/views/nope", 404, "no view named 'nope'"),
        ("/views/plays_by_isp?hours=abc", 400, "parameter 'hours' must be a whole number, not 'abc'"),
        ("/views/plays_by_isp?hours=48", 400, "parameter 'hours' must be at most 24, not 48"),
        ("/views/plays_by_isp?days=2", 400, "view 'plays_by_isp' has no parameter 'days'; it takes hours"),
    ],
)
def test_bad_view_requests_get_client_errors(client, path, status, detail):
    response = client.get(path)
    assert (response.status_code, response.json()) == (status, {"detail": detail})


def test_view_results_are_capped(tmp_path):
    with TestClient(create_app(gateway(tmp_path, max_rows=2))) as client:
        for name in ("plays_by_isp", "hourly"):
            body = client.get(f"/views/{name}").json()
            assert (len(body["rows"]), body["truncated"]) == (2, True)
            assert "Only the first 2 rows are returned; narrow the view with its parameters" in body["notes"][-1]


def test_a_table_view_over_a_table_not_created_yet_has_no_rows(tmp_path):
    with TestClient(create_app(gateway(tmp_path, warehouse=warehouse(exists=False)))) as client:
        body = client.get("/views/hourly").json()
    assert (body["rows"], body["notes"]) == ([], ["gold.hourly doesn't exist yet, so there are no rows"])


def test_mcp_lists_and_runs_views_like_rest(tmp_path, client):
    server = create_mcp_server(gateway(tmp_path))
    listed = call_mcp(server, "call_tool", "list_views", {}).structured_content
    assert listed == client.get("/views").json()
    result = call_mcp(server, "call_tool", "query_view", {"name": "hourly", "params": {"min_plays": 7}})
    assert result.is_error is False
    assert result.structured_content["rows"] == client.get("/views/hourly?min_plays=7").json()["rows"]


@pytest.mark.parametrize(
    ("arguments", "message"),
    [
        ({"name": "nope"}, "no view named 'nope'. Views: hourly, plays_by_isp"),
        ({"name": "plays_by_isp", "params": {"hours": 0}}, "parameter 'hours' must be at least 1, not 0"),
    ],
)
def test_mcp_view_errors_explain_what_to_fix(tmp_path, arguments, message):
    result = call_mcp(create_mcp_server(gateway(tmp_path)), "call_tool", "query_view", arguments)
    assert result.is_error is True
    assert message in result.content[0].text
