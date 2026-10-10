import socket
import sys
from datetime import datetime
import threading
import time
from dataclasses import replace
from pathlib import Path

import anyio
import pyarrow as pa
import pytest
import uvicorn
from fastapi.testclient import TestClient
from mcp import Client, StdioServerParameters
from test_router import FLUSHED, router

from core.api_gateway import Gateway, create_app, create_mcp_server
from core.config import load_platform
from core.engines import EngineError
from core.rollups import RollupStatus
from core.router import Tier

# The DuckDB stand-in tiers from test_router: six hours of events, the historical
# tier flushed through 04:00, the real-time tier holding 02:00 onwards.
START, END = "2026-01-01T00:00:00Z", "2026-01-01T06:00:00Z"


@pytest.fixture
def client():
    with TestClient(create_app(Gateway(router()))) as client:
        yield client


def post_query(client, include_sql=False, **fields):
    return client.post("/query", params={"include_sql": include_sql}, json={"start": START, "end": END, **fields})


def test_catalog_describes_queryable_models(client):
    [model] = client.get("/catalog").json()["models"]  # "billing" is left out: no tier holds its data
    metrics = {m["name"]: m for m in model["metrics"]}
    assert model["time_dimension"] == "ts"
    assert model["grains"] == ["minute", "hour", "day", "week", "month"]
    assert [d["name"] for d in model["dimensions"]] == ["isp", "is_mobile"]  # time is queried via grain
    assert metrics["slow_plays"]["calculation"] == "count of playback events where latency_ms > 100"
    assert metrics["slow_rate"]["calculation"] == "ratio: slow_plays / plays"
    assert metrics["median_latency_ms"]["calculation"].startswith("0.5 quantile of latency_ms")
    assert model["historical_through"] == "2026-01-01T04:00:00Z"
    assert model["realtime_days"] == pytest.approx(4 / 24)


def test_query_returns_rows_columns_and_coverage(client):
    response = post_query(client, metrics=["plays", "slow_rate"], dimensions=["isp"], grain="hour", limit=2)
    assert response.status_code == 200
    body = response.json()
    assert body["columns"] == [
        {"name": "ts", "role": "time"},
        {"name": "isp", "role": "dimension"},
        {"name": "plays", "role": "metric"},
        {"name": "slow_rate", "role": "metric"},
    ]
    assert body["rows"][0]["ts"] == "2026-01-01T00:00:00Z"  # engine timestamps are returned as UTC
    assert [row["isp"] for row in body["rows"]] == ["att", "comcast"]
    assert body["coverage"] == {"start": START, "end": END}
    assert (body["notes"], body["truncated"], body["sql"]) == ([], False, None)


def test_include_sql_shows_what_each_tier_ran(client):
    sql = post_query(client, include_sql=True, metrics=["plays"]).json()["sql"]
    assert sql.keys() == {"historical", "realtime", "merge"}


def test_partial_coverage_is_explained(client):
    body = post_query(client, metrics=["median_latency_ms"]).json()
    assert body["coverage"]["end"] == FLUSHED.isoformat() + "Z"
    assert "can't be combined across the real-time and historical tiers" in body["notes"][0]


def test_results_beyond_max_rows_are_truncated_and_say_so():
    with TestClient(create_app(Gateway(router(), max_rows=3))) as client:
        body = post_query(client, metrics=["plays"], grain="hour").json()  # 6 hourly rows
        assert (len(body["rows"]), body["truncated"]) == (3, True)
        assert "Only the first 3 rows are returned" in body["notes"][-1]
        assert post_query(client, metrics=["plays"], grain="hour", limit=3).json()["truncated"] is False


@pytest.mark.parametrize(
    ("fields", "status", "detail"),
    [
        ({"metrics": ["playz"]}, 400, "unknown metrics: playz. Available:"),
        ({"metrics": ["revenue"]}, 400, "model 'billing' reads dataset 'invoices', which no tier holds"),
        ({"metrics": ["plays"], "end": "2025-12-31T00:00:00Z"}, 422, "start must be before end"),
    ],
)
def test_bad_queries_get_client_errors(client, fields, status, detail):
    response = post_query(client, **fields)
    assert response.status_code == status
    assert detail in str(response.json()["detail"])


def test_engine_failure_is_a_bad_gateway():
    class BrokenEngine:
        name = "duckdb"

        def execute(self, sql):
            raise EngineError("pinot: broker unavailable")

    broken = router()
    broken.placements["playback"] = replace(broken.placements["playback"], realtime=Tier(BrokenEngine(), "playback"))
    with TestClient(create_app(Gateway(broken))) as client:
        response = post_query(client, metrics=["plays"], start="2026-01-01T05:00:00Z")
    assert (response.status_code, response.json()) == (502, {"detail": "pinot: broker unavailable"})


def call_mcp(target, method, *args):
    async def main():
        async with Client(target) as client:
            return await getattr(client, method)(*args)

    return anyio.run(main)


def test_mcp_tools_are_listed_with_schemas():
    tools = {t.name: t for t in call_mcp(create_mcp_server(Gateway(router())), "list_tools").tools}
    assert tools.keys() == {"list_metrics", "query_metrics", "data_status"}
    assert set(tools["query_metrics"].input_schema["required"]) == {"metrics", "start", "end"}
    assert tools["query_metrics"].annotations.read_only_hint is True


def test_mcp_query_matches_rest(client):
    arguments = {"metrics": ["plays", "slow_rate"], "dimensions": ["isp"], "start": START, "end": END}
    result = call_mcp(create_mcp_server(Gateway(router())), "call_tool", "query_metrics", arguments)
    assert result.is_error is False
    assert result.structured_content["rows"] == post_query(client, **arguments).json()["rows"]


@pytest.mark.parametrize(
    ("arguments", "message"),
    [
        ({"metrics": ["playz"], "start": START, "end": END}, "unknown metrics: playz. Available:"),
        ({"metrics": ["plays"], "start": END, "end": START}, "start must be before end"),
        ({"metrics": ["plays"], "start": START}, "end"),  # missing argument
    ],
)
def test_mcp_errors_explain_what_to_fix(arguments, message):
    result = call_mcp(create_mcp_server(Gateway(router())), "call_tool", "query_metrics", arguments)
    assert result.is_error is True
    assert message in result.content[0].text


def test_mcp_over_http_is_mounted_in_the_app():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    server = uvicorn.Server(uvicorn.Config(create_app(Gateway(router())), host="127.0.0.1", port=port, log_level="warning"))
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    try:
        deadline = time.monotonic() + 10
        while not server.started:
            assert time.monotonic() < deadline, "server didn't start"
            time.sleep(0.05)
        result = call_mcp(f"http://127.0.0.1:{port}/mcp", "call_tool", "list_metrics", {})
        assert result.is_error is False
        assert [m["name"] for m in result.structured_content["models"]] == ["playback"]
    finally:
        server.should_exit = True
        thread.join(timeout=10)


def test_run_py_serves_mcp_over_stdio():
    """`python run.py mcp` is what Claude Code or Claude Desktop launch. Listing
    tools needs only the config, not the running stack."""
    root = Path(__file__).parents[1]
    params = StdioServerParameters(command=sys.executable, args=[str(root / "run.py"), "mcp"], cwd=str(root))
    tools = call_mcp(params, "list_tools").tools
    assert {t.name for t in tools} == {"list_metrics", "query_metrics", "data_status"}


class FakeRollups:
    """Stands in for core.rollups.RollupStore (tested in test_rollups.py): one rollup whose last gate blocked."""

    def statuses(self):
        audit = {
            "at": "2026-01-01T05:00:10",
            "published": False,
            "start": "2026-01-01T00:00:00",
            "end": "2026-01-01T05:00:00",
            "checks": [{"check": "freshness(<= 2h)", "severity": "error", "passed": False, "detail": "stale"}],
        }
        return [RollupStatus("gold.hourly", "Plays per hour.", "hour", ["ts", "plays"], datetime(2026, 1, 1, 4), audit)]

    def rows(self, table, since):
        if table != "gold.hourly":
            raise KeyError(table)
        return pa.table({"ts": [datetime(2026, 1, 1, 3)], "plays": [12]})


@pytest.fixture
def status_client(tmp_path):
    (tmp_path / "playback-0123abcd.patch").write_text(
        "# Drift proposal for dataset 'playback' (raw.playback)\n"
        "# Upstream added cdn_pop; proposed a dimension.\n"
        "#\n# Review, then from the repository root: git apply <this file>\n\n--- a/config/ingestion.yaml\n"
    )
    gateway = Gateway(router(), rollups=FakeRollups(), proposals_dir=tmp_path)
    with TestClient(create_app(gateway)) as client:
        yield client


def test_status_reports_freshness_gates_and_drift(status_client):
    status = status_client.get("/status").json()
    assert status["datasets"] == [{"name": "playback", "realtime_days": pytest.approx(4 / 24), "historical_through": "2026-01-01T04:00:00Z"}]
    [rollup] = status["rollups"]
    assert (rollup["table"], rollup["complete_through"]) == ("gold.hourly", "2026-01-01T04:00:00Z")
    assert (rollup["last_audit"]["published"], rollup["last_audit"]["at"]) == (False, "2026-01-01T05:00:10Z")
    [proposal] = status["drift_proposals"]
    assert (proposal["dataset"], proposal["summary"]) == ("playback", "Upstream added cdn_pop; proposed a dimension.")


def test_rollup_rows_are_served(status_client):
    assert status_client.get("/rollups/gold.hourly").json() == {
        "table": "gold.hourly",
        "rows": [{"ts": "2026-01-01T03:00:00Z", "plays": 12}],
    }
    response = status_client.get("/rollups/gold.nowhere")
    assert (response.status_code, response.json()) == (404, {"detail": "no rollup named 'gold.nowhere'"})


def test_the_dashboard_is_served_at_the_root(status_client):
    response = status_client.get("/")
    assert response.status_code == 200 and response.headers["content-type"].startswith("text/html")


def test_mcp_data_status_tool(tmp_path):
    result = call_mcp(create_mcp_server(Gateway(router(), rollups=FakeRollups(), proposals_dir=tmp_path)), "call_tool", "data_status", {})
    assert result.is_error is False
    assert result.structured_content["rollups"][0]["last_audit"]["checks"][0]["detail"] == "stale"


def test_the_gateway_starts_while_the_stack_is_unreachable():
    """Nothing connects until a request needs it: run.py serve and run.py mcp start even when
    the engines or the Iceberg catalog are down (as in CI, where none of them run)."""
    platform = load_platform(Path(__file__).parents[1] / "config")
    unreachable = platform.deployment.model_copy(
        update={"iceberg": platform.deployment.iceberg.model_copy(update={"catalog_uri": "http://127.0.0.1:1"})}
    )
    gateway = Gateway.from_platform(replace(platform, deployment=unreachable))
    assert {t.name for t in call_mcp(create_mcp_server(gateway), "list_tools").tools} == {"list_metrics", "query_metrics", "data_status"}
