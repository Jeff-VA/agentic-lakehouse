"""The example project's views on the real stack, served as `agentic-lakehouse serve` serves them."""

import json
import socket
import threading
import time
from datetime import UTC, datetime, timedelta

import anyio
import httpx
import pytest
import uvicorn
from fastapi.testclient import TestClient
from mcp import Client

from agentic_lakehouse.api_gateway import Gateway, create_app, create_mcp_server
from agentic_lakehouse.live import LiveViews

pytestmark = pytest.mark.integration

ISPS = {"comcast", "verizon", "att", "spectrum", "cox", "frontier"}


@pytest.fixture(scope="module")
def client(platform):
    with TestClient(create_app(Gateway.from_platform(platform))) as client:
        yield client


def test_every_example_view_runs(client):
    names = [v["name"] for v in client.get("/views").json()["views"]]
    assert sorted(names) == ["isp_hourly", "isp_now", "quality_overall"]
    for name in names:
        response = client.get(f"/views/{name}")
        assert response.status_code == 200, response.text


def test_isp_now_reads_the_real_time_tier(client):
    body = client.get("/views/isp_now", params={"minutes": 240, "include_sql": True}).json()
    if not body["rows"]:
        pytest.skip("no events in the last 4 hours; start the stream: agentic-lakehouse stream")
    assert {row["isp_provider"] for row in body["rows"]} <= ISPS
    rates = [row["rebuffer_incidence_rate"] for row in body["rows"]]
    assert rates == sorted(rates, reverse=True)  # the view's ORDER BY
    assert body["sql"].keys() == {"realtime"}
    end = datetime.fromisoformat(body["coverage"]["end"])
    assert datetime.fromisoformat(body["coverage"]["start"]) == end - timedelta(minutes=240)


def test_an_earlier_window_is_a_parameter_away(client):
    body = client.get("/views/quality_overall", params={"from_minutes_ago": 120, "to_minutes_ago": 60}).json()
    assert body["params"] == {"from_minutes_ago": 120, "to_minutes_ago": 60}
    start, end = (datetime.fromisoformat(body["coverage"][key]) for key in ("start", "end"))
    assert end - start == timedelta(hours=1)
    assert abs(datetime.now(UTC) - timedelta(hours=1) - end) < timedelta(minutes=1)


def test_isp_hourly_matches_the_gold_table(client):
    """The view runs on Trino; /rollups reads the same Iceberg table through PyIceberg."""
    body = client.get("/views/isp_hourly", params={"hours": 12}).json()
    if body["data_through"] is None:
        pytest.skip("the hourly rollup hasn't published yet; run: agentic-lakehouse rollups")
    through = datetime.fromisoformat(body["data_through"])
    assert body["rows"], "the rollup has published, so the last 12 hours should have rows"
    now = datetime.now(UTC)
    hours = [datetime.fromisoformat(row["hour"]) for row in body["rows"]]
    assert all(now - timedelta(hours=12) <= h < through for h in hours)  # Trino's now() is UTC
    # Rows more than the rollup's lookback (6 h) before `through` can't change between the two reads.
    stable = through - timedelta(hours=6)
    gold = {
        (row["event_ts"], row["isp_provider"]): row
        for row in client.get("/rollups/gold.hourly_isp_quality", params={"hours": 12}).json()["rows"]
    }
    for row in body["rows"]:
        if datetime.fromisoformat(row["hour"]) < stable:
            expected = gold[(row["hour"], row["isp_provider"])]
            assert {k: row[k] for k in ("unique_sessions", "p99_ttfb")} == {k: expected[k] for k in ("unique_sessions", "p99_ttfb")}


def test_agents_run_views_over_mcp(platform):
    async def main():
        async with Client(create_mcp_server(Gateway.from_platform(platform))) as client:
            return await client.call_tool("query_view", {"name": "quality_overall"})

    result = anyio.run(main)
    assert result.is_error is False
    assert [c["name"] for c in result.structured_content["columns"]] == ["rebuffer_incidence_rate", "p99_ttfb", "unique_sessions"]


@pytest.fixture(scope="module")
def server(platform):
    """The gateway on a real HTTP server, with live views refreshing every second."""
    gateway = Gateway.from_platform(platform)
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    app = create_app(gateway, live=LiveViews(gateway, refresh_seconds=1))
    server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning"))
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    deadline = time.monotonic() + 10
    while not server.started:
        assert time.monotonic() < deadline, "server didn't start"
        time.sleep(0.05)
    yield f"http://127.0.0.1:{port}"
    server.should_exit = True
    thread.join(timeout=10)


def test_new_events_reach_a_live_view(server):
    """Kafka -> Pinot -> the view's feed -> Server-Sent Events, while the stream service publishes."""
    results = []
    with httpx.Client(base_url=server, timeout=30) as client:
        with client.stream("GET", "/views/isp_now/live", params={"minutes": 5}) as response:
            event = None
            for line in response.iter_lines():
                if line.startswith("event: "):
                    event = line.removeprefix("event: ")
                elif line.startswith("data: ") and event == "result":
                    results.append(json.loads(line.removeprefix("data: ")))
                    if len(results) == 2:
                        break
    if not results[0]["rows"]:
        pytest.skip("no events in the last 5 minutes; start the stream: agentic-lakehouse stream")
    sessions = [sum(row["unique_sessions"] for row in result["rows"]) for result in results]
    assert results[0]["rows"] != results[1]["rows"], sessions  # pushed because the numbers moved
