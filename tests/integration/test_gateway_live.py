"""The gateway wired to the real stack, exactly as `python run.py serve` builds it."""

import anyio
import pytest
from fastapi.testclient import TestClient
from mcp import Client

from core.api_gateway import Gateway, create_app, create_mcp_server
from core.compiler import MetricQuery, SqlCompiler
from core.orchestrator import flushed_through
from core.router import Router

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def gateway(platform):
    return Gateway(Router.from_platform(platform))


def test_catalog_reports_how_far_the_data_reaches(platform, gateway):
    with TestClient(create_app(gateway)) as client:
        [model] = client.get("/catalog").json()["models"]
    assert model["realtime_days"] == 7
    assert model["historical_through"] == flushed_through(platform, "telemetry").isoformat() + "Z"


def as_json(window: dict) -> dict:
    return {key: ts.isoformat() for key, ts in window.items()}


def test_rest_query_on_the_live_tiers(platform, engines, window, gateway):
    fields = {"metrics": ["event_count", "unique_sessions"], "dimensions": ["isp_provider"]}
    with TestClient(create_app(gateway)) as client:
        response = client.post("/query", params={"include_sql": True}, json={**fields, **as_json(window)})
    assert response.status_code == 200
    body = response.json()
    assert body["sql"].keys() == {"realtime"}  # the seeded day is inside Pinot's 7-day retention
    query = MetricQuery.model_validate({**fields, **window})
    expected = engines["duckdb"].execute(SqlCompiler(platform.ontology).compile(query, engine="duckdb", table="telemetry"))
    assert body["rows"] == expected.to_pylist()


def test_mcp_query_on_the_live_tiers(window, gateway):
    arguments = {"metrics": ["p99_ttfb"], "dimensions": ["is_mobile_device"], **as_json(window)}

    async def main():
        async with Client(create_mcp_server(gateway)) as client:
            return await client.call_tool("query_metrics", arguments)

    result = anyio.run(main)
    assert result.is_error is False
    assert [row["is_mobile_device"] for row in result.structured_content["rows"]] == [False, True]
