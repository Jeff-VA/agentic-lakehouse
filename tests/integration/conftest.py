"""Fixtures for tests against the local Docker stack. They need it running,
set up and seeded:

    docker compose -f docker/docker-compose.yml up -d
    python run.py setup && python run.py seed

Test modules here set `pytestmark = pytest.mark.integration`; run them with
`pytest -m integration`.
"""

import time
from datetime import timedelta
from pathlib import Path

import httpx
import pytest

from core.config import load_platform
from core.engines import DuckDBEngine, PinotEngine, TrinoEngine
from core.orchestrator import iceberg_catalog

ROOT = Path(__file__).parents[2]


@pytest.fixture(scope="session")
def platform():
    platform = load_platform(ROOT / "config")
    trino = platform.deployment.trino
    for url in (f"{platform.deployment.pinot.broker_url}/health", f"http://{trino.host}:{trino.port}/v1/info"):
        try:
            httpx.get(url, timeout=2).raise_for_status()
        except httpx.HTTPError:
            pytest.skip(f"stack not reachable at {url}; start it with: docker compose -f docker/docker-compose.yml up -d")
    return platform


@pytest.fixture(scope="session")
def engines(platform):
    """DuckDB, Trino and Pinot. DuckDB gets the Iceberg table's rows as a `telemetry` view."""
    duckdb = DuckDBEngine()
    iceberg_table = iceberg_catalog(platform).load_table(platform.iceberg_table("telemetry").table)
    duckdb.con.register("telemetry", iceberg_table.scan().to_arrow())
    return {
        "duckdb": duckdb,
        "trino": TrinoEngine(platform.deployment.trino),
        "pinot": PinotEngine(platform.deployment.pinot.broker_url),
    }


@pytest.fixture(scope="session")
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
