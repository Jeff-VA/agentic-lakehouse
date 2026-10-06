"""Fixtures for tests against the local Docker stack. They need it running,
set up and seeded:

    docker compose -f docker/docker-compose.yml up -d
    python run.py setup && python run.py seed

Test modules here set `pytestmark = pytest.mark.integration`; run them with
`pytest -m integration`.
"""

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
