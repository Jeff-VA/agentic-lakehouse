"""Fixtures for tests against the local Docker stack. They need it running,
set up and seeded:

    docker compose -f docker/docker-compose.yml up -d
    python run.py setup && python run.py seed && python run.py flush

Test modules here set `pytestmark = pytest.mark.integration`; run them with
`pytest -m integration`.
"""

import time
from datetime import datetime, timedelta
from pathlib import Path

import httpx
import pytest

from core.config import load_platform
from core.engines import DuckDBEngine, PinotEngine, TrinoEngine
from core.compiler import bucket_start
from core.orchestrator import flushed_through, iceberg_catalog

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
    """An hour-aligned range that both tiers hold completely: from the first
    flushed event up to the flush watermark (a live stream may run past it),
    once Pinot has ingested the same events."""
    flushed = flushed_through(platform, "telemetry")
    if flushed is None:
        pytest.skip("nothing flushed yet; run: python run.py seed && python run.py flush")
    end = bucket_start(flushed, "hour")
    trino_table = platform.table_for("telemetry", "trino")
    [first] = engines["trino"].execute(f"SELECT min(event_ts) AS t FROM {trino_table}").to_pylist()
    start = bucket_start(first["t"], "hour")
    if start >= end:
        pytest.skip("less than an hour of flushed events; run: python run.py seed && python run.py flush")

    def count(engine: str, table: str, low: str, high: str) -> int:
        sql = f"SELECT COUNT(*) AS n FROM {table} WHERE event_ts >= {low} AND event_ts < {high}"
        return engines[engine].execute(sql)["n"][0].as_py()

    iceberg_rows = count("trino", trino_table, f"TIMESTAMP '{start}'", f"TIMESTAMP '{end}'")
    millis = [str((t - datetime(1970, 1, 1)) // timedelta(milliseconds=1)) for t in (start, end)]
    deadline = time.monotonic() + 30  # Pinot may still be consuming from Kafka
    while (pinot_rows := count("pinot", platform.table_for("telemetry", "pinot"), *millis)) != iceberg_rows:
        if time.monotonic() > deadline:
            pytest.fail(
                f"between {start} and {end} Pinot holds {pinot_rows} events but Iceberg holds {iceberg_rows}; "
                "reload both: python run.py reset --yes && python run.py seed && python run.py flush"
            )
        time.sleep(1)
    return {"start": start, "end": end}
