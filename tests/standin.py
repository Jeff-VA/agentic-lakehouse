"""The example project's gateway on DuckDB instead of the Docker stack.

Six hours of simulated playback events, a table both tiers read, and the
hourly gold rollup computed from them: enough for the example's views and
dashboard to run in tests (and in a browser) without Pinot, Trino or Iceberg.
"""

import contextlib
import socket
import threading
import time
from datetime import UTC, datetime, timedelta

import duckdb
import pyarrow as pa
import uvicorn

from core.api_gateway import Gateway, TableState, Warehouse
from core.compiler import SqlCompiler, bucket_start
from core.config import Platform
from core.engines import DuckDBEngine
from core.router import Placement, Router, Tier
from core.schema_compiler import arrow_schema
from core.simulate import generate_events


def standin_gateway(platform: Platform, *, hours: int = 6) -> Gateway:
    now = datetime.now(UTC).replace(tzinfo=None, second=0, microsecond=0)
    dataset = platform.dataset("telemetry")
    events = generate_events(dataset, now - timedelta(hours=hours), now, rate=6, seed=1)
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")  # now() in views over tables, like Trino's sessions
    con.register("events", pa.Table.from_pylist(events, schema=arrow_schema(dataset)))
    con.execute("CREATE TABLE telemetry AS SELECT * FROM events")
    engine = DuckDBEngine(con)

    compiler = SqlCompiler(platform.ontology)
    complete = bucket_start(now, "hour")
    for rollup in platform.storage.rollups:
        start = bucket_start(now - timedelta(hours=hours), rollup.query.grain)
        sql = compiler.compile(rollup.with_window(start, max(complete, start + timedelta(hours=1))), engine="duckdb", table="telemetry")
        namespace = rollup.table.split(".")[0]
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {namespace}; CREATE TABLE {rollup.table} AS {sql}")

    placement = Placement(
        realtime=Tier(engine, "telemetry"),
        historical=Tier(engine, "telemetry"),
        retention=timedelta(days=dataset.realtime.retention_days),
        flushed_through=lambda: now - timedelta(minutes=5),
    )
    router = Router(compiler, {"telemetry": placement})
    warehouse = Warehouse(engine, table_state=lambda table: TableState(exists=True, complete_through=complete, snapshot=1))
    return Gateway(router, views=platform.views, warehouse=warehouse)


@contextlib.contextmanager
def serving(app):
    """`app` on a real HTTP server, as a browser needs; yields its base URL."""
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning"))
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    deadline = time.monotonic() + 10
    while not server.started:
        assert time.monotonic() < deadline, "server didn't start"
        time.sleep(0.05)
    try:
        yield f"http://127.0.0.1:{port}"
    finally:
        server.should_exit = True
        thread.join(timeout=10)
