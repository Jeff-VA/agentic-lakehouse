"""A project's views and dashboards, run without the Docker stack.

standin_gateway() builds the gateway on DuckDB: a few hours of synthetic
events for every dataset (from its simulator, or random values that fit its
columns), one table that both tiers read, the raw Iceberg tables as views of
it, and every rollup computed from it. A project's tests use it to check that
each view returns rows and each dashboard draws, in CI, without Pinot, Trino
or Iceberg.

    from agentic_lakehouse.browser import capture, serving
    from agentic_lakehouse.api_gateway import create_app
    gateway = standin_gateway(load_platform("config"))
    with serving(create_app(gateway, dashboards_dir="config/dashboards")) as url:
        assert capture(f"{url}/d/overview").problems == []
"""

from datetime import UTC, datetime, timedelta

import duckdb
import pyarrow as pa

from agentic_lakehouse.api_gateway import Gateway, TableState, Warehouse
from agentic_lakehouse.compiler import SqlCompiler, bucket_start
from agentic_lakehouse.config import Platform
from agentic_lakehouse.engines import DuckDBEngine
from agentic_lakehouse.router import Placement, Router, Tier
from agentic_lakehouse.schema_compiler import arrow_schema
from agentic_lakehouse.simulate import generate_events


def standin_gateway(platform: Platform, *, hours: int = 6, rate: float = 6, seed: int = 1) -> Gateway:
    """The gateway over `hours` of synthetic events ending now, `rate` episodes a minute per dataset."""
    now = datetime.now(UTC).replace(tzinfo=None, second=0, microsecond=0)
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")  # now() in views over tables, as in Trino's sessions
    engine = DuckDBEngine(con)

    placements = {}
    for dataset in platform.ingestion.datasets:
        events = generate_events(dataset, now - timedelta(hours=hours), now, rate=rate, seed=seed)
        con.register("_events", pa.Table.from_pylist(events, schema=arrow_schema(dataset)))
        con.execute(f'CREATE TABLE "{dataset.name}" AS SELECT * FROM _events')
        con.unregister("_events")
        raw = platform.iceberg_table(dataset.name).table  # e.g. raw.events, which views over tables read
        con.execute(f'CREATE SCHEMA IF NOT EXISTS {raw.split(".")[0]}; CREATE VIEW {raw} AS SELECT * FROM "{dataset.name}"')
        placements[dataset.name] = Placement(
            realtime=Tier(engine, f'"{dataset.name}"'),
            historical=Tier(engine, f'"{dataset.name}"'),
            retention=timedelta(days=dataset.realtime.retention_days),
            flushed_through=lambda: now - timedelta(minutes=5),
        )

    compiler = SqlCompiler(platform.ontology)
    complete = bucket_start(now, "hour")
    for rollup in platform.storage.rollups:
        start = bucket_start(now - timedelta(hours=hours), rollup.query.grain)
        window = rollup.with_window(start, max(complete, start + timedelta(hours=1)))
        source = platform.rollup_model(rollup).source
        sql = compiler.compile(window, engine="duckdb", table=f'"{source}"')
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {rollup.table.split('.')[0]}; CREATE TABLE {rollup.table} AS {sql}")

    warehouse = Warehouse(engine, table_state=lambda table: TableState(exists=True, complete_through=complete, snapshot=1))
    return Gateway(Router(compiler, placements), views=platform.views, warehouse=warehouse)
