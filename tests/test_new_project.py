"""The template promise: a new use case needs only YAML. Nothing here is about playback."""

import shutil
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import yaml

from agentic_lakehouse.compiler import MetricQuery, SqlCompiler
from agentic_lakehouse.config import load_platform
from agentic_lakehouse.engines import DuckDBEngine
from agentic_lakehouse.schema_compiler import arrow_schema, pinot_schema
from agentic_lakehouse.simulate import generate_events

EXAMPLE = Path(__file__).parents[1] / "examples" / "playback" / "config"

ORDERS = {
    "ingestion.yaml": {
        "version": "1",
        "datasets": [
            {
                "name": "orders",
                "description": "One event per order placed.",
                "topic": "orders",
                "time_column": "placed_at",
                "columns": [
                    {"name": "placed_at", "type": "timestamp"},
                    {"name": "region", "type": "string"},
                    {"name": "amount", "type": "double"},
                    {"name": "items", "type": "int"},
                ],
                "realtime": {"table": "orders", "retention_days": 7},
            }
        ],
    },
    "ontology.yaml": {
        "version": "1",
        "models": [
            {
                "name": "sales",
                "description": "Orders and revenue.",
                "source": "orders",
                "time_dimension": "placed_at",
                "dimensions": [{"name": "placed_at", "type": "timestamp"}, {"name": "region", "type": "string"}],
                "metrics": [
                    {"name": "orders", "description": "Orders placed.", "agg": "count"},
                    {"name": "revenue", "description": "Order value (USD).", "agg": "sum", "sql": "amount"},
                    {"name": "avg_items", "description": "Items per order.", "agg": "avg", "sql": "items"},
                ],
            }
        ],
    },
    "storage.yaml": {
        "version": "1",
        "tables": [
            {
                "dataset": "orders",
                "table": "raw.orders",
                "partition_by": "day",
                "flush_every_minutes": 5,
                "allowed_lateness_minutes": 2,
            }
        ],
    },
    "quality_rules.yaml": {"version": "1"},
}


def test_a_new_project_needs_only_yaml(tmp_path):
    for name, content in ORDERS.items():
        (tmp_path / name).write_text(yaml.safe_dump(content))
    shutil.copy(EXAMPLE / "deployment.yaml", tmp_path)
    platform = load_platform(tmp_path)
    orders = platform.dataset("orders")

    assert {f["name"] for f in pinot_schema(orders)["metricFieldSpecs"]} == {"amount", "items"}  # tables follow the schema
    events = generate_events(orders, datetime(2026, 1, 1), datetime(2026, 1, 1, 3))  # no simulator: random values
    engine = DuckDBEngine()
    engine.con.register("orders", pa.Table.from_pylist(events, schema=arrow_schema(orders)))
    query = MetricQuery.model_validate(
        {
            "metrics": ["orders", "revenue", "avg_items"],
            "dimensions": ["region"],
            "grain": "hour",
            "start": "2026-01-01T00:00:00",
            "end": "2026-01-01T03:00:00",
        }
    )
    rows = engine.execute(SqlCompiler(platform.ontology).compile(query, engine="duckdb", table="orders")).to_pylist()

    assert sum(r["orders"] for r in rows) == len(events)
    assert {r["region"] for r in rows} <= {f"region_{i}" for i in range(1, 6)}
    assert all(r["revenue"] > 0 and 0 <= r["avg_items"] <= 1000 for r in rows)
