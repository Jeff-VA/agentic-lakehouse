"""Pipeline steps (agentic_lakehouse/steps.py): SQL or Python that writes a table, in dependency order."""

import json
from datetime import datetime, timedelta
from pathlib import Path

import duckdb
import pyarrow as pa
import pytest
import yaml
from pyiceberg.catalog.sql import SqlCatalog

from agentic_lakehouse.config import ConfigError, load_platform
from agentic_lakehouse.engines import DuckDBEngine
from agentic_lakehouse.rollups import QualityGateError
from agentic_lakehouse.scaffold import create_project
from agentic_lakehouse.steps import STEP_RUN, IcebergStepStore, StepContext, StepError, run_step
from agentic_lakehouse.testing import duckdb_query, standin_gateway

NOW = datetime(2026, 10, 12, 12, 0)

# Two steps over the starter project's hourly rollup: daily totals in SQL, and a per-category
# baseline "model" fitted in Python from the daily totals: so the Python step runs second.
PIPELINES = {
    "version": "1.0.0",
    "steps": [
        {
            "name": "category_baselines",
            "description": "Each category's mean daily events: a tiny model the dashboard compares against.",
            "run": "test_steps:fit_baselines",
            "reads": ["gold.daily_activity"],
            "output": "gold.category_baselines",
            "columns": {"category": "string", "mean_events": "double", "days": "int"},
            "every_minutes": 360,
        },
        {
            "name": "daily_activity",
            "description": "Events and amount per category and day.",
            "sql": "SELECT date_trunc('day', event_ts) AS day, category, sum(event_count) AS events, "
            "sum(total_amount) AS amount FROM gold.hourly_activity GROUP BY 1, 2",
            "output": "gold.daily_activity",
            "time_column": "day",
            "every_minutes": 60,
        },
    ],
}
QUALITY = {
    "version": "1.0.0",
    "rules": [
        {"table": "gold.hourly_activity", "checks": [{"type": "row_count", "min": 1}]},
        {"table": "gold.category_baselines", "checks": [{"type": "range", "column": "mean_events", "min": 0}]},
    ],
}
VIEW = """/*
description: Today's events per category against the category's baseline.
*/
SELECT d.category, d.events, b.mean_events
FROM gold.daily_activity AS d JOIN gold.category_baselines AS b ON b.category = d.category
"""
calls: list[StepContext] = []


def fit_baselines(context: StepContext) -> pa.Table:
    calls.append(context)
    daily = context.query("SELECT category, avg(events) AS mean_events, count(*) AS days FROM gold.daily_activity GROUP BY category")
    return daily  # its `days` is a BIGINT, cast to the declared int


@pytest.fixture
def project(tmp_path) -> Path:
    create_project(tmp_path / "steps-demo")
    config = tmp_path / "steps-demo" / "config"
    (config / "pipelines.yaml").write_text(yaml.safe_dump(PIPELINES, sort_keys=False))
    (config / "quality_rules.yaml").write_text(yaml.safe_dump(QUALITY, sort_keys=False))
    (config / "views" / "versus_baseline.sql").write_text(VIEW)
    return config


def test_steps_run_in_the_order_their_reads_require(project):
    platform = load_platform(project)
    assert [s.name for s in platform.ordered_steps()] == ["daily_activity", "category_baselines"]
    schemas = platform.table_schemas()
    assert schemas["gold.daily_activity"] == {"day": "TIMESTAMP", "category": "VARCHAR", "events": "BIGINT", "amount": "DOUBLE"}
    assert schemas["gold.category_baselines"] == {"category": "VARCHAR", "mean_events": "DOUBLE", "days": "INTEGER"}
    assert [c.name for c in platform.views["versus_baseline"].columns] == ["category", "events", "mean_events"]


def test_the_stand_in_runs_the_steps_so_views_over_them_work(project):
    calls.clear()
    (project / "views" / "raw_events.sql").write_text("/*\ndescription: Every event so far.\n*/\nSELECT count(*) AS events FROM raw.events")
    gateway = standin_gateway(load_platform(project))
    rows = gateway.view("versus_baseline").rows
    assert rows and all(row["mean_events"] > 0 for row in rows)
    assert gateway.view("raw_events").rows[0]["events"] > 0  # the raw table named like its dataset
    [context] = calls
    assert context.step.name == "category_baselines" and context.last_published is None


class MemoryStore:
    def __init__(self):
        self.tables, self.records = {}, {}

    def last_run(self, table):
        return self.records.get(table)

    def publish(self, step, rows, record):
        self.tables[step.output] = rows if step.mode == "replace" or step.output not in self.tables else pa.concat_tables([self.tables[step.output], rows])
        self.records[step.output] = record

    def record(self, step, record):
        self.records[step.output] = record


@pytest.fixture
def warehouse():
    """DuckDB with the hourly rollup the steps read."""
    con = duckdb.connect()
    con.execute("CREATE SCHEMA gold; CREATE TABLE gold.hourly_activity (event_ts TIMESTAMP, category VARCHAR, event_count BIGINT, total_amount DOUBLE, avg_amount DOUBLE)")
    con.execute("INSERT INTO gold.hourly_activity VALUES ('2026-10-11 10:00', 'a', 4, 40.0, 10.0), ('2026-10-12 10:00', 'a', 6, 30.0, 5.0), ('2026-10-12 10:00', 'b', 2, 8.0, 4.0)")
    return DuckDBEngine(con)


def test_a_step_runs_when_due_and_records_each_run(project, warehouse):
    platform = load_platform(project)
    daily, store = platform.step("daily_activity"), MemoryStore()
    query = duckdb_query(warehouse)
    first = run_step(platform, daily, query=query, store=store, now=NOW, log=lambda m: None)
    assert (first.rows, first.skipped) == (3, None)
    assert store.tables["gold.daily_activity"].column_names == ["day", "category", "events", "amount"]
    assert run_step(platform, daily, query=query, store=store, now=NOW + timedelta(minutes=30), log=lambda m: None).skipped == "ran less than 60 minutes ago"
    assert run_step(platform, daily, query=query, store=store, now=NOW + timedelta(minutes=60), log=lambda m: None).rows == 3
    assert store.records["gold.daily_activity"]["published_at"] == "2026-10-12T13:00:00"


def test_a_python_steps_rows_take_its_declared_types_and_must_have_its_columns(project, warehouse, monkeypatch):
    platform = load_platform(project)
    store, query = MemoryStore(), duckdb_query(warehouse)
    run_step(platform, platform.step("daily_activity"), query=query, store=store, now=NOW, log=lambda m: None)
    warehouse.con.register("_daily", store.tables["gold.daily_activity"])
    warehouse.con.execute("CREATE TABLE gold.daily_activity AS SELECT * FROM _daily")
    run_step(platform, platform.step("category_baselines"), query=query, store=store, now=NOW, log=lambda m: None)
    baselines = store.tables["gold.category_baselines"]
    assert baselines.schema.field("days").type == pa.int32()  # the BIGINT the query gave, as declared
    assert sorted(baselines.to_pylist(), key=lambda r: r["category"]) == [
        {"category": "a", "mean_events": 5.0, "days": 2},
        {"category": "b", "mean_events": 2.0, "days": 1},
    ]
    monkeypatch.setattr("test_steps.fit_baselines", lambda context: pa.table({"category": ["a"]}))
    with pytest.raises(StepError, match="returned the wrong columns; missing days, mean_events"):
        run_step(platform, platform.step("category_baselines"), query=query, store=store, now=NOW, force=True, log=lambda m: None)


def test_a_blocked_quality_gate_publishes_nothing_and_says_so(project, warehouse, monkeypatch):
    platform = load_platform(project)
    store = MemoryStore()
    store.tables["gold.category_baselines"] = "the last good contents"
    monkeypatch.setattr("test_steps.fit_baselines", lambda c: pa.table({"category": ["a"], "mean_events": [-1.0], "days": [1]}))
    with pytest.raises(QualityGateError, match="range"):
        run_step(platform, platform.step("category_baselines"), query=duckdb_query(warehouse), store=store, now=NOW, log=lambda m: None)
    assert store.tables["gold.category_baselines"] == "the last good contents"
    assert store.records["gold.category_baselines"]["published"] is False


def test_iceberg_keeps_replaced_and_appended_outputs(project, tmp_path):
    platform = load_platform(project)
    catalog = SqlCatalog("test", uri=f"sqlite:///{tmp_path}/catalog.db", warehouse=f"file://{tmp_path}")
    store = IcebergStepStore(catalog)
    step = platform.step("category_baselines")
    for mean in (1.0, 2.0):
        store.publish(step, pa.table({"category": ["a"], "mean_events": [mean], "days": pa.array([1], pa.int32())}), {"at": "x"})
    assert catalog.load_table("gold.category_baselines").scan().to_arrow().to_pylist() == [{"category": "a", "mean_events": 2.0, "days": 1}]
    appended = step.model_copy(update={"mode": "append"})
    store.publish(appended, pa.table({"category": ["b"], "mean_events": [3.0], "days": pa.array([1], pa.int32())}), {"at": "y"})
    assert catalog.load_table("gold.category_baselines").scan().to_arrow().num_rows == 2
    assert json.loads(catalog.load_table("gold.category_baselines").properties[STEP_RUN]) == {"at": "y"}
    assert store.last_run("gold.category_baselines") == {"at": "y"}
    assert store.last_run("gold.nothing_yet") is None
    assert store.exists("gold.category_baselines") and not store.exists("gold.nothing_yet")


@pytest.mark.parametrize(
    ("change", "message"),
    [
        (lambda s: s["steps"][0].update(reads=["gold.nope"]), "step 'category_baselines' reads unknown tables: gold.nope"),
        (lambda s: s["steps"][1].update(sql="SELECT nope FROM gold.hourly_activity"), "step 'daily_activity': Column 'nope' could not be resolved"),
        (lambda s: s["steps"][1].update(output="gold.hourly_activity"), "writes gold.hourly_activity, which is already a raw table or rollup"),
        (lambda s: s["steps"][1].update(sql="SELECT category, mean_events AS events FROM gold.category_baselines"),
         "steps read each other in a cycle: category_baselines -> daily_activity -> category_baselines"),
    ],
)
def test_pipeline_mistakes_are_explained(project, change, message):
    pipelines = yaml.safe_load((project / "pipelines.yaml").read_text())
    change(pipelines)
    (project / "pipelines.yaml").write_text(yaml.safe_dump(pipelines, sort_keys=False))
    (project / "views" / "versus_baseline.sql").unlink()
    with pytest.raises(ConfigError) as raised:
        load_platform(project)
    assert message in str(raised.value)


def test_a_step_is_sql_or_python_and_python_declares_its_columns(project):
    pipelines = yaml.safe_load((project / "pipelines.yaml").read_text())
    pipelines["steps"][0].pop("columns")
    (project / "pipelines.yaml").write_text(yaml.safe_dump(pipelines, sort_keys=False))
    with pytest.raises(ValueError, match="declare a Python step's output `columns`"):
        load_platform(project)


def test_a_freshness_check_needs_the_steps_time_column(project):
    quality = yaml.safe_load((project / "quality_rules.yaml").read_text())
    quality["rules"][1]["checks"].append({"type": "freshness", "max_lag_hours": 2})
    (project / "quality_rules.yaml").write_text(yaml.safe_dump(quality, sort_keys=False))
    with pytest.raises(ConfigError, match="check freshness; give step 'category_baselines' a time_column"):
        load_platform(project)


def test_a_python_step_can_publish_nothing_this_run(project, warehouse, monkeypatch):
    platform = load_platform(project)
    store = MemoryStore()
    monkeypatch.setattr("test_steps.fit_baselines", lambda context: None)  # e.g. too few labels to train on
    run = run_step(platform, platform.step("category_baselines"), query=duckdb_query(warehouse), store=store, now=NOW, log=lambda m: None)
    assert run.skipped == "returned nothing to publish"
    assert "gold.category_baselines" not in store.tables
    assert store.records["gold.category_baselines"]["published"] is False


def test_a_step_waits_for_the_tables_it_reads(project, warehouse):
    """A rollup that hasn't run yet has no table: the step says so instead of failing."""
    platform = load_platform(project)
    store = MemoryStore()
    run = run_step(
        platform, platform.step("daily_activity"), query=duckdb_query(warehouse), store=store, now=NOW,
        log=lambda m: None, exists=lambda table: table != "gold.hourly_activity",
    )
    assert run.skipped == "waiting for gold.hourly_activity, which doesn't exist yet"
    assert store.records == {} and store.tables == {}
