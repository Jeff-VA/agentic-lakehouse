import json
from dataclasses import replace
from datetime import datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pytest
from pyiceberg.catalog.sql import SqlCatalog

from agentic_lakehouse.compiler import SqlCompiler
from agentic_lakehouse.config import load_platform
from agentic_lakehouse.engines import DuckDBEngine
from agentic_lakehouse.simulate import generate_events
from agentic_lakehouse.quality import QualityConfig
from agentic_lakehouse.rollups import LAST_AUDIT, ROLLUP_THROUGH, QualityGateError, RollupStore, materialize, window
from agentic_lakehouse.schema_compiler import arrow_schema

PLATFORM = load_platform(Path(__file__).parents[1] / "examples" / "playback" / "config")
DATASET = PLATFORM.dataset("telemetry")
HOURLY = PLATFORM.rollup("gold.hourly_isp_quality")  # lookback 6 h, backfill 14 days
T0 = datetime(2026, 1, 1)
FLUSHED = T0 + timedelta(hours=8, minutes=30)  # the historical tier is complete through 08:30


@pytest.fixture
def source():
    """DuckDB standing in for the historical tier: eight and a half hours of telemetry."""
    engine = DuckDBEngine()
    events = generate_events(DATASET, T0, FLUSHED, seed=3)
    engine.con.register("telemetry", pa.Table.from_pylist(events, schema=arrow_schema(DATASET)))
    return engine


@pytest.fixture
def catalog(tmp_path):
    return SqlCatalog("test", uri=f"sqlite:///{tmp_path}/catalog.db", warehouse=f"file://{tmp_path}")


def refresh(source, catalog, rollup=HOURLY, platform=PLATFORM, watermark=FLUSHED):
    return materialize(
        rollup, platform, engine=source, source_table="telemetry", catalog=catalog, watermark=watermark, log=lambda m: None
    )


def gold_rows(catalog, table="gold.hourly_isp_quality") -> list[dict]:
    return sorted(catalog.load_table(table).scan().to_arrow().to_pylist(), key=lambda r: (r["event_ts"], r["isp_provider"]))


def expected_rows(source, start, end, rollup=HOURLY) -> list[dict]:
    """The rollup's query run directly over the source."""
    sql = SqlCompiler(PLATFORM.ontology).compile(rollup.with_window(start, end), engine="duckdb", table="telemetry")
    return sorted(source.execute(sql).to_pylist(), key=lambda r: (r["event_ts"], r["isp_provider"]))


@pytest.mark.parametrize(
    ("watermark", "previous_end", "expected"),
    [
        (T0 + timedelta(hours=8, minutes=30), None, (T0 - timedelta(days=14) + timedelta(hours=8), T0 + timedelta(hours=8))),  # first run: backfill
        (T0 + timedelta(hours=8, minutes=30), T0 + timedelta(hours=7), (T0 + timedelta(hours=2), T0 + timedelta(hours=8))),  # recount 6 h
        (T0 + timedelta(hours=8, minutes=30), T0 - timedelta(days=2), (T0 - timedelta(days=2), T0 + timedelta(hours=8))),  # after an outage
        (T0 + timedelta(hours=8, minutes=59), T0 + timedelta(hours=8), None),  # no hour has completed since
    ],
)
def test_window(watermark, previous_end, expected):
    assert window(HOURLY, watermark, previous_end) == expected


def test_first_run_publishes_complete_hours_only(source, catalog):
    run = refresh(source, catalog)

    assert (run.start.date(), run.end) == ((T0 - timedelta(days=14)).date(), T0 + timedelta(hours=8))
    assert gold_rows(catalog) == expected_rows(source, run.start, run.end)
    assert max(r["event_ts"] for r in gold_rows(catalog)) == T0 + timedelta(hours=7)  # 08:00-08:30 isn't complete
    table = catalog.load_table("gold.hourly_isp_quality")
    assert table.properties[ROLLUP_THROUGH] == (T0 + timedelta(hours=8)).isoformat()
    audit = json.loads(table.properties[LAST_AUDIT])
    assert (audit["published"], audit["start"], audit["end"]) == (True, run.start.isoformat(), run.end.isoformat())
    assert all(check["passed"] for check in audit["checks"] if check["severity"] == "error")
    assert table.properties["comment"] == HOURLY.description


def test_rerunning_replaces_rather_than_duplicates(source, catalog):
    refresh(source, catalog)
    refresh(source, catalog)
    assert gold_rows(catalog) == expected_rows(source, T0 - timedelta(days=14), T0 + timedelta(hours=8))


def test_late_events_inside_the_lookback_are_recounted(source, catalog):
    refresh(source, catalog)
    late = generate_events(DATASET, T0 + timedelta(hours=5), T0 + timedelta(hours=6), seed=99)  # arrives after the first run
    events = source.con.execute("SELECT * FROM telemetry").to_arrow_table()
    source.con.register("telemetry", pa.concat_tables([events, pa.Table.from_pylist(late, schema=arrow_schema(DATASET))]))

    refresh(source, catalog, watermark=FLUSHED + timedelta(hours=1))

    assert gold_rows(catalog) == expected_rows(source, T0 - timedelta(days=14), T0 + timedelta(hours=9))


def test_a_failed_quality_gate_publishes_nothing(source, catalog):
    refresh(source, catalog)
    snapshot = catalog.load_table("gold.hourly_isp_quality").current_snapshot().snapshot_id
    strict = replace(
        PLATFORM,
        quality=QualityConfig.model_validate(
            {"version": "1", "rules": [{"table": HOURLY.table, "checks": [{"type": "row_count", "max": 5}]}]}
        ),
    )
    with pytest.raises(QualityGateError, match=r"gold.hourly_isp_quality: quality gate failed, nothing published \(row_count"):
        refresh(source, catalog, platform=strict, watermark=FLUSHED + timedelta(hours=1))
    table = catalog.load_table("gold.hourly_isp_quality")
    assert table.current_snapshot().snapshot_id == snapshot  # same data
    audit = json.loads(table.properties[LAST_AUDIT])  # but the blocked audit is on record
    assert (audit["published"], [c["passed"] for c in audit["checks"]]) == (False, [False])
    assert table.properties[ROLLUP_THROUGH] == (T0 + timedelta(hours=8)).isoformat()  # still the last publish


def test_failed_warn_checks_are_reported_but_publish(source, catalog):
    run = refresh(source, catalog)  # the sample rules warn when p99_ttfb exceeds 2000 ms, as degraded ISPs do
    failed = {r.check: r.severity for r in run.checks if not r.passed}
    assert set(failed.values()) <= {"warn"}
    assert gold_rows(catalog)


def test_nothing_is_done_before_the_first_flush(source, catalog):
    run = refresh(source, catalog, watermark=None)
    assert run.skipped == "nothing has been flushed to the historical tier yet"


def test_a_first_run_before_a_complete_hour_of_data_waits(catalog):
    """Data from a live feed that began at 08:10: by 08:30 no hour of it is complete."""
    engine = DuckDBEngine()
    events = generate_events(DATASET, T0 + timedelta(hours=8, minutes=10), FLUSHED, seed=3)
    engine.con.register("telemetry", pa.Table.from_pylist(events, schema=arrow_schema(DATASET)))
    run = refresh(engine, catalog)
    assert run.skipped == "no complete hour of data yet; the first run with one creates the table"
    assert not catalog.table_exists("gold.hourly_isp_quality")
    assert refresh(engine, catalog, watermark=T0 + timedelta(hours=9)).rows > 0  # 08:00 is complete now


def test_a_rollup_can_gain_a_metric(source, catalog):
    refresh(source, catalog)
    query = HOURLY.query.model_copy(update={"metrics": [*HOURLY.query.metrics, "rebuffer_event_count"]})
    grown = HOURLY.model_copy(update={"query": query})

    refresh(source, catalog, rollup=grown, watermark=FLUSHED + timedelta(hours=1))

    rows = gold_rows(catalog)
    assert rows[0]["rebuffer_event_count"] is None  # outside the recomputed window: written before the metric existed
    assert rows[-1]["rebuffer_event_count"] is not None


def test_rollup_store_reports_status_and_reads_rows(source, catalog):
    store = RollupStore(PLATFORM, catalog)
    assert [(s.table, s.complete_through, s.last_audit) for s in store.statuses()] == [
        ("gold.hourly_isp_quality", None, None),
        ("gold.daily_device_quality", None, None),
    ]
    refresh(source, catalog)

    [hourly, _] = store.statuses()
    assert (hourly.grain, hourly.complete_through, hourly.last_audit["published"]) == ("hour", T0 + timedelta(hours=8), True)
    assert hourly.columns[:2] == ["event_ts", "isp_provider"]
    rows = store.rows("gold.hourly_isp_quality", since=T0 + timedelta(hours=6)).to_pylist()
    assert [r["event_ts"] for r in rows[::6]] == [T0 + timedelta(hours=6), T0 + timedelta(hours=7)]  # 6 ISPs an hour
    assert [r["isp_provider"] for r in rows[:6]] == sorted(r["isp_provider"] for r in rows[:6])
    with pytest.raises(KeyError):
        store.rows("gold.nowhere", since=T0)
