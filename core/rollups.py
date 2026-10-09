"""Rollups: semantic queries kept as gold Iceberg tables.

One run of one rollup:
1. Picks a window. It ends at the last complete time bucket before the source's
   flush watermark, so a published bucket never lacks events that merely hadn't
   reached Iceberg yet. It starts where the previous run stopped, or
   `lookback_hours` before the end if that's earlier (recounting late events),
   but never more than `backfill_days` back (what a first run covers).
2. Computes the rows: the rollup's semantic query, compiled for the engine
   that reads the historical tier.
3. Audits them against the table's quality rules (core/quality.py).
4. Publishes: replaces exactly that window in the gold table, in one Iceberg
   commit that also records how far the table is complete and the audit
   results. Re-running is idempotent; a failed audit publishes nothing.
"""

import json
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta

import pyarrow as pa
from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.table import Table
from pyiceberg.transforms import DayTransform, MonthTransform

from core.compiler import SqlCompiler, bucket_start
from core.config import Platform, Rollup
from core.engines import QueryEngine
from core.quality import CheckResult, blocking, run_checks

ROLLUP_THROUGH = "male.rollup-through"  # the gold table is complete before this time (ISO, UTC)
LAST_AUDIT = "male.last-audit"  # JSON results of the checks run before the last publish

Log = Callable[[str], None]


class QualityGateError(RuntimeError):
    """A rollup's new rows failed an `error` check, so nothing was published."""

    def __init__(self, table: str, results: list[CheckResult]):
        self.table, self.results = table, results
        failed = "; ".join(f"{r.check}: {r.detail}" for r in blocking(results))
        super().__init__(f"{table}: quality gate failed, nothing published ({failed})")


@dataclass
class RollupRun:
    table: str
    start: datetime | None = None  # the window replaced, [start, end)
    end: datetime | None = None
    rows: int = 0
    checks: list[CheckResult] = field(default_factory=list)
    skipped: str | None = None  # why nothing was done, if nothing was


def window(rollup: Rollup, watermark: datetime, previous_end: datetime | None) -> tuple[datetime, datetime] | None:
    """The [start, end) a run should recompute, or None if no complete bucket is due."""
    grain = rollup.query.grain
    end = bucket_start(watermark, grain)
    if previous_end is None:
        start = end - timedelta(days=rollup.backfill_days)
    elif end <= previous_end:
        return None  # no bucket has completed since the last run
    else:
        start = min(end - timedelta(hours=rollup.lookback_hours), previous_end)
    start = bucket_start(max(start, end - timedelta(days=rollup.backfill_days)), grain)
    return (start, end) if start < end else None


def materialize(
    rollup: Rollup,
    platform: Platform,
    *,
    engine: QueryEngine,
    source_table: str,
    catalog: Catalog,
    watermark: datetime | None,
    log: Log = print,
) -> RollupRun:
    """Refresh one rollup. `engine` reads `source_table`, the historical tier's
    table for the rollup's dataset; `watermark` is that table's flush watermark
    (naive UTC). Raises QualityGateError if the new rows fail a blocking check."""
    run = RollupRun(rollup.table)
    if watermark is None:
        run.skipped = "nothing has been flushed to the historical tier yet"
        return run
    gold = _load(catalog, rollup.table)
    previous_end = gold.properties.get(ROLLUP_THROUGH) if gold else None
    span = window(rollup, watermark, datetime.fromisoformat(previous_end) if previous_end else None)
    if span is None:
        run.skipped = f"no complete {rollup.query.grain} since the last run"
        return run
    run.start, run.end = span

    model = platform.rollup_model(rollup)
    sql = SqlCompiler(platform.ontology).compile(rollup.with_window(*span), engine=engine.name, table=source_table)
    rows = engine.execute(sql)
    run.rows = rows.num_rows
    run.checks = run_checks(
        rows, platform.quality.checks_for(rollup.table), time_column=model.time_dimension, window_end=run.end
    )
    if blocking(run.checks):
        raise QualityGateError(rollup.table, run.checks)

    gold = gold or _create(catalog, rollup, rows.schema, model.time_dimension)
    time = model.time_dimension
    with gold.transaction() as transaction:
        if set(rows.column_names) - {f.name for f in gold.schema().fields}:  # the rollup gained a metric or dimension
            with transaction.update_schema() as update:
                update.union_by_name(rows.schema)
        if gold.current_snapshot() is None:  # a new table: nothing to replace
            transaction.append(rows)
        else:
            transaction.overwrite(
                rows, overwrite_filter=f"{time} >= '{run.start.isoformat()}' AND {time} < '{run.end.isoformat()}'"
            )
        transaction.set_properties(
            {ROLLUP_THROUGH: run.end.isoformat(), LAST_AUDIT: json.dumps([asdict(r) for r in run.checks])}
        )
    log(f"rollup {rollup.table}: replaced {run.start:%Y-%m-%d %H:%M} to {run.end:%Y-%m-%d %H:%M} UTC with {run.rows} rows")
    return run


def _load(catalog: Catalog, name: str) -> Table | None:
    try:
        return catalog.load_table(name)
    except NoSuchTableError:
        return None


def _create(catalog: Catalog, rollup: Rollup, schema: pa.Schema, time_column: str) -> Table:
    catalog.create_namespace_if_not_exists(rollup.table.split(".")[0])
    table = catalog.create_table(rollup.table, schema=schema, properties={"comment": rollup.description})
    fine = rollup.query.grain in ("minute", "hour")  # many rows per day: partition by day, else by month
    with table.update_spec() as spec:
        spec.add_field(time_column, DayTransform() if fine else MonthTransform(), f"{time_column}_{'day' if fine else 'month'}")
    return table
