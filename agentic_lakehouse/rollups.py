"""Rollups: semantic queries kept as gold Iceberg tables.

One run of one rollup:
1. Picks a window. It ends at the last complete time bucket before the source's
   flush watermark, so a published bucket never lacks events that merely hadn't
   reached Iceberg yet. A first run that finds no rows (data that began
   mid-bucket) waits for the next run instead of creating an empty table. It starts where the previous run stopped, or
   `lookback_hours` before the end if that's earlier (recounting late events),
   but never more than `backfill_days` back (what a first run covers).
2. Computes the rows: the rollup's semantic query, compiled for the engine
   that reads the historical tier.
3. Audits them against the table's quality rules (agentic_lakehouse/quality.py).
4. Publishes: replaces exactly that window in the gold table, in one Iceberg
   commit that also records how far the table is complete and the audit
   results. Re-running is idempotent; a failed audit publishes nothing.
"""

import json
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime, timedelta

import pyarrow as pa
from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.expressions import GreaterThanOrEqual
from pyiceberg.table import Table
from pyiceberg.transforms import DayTransform, MonthTransform

from agentic_lakehouse.compiler import SqlCompiler, bucket_start
from agentic_lakehouse.config import Platform, Rollup
from agentic_lakehouse.engines import QueryEngine
from agentic_lakehouse.lookups import read_lookups
from agentic_lakehouse.quality import CheckResult, blocking, run_checks
from agentic_lakehouse.views import qualify_tables

ROLLUP_THROUGH = "agentic-lakehouse.rollup-through"  # the gold table is complete before this time (ISO, UTC)
LAST_AUDIT = "agentic-lakehouse.last-audit"  # JSON: when the last run audited its rows, whether it published, the check results

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
    ontology = platform.ontology
    if needed := _table_lookups(rollup, model, ontology):  # read them first: without rows, their attributes are empty
        catalog_name = platform.deployment.trino.catalog if engine.name == "trino" else None
        ontology, errors = read_lookups(ontology, lambda sql: engine.execute(qualify_tables(sql, catalog_name, dialect=engine.name)), needed)
        if errors:
            run.skipped = f"{'; '.join(errors)}: skipped rather than written with empty attributes"
            return run
    sql = SqlCompiler(ontology).compile(rollup.with_window(*span), engine=engine.name, table=source_table)
    rows = engine.execute(sql)
    run.rows = rows.num_rows
    if gold is None and not run.rows:  # a live feed that started mid-bucket: wait for a complete one
        run.skipped = f"no complete {rollup.query.grain} of data yet; the first run with one creates the table"
        return run
    run.checks = run_checks(
        rows, platform.quality.checks_for(rollup.table), time_column=model.time_dimension, window_end=run.end
    )
    if blocking(run.checks):
        if gold is not None:  # record the blocked audit; the data stays as it was
            with gold.transaction() as transaction:
                transaction.set_properties({LAST_AUDIT: _audit(run, published=False)})
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
        transaction.set_properties({ROLLUP_THROUGH: run.end.isoformat(), LAST_AUDIT: _audit(run, published=True)})
    log(f"rollup {rollup.table}: replaced {run.start:%Y-%m-%d %H:%M} to {run.end:%Y-%m-%d %H:%M} UTC with {run.rows} rows")
    return run


def _audit(run: RollupRun, *, published: bool) -> str:
    return json.dumps(
        {
            "at": datetime.now(UTC).replace(tzinfo=None).isoformat(timespec="seconds"),
            "published": published,
            "start": run.start.isoformat(),
            "end": run.end.isoformat(),
            "checks": [asdict(r) for r in run.checks],
        }
    )


@dataclass
class RollupStatus:
    table: str
    description: str
    grain: str
    columns: list[str]
    complete_through: datetime | None  # None until the first publish
    last_audit: dict | None  # see _audit


class RollupStore:
    """Read side of the gold tables, for the gateway: their status and rows.

    `catalog` may be a function that creates it, called on first use, so the
    gateway starts even while the Iceberg catalog is unreachable.
    """

    def __init__(self, platform: Platform, catalog: Catalog | Callable[[], Catalog]):
        self.platform = platform
        self._catalog = catalog

    @property
    def catalog(self) -> Catalog:
        if not isinstance(self._catalog, Catalog):
            self._catalog = self._catalog()
        return self._catalog

    def statuses(self) -> list[RollupStatus]:
        statuses = []
        for rollup in self.platform.storage.rollups:
            gold = _load(self.catalog, rollup.table)
            properties = gold.properties if gold else {}
            through = properties.get(ROLLUP_THROUGH)
            audit = properties.get(LAST_AUDIT)
            statuses.append(
                RollupStatus(
                    table=rollup.table,
                    description=rollup.description,
                    grain=rollup.query.grain,
                    columns=self.platform.rollup_columns(rollup),
                    complete_through=datetime.fromisoformat(through) if through else None,
                    last_audit=json.loads(audit) if audit else None,
                )
            )
        return statuses

    def rows(self, table: str, since: datetime) -> pa.Table:
        """The rollup's rows from `since` (naive UTC) on, oldest first. Raises KeyError for an unknown rollup."""
        rollup = next((r for r in self.platform.storage.rollups if r.table == table), None)
        if rollup is None:
            raise KeyError(table)
        time = self.platform.rollup_model(rollup).time_dimension
        gold = _load(self.catalog, table)
        if gold is None:
            return pa.table({name: pa.array([], pa.null()) for name in self.platform.rollup_columns(rollup)})
        rows = gold.scan(row_filter=GreaterThanOrEqual(time, since.isoformat())).to_arrow()
        return rows.sort_by([(time, "ascending"), *((d, "ascending") for d in rollup.query.dimensions)])


def _table_lookups(rollup: Rollup, model, ontology) -> set[str]:
    """The lookups read from tables whose attributes `rollup` groups or filters by."""
    names = {*rollup.query.dimensions, *(f.dimension for f in rollup.query.filters)}
    used = {d.lookup for d in model.dimensions if d.name in names and d.lookup}
    return {lookup.name for lookup in ontology.lookups if lookup.name in used and lookup.table}


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
