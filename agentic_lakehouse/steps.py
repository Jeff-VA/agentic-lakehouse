"""Pipeline steps: SQL or Python transformations that each write one lakehouse table.

pipelines.yaml lists them. A step reads lakehouse tables (raw tables, rollups,
other steps' outputs) and writes one table. Each run of a step:

1. computes its rows: the step's SQL on the warehouse engine (Trino), or its
   Python function, which gets a StepContext to query tables with;
2. checks them: the columns pipelines.yaml says, then the table's quality
   rules in quality_rules.yaml, as rollups are checked;
3. publishes them, replacing the table or adding to it (`mode`), in one commit
   that also records the run (STEP_RUN). A failed `error` check publishes
   nothing, and the table keeps its last good contents.

Steps run in the order their reads require, so a step sees what the steps
before it wrote in the same pass, and each runs again once `every_minutes` have
passed. A machine-learning model fits this shape: a step that trains it writes
its parameters as a table, and whatever applies it (a processor, a view, the
next step) reads that table. No separate model store: the lakehouse versions
models like any data.
"""

import json
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Protocol

import pyarrow as pa
from pyiceberg.catalog import Catalog

from agentic_lakehouse.config import Platform, Step
from agentic_lakehouse.hooks import load
from agentic_lakehouse.quality import CheckResult, blocking, run_checks
from agentic_lakehouse.rollups import QualityGateError, _load

STEP_RUN = "agentic-lakehouse.step-run"  # JSON on the output table: the last run, and the last publish

_ARROW_TYPES = {
    "string": pa.string(),
    "int": pa.int32(),
    "long": pa.int64(),
    "double": pa.float64(),
    "boolean": pa.bool_(),
    "timestamp": pa.timestamp("us"),
}


class StepError(RuntimeError):
    """A step failed or returned something other than its configured columns."""


@dataclass
class StepContext:
    """What a Python step gets: when it runs, and SQL over the lakehouse's tables."""

    step: Step
    now: datetime  # naive UTC
    last_published: datetime | None  # when this step last published, naive UTC; None on its first run
    query: Callable[[str], pa.Table]  # a Trino SELECT over <namespace>.<table> names
    log: Callable[[str], None] = print

    def read(self, table: str, where: str | None = None) -> pa.Table:
        """All of `table`'s rows, or those matching a SQL condition."""
        return self.query(f"SELECT * FROM {table}" + (f" WHERE {where}" if where else ""))


class StepStore(Protocol):
    """Where step outputs live: Iceberg (IcebergStepStore), or DuckDB in agentic_lakehouse.testing."""

    def last_run(self, table: str) -> dict | None: ...
    def publish(self, step: Step, rows: pa.Table, record: dict) -> None: ...
    def record(self, step: Step, record: dict) -> None: ...  # a blocked run: the data stays as it was


@dataclass
class StepRun:
    step: str
    rows: int = 0
    checks: list[CheckResult] = field(default_factory=list)
    skipped: str | None = None  # why it didn't run


def due(step: Step, last: dict | None, now: datetime) -> bool:
    return last is None or now - datetime.fromisoformat(last["at"]) >= timedelta(minutes=step.every_minutes)


def run_step(
    platform: Platform,
    step: Step,
    *,
    query: Callable[[str], pa.Table],
    store: StepStore,
    now: datetime | None = None,
    log: Callable[[str], None] = print,
    force: bool = False,
    exists: Callable[[str], bool] = lambda table: True,
) -> StepRun:
    """Run `step` if it's due (or `force`) and the tables it reads exist (`exists`: a rollup or
    step that hasn't run yet has no table). A Python step may return None to publish nothing this
    run. Raises StepError, or QualityGateError if its rows fail a blocking check (nothing is
    published then)."""
    now = now or datetime.now(UTC).replace(tzinfo=None)
    last = store.last_run(step.output)
    if not force and not due(step, last, now):
        return StepRun(step.name, skipped=f"ran less than {step.every_minutes:g} minutes ago")
    if missing := sorted(table for table in step.inputs() if not exists(table)):
        return StepRun(step.name, skipped=f"waiting for {', '.join(missing)}, which doesn't exist yet")
    published = (last or {}).get("published_at")
    context = StepContext(step, now, datetime.fromisoformat(published) if published else None, query, log)
    if step.sql:
        rows = query(step.sql)
    else:
        rows = load(step.run)(context)
        if rows is None:  # nothing worth publishing yet, e.g. too little data to train on
            store.record(step, {"at": now.isoformat(timespec="seconds"), "published": False, "rows": 0, "checks": [], "published_at": published})
            log(f"step {step.name}: nothing to publish this run")
            return StepRun(step.name, skipped="returned nothing to publish")
        if not isinstance(rows, pa.Table):
            raise StepError(f"step '{step.name}': {step.run} returned {type(rows).__name__}, not a pyarrow.Table")
    rows = _conform(step, rows, platform.table_schemas()[step.output])
    checks = run_checks(rows, platform.quality.checks_for(step.output), time_column=step.time_column or "", window_end=now)
    record = {
        "at": now.isoformat(timespec="seconds"),
        "published": not blocking(checks),
        "rows": rows.num_rows,
        "checks": [asdict(c) for c in checks],
        "published_at": now.isoformat(timespec="seconds") if not blocking(checks) else published,
    }
    if blocking(checks):
        store.record(step, record)
        raise QualityGateError(step.output, checks)
    store.publish(step, rows, record)
    log(f"step {step.name}: {'replaced' if step.mode == 'replace' else 'added'} {rows.num_rows} rows in {step.output}")
    return StepRun(step.name, rows.num_rows, checks)


def _conform(step: Step, rows: pa.Table, columns: dict[str, str]) -> pa.Table:
    """The step's rows with its configured columns in order (and, for a Python step, their types)."""
    if set(rows.column_names) != set(columns):
        missing, extra = sorted(set(columns) - set(rows.column_names)), sorted(set(rows.column_names) - set(columns))
        raise StepError(
            f"step '{step.name}' returned the wrong columns"
            + (f"; missing {', '.join(missing)}" if missing else "")
            + (f"; not in pipelines.yaml: {', '.join(extra)}" if extra else "")
        )
    rows = rows.select(list(columns))
    if step.run:
        schema = pa.schema([(name, _ARROW_TYPES[step.columns[name]]) for name in columns])
        try:
            rows = rows.cast(schema)
        except (pa.ArrowInvalid, pa.ArrowNotImplementedError) as e:
            raise StepError(f"step '{step.name}' returned values that don't fit its columns: {e}") from None
    return rows


class IcebergStepStore:
    """Step outputs as Iceberg tables, each run recorded in the table's properties."""

    def __init__(self, catalog: Catalog | Callable[[], Catalog]):
        self._catalog = catalog

    @property
    def catalog(self) -> Catalog:
        if not isinstance(self._catalog, Catalog):
            self._catalog = self._catalog()  # created on first use
        return self._catalog

    def exists(self, table: str) -> bool:
        return _load(self.catalog, table) is not None

    def last_run(self, table: str) -> dict | None:
        found = _load(self.catalog, table)
        value = found.properties.get(STEP_RUN) if found else None
        return json.loads(value) if value else None

    def publish(self, step: Step, rows: pa.Table, record: dict) -> None:
        table = _load(self.catalog, step.output)
        if table is None:
            self.catalog.create_namespace_if_not_exists(step.output.split(".")[0])
            table = self.catalog.create_table(step.output, schema=rows.schema, properties={"comment": step.description})
        with table.transaction() as transaction:
            if set(rows.column_names) - {f.name for f in table.schema().fields}:
                with transaction.update_schema() as update:
                    update.union_by_name(rows.schema)
            if step.mode == "replace" and table.current_snapshot() is not None:
                transaction.overwrite(rows)
            else:
                transaction.append(rows)
            transaction.set_properties({STEP_RUN: json.dumps(record)})

    def record(self, step: Step, record: dict) -> None:
        table = _load(self.catalog, step.output)
        if table is not None:
            with table.transaction() as transaction:
                transaction.set_properties({STEP_RUN: json.dumps(record)})
