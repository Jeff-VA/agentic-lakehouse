"""The gateway clients use to ask the semantic layer for metrics.

The same operations are served over REST and over the Model Context Protocol
(MCP), so the dashboard and AI agents get identical answers:

- catalog: the metrics and dimensions you can ask for, and what they mean
- query: a MetricQuery, answered by the tier router
- views: the project's named queries (views/*.sql, see agentic_lakehouse/views.py), which
  dashboards read by name with parameters
- status: how fresh each tier is, each rollup's last quality gate, and drift
  proposals waiting for review: whether the data can be trusted right now

REST:  GET /catalog, POST /query, GET /views, GET /views/{name}?<params>,
       GET /status, GET /rollups/{table} (interactive docs at /docs)
Live:  GET /views/{name}/live?<params>, a Server-Sent Events stream of the
       view's result each time it changes, and GET /live?view=<name>&view=...
       for several views over one connection (agentic_lakehouse/live.py)
Dashboards: the project's dashboards/*.html, at /d/<name> (and / for the only one);
       they draw views with the chart tags in /al.js (agentic_lakehouse/static/al.js)
MCP:   Streamable HTTP at /mcp, or stdio via `agentic-lakehouse mcp`. Given the project
       (Project), it also checks and screenshots dashboards for the agent
       building them, and open pages reload when a view or dashboard file changes.
"""

import asyncio
import contextlib
import html
import logging
import math
import re
import uuid
from collections.abc import AsyncIterator, Callable, Mapping
from urllib.parse import parse_qsl
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Annotated, Any, Literal, get_args

import pyarrow as pa
from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from fastapi.sse import EventSourceResponse, ServerSentEvent
from mcp.server.mcpserver import MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from mcp.server.mcpserver.utilities.types import Image
from mcp_types import TextContent, ToolAnnotations
from pydantic import BaseModel, Field, ValidationError
from pyiceberg.exceptions import NoSuchTableError

from agentic_lakehouse import compose
from agentic_lakehouse.agent_sync import pending_proposals, unmodeled_columns
from agentic_lakehouse.browser import BrowserError, Capture, capture, serving
from agentic_lakehouse.compiler import Filter, Grain, MetricQuery, OrderBy, QueryError
from agentic_lakehouse.config import ConfigError, Platform, load_platform
from agentic_lakehouse.dashboards import check_dashboards, dashboard_files, describe_dashboard
from agentic_lakehouse.engines import EngineError, QueryEngine, TrinoEngine
from agentic_lakehouse.live import LiveViews
from agentic_lakehouse.ontology import AggregateMetric, Metric, SemanticModel, display_format, words
from agentic_lakehouse.orchestrator import FLUSHED_THROUGH, iceberg_catalog
from agentic_lakehouse.rollups import ROLLUP_THROUGH, RollupStore
from agentic_lakehouse.router import QueryResult, Router
from agentic_lakehouse.steps import IcebergStepStore
from agentic_lakehouse.views import ComposedSource, ModelSource, View, ViewColumn, ViewError

MAX_ROWS = 5000  # protects clients, and agents' context windows, from runaway results
STATIC = Path(__file__).parent / "static"  # al.js, the chart tags dashboards use
BOOT = uuid.uuid4().hex  # this server process: a page that sees another one reloads

log = logging.getLogger(__name__)


class DimensionInfo(BaseModel):
    name: str
    type: str
    description: str
    label: str


class MetricInfo(BaseModel):
    name: str
    description: str
    calculation: str  # e.g. "99th percentile of request_latency_ms where stream_start_event = TRUE"
    label: str
    format: str  # number, integer or percent (0.05 reads as 5%)
    unit: str
    better: str | None  # "higher" or "lower": which way a change is good news


class ModelInfo(BaseModel):
    name: str
    description: str
    time_dimension: str  # group by it with `grain`; bound it with start/end
    grains: list[str]
    dimensions: list[DimensionInfo]
    metrics: list[MetricInfo]
    realtime_days: float  # the real-time tier keeps this much recent data
    historical_through: datetime | None  # the historical tier is complete up to here


class Catalog(BaseModel):
    now: datetime  # server time, for choosing time ranges
    models: list[ModelInfo]


class ResultColumn(BaseModel):
    name: str
    role: Literal["time", "dimension", "metric"]


class Coverage(BaseModel):
    start: datetime
    end: datetime


class QueryResponse(BaseModel):
    columns: list[ResultColumn]
    rows: list[dict[str, Any]]
    coverage: Coverage  # the time range the rows cover; can be narrower than requested (see notes)
    notes: list[str]
    truncated: bool  # more rows matched than were returned
    sql: dict[str, str] | None = None  # per tier, when requested


class DatasetStatus(BaseModel):
    name: str
    realtime_days: float
    historical_through: datetime | None  # the flush watermark
    unmodeled_columns: list[str] = []  # fields producers send that the config doesn't describe yet


class CheckInfo(BaseModel):
    check: str
    severity: Literal["error", "warn"]
    passed: bool
    detail: str


class Audit(BaseModel):
    at: datetime
    published: bool  # false: an error check failed and the rows were held back
    start: datetime  # the window the run recomputed
    end: datetime
    checks: list[CheckInfo]


class RollupInfo(BaseModel):
    table: str
    description: str
    grain: str
    columns: list[str]
    complete_through: datetime | None  # None until the first publish
    last_audit: Audit | None


class StepRunInfo(BaseModel):
    at: datetime  # when the step last ran
    published: bool  # false: an error check failed and its rows were held back
    rows: int
    checks: list[CheckInfo]
    published_at: datetime | None  # when it last published


class StepInfo(BaseModel):
    name: str
    description: str
    table: str  # the table it writes
    last_run: StepRunInfo | None  # None until it first runs


class DriftProposalInfo(BaseModel):
    file: str
    dataset: str
    summary: str
    created: datetime


class Status(BaseModel):
    now: datetime
    datasets: list[DatasetStatus]
    rollups: list[RollupInfo]
    steps: list[StepInfo] = []  # pipelines.yaml's steps, in the order they run
    drift_proposals: list[DriftProposalInfo]  # config patches waiting for review


class RollupRows(BaseModel):
    table: str
    rows: list[dict[str, Any]]


class ViewParamInfo(BaseModel):
    name: str
    type: str
    description: str
    required: bool
    default: Any = None
    min: float | None = None
    max: float | None = None
    values: list[Any] | None = None  # the only values allowed


class ViewInfo(BaseModel):
    name: str
    description: str
    # "model": answered by the tier router, up to the second. "tables": SQL over Iceberg
    # tables, complete up to their last flush or rollup run. "composed": semantic queries and
    # table queries (stage 1) combined by SQL over their results (stage 2); see docs/design/sql-access.md.
    source: Literal["model", "tables", "composed"]
    reads: list[str]  # the model, or the tables
    params: list[ViewParamInfo]
    columns: list[ViewColumn]
    sql: str
    refresh_seconds: float | None  # how often its live feed rechecks, if the view sets it


class ViewCatalog(BaseModel):
    views: list[ViewInfo]


class PartInfo(BaseModel):
    """One stage-1 part of a composed view: what it read and what its rows cover."""

    source: Literal["model", "tables"]
    rows: int
    coverage: Coverage | None  # a model part: the time range its rows cover
    data_through: datetime | None  # a table part: its tables hold every row before this time
    notes: list[str]


class ViewResult(BaseModel):
    view: str
    params: dict[str, Any]  # every parameter's value, defaults filled in
    columns: list[ViewColumn]
    rows: list[dict[str, Any]]
    coverage: Coverage | None  # views over a model: the time range the rows cover (see notes)
    data_through: datetime | None  # views over tables: they hold every row before this time
    notes: list[str]
    truncated: bool  # more rows matched than were returned
    computed_at: datetime
    sql: dict[str, str] | None = None  # what each engine ran, when requested
    parts: dict[str, PartInfo] | None = None  # a composed view's stage-1 parts, by CTE name


class DashboardInfo(BaseModel):
    name: str
    title: str
    url: str  # path on the gateway, e.g. /d/playback
    views: list[str]  # the views its tags draw
    problems: list[str]  # "file:line: <tag> what's wrong"


class ProjectCheck(BaseModel):
    config_error: str | None  # the config or a view doesn't load; until it does, nothing else is checked
    views: list[str]
    dashboards: list[DashboardInfo]

    @property
    def ok(self) -> bool:
        return self.config_error is None and not any(d.problems for d in self.dashboards)


@dataclass
class Project:
    """The project's config folder, read afresh on every call: an agent edits views and
    dashboards while the server runs. `gateway` builds the gateway a screenshot renders
    against (by default Gateway.from_platform, the real stack)."""

    config_dir: Path
    gateway: Callable[[Platform], "Gateway"] | None = None

    @property
    def dashboards_dir(self) -> Path:
        return self.config_dir / "dashboards"

    def check(self) -> ProjectCheck:
        """The config and views load, and every dashboard's tags fit the views they draw."""
        files = dashboard_files(self.dashboards_dir)

        def info(name: str, path: Path, problems: list[str]) -> DashboardInfo:
            title, views = describe_dashboard(path)
            return DashboardInfo(name=name, title=title, url=f"/d/{name}", views=views, problems=problems)

        try:
            platform = load_platform(self.config_dir)
        except (ConfigError, ValidationError, OSError) as e:
            return ProjectCheck(config_error=str(e), views=[], dashboards=[info(n, p, []) for n, p in files.items()])
        datasets = [d.name for d in platform.ingestion.datasets]
        problems = check_dashboards(self.dashboards_dir, platform.views, datasets)
        dashboards = [
            info(name, path, [str(p) for p in problems if p.file == f"{path.parent.name}/{path.name}"]) for name, path in files.items()
        ]
        return ProjectCheck(config_error=None, views=list(platform.views), dashboards=dashboards)

    def screenshot(self, name: str, *, theme: str = "light", width: int = 1280, timeout: float = 30) -> Capture:
        """Dashboard `name` in headless Chrome, served from the files as they are now. Raises KeyError
        for an unknown dashboard, ConfigError if the config doesn't load, BrowserError."""
        platform = load_platform(self.config_dir)
        if name not in dashboard_files(self.dashboards_dir):
            raise KeyError(name)
        gateway = (self.gateway or Gateway.from_platform)(platform)
        with serving(create_app(gateway, dashboards_dir=self.dashboards_dir)) as url:
            return capture(f"{url}/d/{name}", theme=theme, width=width, timeout=timeout)


@dataclass(frozen=True)
class TableState:
    exists: bool
    complete_through: datetime | None = None  # the table holds every row before this time (naive UTC)
    snapshot: int | None = None  # the current Iceberg snapshot: it changes with every commit of data


@dataclass(frozen=True)
class Warehouse:
    """Where views over tables run (Trino over Iceberg, in the platform)."""

    engine: QueryEngine
    catalog: str | None = None  # qualifies <namespace>.<table>, e.g. "iceberg"
    table_state: Callable[[str], TableState] = lambda table: TableState(exists=True)


class Gateway:
    """The gateway's operations, independent of the transport serving them.

    `views` (by name) and the `warehouse` that runs views over tables are
    optional, and so are `rollups`, `proposals_dir` and `schema_drift` (dataset
    name -> columns the config doesn't describe); without them the status
    leaves out rollups, drift proposals and unmodeled columns.
    """

    part_max_rows = 100_000  # a composed view's stage-1 part may hold this many rows, for stage 2 to work on
    stage2_timeout_seconds = 30.0  # how long a composed view's stage 2 may run

    def __init__(
        self,
        router: Router,
        *,
        views: Mapping[str, View] | None = None,
        warehouse: Warehouse | None = None,
        rollups: RollupStore | None = None,
        proposals_dir: str | Path | None = None,
        schema_drift: Callable[[str], list[str]] | None = None,
        step_runs: Callable[[], list[StepInfo]] | None = None,
        max_rows: int = MAX_ROWS,
    ):
        self.router = router
        self.views = dict(views or {})
        self.warehouse = warehouse
        self.rollups = rollups
        self.proposals_dir = Path(proposals_dir) if proposals_dir else None
        self.schema_drift = schema_drift
        self.step_runs = step_runs
        self.max_rows = max_rows

    @classmethod
    def from_platform(cls, platform: Platform, proposals_dir: str | Path = "proposals") -> "Gateway":
        rollups = RollupStore(platform, lambda: iceberg_catalog(platform))  # its catalog is created on first use

        def schema_drift(dataset: str) -> list[str]:
            table = rollups.catalog.load_table(platform.iceberg_table(dataset).table)
            return unmodeled_columns(platform.dataset(dataset), table)

        def table_state(name: str) -> TableState:
            try:
                table = rollups.catalog.load_table(name)
            except NoSuchTableError:
                return TableState(exists=False)
            except Exception as e:  # e.g. the catalog is unreachable: a failing engine, like Trino being down
                raise EngineError(f"iceberg catalog: {e}") from e
            through = table.properties.get(FLUSHED_THROUGH) or table.properties.get(ROLLUP_THROUGH)
            snapshot = table.current_snapshot()
            return TableState(
                exists=True,
                complete_through=datetime.fromisoformat(through) if through else None,
                snapshot=snapshot.snapshot_id if snapshot else None,
            )

        warehouse = Warehouse(TrinoEngine(platform.deployment.trino), platform.deployment.trino.catalog, table_state)
        store = IcebergStepStore(lambda: rollups.catalog)

        def step_runs() -> list[StepInfo]:
            return [
                StepInfo(name=s.name, description=s.description, table=s.output, last_run=_step_run(store.last_run(s.output)))
                for s in platform.ordered_steps()
            ]

        return cls(
            Router.from_platform(platform),
            views=platform.views,
            warehouse=warehouse,
            rollups=rollups,
            proposals_dir=proposals_dir,
            schema_drift=schema_drift,
            step_runs=step_runs,
        )

    def status(self) -> Status:
        datasets = []
        for name, placement in self.router.placements.items():
            flushed = placement.flushed_through()
            datasets.append(
                DatasetStatus(
                    name=name,
                    realtime_days=placement.retention.total_seconds() / 86400,
                    historical_through=_utc(flushed) if flushed else None,
                    unmodeled_columns=self.schema_drift(name) if self.schema_drift else [],
                )
            )
        rollups = []
        for status in self.rollups.statuses() if self.rollups else []:
            audit = status.last_audit
            rollups.append(
                RollupInfo(
                    table=status.table,
                    description=status.description,
                    grain=status.grain,
                    columns=status.columns,
                    complete_through=_utc(status.complete_through) if status.complete_through else None,
                    last_audit=None
                    if audit is None
                    else Audit(**audit | {key: _utc(datetime.fromisoformat(audit[key])) for key in ("at", "start", "end")}),
                )
            )
        proposals = pending_proposals(self.proposals_dir) if self.proposals_dir and self.proposals_dir.is_dir() else []
        return Status(
            now=datetime.now(UTC),
            datasets=datasets,
            rollups=rollups,
            steps=self.step_runs() if self.step_runs else [],
            drift_proposals=[DriftProposalInfo(**vars(p)) for p in proposals],
        )

    def rollup_rows(self, table: str, hours: float) -> RollupRows:
        """A rollup's rows from the last `hours`. Raises KeyError for an unknown rollup."""
        if self.rollups is None:
            raise KeyError(table)
        since = datetime.now(UTC).replace(tzinfo=None) - timedelta(hours=hours)
        rows = self.rollups.rows(table, since).to_pylist()
        return RollupRows(table=table, rows=[{k: _utc(v) if isinstance(v, datetime) else v for k, v in r.items()} for r in rows])

    def catalog(self) -> Catalog:
        models = []
        for model in self.router.compiler.ontology.models:
            placement = self.router.placements.get(model.source)
            if placement is None:  # no tier holds its dataset, so it can't be queried
                continue
            flushed = placement.flushed_through()
            models.append(
                ModelInfo(
                    name=model.name,
                    description=model.description,
                    time_dimension=model.time_dimension,
                    grains=list(get_args(Grain)),
                    dimensions=[
                        DimensionInfo(name=d.name, type=d.type, description=d.description, label=d.label or words(d.name))
                        for d in model.dimensions
                        if d.name != model.time_dimension
                    ],
                    metrics=[
                        MetricInfo(
                            name=m.name,
                            description=m.description,
                            calculation=_calculation(m, model),
                            label=m.label or words(m.name),
                            format=display_format(m),
                            unit=m.unit,
                            better=m.better,
                        )
                        for m in model.metrics
                    ],
                    realtime_days=placement.retention.total_seconds() / 86400,
                    historical_through=_utc(flushed) if flushed else None,
                )
            )
        return Catalog(now=datetime.now(UTC), models=models)

    def query(self, query: MetricQuery, include_sql: bool = False) -> QueryResponse:
        """Run `query`. Raises QueryError if it doesn't fit the ontology, EngineError if an engine fails."""
        result, rows, notes, truncated = self._run(
            query, "narrow the query with filters, a coarser grain or fewer dimensions, or set order_by and limit for a top-N"
        )
        model = self.router.compiler.model_for(query)
        columns = [ResultColumn(name=model.time_dimension, role="time")] if query.grain else []
        columns += [ResultColumn(name=d, role="dimension") for d in query.dimensions]
        columns += [ResultColumn(name=m, role="metric") for m in query.metrics]
        return QueryResponse(
            columns=columns,
            rows=rows,
            coverage=Coverage(start=result.start, end=result.end),
            notes=notes,
            truncated=truncated,
            sql=result.sql if include_sql else None,
        )

    def view_catalog(self) -> ViewCatalog:
        return ViewCatalog(views=[_view_info(view) for view in self.views.values()])

    def view(self, name: str, params: Mapping[str, Any] | None = None, include_sql: bool = False) -> ViewResult:
        """Run view `name` with `params` (missing ones take their defaults).

        Raises KeyError for an unknown view, ViewError for bad parameters, and
        QueryError or EngineError like a query.
        """
        return self.run_view(self.views[name], params, include_sql)

    def run_view(
        self, view: View, params: Mapping[str, Any] | None = None, include_sql: bool = False, max_rows: int | None = None
    ) -> ViewResult:
        """Run `view`, a project view or one parsed from ad-hoc SQL, returning at most `max_rows`
        rows (default: the gateway's max_rows). Raises like view()."""
        max_rows = max_rows or self.max_rows
        name = view.name
        values = view.bind(params or {})
        computed_at = datetime.now(UTC)
        advice = "narrow the view with its parameters, or give it a LIMIT"
        if isinstance(view.source, ComposedSource):
            return self._run_composed(view, values, include_sql, max_rows, computed_at)
        if isinstance(view.source, ModelSource):
            query = view.source.query(values, now=_utc(self.router.clock()))
            result, rows, notes, truncated = self._run(query, advice, max_rows)
            return ViewResult(
                view=name,
                params=values,
                columns=list(view.columns),
                rows=[{o.name: row[o.field] for o in view.source.outputs} for row in rows],
                coverage=Coverage(start=result.start, end=result.end),
                data_through=None,
                notes=notes,
                truncated=truncated,
                computed_at=computed_at,
                sql=result.sql if include_sql else None,
            )

        if self.warehouse is None:
            raise ViewError(f"view '{name}' reads tables, and this gateway has no warehouse engine to run it on")
        states = {table: self.warehouse.table_state(table) for table in view.source.tables}
        engine = self.warehouse.engine
        sql = view.source.sql(values, dialect=engine.name, catalog=self.warehouse.catalog, limit=max_rows + 1)
        notes = []
        if missing := [table for table, state in states.items() if not state.exists]:
            rows = []  # e.g. a rollup that hasn't run yet; its first run creates the table
            notes.append(f"{', '.join(missing)} doesn't exist yet, so there are no rows")
        else:
            rows = _rows(engine.execute(sql))
        truncated = len(rows) > max_rows
        if truncated:
            rows = rows[:max_rows]
            notes.append(f"Only the first {max_rows} rows are returned; {advice}.")
        through = [state.complete_through for state in states.values()]
        return ViewResult(
            view=name,
            params=values,
            columns=list(view.columns),
            rows=rows,
            coverage=None,
            data_through=_utc(min(through)) if through and None not in through else None,
            notes=notes,
            truncated=truncated,
            computed_at=computed_at,
            sql={"warehouse": sql} if include_sql else None,
        )

    def _run_composed(
        self, view: View, values: dict[str, Any], include_sql: bool, max_rows: int, computed_at: datetime
    ) -> ViewResult:
        """A composed view: answer each stage-1 part in full, then run stage 2 over the results."""
        source = view.source
        now = _utc(self.router.clock())
        tables, parts, sql = {}, {}, {}
        for name, part in source.parts.items():
            if isinstance(part, ModelSource):
                query = part.query(values, now=now)
                if query.limit is None or query.limit > self.part_max_rows:
                    query = query.model_copy(update={"limit": self.part_max_rows + 1})
                result = self.router.run(query)
                fields = [output.field for output in part.outputs]
                table = result.table.select(fields).rename_columns([output.name for output in part.outputs])
                coverage = Coverage(start=result.start, end=result.end)
                parts[name] = PartInfo(source="model", rows=table.num_rows, coverage=coverage, data_through=None, notes=list(result.notes))
                sql |= {f"{name}: {engine}": text for engine, text in result.sql.items()}
            else:
                if self.warehouse is None:
                    raise ViewError(f"view '{view.name}' reads tables in part {name}, and this gateway has no warehouse engine")
                states = {t: self.warehouse.table_state(t) for t in part.tables}
                engine = self.warehouse.engine
                text = part.sql(values, dialect=engine.name, catalog=self.warehouse.catalog, limit=self.part_max_rows + 1)
                notes = []
                if missing := [t for t, state in states.items() if not state.exists]:
                    table = compose.empty([(c.name, c.type) for c in source.part_columns[name]])
                    notes.append(f"{', '.join(missing)} doesn't exist yet, so there are no rows")
                else:
                    table = engine.execute(text)
                through = [state.complete_through for state in states.values()]
                data_through = _utc(min(through)) if through and None not in through else None
                parts[name] = PartInfo(source="tables", rows=table.num_rows, coverage=None, data_through=data_through, notes=notes)
                sql[f"{name}: {engine.name}"] = text
            if table.num_rows > self.part_max_rows:
                raise ViewError(
                    f"part {name} of view '{view.name}' has more than {self.part_max_rows:,} rows, too many to combine; "
                    "narrow it with a filter, a coarser grain or a LIMIT"
                )
            tables[name] = table

        stage2 = source.stage2_sql(values)
        try:
            result = compose.run(tables, stage2, timeout=self.stage2_timeout_seconds)
        except compose.StageTwoError as e:
            raise ViewError(f"view '{view.name}', after the WITH: {e}") from None
        rows = _rows(result)
        notes = [f"{name}: {note}" for name, info in parts.items() for note in info.notes]
        truncated = len(rows) > max_rows
        if truncated:
            rows = rows[:max_rows]
            notes.append(f"Only the first {max_rows} rows are returned; narrow the view with its parameters, or give it a LIMIT.")
        coverages = [info.coverage for info in parts.values() if info.coverage]
        throughs = [info.data_through for info in parts.values() if info.source == "tables"]
        return ViewResult(
            view=view.name,
            params=values,
            columns=list(view.columns),
            rows=rows,
            coverage=Coverage(start=min(c.start for c in coverages), end=max(c.end for c in coverages)) if coverages else None,
            data_through=min(throughs) if throughs and None not in throughs else None,
            notes=notes,
            truncated=truncated,
            computed_at=computed_at,
            sql=sql | {"stage 2": stage2} if include_sql else None,
            parts=parts,
        )

    def _run(
        self, query: MetricQuery, advice: str, max_rows: int | None = None
    ) -> tuple[QueryResult, list[dict[str, Any]], list[str], bool]:
        """Route `query`, returning at most `max_rows` rows (default: the gateway's) and notes on
        what the result covers."""
        max_rows = max_rows or self.max_rows
        capped = query.limit is None or query.limit > max_rows
        if capped:  # fetch one extra row to tell whether anything was cut off
            query = query.model_copy(update={"limit": max_rows + 1})
        result = self.router.run(query)
        rows = _rows(result.table)
        notes = list(result.notes)
        truncated = capped and len(rows) > max_rows
        if truncated:
            rows = rows[:max_rows]
            notes.append(f"Only the first {max_rows} rows are returned; {advice}.")
        return result, rows, notes, truncated


def _step_run(record: dict | None) -> StepRunInfo | None:
    if record is None:
        return None
    published = record.get("published_at")
    return StepRunInfo(
        at=_utc(datetime.fromisoformat(record["at"])),
        published=record["published"],
        rows=record["rows"],
        checks=record["checks"],
        published_at=_utc(datetime.fromisoformat(published)) if published else None,
    )


def _utc(ts: datetime) -> datetime:
    # Engines return naive timestamps; every time in the platform is UTC.
    return ts.replace(tzinfo=UTC) if ts.tzinfo is None else ts


def _rows(table: pa.Table) -> list[dict[str, Any]]:
    """Result rows ready for JSON: times in UTC, NaN and infinity as null."""

    def plain(value: Any) -> Any:
        if isinstance(value, datetime):
            return _utc(value)
        if isinstance(value, float) and not math.isfinite(value):
            return None
        return value

    return [{key: plain(value) for key, value in row.items()} for row in table.to_pylist()]


def _view_info(view: View) -> ViewInfo:
    return ViewInfo(
        name=view.name,
        description=view.description,
        source="composed" if isinstance(view.source, ComposedSource) else "model" if isinstance(view.source, ModelSource) else "tables",
        reads=view.reads,
        params=[
            ViewParamInfo(
                name=name,
                type=param.type,
                description=param.description,
                required=param.required,
                default=param.default,
                min=param.min,
                max=param.max,
                values=param.values,
            )
            for name, param in view.params.items()
        ],
        columns=list(view.columns),
        sql=view.sql,
        refresh_seconds=view.refresh_seconds,
    )


def _calculation(metric: Metric, model: SemanticModel) -> str:
    """How a metric is computed, in words, so a reader (or an agent) can judge if it fits a question."""
    if not isinstance(metric, AggregateMetric):
        return f"ratio: {metric.numerator} / {metric.denominator}"
    of = f" of {metric.sql}" if metric.sql else ""
    if metric.agg == "percentile":
        text = f"{metric.percentile:.4g} quantile{of} (approximate on Trino and Pinot)"
    elif metric.agg == "count" and not metric.sql:
        text = f"count of {model.name} events"
    else:
        names = {"count": "count", "count_distinct": "number of distinct values", "sum": "sum", "avg": "average"}
        text = names.get(metric.agg, metric.agg) + of  # min and max read as themselves
    if metric.filters:
        text += " where " + " AND ".join(metric.filters)
    return text


def _tag_reference() -> str:
    """The tag reference at the top of al.js, for agents writing dashboards."""
    text = (STATIC / "al.js").read_text()
    comment = text[text.index("/*") + 2 : text.index("*/")]
    return "\n".join(line.removeprefix(" *").removeprefix(" ") for line in comment.splitlines()).strip()


DASHBOARD_GUIDE = """Goal: {goal}

You build dashboards for this lakehouse as files in the project's config folder ({config_dir}):
views in views/<name>.sql, pages in dashboards/<name>.html. Work in small steps:

1. Find the data: list_metrics shows the ontology's metrics and dimensions; list_views shows the
   views that already exist, with their parameters, columns and SQL. Reuse one if it fits.
2. Write a view per question. Start the file with a YAML comment, then one SELECT:
     /*
     description: What it answers, for people and agents.
     params:
       hours: {{type: int, default: 24, min: 1, max: 720}}
     */
     SELECT date_trunc('hour', event_ts) AS hour, isp_provider, p99_ttfb
     FROM playback_telemetry
     WHERE event_ts >= now() - INTERVAL :hours HOUR
   FROM a model, select its metrics and dimensions by name (never re-derive a metric), group by
   time with date_trunc, and bound the time dimension with >= and <: it runs live across both
   tiers. FROM a table (namespace.table, e.g. a gold rollup), write any Trino SELECT; label a
   column it computes in the header: columns: {{share: {{label: Share, format: percent}}}}.
   To combine questions (compare two time windows, join a model with a gold table, rank),
   put each part in a WITH clause, one model or tables per CTE, and write the rest over the
   CTEs: the parts are answered first, then the rest runs over their results.
   query_view runs a view so you can see its rows.
3. Write the page in plain HTML with the chart tags below. Names are enough: labels and number
   formats come from the ontology. Keep the page small and readable.
4. check_dashboards after every edit, and fix what it reports.
5. screenshot_dashboard to see the page as people will, with the problems it shows. Look at it
   critically: titles, empty charts, crowded labels. Iterate until it reads well.

Chart tags (al.js):

{reference}
"""


def create_mcp_server(gateway: Gateway, project: Project | None = None) -> MCPServer:
    mcp = MCPServer(
        "lakehouse-metrics",
        instructions=(
            "Answers questions with governed metrics from the lakehouse semantic layer. Call list_metrics "
            "first to see which metrics and dimensions exist and what they mean, then query_metrics. "
            "Times are UTC; ranges are half-open [start, end). Read `coverage` and `notes` in every result: "
            "an answer can cover less than the requested range, and the notes say why. "
            "Views are the project's named queries, which its dashboards read: list_views shows each "
            "one's parameters, columns and SQL, and query_view runs one."
        ),
    )
    read_only = ToolAnnotations(read_only_hint=True, open_world_hint=False)

    @mcp.tool(annotations=read_only)
    def list_views() -> ViewCatalog:
        """List the project's views (config/views/*.sql): named, parameterized queries that dashboards
        read. Each comes with its description, parameters, result columns and SQL."""
        return gateway.view_catalog()

    @mcp.tool(annotations=read_only)
    def query_view(
        name: Annotated[str, Field(description="A view name from list_views")],
        params: Annotated[
            dict[str, Any] | None, Field(description="Parameter values by name; parameters left out take their defaults")
        ] = None,
        include_sql: Annotated[bool, Field(description="Also return the SQL each engine ran")] = False,
    ) -> ViewResult:
        """Run a view and get its rows as a dashboard does, with how fresh the data is."""
        if name not in gateway.views:
            raise ToolError(f"no view named '{name}'. Views: {', '.join(gateway.views) or 'none'}")
        try:
            return gateway.view(name, params or {}, include_sql)
        except (ViewError, QueryError, EngineError) as e:
            raise ToolError(str(e)) from e

    if project is not None:
        _add_project_tools(mcp, project, read_only)

    @mcp.tool(annotations=read_only)
    def list_metrics() -> Catalog:
        """List the metrics and dimensions you can query, what each one means, and how far the data reaches."""
        return gateway.catalog()

    @mcp.tool(annotations=read_only)
    def data_status() -> Status:
        """How fresh each tier's data is, whether each rollup passed its last quality gate, fields
        producers send that the config doesn't describe yet, and drift proposals waiting for review.
        Check it before trusting numbers that matter."""
        return gateway.status()

    @mcp.tool(annotations=read_only)
    def query_metrics(
        metrics: Annotated[list[str], Field(description="Metric names from list_metrics, all from the same model")],
        start: Annotated[datetime, Field(description="Start of the time range, inclusive. ISO 8601; UTC if no offset")],
        end: Annotated[datetime, Field(description="End of the time range, exclusive")],
        dimensions: Annotated[list[str] | None, Field(description="Dimensions to group by")] = None,
        grain: Annotated[Grain | None, Field(description="Also group by time, in buckets of this size")] = None,
        filters: Annotated[
            list[Filter] | None, Field(description="Keep rows where `dimension <op> value`; in/not_in take a list")
        ] = None,
        order_by: Annotated[list[OrderBy] | None, Field(description="Sort by selected metrics or dimensions")] = None,
        limit: Annotated[int | None, Field(description="Return at most this many rows", gt=0)] = None,
        include_sql: Annotated[bool, Field(description="Also return the SQL each engine ran")] = False,
    ) -> QueryResponse:
        """Compute metrics over a time range, optionally grouped by dimensions and time buckets.

        Answers come from the real-time tier, the historical tier or both, whichever holds the range.
        """
        try:
            query = MetricQuery(
                metrics=metrics,
                start=start,
                end=end,
                dimensions=dimensions or [],
                grain=grain,
                filters=filters or [],
                order_by=order_by or [],
                limit=limit,
            )
            return gateway.query(query, include_sql)
        except (ValidationError, QueryError, EngineError) as e:
            raise ToolError(str(e)) from e

    return mcp


def _add_project_tools(mcp: MCPServer, project: Project, read_only: ToolAnnotations) -> None:
    """Tools and a prompt for the agent building dashboards: they read the files as they are now."""

    @mcp.tool(annotations=read_only)
    def check_dashboards() -> ProjectCheck:
        """Check the project as it is on disk now: the config and every view load, and each dashboard's
        tags name views, columns and parameters that exist. Run it after every edit to a view or page."""
        return project.check()

    @mcp.tool(annotations=read_only)
    async def screenshot_dashboard(
        name: Annotated[str, Field(description="The dashboard's name: its file name without .html")],
        theme: Annotated[Literal["light", "dark"], Field(description="Color scheme to render in")] = "light",
        width: Annotated[int, Field(ge=320, le=2400, description="Viewport width in CSS pixels; 400 for a phone")] = 1280,
    ) -> list[TextContent | Image]:
        """Open a dashboard in headless Chrome, as it is on disk now and with live data, wait until every
        chart has drawn, and return a screenshot with the problems the page shows."""
        try:
            shot = await asyncio.to_thread(project.screenshot, name, theme=theme, width=width)
        except KeyError:
            names = ", ".join(dashboard_files(project.dashboards_dir)) or "none"
            raise ToolError(f"no dashboard named '{name}'. Dashboards: {names}") from None
        except (ConfigError, ValidationError, BrowserError) as e:
            raise ToolError(str(e)) from e
        static = next((d.problems for d in project.check().dashboards if d.name == name), [])
        lines = [f"Dashboard {name} (/d/{name}), {theme}, {width} px wide."]
        lines.append("Every tag drew its data or an error." if shot.settled else "Captured at the time limit: some tags were still loading.")
        for title, items in (("Problems the page shows", shot.problems), ("Problems in the file", static), ("Notes about the data", shot.notes)):
            lines.append(f"{title}: " + ("none" if not items else "\n- " + "\n- ".join(items)))
        return [TextContent(type="text", text="\n".join(lines)), Image(data=shot.png, format="png")]

    @mcp.prompt()
    def build_dashboard(goal: str) -> str:
        """Build or change a dashboard: views in SQL, a page in HTML with live chart tags, checked and
        looked at until it reads well."""
        return DASHBOARD_GUIDE.format(goal=goal, config_dir=project.config_dir, reference=_tag_reference())


async def _watch_project(project: Project, gateway: Gateway, live: LiveViews, interval: float = 1.0) -> None:
    """Reload the views when a view file changes, and open pages when a view or dashboard does.
    An edit that breaks a view keeps the last good views, and pages say what broke."""

    def stamps() -> dict[str, int]:
        found = {}
        for path in [*project.config_dir.glob("views/*.sql"), *project.dashboards_dir.glob("*.html")]:
            with contextlib.suppress(OSError):  # deleted while listing
                found[str(path)] = path.stat().st_mtime_ns
        return found

    last = await asyncio.to_thread(stamps)
    while True:
        await asyncio.sleep(interval)
        current = await asyncio.to_thread(stamps)
        if current == last:
            continue
        views_changed = any(path.endswith(".sql") for path in current.keys() ^ last.keys() | {p for p in current if current[p] != last.get(p)})
        last = current
        if views_changed:
            try:
                platform = await asyncio.to_thread(load_platform, project.config_dir)
            except (ConfigError, ValidationError, OSError) as e:
                log.warning("not reloading the views: %s", e)
                live.announce("problem", {"detail": str(e)})
                continue
            gateway.views = dict(platform.views)
            live.restart()
        live.announce("reload", {})


def create_app(
    gateway: Gateway,
    dashboards_dir: str | Path | None = None,
    live: LiveViews | None = None,
    project: Project | None = None,
) -> FastAPI:
    """REST endpoints, live views, the MCP server (Streamable HTTP) at /mcp, and
    the HTML dashboards in `dashboards_dir` (the project's config/dashboards).
    With a `project`, MCP gets the tools for building dashboards, and open pages
    reload when its view or dashboard files change."""
    dashboards = Path(dashboards_dir) if dashboards_dir else None
    live = live or LiveViews(gateway)
    mcp = create_mcp_server(gateway, project)
    mcp_app = mcp.streamable_http_app(streamable_http_path="/mcp", stateless_http=True, json_response=True)

    @contextlib.asynccontextmanager
    async def lifespan(app: FastAPI):
        async with mcp.session_manager.run():  # mounted apps don't get their own lifespan run
            watcher = asyncio.create_task(_watch_project(project, gateway, live)) if project else None
            try:
                yield
            finally:
                if watcher:
                    watcher.cancel()
                live.close()

    app = FastAPI(
        title="Agentic Lakehouse",
        description="Governed metrics from one ontology, answered by DuckDB, Trino or Pinot.",
        lifespan=lifespan,
    )

    @app.exception_handler(QueryError)
    @app.exception_handler(ViewError)
    async def bad_query(request: Request, exc: ValueError) -> JSONResponse:
        return JSONResponse(status_code=400, content={"detail": str(exc)})

    @app.exception_handler(EngineError)
    async def engine_failed(request: Request, exc: EngineError) -> JSONResponse:
        return JSONResponse(status_code=502, content={"detail": str(exc)})

    @app.get("/health")
    def health() -> dict:
        return {"status": "ok"}

    @app.get("/catalog")
    def catalog() -> Catalog:
        return gateway.catalog()

    @app.post("/query")
    def query(query: MetricQuery, include_sql: bool = False) -> QueryResponse:
        return gateway.query(query, include_sql)

    @app.get("/views")
    def views() -> ViewCatalog:
        return gateway.view_catalog()

    @app.get("/views/{name}")
    def view(name: str, request: Request, include_sql: bool = False) -> ViewResult:
        """Run a view. Its parameters go in the query string: /views/isp_now?minutes=30"""
        if name not in gateway.views:
            raise HTTPException(status_code=404, detail=f"no view named '{name}'")
        params = {key: value for key, value in request.query_params.items() if key != "include_sql"}
        return gateway.view(name, params, include_sql)

    def subscription(name: str, request: Request) -> tuple[str, dict[str, Any]]:
        # A dependency, so a bad request gets its 404 or 400 before the stream starts.
        if name not in gateway.views:
            raise HTTPException(status_code=404, detail=f"no view named '{name}'")
        return name, gateway.views[name].bind(dict(request.query_params))

    @app.get("/views/{name}/live", response_class=EventSourceResponse)
    async def live_view(
        subscribed: Annotated[tuple[str, dict[str, Any]], Depends(subscription)],
    ) -> AsyncIterator[ServerSentEvent]:
        """Server-Sent Events: the view's result now (event `result`), then again each time it
        changes. Event `error` means a refresh failed; the last result still stands."""
        async for event in live.events(*subscribed):
            yield event

    def subscriptions(request: Request) -> list[tuple[str, str, dict[str, Any]]]:
        requested = request.query_params.getlist("view")  # none: only the news about the project
        subscribed = []
        for key in dict.fromkeys(requested):  # each once, in order
            name, _, query = key.partition("?")
            if name not in gateway.views:
                raise HTTPException(status_code=404, detail=f"no view named '{name}'")
            subscribed.append((key, name, gateway.views[name].bind(dict(parse_qsl(query, keep_blank_values=True)))))
        return subscribed

    @app.get("/live", response_class=EventSourceResponse)
    async def live_views(
        subscribed: Annotated[list[tuple[str, str, dict[str, Any]]], Depends(subscriptions)],
    ) -> AsyncIterator[ServerSentEvent]:
        """Server-Sent Events for several views over one connection, as a dashboard needs: each
        `view` is a name, with its parameters after a ?, URL-encoded (isp_hourly%3Fhours%3D24).
        Events are `result` ({subscription, result}) and `error` ({subscription, detail}), where
        `subscription` is the `view` value as requested; and, for every page, `hello` ({boot}) first,
        then `reload` when a view or dashboard file changed, or `problem` ({detail}) when an edit
        broke a view."""
        yield ServerSentEvent(event="hello", data={"boot": BOOT})
        async for key, event in live.watch(subscribed):
            if not key:  # news for every page
                yield event
                continue
            body = {"result": event.data} if event.event == "result" else event.data
            yield ServerSentEvent(event=event.event, data={"subscription": key, **body})

    @app.get("/status")
    def status() -> Status:
        return gateway.status()

    @app.get("/rollups/{table}")
    def rollup_rows(table: str, hours: Annotated[float, Query(gt=0, le=24 * 400)] = 48) -> RollupRows:
        try:
            return gateway.rollup_rows(table, hours)
        except KeyError:
            raise HTTPException(status_code=404, detail=f"no rollup named '{table}'") from None

    @app.get("/", include_in_schema=False, response_model=None)
    def home() -> FileResponse | HTMLResponse:
        files = dashboard_files(dashboards)
        if len(files) == 1:
            return FileResponse(next(iter(files.values())), media_type="text/html")
        items = "".join(f'<li><a href="/d/{html.escape(name)}">{html.escape(name)}</a></li>' for name in files)
        body = f"<ul>{items}</ul>" if files else "<p>No dashboards yet: add HTML files to the project's dashboards/ folder.</p>"
        return HTMLResponse(
            '<!doctype html><meta charset="utf-8"><title>Dashboards</title>'
            '<body style="font: 15px system-ui, sans-serif; max-width: 640px; margin: 48px auto; padding: 0 16px">'
            f'<h1 style="font-size: 20px">Dashboards</h1>{body}<p><a href="/docs">API docs</a></p></body>'
        )

    @app.get("/al.js", include_in_schema=False)
    def tag_js() -> FileResponse:
        return FileResponse(STATIC / "al.js", media_type="text/javascript", headers={"Cache-Control": "no-cache"})

    @app.get("/d/{name}", include_in_schema=False)
    def dashboard(name: str) -> FileResponse:
        path = dashboard_files(dashboards).get(name)
        if path is None:
            raise HTTPException(status_code=404, detail=f"no dashboard named '{name}'")
        return FileResponse(path, media_type="text/html")

    app.mount("/", mcp_app)  # after the REST routes, which match first
    return app
