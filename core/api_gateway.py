"""The gateway clients use to ask the semantic layer for metrics.

The same operations are served over REST and over the Model Context Protocol
(MCP), so the dashboard and AI agents get identical answers:

- catalog: the metrics and dimensions you can ask for, and what they mean
- query: a MetricQuery, answered by the tier router
- views: the project's named queries (views/*.sql, see core/views.py), which
  dashboards read by name with parameters
- status: how fresh each tier is, each rollup's last quality gate, and drift
  proposals waiting for review: whether the data can be trusted right now

REST:  GET /catalog, POST /query, GET /views, GET /views/{name}?<params>,
       GET /status, GET /rollups/{table} (interactive docs at /docs)
Dashboards: the project's dashboards/*.html, at /d/<name> (and / for the only one)
MCP:   Streamable HTTP at /mcp, or stdio via `python run.py mcp`
"""

import contextlib
import html
import math
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Annotated, Any, Literal, get_args

import pyarrow as pa
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from mcp.server.mcpserver import MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from mcp_types import ToolAnnotations
from pydantic import BaseModel, Field, ValidationError
from pyiceberg.exceptions import NoSuchTableError

from core.agent_sync import pending_proposals, unmodeled_columns
from core.compiler import Filter, Grain, MetricQuery, OrderBy, QueryError
from core.config import Platform
from core.engines import EngineError, QueryEngine, TrinoEngine
from core.ontology import AggregateMetric, Metric, SemanticModel
from core.orchestrator import FLUSHED_THROUGH, iceberg_catalog
from core.rollups import ROLLUP_THROUGH, RollupStore
from core.router import QueryResult, Router
from core.views import ModelSource, View, ViewColumn, ViewError

MAX_ROWS = 5000  # protects clients, and agents' context windows, from runaway results
DASHBOARD_NAME = re.compile(r"^[A-Za-z0-9_-]+$")


class DimensionInfo(BaseModel):
    name: str
    type: str
    description: str


class MetricInfo(BaseModel):
    name: str
    description: str
    calculation: str  # e.g. "99th percentile of request_latency_ms where stream_start_event = TRUE"


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


class DriftProposalInfo(BaseModel):
    file: str
    dataset: str
    summary: str
    created: datetime


class Status(BaseModel):
    now: datetime
    datasets: list[DatasetStatus]
    rollups: list[RollupInfo]
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
    # tables, complete up to their last flush or rollup run.
    source: Literal["model", "tables"]
    reads: list[str]  # the model, or the tables
    params: list[ViewParamInfo]
    columns: list[ViewColumn]
    sql: str


class ViewCatalog(BaseModel):
    views: list[ViewInfo]


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


@dataclass(frozen=True)
class TableState:
    exists: bool
    complete_through: datetime | None = None  # the table holds every row before this time (naive UTC)


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

    def __init__(
        self,
        router: Router,
        *,
        views: Mapping[str, View] | None = None,
        warehouse: Warehouse | None = None,
        rollups: RollupStore | None = None,
        proposals_dir: str | Path | None = None,
        schema_drift: Callable[[str], list[str]] | None = None,
        max_rows: int = MAX_ROWS,
    ):
        self.router = router
        self.views = dict(views or {})
        self.warehouse = warehouse
        self.rollups = rollups
        self.proposals_dir = Path(proposals_dir) if proposals_dir else None
        self.schema_drift = schema_drift
        self.max_rows = max_rows

    @classmethod
    def from_platform(cls, platform: Platform, proposals_dir: str | Path = "proposals") -> "Gateway":
        rollups = RollupStore(platform, lambda: iceberg_catalog(platform))  # its catalog is created on first use

        def schema_drift(dataset: str) -> list[str]:
            table = rollups.catalog.load_table(platform.iceberg_table(dataset).table)
            return unmodeled_columns(platform.dataset(dataset), table)

        def table_state(name: str) -> TableState:
            try:
                properties = rollups.catalog.load_table(name).properties
            except NoSuchTableError:
                return TableState(exists=False)
            except Exception as e:  # e.g. the catalog is unreachable: a failing engine, like Trino being down
                raise EngineError(f"iceberg catalog: {e}") from e
            through = properties.get(FLUSHED_THROUGH) or properties.get(ROLLUP_THROUGH)
            return TableState(exists=True, complete_through=datetime.fromisoformat(through) if through else None)

        warehouse = Warehouse(TrinoEngine(platform.deployment.trino), platform.deployment.trino.catalog, table_state)
        return cls(
            Router.from_platform(platform),
            views=platform.views,
            warehouse=warehouse,
            rollups=rollups,
            proposals_dir=proposals_dir,
            schema_drift=schema_drift,
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
                        DimensionInfo(name=d.name, type=d.type, description=d.description)
                        for d in model.dimensions
                        if d.name != model.time_dimension
                    ],
                    metrics=[
                        MetricInfo(name=m.name, description=m.description, calculation=_calculation(m, model))
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
        view = self.views[name]
        values = view.bind(params or {})
        computed_at = datetime.now(UTC)
        advice = "narrow the view with its parameters, or give it a LIMIT"
        if isinstance(view.source, ModelSource):
            query = view.source.query(values, now=_utc(self.router.clock()))
            result, rows, notes, truncated = self._run(query, advice)
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
        sql = view.source.sql(values, dialect=engine.name, catalog=self.warehouse.catalog, limit=self.max_rows + 1)
        notes = []
        if missing := [table for table, state in states.items() if not state.exists]:
            rows = []  # e.g. a rollup that hasn't run yet; its first run creates the table
            notes.append(f"{', '.join(missing)} doesn't exist yet, so there are no rows")
        else:
            rows = _rows(engine.execute(sql))
        truncated = len(rows) > self.max_rows
        if truncated:
            rows = rows[: self.max_rows]
            notes.append(f"Only the first {self.max_rows} rows are returned; {advice}.")
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

    def _run(self, query: MetricQuery, advice: str) -> tuple[QueryResult, list[dict[str, Any]], list[str], bool]:
        """Route `query`, returning at most max_rows rows and notes on what the result covers."""
        capped = query.limit is None or query.limit > self.max_rows
        if capped:  # fetch one extra row to tell whether anything was cut off
            query = query.model_copy(update={"limit": self.max_rows + 1})
        result = self.router.run(query)
        rows = _rows(result.table)
        notes = list(result.notes)
        truncated = capped and len(rows) > self.max_rows
        if truncated:
            rows = rows[: self.max_rows]
            notes.append(f"Only the first {self.max_rows} rows are returned; {advice}.")
        return result, rows, notes, truncated


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
        source="model" if isinstance(view.source, ModelSource) else "tables",
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


def create_mcp_server(gateway: Gateway) -> MCPServer:
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


def create_app(gateway: Gateway, dashboards_dir: str | Path | None = None) -> FastAPI:
    """REST endpoints, the MCP server (Streamable HTTP) at /mcp, and the HTML
    dashboards in `dashboards_dir` (the project's config/dashboards)."""
    dashboards = Path(dashboards_dir) if dashboards_dir else None

    def dashboard_files() -> dict[str, Path]:
        if dashboards is None or not dashboards.is_dir():
            return {}
        return {p.stem: p for p in sorted(dashboards.glob("*.html")) if DASHBOARD_NAME.match(p.stem)}
    mcp = create_mcp_server(gateway)
    mcp_app = mcp.streamable_http_app(streamable_http_path="/mcp", stateless_http=True, json_response=True)

    @contextlib.asynccontextmanager
    async def lifespan(app: FastAPI):
        async with mcp.session_manager.run():  # mounted apps don't get their own lifespan run
            yield

    app = FastAPI(
        title="Modular Agentic Lakehouse Engine",
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
        files = dashboard_files()
        if len(files) == 1:
            return FileResponse(next(iter(files.values())), media_type="text/html")
        items = "".join(f'<li><a href="/d/{html.escape(name)}">{html.escape(name)}</a></li>' for name in files)
        body = f"<ul>{items}</ul>" if files else "<p>No dashboards yet: add HTML files to the project's dashboards/ folder.</p>"
        return HTMLResponse(
            '<!doctype html><meta charset="utf-8"><title>Dashboards</title>'
            '<body style="font: 15px system-ui, sans-serif; max-width: 640px; margin: 48px auto; padding: 0 16px">'
            f'<h1 style="font-size: 20px">Dashboards</h1>{body}<p><a href="/docs">API docs</a></p></body>'
        )

    @app.get("/d/{name}", include_in_schema=False)
    def dashboard(name: str) -> FileResponse:
        path = dashboard_files().get(name)
        if path is None:
            raise HTTPException(status_code=404, detail=f"no dashboard named '{name}'")
        return FileResponse(path, media_type="text/html")

    app.mount("/", mcp_app)  # after the REST routes, which match first
    return app
