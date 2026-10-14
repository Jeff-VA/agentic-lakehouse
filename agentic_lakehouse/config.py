"""Typed contracts for the platform config files that sit beside ontology.yaml.

- ingestion.yaml: datasets arriving on Kafka, their raw schema and Pinot table
- storage.yaml: the Iceberg table each dataset is kept in long-term, and the
  rollups: semantic queries materialized as gold Iceberg tables
- quality_rules.yaml: checks a rollup's rows must pass before they're published
  (schema in agentic_lakehouse/quality.py)
- deployment.yaml: where the services are reachable
- pipelines.yaml (optional): steps, SQL or Python transformations that each
  write a table, run in the order their inputs require (agentic_lakehouse/steps.py)
- views/*.sql: named queries for dashboards and agents (agentic_lakehouse/views.py)

load_platform() loads them all and checks that they agree with each other,
e.g. that every column an ontology model reads exists in its source dataset,
and that every view reads known models, tables and columns. Comparing against
the live tables is the drift agent's job.
"""

from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta
from pathlib import Path
from typing import Literal, Self

import yaml
from pydantic import Field, model_validator

from agentic_lakehouse.compiler import Engine, Filter, Grain, MetricQuery, QueryError, SqlCompiler
from agentic_lakehouse.ontology import Identifier, Ontology, SemanticModel, StrictModel, index_by_name
from agentic_lakehouse.hooks import SPEC
from agentic_lakehouse.macros import MacroError, ProjectMacros
from agentic_lakehouse.quality import QualityConfig, TableName
from agentic_lakehouse.views import View, ViewError, describe_query, load_views, tables_in

ColumnType = Literal["string", "int", "long", "double", "boolean", "timestamp"]


class Column(StrictModel):
    name: Identifier
    type: ColumnType


class RealtimeTable(StrictModel):
    table: Identifier  # Pinot table name
    retention_days: int = Field(gt=0)


class Source(StrictModel):
    """A live feed the platform reads itself, with `agentic-lakehouse ingest`, instead of
    producers writing to the dataset's Kafka topic (agentic_lakehouse/sources.py)."""

    kind: Literal["sse"]  # Server-Sent Events over HTTP, one JSON object per event
    url: str = Field(pattern=r"^https?://")
    # module:function: one feed event (a dict) -> this dataset's event, or None to leave it out
    transform: str | None = Field(default=None, pattern=SPEC)
    headers: dict[str, str] = {}  # e.g. a User-Agent, which some public feeds ask for


class Processor(StrictModel):
    """Makes this dataset from another as it streams, with `agentic-lakehouse process`
    (agentic_lakehouse/processors.py)."""

    input: Identifier  # the dataset it reads
    run: str = Field(pattern=SPEC)  # module:Class, built with a ProcessorContext; process(events) -> events
    warmup_minutes: float = Field(default=0, ge=0)  # input replayed at start, to rebuild state after a restart


class Dataset(StrictModel):
    """One event stream: its Kafka topic, raw schema and Pinot real-time table. Its events come
    from producers writing to the topic, from a live `source`, or from a `processor`."""

    name: Identifier
    description: str = Field(min_length=1)
    topic: str = Field(pattern=r"^[A-Za-z0-9._-]+$")  # characters Kafka allows in topic names
    time_column: Identifier
    columns: list[Column] = Field(min_length=1)
    realtime: RealtimeTable
    # Synthetic events for `agentic-lakehouse seed` and `stream`: "package.module:function" making one
    # episode (agentic_lakehouse/simulate.py). Without it, events get random values from the column types.
    simulator: str | None = Field(default=None, pattern=SPEC)
    source: Source | None = None
    processor: Processor | None = None

    @property
    def fed_by(self) -> str | None:
        """What publishes this dataset's events when the platform does: a feed or a processor."""
        if self.source:
            return f"the feed {self.source.url}"
        if self.processor:
            return f"the processor {self.processor.run}"
        return None

    @model_validator(mode="after")
    def _check_time_column(self) -> Self:
        if self.source and self.processor:
            raise ValueError(f"dataset '{self.name}': a dataset comes from a source or a processor, not both")
        columns = index_by_name(self.columns, "column", f"dataset '{self.name}'")
        if getattr(columns.get(self.time_column), "type", None) != "timestamp":
            raise ValueError(f"dataset '{self.name}': time_column '{self.time_column}' must be a timestamp column")
        return self


class IngestionConfig(StrictModel):
    version: str
    datasets: list[Dataset] = Field(min_length=1)

    @model_validator(mode="after")
    def _check_unique_names(self) -> Self:
        index_by_name(self.datasets, "dataset", "ingestion")
        return self


class IcebergTable(StrictModel):
    dataset: Identifier
    table: str = Field(pattern=r"^[a-z][a-z0-9_]*\.[a-z][a-z0-9_]*$")  # <namespace>.<table>
    partition_by: Literal["hour", "day", "month"]  # transform applied to the dataset's time column
    flush_every_minutes: float = Field(gt=0)
    allowed_lateness_minutes: float = Field(ge=0)

    @property
    def namespace(self) -> str:
        return self.table.split(".")[0]


class RollupQuery(StrictModel):
    """A MetricQuery without its time range; each run supplies the window."""

    metrics: list[Identifier] = Field(min_length=1)
    dimensions: list[Identifier] = []
    grain: Grain
    filters: list[Filter] = []


class Rollup(StrictModel):
    """A gold Iceberg table holding a semantic query's results (agentic_lakehouse/pipelines/rollups.py)."""

    table: TableName
    description: str = Field(min_length=1)
    query: RollupQuery
    lookback_hours: float = Field(gt=0)  # each run recomputes at least this much, so late events are corrected
    backfill_days: float = Field(gt=0)  # how far back the first run (or one after an outage) starts
    every_minutes: float = Field(gt=0)

    def with_window(self, start: datetime, end: datetime) -> MetricQuery:
        return MetricQuery(**self.query.model_dump(), start=start, end=end)


class StorageConfig(StrictModel):
    version: str
    tables: list[IcebergTable] = Field(min_length=1)
    rollups: list[Rollup] = []

    @model_validator(mode="after")
    def _check_unique_tables(self) -> Self:
        seen = set()
        for table in self.tables:
            if table.dataset in seen:
                raise ValueError(f"storage: dataset '{table.dataset}' has more than one table")
            seen.add(table.dataset)
        names = [t.table for t in self.tables] + [r.table for r in self.rollups]
        if repeated := sorted({n for n in names if names.count(n) > 1}):
            raise ValueError(f"storage: tables defined more than once: {', '.join(repeated)}")
        return self


class Step(StrictModel):
    """A transformation in pipelines.yaml: SQL or Python over lakehouse tables, writing one table
    on a schedule, behind the table's quality checks (agentic_lakehouse/steps.py)."""

    name: Identifier
    description: str = Field(min_length=1)
    output: TableName  # <namespace>.<table> it writes
    sql: str | None = None  # a Trino SELECT over <namespace>.<table> names; load_platform renders its macro calls
    run: str | None = Field(default=None, pattern=SPEC)  # module:function(StepContext) -> pyarrow.Table, or None
    reads: list[TableName] = []  # what a Python step reads (a SQL step's reads come from its SQL)
    columns: dict[Identifier, ColumnType] = {}  # a Python step's output columns (a SQL step's are worked out)
    mode: Literal["replace", "append"] = "replace"  # rewrite the table each run, or add the rows a run returns
    time_column: Identifier | None = None  # the output's time column, for freshness checks
    every_minutes: float = Field(gt=0)

    @model_validator(mode="after")
    def _check(self) -> Self:
        if (self.sql is None) == (self.run is None):
            raise ValueError(f"step '{self.name}': give it `sql` or `run`, one of them")
        if self.sql is not None and (self.reads or self.columns):
            raise ValueError(f"step '{self.name}': a SQL step's reads and columns come from its SQL; leave them out")
        if self.run is not None and not self.columns:
            raise ValueError(f"step '{self.name}': declare a Python step's output `columns`, so views and checks know them")
        if self.run is not None and self.time_column and self.time_column not in self.columns:
            raise ValueError(f"step '{self.name}': time_column '{self.time_column}' isn't one of its columns")
        return self

    def inputs(self) -> set[str]:
        return tables_in(self.sql) if self.sql else set(self.reads)


class PipelinesConfig(StrictModel):
    version: str = "1.0.0"
    steps: list[Step] = []

    @model_validator(mode="after")
    def _check_unique(self) -> Self:
        index_by_name(self.steps, "step", "pipelines")
        outputs = [s.output for s in self.steps]
        if repeated := sorted({o for o in outputs if outputs.count(o) > 1}):
            raise ValueError(f"pipelines: tables written by more than one step: {', '.join(repeated)}")
        return self


class KafkaEndpoints(StrictModel):
    bootstrap_servers: str
    internal_bootstrap_servers: str  # as seen from inside the container network


class PinotEndpoints(StrictModel):
    controller_url: str
    broker_url: str


class TrinoEndpoint(StrictModel):
    host: str
    port: int
    user: str
    catalog: Identifier


class IcebergEndpoint(StrictModel):
    catalog_uri: str


class S3Endpoint(StrictModel):
    endpoint: str
    region: str
    access_key: str
    secret_key: str


class PrefectEndpoint(StrictModel):
    api_url: str


class DeploymentConfig(StrictModel):
    version: str
    kafka: KafkaEndpoints
    pinot: PinotEndpoints
    trino: TrinoEndpoint
    iceberg: IcebergEndpoint
    s3: S3Endpoint
    prefect: PrefectEndpoint


class ConfigError(ValueError):
    """The config files are each valid but contradict one another."""


@dataclass(frozen=True)
class Platform:
    ontology: Ontology
    ingestion: IngestionConfig
    storage: StorageConfig
    quality: QualityConfig
    deployment: DeploymentConfig
    pipelines: PipelinesConfig = field(default_factory=PipelinesConfig)
    config_dir: Path | None = None  # also holds the project's views/ and dashboards/
    views: dict[str, View] = field(default_factory=dict)

    def dataset(self, name: str) -> Dataset:
        return next(d for d in self.ingestion.datasets if d.name == name)

    def iceberg_table(self, dataset: str) -> IcebergTable:
        return next(t for t in self.storage.tables if t.dataset == dataset)

    def rollup(self, table: str) -> Rollup:
        return next(r for r in self.storage.rollups if r.table == table)

    def rollup_model(self, rollup: Rollup) -> SemanticModel:
        """The ontology model a rollup's metrics come from."""
        return SqlCompiler(self.ontology).model_for(rollup.with_window(_EPOCH, _EPOCH + timedelta(hours=1)))

    def rollup_columns(self, rollup: Rollup) -> list[str]:
        """The gold table's columns: time bucket, dimensions, metrics."""
        return [self.rollup_model(rollup).time_dimension, *rollup.query.dimensions, *rollup.query.metrics]

    def step(self, name: str) -> Step:
        return next(s for s in self.pipelines.steps if s.name == name)

    def ordered_steps(self) -> list[Step]:
        """The steps, each after the steps whose tables it reads. Raises ConfigError on a cycle."""
        by_output = {s.output: s for s in self.pipelines.steps}
        ordered, done, active = [], set(), []

        def visit(step: Step) -> None:
            if step.name in done:
                return
            if step.name in active:
                raise ConfigError(f"steps read each other in a cycle: {' -> '.join(active[active.index(step.name):] + [step.name])}")
            active.append(step.name)
            for table in sorted(step.inputs()):
                if table in by_output:
                    visit(by_output[table])
            active.pop()
            done.add(step.name)
            ordered.append(step)

        for step in self.pipelines.steps:
            visit(step)
        return ordered

    def table_schemas(self) -> dict[str, dict[str, str]]:
        """Column -> SQL type of each Iceberg table views and steps can read: every dataset's raw
        table, every rollup and every step's output."""
        schemas = self._base_schemas()
        for step in self.ordered_steps():
            schemas[step.output] = _step_columns(step, schemas)
        return schemas

    def _base_schemas(self) -> dict[str, dict[str, str]]:
        schemas = {
            table.table: {c.name: _SQL_TYPES[c.type] for c in self.dataset(table.dataset).columns} for table in self.storage.tables
        }
        for rollup in self.storage.rollups:
            model = self.rollup_model(rollup)
            dimensions = {d.name: d for d in model.dimensions}
            metrics = {m.name: m for m in model.metrics}
            columns = {model.time_dimension: "TIMESTAMP"}
            columns |= {name: _DIMENSION_SQL_TYPES[dimensions[name].type] for name in rollup.query.dimensions}
            columns |= {
                name: "BIGINT" if getattr(metrics[name], "agg", None) in ("count", "count_distinct") else "DOUBLE"
                for name in rollup.query.metrics
            }
            schemas[rollup.table] = columns
        return schemas

    def table_for(self, dataset: str, engine: Engine) -> str:
        """The relation that holds `dataset` in `engine`, ready to put in a FROM clause."""
        if engine == "pinot":
            return self.dataset(dataset).realtime.table
        if engine == "trino":
            return f"{self.deployment.trino.catalog}.{self.iceberg_table(dataset).table}"
        raise ValueError(f"{engine} holds no datasets; it queries data handed to it")


def _load_yaml(path: Path) -> dict:
    """A config file's YAML. Raises ConfigError saying where, if it isn't valid YAML."""
    try:
        with open(path) as f:
            return yaml.safe_load(f) or {}
    except yaml.YAMLError as e:
        mark = getattr(e, "problem_mark", None)
        where = f" (line {mark.line + 1}, column {mark.column + 1})" if mark else ""
        problem = getattr(e, "problem", None) or str(e)
        hint = "; put text containing ': ' in quotes, or use > for a long description" if "mapping values" in problem else ""
        raise ConfigError(f"{path.name} isn't valid YAML: {problem}{where}{hint}") from None


def _rendered_steps(pipelines: PipelinesConfig, macros: ProjectMacros) -> PipelinesConfig:
    """The steps with the macro calls in their SQL rendered, with Trino's version of each macro
    since steps run there: the rendered SQL is what's parsed, typed and run. Raises ConfigError
    naming the step."""
    steps = []
    for step in pipelines.steps:
        if step.sql is not None:
            try:
                step = step.model_copy(update={"sql": macros.render(step.sql, "trino")})
            except MacroError as e:
                raise ConfigError(f"step '{step.name}': macros: {e}") from None
        steps.append(step)
    return pipelines.model_copy(update={"steps": steps})


def _step_columns(step: Step, schemas: dict[str, dict[str, str]]) -> dict[str, str]:
    if step.sql:
        return describe_query(step.sql, schemas)[1]
    return {name: _SQL_TYPES[column_type] for name, column_type in step.columns.items()}


_EPOCH = datetime(1970, 1, 1)
_SQL_TYPES = {"string": "VARCHAR", "int": "INTEGER", "long": "BIGINT", "double": "DOUBLE", "boolean": "BOOLEAN", "timestamp": "TIMESTAMP"}
_DIMENSION_SQL_TYPES = {"string": "VARCHAR", "number": "DOUBLE", "boolean": "BOOLEAN", "timestamp": "TIMESTAMP"}


def load_platform(config_dir: str | Path) -> Platform:
    """Load and cross-check the ontology, ingestion, storage, quality and deployment YAML in
    `config_dir`, and the views in its views/ folder.

    Raises pydantic.ValidationError for a malformed file, ConfigError for a file that isn't YAML,
    files that disagree or an invalid view.
    """
    config_dir = Path(config_dir)
    ontology = Ontology.model_validate(_load_yaml(config_dir / "ontology.yaml"))
    try:
        ontology._macros = ProjectMacros(config_dir / "macros")
    except MacroError as e:
        raise ConfigError(str(e)) from None
    platform = Platform(
        ontology=ontology,
        ingestion=IngestionConfig.model_validate(_load_yaml(config_dir / "ingestion.yaml")),
        storage=StorageConfig.model_validate(_load_yaml(config_dir / "storage.yaml")),
        quality=QualityConfig.model_validate(_load_yaml(config_dir / "quality_rules.yaml")),
        deployment=DeploymentConfig.model_validate(_load_yaml(config_dir / "deployment.yaml")),
        pipelines=_rendered_steps(
            PipelinesConfig.model_validate(_load_yaml(config_dir / "pipelines.yaml")) if (config_dir / "pipelines.yaml").exists() else PipelinesConfig(),
            ontology.macros,
        ),
        config_dir=config_dir,
    )
    _check_consistency(platform)
    try:
        views = load_views(config_dir / "views", platform.ontology, platform.table_schemas())
    except ViewError as e:
        raise ConfigError(str(e)) from None
    return replace(platform, views=views)


def _check_steps(platform: Platform, problems: list[str]) -> dict[str, dict[str, str]]:
    """Check pipelines.yaml against the rest; returns each step output's columns that could be worked out."""
    if not platform.pipelines.steps:
        return {}
    schemas = platform._base_schemas()
    for step in platform.pipelines.steps:
        if step.output in schemas:
            problems.append(f"step '{step.name}' writes {step.output}, which is already a raw table or rollup")
    try:
        ordered = platform.ordered_steps()
    except (ConfigError, ViewError) as e:
        problems.append(str(e))
        return {}
    outputs = {}
    for step in ordered:
        if unknown := sorted(step.inputs() - schemas.keys()):
            problems.append(f"step '{step.name}' reads unknown tables: {', '.join(unknown)}")
            continue
        try:
            schemas[step.output] = outputs[step.output] = _step_columns(step, schemas)
        except ViewError as e:
            problems.append(f"step '{step.name}': {e}")
            continue
        if step.sql and step.time_column and step.time_column not in outputs[step.output]:
            problems.append(f"step '{step.name}': time_column '{step.time_column}' isn't one of its columns")
    return outputs


def _check_consistency(platform: Platform) -> None:
    datasets = {d.name: d for d in platform.ingestion.datasets}
    problems = []
    for dataset in datasets.values():
        if dataset.processor and dataset.processor.input not in datasets.keys() - {dataset.name}:
            problems.append(f"dataset '{dataset.name}' is processed from unknown dataset '{dataset.processor.input}'")
    for table in platform.storage.tables:
        if table.dataset not in datasets:
            problems.append(f"storage table '{table.table}' is for unknown dataset '{table.dataset}'")
    stored = {t.dataset for t in platform.storage.tables}
    for name in datasets.keys() - stored:
        problems.append(f"dataset '{name}' has no Iceberg table in storage.yaml")
    for model in platform.ontology.models:
        dataset = datasets.get(model.source)
        if dataset is None:
            problems.append(f"model '{model.name}' reads unknown dataset '{model.source}'")
            continue
        if missing := model.source_columns() - {c.name for c in dataset.columns}:
            problems.append(
                f"model '{model.name}' reads columns that dataset '{dataset.name}' doesn't have: "
                + ", ".join(sorted(missing))
            )
    rollup_columns = {}
    for rollup in platform.storage.rollups:
        try:
            # Compiling checks the query against the ontology, as each run will.
            query = rollup.with_window(_EPOCH, _EPOCH + timedelta(hours=1))
            SqlCompiler(platform.ontology).compile(query, engine="trino", table="t")
            rollup_columns[rollup.table] = set(platform.rollup_columns(rollup))
        except QueryError as e:
            problems.append(f"rollup '{rollup.table}': {e}")
    step_columns = _check_steps(platform, problems)
    for rules in platform.quality.rules:
        if rules.table in step_columns:
            columns, what = set(step_columns[rules.table]), "the step's output"
            step = next(s for s in platform.pipelines.steps if s.output == rules.table)
            if any(check.type == "freshness" for check in rules.checks) and not step.time_column:
                problems.append(f"quality rules for '{rules.table}' check freshness; give step '{step.name}' a time_column")
        elif rules.table in rollup_columns:
            columns, what = rollup_columns[rules.table], "the rollup"
        else:
            if rules.table not in {r.table for r in platform.storage.rollups}:
                problems.append(f"quality rules for '{rules.table}', which isn't a rollup in storage.yaml or a step's output")
            continue
        for check in rules.checks:
            if missing := check.referenced_columns() - columns:
                problems.append(
                    f"quality check {check.type} on '{rules.table}' reads columns {what} doesn't have: " + ", ".join(sorted(missing))
                )
    if problems:
        raise ConfigError("config files disagree:\n  - " + "\n  - ".join(problems))
