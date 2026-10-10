"""Typed contracts for the platform config files that sit beside ontology.yaml.

- ingestion.yaml: datasets arriving on Kafka, their raw schema and Pinot table
- storage.yaml: the Iceberg table each dataset is kept in long-term, and the
  rollups: semantic queries materialized as gold Iceberg tables
- quality_rules.yaml: checks a rollup's rows must pass before they're published
  (schema in core/quality.py)
- deployment.yaml: where the services are reachable
- views/*.sql: named queries for dashboards and agents (core/views.py)

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

from core.compiler import Engine, Filter, Grain, MetricQuery, QueryError, SqlCompiler
from core.ontology import Identifier, Ontology, SemanticModel, StrictModel, index_by_name, load_ontology
from core.quality import QualityConfig, TableName
from core.views import View, ViewError, load_views

ColumnType = Literal["string", "int", "long", "double", "boolean", "timestamp"]


class Column(StrictModel):
    name: Identifier
    type: ColumnType


class RealtimeTable(StrictModel):
    table: Identifier  # Pinot table name
    retention_days: int = Field(gt=0)


class Dataset(StrictModel):
    """One event stream: its Kafka topic, raw schema and Pinot real-time table."""

    name: Identifier
    description: str = Field(min_length=1)
    topic: str = Field(pattern=r"^[A-Za-z0-9._-]+$")  # characters Kafka allows in topic names
    time_column: Identifier
    columns: list[Column] = Field(min_length=1)
    realtime: RealtimeTable
    # Synthetic events for `run.py seed` and `stream`: "package.module:function" making one
    # episode (core/simulate.py). Without it, events get random values from the column types.
    simulator: str | None = Field(default=None, pattern=r"^[A-Za-z_][\w.]*:[A-Za-z_]\w*$")

    @model_validator(mode="after")
    def _check_time_column(self) -> Self:
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
    """A gold Iceberg table holding a semantic query's results (pipelines/rollups.py)."""

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

    def table_schemas(self) -> dict[str, dict[str, str]]:
        """Column -> SQL type of each Iceberg table views can read: every dataset's raw table and every rollup."""
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
    with open(path) as f:
        return yaml.safe_load(f) or {}


_EPOCH = datetime(1970, 1, 1)
_SQL_TYPES = {"string": "VARCHAR", "int": "INTEGER", "long": "BIGINT", "double": "DOUBLE", "boolean": "BOOLEAN", "timestamp": "TIMESTAMP"}
_DIMENSION_SQL_TYPES = {"string": "VARCHAR", "number": "DOUBLE", "boolean": "BOOLEAN", "timestamp": "TIMESTAMP"}


def load_platform(config_dir: str | Path) -> Platform:
    """Load and cross-check the ontology, ingestion, storage, quality and deployment YAML in
    `config_dir`, and the views in its views/ folder.

    Raises pydantic.ValidationError for a malformed file, ConfigError when files disagree or a
    view is invalid.
    """
    config_dir = Path(config_dir)
    platform = Platform(
        ontology=load_ontology(config_dir / "ontology.yaml"),
        ingestion=IngestionConfig.model_validate(_load_yaml(config_dir / "ingestion.yaml")),
        storage=StorageConfig.model_validate(_load_yaml(config_dir / "storage.yaml")),
        quality=QualityConfig.model_validate(_load_yaml(config_dir / "quality_rules.yaml")),
        deployment=DeploymentConfig.model_validate(_load_yaml(config_dir / "deployment.yaml")),
        config_dir=config_dir,
    )
    _check_consistency(platform)
    try:
        views = load_views(config_dir / "views", platform.ontology, platform.table_schemas())
    except ViewError as e:
        raise ConfigError(str(e)) from None
    return replace(platform, views=views)


def _check_consistency(platform: Platform) -> None:
    datasets = {d.name: d for d in platform.ingestion.datasets}
    problems = []
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
    for rules in platform.quality.rules:
        if rules.table not in rollup_columns:
            if rules.table not in {r.table for r in platform.storage.rollups}:
                problems.append(f"quality rules for '{rules.table}', which isn't a rollup in storage.yaml")
            continue
        for check in rules.checks:
            if missing := check.referenced_columns() - rollup_columns[rules.table]:
                problems.append(
                    f"quality check {check.type} on '{rules.table}' reads columns the rollup doesn't have: "
                    + ", ".join(sorted(missing))
                )
    if problems:
        raise ConfigError("config files disagree:\n  - " + "\n  - ".join(problems))
