"""Typed contracts for the platform config files that sit beside ontology.yaml.

- ingestion.yaml: datasets arriving on Kafka, their raw schema and Pinot table
- storage.yaml: the Iceberg table each dataset is kept in long-term
- deployment.yaml: where the services are reachable

load_platform() loads all four files and checks that they agree with each
other, e.g. that every column an ontology model reads exists in its source
dataset. Comparing against the live tables is the drift agent's job.
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Self

import yaml
from pydantic import Field, model_validator

from core.compiler import Engine
from core.ontology import Identifier, Ontology, StrictModel, index_by_name, load_ontology

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

    @property
    def namespace(self) -> str:
        return self.table.split(".")[0]


class StorageConfig(StrictModel):
    version: str
    tables: list[IcebergTable] = Field(min_length=1)

    @model_validator(mode="after")
    def _check_one_table_per_dataset(self) -> Self:
        seen = set()
        for table in self.tables:
            if table.dataset in seen:
                raise ValueError(f"storage: dataset '{table.dataset}' has more than one table")
            seen.add(table.dataset)
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


class DeploymentConfig(StrictModel):
    version: str
    kafka: KafkaEndpoints
    pinot: PinotEndpoints
    trino: TrinoEndpoint
    iceberg: IcebergEndpoint
    s3: S3Endpoint


class ConfigError(ValueError):
    """The config files are each valid but contradict one another."""


@dataclass(frozen=True)
class Platform:
    ontology: Ontology
    ingestion: IngestionConfig
    storage: StorageConfig
    deployment: DeploymentConfig

    def dataset(self, name: str) -> Dataset:
        return next(d for d in self.ingestion.datasets if d.name == name)

    def iceberg_table(self, dataset: str) -> IcebergTable:
        return next(t for t in self.storage.tables if t.dataset == dataset)

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


def load_platform(config_dir: str | Path) -> Platform:
    """Load and cross-check ontology, ingestion, storage and deployment YAML from `config_dir`.

    Raises pydantic.ValidationError for a malformed file, ConfigError when files disagree.
    """
    config_dir = Path(config_dir)
    platform = Platform(
        ontology=load_ontology(config_dir / "ontology.yaml"),
        ingestion=IngestionConfig.model_validate(_load_yaml(config_dir / "ingestion.yaml")),
        storage=StorageConfig.model_validate(_load_yaml(config_dir / "storage.yaml")),
        deployment=DeploymentConfig.model_validate(_load_yaml(config_dir / "deployment.yaml")),
    )
    _check_consistency(platform)
    return platform


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
    if problems:
        raise ConfigError("config files disagree:\n  - " + "\n  - ".join(problems))
