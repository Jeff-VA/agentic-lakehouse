"""Drift signals: what a dataset's data shows that its config may not reflect yet.

detect_drift() compares a dataset's Iceberg table with ingestion.yaml and
reports each finding as a Signal. Today that's new columns: fields producers
send that the config doesn't list, profiled from sampled rows. Columns config
can't express, and type mismatches, are reported for a person.
"""

import re
from dataclasses import dataclass
from datetime import datetime
from functools import reduce
from typing import Literal

import duckdb
import pyarrow as pa
from pydantic import BaseModel
from pyiceberg.expressions import NotNull, Or
from pyiceberg.table import Table
from pyiceberg.types import BooleanType, DoubleType, FloatType, IntegerType, LongType, StringType, TimestampType

from agentic_lakehouse.config import ColumnType, Dataset, Platform

_SNAKE_CASE = re.compile(r"^[a-z][a-z0-9_]*$")
_COLUMN_TYPES = {
    IntegerType: "int",
    LongType: "long",
    FloatType: "double",
    DoubleType: "double",
    BooleanType: "boolean",
    StringType: "string",
    TimestampType: "timestamp",
}

SignalKind = Literal["new_column", "fading_column", "new_value"]


class ColumnProfile(BaseModel):
    name: str
    type: ColumnType
    rows_sampled: int
    rows_set: int  # sampled rows where the column isn't NULL
    distinct_values: int  # approximate
    minimum: int | float | str | None = None
    maximum: int | float | str | None = None
    mean: float | None = None
    top_values: list[tuple[str | bool, int]] = []  # most common values with their counts (text and booleans)


class Signal(BaseModel):
    """One thing in a dataset's data that its config may not reflect yet."""

    key: str  # the same in every run that finds it, e.g. "edits:fading_column:is_bot"
    kind: SignalKind
    column: str
    detail: str  # what was found, in a line, for people and for the prompt
    profile: ColumnProfile | None = None  # new_column: its values
    value: str | None = None  # new_value: the value
    rows: int | None = None  # new_value: rows in the recent window that have it
    share_earlier: float | None = None  # fading_column: the share of rows that set it in the earlier window
    share_recent: float | None = None  # fading_column: the same in the recent window
    last_seen: datetime | None = None  # fading_column: the newest event that set it, as far as file metrics tell
    replaces: str | None = None  # new_column: a fading column of the same type it may be the new name of
    read_by: list[str] = []  # definitions that read the column, e.g. "dimension edits.editor_type, via editor_type()"


class DriftReport(BaseModel):
    dataset: str
    table: str
    window_start: datetime | None = None  # the recent window, [window_start, window_end), naive UTC
    window_end: datetime | None = None
    signals: list[Signal] = []
    unsupported_columns: list[str] = []  # new columns config can't express, with the reason
    type_mismatches: list[str] = []
    notes: list[str] = []  # what the run couldn't judge, e.g. too few recent rows

    @property
    def new_columns(self) -> list[ColumnProfile]:
        return [s.profile for s in self.signals if s.kind == "new_column" and s.profile is not None]

    @property
    def has_drift(self) -> bool:
        return bool(self.signals or self.unsupported_columns or self.type_mismatches)


@dataclass
class DriftSamples:
    """Rows, with all of the table's columns, to check a proposal on and to show its effect."""

    recent: pa.Table  # the recent window, including rows for each signal (where each new column is set, ...)
    earlier: pa.Table  # the window before it; no rows when there's no earlier data


def new_column_signal(dataset: str, profile: ColumnProfile) -> Signal:
    detail = f"new column {profile.name} ({profile.type}): set in {profile.rows_set} of {profile.rows_sampled} sampled rows"
    if profile.top_values:
        detail += ", most often " + ", ".join(f"{value} ({count})" for value, count in profile.top_values[:3])
    elif profile.minimum is not None:
        detail += f", from {profile.minimum} to {profile.maximum}"
    return Signal(key=f"{dataset}:new_column:{profile.name}", kind="new_column", column=profile.name, detail=detail, profile=profile)


def unmodeled_columns(dataset: Dataset, table: Table) -> list[str]:
    """Columns the dataset's Iceberg table has but ingestion.yaml doesn't list: fields producers
    send that the config doesn't describe yet. Reads only the table's schema."""
    configured = {c.name for c in dataset.columns}
    return [f.name for f in table.schema().fields if f.name not in configured]


def detect_drift(dataset_name: str, platform: Platform, table: Table, sample_rows: int = 10_000) -> tuple[DriftReport, DriftSamples | None]:
    """Compare `table`'s live schema with the dataset in ingestion.yaml.

    Returns the report and, when there are signals, samples of rows: the
    recent ones are rows in which at least one new column is set.
    """
    dataset = platform.dataset(dataset_name)
    configured = {c.name: c.type for c in dataset.columns}
    report = DriftReport(dataset=dataset.name, table=".".join(table.name()))
    candidates = {}
    for field in table.schema().fields:
        live_type = _COLUMN_TYPES.get(type(field.field_type))
        if field.name in configured:
            if live_type != configured[field.name]:
                report.type_mismatches.append(
                    f"{field.name}: ingestion.yaml says {configured[field.name]}, the Iceberg table has {field.field_type}"
                )
        elif not _SNAKE_CASE.match(field.name):
            report.unsupported_columns.append(f"{field.name}: config needs lowercase snake_case names")
        elif live_type is None:
            report.unsupported_columns.append(f"{field.name}: Iceberg type {field.field_type} has no config equivalent")
        else:
            candidates[field.name] = live_type
    if not candidates:
        return report, None

    sample = table.scan(row_filter=reduce(Or, [NotNull(name) for name in candidates]), limit=sample_rows).to_arrow()
    con = duckdb.connect()
    con.register("sample", sample)
    for name, column_type in candidates.items():
        column = '"' + name.replace('"', '""') + '"'
        rows_set, distinct = con.execute(f"SELECT count({column}), approx_count_distinct({column}) FROM sample").fetchone()
        profile = ColumnProfile(
            name=name, type=column_type, rows_sampled=sample.num_rows, rows_set=rows_set, distinct_values=distinct
        )
        if column_type in ("int", "long", "double"):
            profile.minimum, profile.maximum, profile.mean = con.execute(
                f"SELECT min({column}), max({column}), avg({column}) FROM sample"
            ).fetchone()
        elif column_type == "timestamp":
            low, high = con.execute(f"SELECT min({column}), max({column}) FROM sample").fetchone()
            profile.minimum, profile.maximum = str(low), str(high)
        else:
            profile.top_values = [
                (value if isinstance(value, bool) else str(value)[:80], count)
                for value, count in con.execute(
                    f"SELECT {column}, count(*) FROM sample WHERE {column} IS NOT NULL GROUP BY 1 ORDER BY 2 DESC, 1 LIMIT 5"
                ).fetchall()
            ]
        report.signals.append(new_column_signal(dataset.name, profile))
    return report, DriftSamples(recent=sample, earlier=sample.slice(0, 0))
