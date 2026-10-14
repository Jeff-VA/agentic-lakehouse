"""View results as files, for SQL clients that read URLs, such as DuckDB.

GET /views/<name> and /sql answer with format=csv or format=parquet
(agentic_lakehouse/api_gateway.py). Both formats keep a result's columns when
it has no rows: CSV always starts with its header row, and Parquet's schema
comes from the view's column types rather than from the values.

    CSV        times in ISO 8601 UTC (2026-10-12T00:00:00Z), true and false,
               a missing value as an empty field
    Parquet    string, number (int64 when every value is whole, else float64),
               boolean, timestamp (UTC, microseconds) and date columns;
               a column of unknown type holds the values as text, as in CSV
"""

import csv
import io
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from typing import Any, Literal

import pyarrow as pa
import pyarrow.parquet as pq

from agentic_lakehouse.views import ViewColumn

FileFormat = Literal["csv", "parquet"]
MEDIA_TYPES = {"json": "application/json", "csv": "text/csv", "parquet": "application/vnd.apache.parquet"}

Rows = Sequence[Mapping[str, Any]]


def render(file_format: FileFormat, columns: Sequence[ViewColumn], rows: Rows) -> bytes:
    """`rows` as a CSV or Parquet file with `columns`, in their order."""
    return to_csv(columns, rows) if file_format == "csv" else to_parquet(columns, rows)


def error_table(message: str) -> tuple[list[ViewColumn], list[dict[str, Any]]]:
    """A failed request as a result: one row, in one string column, `error`. A SQL client then
    shows the message, where an HTTP error would reach it as a bare status code."""
    return [ViewColumn(name="error", type="string", label="Error")], [{"error": message}]


def to_csv(columns: Sequence[ViewColumn], rows: Rows) -> bytes:
    """CSV with a header row, then a line per row (RFC 4180)."""
    out = io.StringIO()
    writer = csv.writer(out)
    writer.writerow([c.name for c in columns])
    writer.writerows([_text(row.get(c.name)) for c in columns] for row in rows)
    return out.getvalue().encode()


def to_parquet(columns: Sequence[ViewColumn], rows: Rows) -> bytes:
    """A Parquet file with a column per view column, typed by the view column's type."""
    arrays =[_array(column, [row.get(column.name) for row in rows]) for column in columns]
    table = pa.Table.from_arrays(arrays, names=[c.name for c in columns])
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink)
    return sink.getvalue().to_pybytes()


def _text(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, datetime):  # before date: a datetime is a date
        moment = value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)  # naive means UTC
        return moment.isoformat().replace("+00:00", "Z")
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def _array(column: ViewColumn, values: list[Any]) -> pa.Array:
    if column.type == "number":
        present = [v for v in values if v is not None]
        if present and all(isinstance(v, int) and not isinstance(v, bool) for v in present):
            return pa.array(values, pa.int64())
        return pa.array([None if v is None else float(v) for v in values], pa.float64())
    if column.type == "boolean":
        return pa.array(values, pa.bool_())
    if column.type == "timestamp":
        return pa.array(values, pa.timestamp("us", tz="UTC"))  # naive values are read as UTC
    if column.type == "date":
        return pa.array(values, pa.date32())
    return pa.array([_text(v) for v in values], pa.string())  # string, and unknown
