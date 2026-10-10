"""Run SQL on an engine and get the result back as an Arrow table.

Each engine reports column types its own way (Trino type names, Pinot JSON
with timestamps as strings). These clients convert results to plain Arrow
types so results from different engines can be compared or combined directly.
"""

import math
from datetime import datetime
from typing import Protocol

import duckdb
import httpx
import pyarrow as pa
import trino

from core.config import TrinoEndpoint


class EngineError(RuntimeError):
    """The engine rejected or failed the query."""


def plain_types(table: pa.Table) -> pa.Table:
    """Replace decimal columns with int64 (whole numbers) or float64.

    DuckDB sums BIGINTs into HUGEINT, which Arrow represents as a decimal and
    JSON serializers turn into strings; every other engine returns plain numbers.
    """
    for i, field in enumerate(table.schema):
        if pa.types.is_decimal(field.type):
            target = pa.int64() if field.type.scale == 0 else pa.float64()
            table = table.set_column(i, field.name, table.column(i).cast(target))
    return table


class QueryEngine(Protocol):
    name: str

    def execute(self, sql: str) -> pa.Table: ...


class DuckDBEngine:
    name = "duckdb"

    def __init__(self, con: duckdb.DuckDBPyConnection | None = None):
        self.con = con or duckdb.connect()

    def execute(self, sql: str) -> pa.Table:
        try:
            return plain_types(self.con.execute(sql).to_arrow_table())
        except duckdb.Error as e:
            raise EngineError(f"duckdb: {e}") from e


# Trino type name (before any parameters) -> Arrow type.
_TRINO_TYPES = {
    "boolean": pa.bool_(),
    "tinyint": pa.int8(),
    "smallint": pa.int16(),
    "integer": pa.int32(),
    "bigint": pa.int64(),
    "real": pa.float32(),
    "double": pa.float64(),
    "varchar": pa.string(),
    "char": pa.string(),
    "date": pa.date32(),
    "timestamp": pa.timestamp("us"),
}


class TrinoEngine:
    name = "trino"

    def __init__(self, endpoint: TrinoEndpoint):
        self.endpoint = endpoint

    def execute(self, sql: str) -> pa.Table:
        conn = trino.dbapi.connect(
            host=self.endpoint.host,
            port=self.endpoint.port,
            user=self.endpoint.user,
            catalog=self.endpoint.catalog,
            timezone="UTC",  # like every time in the platform, so now() compares right with stored timestamps
        )
        try:
            cursor = conn.cursor()
            cursor.execute(sql)
            rows = cursor.fetchall()
            description = cursor.description
        except trino.exceptions.TrinoQueryError as e:
            raise EngineError(f"trino: {e.message}") from e
        except trino.exceptions.Error as e:  # e.g. Trino unreachable
            raise EngineError(f"trino: {e}") from e
        finally:
            conn.close()
        return plain_types(
            pa.table(
                {
                    column[0]: pa.array([row[i] for row in rows], type=_TRINO_TYPES.get(column[1].split("(")[0]))
                    for i, column in enumerate(description)
                }
            )
        )


# Pinot column data type -> Arrow type.
_PINOT_TYPES = {
    "BOOLEAN": pa.bool_(),
    "INT": pa.int32(),
    "LONG": pa.int64(),
    "FLOAT": pa.float32(),
    "DOUBLE": pa.float64(),
    "STRING": pa.string(),
    "TIMESTAMP": pa.timestamp("ms"),
}


def _non_finite(value) -> bool:
    if isinstance(value, str):
        return value in ("NaN", "Infinity", "-Infinity")
    return isinstance(value, float) and not math.isfinite(value)


class PinotEngine:
    name = "pinot"

    def __init__(self, broker_url: str, client: httpx.Client | None = None):
        self.broker_url = broker_url.rstrip("/")
        self.client = client or httpx.Client(timeout=30)

    def execute(self, sql: str) -> pa.Table:
        # The SQL from core/templates/macros/pinot.sql needs the multi-stage engine. Without null
        # handling, MIN and MAX over no rows return integer sentinels instead of NULL.
        request = {"sql": sql, "queryOptions": "useMultistageEngine=true;enableNullHandling=true"}
        try:
            response = self.client.post(f"{self.broker_url}/query/sql", json=request)
            response.raise_for_status()
        except httpx.HTTPError as e:
            raise EngineError(f"pinot: {e}") from e
        body = response.json()
        if body.get("exceptions"):
            raise EngineError("pinot: " + "; ".join(e["message"] for e in body["exceptions"]))
        result = body["resultTable"]
        schema = result["dataSchema"]
        columns = {}
        for i, (name, pinot_type) in enumerate(zip(schema["columnNames"], schema["columnDataTypes"])):
            values = [row[i] for row in result["rows"]]
            try:
                if pinot_type == "TIMESTAMP":  # Pinot sends timestamps as "2026-10-06 01:00:00.0"
                    values = [None if v is None else datetime.fromisoformat(v) for v in values]
                elif pinot_type in ("DOUBLE", "FLOAT"):  # e.g. a percentile over no rows is "NaN"
                    values = [None if _non_finite(v) else v for v in values]
                columns[name] = pa.array(values, type=_PINOT_TYPES.get(pinot_type))
            except (ValueError, TypeError, pa.ArrowException) as e:
                raise EngineError(f"pinot: column '{name}' has a value that isn't {pinot_type}: {e}") from e
        return pa.table(columns)
