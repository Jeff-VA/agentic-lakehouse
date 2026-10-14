"""Stage 2 of a composed view: SQL over the results of its parts, in a sandboxed DuckDB.

A composed view (docs/design/sql-access.md) first answers each of its stage-1
parts in full: a semantic query, cut across the tiers by the router, or SQL
over tables on Trino. The rest of its SQL then runs here, over those results:
joins, window functions, comparisons between time windows. Nothing here can
double count across the tiers, because each part arrives already merged.

Every run gets a fresh in-memory DuckDB that can't read files, reach the
network or load extensions, with a memory limit, a thread limit and a
timeout. The SQL it runs was checked when the view loaded, but the sandbox
doesn't rely on that.
"""

import threading

import duckdb
import pyarrow as pa

# A part's columns as the views module types them, and the Arrow type an empty part gets
_ARROW = {
    "string": pa.string(),
    "number": pa.float64(),
    "boolean": pa.bool_(),
    "timestamp": pa.timestamp("us"),
    "date": pa.date32(),
    "unknown": pa.string(),
}


class StageTwoError(RuntimeError):
    """Stage 2 failed: invalid SQL, a type error, or it ran out of time or memory."""


def run(
    parts: dict[str, pa.Table], sql: str, *, timeout: float = 30, memory_limit: str = "256MB", threads: int = 2
) -> pa.Table:
    """Run `sql` (DuckDB's dialect) over `parts`, each registered under its name."""
    con = duckdb.connect(":memory:")
    try:
        for setting in (
            f"SET memory_limit = '{memory_limit}'",
            f"SET threads = {int(threads)}",
            "SET enable_external_access = false",  # no files, network or extensions
            "SET autoinstall_known_extensions = false",
            "SET autoload_known_extensions = false",
            "SET lock_configuration = true",  # and the SQL can't undo any of it
        ):
            con.execute(setting)
        for name, table in parts.items():
            con.register(name, table)
        timer = threading.Timer(timeout, con.interrupt)
        timer.start()
        try:
            return con.execute(sql).to_arrow_table()
        except duckdb.InterruptException:
            raise StageTwoError(f"it took longer than {timeout:g} seconds") from None
        except duckdb.Error as e:
            raise StageTwoError(str(e).split("\n")[0]) from None
        finally:
            timer.cancel()
    finally:
        con.close()


def check(schemas: dict[str, list[tuple[str, str]]], sql: str) -> list[tuple[str, str]]:
    """Run `sql` over empty parts with these columns, (name, value type) each: the columns the
    result has, with their value types. Raises StageTwoError naming what doesn't work."""
    result = run({name: empty(columns) for name, columns in schemas.items()}, sql, timeout=10)
    return [(field.name, value_type(field.type)) for field in result.schema]


def value_type(arrow_type: pa.DataType) -> str:
    """How the views module types a result column with this Arrow type."""
    if pa.types.is_integer(arrow_type) or pa.types.is_floating(arrow_type) or pa.types.is_decimal(arrow_type):
        return "number"
    if pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
        return "string"
    if pa.types.is_boolean(arrow_type):
        return "boolean"
    if pa.types.is_date(arrow_type):
        return "date"
    if pa.types.is_timestamp(arrow_type):
        return "timestamp"
    return "unknown"


def empty(columns: list[tuple[str, str]]) -> pa.Table:
    """A part with no rows: a table of these (name, value type) columns."""
    return pa.table({column: pa.array([], type=_ARROW.get(kind, pa.string())) for column, kind in columns})
