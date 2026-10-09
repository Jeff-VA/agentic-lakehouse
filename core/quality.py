"""Data quality gates for rollups: the contract for config/quality_rules.yaml.

A rollup run computes its new rows, audits them against the table's checks,
and only then publishes them (write-audit-publish). When a check with severity
`error` fails, the gold table keeps its last good contents and the run fails;
`warn` checks are reported but don't block.

Checks run in DuckDB over the new rows. A check passes or fails as a whole and
reports what it found, e.g. "3 rows have NULL isp_provider".
"""

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Annotated, Literal, Self, Union

import duckdb
import pyarrow as pa
from pydantic import Field, model_validator
from sqlglot import exp

from core.compiler import Scalar
from core.ontology import Identifier, SqlExpression, StrictModel, parse_fragment

Severity = Literal["error", "warn"]
TableName = Annotated[str, Field(pattern=r"^[a-z][a-z0-9_]*\.[a-z][a-z0-9_]*$")]  # <namespace>.<table>


class _Check(StrictModel):
    severity: Severity = "error"

    def referenced_columns(self) -> set[str]:
        """Rollup columns the check reads, so config loading can verify they exist."""
        return set()


class RowCount(_Check):
    """The run produced between `min` and `max` rows."""

    type: Literal["row_count"]
    min: int | None = Field(default=None, ge=0)
    max: int | None = Field(default=None, ge=0)


class NotNull(_Check):
    type: Literal["not_null"]
    columns: list[Identifier] = Field(min_length=1)

    def referenced_columns(self) -> set[str]:
        return set(self.columns)


class Unique(_Check):
    """No two rows share these columns' values (e.g. the time bucket and the dimensions)."""

    type: Literal["unique"]
    columns: list[Identifier] = Field(min_length=1)

    def referenced_columns(self) -> set[str]:
        return set(self.columns)


class Range(_Check):
    """Values of `column` lie within [min, max]. NULLs are left to not_null."""

    type: Literal["range"]
    column: Identifier
    min: float | None = None
    max: float | None = None

    @model_validator(mode="after")
    def _check_bounds(self) -> Self:
        if self.min is None and self.max is None:
            raise ValueError(f"range check on '{self.column}' needs `min`, `max` or both")
        return self

    def referenced_columns(self) -> set[str]:
        return {self.column}


class AcceptedValues(_Check):
    type: Literal["accepted_values"]
    column: Identifier
    values: list[Scalar] = Field(min_length=1)

    def referenced_columns(self) -> set[str]:
        return {self.column}


class Expression(_Check):
    """Every row satisfies a SQL condition over the rollup's columns (DuckDB
    dialect). Rows where it evaluates to NULL pass, as with SQL CHECK constraints."""

    type: Literal["expression"]
    description: str = Field(min_length=1)  # the invariant in words, shown when it fails
    sql: SqlExpression

    def referenced_columns(self) -> set[str]:
        return {column.name for column in parse_fragment(self.sql).find_all(exp.Column)}


class Freshness(_Check):
    """The newest time bucket is no more than `max_lag_hours` before the end of
    the window: catches data that silently stopped arriving."""

    type: Literal["freshness"]
    max_lag_hours: float = Field(gt=0)


Check = Annotated[
    Union[RowCount, NotNull, Unique, Range, AcceptedValues, Expression, Freshness], Field(discriminator="type")
]


class TableRules(StrictModel):
    table: TableName  # a rollup's gold table
    checks: list[Check] = Field(min_length=1)


class QualityConfig(StrictModel):
    version: str
    rules: list[TableRules] = []

    @model_validator(mode="after")
    def _check_one_entry_per_table(self) -> Self:
        tables = [r.table for r in self.rules]
        if repeated := sorted({t for t in tables if tables.count(t) > 1}):
            raise ValueError(f"quality rules list these tables more than once: {', '.join(repeated)}")
        return self

    def checks_for(self, table: str) -> list[Check]:
        return next((r.checks for r in self.rules if r.table == table), [])


@dataclass(frozen=True)
class CheckResult:
    check: str  # e.g. "not_null(isp_provider, event_count)"
    severity: Severity
    passed: bool
    detail: str


def blocking(results: list[CheckResult]) -> list[CheckResult]:
    """The failed checks that must stop the rows from being published."""
    return [r for r in results if not r.passed and r.severity == "error"]


def run_checks(rows: pa.Table, checks: list[Check], *, time_column: str, window_end: datetime) -> list[CheckResult]:
    """Run every check against `rows`, the rollup rows a run is about to publish."""
    con = duckdb.connect()
    con.register("rows", rows)

    def count(where: str) -> int:
        return con.execute(f"SELECT count(*) FROM rows WHERE {where}").fetchone()[0]

    results = []
    for check in checks:
        label, passed, detail = _run(check, rows, con, count, time_column, window_end)
        results.append(CheckResult(label, check.severity, passed, detail))
    return results


def _quote(name: str) -> str:
    return f'"{name}"'


def _rows(n: int) -> str:
    return f"{n} row" if n == 1 else f"{n} rows"


def _run(check, rows, con, count, time_column: str, window_end: datetime) -> tuple[str, bool, str]:
    match check:
        case RowCount(min=low, max=high):
            n = rows.num_rows
            label = f"row_count(min={'-' if low is None else low}, max={'-' if high is None else high})"
            return label, (low is None or n >= low) and (high is None or n <= high), _rows(n)
        case NotNull(columns=columns):
            nulls = {c: count(f"{_quote(c)} IS NULL") for c in columns}
            bad = {c: n for c, n in nulls.items() if n}
            detail = "; ".join(f"{_rows(n)} with NULL {c}" for c, n in bad.items()) or "no NULLs"
            return f"not_null({', '.join(columns)})", not bad, detail
        case Unique(columns=columns):
            keys = ", ".join(_quote(c) for c in columns)
            duplicated = con.execute(
                f"SELECT count(*) FROM (SELECT 1 FROM rows GROUP BY {keys} HAVING count(*) > 1)"
            ).fetchone()[0]
            return f"unique({', '.join(columns)})", not duplicated, f"{duplicated} duplicated keys" if duplicated else "all unique"
        case Range(column=column, min=low, max=high):
            conditions = []
            if low is not None:
                conditions.append(f"{_quote(column)} < {low!r}")
            if high is not None:
                conditions.append(f"{_quote(column)} > {high!r}")
            n = count(" OR ".join(conditions))
            label = f"range({column}, min={'-' if low is None else low}, max={'-' if high is None else high})"
            return label, not n, f"{_rows(n)} outside" if n else "all within"
        case AcceptedValues(column=column, values=values):
            accepted = ", ".join(exp.convert(v).sql("duckdb") for v in values)
            n = count(f"{_quote(column)} NOT IN ({accepted})")
            unexpected = [
                v
                for (v,) in con.execute(
                    f"SELECT DISTINCT {_quote(column)} FROM rows WHERE {_quote(column)} NOT IN ({accepted}) LIMIT 5"
                ).fetchall()
            ]
            detail = f"{_rows(n)} with unexpected values, e.g. {unexpected}" if n else "all accepted"
            return f"accepted_values({column})", not n, detail
        case Expression(description=description, sql=sql):
            condition = parse_fragment(sql).sql("duckdb")
            try:
                n = count(f"NOT ({condition})")
            except duckdb.Error as e:
                return f"expression({sql})", False, f"couldn't evaluate: {e}"
            return f"expression({sql})", not n, f"{_rows(n)} violating: {description}" if n else description
        case Freshness(max_lag_hours=max_lag):
            newest = con.execute(f"SELECT max({_quote(time_column)}) FROM rows").fetchone()[0]
            if newest is None:
                return f"freshness(<= {max_lag:g}h)", False, "no rows"
            lag = window_end - newest
            passed = lag <= timedelta(hours=max_lag)
            return f"freshness(<= {max_lag:g}h)", passed, f"newest bucket {newest:%Y-%m-%d %H:%M}, {lag} before the window end"
    raise TypeError(f"unknown check {check!r}")
