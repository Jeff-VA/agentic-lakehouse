"""Drift signals: what a dataset's data shows that its config may not reflect yet.

detect_drift() compares a dataset's Iceberg table with ingestion.yaml and the
ontology, and its last 24 hours of data with the 7 days before. The windows end
at the newest event, which the flush records, not at the wall clock, so a
stalled stream doesn't look as if every column stopped. Each finding is a
Signal, with a key that stays the same from run to run:

- New column: a field producers send that ingestion.yaml doesn't list,
  profiled from rows where it's set. One whose first values arrive as a fading
  column of the same type stops is reported as possibly replacing it.
- Fading column: a configured column set on far fewer recent events than the
  earlier window predicts. Read from Iceberg's per-file metrics alone; a file
  belongs to the window that holds the middle of its time range.
- New value: a value of an enumerated column (text that a definition compares
  with string literals, also inside a macro) seen in the recent window but not
  the earlier one, unless every definition that compares the column already
  names it. Only those columns are read, up to a cap per window, and a
  candidate the cap may have hidden is confirmed with a targeted scan. A column
  with more than 200 different recent values isn't a set of categories, and is
  left out.

Fading columns and new values list the definitions that read the column. The
samples hold rows of both windows and rows for each signal, to check a
proposal on. Columns config can't express, and type mismatches, are reported
for a person.
"""

import re
from collections import Counter
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from functools import reduce
from typing import Literal

import duckdb
import pyarrow as pa
import pyarrow.compute as pc
from pydantic import BaseModel
from pyiceberg.expressions import And, BooleanExpression, EqualTo, GreaterThanOrEqual, LessThan, NotNull, Or
from pyiceberg.table import Table
from pyiceberg.types import BooleanType, DoubleType, FloatType, IntegerType, LongType, StringType, TimestampType
from sqlglot import exp

from agentic_lakehouse.config import ColumnType, Dataset, Platform
from agentic_lakehouse.flush import NEWEST
from agentic_lakehouse.macros import ProjectMacros
from agentic_lakehouse.ontology import AggregateMetric, parse_fragment

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
# One type for rename hints: the flush makes every new numeric field a double, so a renamed int column arrives as one.
_NUMBERS = {"int", "long", "double"}
# A macro call in a snippet: {{ name(...) }} or {{ adapter.name(...) }}
_MACRO_CALL = re.compile(r"\{\{-?\s*(?:adapter\.)?([A-Za-z_]\w*)\s*\(")

_MIN_EXPECTED = 50  # a column is fading only if the earlier share predicts at least this many recent rows set
_FADED = 0.25  # ... and fewer than this share of them arrived
_RENAME_GAP = timedelta(hours=2)  # how close a new column's first values must be to a fading column's last
_MAX_NEW_VALUES = 20  # per column, the most frequent first
_MAX_DISTINCT = 200  # a column with more different recent values isn't a set of categories: not watched
_MAX_CHECKED = 50  # targeted scans per column, when the earlier window was read only up to the cap
_VALUE_ROWS = 100  # sampled rows with each new value
_FADING_ROWS = 200  # sampled earlier rows that set each fading column
_TICK = timedelta(microseconds=1)  # Iceberg's timestamp resolution: a window ending at newest + _TICK holds the newest event

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
    earlier_start: datetime | None = None  # the earlier window is [earlier_start, window_start)
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
    """Rows, with all of the table's columns, to check a proposal on and to show its effect. Each
    holds a sample of its window first, then the rows for signals it doesn't already hold; no row
    appears twice."""

    recent: pa.Table  # the recent window, then rows where each new column is set and rows with each new value
    earlier: pa.Table  # the window before it, then rows where each fading column is set; no rows when there's no earlier data


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


def detect_drift(
    dataset_name: str,
    platform: Platform,
    table: Table,
    sample_rows: int = 5_000,
    *,
    now: datetime | None = None,
    recent: timedelta = timedelta(hours=24),
    earlier: timedelta = timedelta(days=7),
    min_recent_rows: int = 500,
    min_value_rows: int = 5,
    value_scan_limit: int = 2_000_000,
) -> tuple[DriftReport, DriftSamples | None]:
    """Compare `table` with the dataset's config, and its last `recent` of events with the
    `earlier` before that.

    The windows end just after the newest event, which the flush records in a table property
    (without one, the file metrics show it); `now` (naive UTC) overrides that, and a table with
    no events uses the wall clock. Columns are judged fading only with `min_recent_rows` recent
    events; a new value needs `min_value_rows`, and each window's values are read from at most
    `value_scan_limit` rows. Returns the report and, when there are signals, samples of rows: up
    to `sample_rows` of each window, plus rows for each signal.
    """
    dataset = platform.dataset(dataset_name)
    configured = {c.name: c.type for c in dataset.columns}
    live = {f.name: _COLUMN_TYPES.get(type(f.field_type)) for f in table.schema().fields}
    report = DriftReport(dataset=dataset.name, table=".".join(table.name()))
    new_columns = _new_columns(table, configured, live, report)

    files = _data_files(table, dataset.time_column)
    end = _window_end(table, files, now)
    recent_window = _Window(dataset.time_column, end - recent, end)
    earlier_window = _Window(dataset.time_column, end - recent - earlier, end - recent)
    report.earlier_start, report.window_start, report.window_end = earlier_window.start, recent_window.start, end

    new_signals, profiled = _profile(table, dataset.name, new_columns, sample_rows) if new_columns else ([], None)
    fading = _fading_columns(dataset, files, recent_window, earlier_window, min_recent_rows, report)
    _hint_renames(new_signals, fading, files, configured | new_columns)
    reads = _reads(platform, dataset.name)
    text_columns = [c.name for c in dataset.columns if c.type == "string" and live.get(c.name) == "string"]
    enumerated = _enumerated(reads, text_columns)
    new_values = _new_values(table, dataset.name, enumerated, recent_window, earlier_window, min_value_rows, value_scan_limit, report)
    for signal in fading + new_values:
        signal.read_by = _read_by(reads, signal.column)
    report.signals = new_signals + fading + new_values
    if not report.signals:
        return report, None
    return report, _samples(table, report.signals, profiled, recent_window, earlier_window, sample_rows)


def _new_columns(table: Table, configured: dict[str, str], live: dict[str, str | None], report: DriftReport) -> dict[str, str]:
    """The table's columns that ingestion.yaml doesn't list and could, with their config types, in
    schema order. Type mismatches, and columns config can't express, go in the report."""
    candidates = {}
    for field in table.schema().fields:
        live_type = live[field.name]
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
    return candidates


def _profile(table: Table, dataset: str, columns: dict[str, str], sample_rows: int) -> tuple[list[Signal], pa.Table]:
    """A new_column signal for each of `columns`, profiled from rows where at least one is set;
    and those rows."""
    sample = table.scan(row_filter=reduce(Or, [NotNull(name) for name in columns]), limit=sample_rows).to_arrow()
    con = duckdb.connect()
    con.register("sample", sample)
    signals = []
    for name, column_type in columns.items():
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
        signals.append(new_column_signal(dataset, profile))
    return signals, sample


# Column health, from file metrics


@dataclass
class _DataFile:
    """What Iceberg's metrics say about one data file, without reading it."""

    rows: int
    first: datetime | None  # the time column's bounds
    last: datetime | None
    set_rows: dict[str, int]  # rows that set each column: 0 for a column added after the file was written

    @property
    def middle(self) -> datetime | None:
        return None if self.first is None or self.last is None else self.first + (self.last - self.first) / 2


def _data_files(table: Table, time_column: str) -> list[_DataFile]:
    files = table.inspect.files()
    files = files.filter(pc.equal(files["content"], 0))  # data files: a delete file's record count is of deleted rows
    metrics = files["readable_metrics"]  # per column of the current schema; empty for a column the file predates

    def metric(column: str, name: str) -> list:
        return pc.struct_field(metrics, [column, name]).to_pylist()

    columns = [f.name for f in table.schema().fields]
    set_rows = {
        name: [(values or 0) - (nulls or 0) for values, nulls in zip(metric(name, "value_count"), metric(name, "null_value_count"))]
        for name in columns
    }
    first, last = metric(time_column, "lower_bound"), metric(time_column, "upper_bound")
    return [
        _DataFile(rows, first[i], last[i], {name: set_rows[name][i] for name in columns})
        for i, rows in enumerate(files["record_count"].to_pylist())
    ]


def _window_end(table: Table, files: list[_DataFile], now: datetime | None) -> datetime:
    """Just after the newest event: the flush records it as a table property; without one, the
    file metrics tell. With no events at all, the wall clock."""
    if now is not None:
        return now
    if NEWEST in table.properties:
        newest = datetime.fromisoformat(table.properties[NEWEST])
    else:
        newest = max((f.last for f in files if f.last is not None), default=None)
    return datetime.now(UTC).replace(tzinfo=None) if newest is None else newest + _TICK


@dataclass(frozen=True)
class _Window:
    """[start, end) of the dataset's time column."""

    time_column: str
    start: datetime
    end: datetime

    @property
    def rows(self) -> BooleanExpression:
        """A row filter for the window."""
        return And(GreaterThanOrEqual(self.time_column, self.start), LessThan(self.time_column, self.end))

    def holds(self, file: _DataFile) -> bool:
        return file.middle is not None and self.start <= file.middle < self.end

    @property
    def length(self) -> str:
        """As details say it: '24 hours', '7 days', 'hour'."""
        hours = (self.end - self.start) / timedelta(hours=1)
        if hours > 24 and hours % 24 == 0:
            count, unit = int(hours // 24), "day"
        elif hours == int(hours):
            count, unit = int(hours), "hour"
        else:
            count, unit = round(hours * 60), "minute"
        return unit if count == 1 else f"{count} {unit}s"


def _fading_columns(
    dataset: Dataset, files: list[_DataFile], recent: _Window, earlier: _Window, min_recent_rows: int, report: DriftReport
) -> list[Signal]:
    """Configured columns set on far fewer recent rows than the earlier window's share predicts, in
    config order."""
    recent_files = [f for f in files if recent.holds(f)]
    earlier_files = [f for f in files if earlier.holds(f)]
    recent_rows, earlier_rows = sum(f.rows for f in recent_files), sum(f.rows for f in earlier_files)
    if recent_rows < min_recent_rows:
        report.notes.append(
            f"fading columns not judged: {recent_rows:,} events in the last {recent.length}, fewer than {min_recent_rows:,}"
        )
        return []
    if not earlier_rows:
        report.notes.append(f"fading columns not judged: no events in the {earlier.length} before the last {recent.length}")
        return []
    signals = []
    for name in [c.name for c in dataset.columns if c.name != dataset.time_column]:
        earlier_set = sum(f.set_rows.get(name, 0) for f in earlier_files)
        recent_set = sum(f.set_rows.get(name, 0) for f in recent_files)
        expected = earlier_set / earlier_rows * recent_rows
        if expected < _MIN_EXPECTED or recent_set >= expected * _FADED:
            continue
        share_earlier, share_recent = earlier_set / earlier_rows, recent_set / recent_rows
        last_seen = max((f.last for f in files if f.set_rows.get(name) and f.last is not None), default=None)
        detail = (
            f"column {name} set on {_percent(share_recent)} of {recent_rows:,} events in the last {recent.length}, "
            f"{_percent(share_earlier)} in the {earlier.length} before"
        )
        if last_seen is not None:
            detail += f"; last set {last_seen:%Y-%m-%d %H:%M}"
        signals.append(
            Signal(
                key=f"{dataset.name}:fading_column:{name}",
                kind="fading_column",
                column=name,
                detail=detail,
                share_earlier=share_earlier,
                share_recent=share_recent,
                last_seen=last_seen,
            )
        )
    return signals


def _hint_renames(new: list[Signal], fading: list[Signal], files: list[_DataFile], types: dict[str, str]) -> None:
    """Mark a new column whose first values arrive within two hours of a fading column's last ones,
    with the same type, as possibly replacing it, and say so in both details."""
    for signal in new:
        first = min((f.first for f in files if f.set_rows.get(signal.column) and f.first is not None), default=None)
        if first is None:
            continue
        near = [
            old
            for old in fading
            if old.last_seen is not None
            and abs(first - old.last_seen) <= _RENAME_GAP
            and _same_type(types[old.column], types[signal.column])
        ]
        if near:
            old = min(near, key=lambda old: abs(first - old.last_seen))
            signal.replaces = old.column
            signal.detail += (
                f"; first set {first:%Y-%m-%d %H:%M}, near when column {old.column} was last set "
                f"({old.last_seen:%Y-%m-%d %H:%M}): it may replace {old.column}"
            )
            old.detail += f"; new column {signal.column}, first set {first:%Y-%m-%d %H:%M}, may replace it"


def _same_type(a: ColumnType, b: ColumnType) -> bool:
    return a == b or {a, b} <= _NUMBERS


# What the ontology reads: lineage and enumerated columns


@dataclass
class _Read:
    """A definition's read of the dataset's columns, through one of its SQL snippets."""

    definition: str  # e.g. "metric edits.human_edits"
    columns: set[str]
    macros: list[str]  # the macros the snippet calls, which the read may go through
    literals: dict[str, list[str]]  # column -> the string literals the snippet compares it with


def _reads(platform: Platform, dataset: str) -> list[_Read]:
    """The reads of every dimension and metric of the models over `dataset`, in ontology order."""
    macros = platform.ontology.macros
    reads = []
    for model in platform.ontology.models:
        if model.source != dataset:
            continue
        for dimension in model.dimensions:
            definition = f"dimension {model.name}.{dimension.name}"
            if dimension.sql is None:  # the column of its own name
                reads.append(_Read(definition, {dimension.name}, [], {}))
            else:
                reads.append(_read(definition, dimension.sql, macros))
        for metric in model.metrics:
            if isinstance(metric, AggregateMetric):  # a ratio divides two metrics and reads no columns itself
                snippets = ([metric.sql] if metric.sql else []) + metric.filters
                reads += [_read(f"metric {model.name}.{metric.name}", sql, macros) for sql in snippets]
    return reads


def _read(definition: str, sql: str, macros: ProjectMacros) -> _Read:
    """What one of `definition`'s snippets reads, and what it compares with text."""
    # Rendered for DuckDB, the ontology's dialect, so the columns and comparisons inside macros count.
    tree = parse_fragment(macros.render(sql, "duckdb"))
    literals: dict[str, list[str]] = {}
    for column, literal in _comparisons(tree):
        known = literals.setdefault(column, [])
        if literal not in known:
            known.append(literal)
    columns = {column.name for column in tree.find_all(exp.Column)}
    return _Read(definition, columns, list(dict.fromkeys(_MACRO_CALL.findall(sql))), literals)


def _comparisons(tree: exp.Expression) -> Iterator[tuple[str, str]]:
    """(column, text) for each comparison of a column with a string literal: col = 'a', col <> 'a',
    col IN ('a', ...), CASE col WHEN 'a' ..."""
    for node in tree.find_all(exp.EQ, exp.NEQ, exp.In, exp.Case):
        if isinstance(node, exp.In):
            pairs = [(node.this, value) for value in node.expressions]
        elif isinstance(node, exp.Case):  # a searched CASE (CASE WHEN ...) has no operand
            pairs = [(node.this, branch.this) for branch in node.args.get("ifs") or []] if node.this else []
        else:
            pairs = [(node.this, node.expression), (node.expression, node.this)]
        for column, literal in pairs:
            column, literal = column.unnest(), literal.unnest()
            if isinstance(column, exp.Column) and isinstance(literal, exp.Literal) and literal.is_string:
                yield column.name, literal.this


def _read_by(reads: list[_Read], column: str) -> list[str]:
    """The definitions that read `column`, each with the macros it reads it through."""
    via: dict[str, list[str]] = {}
    for read in reads:
        if column in read.columns:
            macros = via.setdefault(read.definition, [])
            macros += [m for m in read.macros if m not in macros]
    return [definition + (", via " + ", ".join(f"{m}()" for m in macros) if macros else "") for definition, macros in via.items()]


@dataclass
class _Enumerated:
    """A text column that definitions compare with string literals."""

    literals: list[str]  # every literal it's compared with
    definitions: list[str]  # the definitions that compare it
    named_by_all: set[str]  # the literals every one of those definitions names: values they expect


def _enumerated(reads: list[_Read], columns: list[str]) -> dict[str, _Enumerated]:
    """Which of `columns` (text) definitions compare with string literals, in the order of `columns`."""
    named: dict[str, dict[str, set[str]]] = {}  # column -> definition -> the literals it names
    found: dict[str, _Enumerated] = {}
    for read in reads:
        for column, literals in read.literals.items():
            if column in columns:
                enumerated = found.setdefault(column, _Enumerated([], [], set()))
                enumerated.literals += [literal for literal in literals if literal not in enumerated.literals]
                if read.definition not in enumerated.definitions:
                    enumerated.definitions.append(read.definition)
                named.setdefault(column, {}).setdefault(read.definition, set()).update(literals)
    for column, enumerated in found.items():
        enumerated.named_by_all = set.intersection(*named[column].values())
    return {column: found[column] for column in columns if column in found}


# New values in enumerated columns


def _new_values(
    table: Table,
    dataset: str,
    enumerated: dict[str, _Enumerated],
    recent: _Window,
    earlier: _Window,
    min_value_rows: int,
    limit: int,
    report: DriftReport,
) -> list[Signal]:
    """Values with at least `min_value_rows` recent rows that the earlier window doesn't have, and
    that not every definition comparing the column names already: up to 20 per column, by column,
    then by rows, most first."""
    if not enumerated:
        return []
    counts, read, capped = _count_values(table, list(enumerated), recent.rows, limit)
    if capped:
        report.notes.append(f"new values counted in the first {read:,} events of the last {recent.length}")
    for column in list(enumerated):
        if len(counts[column]) > _MAX_DISTINCT:  # e.g. a title compared with one literal: not categories
            report.notes.append(
                f"new values not watched in {column}: {len(counts[column]):,} different values in the last {recent.length}"
            )
            del enumerated[column]
    candidates = {
        column: values
        for column in enumerated
        if (
            values := {
                value: rows
                for value, rows in counts[column].items()
                if rows >= min_value_rows and value not in enumerated[column].named_by_all
            }
        )
    }
    if not candidates:
        return []
    missing, read, capped = _missing_values(table, candidates, earlier.rows, limit)
    if not read:
        report.notes.append(f"new values not judged: no events in the {earlier.length} before the last {recent.length}")
        return []
    signals = []
    for column, values in missing.items():
        literals, definitions = enumerated[column].literals, enumerated[column].definitions
        compared = ", ".join(f"'{literal}'" for literal in literals[:10]) + (", ..." if len(literals) > 10 else "")
        new = []
        for value in sorted(values, key=lambda value: (-candidates[column][value], value))[:_MAX_CHECKED]:
            if len(new) == _MAX_NEW_VALUES:
                break
            if not capped or not _occurs(table, column, value, earlier.rows):  # the rows the cap left unread may have it
                new.append(value)
        for value in new:
            rows = candidates[column][value]
            signals.append(
                Signal(
                    key=f"{dataset}:new_value:{column}={value}",
                    kind="new_value",
                    column=column,
                    value=value,
                    rows=rows,
                    detail=(
                        f"new value '{value[:80]}' in {column}: {rows:,} event{'s' if rows != 1 else ''} in the last "
                        f"{recent.length}, none in the {earlier.length} before; compared with {compared} in {', '.join(definitions)}"
                    ),
                )
            )
    return signals


def _count_values(table: Table, columns: list[str], rows: BooleanExpression, limit: int) -> tuple[dict[str, Counter], int, bool]:
    """Each column's values, with their counts, in up to `limit` of the rows `rows` selects; how many
    rows were read; and whether the limit cut the read short."""
    counts: dict[str, Counter] = {column: Counter() for column in columns}
    read, capped = 0, False
    # Reads these columns, and the time column for the filter: nothing else.
    with table.scan(row_filter=rows, selected_fields=tuple(columns)).to_arrow_batch_reader() as reader:
        for batch in reader:
            if batch.num_rows > limit - read:
                batch, capped = batch.slice(0, limit - read), True
            read += batch.num_rows
            for column in columns:
                for item in pc.value_counts(batch.column(column)).to_pylist():
                    if item["values"] is not None:
                        counts[column][item["values"]] += item["counts"]
            if capped:
                break
    return counts, read, capped


def _missing_values(
    table: Table, candidates: dict[str, dict[str, int]], rows: BooleanExpression, limit: int
) -> tuple[dict[str, set[str]], int, bool]:
    """The candidate values not found in up to `limit` of the rows `rows` selects, by column; how many
    rows were read; and whether the limit cut the read short, so the unread rows may have some. Stops
    as soon as every candidate is found."""
    missing = {column: set(values) for column, values in candidates.items()}
    read, capped = 0, False
    with table.scan(row_filter=rows, selected_fields=tuple(missing)).to_arrow_batch_reader() as reader:
        for batch in reader:
            if batch.num_rows > limit - read:
                batch, capped = batch.slice(0, limit - read), True
            read += batch.num_rows
            for column, values in missing.items():
                if values:
                    seen = batch.column(column)
                    values -= set(pc.unique(seen.filter(pc.is_in(seen, value_set=pa.array(list(values), seen.type)))).to_pylist())
            if capped or not any(missing.values()):
                break
    return missing, read, capped


def _occurs(table: Table, column: str, value: str, rows: BooleanExpression) -> bool:
    """Whether a row `rows` selects has `value` in `column`: a targeted scan, which file metrics prune."""
    return table.scan(row_filter=And(EqualTo(column, value), rows), selected_fields=(column,), limit=1).to_arrow().num_rows > 0


def _samples(
    table: Table, signals: list[Signal], profiled: pa.Table | None, recent: _Window, earlier: _Window, sample_rows: int
) -> DriftSamples:
    """Up to `sample_rows` rows of each window, then rows for each signal: where the new columns are
    set (`profiled`), with each new value, and where each fading column was set."""
    recent_rows = [table.scan(row_filter=recent.rows, limit=sample_rows).to_arrow()]
    if profiled is not None:
        recent_rows.append(profiled)
    earlier_rows = [table.scan(row_filter=earlier.rows, limit=sample_rows).to_arrow()]
    for signal in signals:
        if signal.kind == "new_value":
            in_recent = And(EqualTo(signal.column, signal.value), recent.rows)
            recent_rows.append(table.scan(row_filter=in_recent, limit=_VALUE_ROWS).to_arrow())
        elif signal.kind == "fading_column":
            in_earlier = And(NotNull(signal.column), earlier.rows)
            earlier_rows.append(table.scan(row_filter=in_earlier, limit=_FADING_ROWS).to_arrow())
    return DriftSamples(recent=_distinct(recent_rows), earlier=_distinct(earlier_rows))


def _distinct(scans: list[pa.Table]) -> pa.Table:
    """The scans' rows, each once, in order: a signal's rows are often in its window's sample too."""
    # A column that some files predate can come back as string from one scan, large_string from another.
    rows = pa.concat_tables(scans, promote_options="permissive")
    return rows.group_by(rows.column_names, use_threads=False).aggregate([])  # keeps first occurrences, in order


def _percent(share: float) -> str:
    return "<1%" if 0 < share < 0.01 else f"{share:.0%}"
