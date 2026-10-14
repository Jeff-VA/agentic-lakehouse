"""Views: named SQL queries that dashboards and agents read by name.

A view is a .sql file in the project's views/ folder (config/views/<name>.sql):
a comment block of YAML describing it, then one SELECT in Trino's dialect.

    /*
    description: Playback quality per ISP over the last hour.
    params:
      minutes: {type: int, default: 60, min: 5, max: 1440}
    */
    SELECT isp_provider, rebuffer_incidence_rate, p99_ttfb
    FROM playback_telemetry
    WHERE event_ts >= now() - INTERVAL :minutes MINUTE

What the SELECT reads decides how it runs:

- FROM an ontology model (as above): columns are the model's metrics and
  dimensions by name, so metric math stays defined once, in ontology.yaml. The
  view becomes a MetricQuery that the tier router answers from Pinot, Iceberg
  or both, so it includes events from seconds ago. `date_trunc('hour', <time
  dimension>)` groups by time. WHERE bounds the time dimension with >= and <
  (half-open; the end defaults to now) and ANDs comparisons and IN lists on
  dimensions. ORDER BY and LIMIT work as usual; GROUP BY is implied by the
  selected dimensions, so it may be left out.
- FROM Iceberg tables, written <namespace>.<table> (each dataset's raw table and
  the gold rollups in storage.yaml): any SELECT, run on Trino as written. Its
  data ends where each table's last flush or rollup run did.
- Composed (docs/design/sql-access.md): a WITH query whose CTEs read a model
  (as above), or tables (as above), or other CTEs. The model and table parts
  are answered first, in full; the rest of the SQL then runs over their
  results in a sandboxed DuckDB (agentic_lakehouse/compose.py): joins, window
  functions, comparing one time window with another. Metrics are still cut
  across the tiers only by the router, so nothing is counted twice.

The header can also set `refresh_seconds`: how often a live view rechecks its
data (agentic_lakehouse/live.py), and `columns`: how to read columns the
ontology doesn't describe, such as one a view over tables computes:

    columns:
      accuracy: {label: Accuracy, format: percent}

Parameters are written :name and declared in the header
with a type and an optional default. Values are bound as SQL literals, never pasted in as text. A
parameter declared with `default: null` is optional: in a view over a model, a
dimension condition comparing with it is dropped while it has no value, which
is how a dashboard offers "all ISPs".

load_views() checks every view when the platform loads, naming the file in each
error: one SELECT, reading known models or tables, using known columns. Each
view's output columns are known up front, so dashboards can be checked against
them too.
"""

import math
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Literal, get_args

import sqlglot
import yaml
from pydantic import BaseModel, Field, ValidationError, model_validator
from sqlglot import exp
from sqlglot.errors import SqlglotError
from sqlglot.optimizer.annotate_types import annotate_types
from sqlglot.optimizer.qualify import qualify

from agentic_lakehouse import compose
from agentic_lakehouse.compiler import Filter, Grain, MetricQuery, OrderBy, QueryError, SqlCompiler, bucket_start
from agentic_lakehouse.ontology import (
    AggregateMetric,
    Better,
    Dimension,
    Format,
    Identifier,
    Metric,
    Ontology,
    SemanticModel,
    StrictModel,
    display_format,
    words,
)

VIEW_DIALECT = "trino"  # views over tables run on Trino as written
VIEW_NAME = re.compile(r"^[a-z][a-z0-9_]*$")
RESERVED_PARAMS = {"include_sql", "format", "errors", "q"}  # the gateway's own query-string options

ParamType = Literal["int", "float", "string", "boolean", "timestamp"]
ValueType = Literal["string", "number", "boolean", "timestamp", "date", "unknown"]
Role = Literal["time", "dimension", "metric"]
Value = bool | int | float | str | datetime | None

# Each table a view can read: "namespace.table" -> {column: SQL type}.
TableSchemas = Mapping[str, Mapping[str, str]]


class ViewError(ValueError):
    """A view file is invalid, or a request gave a view bad parameters."""


def _as_int(raw: Any) -> int:
    if isinstance(raw, bool) or (isinstance(raw, float) and not raw.is_integer()):
        raise ValueError
    return int(raw.strip()) if isinstance(raw, str) else int(raw)


def _as_float(raw: Any) -> float:
    if isinstance(raw, bool):
        raise ValueError
    value = float(raw)
    if not math.isfinite(value):
        raise ValueError
    return value


def _as_string(raw: Any) -> str:
    if not isinstance(raw, str):
        raise ValueError
    return raw


def _as_boolean(raw: Any) -> bool:
    if isinstance(raw, bool):
        return raw
    if isinstance(raw, str) and raw.lower() in ("true", "1", "false", "0"):
        return raw.lower() in ("true", "1")
    raise ValueError


def _as_timestamp(raw: Any) -> datetime:
    value = raw if isinstance(raw, datetime) else datetime.fromisoformat(raw)
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


_CONVERT = {"int": _as_int, "float": _as_float, "string": _as_string, "boolean": _as_boolean, "timestamp": _as_timestamp}
_DESCRIBE = {"int": "a whole number", "float": "a number", "string": "text", "boolean": "true or false", "timestamp": "an ISO 8601 time"}


class ViewParam(StrictModel):
    """A value the view's SQL refers to as :name, supplied with each request."""

    type: ParamType
    description: str = ""
    default: Value = None  # leave it out to make the parameter required; `null` makes it optional
    min: float | None = None
    max: float | None = None
    values: list[bool | int | float | str] | None = None  # the only values allowed; a dashboard can offer them

    @property
    def required(self) -> bool:
        return "default" not in self.model_fields_set

    @model_validator(mode="after")
    def _check(self) -> "ViewParam":
        if (self.min is not None or self.max is not None) and self.type not in ("int", "float"):
            raise ValueError("min and max only apply to int and float parameters")
        if self.values is not None:
            if not self.values:
                raise ValueError("values must list at least one value")
            for value in self.values:
                self.convert(value, "each of values", check_allowed=False)
        if self.default is not None:
            self.convert(self.default, "default")
        return self

    def convert(self, raw: Any, label: str, *, check_allowed: bool = True) -> Any:
        """`raw` (from JSON, YAML or a query string) as this parameter's type; "" and None mean no value.
        Errors start with `label`, e.g. "parameter 'hours'"."""
        if raw is None or raw == "":
            return None
        try:
            value = _CONVERT[self.type](raw)
        except (ValueError, TypeError):
            raise ViewError(f"{label} must be {_DESCRIBE[self.type]}, not {raw!r}") from None
        if self.min is not None and value < self.min:
            raise ViewError(f"{label} must be at least {self.min:g}, not {value}")
        if self.max is not None and value > self.max:
            raise ViewError(f"{label} must be at most {self.max:g}, not {value}")
        if check_allowed and self.values is not None and value not in self.values:
            raise ViewError(f"{label} must be one of {', '.join(map(str, self.values))}, not {value!r}")
        return value


class ColumnDisplay(StrictModel):
    """How people read one of a view's columns, set in its header: for a column the ontology
    doesn't describe (one the view computes, say), or to say something else than it does."""

    label: str = ""
    format: Format | None = None
    unit: str = ""
    better: Better | None = None


class ViewHeader(StrictModel):
    description: str = Field(min_length=1)  # agents and people pick views by reading this
    params: dict[Identifier, ViewParam] = {}
    columns: dict[str, ColumnDisplay] = {}  # by column name
    refresh_seconds: float | None = Field(default=None, ge=1)  # how often a live view rechecks (agentic_lakehouse/live.py)


class ViewColumn(BaseModel):
    """One column of a view's result, and how people read it (from the ontology, when the column
    holds a metric or dimension)."""

    name: str
    type: ValueType
    label: str  # the metric's or dimension's label, else the name in words
    role: Role | None = None  # views over models: whether the column is the time bucket, a dimension or a metric
    format: Format | None = None  # metrics: number, integer or percent
    unit: str = ""
    better: Better | None = None  # metrics: which way a change is good news
    grain: Grain | None = None  # the time bucket's size, in views over models


def _column(name: str, value_type: ValueType, field: Metric | Dimension | None, **extra: Any) -> ViewColumn:
    if isinstance(field, Dimension):
        return ViewColumn(name=name, type=value_type, label=field.label or words(name), **extra)
    if field is not None:  # a metric
        return ViewColumn(
            name=name,
            type=value_type,
            label=field.label or words(name),
            format=display_format(field),
            unit=field.unit,
            better=field.better,
            **extra,
        )
    return ViewColumn(name=name, type=value_type, label=words(name), **extra)


@dataclass(frozen=True)
class Output:
    """A column of a view over a model: what the SELECT names it, and the metric or dimension it holds."""

    name: str
    field: str  # the time dimension, for the time bucket
    role: Role


@dataclass(frozen=True)
class Condition:
    """`dimension <op> value` from the WHERE clause; values are literals or :parameters."""

    dimension: str
    op: str  # as in agentic_lakehouse.compiler.Filter
    value: exp.Expression | tuple[exp.Expression, ...]


@dataclass(frozen=True)
class ModelSource:
    """A view over an ontology model, as the parts of a MetricQuery."""

    model: str
    outputs: tuple[Output, ...]
    grain: Grain | None
    start: exp.Expression
    end: exp.Expression | None  # None: now
    conditions: tuple[Condition, ...]
    order_by: tuple[OrderBy, ...]
    limit: exp.Expression | None

    def query(self, values: Mapping[str, Any], now: datetime) -> MetricQuery:
        """The MetricQuery this view asks with parameter `values` at time `now`. Raises ViewError."""
        start = _time(self.start, values, now)
        end = now if self.end is None else _time(self.end, values, now)
        return self.query_between(values, start, end)

    def query_between(self, values: Mapping[str, Any], start: datetime, end: datetime) -> MetricQuery:
        try:
            filters = []
            for condition in self.conditions:
                if isinstance(condition.value, tuple):
                    listed = [v for v in (_scalar(e, values) for e in condition.value) if v is not None]
                    if listed:
                        filters.append(Filter(dimension=condition.dimension, op=condition.op, value=listed))
                elif (value := _scalar(condition.value, values)) is not None:  # None: an optional parameter is unset
                    filters.append(Filter(dimension=condition.dimension, op=condition.op, value=value))
            return MetricQuery(
                metrics=[o.field for o in self.outputs if o.role == "metric"],
                dimensions=[o.field for o in self.outputs if o.role == "dimension"],
                start=start,
                end=end,
                grain=self.grain,
                filters=filters,
                order_by=list(self.order_by),
                limit=None if self.limit is None else _scalar(self.limit, values),
            )
        except ValidationError as e:
            raise ViewError("; ".join(error["msg"].removeprefix("Value error, ") for error in e.errors())) from None


@dataclass(frozen=True)
class TableSource:
    """A view over Iceberg tables: SQL run on the warehouse engine as written."""

    tree: exp.Query  # with :parameters still in place
    tables: tuple[str, ...]  # "namespace.table" names it reads

    def sql(self, values: Mapping[str, Any], *, dialect: str, catalog: str | None = None, limit: int | None = None) -> str:
        """SQL for `dialect` with parameter `values` bound as literals. `catalog` qualifies table
        names (Trino's iceberg.raw.telemetry); `limit` caps the rows unless the view's LIMIT is lower."""
        tree = self.tree.copy()
        for placeholder in list(tree.find_all(exp.Placeholder)):
            in_interval = isinstance(placeholder.parent, exp.Interval)
            if in_interval and values[placeholder.name] is None:
                raise ViewError(f"parameter '{placeholder.name}' needs a value: it sets an INTERVAL")
            placeholder.replace(_literal(values[placeholder.name], in_interval=in_interval))
        if catalog:
            for table in tree.find_all(exp.Table):
                if table.db:  # a table, not a CTE (load_views checked every name)
                    table.set("catalog", exp.to_identifier(catalog))
        existing = tree.args.get("limit")
        if existing is not None and isinstance(existing.expression, exp.Null):  # an optional :limit with no value
            tree.set("limit", None)
            existing = None
        if limit is not None and (existing is None or _limit_value(existing) > limit):
            tree = tree.limit(limit)
        return tree.sql(dialect=dialect)


@dataclass(frozen=True)
class ComposedSource:
    """A composed view: stage-1 parts, each a view over a model or over tables, answered in
    full first; then stage 2, SQL over their results, which DuckDB runs (agentic_lakehouse/compose.py)."""

    parts: dict[str, ModelSource | TableSource]  # by CTE name, in the order written
    part_columns: dict[str, tuple[ViewColumn, ...]]  # each part's columns, so an empty part still has them
    stage2: exp.Query  # the query over the parts' names, in Trino's dialect, with :parameters in place

    def stage2_sql(self, values: Mapping[str, Any]) -> str:
        """Stage 2 in DuckDB's dialect, with parameter `values` bound as literals."""
        tree = self.stage2.copy()
        for placeholder in list(tree.find_all(exp.Placeholder)):
            placeholder.replace(_literal(values[placeholder.name], in_interval=isinstance(placeholder.parent, exp.Interval)))
        return tree.sql(dialect="duckdb")


@dataclass(frozen=True, eq=False)
class View:
    name: str
    description: str
    params: dict[str, ViewParam]
    sql: str  # the SELECT as written, without the header
    columns: tuple[ViewColumn, ...]
    source: ModelSource | TableSource | ComposedSource
    refresh_seconds: float | None = None

    @property
    def reads(self) -> list[str]:
        """The models and tables the view reads."""
        names = []
        for source in self._sources():
            names += [source.model] if isinstance(source, ModelSource) else list(source.tables)
        return list(dict.fromkeys(names))

    @property
    def realtime(self) -> bool:
        """Whether it reads a model, whose data changes from second to second."""
        return any(isinstance(source, ModelSource) for source in self._sources())

    @property
    def tables(self) -> list[str]:
        """The tables it reads directly, which a flush or rollup run changes."""
        return list(dict.fromkeys(t for source in self._sources() if isinstance(source, TableSource) for t in source.tables))

    def _sources(self) -> list["ModelSource | TableSource"]:
        return list(self.source.parts.values()) if isinstance(self.source, ComposedSource) else [self.source]

    def bind(self, params: Mapping[str, Any]) -> dict[str, Any]:
        """Every parameter's value for a request: from `params`, else the default. Raises ViewError."""
        if unknown := sorted(set(params) - set(self.params)):
            takes = f"it takes {', '.join(self.params)}" if self.params else "it takes none"
            raise ViewError(f"view '{self.name}' has no parameter {', '.join(map(repr, unknown))}; {takes}")
        values = {}
        for name, param in self.params.items():
            value = param.convert(params.get(name), f"parameter '{name}'")
            if value is None:
                if param.required:
                    raise ViewError(f"view '{self.name}' needs parameter '{name}' ({param.type})")
                value = param.convert(param.default, f"parameter '{name}'")
            values[name] = value
        return values


def load_views(directory: str | Path, ontology: Ontology, tables: TableSchemas) -> dict[str, View]:
    """Load and check every views/<name>.sql. Raises ViewError listing each problem with its file."""
    directory = Path(directory)
    views, problems = {}, []
    for path in sorted(directory.glob("*.sql")) if directory.is_dir() else []:
        try:
            if not VIEW_NAME.match(path.stem):
                raise ViewError("name views in lowercase snake_case, like isp_quality.sql")
            views[path.stem] = parse_view(path.stem, path.read_text(), ontology, tables)
        except ViewError as e:
            problems.append(f"{directory.name}/{path.name}: {e}")
    if problems:
        raise ViewError("invalid views:\n  - " + "\n  - ".join(problems))
    return views


def parse_view(name: str, text: str, ontology: Ontology, tables: TableSchemas) -> View:
    """Parse and check one view file's text. Raises ViewError saying what to fix."""
    match = re.match(r"\s*/\*(.*?)\*/(.*)", text, re.DOTALL)
    if match is None:
        raise ViewError("start the file with a /* ... */ comment holding YAML, at least `description: ...`")
    try:
        header = ViewHeader.model_validate(yaml.safe_load(match[1]) or {})
    except yaml.YAMLError as e:
        raise ViewError(f"the header comment isn't valid YAML: {e}") from None
    except ValidationError as e:
        raise ViewError(_validation_message(e)) from None
    if reserved := RESERVED_PARAMS & header.params.keys():
        raise ViewError(f"parameter names {', '.join(sorted(reserved))} are reserved")
    sql = match[2].strip().rstrip(";").strip()
    try:
        statements = [s for s in sqlglot.parse(sql, read=VIEW_DIALECT) if s is not None]
    except sqlglot.ParseError as e:
        raise ViewError(f"invalid SQL: {e.errors[0]['description']} (line {e.errors[0]['line']})") from None
    except SqlglotError as e:  # e.g. an unterminated string
        raise ViewError(f"invalid SQL: {e}") from None
    if len(statements) != 1 or not isinstance(statements[0], exp.Query):
        raise ViewError("a view is exactly one SELECT statement")
    [tree] = statements
    _check_parameters(tree, header.params)

    models = {model.name: model for model in ontology.models}
    if _composed(tree, models):
        source, columns = _composed_source(tree, header.params, ontology, tables, models)
    elif (model := _model_read(tree, models)) is not None:
        source, columns = _model_source(tree, model, header.params, ontology)
    else:
        source, columns = _table_source(tree, header.params, tables, models)
    columns = _described(columns, header.columns)
    return View(name, header.description, dict(header.params), sql, tuple(columns), source, header.refresh_seconds)


def _described(columns: list["ViewColumn"], described: Mapping[str, ColumnDisplay]) -> list["ViewColumn"]:
    """The columns, with what the header's `columns` says about them."""
    names = [c.name for c in columns]
    if unknown := sorted(set(described) - set(names)):
        raise ViewError(f"columns: {', '.join(unknown)} isn't one of the view's columns ({', '.join(names)})")
    result = []
    for column in columns:
        display = described.get(column.name)
        if display is None:
            result.append(column)
            continue
        said = display.model_dump(exclude_unset=True)
        if column.type != "number" and said.keys() & {"format", "unit", "better"}:
            raise ViewError(f"columns: {column.name} holds {column.type} values; format, unit and better are for numbers")
        result.append(column.model_copy(update=said))
    return result


def _validation_message(error: ValidationError) -> str:
    parts = []
    for item in error.errors():
        where = ".".join(str(p) for p in item["loc"])
        parts.append(f"{where}: {item['msg'].removeprefix('Value error, ')}" if where else item["msg"])
    return "; ".join(parts)


def _check_parameters(tree: exp.Query, params: Mapping[str, ViewParam]) -> None:
    used = set()
    for placeholder in tree.find_all(exp.Placeholder):
        if not placeholder.this:  # a positional ?
            raise ViewError("write parameters by name, like :hours, not ?")
        if placeholder.name not in params:
            raise ViewError(f":{placeholder.name} isn't declared under params in the header")
        used.add(placeholder.name)
    if unused := sorted(params.keys() - used):
        raise ViewError(f"declared parameters the SQL never uses: {', '.join(unused)}")


def _model_read(tree: exp.Query, models: Mapping[str, SemanticModel]) -> SemanticModel | None:
    """The model a SELECT reads, if it reads one directly (FROM <model>)."""
    from_ = tree.args.get("from_") if isinstance(tree, exp.Select) else None
    table = from_.this if from_ is not None else None
    if isinstance(table, exp.Table) and not table.db and isinstance(table.this, exp.Identifier):
        return models.get(table.name)
    return None


# ---------------------------------------------------------------- views over models

_CLAUSES = {"expressions", "from_", "where", "group", "order", "limit"}
_OPERATORS = {exp.EQ: "=", exp.NEQ: "!=", exp.LT: "<", exp.LTE: "<=", exp.GT: ">", exp.GTE: ">="}
_FLIPPED = {"=": "=", "!=": "!=", "<": ">", "<=": ">=", ">": "<", ">=": "<="}
_VALUE_NODES = (exp.Literal, exp.Boolean, exp.Neg, exp.Placeholder)
_TRUNC = (exp.TimestampTrunc, exp.DateTrunc)
_SAMPLE_VALUES = {"int": 1, "float": 1.0, "string": "x", "boolean": True}
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_FILTER_TYPES = {"string": ("string",), "number": ("int", "float"), "boolean": ("boolean",)}


def _model_source(
    select: exp.Select, model: SemanticModel, params: Mapping[str, ViewParam], ontology: Ontology
) -> tuple[ModelSource, list[ViewColumn]]:
    over = f"a view over model '{model.name}'"
    if extra := sorted(key for key, value in select.args.items() if value and key not in _CLAUSES):
        raise ViewError(
            f"{over} can only SELECT, WHERE, GROUP BY, ORDER BY and LIMIT (it has {', '.join(extra)}). "
            "To combine it with other data, read it in a WITH clause and join the CTE; "
            "or write the view over a table, such as a gold rollup"
        )
    table = select.args["from_"].this
    qualifiers = {model.name, table.alias_or_name}
    dimensions = {d.name: d for d in model.dimensions}
    metrics = {m.name: m for m in model.metrics}
    time = model.time_dimension
    choices = (
        f"Metrics: {', '.join(metrics)}. Dimensions: {', '.join(d for d in dimensions if d != time)}; "
        f"group by time with date_trunc('hour', {time})"
    )

    def column(node: exp.Expression) -> str | None:
        if not isinstance(node, exp.Column):
            return None
        if node.table and node.table not in qualifiers:
            raise ViewError(f"{node.sql(VIEW_DIALECT)}: '{node.table}' isn't the model being read")
        return node.name

    outputs: list[Output] = []
    grain = None
    for item in select.expressions:
        expression = item.this if isinstance(item, exp.Alias) else item
        alias = item.alias if isinstance(item, exp.Alias) else None
        name = column(expression)
        if name in metrics:
            outputs.append(Output(alias or name, name, "metric"))
        elif name == time:
            raise ViewError(f"'{time}' is the time dimension: select date_trunc('hour', {time}) to group by time")
        elif name in dimensions:
            outputs.append(Output(alias or name, name, "dimension"))
        elif name is not None:
            raise ViewError(f"'{name}' isn't a metric or dimension of model '{model.name}'. {choices}")
        elif isinstance(expression, _TRUNC) and column(expression.this) == time:
            unit = expression.unit.name.lower() if expression.unit else ""
            if unit not in get_args(Grain):
                raise ViewError(f"date_trunc unit must be one of {', '.join(get_args(Grain))}, not '{unit}'")
            if grain is not None:
                raise ViewError("select one time bucket at most")
            grain = unit
            outputs.append(Output(alias or time, time, "time"))
        elif isinstance(expression, exp.Star):
            raise ViewError(f"list the columns to select instead of *. {choices}")
        elif expression.find(exp.AggFunc):
            raise ViewError(
                f"{expression.sql(VIEW_DIALECT)}: aggregations are defined once, in ontology.yaml, so select a "
                f"metric by name. {choices}"
            )
        else:
            raise ViewError(
                f"can't select {expression.sql(VIEW_DIALECT)}: {over} selects metric and dimension names, and "
                f"date_trunc('<grain>', {time}). Compute anything else in the dashboard or in a view over a table"
            )
    if not any(o.role == "metric" for o in outputs):
        raise ViewError(f"select at least one metric. {choices}")
    for kind, names in (("column", [o.name for o in outputs]), ("metric or dimension", [o.field for o in outputs])):
        if repeated := sorted({n for n in names if names.count(n) > 1}):
            raise ViewError(f"each {kind} can be selected once: {', '.join(repeated)}")

    start = end = None
    conditions = []
    where = select.args.get("where")
    for predicate in _conjuncts(where.this) if where else []:
        negated = isinstance(predicate, exp.Not) and isinstance(predicate.this, exp.In)
        node = predicate.this if negated else predicate
        if isinstance(node, exp.In) and column(node.this) in dimensions and not node.args.get("query"):
            values = tuple(node.expressions)
            _check_filter_values(node.this.name, values, dimensions, params, time)
            conditions.append(Condition(node.this.name, "not_in" if negated else "in", values))
            continue
        if type(node) in _OPERATORS:
            left, right, op = node.this, node.expression, _OPERATORS[type(node)]
            if column(left) is None and column(right) is not None:
                left, right, op = right, left, _FLIPPED[op]
            name = column(left)
            if name == time:
                if op == ">=" and start is None:
                    start = right
                elif op == "<" and end is None:
                    end = right
                else:
                    raise ViewError(f"bound the time range once each way, half-open: {time} >= <start> AND {time} < <end>")
                continue
            if name in dimensions:
                _check_filter_values(name, (right,), dimensions, params, time)
                conditions.append(Condition(name, op, right))
                continue
            if name in metrics:
                raise ViewError(f"WHERE can't filter on metric '{name}': filter rows in the dashboard instead")
            if name is not None:
                raise ViewError(f"WHERE: '{name}' isn't a dimension of model '{model.name}'. {choices}")
        raise ViewError(
            f"can't use {predicate.sql(VIEW_DIALECT)} in WHERE: {over} ANDs comparisons and IN lists on "
            f"dimensions, and a time range: {time} >= <start> AND {time} < <end>"
        )
    if start is None:
        raise ViewError(f"{over} needs a time range: WHERE {time} >= now() - INTERVAL '1' HOUR (the end defaults to now)")
    sample = {name: _SAMPLE_VALUES.get(param.type, _EPOCH) for name, param in params.items()}
    for bound in (start, end):
        if bound is not None and not isinstance(_evaluate(bound, sample, _EPOCH), datetime):
            raise ViewError(f"{bound.sql(VIEW_DIALECT)} isn't a time; compare {time} with one, like now() - INTERVAL '1' HOUR")

    def output(key: exp.Expression, clause: str) -> Output:
        if isinstance(key, exp.Literal) and not key.is_string and 1 <= int(key.this) <= len(outputs):
            return outputs[int(key.this) - 1]
        if (name := column(key)) is not None:
            if found := next((o for o in outputs if o.name == name), None) or next((o for o in outputs if o.field == name), None):
                return found
        if isinstance(key, _TRUNC) and grain is not None:
            return next(o for o in outputs if o.role == "time")
        raise ViewError(f"{clause}: {key.sql(VIEW_DIALECT)} isn't a selected column (use its name or position)")

    if group := select.args.get("group"):
        grouped = {output(key, "GROUP BY").field for key in group.expressions}
        expected = {o.field for o in outputs if o.role != "metric"}
        if grouped != expected:
            raise ViewError(
                f"GROUP BY must list exactly the selected dimensions and time bucket ({', '.join(sorted(expected)) or 'none'}), "
                "or be left out: views over models group by them anyway"
            )
    order_by = tuple(
        OrderBy(field=output(key.this, "ORDER BY").field, descending=bool(key.args.get("desc")))
        for key in (select.args["order"].expressions if select.args.get("order") else [])
    )
    limit = select.args["limit"].expression if select.args.get("limit") else None
    if limit is not None and not (
        (isinstance(limit, exp.Literal) and not limit.is_string and limit.this.isdigit())
        or (isinstance(limit, exp.Placeholder) and params[limit.name].type == "int")
    ):
        raise ViewError("LIMIT takes a whole number or an int :parameter")

    source = ModelSource(model.name, tuple(outputs), grain, start, end, tuple(conditions), order_by, limit)
    try:  # compile once, as every request will, so a mismatch with the ontology fails now
        query = source.query_between(sample, _EPOCH, _EPOCH + timedelta(hours=1))
        SqlCompiler(ontology).compile(query, engine="duckdb", table="t")
    except (QueryError, ValueError) as e:
        raise ViewError(str(e)) from None

    columns = []
    for o in outputs:
        if o.role == "metric":
            columns.append(_column(o.name, "number", metrics[o.field], role="metric"))
        else:
            extra = {"grain": grain} if o.role == "time" else {}
            columns.append(_column(o.name, dimensions[o.field].type, dimensions[o.field], role=o.role, **extra))
    return source, columns


def _conjuncts(node: exp.Expression):
    while isinstance(node, exp.Paren):
        node = node.this
    if isinstance(node, exp.And):
        yield from _conjuncts(node.this)
        yield from _conjuncts(node.expression)
    else:
        yield node


def _check_filter_values(
    dimension: str, values: tuple[exp.Expression, ...], dimensions: Mapping, params: Mapping[str, ViewParam], time: str
) -> None:
    allowed = _FILTER_TYPES.get(dimensions[dimension].type)
    if allowed is None:
        raise ViewError(f"WHERE can't filter on timestamp dimension '{dimension}'; bound '{time}' instead")
    for value in values:
        if not isinstance(value, _VALUE_NODES) or (isinstance(value, exp.Neg) and not isinstance(value.this, exp.Literal)):
            raise ViewError(f"compare '{dimension}' with literals or :parameters, not {value.sql(VIEW_DIALECT)}")
        if isinstance(value, exp.Placeholder):
            kind = params[value.name].type
        elif isinstance(value, exp.Boolean):
            kind = "boolean"
        else:
            kind = "string" if isinstance(value, exp.Literal) and value.is_string else "float"
        if kind not in allowed:
            raise ViewError(f"'{dimension}' is a {dimensions[dimension].type} dimension; compare it with a {dimensions[dimension].type}")


def _scalar(node: exp.Expression, values: Mapping[str, Any]) -> Any:
    if isinstance(node, exp.Placeholder):
        return values[node.name]
    if isinstance(node, exp.Boolean):
        return node.this
    if isinstance(node, exp.Neg):
        return -node.this.to_py()
    return node.to_py()


_UNITS = {"second": timedelta(seconds=1), "minute": timedelta(minutes=1), "hour": timedelta(hours=1), "day": timedelta(days=1)}


def _time(node: exp.Expression, values: Mapping[str, Any], now: datetime) -> datetime:
    value = _evaluate(node, values, now)
    if not isinstance(value, datetime):
        raise ViewError(f"{node.sql(VIEW_DIALECT)} isn't a time")
    return value


def _evaluate(node: exp.Expression, values: Mapping[str, Any], now: datetime) -> datetime | timedelta | float:
    """The value of a time-range expression: now(), TIMESTAMP '...', INTERVAL, date_trunc, :parameters and arithmetic."""
    if isinstance(node, exp.Paren):
        return _evaluate(node.this, values, now)
    if isinstance(node, exp.CurrentTimestamp):
        return now
    if isinstance(node, exp.Placeholder):
        value = values[node.name]
        if value is None:
            raise ViewError(f"parameter '{node.name}' needs a value: it sets the time range")
        if isinstance(value, (bool, str)):
            raise ViewError(f"parameter '{node.name}' is {type(value).__name__}; the time range needs numbers and times")
        return value
    if isinstance(node, exp.Literal) and not node.is_string:
        return node.to_py()
    if isinstance(node, exp.Cast) and node.to.is_type(*exp.DataType.TEMPORAL_TYPES) and isinstance(node.this, exp.Literal):
        try:
            return _as_timestamp(node.this.this)
        except ValueError:
            raise ViewError(f"{node.sql(VIEW_DIALECT)} isn't a valid time") from None
    if isinstance(node, exp.Interval):
        unit = node.unit.name.lower() if node.unit else ""
        if unit not in _UNITS:
            raise ViewError(f"INTERVAL units for time ranges are SECOND, MINUTE, HOUR and DAY, not {unit.upper() or 'none'}")
        try:
            amount = float(node.this.this) if isinstance(node.this, exp.Literal) else _evaluate(node.this, values, now)
        except ValueError:
            amount = None
        if isinstance(amount, bool) or not isinstance(amount, (int, float)):
            raise ViewError(f"{node.sql(VIEW_DIALECT)}: the amount must be a number")
        return amount * _UNITS[unit]
    if isinstance(node, exp.Neg):
        value = _evaluate(node.this, values, now)
        if not isinstance(value, datetime):
            return -value
    if isinstance(node, (exp.Add, exp.Sub, exp.Mul)):
        left, right = _evaluate(node.this, values, now), _evaluate(node.expression, values, now)
        try:
            if isinstance(node, exp.Mul) and not (isinstance(left, datetime) or isinstance(right, datetime)):
                return left * right
            if isinstance(node, exp.Add) and not (isinstance(left, datetime) and isinstance(right, datetime)):
                return left + right
            if isinstance(node, exp.Sub) and not isinstance(right, datetime):
                return left - right
        except TypeError:
            pass
        raise ViewError(f"can't compute {node.sql(VIEW_DIALECT)}: add or subtract intervals to times, multiply intervals by numbers")
    if isinstance(node, _TRUNC):
        unit = node.unit.name.lower() if node.unit else ""
        value = _evaluate(node.this, values, now)
        if unit not in get_args(Grain) or not isinstance(value, datetime):
            raise ViewError(f"{node.sql(VIEW_DIALECT)}: date_trunc takes a unit ({', '.join(get_args(Grain))}) and a time")
        return bucket_start(value, unit)
    raise ViewError(
        f"can't work out a time from {node.sql(VIEW_DIALECT)}: use now(), TIMESTAMP '2026-01-01 00:00:00', "
        "INTERVAL '1' HOUR, date_trunc('day', now()) and :parameters"
    )


# ---------------------------------------------------------------- composed views


def _composed(tree: exp.Query, models: Mapping[str, SemanticModel]) -> bool:
    """Whether a query is a composed view: a WITH query that reads a model somewhere (or names a
    CTE like one, which _composed_source then explains)."""
    with_ = tree.args.get("with_")
    if with_ is None:
        return False
    ctes = {cte.alias_or_name for cte in tree.find_all(exp.CTE)}
    return bool(ctes & models.keys()) or any(not t.db and t.name in models and t.name not in ctes for t in tree.find_all(exp.Table))


def _composed_source(
    tree: exp.Query,
    params: Mapping[str, ViewParam],
    ontology: Ontology,
    tables: TableSchemas,
    models: Mapping[str, SemanticModel],
) -> tuple[ComposedSource, list[ViewColumn]]:
    with_ = tree.args["with_"]
    if with_.args.get("recursive"):
        raise ViewError("composed views can't use WITH RECURSIVE")
    names = [cte.alias_or_name for cte in with_.expressions]
    if clash := sorted(set(names) & models.keys()):
        raise ViewError(f"name the CTE {clash[0]} something other than the model it shares a name with")
    parts, part_columns, stage2_ctes = {}, {}, []
    for cte in with_.expressions:
        name, body = cte.alias_or_name, cte.this
        kind = _cte_kind(name, body, models, set(names))
        if kind == "model":
            model = _model_read(body, models)
            if model is None or not isinstance(body, exp.Select):
                raise ViewError(f"CTE {name}: read the model alone, like SELECT … FROM {next(iter(_models_in(body, models)))} WHERE …")
            try:
                parts[name], columns = _model_source(body, model, params, ontology)
            except ViewError as e:
                raise ViewError(f"CTE {name}: {e}") from None
        elif kind == "tables":
            try:
                parts[name], columns = _table_source(body, params, tables, models)
            except ViewError as e:
                raise ViewError(f"CTE {name}: {e}") from None
        else:
            stage2_ctes.append(cte)
            continue
        part_columns[name] = tuple(columns)

    stage2 = tree.copy()
    if stage2_ctes:
        stage2.set("with_", exp.With(expressions=[cte.copy() for cte in stage2_ctes]))
    else:
        stage2.set("with_", None)
    stage2_names = {cte.alias_or_name for cte in stage2_ctes}
    if forbidden := stage2.find(*_FORBIDDEN):
        raise ViewError(f"views only read data; remove {forbidden.key.upper()}")
    for table in stage2.find_all(exp.Table):
        if not isinstance(table.this, exp.Identifier):
            raise ViewError(f"after the WITH, read the CTEs, not table functions like {table.sql(VIEW_DIALECT)}")
        if table.db or (table.name not in parts and table.name not in stage2_names):
            what = "the model" if table.name in models else "the table" if table.db else "the unknown table"
            named = f"{table.db}.{table.name}" if table.db else table.name
            raise ViewError(f"after the WITH, read only the CTEs; read {what} {named} in a CTE of its own")

    # Run stage 2 over empty parts: what doesn't work in DuckDB fails now, naming the view,
    # and the result's columns and their types are known up front.
    sample = ComposedSource(parts, part_columns, stage2).stage2_sql(
        {name: _SAMPLE_VALUES.get(param.type, datetime(2026, 1, 1)) for name, param in params.items()}
    )
    try:
        outputs = compose.check({name: [(c.name, c.type) for c in cols] for name, cols in part_columns.items()}, sample)
    except compose.StageTwoError as e:
        raise ViewError(f"after the WITH: {e}") from None
    names_out = [name for name, _ in outputs]
    if repeated := sorted({n for n in names_out if names_out.count(n) > 1}):
        raise ViewError(f"column names must be unique: {', '.join(repeated)}")

    # A column named like a part's metric or dimension reads like it; anything else is plain.
    described = {}
    for cols in part_columns.values():
        for column in cols:
            if column.role is not None or column.format is not None or column.label != words(column.name):
                described.setdefault(column.name, column)
    columns = [
        described[name].model_copy(update={"type": kind}) if name in described else ViewColumn(name=name, type=kind, label=words(name))
        for name, kind in outputs
    ]
    return ComposedSource(parts, part_columns, stage2), columns


def _cte_kind(name: str, body: exp.Query, models: Mapping[str, SemanticModel], ctes: set[str]) -> str:
    """What a CTE of a composed view reads: "model" (one), "tables", or "ctes" (other CTEs, or nothing)."""
    own = {cte.alias_or_name for cte in body.find_all(exp.CTE)}  # a table part may have a WITH of its own
    reads = {"model": set(), "tables": set(), "ctes": set()}
    for table in body.find_all(exp.Table):
        if table.db:
            reads["tables"].add(f"{table.db}.{table.name}")
        elif table.name in models and table.name not in own:
            reads["model"].add(table.name)
        elif table.name in ctes and table.name not in own:
            reads["ctes"].add(table.name)
        elif table.name not in own:
            raise ViewError(f"CTE {name} reads '{table.name}', which is no model, table or earlier CTE")
    kinds = [kind for kind, found in reads.items() if found]
    if len(kinds) > 1:
        raise ViewError(
            f"CTE {name} reads {' and '.join(sorted(n for found in reads.values() for n in found))}: a CTE reads one "
            "model, or tables, or other CTEs. Split it into CTEs and join them"
        )
    if len(reads["model"]) > 1:
        raise ViewError(f"CTE {name} reads {' and '.join(sorted(reads['model']))}: read each model in a CTE of its own")
    return kinds[0] if kinds else "ctes"


def _models_in(body: exp.Query, models: Mapping[str, SemanticModel]) -> set[str]:
    return {t.name for t in body.find_all(exp.Table) if not t.db and t.name in models}


# ---------------------------------------------------------------- views over tables

_FORBIDDEN = (exp.Insert, exp.Update, exp.Delete, exp.Merge, exp.Create, exp.Drop, exp.Alter, exp.Command, exp.Into)


def tables_in(sql: str) -> set[str]:
    """The <namespace>.<table> names a query reads (not its CTEs). Raises ViewError for invalid SQL."""
    try:
        tree = sqlglot.parse_one(sql, read=VIEW_DIALECT)
    except SqlglotError as e:
        raise ViewError(f"invalid SQL: {e}") from None
    return {f"{t.db}.{t.name}" for t in tree.find_all(exp.Table) if t.db}


def describe_query(sql: str, tables: TableSchemas) -> tuple[tuple[str, ...], dict[str, str]]:
    """The tables a SELECT (Trino's dialect, without parameters) reads, and its columns with their
    SQL types: how a pipeline step's output is known before it runs. Raises ViewError."""
    try:
        statements = [s for s in sqlglot.parse(sql, read=VIEW_DIALECT) if s is not None]
    except SqlglotError as e:
        raise ViewError(f"invalid SQL: {e}") from None
    if len(statements) != 1 or not isinstance(statements[0], exp.Query):
        raise ViewError("write exactly one SELECT statement")
    read, typed = _analyze(statements[0], {}, tables, {}, kind="queries")
    columns: dict[str, str] = {}
    for select in typed.selects:
        if select.alias_or_name in columns:
            raise ViewError(f"column names must be unique: {select.alias_or_name}")
        columns[select.alias_or_name] = select.type.sql(dialect=VIEW_DIALECT).upper() if select.type else "UNKNOWN"
    return read, columns


def _table_source(
    tree: exp.Query,
    params: Mapping[str, ViewParam],
    tables: TableSchemas,
    models: Mapping[str, SemanticModel],
) -> tuple[TableSource, list[ViewColumn]]:
    read, typed = _analyze(tree, params, tables, models)
    # A column read as-is (aliased or not) keeps the label and format of the metric or dimension it holds.
    fields: dict[str, Metric | Dimension] = {}
    for model in models.values():
        for field in [*model.metrics, *model.dimensions]:
            fields.setdefault(field.name, field)
    columns = []
    for select in typed.selects:
        source_column = select.this if isinstance(select, exp.Alias) else select
        field = fields.get(source_column.name) if isinstance(source_column, exp.Column) else None
        columns.append(_column(select.alias_or_name, _value_type(select.type), field))
    names = [c.name for c in columns]
    if repeated := sorted({n for n in names if names.count(n) > 1}):
        raise ViewError(f"column names must be unique: {', '.join(repeated)}")
    return TableSource(tree, read), columns


def _analyze(
    tree: exp.Query,
    params: Mapping[str, ViewParam],
    tables: TableSchemas,
    models: Mapping[str, SemanticModel],
    kind: str = "views",
) -> tuple[tuple[str, ...], exp.Query]:
    """Check a SELECT over tables, and type it: the tables it reads, and the qualified, typed tree."""
    if forbidden := tree.find(*_FORBIDDEN):
        raise ViewError(f"{kind} only read data; remove {forbidden.key.upper()}")
    ctes = {cte.alias_or_name for cte in tree.find_all(exp.CTE)}
    known = f"Models: {', '.join(models) or 'none'}. Tables: {', '.join(sorted(tables)) or 'none'}"
    read = []
    for table in tree.find_all(exp.Table):
        if not isinstance(table.this, exp.Identifier):
            raise ViewError(f"{kind} read models and tables, not table functions like {table.sql(VIEW_DIALECT)}")
        if table.catalog:
            raise ViewError(f"write tables as <namespace>.<table>, without a catalog: {table.sql(VIEW_DIALECT)}")
        if not table.db:
            if table.name in ctes:
                continue
            if table.name in models:
                raise ViewError(
                    f"a view reading model '{table.name}' must select from it alone, without joins or subqueries; "
                    f"to combine it with other data, read it in a WITH clause and join the CTE"
                )
            raise ViewError(f"unknown model or table '{table.name}'. {known}")
        name = f"{table.db}.{table.name}"
        if name not in tables:
            raise ViewError(f"unknown table '{name}'. {known}")
        read.append(name)
    for expression in tree.selects:
        if not isinstance(expression, (exp.Alias, exp.Column, exp.Star)):
            raise ViewError(f"name computed columns: {expression.sql(VIEW_DIALECT)} AS <name>")

    schema: dict[str, dict[str, dict[str, str]]] = {}
    for name, columns in tables.items():
        namespace, table_name = name.split(".")
        schema.setdefault(namespace, {})[table_name] = dict(columns)
    sample = tree.copy()
    for placeholder in list(sample.find_all(exp.Placeholder)):
        param = params[placeholder.name]
        value = _SAMPLE_VALUES.get(param.type, datetime(2026, 1, 1))
        placeholder.replace(_literal(value, in_interval=isinstance(placeholder.parent, exp.Interval)))
    try:
        typed = annotate_types(qualify(sample, schema=schema, dialect=VIEW_DIALECT), schema=schema, dialect=VIEW_DIALECT)
    except SqlglotError as e:  # e.g. "Column 'x' could not be resolved"
        raise ViewError(str(e).split(". Line:")[0]) from None
    for select in typed.selects:  # sqlglot leaves date_trunc's type unknown: truncating a time gives a time
        inner = select.this if isinstance(select, exp.Alias) else select
        if isinstance(inner, _TRUNC) and (select.type is None or select.type.is_type(exp.DataType.Type.UNKNOWN)):
            select.type = exp.DataType.build("TIMESTAMP")
    return tuple(sorted(set(read))), typed


def qualify_tables(sql: str, catalog: str | None, dialect: str = VIEW_DIALECT) -> str:
    """`sql` (Trino's dialect) with its <namespace>.<table> names qualified by `catalog`, printed for
    `dialect`: how code that names lakehouse tables runs them on the warehouse engine."""
    tree = sqlglot.parse_one(sql, read=VIEW_DIALECT)
    if catalog:
        for table in tree.find_all(exp.Table):
            if table.db and not table.catalog:
                table.set("catalog", exp.to_identifier(catalog))
    return tree.sql(dialect=dialect)


def _value_type(data_type: exp.DataType | None) -> ValueType:
    if data_type is None:
        return "unknown"
    if data_type.is_type(*exp.DataType.NUMERIC_TYPES):
        return "number"
    if data_type.is_type(*exp.DataType.TEXT_TYPES):
        return "string"
    if data_type.is_type(exp.DataType.Type.BOOLEAN):
        return "boolean"
    if data_type.is_type(exp.DataType.Type.DATE):
        return "date"
    if data_type.is_type(*exp.DataType.TEMPORAL_TYPES):
        return "timestamp"
    return "unknown"


def _literal(value: Any, *, in_interval: bool = False) -> exp.Expression:
    if in_interval:  # INTERVAL '48' HOUR: Trino wants the amount as a string
        return exp.Literal.string(f"{value:g}" if isinstance(value, float) else str(value))
    if isinstance(value, datetime):
        value = value.astimezone(UTC).replace(tzinfo=None) if value.tzinfo else value
    return exp.convert(value)


def _limit_value(limit: exp.Limit) -> float:
    node = limit.expression
    return int(node.this) if isinstance(node, exp.Literal) and not node.is_string else math.inf
