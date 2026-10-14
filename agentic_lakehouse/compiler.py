"""Compile semantic queries into SQL for a specific engine.

A MetricQuery asks for metrics from the ontology, sliced by dimensions, over a
time range. SqlCompiler turns it into SQL for DuckDB, Trino or Pinot:

- templates/query.sql is the query skeleton, shared by every engine.
- templates/macros/<engine>.sql renders the engine-specific parts
  (aggregations, time buckets, literals) and overrides only what differs from
  templates/macros/default.sql, like dbt's adapter.dispatch. Supporting another
  engine means writing one macro file. A project's own macros (config/macros/,
  agentic_lakehouse/macros.py) come first, so it can add and override them.
- SQL from the ontology and filter values from clients never reach a template
  as raw text: sqlglot parses or builds them and prints them in the engine's
  syntax, correctly quoted.
"""

from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Literal, NamedTuple, Self

from pydantic import Field, field_validator, model_validator
from sqlglot import exp
from sqlglot.dialects.dialect import Dialect

from agentic_lakehouse.macros import Adapter
from agentic_lakehouse.ontology import (
    AggregateMetric,
    Dimension,
    Identifier,
    Metric,
    Ontology,
    SemanticModel,
    StrictModel,
    parse_fragment,
)

Engine = Literal["duckdb", "trino", "pinot"]
Grain = Literal["minute", "hour", "day", "week", "month"]
Scalar = str | bool | int | float

class Pinot(Dialect):
    """sqlglot dialect for Apache Pinot, which sqlglot doesn't ship.

    Pinot's Calcite-based SQL accepts sqlglot's generic output for the scalar
    expressions ontology fragments contain. That output doesn't know Pinot's
    reserved words (a column named "timestamp" or "value"), so every
    identifier is quoted.
    """

    def generate(self, expression: exp.Expression, copy: bool = True, **opts) -> str:
        return super().generate(expression, copy=copy, **{"identify": True, **opts})


# The sqlglot dialect that prints ontology fragments and filters for each engine.
SQLGLOT_DIALECTS: dict[Engine, str | type[Dialect]] = {"duckdb": "duckdb", "trino": "trino", "pinot": Pinot}


def bucket_start(ts: datetime, grain: Grain) -> datetime:
    """Start of the `grain` bucket containing ts, matching date_trunc (weeks start on Monday)."""
    if grain == "minute":
        return ts.replace(second=0, microsecond=0)
    if grain == "hour":
        return ts.replace(minute=0, second=0, microsecond=0)
    day = ts.replace(hour=0, minute=0, second=0, microsecond=0)
    if grain == "day":
        return day
    if grain == "week":
        return day - timedelta(days=day.weekday())
    return day.replace(day=1)


class QueryError(ValueError):
    """The query asks for something its ontology doesn't define."""


class Filter(StrictModel):
    """Keep only rows where `dimension <op> value`. `in` and `not_in` take a list of values."""

    dimension: Identifier
    op: Literal["=", "!=", "<", "<=", ">", ">=", "in", "not_in"]
    value: Scalar | list[Scalar]

    @model_validator(mode="after")
    def _check_value(self) -> Self:
        wants_list = self.op in ("in", "not_in")
        if wants_list != isinstance(self.value, list):
            expected = "a list of values" if wants_list else "a single value"
            raise ValueError(f"filter on '{self.dimension}': op '{self.op}' takes {expected}")
        if wants_list and not self.value:
            raise ValueError(f"filter on '{self.dimension}': op '{self.op}' needs at least one value")
        return self


class OrderBy(StrictModel):
    field: Identifier  # a requested metric or dimension, or the time dimension when `grain` is set
    descending: bool = False


class MetricQuery(StrictModel):
    """A question for the engine: metrics, sliced by dimensions, over a time range.

    The range is half-open, [start, end). Naive datetimes are taken as UTC.
    """

    metrics: list[Identifier] = Field(min_length=1)
    dimensions: list[Identifier] = []
    start: datetime
    end: datetime
    grain: Grain | None = None  # bucket the time dimension by this; None aggregates the whole range
    filters: list[Filter] = []
    order_by: list[OrderBy] = []  # default: time bucket, then dimensions, ascending
    limit: int | None = Field(default=None, gt=0)

    @field_validator("start", "end")
    @classmethod
    def _as_utc(cls, value: datetime) -> datetime:
        return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)

    @model_validator(mode="after")
    def _check(self) -> Self:
        if self.start >= self.end:
            raise ValueError("start must be before end")
        for kind, names in (("metric", self.metrics), ("dimension", self.dimensions)):
            if repeated := sorted({name for name in names if names.count(name) > 1}):
                raise ValueError(f"{kind}s requested more than once: {', '.join(repeated)}")
        return self


class _Column(NamedTuple):
    name: str
    sql: str


# Expressions that are safe as an operand without parentheses.
_ATOMIC = (exp.Column, exp.Literal, exp.Boolean, exp.Null, exp.Func, exp.Paren)

_COMPARISONS = {"=": exp.EQ, "!=": exp.NEQ, "<": exp.LT, "<=": exp.LTE, ">": exp.GT, ">=": exp.GTE}


def _operand(tree: exp.Expression) -> exp.Expression:
    # sqlglot doesn't add parentheses for precedence: `x OR y` IN (...) must
    # print as (x OR y) IN (...).
    return tree if isinstance(tree, _ATOMIC) else exp.Paren(this=tree)


def _dimension_tree(dimension: Dimension) -> exp.Expression:
    # exp.column quotes names that are keywords in the target dialect.
    return parse_fragment(dimension.sql) if dimension.sql else exp.column(dimension.name)


def _filter_tree(query_filter: Filter, column: exp.Expression) -> exp.Expression:
    operand = _operand(column)
    if query_filter.op in ("in", "not_in"):
        condition = operand.isin(*query_filter.value)
        return exp.not_(condition) if query_filter.op == "not_in" else condition
    return _COMPARISONS[query_filter.op](this=operand, expression=exp.convert(query_filter.value))


# How each aggregation's partial results from several tiers combine. Averages
# split into a sum and a count; count_distinct and percentile can't be combined.
_MERGE = {"count": "sum", "sum": "sum", "min": "min", "max": "max"}


class SqlCompiler:
    """Compiles MetricQuery objects into SQL for any supported engine.

    compile() answers a query on one engine. For a query split across tiers,
    compile(partial=True) gives each tier's partial aggregates and merge_sql()
    combines them in DuckDB.
    """

    def __init__(self, ontology: Ontology):
        self.ontology = ontology
        self._model_of = {metric.name: model for model in ontology.models for metric in model.metrics}

    @property
    def _macros(self):
        # Read at each use, not kept: a live reload of config/macros/ replaces the ontology's macros.
        return self.ontology.macros

    @property
    def _env(self):
        return self.ontology.macros.env

    def model_for(self, query: MetricQuery) -> SemanticModel:
        """The model the query's metrics belong to. Raises QueryError if they don't share one."""
        if unknown := [name for name in query.metrics if name not in self._model_of]:
            raise QueryError(f"unknown metrics: {', '.join(unknown)}. Available: {', '.join(sorted(self._model_of))}")
        models = sorted({self._model_of[name].name for name in query.metrics})
        if len(models) > 1:
            raise QueryError(f"metrics come from different models ({', '.join(models)}); query one model at a time")
        return self._model_of[query.metrics[0]]

    def unmergeable_metrics(self, query: MetricQuery) -> list[str]:
        """Requested metrics whose results from two tiers can't be combined (percentiles, distinct counts)."""
        metrics = {m.name: m for m in self.model_for(query).metrics}
        return [name for name in query.metrics if any(m.agg not in _MERGE and m.agg != "avg" for m in _parts(metrics[name], metrics))]

    def compile(self, query: MetricQuery, *, engine: Engine, table: str, partial: bool = False) -> str:
        """Render `query` as SQL for `engine`, reading from `table`.

        `table` comes from trusted storage config and is inserted verbatim, so
        it can be a qualified name (iceberg.raw.telemetry) or a table function.
        With partial=True the result has one column per partial aggregate
        (named `_<agg>__<metric>`) instead of the metrics, for merge_sql() to
        combine; ordering and limit are left to the merge.
        Raises QueryError if the query doesn't match the ontology, or if
        partial=True and a metric can't be combined across tiers.
        """
        model = self.model_for(query)
        dimensions = {d.name: d for d in model.dimensions}
        self._check_dimensions(query, model, dimensions)
        adapter = self._adapter(engine)
        dialect = SQLGLOT_DIALECTS[engine]

        time_tree = _dimension_tree(dimensions[model.time_dimension])
        grouped: list[_Column] = []
        if query.grain:
            grouped.append(_Column(model.time_dimension, adapter.time_bucket(time_tree.sql(dialect=dialect), query.grain)))
        for name in query.dimensions:
            grouped.append(_Column(name, _dimension_tree(dimensions[name]).sql(dialect=dialect)))

        metrics = {m.name: m for m in model.metrics}
        aggregates: list[_Column] = []
        if partial:
            if unmergeable := self.unmergeable_metrics(query):
                raise QueryError(f"can't combine across tiers: {', '.join(unmergeable)}")
            columns = {}
            for name in query.metrics:
                for part in _parts(metrics[name], metrics):
                    for column, piece in _partial_pieces(part):
                        columns[column] = self._aggregate_sql(piece, adapter, dialect)
            aggregates = [_Column(column, sql) for column, sql in columns.items()]
        else:
            for name in query.metrics:
                metric = metrics[name]
                if isinstance(metric, AggregateMetric):
                    aggregates.append(_Column(name, self._aggregate_sql(metric, adapter, dialect)))
                else:
                    numerator = self._aggregate_sql(metrics[metric.numerator], adapter, dialect)
                    denominator = self._aggregate_sql(metrics[metric.denominator], adapter, dialect)
                    aggregates.append(_Column(name, adapter.ratio(numerator, denominator)))

        return self._env.get_template("engine/query.sql").render(
            adapter=adapter,
            columns=grouped + aggregates,
            table=table,
            time_expr=_operand(time_tree).sql(dialect=dialect),
            start=query.start,
            end=query.end,
            filters=[
                _filter_tree(f, _dimension_tree(dimensions[f.dimension])).sql(dialect=dialect) for f in query.filters
            ],
            group_by=[c.sql for c in grouped],
            order_by=[] if partial else self._order_by(query, model, grouped + aggregates),
            limit=None if partial else query.limit,
        ).strip()

    def merge_sql(self, query: MetricQuery, relation: str, *, partial: bool) -> str:
        """DuckDB SQL that turns rows from several tiers (`relation`) into the query's result.

        partial=True: the rows are compile(partial=True) output; partial
        aggregates are combined per group. partial=False: the rows are complete
        results for disjoint time ranges, which only need ordering and limiting.
        """
        model = self.model_for(query)
        adapter = self._adapter("duckdb")
        quote = adapter.quote
        keys = ([model.time_dimension] if query.grain else []) + query.dimensions
        columns = [_Column(key, quote(key)) for key in keys]
        metrics = {m.name: m for m in model.metrics}
        for name in query.metrics:
            if partial:
                columns.append(_Column(name, _final_sql(metrics[name], metrics, adapter)))
            else:
                columns.append(_Column(name, quote(name)))
        return self._env.get_template("engine/merge.sql").render(
            adapter=adapter,
            columns=columns,
            relation=relation,
            group_by=[quote(key) for key in keys] if partial else [],
            order_by=self._order_by(query, model, columns),
            limit=query.limit,
        ).strip()

    def _adapter(self, engine: Engine) -> Adapter:
        return self._macros.adapter(engine)

    @staticmethod
    def _order_by(query: MetricQuery, model: SemanticModel, columns: list[_Column]) -> list[OrderBy]:
        selected = [c.name for c in columns]
        for key in query.order_by:
            if key.field not in selected:
                raise QueryError(f"cannot order by '{key.field}': it is not a selected column ({', '.join(selected)})")
        keys = ([model.time_dimension] if query.grain else []) + query.dimensions
        return query.order_by or [OrderBy(field=key) for key in keys]

    @staticmethod
    def _check_dimensions(query: MetricQuery, model: SemanticModel, dimensions: dict[str, Dimension]) -> None:
        available = ", ".join(name for name in dimensions if name != model.time_dimension)
        for where, name in [("dimensions", d) for d in query.dimensions] + [("filters", f.dimension) for f in query.filters]:
            if name == model.time_dimension:
                fix = "set `grain` to group by time" if where == "dimensions" else "use start/end to filter by time"
                raise QueryError(f"{where}: '{name}' is the time dimension; {fix}")
            if name not in dimensions:
                raise QueryError(f"{where}: unknown dimension '{name}' in model '{model.name}'. Available: {available}")

    @staticmethod
    def _aggregate_sql(metric: AggregateMetric, adapter: Adapter, dialect: str | type[Dialect]) -> str:
        expr = parse_fragment(metric.sql).sql(dialect=dialect) if metric.sql else None
        args = (expr, metric.percentile) if metric.agg == "percentile" else (expr,)
        aggregate = getattr(adapter, metric.agg)(*args)
        if metric.filters:
            predicate = exp.and_(*(parse_fragment(f) for f in metric.filters))
            aggregate = adapter.filtered(aggregate, predicate.sql(dialect=dialect))
        return aggregate


def _parts(metric: Metric, metrics: dict) -> list[AggregateMetric]:
    """The aggregate metrics a metric is computed from: itself, or a ratio's two parts."""
    if isinstance(metric, AggregateMetric):
        return [metric]
    return [metrics[metric.numerator], metrics[metric.denominator]]


def _partial_pieces(metric: AggregateMetric) -> list[tuple[str, AggregateMetric]]:
    """(column name, aggregate to compute per tier) for a mergeable aggregate metric.
    Names start with "_", which ontology names can't, so they never collide."""
    if metric.agg == "avg":
        return [
            (f"_sum__{metric.name}", metric.model_copy(update={"agg": "sum"})),
            (f"_count__{metric.name}", metric.model_copy(update={"agg": "count"})),
        ]
    return [(f"_{metric.agg}__{metric.name}", metric)]


def _final_sql(metric: Metric, metrics: dict, adapter: Adapter) -> str:
    """DuckDB expression combining a metric's partial columns across tiers."""
    quote = adapter.quote
    if not isinstance(metric, AggregateMetric):
        return adapter.ratio(
            _final_sql(metrics[metric.numerator], metrics, adapter),
            _final_sql(metrics[metric.denominator], metrics, adapter),
        )
    if metric.agg == "avg":
        return adapter.ratio(adapter.sum(quote(f"_sum__{metric.name}")), adapter.sum(quote(f"_count__{metric.name}")))
    return getattr(adapter, _MERGE[metric.agg])(quote(f"_{metric.agg}__{metric.name}"))
