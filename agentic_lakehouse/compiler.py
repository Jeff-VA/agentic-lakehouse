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

from bisect import bisect_right
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Literal, NamedTuple, Self

from pydantic import Field, field_validator, model_validator
from sqlglot import exp, generator
from sqlglot.dialects.dialect import Dialect

from agentic_lakehouse.macros import Adapter, templated
from agentic_lakehouse.ontology import (
    AggregateMetric,
    Dimension,
    Identifier,
    Metric,
    Ontology,
    SemanticModel,
    StrictModel,
    models_together,
    parse_fragment,
)

Engine = Literal["duckdb", "trino", "pinot"]
Grain = Literal["minute", "hour", "day", "week", "month"]
Scalar = str | bool | int | float

def _coalesce_as_case(self: generator.Generator, expression: exp.Coalesce) -> str:
    """COALESCE(a, b) as CASE WHEN a IS NULL THEN b ELSE a END, nested for more arguments."""
    args = [expression.this, *expression.expressions]
    case = args[-1].copy()
    for arg in reversed(args[:-1]):
        case = exp.Case(ifs=[exp.If(this=exp.Is(this=arg.copy(), expression=exp.Null()), true=case)], default=arg.copy())
    return self.sql(case)


class Pinot(Dialect):
    """sqlglot dialect for Apache Pinot, which sqlglot doesn't ship.

    Pinot's Calcite-based SQL accepts sqlglot's generic output for the scalar
    expressions ontology fragments contain, with two exceptions:
    - That output doesn't know Pinot's reserved words (a column named
      "timestamp" or "value"), so every identifier is quoted.
    - Pinot's multi-stage engine can't COALESCE booleans: grouping by one
      fails ("String cannot be cast to Number"), and in a filter it silently
      counts nothing (Pinot 1.5). So COALESCE is written as the CASE it means.
    """

    class Generator(generator.Generator):
        TRANSFORMS = {**generator.Generator.TRANSFORMS, exp.Coalesce: _coalesce_as_case}

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


def _filter_tree(query_filter: Filter, column: exp.Expression) -> exp.Expression:
    operand = _operand(column)
    if query_filter.op in ("in", "not_in"):
        condition = operand.isin(*query_filter.value)
        return exp.not_(condition) if query_filter.op == "not_in" else condition
    return _COMPARISONS[query_filter.op](this=operand, expression=exp.convert(query_filter.value))


# How each aggregation's partial results from several tiers combine. Averages
# split into a sum and a count; count_distinct and percentile can't be combined
# as partial aggregates alone, but can once the router has one tier's values (_BY_VALUES).
_MERGE = {"count": "sum", "sum": "sum", "min": "min", "max": "max"}
_BY_VALUES = ("count_distinct", "percentile")
# The quantiles the historical tier gives a percentile's distribution at, for the merge with the
# real-time tier's values: 0, 0.001, ..., 1. Every percentile to 0.1% is one of them.
GRID = [i / 1000 for i in range(1001)]


class SqlCompiler:
    """Compiles MetricQuery objects into SQL for any supported engine.

    compile() answers a query on one engine. For a query split across tiers,
    compile(partial=True) gives each tier's partial aggregates and merge_sql()
    combines them in DuckDB.
    """

    def __init__(self, ontology: Ontology):
        self.ontology = ontology
        self._model_of = {metric.name: model for model in ontology.models for metric in model.metrics}
        self._together: dict[tuple[str, ...], tuple[SemanticModel, dict]] = {}  # by the models' names

    @property
    def _macros(self):
        # Read at each use, not kept: a live reload of config/macros/ replaces the ontology's macros.
        return self.ontology.macros

    @property
    def _env(self):
        return self.ontology.macros.env

    def model_for(self, query: MetricQuery) -> SemanticModel:
        """The model the query reads: its metrics' model, as it always was; or, when they come from
        several models over one dataset, or it asks for a dimension only another model over the
        dataset has, those models together (models_together). Raises QueryError if its metrics come
        from models over different datasets, or a dimension it asks for means different things in
        the models its metrics come from."""
        if unknown := [name for name in query.metrics if name not in self._model_of]:
            raise QueryError(f"unknown metrics: {', '.join(unknown)}. Available: {', '.join(sorted(self._model_of))}")
        models = list({self._model_of[name].name: self._model_of[name] for name in query.metrics}.values())
        lenses = {m.name for model in models[:1] for m in self.ontology.lenses_of(model)}
        if len(models) > 1 and not {m.name for m in models[1:]} <= lenses:
            raise QueryError(f"metrics come from different models ({', '.join(sorted(m.name for m in models))}); query one model at a time")
        asked = [*query.dimensions, *(f.dimension for f in query.filters)]
        if len(models) == 1:
            own = {d.name for d in models[0].dimensions}
            elsewhere = {d.name for m in self.ontology.lenses_of(models[0]) for d in m.dimensions}
            if all(name in own or name not in elsewhere for name in asked):
                return models[0]  # an unknown dimension is reported against it, as before
        names = tuple(m.name for m in models)
        if names not in self._together:
            self._together[names] = models_together(models, [m for m in self.ontology.lenses_of(models[0]) if m.name not in names])
        together, ambiguous = self._together[names]
        if clash := next((name for name in asked if name in ambiguous), None):
            raise QueryError(
                f"'{clash}' means different things in models {' and '.join(ambiguous[clash])}: ask for it with metrics "
                "of one of them, or give the other's dimension its own name"
            )
        return together

    def unmergeable_metrics(self, query: MetricQuery) -> list[str]:
        """Requested metrics whose results from two tiers can't be combined (percentiles, distinct counts)."""
        metrics = {m.name: m for m in self.model_for(query).metrics}
        return [name for name in query.metrics if any(m.agg not in _MERGE and m.agg != "avg" for m in _parts(metrics[name], metrics))]

    def distinct_parts(self, query: MetricQuery) -> list[AggregateMetric]:
        """The count_distinct aggregates `query`'s metrics are computed from, each once."""
        return self._parts_with(query, "count_distinct")

    def percentile_parts(self, query: MetricQuery) -> list[AggregateMetric]:
        """The percentile aggregates `query`'s metrics are computed from, each once."""
        return self._parts_with(query, "percentile")

    def combinable_given_values(self, query: MetricQuery) -> bool:
        """Whether two tiers' results for `query` combine once the router has one tier's values for its
        distinct counts and percentiles (values_sql): `query` has some, and the rest merge as
        partial aggregates."""
        metrics = {m.name: m for m in self.model_for(query).metrics}
        parts = [p for name in query.metrics for p in _parts(metrics[name], metrics)]
        return any(p.agg in _BY_VALUES for p in parts) and all(p.agg in _MERGE or p.agg == "avg" or p.agg in _BY_VALUES for p in parts)

    def _parts_with(self, query: MetricQuery, agg: str) -> list[AggregateMetric]:
        metrics = {m.name: m for m in self.model_for(query).metrics}
        return list({p.name: p for name in query.metrics for p in _parts(metrics[name], metrics) if p.agg == agg}.values())

    def compile(
        self,
        query: MetricQuery,
        *,
        engine: Engine,
        table: str,
        partial: bool = False,
        overlap: dict[str, list[tuple]] | None = None,
        grids: bool = False,
    ) -> str:
        """Render `query` as SQL for `engine`, reading from `table`.

        `table` comes from trusted storage config and is inserted verbatim, so
        it can be a qualified name (iceberg.raw.telemetry) or a table function.
        With partial=True the result has one column per partial aggregate
        (named `_<agg>__<metric>`) instead of the metrics, for merge_sql() to
        combine; ordering and limit are left to the merge.
        Raises QueryError if the query doesn't match the ontology, or if
        partial=True and a metric can't be combined across tiers.

        `overlap` and `grids`, with partial=True, are for the router's merge of
        distinct counts and percentiles, once it has the other tier's values
        (values_sql). Given `overlap`:
        - count_distinct metrics become partial aggregates too
          (`_count_distinct__<metric>`), and each one `overlap` names gets an
          `_overlap__<metric>` column: how many of the values it lists (tuples of
          the query's dimension values, then the value) this tier saw as well.
          An empty dict gives the distinct counts and no overlap columns.
        - percentile metrics, with grids=True, become this tier's distribution:
          its quantiles at each of GRID (`_grid__<metric>`) and its count of values
          (`_n__<metric>`). With grids=False they're left out; their values come
          from values_sql.
        """
        model = self.model_for(query)
        dimensions = {d.name: d for d in model.dimensions}
        self._check_dimensions(query, model, dimensions)
        adapter = self._adapter(engine)
        dialect = SQLGLOT_DIALECTS[engine]

        time_tree = self._dimension_tree(dimensions[model.time_dimension], engine)
        grouped: list[_Column] = []
        if query.grain:
            grouped.append(_Column(model.time_dimension, adapter.time_bucket(time_tree.sql(dialect=dialect), query.grain)))
        for name in query.dimensions:
            grouped.append(_Column(name, self._dimension_tree(dimensions[name], engine).sql(dialect=dialect)))

        metrics = {m.name: m for m in model.metrics}
        aggregates: list[_Column] = []
        if partial:
            if (unmergeable := self.unmergeable_metrics(query)) and (overlap is None or not self.combinable_given_values(query)):
                raise QueryError(f"can't combine across tiers: {', '.join(unmergeable)}")
            columns = {}
            for name in query.metrics:
                for part in _parts(metrics[name], metrics):
                    if part.agg == "percentile":
                        if grids:
                            columns[f"_grid__{part.name}"] = self._grid_sql(part, engine)
                            columns[f"_n__{part.name}"] = self._aggregate_sql(part.model_copy(update={"agg": "count", "percentile": None}), engine)
                        continue
                    for column, piece in _partial_pieces(part):
                        columns[column] = self._aggregate_sql(piece, engine)
                    if overlap is not None and part.name in overlap:
                        columns[f"_overlap__{part.name}"] = self._overlap_sql(part, query.dimensions, dimensions, overlap[part.name], engine)
            if not columns and not grouped:  # only percentiles, whose values come from values_sql: a SELECT needs a column
                columns["_rows"] = adapter.count(None)
            aggregates = [_Column(column, sql) for column, sql in columns.items()]
        else:
            for name in query.metrics:
                metric = metrics[name]
                if isinstance(metric, AggregateMetric):
                    aggregates.append(_Column(name, self._aggregate_sql(metric, engine)))
                else:
                    numerator = self._aggregate_sql(metrics[metric.numerator], engine)
                    denominator = self._aggregate_sql(metrics[metric.denominator], engine)
                    aggregates.append(_Column(name, adapter.ratio(numerator, denominator)))

        return self._render(
            query, model, dimensions, engine, table, grouped + aggregates,
            group_by=[c.sql for c in grouped],
            order_by=[] if partial else self._order_by(query, model, grouped + aggregates),
            limit=None if partial else query.limit,
        )  # fmt: skip

    def values_sql(self, query: MetricQuery, metric: AggregateMetric, *, engine: Engine, table: str) -> str:
        """SQL listing, for each group of `query`, the distinct values of `metric`'s expression
        (a count_distinct or percentile metric) where its filters hold, as `_value` (NULL where they
        don't), and how many rows had each, as `_n`. The router asks the other tier how many of the
        values it saw too (compile's `overlap`), or merges them with its distribution
        (merge_quantile). `query` has no time grain."""
        model = self.model_for(query)
        dimensions = {d.name: d for d in model.dimensions}
        self._check_dimensions(query, model, dimensions)
        adapter, dialect = self._adapter(engine), SQLGLOT_DIALECTS[engine]
        grouped = [_Column(name, self._dimension_tree(dimensions[name], engine).sql(dialect=dialect)) for name in query.dimensions]
        value = self._fragment(metric.sql, engine)
        if metric.filters:
            value = exp.Case(ifs=[exp.If(this=exp.and_(*(self._fragment(f, engine) for f in metric.filters)), true=value)])
        value_sql = value.sql(dialect=dialect)
        return self._render(
            query, model, dimensions, engine, table, [*grouped, _Column("_value", value_sql), _Column("_n", adapter.count(None))],
            group_by=[c.sql for c in grouped] + [value_sql], order_by=[], limit=None,
        )  # fmt: skip

    def percentile_inputs_sql(self, query: MetricQuery, metric: AggregateMetric, historical: str, values: str) -> str:
        """DuckDB SQL giving, for each group, what merge_quantile() needs for percentile `metric`:
        the historical tier's grid and count of values (`grid`, `grid_count`, from compile(grids=True)
        rows in `historical`) and the real-time tier's values ascending with their counts (`values`,
        `counts`, from values_sql rows in `values`). The router puts the result back in the merge
        as `_merged__<metric>`."""
        quote = self._adapter("duckdb").quote
        groups = [quote(name) for name in query.dimensions]
        select = "".join(f"{g}, " for g in groups)
        group_by = f" GROUP BY {', '.join(groups)}" if groups else ""
        grid, count = quote(f"_grid__{metric.name}"), quote(f"_n__{metric.name}")
        return (
            f"SELECT {select}CAST(ANY_VALUE(grid) AS DOUBLE[]) AS grid, CAST(SUM(grid_count) AS BIGINT) AS grid_count, "
            f"ANY_VALUE(\"values\") AS \"values\", ANY_VALUE(counts) AS counts FROM ("
            f"SELECT {select}{grid} AS grid, {count} AS grid_count FROM {historical} UNION ALL BY NAME "
            f"SELECT {select}list(CAST(\"_value\" AS DOUBLE) ORDER BY \"_value\") AS \"values\", "
            f"list(CAST(\"_n\" AS BIGINT) ORDER BY \"_value\") AS counts FROM {values} WHERE \"_value\" IS NOT NULL{group_by}"
            f"){group_by}"
        )

    def _render(self, query, model, dimensions, engine, table, columns, *, group_by, order_by, limit) -> str:
        adapter, dialect = self._adapter(engine), SQLGLOT_DIALECTS[engine]
        time_tree = self._dimension_tree(dimensions[model.time_dimension], engine)
        return self._env.get_template("engine/query.sql").render(
            adapter=adapter,
            columns=columns,
            table=table,
            time_expr=_operand(time_tree).sql(dialect=dialect),
            start=query.start,
            end=query.end,
            filters=[
                _filter_tree(f, self._dimension_tree(dimensions[f.dimension], engine)).sql(dialect=dialect) for f in query.filters
            ],
            group_by=group_by,
            order_by=order_by,
            limit=limit,
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

    def _aggregate_sql(self, metric: AggregateMetric, engine: Engine) -> str:
        adapter, dialect = self._adapter(engine), SQLGLOT_DIALECTS[engine]
        expr = self._fragment(metric.sql, engine).sql(dialect=dialect) if metric.sql else None
        args = (expr, metric.percentile) if metric.agg == "percentile" else (expr,)
        aggregate = getattr(adapter, metric.agg)(*args)
        if metric.filters:
            predicate = exp.and_(*(self._fragment(f, engine) for f in metric.filters))
            aggregate = adapter.filtered(aggregate, predicate.sql(dialect=dialect))
        return aggregate

    def _grid_sql(self, metric: AggregateMetric, engine: Engine) -> str:
        """Percentile `metric`'s quantiles at each of GRID, as one array, where its filters hold."""
        adapter, dialect = self._adapter(engine), SQLGLOT_DIALECTS[engine]
        grid = adapter.quantile_grid(self._fragment(metric.sql, engine).sql(dialect=dialect), GRID)
        if metric.filters:
            grid = adapter.filtered(grid, exp.and_(*(self._fragment(f, engine) for f in metric.filters)).sql(dialect=dialect))
        return grid

    def _overlap_sql(self, metric: AggregateMetric, names: list[str], dimensions: dict, keys: list[tuple], engine: Engine) -> str:
        """count_distinct `metric`, counting only values seen in the same group as in `keys` (tuples
        of the dimension values named by `names`, then the value): how many of the other tier's
        values this tier saw too. The values become SQL literals, so names with quotes are safe."""
        if not keys:
            return "0"
        adapter, dialect = self._adapter(engine), SQLGLOT_DIALECTS[engine]
        columns = [self._dimension_tree(dimensions[name], engine) for name in names] + [self._fragment(metric.sql, engine)]

        def row(values: tuple) -> exp.Expression:
            return exp.convert(values[0]) if len(columns) == 1 else exp.Tuple(expressions=[exp.convert(v) for v in values])

        conditions = []
        if plain := [k for k in keys if None not in k]:
            this = columns[0].copy() if len(columns) == 1 else exp.Tuple(expressions=[c.copy() for c in columns])
            conditions.append(exp.In(this=this, expressions=[row(k) for k in plain]))
        for k in keys:  # IN never matches NULL, so a group whose dimension is NULL is matched with IS NULL
            if None in k:
                conditions.append(exp.and_(*(c.copy().is_(exp.null()) if v is None else c.copy().eq(exp.convert(v)) for c, v in zip(columns, k))))
        predicate = exp.and_(*(self._fragment(f, engine) for f in metric.filters), exp.or_(*conditions))
        return adapter.filtered(adapter.count_distinct(columns[-1].sql(dialect=dialect)), predicate.sql(dialect=dialect))

    def _dimension_tree(self, dimension: Dimension, engine: Engine) -> exp.Expression:
        # exp.column quotes names that are keywords in the target dialect.
        return self._fragment(dimension.sql, engine) if dimension.sql else exp.column(dimension.name)

    def _fragment(self, sql: str, engine: Engine) -> exp.Expression:
        """An ontology snippet as a tree for `engine`. One that calls the project's macros is
        rendered with the engine's and read in its dialect; a plain one is read in DuckDB's, as
        written, and printed for the engine by sqlglot."""
        if not templated(sql):
            return parse_fragment(sql)
        return parse_fragment(self._macros.render(sql, engine), read=SQLGLOT_DIALECTS[engine])


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
    if metric.agg == "count_distinct":  # each tier's count, less the values both saw (compile's `overlap`)
        overlap = adapter.sum(quote(f"_overlap__{metric.name}"))
        return f"({adapter.sum(quote(f'_count_distinct__{metric.name}'))} - COALESCE({overlap}, 0))"
    if metric.agg == "percentile":  # merged by the router for each group (merge_quantile), one row per group
        return f"ANY_VALUE({quote(f'_merged__{metric.name}')})"
    return getattr(adapter, _MERGE[metric.agg])(quote(f"_{metric.agg}__{metric.name}"))


def merge_quantile(grid: list | None, grid_count: int | None, values: list | None, counts: list | None, fraction: float) -> float | None:
    """The `fraction` quantile of two tiers' values together: one tier's distribution, as its
    quantiles at each of GRID over `grid_count` values, and the other tier's `values`, ascending,
    with their `counts`. Each side is read as a piecewise-linear distribution through its
    quantiles (between ordered values, as quantile_cont interpolates), and the two are weighted by
    their counts, so a group seen on one side only gets that side's own quantile. None without
    values. The router calls it for each group, with percentile_inputs_sql's rows."""
    sides = []  # (how many values, the quantiles' values ascending, their fractions)
    if grid and grid_count:
        sides.append((grid_count, list(grid), [i / (len(grid) - 1) for i in range(len(grid))]))
    if values:
        n, xs, qs, rank = sum(counts), [], [], 0
        for value, count in zip(values, counts):  # a value repeated `count` times spans ranks rank .. rank + count - 1
            xs += [value, value]
            qs += [rank / max(n - 1, 1), (rank + count - 1) / max(n - 1, 1)]
            rank += count
        if n == 1:
            qs = [0.0, 1.0]
        sides.append((n, xs, qs))
    if not sides:
        return None

    total = sum(n for n, _, _ in sides)

    def share_at_or_below(x: float) -> float:
        return sum(n * _cdf(xs, qs, x) for n, xs, qs in sides) / total

    lo, hi = min(xs[0] for _, xs, _ in sides), max(xs[-1] for _, xs, _ in sides)
    if fraction <= 0:
        return lo
    while (mid := (lo + hi) / 2) not in (lo, hi):  # the least x with share_at_or_below(x) >= fraction
        if share_at_or_below(mid) >= fraction:
            hi = mid
        else:
            lo = mid
    return hi


def _cdf(xs: list, qs: list, x: float) -> float:
    """The share of a piecewise-linear distribution through (xs[i], qs[i]) at or below `x`."""
    if x < xs[0]:
        return 0.0
    if x >= xs[-1]:
        return 1.0
    i = bisect_right(xs, x)  # xs[i - 1] <= x < xs[i]
    return qs[i - 1] + (x - xs[i - 1]) / (xs[i] - xs[i - 1]) * (qs[i] - qs[i - 1])
