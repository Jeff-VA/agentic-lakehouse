"""Compile semantic queries into SQL for a specific engine.

A MetricQuery asks for metrics from the ontology, sliced by dimensions, over a
time range. SqlCompiler turns it into SQL for DuckDB, Trino or Pinot:

- templates/query.sql is the query skeleton, shared by every engine.
- templates/macros/<engine>.sql renders the engine-specific parts
  (aggregations, time buckets, literals) and overrides only what differs from
  templates/macros/default.sql, like dbt's adapter.dispatch. Supporting another
  engine means writing one macro file.
- SQL from the ontology and filter values from clients never reach a template
  as raw text: sqlglot parses or builds them and prints them in the engine's
  syntax, correctly quoted.
"""

from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Literal, NamedTuple, Self

import jinja2
from pydantic import Field, field_validator, model_validator
from sqlglot import exp
from sqlglot.dialects.dialect import Dialect

from core.ontology import AggregateMetric, Dimension, Identifier, Ontology, SemanticModel, StrictModel, parse_fragment

Engine = Literal["duckdb", "trino", "pinot"]
Grain = Literal["minute", "hour", "day", "week", "month"]
Scalar = str | bool | int | float

TEMPLATES = Path(__file__).parent / "templates"


class Pinot(Dialect):
    """sqlglot dialect for Apache Pinot, which sqlglot doesn't ship.

    Pinot's Calcite-based SQL accepts sqlglot's generic output for the scalar
    expressions ontology fragments contain, so nothing is overridden yet.
    """


# The sqlglot dialect that prints ontology fragments and filters for each engine.
SQLGLOT_DIALECTS: dict[Engine, str | type[Dialect]] = {"duckdb": "duckdb", "trino": "trino", "pinot": Pinot}


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


_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def _environment() -> jinja2.Environment:
    env = jinja2.Environment(
        loader=jinja2.FileSystemLoader(TEMPLATES),
        undefined=jinja2.StrictUndefined,  # a typo in a template fails instead of rendering ""
        trim_blocks=True,
        lstrip_blocks=True,
        autoescape=False,  # this renders SQL, not HTML; values are already escaped by sqlglot
    )
    env.filters["sql_timestamp"] = lambda ts: ts.replace(tzinfo=None).isoformat(sep=" ")
    env.filters["epoch_millis"] = lambda ts: (ts - _EPOCH) // timedelta(milliseconds=1)
    return env


class _Adapter:
    """dbt-style macro dispatch: the engine's macro file first, then default.sql.

    Each macro comes back as a function returning a stripped SQL string. Macro
    files see this object as `adapter`, so macros calling each other get the
    same engine-first lookup.
    """

    def __init__(self, env: jinja2.Environment, engine: Engine):
        self.engine = engine
        context = {"adapter": self}
        self._modules = [
            env.get_template(f"macros/{engine}.sql").make_module(context),
            env.get_template("macros/default.sql").make_module(context),
        ]

    def __getattr__(self, name: str):
        if name.startswith("_"):  # not a macro; also stops recursion before _modules exists
            raise AttributeError(name)
        for module in self._modules:
            if (macro := getattr(module, name, None)) is not None:
                return lambda *args: str(macro(*args)).strip()
        raise AttributeError(f"no SQL macro '{name}' for engine '{self.engine}'")


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


class SqlCompiler:
    """Compiles MetricQuery objects into SQL for any supported engine."""

    def __init__(self, ontology: Ontology):
        self.ontology = ontology
        self._model_of = {metric.name: model for model in ontology.models for metric in model.metrics}
        self._env = _environment()
        self._adapters: dict[Engine, _Adapter] = {}

    def compile(self, query: MetricQuery, *, engine: Engine, table: str) -> str:
        """Render `query` as SQL for `engine`, reading from `table`.

        `table` comes from trusted storage config and is inserted verbatim, so
        it can be a qualified name (iceberg.raw.telemetry) or a table function.
        Raises QueryError if the query doesn't match the ontology.
        """
        model = self._model_for(query)
        dimensions = {d.name: d for d in model.dimensions}
        self._check_dimensions(query, model, dimensions)
        if engine not in self._adapters:
            self._adapters[engine] = _Adapter(self._env, engine)
        adapter = self._adapters[engine]
        dialect = SQLGLOT_DIALECTS[engine]

        time_tree = _dimension_tree(dimensions[model.time_dimension])
        grouped: list[_Column] = []
        if query.grain:
            grouped.append(_Column(model.time_dimension, adapter.time_bucket(time_tree.sql(dialect=dialect), query.grain)))
        for name in query.dimensions:
            grouped.append(_Column(name, _dimension_tree(dimensions[name]).sql(dialect=dialect)))

        metrics = {m.name: m for m in model.metrics}
        aggregates: list[_Column] = []
        for name in query.metrics:
            metric = metrics[name]
            if isinstance(metric, AggregateMetric):
                aggregates.append(_Column(name, self._aggregate_sql(metric, adapter, dialect)))
            else:
                numerator = self._aggregate_sql(metrics[metric.numerator], adapter, dialect)
                denominator = self._aggregate_sql(metrics[metric.denominator], adapter, dialect)
                aggregates.append(_Column(name, adapter.ratio(numerator, denominator)))

        selected = [c.name for c in grouped + aggregates]
        for key in query.order_by:
            if key.field not in selected:
                raise QueryError(f"cannot order by '{key.field}': it is not a selected column ({', '.join(selected)})")

        sql = self._env.get_template("query.sql").render(
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
            order_by=query.order_by or [OrderBy(field=c.name) for c in grouped],
            limit=query.limit,
        )
        return sql.strip()

    def _model_for(self, query: MetricQuery) -> SemanticModel:
        if unknown := [name for name in query.metrics if name not in self._model_of]:
            raise QueryError(f"unknown metrics: {', '.join(unknown)}. Available: {', '.join(sorted(self._model_of))}")
        models = sorted({self._model_of[name].name for name in query.metrics})
        if len(models) > 1:
            raise QueryError(f"metrics come from different models ({', '.join(models)}); query one model at a time")
        return self._model_of[query.metrics[0]]

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
    def _aggregate_sql(metric: AggregateMetric, adapter: _Adapter, dialect: str | type[Dialect]) -> str:
        expr = parse_fragment(metric.sql).sql(dialect=dialect) if metric.sql else None
        args = (expr, metric.percentile) if metric.agg == "percentile" else (expr,)
        aggregate = getattr(adapter, metric.agg)(*args)
        if metric.filters:
            predicate = exp.and_(*(parse_fragment(f) for f in metric.filters))
            aggregate = adapter.filtered(aggregate, predicate.sql(dialect=dialect))
        return aggregate
