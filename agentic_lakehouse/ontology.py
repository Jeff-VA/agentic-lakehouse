"""Typed contract for config/ontology.yaml.

The ontology describes what the data *means*: the dimensions you can slice by
and the metrics you can ask for. It says nothing about which engine (DuckDB,
Pinot, Trino) runs a query. The SQL compiler, query router, API gateway and
drift agent all read this contract, so it is validated strictly when it loads.
A bad config fails here with a clear message instead of producing broken SQL
later.
"""

from pathlib import Path
from typing import Annotated, Any, Literal, Self, Union

import sqlglot
import yaml
from pydantic import AfterValidator, BaseModel, ConfigDict, Discriminator, Field, StringConstraints, Tag, model_validator
from sqlglot import exp

# Names become SQL column aliases and API/MCP identifiers, so keep them plain:
# lowercase snake_case only. This also rules out SQL injection through a name.
Identifier = Annotated[str, StringConstraints(pattern=r"^[a-z][a-z0-9_]*$")]

# SQL fragments in the ontology (dimension and metric `sql`, metric `filters`)
# are written in DuckDB's dialect. The compiler re-renders them for each engine.
FRAGMENT_DIALECT = "duckdb"


def parse_fragment(sql: str) -> exp.Expression:
    """Parse an ontology SQL fragment into a sqlglot expression tree.

    A fragment must be one scalar expression over the source table's columns.
    Statements, subqueries, aliases and `*` are rejected, and so are aggregate
    and window functions: aggregation is the compiler's job, driven by a
    metric's `agg`. Raises ValueError explaining what is wrong.
    """
    # Parsing it as a SELECT list item catches anything that isn't a single
    # expression, including a smuggled second statement ("x; DROP TABLE t").
    try:
        statements = sqlglot.parse(f"SELECT {sql}", read=FRAGMENT_DIALECT)
    except sqlglot.ParseError as e:
        raise ValueError(f"invalid SQL {sql!r}: {e.errors[0]['description']}") from None
    select = statements[0] if len(statements) == 1 else None
    if (
        not isinstance(select, exp.Select)
        or len(select.expressions) != 1
        or any(value for key, value in select.args.items() if key != "expressions")
    ):
        raise ValueError(f"{sql!r} must be a single SQL expression")

    [tree] = select.expressions
    if tree.find(exp.IntDiv):
        raise ValueError(f"{sql!r} must not use // (integer division): DuckDB truncates it, Trino rounds; write floor(a / b)")
    forbidden = {
        "an alias (AS ...)": exp.Alias,
        "*": exp.Star,
        "a subquery": (exp.Subquery, exp.Select),
        "an aggregate function": exp.AggFunc,
        "a window function": exp.Window,
    }
    for what, kinds in forbidden.items():
        if tree.find(kinds):
            raise ValueError(f"{sql!r} must not contain {what}")
    return tree


def _check_fragment(sql: str) -> str:
    parse_fragment(sql)
    return sql


# A SQL expression over the source table's columns, e.g. "client_isp" or a CASE.
SqlExpression = Annotated[str, AfterValidator(_check_fragment)]

DimensionType = Literal["string", "number", "boolean", "timestamp"]
Aggregation = Literal["count", "count_distinct", "sum", "avg", "min", "max", "percentile"]
# How people read a metric's values: dashboards and agents format them the same way.
Format = Literal["number", "integer", "percent"]  # percent: 0.05 reads as 5%
Better = Literal["higher", "lower"]  # which way a change is good news


def words(name: str) -> str:
    """A name as words, for when no label is set: rebuffer_incidence_rate -> Rebuffer incidence rate."""
    text = name.replace("_", " ").strip()
    return text[:1].upper() + text[1:]


class StrictModel(BaseModel):
    # Unknown keys are almost always typos (e.g. `descripton:`), so reject them
    # instead of silently ignoring them. Frozen: loaded config is read-only.
    model_config = ConfigDict(extra="forbid", frozen=True)


class Dimension(StrictModel):
    name: Identifier
    type: DimensionType
    description: str = ""
    sql: SqlExpression | None = None  # defaults to the column of the same name
    label: str = ""  # how people say it, e.g. "ISP"; empty means the name in words

    @property
    def expr(self) -> str:
        return self.sql or self.name


class AggregateMetric(StrictModel):
    """One aggregation over one expression, e.g. the p99 of request_latency_ms."""

    name: Identifier
    type: Literal["aggregate"] = "aggregate"
    description: str = Field(min_length=1)  # the AI agent picks metrics by reading these
    agg: Aggregation
    sql: SqlExpression | None = None  # expression to aggregate; omit it with `count` for COUNT(*)
    percentile: float | None = Field(default=None, gt=0, lt=1)  # 0.99 means p99
    filters: list[SqlExpression] = []  # SQL predicates, ANDed together
    label: str = ""  # how people say it, e.g. "p99 time to first byte"
    format: Format | None = None  # counts default to integer, everything else to number
    unit: str = ""  # shown after values, e.g. "ms"
    better: Better | None = None

    @model_validator(mode="after")
    def _check_agg_params(self) -> Self:
        if self.sql is None and self.agg != "count":
            raise ValueError(f"metric '{self.name}': agg '{self.agg}' needs a `sql` expression to aggregate")
        if (self.agg == "percentile") != (self.percentile is not None):
            raise ValueError(
                f"metric '{self.name}': `percentile` (e.g. 0.99) is required with agg 'percentile' "
                "and not allowed otherwise"
            )
        return self


class RatioMetric(StrictModel):
    """numerator / denominator, where both are aggregate metrics in the same model.

    The two parts are kept separate instead of being written as one SQL
    expression. That lets the query router add up each part across the
    real-time and historical tiers and divide once at the end.
    """

    name: Identifier
    type: Literal["ratio"]
    description: str = Field(min_length=1)
    numerator: Identifier
    denominator: Identifier
    label: str = ""
    format: Format | None = None  # defaults to number
    unit: str = ""
    better: Better | None = None


def _metric_type(value: Any) -> str:
    # `type` may be omitted for the common case, so it defaults to "aggregate".
    if isinstance(value, dict):
        return value.get("type", "aggregate")
    return getattr(value, "type", "aggregate")


Metric = Annotated[
    Union[Annotated[AggregateMetric, Tag("aggregate")], Annotated[RatioMetric, Tag("ratio")]],
    Discriminator(_metric_type),
]


def display_format(metric: "AggregateMetric | RatioMetric") -> Format:
    if metric.format:
        return metric.format
    return "integer" if isinstance(metric, AggregateMetric) and metric.agg in ("count", "count_distinct") else "number"


def index_by_name(items: list, kind: str, where: str) -> dict:
    index = {}
    for item in items:
        if item.name in index:
            raise ValueError(f"{where}: duplicate {kind} name '{item.name}'")
        index[item.name] = item
    return index


class SemanticModel(StrictModel):
    """One logical dataset (e.g. playback telemetry) and the questions you can ask of it."""

    name: Identifier
    description: str = Field(min_length=1)
    source: Identifier  # logical dataset name; storage config will bind it to physical tables per tier
    time_dimension: Identifier  # drives time windows and real-time vs historical routing
    dimensions: list[Dimension] = Field(min_length=1)
    metrics: list[Metric] = Field(min_length=1)

    def source_columns(self) -> set[str]:
        """Columns of the source dataset that this model's dimensions and metrics read."""
        columns = {d.name for d in self.dimensions if d.sql is None}
        fragments = [d.sql for d in self.dimensions if d.sql is not None]
        for metric in self.metrics:
            if isinstance(metric, AggregateMetric):
                fragments += [metric.sql] if metric.sql else []
                fragments += metric.filters
        for sql in fragments:
            columns |= {column.name for column in parse_fragment(sql).find_all(exp.Column)}
        return columns

    @model_validator(mode="after")
    def _check_references(self) -> Self:
        where = f"model '{self.name}'"
        dimensions = index_by_name(self.dimensions, "dimension", where)
        metrics = index_by_name(self.metrics, "metric", where)

        # Dimensions and metrics share one namespace: both become result columns.
        if clash := dimensions.keys() & metrics.keys():
            raise ValueError(f"{where}: names used for both a dimension and a metric: {', '.join(sorted(clash))}")

        time_dim = dimensions.get(self.time_dimension)
        if time_dim is None:
            raise ValueError(f"{where}: time_dimension '{self.time_dimension}' is not one of its dimensions")
        if time_dim.type != "timestamp":
            raise ValueError(
                f"{where}: time_dimension '{self.time_dimension}' must have type timestamp, not {time_dim.type}"
            )

        for metric in self.metrics:
            if isinstance(metric, RatioMetric):
                for part in (metric.numerator, metric.denominator):
                    if not isinstance(metrics.get(part), AggregateMetric):
                        raise ValueError(
                            f"{where}: ratio metric '{metric.name}' uses '{part}', "
                            "which is not an aggregate metric in this model"
                        )
        return self


class Ontology(StrictModel):
    version: str
    models: list[SemanticModel] = Field(min_length=1)

    @model_validator(mode="after")
    def _check_unique_names(self) -> Self:
        index_by_name(self.models, "model", "ontology")
        # Clients ask for a metric by name alone, so each name must mean one thing.
        owner: dict[str, str] = {}
        for model in self.models:
            for metric in model.metrics:
                if metric.name in owner:
                    raise ValueError(
                        f"metric '{metric.name}' is defined in both '{owner[metric.name]}' and '{model.name}'"
                    )
                owner[metric.name] = model.name
        return self


def load_ontology(path: str | Path) -> Ontology:
    """Parse and validate an ontology YAML file. Raises pydantic.ValidationError if it is invalid."""
    with open(path) as f:
        return Ontology.model_validate(yaml.safe_load(f) or {})
