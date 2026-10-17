"""Typed contract for config/ontology.yaml.

The ontology describes what the data *means*: the dimensions you can slice by
and the metrics you can ask for. It says nothing about which engine (DuckDB,
Pinot, Trino) runs a query. The SQL compiler, query router, API gateway and
drift agent all read this contract, so it is validated strictly when it loads.
A bad config fails here with a clear message instead of producing broken SQL
later.
"""

from pathlib import Path
from collections.abc import Callable
from typing import Annotated, Any, Literal, Self, Union

import sqlglot
import yaml
from pydantic import AfterValidator, BaseModel, ConfigDict, Discriminator, Field, PrivateAttr, StringConstraints, Tag, model_validator
from sqlglot import exp

from agentic_lakehouse.macros import NO_PROJECT_MACROS, ProjectMacros, templated

# Names become SQL column aliases and API/MCP identifiers, so keep them plain:
# lowercase snake_case only. This also rules out SQL injection through a name.
Identifier = Annotated[str, StringConstraints(pattern=r"^[a-z][a-z0-9_]*$")]

# SQL fragments in the ontology (dimension and metric `sql`, metric `filters`)
# are written in DuckDB's dialect. The compiler re-renders them for each engine.
FRAGMENT_DIALECT = "duckdb"


def parse_fragment(sql: str, read: Any = FRAGMENT_DIALECT) -> exp.Expression:
    """Parse an ontology SQL fragment into a sqlglot expression tree, in `read`'s dialect
    (DuckDB's, unless it was rendered for an engine by a project macro).

    A fragment must be one scalar expression over the source table's columns.
    Statements, subqueries, aliases and `*` are rejected, and so are aggregate
    and window functions: aggregation is the compiler's job, driven by a
    metric's `agg`. Raises ValueError explaining what is wrong.
    """
    # Parsing it as a SELECT list item catches anything that isn't a single
    # expression, including a smuggled second statement ("x; DROP TABLE t").
    try:
        statements = sqlglot.parse(f"SELECT {sql}", read=read)
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
    if not templated(sql):  # one that calls the project's macros is checked once they're loaded (load_platform)
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
    lookup: Identifier | None = None  # set on a lookup's attribute, which the ontology adds (Lookup)

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

    def source_columns(self, render: Callable[[str], str] = lambda sql: sql) -> set[str]:
        """Columns of the source dataset that this model's dimensions and metrics read. `render`
        turns a snippet that calls macros into SQL (DuckDB's dialect)."""
        columns = {d.name for d in self.dimensions if d.sql is None}
        fragments = [d.sql for d in self.dimensions if d.sql is not None]
        for metric in self.metrics:
            if isinstance(metric, AggregateMetric):
                fragments += [metric.sql] if metric.sql else []
                fragments += metric.filters
        for sql in fragments:
            columns |= {column.name for column in parse_fragment(render(sql)).find_all(exp.Column)}
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


class CrossModelMetric(StrictModel):
    """A metric across models: numerator / denominator, times `scale`, where the two are metrics of
    any models, over different datasets even (bursts per 10,000 edits). The router asks for both,
    drilling across datasets when they're in different ones, and divides once it has them."""

    name: Identifier
    type: Literal["ratio"]
    description: str = Field(min_length=1)
    numerator: Identifier
    denominator: Identifier
    scale: float = Field(default=1, gt=0)  # e.g. 10000 for a rate per 10,000
    label: str = ""
    format: Format | None = None  # defaults to number
    unit: str = ""
    better: Better | None = None


LOOKUP_MAX_ROWS = 10_000  # a lookup compiles into every query that uses it, so it stays small
_SQL_TYPES = {"string": "VARCHAR", "number": "DOUBLE", "boolean": "BOOLEAN"}


class LookupAttribute(StrictModel):
    name: Identifier
    type: Literal["string", "number", "boolean"]
    description: str = ""
    label: str = ""
    missing: str | float | bool | None = None  # for a key the rows don't list; empty without it


class Lookup(StrictModel):
    """A small table keyed by a dimension, whose attributes become dimensions of every model with
    that dimension: each wiki's size tier, say. An attribute compiles into the query as a CASE over
    the key, built from the lookup's rows, so it's exact on every tier and for every aggregation.
    The rows are inline (`rows`), or read from `table` (lookups.read_lookups) every
    `refresh_minutes`; until then its attributes are empty. They describe the present, applied to
    all of history: a wiki that grew is counted in its new tier throughout."""

    name: Identifier
    description: str = ""
    key: Identifier  # the dimension it's keyed by
    rows: list[dict[str, Any]] | None = None  # inline, or the table's rows last read; each has the key and attributes
    table: Annotated[str, StringConstraints(pattern=r"^[a-z][a-z0-9_]*\.[a-z][a-z0-9_]*$")] | None = None  # <namespace>.<table>
    refresh_minutes: float = Field(default=60, gt=0)
    attributes: list[LookupAttribute] = Field(min_length=1)


class Ontology(StrictModel):
    version: str
    models: list[SemanticModel] = Field(min_length=1)
    metrics: list[CrossModelMetric] = []  # metrics across models; optional
    lookups: list[Lookup] = []  # dimension tables, whose attributes the models gain; optional
    _macros: Any = PrivateAttr(default=None)  # set by load_platform from config/macros/

    @model_validator(mode="before")
    @classmethod
    def _add_lookup_attributes(cls, data: Any) -> Any:
        """Each lookup's attributes, as dimensions of every model with its key, compiled from its
        rows. They're made again each time, so a dumped and reloaded ontology doesn't hold them twice."""
        if not isinstance(data, dict) or not data.get("lookups"):
            return data
        lookups = [Lookup.model_validate(lookup) for lookup in data["lookups"]]
        models = [dict(model) for model in data.get("models") or []]
        for model in models:  # a reloaded ontology's attributes come back as the lookups say
            model["dimensions"] = [d for d in model.get("dimensions") or [] if not _field(d, "lookup")]
        metric_names = {_field(m, "name") for model in models for m in model.get("metrics") or []}
        metric_names |= {_field(m, "name") for m in data.get("metrics") or []}
        for lookup in lookups:
            _check_lookup(lookup, models, metric_names)
            for model in models:
                key = next((d for d in model["dimensions"] if _field(d, "name") == lookup.key), None)
                if key is None:
                    continue
                key_sql = _field(key, "sql") or lookup.key
                model["dimensions"] = [
                    *model["dimensions"],
                    *(
                        {
                            "name": a.name, "type": a.type, "label": a.label, "lookup": lookup.name,
                            "description": a.description or lookup.description,
                            "sql": _attribute_sql(key_sql, lookup, a),
                        }
                        for a in lookup.attributes
                    ),
                ]  # fmt: skip
        return {**data, "models": models}

    def with_lookup_rows(self, rows: dict[str, list[dict]]) -> "Ontology":
        """This ontology with the named lookups' rows replaced (a table's, read again), and every
        attribute compiled from them. Raises ValidationError if the rows don't pass its checks."""
        spec = self.model_dump()
        for lookup in spec["lookups"]:
            if lookup["name"] in rows:
                lookup["rows"] = rows[lookup["name"]]
        ontology = Ontology.model_validate(spec)
        ontology._macros = self._macros
        return ontology

    @property
    def macros(self) -> "ProjectMacros":
        """The SQL macros this ontology compiles with: its project's over the engine's."""
        return self._macros or NO_PROJECT_MACROS

    def lenses_of(self, model: SemanticModel) -> list[SemanticModel]:
        """The other models over `model`'s dataset that keep time as it does: lenses on the same rows,
        whose metrics and dimensions combine with its own (models_together)."""
        return [m for m in self.models if m.name != model.name and m.source == model.source and _time_key(m) == _time_key(model)]

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
        across = set()
        for metric in self.metrics:
            if metric.name in owner or metric.name in across:
                where = f"'{owner[metric.name]}'" if metric.name in owner else "the metrics across models"
                raise ValueError(f"metric '{metric.name}' is defined in both {where} and the metrics across models")
            across.add(metric.name)
            for part in (metric.numerator, metric.denominator):
                if part not in owner:
                    raise ValueError(f"metric '{metric.name}' divides '{part}', which no model has")
        return self


def _field(item: Any, name: str) -> Any:
    """A field of a dimension or metric, given as a dict (YAML) or a model."""
    return item.get(name) if isinstance(item, dict) else getattr(item, name, None)


def _check_lookup(lookup: Lookup, models: list[dict], metric_names: set[str]) -> None:
    if lookup.rows is None and lookup.table is None:
        raise ValueError(f"lookup {lookup.name} has neither rows nor a table to read them from")
    keyed = [m for m in models if any(_field(d, "name") == lookup.key for d in m["dimensions"])]
    if not keyed:
        raise ValueError(f"lookup {lookup.name} is keyed by '{lookup.key}', which no model has as a dimension")
    rows = lookup.rows or []
    if len(rows) > LOOKUP_MAX_ROWS:
        raise ValueError(f"lookup {lookup.name} has {len(rows)} rows; at most {LOOKUP_MAX_ROWS}, as it compiles into every query using it")
    seen = set()
    for row in rows:
        if (key := row.get(lookup.key)) is not None and key in seen:  # a row without a key is left out
            raise ValueError(f"lookup {lookup.name} lists {key!r} twice")
        seen.add(key)
    for attribute in lookup.attributes:
        if attribute.name in metric_names:
            raise ValueError(f"attribute '{attribute.name}' of lookup {lookup.name} is a metric's name")
        for model in keyed:
            if any(_field(d, "name") == attribute.name for d in model["dimensions"]):
                raise ValueError(f"attribute '{attribute.name}' of lookup {lookup.name} is already a dimension of model {model['name']}")


def _attribute_sql(key_sql: str, lookup: Lookup, attribute: LookupAttribute) -> str:
    """The attribute as SQL (DuckDB's dialect, as ontology fragments are): a CASE over the key's
    SQL, with a WHEN for each row, and `missing` (or an empty value of its type) for other keys."""
    empty = f"CAST(NULL AS {_SQL_TYPES[attribute.type]})"
    otherwise = empty if attribute.missing is None else _literal(attribute.missing, attribute.type)
    whens = [
        f"WHEN {_literal(row[lookup.key])} THEN {_literal(row.get(attribute.name), attribute.type)}"
        for row in lookup.rows or []
        if row.get(lookup.key) is not None
    ]
    return f"CASE ({key_sql}) {' '.join(whens)} ELSE {otherwise} END" if whens else otherwise


def _literal(value: Any, type: str | None = None) -> str:
    """`value` as a SQL literal, quotes escaped; as `type` (an attribute's) when given."""
    if value is None:
        return f"CAST(NULL AS {_SQL_TYPES[type]})" if type else "NULL"
    if type == "string":
        value = str(value)
    elif type == "number":
        value = float(value)
    elif type == "boolean":
        value = bool(value)
    return exp.convert(value).sql(dialect=FRAGMENT_DIALECT)


def _time_key(model: SemanticModel) -> tuple:
    time = next(d for d in model.dimensions if d.name == model.time_dimension)
    return model.time_dimension, time.expr


def models_together(primary: list[SemanticModel], others: list[SemanticModel] = ()) -> tuple[SemanticModel, dict[str, tuple[str, ...]]]:
    """Models over the same dataset as one: every lens on the same rows, so their metrics and
    dimensions combine in one query. `primary` are the models asked for (a query's metrics', or a
    view's FROM); `others`, the rest over the dataset, add the metrics and dimensions they don't have.
    Returns the combined model and its ambiguous dimensions, by name: those defined differently by
    two models of the same group, with their models. A primary model's definition always wins over
    another model's, so a model asked for alone means what it always did."""
    first = primary[0]
    dimensions: dict[str, Dimension] = {}
    ambiguous: dict[str, tuple[str, ...]] = {}
    for group in (primary, others):
        found: dict[str, list[tuple[str, Dimension]]] = {}
        for model in group:
            for dimension in model.dimensions:
                found.setdefault(dimension.name, []).append((model.name, dimension))
        for name, definitions in found.items():
            if name in dimensions or name in ambiguous:
                continue
            if len({(d.type, d.expr) for _, d in definitions}) > 1:
                ambiguous[name] = tuple(dict.fromkeys(model for model, _ in definitions))
            else:
                dimensions[name] = definitions[0][1]
    metrics = list({m.name: m for model in (*primary, *others) for m in model.metrics}.values())
    names = [m.name for m in (*primary, *others)]
    together = SemanticModel(
        name="__".join(names),  # an identifier, as model names are
        description=f"Models {', '.join(names)} together: the same {first.source} rows.",
        source=first.source,
        time_dimension=first.time_dimension,
        dimensions=[d for name, d in dimensions.items() if name not in {m.name for m in metrics}],
        metrics=metrics,
    )
    return together, ambiguous


def load_ontology(path: str | Path) -> Ontology:
    """Parse and validate an ontology YAML file. Raises pydantic.ValidationError if it is invalid."""
    with open(path) as f:
        return Ontology.model_validate(yaml.safe_load(f) or {})
