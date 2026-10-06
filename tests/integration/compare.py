"""Check an engine's answer to a semantic query against DuckDB's exact answer
over the same seeded events (DuckDB's `telemetry` view)."""

from datetime import UTC, timedelta

import pytest

from core.compiler import Filter, MetricQuery, SqlCompiler
from core.engines import DuckDBEngine
from core.ontology import AggregateMetric, Ontology, SemanticModel

# Approximate percentiles (Trino's approx_percentile, Pinot's t-digest) promise
# accuracy in rank, not in value: the answer lands within about 1% of the
# requested position in the sorted values. Where values are sparse, such as the
# top of a small group, neighbours can be 30% apart, so a value tolerance fails
# good answers while a rank tolerance doesn't.
RANK_ERROR = 0.01

_GRAIN_STEPS = {
    "minute": timedelta(minutes=1),
    "hour": timedelta(hours=1),
    "day": timedelta(days=1),
    "week": timedelta(weeks=1),
}


def assert_same_result(ontology: Ontology, duckdb: DuckDBEngine, query: MetricQuery, expected: list, actual: list) -> None:
    """`actual` rows answer `query` like DuckDB's exact `expected` rows: exact
    keys and counts, floats to 1e-9, percentiles by rank (see RANK_ERROR)."""
    model = SqlCompiler(ontology).model_for(query)
    percentiles = {m.name: m for m in model.metrics if isinstance(m, AggregateMetric) and m.agg == "percentile"}
    assert len(actual) == len(expected)
    for actual_row, expected_row in zip(actual, expected):
        assert list(actual_row) == list(expected_row)
        for column, value in expected_row.items():
            if column in percentiles and value is not None:
                assert_percentile_rank(ontology, duckdb, query, model, actual_row, percentiles[column])
            elif isinstance(value, float):
                assert actual_row[column] == pytest.approx(value, rel=1e-9), column
            else:
                assert actual_row[column] == value, column


def assert_percentile_rank(
    ontology: Ontology, duckdb: DuckDBEngine, query: MetricQuery, model: SemanticModel, row: dict, metric: AggregateMetric
) -> None:
    """row[metric] sits within RANK_ERROR of the metric's percentile among the
    values it summarizes, give or take one value (the step size of a small group)."""
    value = row[metric.name]
    n, below, at_or_below = _rank_counts(ontology, duckdb, _group_query(query, model, row), model, metric, value)
    slack = RANK_ERROR + 1 / n
    assert below / n - slack <= metric.percentile <= at_or_below / n + slack, (
        f"{metric.name} = {value} in group {row}: {below} to {at_or_below} of {n} values are below it, "
        f"so it is not within {RANK_ERROR:.0%} of the {metric.percentile:.0%} position"
    )


def _group_query(query: MetricQuery, model: SemanticModel, row: dict) -> MetricQuery:
    """`query` narrowed to the events behind one result row."""
    update = {
        "dimensions": [],
        "filters": [*query.filters, *(Filter(dimension=d, op="=", value=row[d]) for d in query.dimensions)],
        "grain": None,
        "order_by": [],
        "limit": None,
    }
    if query.grain:
        bucket = row[model.time_dimension].replace(tzinfo=UTC)
        update["start"] = max(query.start, bucket)
        update["end"] = min(query.end, bucket + _GRAIN_STEPS[query.grain])
    return query.model_copy(update=update)


def _rank_counts(
    ontology: Ontology, duckdb: DuckDBEngine, query: MetricQuery, model: SemanticModel, metric: AggregateMetric, value: float
) -> tuple[int, int, int]:
    """How many values the percentile summarizes, and how many are below / at or below `value`.

    Counted through the semantic layer itself: three count metrics, with the
    percentile metric's own filters, are added to a copy of the ontology.
    """
    def count(name: str, condition: str) -> dict:
        return {"name": name, "description": "Rank check.", "agg": "count", "filters": [*metric.filters, condition]}

    raw = ontology.model_dump()
    [raw_model] = [m for m in raw["models"] if m["name"] == model.name]
    raw_model["metrics"] += [
        count("rank_n", f"{metric.sql} IS NOT NULL"),
        count("rank_below", f"{metric.sql} < {value}"),
        count("rank_at_or_below", f"{metric.sql} <= {value}"),
    ]
    counting = query.model_copy(update={"metrics": ["rank_n", "rank_below", "rank_at_or_below"]})
    sql = SqlCompiler(Ontology.model_validate(raw)).compile(counting, engine="duckdb", table="telemetry")
    [counts] = duckdb.execute(sql).to_pylist()
    return counts["rank_n"], counts["rank_below"], counts["rank_at_or_below"]
