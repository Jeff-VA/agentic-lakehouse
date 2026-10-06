import copy
from pathlib import Path

import pytest
from pydantic import ValidationError

from core.ontology import AggregateMetric, Ontology, RatioMetric, load_ontology

SAMPLE_CONFIG = Path(__file__).parents[1] / "config" / "ontology.yaml"

# Smallest valid ontology. Each test breaks one thing in a fresh copy of it.
MINIMAL = {
    "version": "1.0.0",
    "models": [
        {
            "name": "orders",
            "description": "One row per order.",
            "source": "orders",
            "time_dimension": "created_at",
            "dimensions": [
                {"name": "created_at", "type": "timestamp"},
                {"name": "country", "type": "string"},
            ],
            "metrics": [
                {"name": "order_count", "description": "Orders placed.", "agg": "count"},
                {"name": "revenue", "description": "Gross revenue.", "agg": "sum", "sql": "amount"},
            ],
        }
    ],
}

RATIO = {
    "name": "avg_order_value",
    "type": "ratio",
    "description": "Revenue per order.",
    "numerator": "revenue",
    "denominator": "order_count",
}


@pytest.fixture
def raw():
    return copy.deepcopy(MINIMAL)


def assert_rejected(raw, match):
    with pytest.raises(ValidationError, match=match):
        Ontology.model_validate(raw)


def test_sample_config_loads():
    ontology = load_ontology(SAMPLE_CONFIG)
    [model] = ontology.models
    dimensions = {d.name: d for d in model.dimensions}
    metrics = {m.name: m for m in model.metrics}

    assert model.time_dimension == "event_ts"
    assert dimensions["event_ts"].expr == "event_ts"  # sql defaults to the dimension name
    assert dimensions["isp_provider"].expr == "client_isp"
    assert isinstance(metrics["p99_ttfb"], AggregateMetric)  # type defaults to aggregate
    assert metrics["p99_ttfb"].percentile == 0.99
    assert isinstance(metrics["rebuffer_incidence_rate"], RatioMetric)


def test_minimal_ontology_is_valid(raw):
    Ontology.model_validate(raw)


def test_unknown_key_is_rejected(raw):
    raw["models"][0]["metrics"][0]["descripton"] = "typo of description"
    assert_rejected(raw, r"descripton\s+Extra inputs are not permitted")


def test_metric_requires_description(raw):
    del raw["models"][0]["metrics"][0]["description"]
    assert_rejected(raw, r"description\s+Field required")


@pytest.mark.parametrize("bad_name", ["Order Count", "1st_orders", "x; DROP TABLE orders"])
def test_names_must_be_snake_case(raw, bad_name):
    raw["models"][0]["metrics"][0]["name"] = bad_name
    assert_rejected(raw, "should match pattern")


def test_duplicate_metric_names_are_rejected(raw):
    raw["models"][0]["metrics"].append({"name": "revenue", "description": "Again.", "agg": "count"})
    assert_rejected(raw, "duplicate metric name 'revenue'")


def test_dimension_and_metric_cannot_share_a_name(raw):
    raw["models"][0]["dimensions"].append({"name": "revenue", "type": "number"})
    assert_rejected(raw, "used for both a dimension and a metric: revenue")


@pytest.mark.parametrize(
    ("time_dimension", "match"),
    [("updated_at", "is not one of its dimensions"), ("country", "must have type timestamp")],
)
def test_time_dimension_must_be_a_timestamp_dimension(raw, time_dimension, match):
    raw["models"][0]["time_dimension"] = time_dimension
    assert_rejected(raw, match)


def test_aggregations_other_than_count_need_sql(raw):
    del raw["models"][0]["metrics"][1]["sql"]  # revenue: sum of nothing
    assert_rejected(raw, "agg 'sum' needs a `sql` expression")


@pytest.mark.parametrize(
    "metric",
    [
        {"name": "p95_amount", "description": "d", "agg": "percentile", "sql": "amount"},
        {"name": "avg_amount", "description": "d", "agg": "avg", "sql": "amount", "percentile": 0.5},
    ],
)
def test_percentile_goes_with_percentile_agg_only(raw, metric):
    raw["models"][0]["metrics"].append(metric)
    assert_rejected(raw, "is required with agg 'percentile'")


def test_percentile_is_a_fraction(raw):
    raw["models"][0]["metrics"].append(
        {"name": "p99_amount", "description": "d", "agg": "percentile", "sql": "amount", "percentile": 99}
    )
    assert_rejected(raw, "less than 1")


def test_ratio_metric_parses(raw):
    raw["models"][0]["metrics"].append(dict(RATIO))
    metric = Ontology.model_validate(raw).models[0].metrics[-1]
    assert isinstance(metric, RatioMetric)


@pytest.mark.parametrize("denominator", ["orders_total", "avg_order_value"])  # missing / itself a ratio
def test_ratio_parts_must_be_aggregate_metrics_in_the_model(raw, denominator):
    raw["models"][0]["metrics"].append({**RATIO, "denominator": denominator})
    assert_rejected(raw, f"uses '{denominator}', which is not an aggregate metric")


def test_unknown_metric_type_is_rejected(raw):
    raw["models"][0]["metrics"].append({**RATIO, "type": "ratoi"})
    assert_rejected(raw, "ratoi")


def test_metric_names_are_unique_across_models(raw):
    refunds = copy.deepcopy(raw["models"][0])
    refunds["name"] = "refunds"
    refunds["metrics"] = [{"name": "revenue", "description": "Refunded revenue.", "agg": "sum", "sql": "amount"}]
    raw["models"].append(refunds)
    assert_rejected(raw, "metric 'revenue' is defined in both 'orders' and 'refunds'")
