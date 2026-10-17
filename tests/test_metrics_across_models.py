"""Metrics across models (the ontology's top-level `metrics`): a ratio of two metrics of any models,
over different datasets even, which the router computes once it has both, drilling across."""

import copy

import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

from agentic_lakehouse.api_gateway import Gateway, create_app
from agentic_lakehouse.compiler import SqlCompiler
from agentic_lakehouse.ontology import Ontology
from agentic_lakehouse.views import ViewError
from test_drill_across import TICKETS, _spec as DRILL_ACROSS, assert_same, joined, router, sql_view
from test_router import NOW, ROWS, query

ACROSS_MODELS = [
    {
        "name": "tickets_per_100_plays",
        "description": "Support tickets for every 100 plays: how often playing leads to a complaint.",
        "type": "ratio",
        "numerator": "tickets",
        "denominator": "plays",
        "scale": 100,
        "label": "Tickets per 100 plays",
        "better": "lower",
    },
    {"name": "tickets_per_viewer", "description": "Tickets per distinct viewer.", "type": "ratio", "numerator": "tickets", "denominator": "viewers"},
]
_spec = copy.deepcopy(DRILL_ACROSS) | {"metrics": ACROSS_MODELS}
COMPILER = SqlCompiler(Ontology.model_validate(_spec))
PARTS = {m["name"]: (m["numerator"], m["denominator"], m.get("scale", 1)) for m in ACROSS_MODELS}


def expected(q) -> list[dict]:
    """Each metric across models from its parts' drill-across, numerator / denominator × scale
    (empty where the denominator is 0), then ordered and limited."""
    names = list(dict.fromkeys(n for m in q.metrics for n in (PARTS[m][:2] if m in PARTS else [m])))
    keys = (["ts"] if q.grain else []) + q.dimensions
    rows = []
    for row in joined(q.model_copy(update={"metrics": names, "order_by": [], "limit": None})):
        out = {k: row[k] for k in keys}
        for m in q.metrics:
            if m in PARTS:
                numerator, denominator, scale = PARTS[m]
                out[m] = scale * row[numerator] / row[denominator] if row[denominator] else None
            else:
                out[m] = row[m]
        rows.append(out)
    for key in reversed(q.order_by):
        rows.sort(key=lambda r: (r[key.field] is None, r[key.field]), reverse=key.descending)
    return rows[: q.limit] if q.limit else rows


def test_a_ratio_of_two_datasets_metrics_by_the_dimension_they_share():
    q = query(0, 6, metrics=["tickets_per_100_plays", "plays", "tickets"], dimensions=["isp"])
    result = router(compiler=COMPILER).run(q)
    assert not result.notes and "across models" in result.sql
    assert_same(result.table.to_pylist(), expected(q))
    by_isp = {r["isp"]: r["tickets_per_100_plays"] for r in result.table.to_pylist()}
    assert (by_isp["tmobile"], by_isp["verizon"]) == (None, 0)  # no plays to divide by; no tickets


@pytest.mark.parametrize(
    "fields",
    [
        {"grain": "hour", "order_by": [{"field": "tickets_per_viewer", "descending": True}, {"field": "ts"}], "limit": 3},
        {},  # one row: the totals' ratio
    ],
)
def test_metrics_across_models_order_limit_and_total(fields):
    q = query(0, 6, metrics=["tickets_per_viewer", "tickets_per_100_plays"], **fields)
    assert_same(router(compiler=COMPILER).run(q).table.to_pylist(), expected(q))


def test_the_total_is_the_ratio_of_totals():
    [row] = router(compiler=COMPILER).run(query(0, 6, metrics=["tickets_per_100_plays"])).table.to_pylist()
    assert row["tickets_per_100_plays"] == pytest.approx(100 * len(TICKETS) / len(ROWS))


@pytest.mark.parametrize(
    ("metric", "message"),
    [
        ({"name": "x", "description": "d", "type": "ratio", "numerator": "nope", "denominator": "plays"}, r"metric 'x' divides 'nope', which no model has"),
        ({"name": "plays", "description": "d", "type": "ratio", "numerator": "tickets", "denominator": "plays"}, r"metric 'plays' is defined in both"),
        ({"name": "x", "description": "d", "type": "ratio", "numerator": "tickets", "denominator": "plays", "scale": 0}, r"scale"),
    ],
)
def test_a_metric_across_models_is_checked_when_the_ontology_loads(metric, message):
    with pytest.raises(ValidationError, match=message):
        Ontology.model_validate(copy.deepcopy(DRILL_ACROSS) | {"metrics": [metric]})


def test_an_ontology_without_them_loads_as_before():
    assert Ontology.model_validate(copy.deepcopy(DRILL_ACROSS)).metrics == []


def test_a_view_from_metrics_selects_them_and_reads_their_parts_models():
    v = sql_view("SELECT isp, tickets_per_100_plays FROM metrics WHERE ts >= now() - INTERVAL :hours HOUR", COMPILER.ontology)
    assert v.reads == ["support", "playback"]
    [_, column] = v.columns
    assert (column.name, column.label, column.better) == ("tickets_per_100_plays", "Tickets per 100 plays", "lower")
    q = v.source.query({"hours": 6}, NOW)
    assert_same(router(compiler=COMPILER).run(q).table.to_pylist(), expected(q))


def test_a_view_from_a_model_doesnt_select_them():
    with pytest.raises(ViewError, match=r"'tickets_per_100_plays' isn't a metric or dimension of model 'playback'"):
        sql_view("SELECT tickets_per_100_plays FROM playback WHERE ts >= now() - INTERVAL :hours HOUR", COMPILER.ontology)


def test_the_catalog_lists_them_and_the_api_answers_them():
    with TestClient(create_app(Gateway(router(compiler=COMPILER)))) as client:
        catalog = client.get("/catalog").json()
        body = client.post(
            "/query", json={"metrics": ["tickets_per_100_plays"], "dimensions": ["isp"], "start": "2026-01-01T00:00:00", "end": "2026-01-01T06:00:00"}
        ).json()
    [first, second] = catalog["metrics_across_models"]
    assert (first["name"], first["label"], first["calculation"]) == (
        "tickets_per_100_plays", "Tickets per 100 plays", "ratio across models: 100 × tickets (support) / plays (playback)",
    )  # fmt: skip
    assert second["calculation"] == "ratio across models: tickets (support) / viewers (playback)"
    assert [(c["name"], c["role"]) for c in body["columns"]] == [("isp", "dimension"), ("tickets_per_100_plays", "metric")]
    assert len(body["rows"]) == 4
