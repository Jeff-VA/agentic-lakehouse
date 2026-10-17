"""Empty values sort last, ascending and descending, on every engine. Left to each engine, Pinot sorts
NULLs first when descending while Trino and DuckDB sort them last, so the same query ordered
differently by the tier that answered it: a top-N by a metric some groups lack led with those groups."""

import pytest

from agentic_lakehouse.compiler import MetricQuery
from test_drill_across import router as two_datasets
from test_metrics_across_models import COMPILER as ACROSS
from test_router import COMPILER, query, router

ORDERED = {"order_by": [{"field": "avg_latency_ms", "descending": True}, {"field": "isp"}], "limit": 2}


@pytest.mark.parametrize("engine", ["pinot", "trino", "duckdb"])
def test_every_engine_is_told_where_empty_values_go(engine):
    sql = COMPILER.compile(query(0, 6, metrics=["plays", "avg_latency_ms"], dimensions=["isp"], **ORDERED), engine=engine, table="t")
    order = sql[sql.index("ORDER BY") :]
    assert "DESC NULLS LAST" in order and order.count("NULLS LAST") == 2, order


def test_a_split_merges_with_empty_values_last():
    result = router().run(query(0, 6, metrics=["plays", "avg_latency_ms"], dimensions=["isp"], **ORDERED))
    assert result.sql["merge"].count("NULLS LAST") == 2


def test_a_drill_across_and_metrics_across_models_order_empty_values_last():
    q = query(0, 6, metrics=["tickets_per_100_plays", "plays"], dimensions=["isp"], order_by=[{"field": "tickets_per_100_plays", "descending": True}])
    result = two_datasets(compiler=ACROSS).run(q)
    assert "DESC NULLS LAST" in result.sql["across models"] and "NULLS LAST" in result.sql["join"]
    rows = result.table.to_pylist()
    assert rows[-1]["isp"] == "tmobile" and rows[-1]["tickets_per_100_plays"] is None  # no plays: last, not first


def test_ascending_puts_empty_values_last_too():
    q = MetricQuery.model_validate(
        {"metrics": ["tickets_per_100_plays"], "dimensions": ["isp"], "order_by": [{"field": "tickets_per_100_plays"}],
         "start": "2026-01-01T00:00:00", "end": "2026-01-01T06:00:00"}
    )  # fmt: skip
    assert two_datasets(compiler=ACROSS).run(q).table.to_pylist()[-1]["isp"] == "tmobile"
