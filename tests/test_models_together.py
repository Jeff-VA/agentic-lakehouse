"""Metrics from several models over the same dataset, queried together: each model is a lens on the
same events, so their metrics and dimensions combine in one query, live and across the tiers."""

from collections import Counter
from datetime import timedelta

import pytest
import yaml

from agentic_lakehouse.compiler import QueryError, SqlCompiler
from agentic_lakehouse.ontology import Ontology
from agentic_lakehouse.views import ViewError, parse_view
from test_compiler import ONTOLOGY
from test_router import COMPILER, EVERYTHING, NOW, ROWS, T0, query, router
from test_views import TABLES

# A second model over the playback events: how fast they were. It shares `isp` with playback, adds
# `speed`, and defines `is_mobile` differently (iOS only), so the two can't both be meant at once.
_spec = ONTOLOGY.model_dump(exclude_defaults=True)
_spec["models"].append(
    {
        "name": "quality",
        "description": "How fast playback was, on the same events.",
        "source": "playback",
        "time_dimension": "ts",
        "dimensions": [
            {"name": "ts", "type": "timestamp"},
            {"name": "isp", "type": "string"},
            {"name": "speed", "type": "string", "sql": "CASE WHEN latency_ms < 100 THEN 'fast' ELSE 'slow' END"},
            {"name": "is_mobile", "type": "boolean", "sql": "device = 'ios'"},
        ],
        "metrics": [
            {"name": "timed_plays", "description": "Plays with a latency.", "agg": "count", "sql": "latency_ms"},
            {"name": "fast_plays", "description": "Plays under 100 ms.", "agg": "count", "filters": ["latency_ms < 100"]},
            {"name": "fast_share", "type": "ratio", "description": "d", "numerator": "fast_plays", "denominator": "timed_plays"},
            {"name": "p95_latency_ms", "description": "d", "agg": "percentile", "percentile": 0.95, "sql": "latency_ms"},
        ],
    }
)
TWO = Ontology.model_validate(_spec)
TOGETHER = SqlCompiler(TWO)
PLAYBACK = next(m for m in TWO.models if m.name == "playback")


def run(q) -> list[dict]:
    """`q` on DuckDB over every event, in one query."""
    return EVERYTHING.execute(TOGETHER.compile(q, engine="duckdb", table="playback")).to_pylist()


def test_metrics_of_two_models_over_one_dataset_are_queried_together():
    together = run(query(0, 6, metrics=["plays", "avg_latency_ms", "fast_share", "p95_latency_ms"], dimensions=["isp"]))
    playback = run(query(0, 6, metrics=["plays", "avg_latency_ms"], dimensions=["isp"]))
    quality = run(query(0, 6, metrics=["fast_share", "p95_latency_ms"], dimensions=["isp"]))
    assert together == [p | q for p, q in zip(playback, quality, strict=True)]


def test_a_dimension_of_one_model_slices_the_metrics_of_another():
    rows = run(query(0, 6, metrics=["plays", "viewers"], dimensions=["speed"]))
    speed = lambda latency: "fast" if latency < 100 else "slow"  # noqa: E731
    plays = Counter(speed(r[4]) for r in ROWS)
    viewers = {s: len({r[3] for r in ROWS if speed(r[4]) == s}) for s in plays}
    assert rows == [{"speed": s, "plays": plays[s], "viewers": viewers[s]} for s in sorted(plays)]


def test_a_query_of_one_model_reads_it_as_before():
    """Additive: a model's own definition of a dimension is used, though another model over the
    dataset defines it differently, and its SQL is what it was without the other model."""
    q = query(0, 6, metrics=["plays", "slow_rate"], dimensions=["is_mobile"])
    assert TOGETHER.model_for(q) is PLAYBACK
    assert TOGETHER.compile(q, engine="duckdb", table="playback") == COMPILER.compile(q, engine="duckdb", table="playback")


@pytest.mark.parametrize(
    "fields",
    [{"dimensions": ["is_mobile"]}, {"filters": [{"dimension": "is_mobile", "op": "=", "value": True}]}],
)
def test_a_dimension_the_models_define_differently_is_refused(fields):
    with pytest.raises(QueryError, match=r"'is_mobile' means different things in models playback and quality"):
        TOGETHER.compile(query(0, 6, metrics=["plays", "fast_share"], **fields), engine="duckdb", table="playback")


def test_models_over_different_datasets_still_cant_be_queried_together():
    with pytest.raises(QueryError, match=r"different models \(billing, playback\)"):
        TOGETHER.model_for(query(0, 6, metrics=["plays", "revenue"]))


@pytest.mark.parametrize("fields", [{}, {"dimensions": ["isp"]}, {"dimensions": ["speed"], "grain": "hour"}])
def test_metrics_of_two_models_are_routed_across_the_tiers_together(fields):
    q = query(0, 6, metrics=["plays", "viewers", "fast_share", "avg_latency_ms"], **fields)
    result = router(compiler=TOGETHER).run(q)
    assert "historical" in result.sql and "realtime" in result.sql and not result.notes
    expected = run(q.model_copy(update={"start": result.start, "end": result.end}))
    assert result.table.to_pylist() == [{k: pytest.approx(v) if isinstance(v, float) else v for k, v in r.items()} for r in expected]


def view(sql: str):
    header = yaml.safe_dump({"description": "A test view.", "params": {"hours": {"type": "int", "default": 6}}})
    return parse_view("test", f"/*\n{header}*/\n{sql}", TWO, TABLES)


@pytest.mark.parametrize("model", ["playback", "quality"])
def test_a_view_over_one_model_selects_the_metrics_of_another_over_the_dataset(model):
    v = view(f"SELECT isp, speed, plays, fast_share FROM {model} WHERE ts >= now() - INTERVAL :hours HOUR")
    q = v.source.query({"hours": 6}, NOW)
    assert (q.metrics, q.dimensions) == (["plays", "fast_share"], ["isp", "speed"])
    assert [(c.name, c.role) for c in v.columns] == [("isp", "dimension"), ("speed", "dimension"), ("plays", "metric"), ("fast_share", "metric")]
    assert len(run(q)) == 6  # every isp, fast and slow


def test_a_view_keeps_its_own_models_dimensions():
    q = view("SELECT is_mobile, plays FROM playback WHERE ts >= now() - INTERVAL :hours HOUR").source.query({"hours": 6}, NOW)
    assert TOGETHER.model_for(q) is PLAYBACK  # playback's is_mobile, as before


def test_a_view_mixing_models_refuses_a_dimension_they_define_differently():
    with pytest.raises(ViewError, match=r"'is_mobile' means different things in models playback and quality"):
        view("SELECT is_mobile, plays, fast_share FROM playback WHERE ts >= now() - INTERVAL :hours HOUR")


def test_rollups_can_keep_metrics_of_two_models_over_one_dataset():
    from agentic_lakehouse.config import Rollup

    rollup = Rollup.model_validate(
        {
            "table": "gold.playback_quality",
            "description": "d",
            "query": {"metrics": ["plays", "fast_share"], "dimensions": ["isp", "speed"], "grain": "hour"},
            "lookback_hours": 3,
            "backfill_days": 1,
            "every_minutes": 10,
        }
    )
    window = rollup.with_window(T0, T0 + timedelta(hours=6))
    assert TOGETHER.model_for(window).source == "playback"
    rows = run(window)
    assert sum(r["plays"] for r in rows) == len(ROWS) and all(r["fast_share"] in (0, 1) for r in rows)  # one speed a row
