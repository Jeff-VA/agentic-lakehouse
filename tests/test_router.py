from datetime import UTC, datetime, timedelta

import duckdb
import pytest

import pyarrow as pa

from agentic_lakehouse.compiler import MetricQuery, QueryError, SqlCompiler
from agentic_lakehouse.engines import DuckDBEngine
from agentic_lakehouse.ontology import Ontology
from agentic_lakehouse.router import Placement, Router, Tier
from test_compiler import ONTOLOGY

# Six hours of playback events, every 5 minutes. DuckDB stands in for both
# tiers: "historical" holds what was flushed (before 04:00), "realtime" the last
# 4 hours (from 02:00). Routed answers must match one query over all events.
T0 = datetime(2026, 1, 1)
NOW, RETENTION, FLUSHED = T0 + timedelta(hours=6), timedelta(hours=4), T0 + timedelta(hours=4)
REALTIME_FROM = NOW - RETENTION
ROWS = [
    (T0 + timedelta(minutes=5 * i), ["comcast", "verizon", "att"][i % 3], ["ios", "web"][i % 2], f"u{i % 7}", (i * 37) % 400 + 10)
    for i in range(72)
]
COMPILER = SqlCompiler(ONTOLOGY)


def duckdb_with(rows) -> DuckDBEngine:
    con = duckdb.connect()
    con.execute("CREATE TABLE playback (ts TIMESTAMP, isp VARCHAR, device VARCHAR, user_id VARCHAR, latency_ms INTEGER)")
    if rows:
        con.executemany("INSERT INTO playback VALUES (?, ?, ?, ?, ?)", rows)
    return DuckDBEngine(con)


EVERYTHING = duckdb_with(ROWS)


def router(flushed=FLUSHED, rows=ROWS, compiler=COMPILER, **options) -> Router:
    placement = Placement(
        realtime=Tier(duckdb_with([r for r in rows if r[0] >= REALTIME_FROM]), "playback"),
        historical=Tier(duckdb_with([r for r in rows if r[0] < FLUSHED]), "playback"),
        retention=RETENTION,
        flushed_through=lambda: flushed,
    )
    return Router(compiler, {"playback": placement}, clock=lambda: NOW, **options)


def query(start_hour, end_hour, **fields) -> MetricQuery:
    return MetricQuery.model_validate(
        {"start": T0 + timedelta(hours=start_hour), "end": T0 + timedelta(hours=end_hour), **fields}
    )


def assert_matches_all_events(result, q, everything=EVERYTHING):
    """The routed result equals a single query over every event in the range the result covers."""
    covered = q.model_copy(update={"start": result.start, "end": result.end})
    expected = everything.execute(COMPILER.compile(covered, engine="duckdb", table="playback")).to_pylist()
    actual = result.table.to_pylist()
    assert len(actual) == len(expected)
    for actual_row, expected_row in zip(actual, expected):
        assert actual_row.keys() == expected_row.keys()
        for column, value in expected_row.items():
            assert actual_row[column] == (pytest.approx(value) if isinstance(value, float) else value), column


MERGEABLE = ["plays", "slow_rate", "total_latency_ms", "avg_latency_ms", "min_latency_ms", "max_latency_ms"]


@pytest.mark.parametrize(
    ("start_hour", "end_hour", "tiers"),
    [(3, 6, {"realtime"}), (0, 3, {"historical"}), (2, 4, {"realtime"})],  # Pinot is preferred when it holds the range
)
def test_one_tier_answers_when_it_holds_the_whole_range(start_hour, end_hour, tiers):
    q = query(start_hour, end_hour, metrics=MERGEABLE + ["median_latency_ms"], dimensions=["isp"])
    result = router().run(q)
    assert result.sql.keys() == tiers
    assert (result.start, result.end) == (q.start, q.end)
    assert_matches_all_events(result, q)


@pytest.mark.parametrize(
    "fields",
    [
        {"dimensions": ["isp"]},
        {"dimensions": ["isp", "is_mobile"], "order_by": [{"field": "total_latency_ms", "descending": True}], "limit": 2},
        {"grain": "day"},  # one bucket straddles the split; its partials are merged
        {"filters": [{"dimension": "isp", "op": "=", "value": "nobody"}]},  # count 0, average NULL
    ],
)
def test_mergeable_metrics_are_combined_across_tiers(fields):
    q = query(0, 6, metrics=MERGEABLE, **fields)
    result = router().run(q)
    assert result.sql.keys() == {"historical", "realtime", "merge"}
    assert "_sum__avg_latency_ms" in result.sql["realtime"]
    assert not result.notes
    assert_matches_all_events(result, q)


def test_grained_query_splits_on_a_bucket_boundary_so_every_metric_is_exact():
    q = query(0, 6, metrics=["median_latency_ms", "viewers", "plays"], grain="hour", order_by=[{"field": "plays", "descending": True}, {"field": "ts"}], limit=4)
    result = router().run(q)
    assert result.sql.keys() == {"historical", "realtime", "merge"}
    assert "TIMESTAMP '2026-01-01 04:00:00'" in result.sql["realtime"]
    assert_matches_all_events(result, q)


@pytest.mark.parametrize("fields", [{}, {"grain": "month"}])  # month: no bucket boundary inside Pinot's retention
def test_unmergeable_metrics_fall_back_to_history_up_to_the_flush(fields):
    q = query(0, 6, metrics=["plays", "median_latency_ms", "viewers"], dimensions=["isp"], **fields)
    result = router().run(q)
    assert result.sql.keys() == {"historical"}
    assert (result.start, result.end) == (q.start, FLUSHED.replace(tzinfo=UTC))
    assert "median_latency_ms, viewers can't be combined" in result.notes[0]
    assert_matches_all_events(result, q)


@pytest.mark.parametrize(
    "fields",
    [
        {},
        {"dimensions": ["isp"]},
        {  # every group has all 7 viewers, so the order needs its tie-breaks
            "dimensions": ["isp", "is_mobile"],
            "order_by": [{"field": "viewers", "descending": True}, {"field": "isp"}, {"field": "is_mobile"}],
            "limit": 2,
        },
        {"filters": [{"dimension": "isp", "op": "=", "value": "comcast"}]},
        {"filters": [{"dimension": "isp", "op": "=", "value": "nobody"}]},  # no viewers on either side
    ],
)
def test_distinct_counts_are_combined_across_tiers_exactly(fields):
    """A viewer seen both before and after the flush counts once. The real-time tier holds only
    the minutes since the flush, so it lists its few viewers; the historical tier counts how many
    of them it has seen already, and the merge subtracts that overlap."""
    q = query(0, 6, metrics=["plays", "viewers", "avg_latency_ms"], **fields)
    result = router().run(q)
    assert result.sql.keys() == {"historical", "realtime", "realtime_keys", "merge"}
    assert not result.notes
    assert_matches_all_events(result, q)


def test_values_with_quotes_and_null_dimensions_are_matched_exactly():
    rows = [
        (T0 + timedelta(minutes=20 * i), None if i % 2 else "comcast", "ios", ["o'brien", 'say "hi"', "u3"][i % 3], 100)
        for i in range(18)
    ]  # every viewer, and the NULL isp, on both sides of the flush
    q = query(0, 6, metrics=["viewers"], dimensions=["isp"])
    result = router(rows=rows).run(q)
    assert result.sql.keys() == {"historical", "realtime", "realtime_keys", "merge"}
    assert_matches_all_events(result, q, everything=duckdb_with(rows))


def test_a_filtered_distinct_count_and_a_ratio_of_one_are_combined_exactly():
    """Only values where the metric's filters hold are listed and checked, so a viewer with slow and
    fast plays on both sides of the flush counts once among slow viewers."""
    spec = ONTOLOGY.model_dump(exclude_defaults=True)
    spec["models"][0]["metrics"] += [
        {"name": "slow_viewers", "description": "d", "agg": "count_distinct", "sql": "user_id", "filters": ["latency_ms > 100"]},
        {"name": "slow_viewer_share", "type": "ratio", "description": "d", "numerator": "slow_viewers", "denominator": "viewers"},
    ]
    compiler = SqlCompiler(Ontology.model_validate(spec))
    q = query(0, 6, metrics=["slow_viewers", "slow_viewer_share", "plays"], dimensions=["isp"])
    result = router(compiler=compiler).run(q)
    assert result.sql.keys() == {"historical", "realtime", "realtime_keys", "merge"}
    covered = q.model_copy(update={"start": result.start, "end": result.end})
    expected = EVERYTHING.execute(compiler.compile(covered, engine="duckdb", table="playback")).to_pylist()
    assert result.table.to_pylist() == [{k: pytest.approx(v) if isinstance(v, float) else v for k, v in row.items()} for row in expected]


def test_more_new_values_than_the_router_checks_fall_back_to_history_up_to_the_flush():
    q = query(0, 6, metrics=["plays", "viewers"], dimensions=["isp"])
    result = router(max_realtime_values=2).run(q)
    assert result.sql.keys() == {"historical"}
    assert (result.start, result.end) == (q.start, FLUSHED.replace(tzinfo=UTC))
    assert "viewers: the real-time tier saw more than 2" in result.notes[0]
    assert_matches_all_events(result, q)


def test_grained_distinct_counts_with_no_bucket_boundary_in_retention_still_fall_back():
    q = query(0, 6, metrics=["plays", "viewers"], grain="month")
    result = router().run(q)
    assert result.sql.keys() == {"historical"}
    assert "viewers can't be combined" in result.notes[0]
    assert_matches_all_events(result, q)


@pytest.mark.parametrize("flushed", [None, T0 + timedelta(hours=1)])
def test_gap_between_tiers_is_reported(flushed):
    q = query(0, 6, metrics=["plays"])
    result = router(flushed=flushed).run(q)
    assert result.sql.keys() == {"realtime"}
    assert result.start == REALTIME_FROM.replace(tzinfo=UTC)
    assert "no tier holds events" in result.notes[0]
    assert_matches_all_events(result, q)


def test_merged_counts_stay_integers():
    result = router().run(query(0, 6, metrics=["plays", "total_latency_ms"], dimensions=["isp"]))
    assert result.sql.keys() == {"historical", "realtime", "merge"}
    assert pa.types.is_integer(result.table.schema.field("plays").type)  # not a HUGEINT decimal
    assert all(isinstance(row["plays"], int) for row in result.table.to_pylist())


def test_a_range_no_tier_holds_is_a_query_error():
    # Before the first flush nothing covers ranges older than the real-time tier.
    with pytest.raises(QueryError, match="no tier holds events from 2026-01-01 00:00 to 2026-01-01 01:00 UTC"):
        router(flushed=None).run(query(0, 1, metrics=["plays"]))


def test_a_gap_is_answered_by_the_tier_holding_more_of_the_range():
    flushed = REALTIME_FROM - timedelta(minutes=5)  # the flush is 5 minutes behind retention
    q = MetricQuery.model_validate({"metrics": ["plays"], "start": T0, "end": T0 + timedelta(hours=2, minutes=5)})
    result = router(flushed=flushed).run(q)
    assert result.sql.keys() == {"historical"}
    assert (result.start, result.end) == (q.start, flushed.replace(tzinfo=UTC))
    assert_matches_all_events(result, q)
