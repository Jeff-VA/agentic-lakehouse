"""Reusing the historical tier's results (docs/design/split-query-cache.md): kept by their SQL and
the Iceberg snapshot they read, and in a split, cut where a rolling window's moving start stops
mattering. Every answer must still match one query over all events."""

from datetime import datetime, timedelta

import pyarrow as pa
import pytest
from test_router import COMPILER, FLUSHED, MERGEABLE, NOW, REALTIME_FROM, RETENTION, ROWS, T0, assert_matches_all_events, duckdb_with

from agentic_lakehouse.compiler import MetricQuery
from agentic_lakehouse.engines import DuckDBEngine
from agentic_lakehouse.router import Placement, ResultCache, Router, Tier, boundary_after


class Counting(DuckDBEngine):
    """DuckDB standing in for Trino, counting the queries it runs."""

    def __init__(self, rows):
        super().__init__(duckdb_with(rows).con)
        self.ran: list[str] = []

    def execute(self, sql: str) -> pa.Table:
        self.ran.append(sql)
        return super().execute(sql)


class Stack:
    """The two tiers over the router test's events, with the historical table's snapshot to change."""

    def __init__(self, snapshot: int | None = 1):
        self.trino = Counting([r for r in ROWS if r[0] < FLUSHED])
        self.snapshot = snapshot
        placement = Placement(
            realtime=Tier(duckdb_with([r for r in ROWS if r[0] >= REALTIME_FROM]), "playback"),
            historical=Tier(self.trino, "playback"),
            retention=RETENTION,
            flushed_through=lambda: FLUSHED,
            snapshot=lambda: self.snapshot,
        )
        self.router = Router(COMPILER, {"playback": placement}, clock=lambda: NOW)


def query(start: datetime, end: datetime, **fields) -> MetricQuery:
    return MetricQuery.model_validate({"start": start, "end": end, **fields})


SPLITS = [{"grain": "hour", "metrics": ["plays", "median_latency_ms"]}, {"dimensions": ["isp"], "metrics": MERGEABLE}]
SPLIT_IDS = ["grained split", "merged partials"]


@pytest.mark.parametrize("fields", SPLITS, ids=SPLIT_IDS)
def test_a_repeated_split_runs_its_historical_half_once(fields):
    stack = Stack()
    q = query(T0, NOW, **fields)
    first, second = stack.router.run(q), stack.router.run(q)
    assert len(stack.trino.ran) == 1
    assert second.table.to_pylist() == first.table.to_pylist()
    assert second.sql["historical"].startswith("-- reused: Iceberg snapshot 1 hasn't changed since this ran\n")
    assert_matches_all_events(second, q)


@pytest.mark.parametrize("fields", SPLITS, ids=SPLIT_IDS)
def test_as_a_rolling_window_moves_only_its_edge_runs_again(fields):
    """Live refreshes 10 seconds apart: the window starts at 00:07:00, then 00:07:10, ..."""
    stack = Stack()
    for refresh in range(6):
        start = T0 + timedelta(minutes=7, seconds=10 * refresh)
        q = query(start, NOW + timedelta(seconds=10 * refresh), **fields)
        result = stack.router.run(q)
        assert_matches_all_events(result, q)  # still exact, edge and body together
    assert len(stack.trino.ran) == 6 + 1  # each refresh's edge, and the body once
    assert result.sql["historical"].startswith("-- runs each time, up to 2026-01-01 01:00 UTC\n")
    assert "-- reused: Iceberg snapshot 1" in result.sql["historical"]


def test_a_commit_runs_the_body_again_and_its_late_events_count():
    stack = Stack()
    q = query(T0, NOW, grain="hour", metrics=["plays"])

    def plays_at(hour: int) -> int:
        result = stack.router.run(q)
        assert result.table.num_rows == 6
        return next(row["plays"] for row in result.table.to_pylist() if row["ts"].replace(tzinfo=None) == T0 + timedelta(hours=hour))

    assert plays_at(2) == 12
    stack.trino.con.execute("INSERT INTO playback VALUES (?, 'comcast', 'ios', 'u1', 50)", [T0 + timedelta(hours=2, minutes=1)])
    assert plays_at(2) == 12  # the flush wrote a late event, but nothing says so until it commits
    stack.snapshot = 2  # the commit
    assert plays_at(2) == 13
    assert len(stack.trino.ran) == 2


def test_a_range_history_holds_alone_is_reused_when_it_repeats():
    stack = Stack()
    yesterday = query(T0, T0 + timedelta(hours=3), metrics=["plays", "viewers"], dimensions=["isp"])
    assert stack.router.run(yesterday).sql.keys() == {"historical"}
    stack.router.run(yesterday)
    assert len(stack.trino.ran) == 1


def test_a_rolling_window_of_metrics_that_cant_merge_isnt_reused():
    """A distinct count can't be put together from an edge and a body, so the whole historical
    range runs, and with a moving start it's never the same query twice."""
    stack = Stack()
    for refresh in range(3):
        q = query(T0 + timedelta(minutes=7, seconds=10 * refresh), NOW, metrics=["viewers"])
        assert stack.router.run(q).sql.keys() == {"historical"}
    assert len(stack.trino.ran) == 3


def test_without_a_snapshot_nothing_is_cut_or_reused():
    stack = Stack(snapshot=None)
    q = query(T0 + timedelta(minutes=7), NOW, grain="hour", metrics=["plays"])
    for _ in range(2):
        result = stack.router.run(q)
        assert "--" not in result.sql["historical"]
        assert_matches_all_events(result, q)
    assert len(stack.trino.ran) == 2


def test_the_cache_keeps_within_its_bytes():
    rows = pa.table({"x": list(range(1000))})  # 8,000 bytes
    cache = ResultCache(max_bytes=20_000)
    cache.put("a", rows)
    cache.put("b", rows)
    assert cache.get("a") is rows  # "a" is now the most recently used
    cache.put("c", rows)
    assert (cache.get("a"), cache.get("b"), cache.get("c")) == (rows, None, rows)
    cache.put("big", pa.table({"x": list(range(5000))}))  # 40,000 bytes: more than the whole cache
    assert cache.get("big") is None and cache.get("a") is rows


@pytest.mark.parametrize(
    ("ts", "grain", "expected"),
    [
        (datetime(2026, 1, 1, 0, 7, 30), "hour", datetime(2026, 1, 1, 1)),
        (datetime(2026, 1, 1, 1), "hour", datetime(2026, 1, 1, 1)),  # a boundary already
        (datetime(2026, 1, 1, 0, 7, 30), "minute", datetime(2026, 1, 1, 0, 8)),
        (datetime(2026, 1, 1, 13), "day", datetime(2026, 1, 2)),
        (datetime(2026, 1, 1, 13), "week", datetime(2026, 1, 5)),  # weeks start on Monday
        (datetime(2026, 1, 31, 13), "month", datetime(2026, 2, 1)),
        (datetime(2026, 12, 3), "month", datetime(2027, 1, 1)),
    ],
)
def test_boundary_after(ts, grain, expected):
    assert boundary_after(ts, grain) == expected
