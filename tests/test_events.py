from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from statistics import mean

import pyarrow as pa

from core.config import load_platform
from core.events import ISPS, LiveSource, generate_events
from core.schema_compiler import arrow_schema

START, END = datetime(2026, 1, 1), datetime(2026, 1, 1, 2)
EVENTS = generate_events(START, END)
DATASET = load_platform(Path(__file__).parents[1] / "config").dataset("telemetry")


def test_same_seed_same_events():
    assert generate_events(START, END, seed=1) == generate_events(START, END, seed=1)
    assert generate_events(START, END, seed=1) != generate_events(START, END, seed=2)


def test_events_fit_the_dataset_schema():
    table = pa.Table.from_pylist(EVENTS, schema=arrow_schema(DATASET))  # raises if a value doesn't fit
    assert table.column_names == [c.name for c in DATASET.columns]
    assert set(EVENTS[0]) == set(table.column_names)


def test_events_are_sorted_and_inside_the_window():
    times = [e["event_ts"] for e in EVENTS]
    assert times == sorted(times)
    assert START <= times[0] and times[-1] < END


def test_each_session_opens_with_one_stream_start():
    sessions = defaultdict(list)
    for event in EVENTS:
        sessions[event["session_id"]].append(event["stream_start_event"])
    assert all(flags == [True] + [False] * (len(flags) - 1) for flags in sessions.values())


def test_cox_rebuffers_most():
    def rebuffer_rate(isp):
        return mean(e["buffer_duration_ms"] > 200 for e in EVENTS if e["client_isp"] == isp)

    assert max(ISPS, key=rebuffer_rate) == "cox"


def test_live_source_streams_sessions_across_polls():
    source = LiveSource(START, sessions_per_minute=6, seed=1)
    ticks = [START + timedelta(seconds=5 * i) for i in range(1, 12 * 60 + 1)]  # an hour of polls, 5 s apart
    polls = [source.poll(tick) for tick in ticks]

    for previous, tick, batch in zip([START, *ticks], ticks, polls):
        times = [e["event_ts"] for e in batch]
        assert times == sorted(times) and all(previous <= t < tick for t in times)
    polls_per_session = defaultdict(set)
    first_event = {}
    for i, batch in enumerate(polls):
        for event in batch:
            polls_per_session[event["session_id"]].add(i)
            first_event.setdefault(event["session_id"], event["stream_start_event"])
    assert 300 < len(first_event) < 420  # ~6 new sessions a minute
    assert all(first_event.values())  # every session opens with its stream start
    assert mean(len(p) > 1 for p in polls_per_session.values()) > 0.5  # sessions outlive a poll


def test_live_source_returns_nothing_until_time_passes():
    source = LiveSource(START, seed=1)
    assert source.poll(START) == []
