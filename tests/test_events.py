from collections import defaultdict
from datetime import datetime
from pathlib import Path
from statistics import mean

import pyarrow as pa

from core.config import load_platform
from core.events import ISPS, generate_events
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
