"""The playback example project: its simulator tells the story its dashboard shows."""

from collections import defaultdict
from datetime import datetime
from pathlib import Path
from statistics import mean

import pyarrow as pa

from agentic_lakehouse.config import load_platform
from agentic_lakehouse.schema_compiler import arrow_schema
from agentic_lakehouse.simulate import generate_events
from examples.playback.simulator import ISPS

DATASET = load_platform(Path(__file__).parents[1] / "examples" / "playback" / "config").dataset("telemetry")
EVENTS = generate_events(DATASET, datetime(2026, 1, 1), datetime(2026, 1, 1, 2))


def test_sessions_fit_the_dataset_schema():
    table = pa.Table.from_pylist(EVENTS, schema=arrow_schema(DATASET))  # raises if a value doesn't fit
    assert table.column_names == [c.name for c in DATASET.columns]


def test_each_session_opens_with_one_stream_start():
    sessions = defaultdict(list)
    for event in EVENTS:
        sessions[event["session_id"]].append(event["stream_start_event"])
    assert all(flags == [True] + [False] * (len(flags) - 1) for flags in sessions.values())


def test_cox_rebuffers_most():
    def rebuffer_rate(isp):
        return mean(e["buffer_duration_ms"] > 200 for e in EVENTS if e["client_isp"] == isp)

    assert max(ISPS, key=rebuffer_rate) == "cox"
