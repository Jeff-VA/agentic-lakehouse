import random
from datetime import datetime, timedelta
from pathlib import Path
from statistics import mean

import pyarrow as pa

from core.config import load_platform
from core.schema_compiler import arrow_schema
from core.simulate import LiveSource, add_fields, episode_for, generate_events
from examples.playback.simulator import session

EXAMPLE = Path(__file__).parents[1] / "examples" / "playback" / "config"
TELEMETRY = load_platform(EXAMPLE).dataset("telemetry")  # names a simulator: playback sessions
PLAIN = TELEMETRY.model_copy(update={"simulator": None})  # the same schema, random values
START, END = datetime(2026, 1, 1), datetime(2026, 1, 1, 2)


def test_without_a_simulator_events_get_random_values_that_fit_the_schema():
    events = generate_events(PLAIN, START, END)
    pa.Table.from_pylist(events, schema=arrow_schema(PLAIN))  # raises if a value doesn't fit its column
    assert len(events) == 2 * 60 * 6  # one event an episode, 6 a minute
    assert {e["client_isp"] for e in events} <= {f"client_isp_{i}" for i in range(1, 6)}  # few values: groups well


def test_a_named_simulator_is_used():
    assert episode_for(TELEMETRY) is session


def test_same_seed_same_events():
    assert generate_events(TELEMETRY, START, END, seed=1) == generate_events(TELEMETRY, START, END, seed=1)
    assert generate_events(TELEMETRY, START, END, seed=1) != generate_events(TELEMETRY, START, END, seed=2)


def test_events_are_sorted_and_inside_the_window():
    times = [e["event_ts"] for e in generate_events(TELEMETRY, START, END)]
    assert times == sorted(times)
    assert START <= times[0] and times[-1] < END


def test_live_source_keeps_episodes_going_across_polls():
    source = LiveSource(TELEMETRY, START, rate=6, seed=1)
    ticks = [START + timedelta(seconds=5 * i) for i in range(1, 12 * 60 + 1)]  # an hour of polls, 5 s apart
    polls = [source.poll(tick) for tick in ticks]

    for previous, tick, batch in zip([START, *ticks], ticks, polls):
        times = [e["event_ts"] for e in batch]
        assert times == sorted(times) and all(previous <= t < tick for t in times)
    polls_per_session, first_event = {}, {}
    for i, batch in enumerate(polls):
        for event in batch:
            polls_per_session.setdefault(event["session_id"], set()).add(i)
            first_event.setdefault(event["session_id"], event["stream_start_event"])
    assert 300 < len(first_event) < 420  # ~6 new sessions a minute
    assert all(first_event.values())  # every session opens with its stream start
    assert mean(len(p) > 1 for p in polls_per_session.values()) > 0.5  # sessions outlive a poll


def test_live_source_returns_nothing_until_time_passes():
    assert LiveSource(TELEMETRY, START, seed=1).poll(START) == []


def test_added_fields_simulate_an_upstream_release():
    events = generate_events(PLAIN, START, END)
    add_fields(events, {"cdn_pop": "string", "dropped_frames": "double"}, random.Random(0))
    assert all(e["cdn_pop"].startswith("cdn_pop_") and isinstance(e["dropped_frames"], float) for e in events)
