"""Synthetic events for any dataset: to try a config before real producers
exist, and to keep a demo live.

Events come in episodes: a function (rng, start) -> list of events, such as
one viewer's playback session. A dataset in ingestion.yaml can name its own
(`simulator: package.module:function`); otherwise each episode is a single
event with random values for every column. Output is fully determined by the
window and seed, so a batch can be regenerated exactly.
"""

import heapq
import importlib
import itertools
import random
from collections.abc import Callable
from datetime import datetime, timedelta

from core.config import ColumnType, Dataset

Episode = Callable[[random.Random, datetime], list[dict]]


def episode_for(dataset: Dataset) -> Episode:
    """The dataset's simulator from ingestion.yaml, or random values from its column types."""
    if dataset.simulator is None:
        return random_episode(dataset)
    module, _, name = dataset.simulator.partition(":")
    return getattr(importlib.import_module(module), name)


def random_episode(dataset: Dataset) -> Episode:
    def episode(rng: random.Random, start: datetime) -> list[dict]:
        return [{c.name: start if c.name == dataset.time_column else random_value(rng, c.name, c.type, start) for c in dataset.columns}]

    return episode


def random_value(rng: random.Random, name: str, column_type: ColumnType, now: datetime):
    """A plausible value of `column_type`. Text comes from five values per column, so it groups well."""
    match column_type:
        case "string":
            return f"{name}_{rng.randint(1, 5)}"
        case "int" | "long":
            return rng.randint(0, 1000)
        case "double":
            return round(rng.uniform(0, 1000), 3)
        case "boolean":
            return rng.random() < 0.5
        case "timestamp":
            return now
    raise ValueError(f"no random values for column type {column_type!r}")


def generate_events(
    dataset: Dataset, start: datetime, end: datetime, *, rate: float = 6, seed: int = 0, episode: Episode | None = None
) -> list[dict]:
    """Events in [start, end) with `rate` new episodes a minute, sorted by time.

    `start` and `end` are naive UTC datetimes. Episodes start uniformly across
    the window; events they'd send after `end` are left out.
    """
    episode = episode or episode_for(dataset)
    time = dataset.time_column
    rng = random.Random(seed)
    span_ms = int((end - start) / timedelta(milliseconds=1))
    events = [
        event
        for _ in range(round(span_ms / 60_000 * rate))
        for event in episode(rng, start + timedelta(milliseconds=rng.randrange(span_ms)))
        if event[time] < end
    ]
    events.sort(key=lambda e: e[time])
    return events


def add_fields(events: list[dict], fields: dict[str, ColumnType], rng: random.Random) -> None:
    """Give every event extra fields with random values: simulates an upstream release, so
    the flush's schema evolution and the drift agent can be seen at work."""
    for event in events:
        for name, column_type in fields.items():
            event[name] = random_value(rng, name, column_type, datetime.now())


class LiveSource:
    """A dataset's events in real time, for `run.py stream`.

    poll(now) returns the events that happened since the previous poll. New
    episodes start at `rate` a minute on average, and each one keeps sending
    events across later polls until it ends, like real viewers.
    """

    def __init__(
        self, dataset: Dataset, start: datetime, *, rate: float = 6, seed: int | None = None, episode: Episode | None = None
    ):
        self.episode = episode or episode_for(dataset)
        self.time = dataset.time_column
        self.rng = random.Random(seed)
        self.rate = rate
        self.last = start
        self._scheduled: list[tuple[datetime, int, dict]] = []  # heap of future events
        self._order = itertools.count()  # tie-breaker, so the heap never compares dicts

    def poll(self, now: datetime) -> list[dict]:
        """Events with times in [previous poll, now), sorted."""
        if now <= self.last:
            return []
        span_ms = int((now - self.last) / timedelta(milliseconds=1))
        expected = span_ms / 60_000 * self.rate
        starting = int(expected) + (self.rng.random() < expected % 1)  # rounds without bias over many polls
        for _ in range(starting):
            for event in self.episode(self.rng, self.last + timedelta(milliseconds=self.rng.randrange(span_ms))):
                heapq.heappush(self._scheduled, (event[self.time], next(self._order), event))
        due = []
        while self._scheduled and self._scheduled[0][0] < now:
            due.append(heapq.heappop(self._scheduled)[2])
        self.last = now
        return due
