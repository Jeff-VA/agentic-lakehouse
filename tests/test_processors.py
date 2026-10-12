"""Processors (agentic_lakehouse/processors.py): a dataset made from another as it streams."""

import json
from collections import Counter
from datetime import datetime, timedelta
from types import SimpleNamespace

import pytest
from confluent_kafka import KafkaError, KafkaException

from agentic_lakehouse.config import Dataset
from agentic_lakehouse.processors import ProcessorContext, run_processor
from test_sources import EDITS, FakePublisher

COUNTS = Dataset.model_validate(
    {
        "name": "wiki_counts",
        "description": "Running edit counts per wiki.",
        "topic": "wiki-counts",
        "time_column": "event_ts",
        "columns": [{"name": "event_ts", "type": "timestamp"}, {"name": "wiki", "type": "string"}, {"name": "edits", "type": "long"}],
        "realtime": {"table": "wiki_counts", "retention_days": 7},
        "processor": {"input": "edits", "run": "test_processors:EditCounter", "warmup_minutes": 10},
    }
)
PLATFORM = SimpleNamespace(dataset=lambda name: {"edits": EDITS, "wiki_counts": COUNTS}[name])
NOW = 1_791_800_000.0  # seconds since 1970, the processor's clock


class EditCounter:
    """Counts edits per wiki. After each batch, emits the running count of every wiki in it."""

    instances: list["EditCounter"] = []

    def __init__(self, context: ProcessorContext):
        self.counts, self.ticks, self.times = Counter(), 0, []
        EditCounter.instances.append(self)

    def process(self, events):
        if not events:
            self.ticks += 1
            return []
        self.times += [e["event_ts"] for e in events]
        for event in events:
            self.counts[event["wiki"]] += 1
        last = events[-1]["event_ts"]
        if any(e["wiki"] == "badwiki" for e in events):
            return [{"event_ts": last, "wiki": "badwiki", "edits": "many"}]  # doesn't fit the output's columns
        return [{"event_ts": last, "wiki": wiki, "edits": self.counts[wiki]} for wiki in sorted({e["wiki"] for e in events})]


class FakeKafka:
    """The slice of confluent_kafka.Consumer that run_processor uses, over one in-memory partition.
    Each message is (seconds since 1970 when it was published, event)."""

    def __init__(self, topic: str, messages: list[tuple[float, dict]], committed: int = -1001):
        self.topic, self.messages, self.committed_offset = topic, messages, committed
        self.position, self.commits = 0, []

    def list_topics(self, topic, timeout=None):
        return SimpleNamespace(topics={topic: SimpleNamespace(partitions={0: None})})

    def committed(self, partitions, timeout=None):
        return [SimpleNamespace(partition=0, offset=self.committed_offset)]

    def offsets_for_times(self, partitions, timeout=None):
        [tp] = partitions
        offset = next((i for i, (at, _) in enumerate(self.messages) if at * 1000 >= tp.offset), -1)
        return [SimpleNamespace(partition=0, offset=offset)]

    def get_watermark_offsets(self, tp, timeout=None):
        return 0, len(self.messages)

    def assign(self, partitions):
        [tp] = partitions
        self.position = tp.offset

    def consume(self, num_messages, timeout):
        batch = self.messages[self.position : self.position + 2]  # small batches, to cross the resume point mid-batch
        start, self.position = self.position, self.position + len(batch)
        return [
            SimpleNamespace(partition=lambda: 0, offset=lambda o=start + i: o, value=lambda e=event: json.dumps(e).encode(), error=lambda: None)
            for i, (_, event) in enumerate(batch)
        ]

    def commit(self, offsets, asynchronous=True):
        self.commits += [tp.offset for tp in offsets]


def edit(minutes_ago: float, wiki: str) -> tuple[float, dict]:
    published = NOW - minutes_ago * 60
    return published, {"event_ts": int(published * 1000), "wiki": wiki, "bytes": 1}


def run(kafka: FakeKafka, loops: int, sleep=lambda seconds: None):
    publisher, calls = FakePublisher(), iter(range(loops))
    context = ProcessorContext(COUNTS, EDITS, query=lambda sql: None, log=lambda m: None)
    counts = run_processor(
        PLATFORM, COUNTS, consumer=kafka, publisher=publisher, context=context, log=lambda m: None,
        stop=lambda: next(calls, None) is None, commit_every=0, clock=lambda: NOW, sleep=sleep,
    )
    return [event for event, _ in publisher.messages], counts


def test_a_restart_replays_the_warmup_without_publishing_twice():
    EditCounter.instances.clear()
    messages = [edit(30, "enwiki"), edit(9, "enwiki"), edit(8, "dewiki"), edit(7, "enwiki"), edit(6, "enwiki"), edit(5, "dewiki"), edit(4, "enwiki")]
    kafka = FakeKafka("edits", messages, committed=4)  # offsets 0-3 were processed before the restart
    published, counts = run(kafka, loops=6)
    # Warm-up starts 10 minutes back, at offset 1: offsets 1-3 rebuild state and publish nothing.
    assert [(e["wiki"], e["edits"]) for e in published] == [("enwiki", 3), ("dewiki", 2), ("enwiki", 4)]
    assert published[0]["event_ts"] == datetime(1970, 1, 1) + timedelta(seconds=NOW - 6 * 60)
    assert counts["outputs dropped while warming up"] == 3 and counts["published"] == 3
    assert min(kafka.commits) == 4 and kafka.commits[-1] == 7  # never behind what was processed before
    [processor] = EditCounter.instances
    assert all(isinstance(t, datetime) for t in processor.times)  # times arrive as datetimes, not milliseconds
    assert processor.ticks >= 1  # quiet input still reaches the processor, as time passing


def test_a_first_start_processes_the_warmup_window_as_new():
    messages = [edit(30, "enwiki"), edit(5, "enwiki"), edit(1, "dewiki")]
    published, _ = run(FakeKafka("edits", messages), loops=4)
    assert [(e["wiki"], e["edits"]) for e in published] == [("dewiki", 1), ("enwiki", 1)]  # from 10 minutes back


def test_outputs_that_dont_fit_are_skipped():
    published, counts = run(FakeKafka("edits", [edit(1, "badwiki")]), loops=2)
    assert published == [] and counts["outputs skipped: edits isn't long"] == 1


def test_a_quiet_start_with_nothing_recent_waits_at_the_end():
    kafka = FakeKafka("edits", [edit(60, "enwiki")])
    published, counts = run(kafka, loops=3)
    assert (published, counts["input events"]) == ([], 0)
    assert kafka.commits[-1] == 1


def failing_first(kafka: FakeKafka, *codes: int) -> None:
    """Make kafka.committed() fail with these error codes, once each, before it answers."""
    errors, answer = iter(codes), kafka.committed

    def committed(partitions, timeout=None):
        if (code := next(errors, None)) is not None:
            raise KafkaException(KafkaError(code))
        return answer(partitions, timeout)

    kafka.committed = committed


def test_a_broker_still_starting_is_waited_for():
    kafka = FakeKafka("edits", [edit(1, "dewiki")])
    failing_first(kafka, KafkaError.NOT_COORDINATOR, KafkaError.COORDINATOR_LOAD_IN_PROGRESS)
    waits = []
    published, _ = run(kafka, loops=2, sleep=waits.append)
    assert waits == [2, 2]
    assert [(e["wiki"], e["edits"]) for e in published] == [("dewiki", 1)]


def test_other_kafka_errors_stop_the_processor():
    kafka = FakeKafka("edits", [edit(1, "dewiki")])
    failing_first(kafka, KafkaError.UNKNOWN_TOPIC_OR_PART)
    with pytest.raises(KafkaException):
        run(kafka, loops=2)
