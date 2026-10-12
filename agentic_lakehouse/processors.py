"""Processors: datasets made from other datasets as they stream.

A dataset with a `processor` in ingestion.yaml is computed from another
dataset in real time. `agentic-lakehouse process` reads the input dataset's
Kafka topic, hands each batch of events to the project's processor, and
publishes what it returns as this dataset's events. From there they are like
any others: in Pinot within seconds, in Iceberg after the next flush, and live
in the ontology, views, dashboards and agents.

A processor is a class in the project's code:

    class TrendDetector:
        def __init__(self, context: ProcessorContext): ...
        def process(self, events: list[dict]) -> list[dict]: ...

process() gets the input's events in order, times as naive UTC datetimes, and
an empty batch about once a second while the input is quiet, so it can act on
time passing too. It keeps whatever state it needs in memory.
`warmup_minutes` replays that much input at start, so the state is whole again
after a restart; what the replay of already-processed input produces is
dropped, so nothing is published twice. The context runs SQL over the
lakehouse's tables (to load a model the processor applies, say) and logs.

Input offsets are committed to the processor's consumer group after the
output they led to is delivered. If the process dies in between, those events
are processed again: at least once.
"""

import json
import time
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

import pyarrow as pa
from confluent_kafka import KafkaException, TopicPartition

from agentic_lakehouse.config import Dataset, Platform
from agentic_lakehouse.hooks import load
from agentic_lakehouse.sources import Publisher, conform

_EPOCH = datetime(1970, 1, 1)


@dataclass
class ProcessorContext:
    dataset: Dataset  # the dataset the processor makes
    input: Dataset  # the dataset it reads
    query: Callable[[str], pa.Table]  # SQL over the lakehouse's tables, named <namespace>.<table>
    log: Callable[[str], None] = print
    now: Callable[[], datetime] = field(default=lambda: datetime.now(UTC).replace(tzinfo=None))  # naive UTC


def consumer_group(dataset: Dataset) -> str:
    return f"agentic-lakehouse-process-{dataset.name}"


def run_processor(
    platform: Platform,
    dataset: Dataset,
    *,
    consumer,
    publisher: Publisher,
    context: ProcessorContext,
    log: Callable[[str], None] = print,
    stop: Callable[[], bool] = lambda: False,
    commit_every: float = 5,
    report_every: float = 60,
    clock: Callable[[], float] = time.time,
) -> Counter:
    """Run the dataset's processor until `stop()` is true. `consumer` is a confluent_kafka.Consumer
    in the dataset's consumer group (consumer_group()). Returns counts of what happened."""
    spec = dataset.processor
    source = platform.dataset(spec.input)
    processor = load(spec.run)(context)
    topic = source.topic
    partitions = sorted(consumer.list_topics(topic, timeout=10).topics[topic].partitions)
    committed = {tp.partition: tp.offset for tp in consumer.committed([TopicPartition(topic, p) for p in partitions], timeout=10)}
    warmup_ms = int((clock() - spec.warmup_minutes * 60) * 1000)
    by_time = {tp.partition: tp.offset for tp in consumer.offsets_for_times([TopicPartition(topic, p, warmup_ms) for p in partitions], timeout=10)}

    positions, resume = {}, {}  # resume: where input not processed before starts; what's before only rebuilds state
    for p in partitions:
        low, high = consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=10)
        warm = by_time[p] if by_time[p] >= 0 else high  # no message that recent: start at the end
        resume[p] = committed[p] if committed[p] >= 0 else max(warm, low)
        positions[p] = max(min(warm, resume[p]), low)
    consumer.assign([TopicPartition(topic, p, positions[p]) for p in partitions])
    replay = sum(resume[p] - positions[p] for p in partitions)
    log(f"{dataset.name}: processing {topic} with {spec.run}" + (f", replaying {replay} events to warm up" if replay else ""))

    timestamps = [c.name for c in source.columns if c.type == "timestamp"]
    counts, window = Counter(), Counter()
    last_commit = last_report = time.monotonic()

    def commit() -> None:
        publisher.flush()
        # Never behind `resume`: the replay doesn't move what was processed backwards.
        offsets = [TopicPartition(topic, p, max(positions[p], resume[p])) for p in partitions]
        consumer.commit(offsets=offsets, asynchronous=False)

    def handle(outputs: list[dict], publish: bool) -> None:
        if not outputs:
            return
        if not publish:
            window["outputs dropped while warming up"] += len(outputs)
            return
        good = []
        for output in outputs:
            if problem := conform(dataset, output):
                window[f"outputs skipped: {problem}"] += 1
            else:
                good.append(output)
        publisher.publish(good)
        window["published"] += len(good)

    try:
        while not stop():
            messages = consumer.consume(num_messages=500, timeout=1)
            replayed, new = [], []  # input processed before this start, and input that isn't
            for message in messages:
                if message.error():
                    raise KafkaException(message.error())
                p, offset = message.partition(), message.offset()
                positions[p] = offset + 1
                try:
                    event = json.loads(message.value())
                except json.JSONDecodeError:
                    window["input events not JSON"] += 1
                    continue
                for name in timestamps:  # Kafka carries epoch milliseconds
                    if isinstance(event.get(name), int):
                        event[name] = _EPOCH + timedelta(milliseconds=event[name])
                (new if offset >= resume[p] else replayed).append(event)
            window["input events"] += len(replayed) + len(new)
            if replayed:
                handle(processor.process(replayed), publish=False)
            if new or not messages:  # a quiet second still reaches the processor, as time passing
                handle(processor.process(new), publish=all(positions[p] >= resume[p] for p in partitions) or bool(new))
            if time.monotonic() - last_commit >= commit_every:
                commit()
                last_commit = time.monotonic()
            if time.monotonic() - last_report >= report_every:
                log(f"{dataset.name}: " + ", ".join(f"{n} {what}" for what, n in window.most_common()))
                counts += window
                window, last_report = Counter(), time.monotonic()
    finally:
        commit()
    return counts + window
