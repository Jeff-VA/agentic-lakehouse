"""Move events from a dataset's Kafka topic into its Iceberg table.

Each flush reads every message published before it started, appends the
events to Iceberg and, in the same Iceberg commit, records:

- the Kafka offsets read up to (table property agentic-lakehouse.kafka-offsets). The next
  flush resumes exactly there. Keeping offsets in Iceberg rather than in a
  Kafka consumer group means a crash between writing rows and saving offsets
  can't load a batch twice: both happen in one commit, or neither does.
- the flush watermark (agentic-lakehouse.flushed-through): Iceberg now holds every event
  older than this, the newest event flushed so far (or the flush's start, if
  earlier) minus the allowed lateness. It follows event time, not the clock,
  so it stays honest while a source catches up on a backlog after an outage:
  rollups then wait for the hours still arriving instead of publishing them
  short. The router relies on it to decide what the historical tier can answer.

Producers can evolve their events without breaking the flush: a new field
becomes an optional Iceberg column (NULL in older rows), and a field that
stops arriving is stored as NULL. The drift agent then notices the new
columns and proposes ontology updates.
"""

import json
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta

import pyarrow as pa
from confluent_kafka import Consumer, KafkaException, TopicPartition
from pyiceberg.table import Table
from pyiceberg.types import BooleanType, DoubleType, IcebergType, StringType

from agentic_lakehouse.config import Dataset
from agentic_lakehouse.orchestrator import FLUSHED_THROUGH

OFFSETS = "agentic-lakehouse.kafka-offsets"  # JSON {partition: next offset to read}
NEWEST = "agentic-lakehouse.newest-event"  # the newest event time flushed so far (ISO, naive UTC)

_EPOCH = datetime(1970, 1, 1)


class FlushError(RuntimeError):
    """Events couldn't be flushed; nothing was committed."""


@dataclass
class FlushResult:
    events: int
    watermark: datetime | None  # None until the first event is flushed
    new_columns: dict[str, str] = field(default_factory=dict)  # name -> Iceberg type


def flush(
    table: Table,
    consumer,
    dataset: Dataset,
    *,
    now: datetime,
    lateness: timedelta,
    batch_size: int = 50_000,
    batch_bytes: int = 8 * 2**20,
    timeout: float = 30,
) -> FlushResult:
    """Append every message in `dataset.topic` that `table` doesn't hold yet.

    `consumer` is a confluent_kafka.Consumer (only assign/consume are used, no
    consumer-group offsets). Events are committed in batches of `batch_size`
    events or about `batch_bytes` of JSON, whichever comes first, so a large
    backlog of wide events fits in memory; the watermark only moves with the
    last batch. `now` is naive UTC.
    """
    topic = dataset.topic
    start = json.loads(table.properties.get(OFFSETS, "{}"))
    partitions = sorted(consumer.list_topics(topic, timeout=timeout).topics[topic].partitions)
    ends, positions = {}, {}
    for partition in partitions:
        low, high = consumer.get_watermark_offsets(TopicPartition(topic, partition), timeout=timeout)
        position = start.get(str(partition), low)
        if position < low:
            raise FlushError(f"{topic}[{partition}]: offsets {position}-{low - 1} expired from Kafka before being flushed")
        if position > high:
            raise FlushError(
                f"{topic}[{partition}]: offset {position} is past the end of the topic ({high}); was it recreated? "
                f"To load it from the start, remove the table property {OFFSETS}"
            )
        ends[partition], positions[partition] = high, position
    consumer.assign([TopicPartition(topic, p, positions[p]) for p in partitions])

    previous = table.properties.get(FLUSHED_THROUGH)
    result = FlushResult(events=0, watermark=datetime.fromisoformat(previous) if previous else None)
    newest = datetime.fromisoformat(table.properties[NEWEST]) if NEWEST in table.properties else None
    while True:
        events = _read(consumer, positions, ends, batch_size, batch_bytes, timeout)
        done = all(positions[p] >= ends[p] for p in partitions)
        newest = max(filter(None, [newest, _newest(events, dataset.time_column)]), default=None)
        if events or done:
            properties = {OFFSETS: json.dumps({str(p): o for p, o in positions.items()})}
            if newest is not None:
                properties[NEWEST] = newest.isoformat()
            if done and newest is not None:
                # Never ahead of the clock (a producer's skewed clock), never back from the last flush
                result.watermark = _watermark(table, min(now, newest) - lateness)
                properties[FLUSHED_THROUGH] = result.watermark.isoformat()
            new_columns = _commit(table, events, properties)
            result.events += len(events)
            result.new_columns.update(new_columns)
        if done:
            return result


def _watermark(table: Table, candidate: datetime) -> datetime:
    previous = table.properties.get(FLUSHED_THROUGH)
    return max(candidate, datetime.fromisoformat(previous)) if previous else candidate


def _newest(events: list[dict], time_column: str) -> datetime | None:
    """The newest event time in `events`, which carry it as epoch milliseconds."""
    newest = max((e[time_column] for e in events if isinstance(e.get(time_column), int)), default=None)
    return None if newest is None else _EPOCH + timedelta(milliseconds=newest)


def _read(consumer, positions: dict, ends: dict, batch_size: int, batch_bytes: int, timeout: float) -> list[dict]:
    """Up to `batch_size` events, or about `batch_bytes` of them, from where `positions` stand to
    `ends`, advancing `positions`."""
    events, size = [], 0
    stalled_since = time.monotonic()
    while len(events) < batch_size and size < batch_bytes and (remaining := sum(max(0, ends[p] - positions[p]) for p in positions)):
        # consume() waits until it has num_messages or the timeout passes, so
        # ask for no more than remain, and poll briefly. Small reads keep a
        # batch close to batch_bytes.
        messages = consumer.consume(num_messages=min(100, batch_size - len(events), remaining), timeout=1)
        if not messages:
            if time.monotonic() - stalled_since > timeout:
                raise FlushError(f"no messages from Kafka for {timeout}s while reading up to offsets {ends}")
            continue
        stalled_since = time.monotonic()
        for message in messages:
            if message.error():
                raise KafkaException(message.error())
            partition, offset = message.partition(), message.offset()
            if offset >= ends[partition]:
                continue  # published after this flush started; the next flush takes it
            value = message.value()
            size += len(value)
            try:
                events.append(json.loads(value))
            except json.JSONDecodeError as e:
                raise FlushError(f"{message.topic()}[{partition}] offset {offset} isn't JSON: {e}") from None
            positions[partition] = offset + 1
    return events


def _commit(table: Table, events: list[dict], properties: dict[str, str]) -> dict[str, str]:
    """Append `events` with `properties` in one Iceberg commit, adding columns
    for fields the table doesn't have. Returns the added columns."""
    current = table.schema()
    known = {f.name for f in current.fields}
    new_columns = {}
    for name in dict.fromkeys(key for event in events for key in event):  # first-seen order
        if name not in known and (iceberg_type := _infer_type([e.get(name) for e in events])) is not None:
            new_columns[name] = iceberg_type
    arrow_schema = pa.schema(
        [pa.field(f.name, f.type) for f in current.as_arrow()]
        + [pa.field(name, _ARROW[type(t)]) for name, t in new_columns.items()]
    )
    rows = _to_arrow(events, arrow_schema) if events else None
    with table.transaction() as transaction:
        if new_columns:
            with transaction.update_schema() as update:
                for name, iceberg_type in new_columns.items():
                    # A one-element path: "device.os" is a column of that name, not a nested field.
                    update.add_column((name,), iceberg_type, doc="added by the flush: first seen in Kafka events")
        if rows is not None:
            transaction.append(rows)
        transaction.set_properties(properties)
    return {name: str(t) for name, t in new_columns.items()}


_ARROW = {BooleanType: pa.bool_(), DoubleType: pa.float64(), StringType: pa.string()}


def _infer_type(values: list) -> IcebergType | None:
    """Column type for a new field, from its values; None until a non-null value arrives.

    Numbers are always doubles: producers in JavaScript or Go send 0.0 as 0, so a
    field that looks whole in its first batch may carry 0.25 later, and Iceberg
    can't widen a long column to double afterwards.
    """
    kinds = {type(v) for v in values if v is not None}
    if not kinds:
        return None
    if kinds == {bool}:
        return BooleanType()
    if kinds <= {int, float}:
        return DoubleType()
    return StringType()  # text, or mixed/nested values kept as JSON


def _to_arrow(events: list[dict], schema: pa.Schema) -> pa.Table:
    columns = {}
    for column in schema:
        values = [event.get(column.name) for event in events]
        if pa.types.is_timestamp(column.type):  # events carry epoch milliseconds
            values = [None if v is None else _EPOCH + timedelta(milliseconds=v) for v in values]
        elif pa.types.is_string(column.type):
            values = [v if v is None or isinstance(v, str) else json.dumps(v) for v in values]
        elif pa.types.is_integer(column.type):  # Arrow would silently truncate 123.9 to 123
            if fractional := next((v for v in values if isinstance(v, float) and not v.is_integer()), None):
                raise FlushError(f"field '{column.name}' has a value that doesn't fit {column.type}: {fractional}")
        try:
            columns[column.name] = pa.array(values, type=column.type)
        except (pa.ArrowInvalid, pa.ArrowTypeError, TypeError, OverflowError) as e:
            raise FlushError(f"field '{column.name}' has a value that doesn't fit {column.type}: {e}") from None
    return pa.table(columns, schema=schema)


def kafka_consumer(bootstrap_servers: str) -> Consumer:
    """A consumer for flush(): offsets come from Iceberg, so the group never commits any."""
    return Consumer({"bootstrap.servers": bootstrap_servers, "group.id": "agentic-lakehouse-flush", "enable.auto.commit": False})
