import json
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest
from pyiceberg.catalog.sql import SqlCatalog

from core.config import load_platform
from core.flush import OFFSETS, FlushError, flush
from core.orchestrator import FLUSHED_THROUGH
from core.schema_compiler import iceberg_partition_spec, iceberg_schema

PLATFORM = load_platform(Path(__file__).parents[1] / "config")
DATASET = PLATFORM.dataset("telemetry")
T0 = datetime(2026, 1, 1)
NOW = T0 + timedelta(hours=1)
LATENESS = timedelta(minutes=2)


def event(minute: int, **fields) -> dict:
    """A telemetry event as producers publish it: JSON-ready, time in epoch milliseconds."""
    ts = T0 + timedelta(minutes=minute)
    return {
        "event_ts": (ts - datetime(1970, 1, 1)) // timedelta(milliseconds=1),
        "session_id": f"s{minute}",
        "client_isp": "comcast",
        "client_device": "ios",
        "request_latency_ms": 100 + minute,
        "stream_start_event": minute == 0,
        "buffer_duration_ms": 0,
        "bitrate_kbps": 2500,
        **fields,
    }


class FakeConsumer:
    """The slice of confluent_kafka.Consumer that flush() uses, over in-memory partitions.

    `published` holds what was in each partition when the flush started;
    `late` is published while it runs, past the end offsets it was given.
    """

    def __init__(self, topic, published: dict[int, list[dict]], late=None, low=None):
        self.topic, self.published, self.late, self.low = topic, published, late or {}, low or {}
        self.queue = []

    def list_topics(self, topic, timeout=None):
        return SimpleNamespace(topics={topic: SimpleNamespace(partitions=dict.fromkeys(self.published))})

    def get_watermark_offsets(self, tp, timeout=None):
        return self.low.get(tp.partition, 0), len(self.published[tp.partition])

    def assign(self, partitions):
        for tp in partitions:
            messages = self.published[tp.partition] + self.late.get(tp.partition, [])
            self.queue += [
                SimpleNamespace(
                    partition=lambda p=tp.partition: p,
                    offset=lambda o=offset: o,
                    topic=lambda: self.topic,
                    value=lambda m=message: json.dumps(m).encode(),
                    error=lambda: None,
                )
                for offset, message in enumerate(messages)
                if offset >= tp.offset
            ]

    def consume(self, num_messages, timeout):
        batch, self.queue = self.queue[:num_messages], self.queue[num_messages:]
        return batch


@pytest.fixture
def table(tmp_path):
    catalog = SqlCatalog("test", uri=f"sqlite:///{tmp_path}/catalog.db", warehouse=f"file://{tmp_path}")
    catalog.create_namespace("raw")
    return catalog.create_table(
        "raw.telemetry",
        schema=iceberg_schema(DATASET),
        partition_spec=iceberg_partition_spec(DATASET, PLATFORM.iceberg_table("telemetry")),
    )


def run_flush(table, published, now=NOW, **kwargs):
    return flush(table, FakeConsumer(DATASET.topic, published, **kwargs), DATASET, now=now, lateness=LATENESS)


def rows(table) -> list[dict]:
    table.refresh()
    return sorted(table.scan().to_arrow().to_pylist(), key=lambda r: r["event_ts"])


def test_first_flush_loads_everything_with_offsets_and_watermark(table):
    result = run_flush(table, {0: [event(0), event(1)], 1: [event(2)]})

    assert (result.events, result.watermark, result.new_columns) == (3, NOW - LATENESS, {})
    assert [r["event_ts"] for r in rows(table)] == [T0, T0 + timedelta(minutes=1), T0 + timedelta(minutes=2)]
    assert json.loads(table.properties[OFFSETS]) == {"0": 2, "1": 1}
    assert table.properties[FLUSHED_THROUGH] == (NOW - LATENESS).isoformat()


def test_flushing_again_loads_nothing_twice(table):
    published = {0: [event(0), event(1)]}
    run_flush(table, published)
    result = run_flush(table, published, now=NOW + timedelta(minutes=5))

    assert result.events == 0
    assert len(rows(table)) == 2
    assert table.properties[FLUSHED_THROUGH] == (NOW + timedelta(minutes=5) - LATENESS).isoformat()
    run_flush(table, published, now=NOW - timedelta(hours=1))  # a clock behind the last flush
    assert table.properties[FLUSHED_THROUGH] == (NOW + timedelta(minutes=5) - LATENESS).isoformat()


def test_later_flushes_pick_up_where_the_last_stopped(table):
    run_flush(table, {0: [event(0)]})
    result = run_flush(table, {0: [event(0), event(1), event(2)]})
    assert result.events == 2
    assert [r["session_id"] for r in rows(table)] == ["s0", "s1", "s2"]


def test_events_published_during_a_flush_wait_for_the_next(table):
    result = run_flush(table, {0: [event(0)]}, late={0: [event(1)]})
    assert result.events == 1
    assert json.loads(table.properties[OFFSETS]) == {"0": 1}


def test_new_fields_become_optional_columns(table):
    run_flush(table, {0: [event(0)]})
    newer = event(1, dropped_frames=3, cdn_pop="den-1", rebuffer_ratio=0.25, is_live=True, codec={"name": "av1"})
    result = run_flush(table, {0: [event(0), newer]})

    assert result.new_columns == {
        "dropped_frames": "long",
        "cdn_pop": "string",
        "rebuffer_ratio": "double",
        "is_live": "boolean",
        "codec": "string",
    }
    old, new = rows(table)
    assert old["dropped_frames"] is None  # rows written before the column existed read as NULL
    assert (new["dropped_frames"], new["cdn_pop"], new["rebuffer_ratio"], new["is_live"]) == (3, "den-1", 0.25, True)
    assert json.loads(new["codec"]) == {"name": "av1"}  # nested values are kept as JSON text


def test_fields_that_stop_arriving_are_stored_as_null(table):
    without_bitrate = event(0)
    del without_bitrate["bitrate_kbps"]
    run_flush(table, {0: [without_bitrate]})
    assert rows(table)[0]["bitrate_kbps"] is None


def test_a_bad_value_fails_the_flush_and_commits_nothing(table):
    with pytest.raises(FlushError, match="field 'request_latency_ms' has a value that doesn't fit int32"):
        run_flush(table, {0: [event(0), event(1, request_latency_ms="slow")]})
    table.refresh()
    assert table.current_snapshot() is None
    assert OFFSETS not in table.properties


def test_large_backlogs_are_committed_in_batches(table):
    consumer = FakeConsumer(DATASET.topic, {0: [event(i) for i in range(5)]})
    result = flush(table, consumer, DATASET, now=NOW, lateness=LATENESS, batch_size=2)
    assert result.events == 5
    assert len(table.history()) == 3  # 2 + 2 + 1 events
    assert len(rows(table)) == 5


def test_events_that_expired_from_kafka_before_being_flushed_are_reported(table):
    run_flush(table, {0: [event(0), event(1)]})  # next to read: offset 2
    with pytest.raises(FlushError, match=r"offsets 2-4 expired from Kafka before being flushed"):
        run_flush(table, {0: [event(i) for i in range(8)]}, low={0: 5})  # retention deleted 0-4


def test_a_new_table_starts_from_the_oldest_event_kafka_still_has(table):
    assert run_flush(table, {0: [event(i) for i in range(8)]}, low={0: 5}).events == 3
