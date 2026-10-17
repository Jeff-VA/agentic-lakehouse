import json
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest
from pyiceberg.catalog.sql import SqlCatalog

from agentic_lakehouse.config import load_platform
from agentic_lakehouse.flush import NEWEST, OFFSETS, FlushError, flush, kafka_consumer
from agentic_lakehouse.orchestrator import FLUSHED_THROUGH
from agentic_lakehouse.schema_compiler import iceberg_partition_spec, iceberg_schema

PLATFORM = load_platform(Path(__file__).parents[1] / "examples" / "playback" / "config")
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
    result = run_flush(table, {0: [event(0), event(1)], 1: [event(2)]}, now=T0 + timedelta(minutes=2, seconds=5))

    newest = T0 + timedelta(minutes=2)
    assert (result.events, result.watermark, result.new_columns) == (3, newest - LATENESS, {})
    assert [r["event_ts"] for r in rows(table)] == [T0, T0 + timedelta(minutes=1), newest]
    assert json.loads(table.properties[OFFSETS]) == {"0": 2, "1": 1}
    assert table.properties[FLUSHED_THROUGH] == (newest - LATENESS).isoformat()
    assert table.properties[NEWEST] == newest.isoformat()


def test_flushing_again_loads_nothing_twice(table):
    published = {0: [event(0), event(1)]}
    run_flush(table, published)
    result = run_flush(table, published, now=NOW + timedelta(minutes=5))

    assert result.events == 0
    assert len(rows(table)) == 2
    # No new events: the watermark can't claim more than the newest event flushed
    assert table.properties[FLUSHED_THROUGH] == (T0 + timedelta(minutes=1) - LATENESS).isoformat()
    grown = {0: [event(0), event(1), event(30)]}
    run_flush(table, grown, now=T0 + timedelta(minutes=31))
    run_flush(table, grown, now=T0)  # a clock behind the last flush
    assert table.properties[FLUSHED_THROUGH] == (T0 + timedelta(minutes=30) - LATENESS).isoformat()


def test_the_watermark_follows_event_time_while_a_source_catches_up(table):
    """A feed that was down for hours replays its backlog: Iceberg is complete only through
    the newest event it holds, however late the clock says it is."""
    result = run_flush(table, {0: [event(0), event(5), event(10)]}, now=T0 + timedelta(hours=6))
    assert result.watermark == T0 + timedelta(minutes=10) - LATENESS


def test_an_event_from_a_skewed_clock_cant_move_the_watermark_past_now(table):
    result = run_flush(table, {0: [event(0), event(240)]}, now=NOW)  # minute 240 is three hours ahead
    assert result.watermark == NOW - LATENESS


def test_an_empty_topic_claims_nothing(table):
    result = run_flush(table, {0: []})
    assert result.watermark is None and FLUSHED_THROUGH not in table.properties


def test_later_flushes_pick_up_where_the_last_stopped(table):
    run_flush(table, {0: [event(0)]})
    result = run_flush(table, {0: [event(0), event(1), event(2)]})
    assert result.events == 2
    assert [r["session_id"] for r in rows(table)] == ["s0", "s1", "s2"]


def test_flushes_started_together_load_each_event_once(table):
    """A laptop that sleeps wakes for a few seconds every quarter hour, and Prefect then starts
    every flush it missed at once. Each loads the table before any has committed, so all of them
    start from the same offsets: only one may append those events."""
    run_flush(table, {0: [event(0)]})  # a table the flush has been keeping current
    published = {0: [event(0), event(1), event(2)]}
    started = [table.catalog.load_table("raw.telemetry") for _ in range(3)]  # all at the same snapshot
    results = [run_flush(each, published) for each in started]

    assert [r["session_id"] for r in rows(table)] == ["s0", "s1", "s2"]
    assert json.loads(table.properties[OFFSETS]) == {"0": 3}
    assert [(r.events, r.overtaken) for r in results] == [(2, False), (0, True), (0, True)]


def test_a_flush_that_was_overtaken_leaves_newer_events_for_the_next(table):
    run_flush(table, {0: [event(0)]})
    late = table.catalog.load_table("raw.telemetry")
    run_flush(table, {0: [event(0), event(1)]})
    assert run_flush(late, {0: [event(0), event(1), event(2)]}).overtaken  # writes nothing, s2 included
    run_flush(table, {0: [event(0), event(1), event(2)]})
    assert [r["session_id"] for r in rows(table)] == ["s0", "s1", "s2"]


def test_a_commit_that_lost_a_race_to_another_writer_is_made_again_on_top(table):
    """Something other than a flush (a clean-up deleting rows, say) also makes the flush's commit
    fail; the offsets are still the ones it started from, so it commits again."""
    run_flush(table, {0: [event(0)]})
    flushing = table.catalog.load_table("raw.telemetry")
    table.delete("session_id = 's0'")
    result = run_flush(flushing, {0: [event(0), event(1), event(2)]})

    assert (result.events, result.overtaken) == (2, False)
    assert [r["session_id"] for r in rows(table)] == ["s1", "s2"]


def test_events_published_during_a_flush_wait_for_the_next(table):
    result = run_flush(table, {0: [event(0)]}, late={0: [event(1)]})
    assert result.events == 1
    assert json.loads(table.properties[OFFSETS]) == {"0": 1}


def test_new_fields_become_optional_columns(table):
    run_flush(table, {0: [event(0)]})
    newer = event(1, dropped_frames=3, cdn_pop="den-1", rebuffer_ratio=0.25, is_live=True, codec={"name": "av1"})
    result = run_flush(table, {0: [event(0), newer]})

    assert result.new_columns == {
        "dropped_frames": "double",  # numbers become doubles: a later 0.25 must not be truncated
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


def test_a_backlog_of_wide_events_is_committed_in_batches_that_fit_in_memory(table):
    """Wide events (long text) fill memory long before batch_size: batches are capped in bytes too."""
    wide = [event(i % 60, client_isp="x" * 2000) for i in range(250)]  # about 2.3 kB of JSON each
    consumer = FakeConsumer(DATASET.topic, {0: wide})
    result = flush(table, consumer, DATASET, now=NOW, lateness=LATENESS, batch_bytes=100_000)
    assert result.events == 250
    assert len(table.history()) == 3  # 100 + 100 + 50 events: each read of 100 passes 100 kB
    assert len(rows(table)) == 250


def test_events_that_expired_from_kafka_before_being_flushed_are_reported(table):
    run_flush(table, {0: [event(0), event(1)]})  # next to read: offset 2
    with pytest.raises(FlushError, match=r"offsets 2-4 expired from Kafka before being flushed"):
        run_flush(table, {0: [event(i) for i in range(8)]}, low={0: 5})  # retention deleted 0-4


def test_a_new_table_starts_from_the_oldest_event_kafka_still_has(table):
    assert run_flush(table, {0: [event(i) for i in range(8)]}, low={0: 5}).events == 3


def test_fractional_numbers_are_not_truncated(table):
    run_flush(table, {0: [event(0, frames_ratio=0)]})  # first seen as a whole number, as JS and Go send 0.0
    run_flush(table, {0: [event(0, frames_ratio=0), event(1, frames_ratio=0.25)]})
    assert [r["frames_ratio"] for r in rows(table)] == [0.0, 0.25]


def test_a_fractional_number_in_an_integer_column_fails_the_flush(table):
    with pytest.raises(FlushError, match="field 'request_latency_ms' has a value that doesn't fit int32: 123.9"):
        run_flush(table, {0: [event(0, request_latency_ms=123.9)]})


def test_dotted_field_names_become_columns_of_that_name(table):
    result = run_flush(table, {0: [event(0, **{"device.os": "ios"})]})
    assert result.new_columns == {"device.os": "string"}
    assert rows(table)[0]["device.os"] == "ios"


def test_offsets_past_the_end_of_the_topic_are_reported(table):
    run_flush(table, {0: [event(i) for i in range(5)]})  # next to read: offset 5
    with pytest.raises(FlushError, match=r"offset 5 is past the end of the topic \(2\); was it recreated"):
        run_flush(table, {0: [event(i) for i in range(2)]})  # the topic was deleted and recreated


def test_the_kafka_client_prefetches_about_one_batch_not_64_mib(monkeypatch):
    """After a laptop sleeps, the client's default prefetch filled with the backlog and the flush
    was killed for memory before it read its first batch."""
    configs = []
    monkeypatch.setattr("agentic_lakehouse.flush.Consumer", configs.append)
    kafka_consumer("kafka:9092")
    [config] = configs
    assert config["queued.max.messages.kbytes"] * 2**10 == config["fetch.max.bytes"] == 8 * 2**20
