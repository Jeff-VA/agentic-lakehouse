"""Live sources (agentic_lakehouse/sources.py): a feed read into Kafka, resumed where it left off."""

import json
import shutil
from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest
import yaml
from pydantic import ValidationError

from agentic_lakehouse.config import ConfigError, Dataset, load_platform
from agentic_lakehouse.sources import EVENT_ID_HEADER, conform, ingest, read_sse

EXAMPLE = Path(__file__).parents[1] / "examples" / "playback" / "config"
URL = "https://feed.test/stream"

EDITS = Dataset.model_validate(
    {
        "name": "edits",
        "description": "Edits from a feed.",
        "topic": "edits",
        "time_column": "event_ts",
        "columns": [
            {"name": "event_ts", "type": "timestamp"},
            {"name": "wiki", "type": "string"},
            {"name": "bytes", "type": "int"},
        ],
        "realtime": {"table": "edits", "retention_days": 7},
        "source": {"kind": "sse", "url": URL, "transform": "test_sources:to_edit", "headers": {"User-Agent": "tests"}},
    }
)


def to_edit(raw: dict) -> dict | None:
    """The source's transform: a feed event to an edit, leaving out everything but edits."""
    if raw.get("type") != "edit":
        return None
    return {"event_ts": datetime.fromtimestamp(raw["timestamp"], UTC), "wiki": raw["wiki"], "bytes": raw["bytes"], "user": raw.get("user")}


def sse(*events: tuple[str | None, object]) -> bytes:
    lines = [": a keep-alive comment"]
    for event_id, data in events:
        if event_id:
            lines.append(f"id: {event_id}")
        lines.append(f"data: {data if isinstance(data, str) else json.dumps(data)}")
        lines.append("")
    return ("\n".join(lines) + "\n").encode()


class FakePublisher:
    def __init__(self):
        self.messages = []

    def publish(self, events, headers=None):
        self.messages += [(event, headers) for event in events]

    def flush(self, timeout=30):
        pass


def test_server_sent_events_are_read_with_their_ids():
    body = b": hello\n\nid: 1\ndata: {\"a\":\ndata: 1}\n\ndata: no id\n\nid: 3\ndata: last\n\nid: 4\ndata: never ended"
    transport = httpx.MockTransport(lambda request: httpx.Response(200, content=body))
    with httpx.Client(transport=transport) as client:
        events = list(read_sse(client, URL, {}, None))
    assert events == [("1", '{"a":\n1}'), ("1", "no id"), ("3", "last")]  # an id holds until the next one


def test_a_feed_is_published_and_resumed_after_its_last_event():
    edit = {"type": "edit", "timestamp": 1791768258, "wiki": "enwiki", "bytes": 167, "user": "PhuzBot"}
    connections = []

    def feed(request: httpx.Request) -> httpx.Response:
        connections.append(dict(request.headers))
        if len(connections) == 1:
            return httpx.Response(200, content=sse(
                ("a", "not json"),
                ("b", {"type": "log", "timestamp": 1, "wiki": "enwiki"}),  # left out by the transform
                ("c", edit | {"bytes": "lots"}),  # doesn't fit the columns
                ("d", edit),
            ))  # then the feed closes the stream
        return httpx.Response(200, content=sse(("e", edit | {"wiki": "dewiki"})))

    publisher = FakePublisher()
    with httpx.Client(transport=httpx.MockTransport(feed)) as client:
        counts = ingest(EDITS, publisher, client=client, log=lambda m: None, stop=lambda: len(publisher.messages) == 2, sleep=lambda s: None)
    assert [headers for _, headers in publisher.messages] == [{EVENT_ID_HEADER: "d"}, {EVENT_ID_HEADER: "e"}]
    first, _ = publisher.messages[0]
    assert first == {"event_ts": datetime(2026, 10, 12, 1, 24, 18), "wiki": "enwiki", "bytes": 167, "user": "PhuzBot"}  # naive UTC
    assert connections[0]["user-agent"] == "tests" and "last-event-id" not in connections[0]
    assert connections[1]["last-event-id"] == "d"  # resumed after the last event read
    assert counts == {"published": 2, "not JSON": 1, "left out by the transform": 1, "skipped: bytes isn't int": 1}


def test_a_restarted_ingest_resumes_after_the_id_kafka_kept():
    seen = []

    def feed(request):
        seen.append(request.headers.get("last-event-id"))
        return httpx.Response(200, content=sse())

    with httpx.Client(transport=httpx.MockTransport(feed)) as client:
        ingest(EDITS, FakePublisher(), resume_after="[{\"offset\": 42}]", client=client, log=lambda m: None, stop=lambda: bool(seen), sleep=lambda s: None)
    assert seen == ['[{"offset": 42}]']


def test_a_failing_feed_is_retried_with_backoff():
    statuses = iter([500, 503, 500])
    waits, log = [], []

    def feed(request):
        return httpx.Response(next(statuses, 200), content=sse())

    with httpx.Client(transport=httpx.MockTransport(feed)) as client:
        ingest(EDITS, FakePublisher(), client=client, log=log.append, stop=lambda: len(waits) >= 4, sleep=waits.append)
    assert waits == [1, 2, 4, 1]  # doubling while it fails; after a clean (empty) stream, a short pause
    assert "the feed failed" in log[0] and "reconnecting in 1s" in log[0]


@pytest.mark.parametrize(
    ("event", "problem"),
    [
        ({"wiki": "enwiki"}, "no event_ts"),
        ({"event_ts": 1791768258000, "wiki": 5}, "wiki isn't string"),
        ({"event_ts": 1791768258000, "bytes": True}, "bytes isn't int"),  # bools aren't counts
        ({"event_ts": 1791768258000, "bytes": None}, None),  # missing values are stored as null
        ({"event_ts": 1791768258000, "extra": [1, 2]}, None),  # unknown fields are kept, for the drift agent
    ],
)
def test_events_must_fit_the_dataset(event, problem):
    assert conform(EDITS, event) == problem


def test_a_dataset_comes_from_a_source_or_a_processor_not_both():
    with pytest.raises(ValidationError, match="from a source or a processor, not both"):
        Dataset.model_validate(EDITS.model_dump() | {"processor": {"input": "edits", "run": "m:Detector"}})


def test_a_processor_must_read_a_known_dataset(tmp_path):
    for path in EXAMPLE.glob("*.yaml"):
        shutil.copy(path, tmp_path)
    ingestion = yaml.safe_load((tmp_path / "ingestion.yaml").read_text())
    trends = ingestion["datasets"][0] | {"name": "trends", "topic": "trends", "simulator": None}
    trends["realtime"] = {"table": "trends", "retention_days": 7}
    ingestion["datasets"].append(trends | {"processor": {"input": "edits", "run": "m:Detector"}})
    (tmp_path / "ingestion.yaml").write_text(yaml.safe_dump(ingestion))
    storage = yaml.safe_load((tmp_path / "storage.yaml").read_text())
    storage["tables"].append({"dataset": "trends", "table": "raw.trends", "partition_by": "day", "flush_every_minutes": 5, "allowed_lateness_minutes": 2})
    (tmp_path / "storage.yaml").write_text(yaml.safe_dump(storage))
    with pytest.raises(ConfigError, match="dataset 'trends' is processed from unknown dataset 'edits'"):
        load_platform(tmp_path)
