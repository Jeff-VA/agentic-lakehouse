"""Live sources: feeds the platform reads itself and publishes to Kafka.

A dataset with a `source` in ingestion.yaml gets its events from that feed
instead of from producers writing to its topic. `agentic-lakehouse ingest`
reads the feed, turns each feed event into the dataset's event with the
source's `transform` (a function in the project's code), checks that it fits
the dataset's columns, and publishes it. From Kafka on, it's like any other
event: Pinot has it in seconds, the flush moves it into Iceberg.

The feed's own id for each event travels with its Kafka message, so a
restarted ingest asks the feed to resume after the last event it published;
feeds that keep history, as Wikimedia's does, send what was missed.

Kinds of feed:
- sse: Server-Sent Events over HTTP, with one JSON object in each event's data.
"""

import json
import time
from collections import Counter
from collections.abc import Callable, Iterator
from datetime import UTC, datetime
from typing import Any, Protocol

import httpx

from agentic_lakehouse.config import Dataset
from agentic_lakehouse.hooks import load

EVENT_ID_HEADER = "agentic-lakehouse-feed-id"  # the feed's id for the event, on each Kafka message


class Publisher(Protocol):
    def publish(self, events: list[dict], headers: dict[str, str] | None = None) -> None: ...
    def flush(self, timeout: float = 30) -> None: ...


def read_sse(client: httpx.Client, url: str, headers: dict[str, str], last_event_id: str | None) -> Iterator[tuple[str | None, str]]:
    """(event id, data) for each event of a Server-Sent Events stream, until it ends."""
    request_headers = {"Accept": "text/event-stream", **headers}
    if last_event_id:
        request_headers["Last-Event-ID"] = last_event_id
    with client.stream("GET", url, headers=request_headers) as response:
        response.raise_for_status()
        event_id, data = last_event_id, []
        for line in response.iter_lines():
            if not line:  # a blank line ends an event
                if data:
                    yield event_id, "\n".join(data)
                data = []
                continue
            if line.startswith(":"):  # a comment, such as a keep-alive
                continue
            field, _, value = line.partition(":")
            value = value.removeprefix(" ")
            if field == "data":
                data.append(value)
            elif field == "id":
                event_id = value


_TYPES = {
    "string": (str,),
    "int": (int,),
    "long": (int,),
    "double": (int, float),
    "boolean": (bool,),
    "timestamp": (datetime, int),
}


def conform(dataset: Dataset, event: dict[str, Any]) -> str | None:
    """Fit `event` to the dataset in place: times become naive UTC. Returns what's wrong with it, if anything.

    The time column must be there; other configured columns may be missing (stored as null), and
    fields the config doesn't know are kept (they become new columns, for the drift agent).
    """
    for column in dataset.columns:
        value = event.get(column.name)
        if value is None:
            if column.name == dataset.time_column:
                return f"no {column.name}"
            continue
        if not isinstance(value, _TYPES[column.type]) or (isinstance(value, bool) and column.type != "boolean"):
            return f"{column.name} isn't {column.type}"
        if isinstance(value, datetime) and value.tzinfo:
            event[column.name] = value.astimezone(UTC).replace(tzinfo=None)
    return None


def ingest(
    dataset: Dataset,
    publisher: Publisher,
    *,
    resume_after: str | None = None,
    client: httpx.Client | None = None,
    log: Callable[[str], None] = print,
    stop: Callable[[], bool] = lambda: False,
    report_every: float = 60,
    sleep: Callable[[float], None] = time.sleep,
) -> Counter:
    """Read the dataset's feed and publish its events until `stop()` is true. Reconnects, with
    backoff, whenever the feed drops. Returns counts of what happened to the feed's events."""
    source = dataset.source
    transform = load(source.transform) if source.transform else (lambda event: event)
    client = client or httpx.Client(timeout=httpx.Timeout(30, read=120), follow_redirects=True)
    last_id, backoff, totals, window = resume_after, 1.0, Counter(), Counter()
    reported = time.monotonic()
    while not stop():
        try:
            for event_id, data in read_sse(client, source.url, source.headers, last_id):
                last_id = event_id or last_id
                outcome = _handle(dataset, transform, data, publisher, last_id)
                window[outcome] += 1
                backoff = 1.0
                if time.monotonic() - reported >= report_every:
                    log(f"{dataset.name}: " + ", ".join(f"{n} {what}" for what, n in window.most_common()))
                    totals += window
                    window, reported = Counter(), time.monotonic()
                if stop():
                    break
            else:
                sleep(1)  # the feed closed the stream; don't reconnect in a tight loop
        except (httpx.HTTPError, httpx.StreamError) as e:
            log(f"{dataset.name}: the feed failed ({e!r}); reconnecting in {backoff:g}s")
            sleep(backoff)
            backoff = min(backoff * 2, 60)
    publisher.flush()
    return totals + window


def _handle(dataset: Dataset, transform: Callable, data: str, publisher: Publisher, event_id: str | None) -> str:
    try:
        raw = json.loads(data)
    except json.JSONDecodeError:
        return "not JSON"
    event = transform(raw)
    if event is None:
        return "left out by the transform"
    if problem := conform(dataset, event):
        return f"skipped: {problem}"
    publisher.publish([event], headers={EVENT_ID_HEADER: event_id} if event_id else None)
    return "published"

