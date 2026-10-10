"""Live views: dashboards get a view's new result as soon as its data changes.

GET /views/{name}/live?<params> is a Server-Sent Events stream. Its first event
is the view's current result; after that an event arrives only when something
changes:

    event: result
    data: {"view": "isp_now", "rows": [...], ...}   (the JSON GET /views/{name} returns)

    event: error
    data: {"detail": "trino: ..."}   (the last result still stands; the feed keeps trying)

GET /live?view=<name>&view=<name>?<params> watches several views over one
connection, which is what a dashboard uses: browsers allow only about six open
connections to a server, so one stream per chart would stall a page with many.
Each event's data then says which `view` value it answers:

    event: result
    data: {"subscription": "isp_hourly?hours=24", "result": {...}}

That stream also carries news about the project to every open page: `hello`
first (with the server's boot id: a page that sees a new one reloads), then
`reload` after a dashboard or view file changes, or `problem` when an edit
broke a view (the page keeps showing the last good version).

Everyone watching the same view with the same parameters shares one feed, which
recomputes the view in the background while anyone watches, so a hundred open
dashboards cost what one does. When the feed recomputes:

- A view over a model reads the real-time tier, which changes all the time:
  every `refresh_seconds` (the view's header can set it).
- A view over tables changes when a flush or rollup commits to one of them. Their
  Iceberg snapshots are checked every `refresh_seconds` (a metadata read), and
  the view reruns after a commit, or once `max_age_seconds` have passed anyway,
  since its SQL can depend on now().

When the last subscriber leaves, the feed stops.
"""

import asyncio
import json
import logging
import time
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING, Any

from fastapi.sse import ServerSentEvent

from core.compiler import QueryError
from core.engines import EngineError
from core.views import ModelSource, View, ViewError

if TYPE_CHECKING:
    from core.api_gateway import Gateway

log = logging.getLogger(__name__)


class _Feed:
    """One view with one set of parameters, and the inboxes of everyone watching it."""

    def __init__(self, name: str, values: dict[str, Any]):
        self.name, self.values = name, values
        self.inboxes: list[asyncio.Queue[ServerSentEvent]] = []
        self.result: ServerSentEvent | None = None  # the last result, for whoever subscribes next
        self.error: ServerSentEvent | None = None  # set while the latest refresh failed
        self.task: asyncio.Task | None = None

    def subscribe(self) -> asyncio.Queue[ServerSentEvent]:
        inbox: asyncio.Queue[ServerSentEvent] = asyncio.Queue(maxsize=2)
        for event in (self.result, self.error):
            if event is not None:
                inbox.put_nowait(event)
        self.inboxes.append(inbox)
        return inbox

    def publish(self, event: ServerSentEvent) -> None:
        if event.event == "result":
            self.result, self.error = event, None
        else:
            self.error = event
        for inbox in self.inboxes:
            if inbox.full():  # a slow client only needs the newest events
                inbox.get_nowait()
            inbox.put_nowait(event)


class LiveViews:
    """The feeds behind GET /views/{name}/live."""

    def __init__(self, gateway: "Gateway", *, refresh_seconds: float = 10, max_age_seconds: float = 60):
        self.gateway = gateway
        self.refresh_seconds = refresh_seconds
        self.max_age_seconds = max_age_seconds
        self._feeds: dict[tuple[str, str], _Feed] = {}
        self._pages: set[asyncio.Queue] = set()  # every open /live stream's queue, for announcements

    async def events(self, name: str, values: dict[str, Any]) -> AsyncIterator[ServerSentEvent]:
        """View `name`'s result with parameter `values` (already bound), then every change to it."""
        key = (name, json.dumps(values, sort_keys=True, default=str))
        feed = self._feeds.get(key)
        if feed is None:
            feed = self._feeds[key] = _Feed(name, values)
            feed.task = asyncio.create_task(self._refresh(feed))
        inbox = feed.subscribe()
        try:
            while True:
                yield await inbox.get()
        finally:
            feed.inboxes.remove(inbox)
            if not feed.inboxes:
                feed.task.cancel()
                if self._feeds.get(key) is feed:  # not if restart() already replaced it
                    del self._feeds[key]

    async def watch(self, subscriptions: list[tuple[str, str, dict[str, Any]]]) -> AsyncIterator[tuple[str, ServerSentEvent]]:
        """Several views in one stream. `subscriptions` are (key, view name, parameter values);
        yields (key, event) as each view's events arrive."""
        merged: asyncio.Queue[tuple[str, ServerSentEvent]] = asyncio.Queue(maxsize=2 * len(subscriptions) + 2)

        async def forward(key: str, name: str, values: dict[str, Any]) -> None:
            async for event in self.events(name, values):
                await merged.put((key, event))  # waits for a slow client; its feed's inbox keeps the newest

        tasks = [asyncio.create_task(forward(*subscription)) for subscription in subscriptions]
        self._pages.add(merged)
        try:
            while True:
                yield await merged.get()
        finally:
            self._pages.discard(merged)
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    def announce(self, event: str, data: dict[str, Any]) -> None:
        """Send every open page an event about the project, such as `reload`. Its key is ""."""
        for page in self._pages:
            if page.full():
                page.get_nowait()  # older results matter less than this
            page.put_nowait(("", ServerSentEvent(event=event, data=data)))

    def restart(self) -> None:
        """Stop every feed, as views were redefined: pages reload and start new ones."""
        self.close()

    def watched(self) -> list[tuple[str, int]]:
        """(view, subscribers) for every running feed."""
        return [(feed.name, len(feed.inboxes)) for feed in self._feeds.values()]

    def close(self) -> None:
        for feed in self._feeds.values():
            feed.task.cancel()
        self._feeds.clear()

    async def _refresh(self, feed: _Feed) -> None:
        view = self.gateway.views[feed.name]
        interval = view.refresh_seconds or self.refresh_seconds
        version = computed_at = published = None
        while True:
            try:
                current = await asyncio.to_thread(self._version, view)
                due = computed_at is None or time.monotonic() - computed_at >= self.max_age_seconds
                if current is None or current != version or due:
                    result = await asyncio.to_thread(self.gateway.view, feed.name, feed.values)
                    version, computed_at = current, time.monotonic()
                    # When it was computed, and the time range a live view covers, move on every
                    # refresh; only a change in what the result says is worth pushing.
                    said = ("result", result.model_dump_json(exclude={"computed_at", "coverage"}))
                    if said != published:
                        feed.publish(ServerSentEvent(event="result", data=result))
                        published = said
            except Exception as e:
                if not isinstance(e, (ViewError, QueryError, EngineError)):
                    log.exception("live view %s failed", feed.name)
                said = ("error", str(e))
                if said != published:
                    feed.publish(ServerSentEvent(event="error", data={"detail": str(e)}))
                    published = said
            await asyncio.sleep(interval)

    def _version(self, view: View) -> tuple | None:
        """What a view over tables last read: its tables' states. None for a view over a model,
        whose real-time data has no version to compare, so it reruns every time."""
        warehouse = self.gateway.warehouse
        if isinstance(view.source, ModelSource) or warehouse is None:
            return None
        return tuple(warehouse.table_state(table) for table in view.source.tables)
