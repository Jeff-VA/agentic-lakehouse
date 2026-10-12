"""Live views (agentic_lakehouse/live.py): one shared feed per view and parameters, pushing only changes."""

import asyncio
import json
import socket
import threading
import time
from dataclasses import replace
from datetime import UTC, datetime

import httpx
import pytest
import uvicorn
from test_view_api import gateway, warehouse

from agentic_lakehouse.api_gateway import TableState, create_app
from agentic_lakehouse.engines import EngineError
from agentic_lakehouse.live import LiveViews

REFRESH = 0.05  # seconds; the default is 10


class Watch:
    """Collects a live view's events in the background, like an open dashboard."""

    def __init__(self, live: LiveViews, name: str, params: dict | None = None):
        self.events = []
        view = live.gateway.views[name]

        async def consume():
            async for event in live.events(name, view.bind(params or {})):
                self.events.append(event)

        self.task = asyncio.create_task(consume())

    async def until(self, count: int, timeout: float = 5) -> list:
        deadline = time.monotonic() + timeout
        while len(self.events) < count:
            assert time.monotonic() < deadline, f"expected {count} events, got {[e.event for e in self.events]}"
            await asyncio.sleep(0.01)
        return self.events

    async def stop(self) -> None:
        self.task.cancel()
        await asyncio.gather(self.task, return_exceptions=True)


def counting(gw):
    """Counts the gateway's view computations."""
    gw.calls = 0
    view = gw.view

    def counted(*args, **kwargs):
        gw.calls += 1
        return view(*args, **kwargs)

    gw.view = counted
    return gw


def test_everyone_watching_a_view_shares_one_feed(tmp_path):
    live = LiveViews(gateway(tmp_path), refresh_seconds=REFRESH)

    async def main():
        first = Watch(live, "plays_by_isp")
        [event] = await first.until(1)
        assert event.event == "result" and [r["isp"] for r in event.data.rows] == ["att", "comcast", "verizon"]
        second = Watch(live, "plays_by_isp", {"hours": "6"})  # the same values, so the same feed
        other = Watch(live, "plays_by_isp", {"hours": "2"})
        await second.until(1)
        await other.until(1)
        assert second.events[0] is event  # newcomers get the current result without a new query
        assert sorted(live.watched()) == [("plays_by_isp", 1), ("plays_by_isp", 2)]
        for watch in (first, second, other):
            await watch.stop()
        assert live.watched() == []

    asyncio.run(main())


def test_a_change_in_the_data_is_pushed_and_nothing_else_is(tmp_path):
    gw = gateway(tmp_path)
    live = LiveViews(gw, refresh_seconds=REFRESH)
    realtime = gw.router.placements["playback"].realtime.engine  # its execute() takes turns with the feed's queries

    async def main():
        watch = Watch(live, "plays_by_isp")
        [before] = await watch.until(1)
        await asyncio.sleep(10 * REFRESH)
        assert len(watch.events) == 1  # rerun every refresh, but the result didn't change
        realtime.execute("INSERT INTO playback VALUES ('2026-01-01 05:30:00', 'verizon', 'ios', 'u1', 500)")
        _, after = await watch.until(2)
        plays = {e: {r["isp"]: r["plays"] for r in event.data.rows} for e, event in (("before", before), ("after", after))}
        assert plays["after"] == plays["before"] | {"verizon": plays["before"]["verizon"] + 1}
        await watch.stop()

    asyncio.run(main())


def test_a_view_over_tables_reruns_only_after_a_commit(tmp_path):
    stand_in = warehouse()
    snapshot = {"id": 1}
    state = lambda table: TableState(exists=True, complete_through=datetime(2026, 1, 1, 4), snapshot=snapshot["id"])  # noqa: E731
    gw = counting(gateway(tmp_path, warehouse=replace(stand_in, table_state=state)))
    live = LiveViews(gw, refresh_seconds=REFRESH)
    gold = stand_in.engine

    async def main():
        watch = Watch(live, "hourly")
        await watch.until(1)
        gold.execute("INSERT INTO gold.hourly VALUES ('2026-01-01 05:00:00', 'att', 4, 0.4)")
        await asyncio.sleep(10 * REFRESH)
        assert (gw.calls, len(watch.events)) == (1, 1)  # no commit was recorded, so no rerun
        snapshot["id"] = 2
        _, after = await watch.until(2)
        assert after.data.rows[-1] == {"hour": datetime(2026, 1, 1, 5, tzinfo=UTC), "isp": "att", "plays": 4, "slow_rate": 0.4}
        assert gw.calls == 2
        await watch.stop()

    asyncio.run(main())


def test_a_view_over_tables_also_reruns_once_its_result_is_old(tmp_path):
    gw = counting(gateway(tmp_path))
    live = LiveViews(gw, refresh_seconds=REFRESH, max_age_seconds=3 * REFRESH)

    async def main():
        watch = Watch(live, "hourly")
        await watch.until(1)
        await asyncio.sleep(12 * REFRESH)
        assert gw.calls >= 3  # its SQL could depend on now()
        assert len(watch.events) == 1  # the same rows, so nothing to push
        await watch.stop()

    asyncio.run(main())


def test_a_failed_refresh_is_reported_and_the_feed_recovers(tmp_path):
    gw = gateway(tmp_path)
    live = LiveViews(gw, refresh_seconds=REFRESH)
    broken = {"now": False}
    view = gw.view

    def flaky(*args, **kwargs):
        if broken["now"]:
            raise EngineError("pinot: broker unavailable")
        return view(*args, **kwargs)

    gw.view = flaky

    async def main():
        watch = Watch(live, "plays_by_isp")
        [good] = await watch.until(1)
        broken["now"] = True
        _, error = await watch.until(2)
        assert (error.event, error.data) == ("error", {"detail": "pinot: broker unavailable"})
        late = Watch(live, "plays_by_isp")
        assert [e.event for e in await late.until(2)] == ["result", "error"]  # the last result still stands
        broken["now"] = False
        _, _, recovered = await watch.until(3)
        assert recovered.event == "result" and recovered.data.rows == good.data.rows
        await asyncio.sleep(5 * REFRESH)
        assert len(watch.events) == 3  # the error is reported once, not on every attempt
        for w in (watch, late):
            await w.stop()

    asyncio.run(main())


def test_a_feed_stops_when_its_last_subscriber_leaves(tmp_path):
    gw = counting(gateway(tmp_path))
    live = LiveViews(gw, refresh_seconds=REFRESH)

    async def main():
        watch = Watch(live, "plays_by_isp")
        await watch.until(1)
        await watch.stop()
        calls = gw.calls
        await asyncio.sleep(5 * REFRESH)
        assert (live.watched(), gw.calls) == ([], calls)

    asyncio.run(main())


@pytest.fixture
def server(tmp_path):
    """The app on a real HTTP server: streaming needs one."""
    gw = gateway(tmp_path)
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    app = create_app(gw, live=LiveViews(gw, refresh_seconds=REFRESH))
    server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning"))
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    deadline = time.monotonic() + 10
    while not server.started:
        assert time.monotonic() < deadline, "server didn't start"
        time.sleep(0.05)
    yield f"http://127.0.0.1:{port}"
    server.should_exit = True
    thread.join(timeout=10)


def test_live_views_stream_server_sent_events(server):
    with httpx.Client(base_url=server, timeout=10) as client:
        with client.stream("GET", "/views/plays_by_isp/live", params={"hours": 6}) as response:
            assert response.headers["content-type"].startswith("text/event-stream")
            lines = []
            for line in response.iter_lines():
                if not line and lines:
                    break
                lines.append(line)
        assert lines[0] == "event: result"
        streamed = json.loads(lines[1].removeprefix("data: "))
        assert streamed["rows"] == client.get("/views/plays_by_isp").json()["rows"]
        assert client.get("/views/nope/live").status_code == 404
        response = client.get("/views/plays_by_isp/live", params={"hours": "many"})
        assert (response.status_code, response.json()) == (400, {"detail": "parameter 'hours' must be a whole number, not 'many'"})


def test_one_stream_can_carry_several_views(tmp_path):
    live = LiveViews(gateway(tmp_path), refresh_seconds=REFRESH)

    async def main():
        received = []
        subscriptions = [("plays_by_isp", "plays_by_isp", {"hours": 6}), ("hourly?min_plays=7", "hourly", {"min_plays": 7})]

        async def consume():
            async for key, event in live.watch(subscriptions):
                received.append((key, event))

        task = asyncio.create_task(consume())
        deadline = time.monotonic() + 5
        while len(received) < 2:
            assert time.monotonic() < deadline
            await asyncio.sleep(0.01)
        assert sorted(key for key, _ in received) == ["hourly?min_plays=7", "plays_by_isp"]
        assert sorted(live.watched()) == [("hourly", 1), ("plays_by_isp", 1)]
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        assert live.watched() == []  # leaving the stream leaves every feed

    asyncio.run(main())


def read_events(response, count: int) -> list[tuple[str, dict]]:
    events, event = [], None
    for line in response.iter_lines():
        if line.startswith("event: "):
            event = line.removeprefix("event: ")
        elif line.startswith("data: "):
            events.append((event, json.loads(line.removeprefix("data: "))))
            if len(events) == count:
                return events
    raise AssertionError(f"the stream ended after {len(events)} events")


def test_a_dashboard_watches_its_views_over_one_connection(server):
    with httpx.Client(base_url=server, timeout=10) as client:
        params = [("view", "plays_by_isp"), ("view", "hourly?min_plays=7")]
        with client.stream("GET", "/live", params=params) as response:
            hello, *results = read_events(response, 3)
        assert hello[0] == "hello" and len(hello[1]["boot"]) == 32  # pages reload when this changes
        events = {data["subscription"]: (event, data) for event, data in results}
        assert events.keys() == {"plays_by_isp", "hourly?min_plays=7"}
        event, data = events["hourly?min_plays=7"]
        assert event == "result"
        assert data["result"]["rows"] == client.get("/views/hourly", params={"min_plays": 7}).json()["rows"]
        with client.stream("GET", "/live") as response:  # no views: only news about the project
            assert read_events(response, 1)[0][0] == "hello"
        for params, status, detail in [
            ([("view", "nope")], 404, "no view named 'nope'"),
            ([("view", "hourly?min_plays=lots")], 400, "parameter 'min_plays' must be a whole number"),
        ]:
            response = client.get("/live", params=params)
            assert response.status_code == status and detail in response.json()["detail"]
