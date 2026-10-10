"""Headless Chrome: what a dashboard looks like, and what it reports, once its data has arrived.

A dashboard's live connection never goes idle, so Chrome's own --screenshot
can't tell when the page is ready. This drives Chrome over the DevTools
protocol instead: load the page, wait until male.js says every tag has drawn
its data or an error, then capture the page and the errors it shows. Tests
use it, and so does the MCP tool that lets an agent look at what it built.
"""

import asyncio
import base64
import contextlib
import json
import os
import re
import shutil
import socket
import subprocess
import tempfile
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path

import httpx
import uvicorn
import websockets

CANDIDATES = [
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
    "/Applications/Chromium.app/Contents/MacOS/Chromium",
    "google-chrome",
    "google-chrome-stable",
    "chromium",
    "chromium-browser",
    "chrome",
]

# True once every tag on the page has drawn data or an error (see settled() in male.js);
# pages without male.js are ready when they've loaded.
READY = "document.readyState === 'complete' && (!window.male || window.male.settled())"
# What the page shows, by tag: errors (a missing view or column, a failed refresh), and notes
# about the data (a result that covers less than asked).
_REPORT = """[...document.querySelectorAll('{selector}')].map(e => {{
  const host = e.closest('male-kpi, male-bars, male-lines, male-table, male-text, male-freshness, male-gates, male-drift');
  const view = host?.getAttribute('view');
  return `${{host ? host.tagName.toLowerCase() + (view ? ` view=${{view}}` : '') : 'page'}}: ${{e.textContent.trim()}}`;
}})"""
PROBLEMS = _REPORT.format(selector=".male-error")
NOTES = _REPORT.format(selector=".male-notes:not(.male-error)")


class BrowserError(RuntimeError):
    """Chrome isn't installed, didn't start, or the page didn't settle in time."""


@dataclass
class Capture:
    png: bytes
    problems: list[str] = field(default_factory=list)  # errors the page shows, by tag
    notes: list[str] = field(default_factory=list)  # caveats about the data, by tag
    settled: bool = True  # False: the page was captured at the timeout, still loading
    value: object = None  # what `script` returned


@contextlib.contextmanager
def serving(app):
    """An ASGI app on a local HTTP server for as long as the block runs; yields its base URL."""
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning"))
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    deadline = time.monotonic() + 10
    while not server.started:
        if time.monotonic() > deadline or not thread.is_alive():
            raise BrowserError("the local server didn't start")
        time.sleep(0.05)
    try:
        yield f"http://127.0.0.1:{port}"
    finally:
        server.should_exit = True
        thread.join(timeout=10)


def find_chrome() -> str | None:
    """Chrome or Chromium: $CHROME if set, else the usual install locations."""
    for candidate in [os.environ.get("CHROME"), *CANDIDATES]:
        if candidate and (Path(candidate).is_file() or shutil.which(candidate)):
            return shutil.which(candidate) or candidate
    return None


def capture(
    url: str,
    *,
    width: int = 1280,
    height: int = 900,
    theme: str | None = None,
    timeout: float = 20,
    script: str | None = None,
) -> Capture:
    """Load `url` in headless Chrome and capture the whole page once it settles.

    `theme` ("light" or "dark") sets prefers-color-scheme. `script`, a JavaScript
    expression run once the page settles (a promise is awaited), can click and
    read the page; the page settles again before the capture, and Capture.value
    is what the script returned. Raises BrowserError.
    """
    return asyncio.run(_capture(url, width=width, height=height, theme=theme, timeout=timeout, script=script))


async def _capture(url: str, *, width: int, height: int, theme: str | None, timeout: float, script: str | None) -> Capture:
    chrome = find_chrome()
    if chrome is None:
        raise BrowserError("Chrome isn't installed; install it or set CHROME to its path")
    with tempfile.TemporaryDirectory(prefix="male-chrome-") as profile:
        # Chrome refuses to sandbox as root, and CI runners often can't give it the namespaces it needs.
        root = hasattr(os, "geteuid") and os.geteuid() == 0
        sandbox = ["--no-sandbox"] if root or os.environ.get("CI") else []
        process = subprocess.Popen(
            [
                chrome,
                *sandbox,
                "--headless=new",
                "--disable-gpu",
                "--hide-scrollbars",
                "--no-first-run",
                "--no-default-browser-check",
                "--remote-debugging-port=0",
                f"--user-data-dir={profile}",
                f"--window-size={width},{height}",
                "about:blank",
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
        )
        try:
            port = await asyncio.wait_for(asyncio.to_thread(_devtools_port, process), timeout=15)
            async with httpx.AsyncClient() as client:
                targets = (await client.get(f"http://127.0.0.1:{port}/json/list")).json()
            page = next(t for t in targets if t["type"] == "page")
            async with websockets.connect(page["webSocketDebuggerUrl"], max_size=64 * 2**20) as socket:
                return await _Session(socket).capture(url, width, height, theme, timeout, script)
        except (OSError, TimeoutError, StopIteration, websockets.WebSocketException) as e:
            raise BrowserError(f"headless Chrome failed: {e!r}") from e
        finally:
            process.kill()
            process.wait()


def _devtools_port(process: subprocess.Popen) -> int:
    for line in process.stderr:
        if match := re.search(r"DevTools listening on ws://[^:]+:(\d+)/", line):
            return int(match[1])
    raise BrowserError("Chrome exited before it started listening")


class _Session:
    def __init__(self, socket):
        self.socket = socket
        self.ids = 0

    async def call(self, method: str, **params) -> dict:
        self.ids += 1
        await self.socket.send(json.dumps({"id": self.ids, "method": method, "params": params}))
        while True:
            message = json.loads(await self.socket.recv())
            if message.get("id") == self.ids:
                if "error" in message:
                    raise BrowserError(f"{method}: {message['error'].get('message')}")
                return message["result"]

    async def evaluate(self, expression: str):
        result = await self.call("Runtime.evaluate", expression=expression, returnByValue=True, awaitPromise=True)
        if "exceptionDetails" in result:
            details = result["exceptionDetails"]
            raise BrowserError(f"script failed: {details.get('exception', {}).get('description') or details.get('text')}")
        return result["result"].get("value")

    async def settle(self, timeout: float) -> bool:
        deadline = asyncio.get_running_loop().time() + timeout
        while asyncio.get_running_loop().time() < deadline:
            if await self.evaluate(READY):
                await asyncio.sleep(0.2)  # let the last redraw paint
                return True
            await asyncio.sleep(0.2)
        return False

    async def capture(self, url: str, width: int, height: int, theme: str | None, timeout: float, script: str | None) -> Capture:
        await self.call("Emulation.setDeviceMetricsOverride", width=width, height=height, deviceScaleFactor=1, mobile=False)
        if theme:
            await self.call("Emulation.setEmulatedMedia", features=[{"name": "prefers-color-scheme", "value": theme}])
        await self.call("Page.navigate", url=url)
        settled = await self.settle(timeout)
        value = None
        if script:
            value = await self.evaluate(script)
            settled = await self.settle(timeout)
        content = (await self.call("Page.getLayoutMetrics"))["cssContentSize"]
        full_height = max(height, int(content["height"]))
        await self.call("Emulation.setDeviceMetricsOverride", width=width, height=full_height, deviceScaleFactor=1, mobile=False)
        shot = await self.call("Page.captureScreenshot", format="png", captureBeyondViewport=True)
        problems = await self.evaluate(PROBLEMS) or []
        notes = await self.evaluate(NOTES) or []
        return Capture(base64.b64decode(shot["data"]), problems, notes, settled, value)
