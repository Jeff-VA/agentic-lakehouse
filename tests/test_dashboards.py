"""Dashboards in a real browser: al.js draws the example's views, and says what's wrong."""

import base64
import json
import shutil
import subprocess
import sys
from pathlib import Path

import httpx
import pytest
from fastapi.testclient import TestClient
from standin import standin_gateway
from test_api_gateway import call_mcp

from agentic_lakehouse.api_gateway import Project, create_app, create_mcp_server
from agentic_lakehouse.browser import capture, find_chrome, serving
from agentic_lakehouse.config import load_platform
from agentic_lakehouse.dashboards import check_dashboards
from agentic_lakehouse.live import LiveViews
from agentic_lakehouse.views import parse_view

EXAMPLE = Path(__file__).parents[1] / "examples" / "playback" / "config"
needs_chrome = pytest.mark.skipif(find_chrome() is None, reason="needs Chrome or Chromium")


@pytest.fixture(scope="module")
def gateway():
    return standin_gateway(load_platform(EXAMPLE))


def app(gateway, dashboards: Path = EXAMPLE / "dashboards"):
    return create_app(gateway, dashboards_dir=dashboards, live=LiveViews(gateway, refresh_seconds=1))


def test_al_js_is_served(gateway):
    with TestClient(app(gateway)) as client:
        response = client.get("/al.js")
    assert response.headers["content-type"].startswith("text/javascript")
    assert response.headers["cache-control"] == "no-cache"  # edits show up on reload
    assert "customElements.define(\"al-lines\"" in response.text


FACTS = """(async () => {
  const text = selector => [...document.querySelectorAll(selector)].map(e => e.textContent.trim());
  const facts = {
    kpis: text('al-kpi .al-value'),
    deltas: text('al-kpi .al-delta'),
    bars: [...document.querySelectorAll('al-bars')].map(b => b.querySelectorAll('.bar-row').length),
    highlighted: text('al-bars .strong').filter((t, i) => i % 2 === 0),
    caption: text('al-text')[0],
    legend: text('al-lines .al-legend button'),
    lines: document.querySelectorAll('al-lines svg path[stroke^="var(--series"]').length,
    title: text('al-lines .al-title')[0],
  };
  const select = document.querySelector('al-param[name=metric] select');
  select.value = 'p99_ttfb';
  select.dispatchEvent(new Event('change'));
  [...document.querySelectorAll('al-param[name=hours] button')].find(b => b.textContent === '24 h').click();
  await new Promise(resolve => setTimeout(resolve, 100));
  while (!al.settled()) await new Promise(resolve => setTimeout(resolve, 50));
  facts.after = { title: text('al-lines .al-title')[0], url: location.search };
  return facts;
})()"""


@needs_chrome
def test_the_example_dashboard_draws_every_view(gateway, tmp_path):
    with serving(app(gateway)) as url:
        shot = capture(f"{url}/d/playback", script=FACTS, timeout=30)
    (tmp_path / "playback.png").write_bytes(shot.png)
    assert shot.settled and shot.problems == []
    facts = shot.value
    assert len(facts["kpis"]) == 3 and all(value not in ("…", "—") for value in facts["kpis"])
    assert len(facts["deltas"]) == 3  # each compared with the hour before
    assert facts["bars"] == [6, 6, 6]  # one bar per ISP
    assert set(facts["highlighted"]) == {"cox"}  # the simulator's degraded ISP, highlighted on all three
    assert facts["caption"] == "Sorted by rebuffer rate. Highlighted: cox, the highest in the last hour."
    assert facts["legend"] == ["comcast", "verizon", "att", "spectrum", "cox", "frontier"]  # the colors="..." order
    assert facts["lines"] == 6
    assert facts["title"] == "Rebuffer rate"  # the column's label, from the ontology
    # The metric select and range buttons set page parameters: the chart and the URL follow.
    assert facts["after"] == {"title": "p99 time to first byte (ms)", "url": "?metric=p99_ttfb&hours=24"}


@needs_chrome
def test_the_dashboard_says_what_is_wrong_with_it(gateway, tmp_path):
    shutil.copy(EXAMPLE / "dashboards" / "playback.html", tmp_path / "broken.html")
    page = (tmp_path / "broken.html").read_text()
    page = page.replace('view="isp_now" category="isp_provider" value="p99_ttfb"', 'view="isp_nwo" category="isp_provider" value="p99_ttfb"')
    page = page.replace('y="$metric"', 'y="rebuffer_rate"')
    (tmp_path / "broken.html").write_text(page)
    with serving(app(gateway, tmp_path)) as url:
        shot = capture(f"{url}/d/broken", timeout=30)
    assert shot.settled
    assert shot.problems == [
        'al-bars view=isp_nwo: No view named "isp_nwo". Views: isp_hourly, isp_now, quality_overall.',
        'al-lines view=isp_hourly: View "isp_hourly" has no column "rebuffer_rate".',
    ]


# ---------------------------------------------------------------- checking without a browser

BROKEN = """<!doctype html>
<title>Broken</title>
<al-param name="metric" kind="select" options="rebuffer_incidence_rate,rebuffer_rate"></al-param>
<al-param name="hours" options="24,9999" value="12"></al-param>
<al-line view="isp_hourly" x="hour" y="p99_ttfb"></al-line>
<al-bars view="isp_nwo" category="isp_provider" value="p99_ttfb"></al-bars>
<al-lines view="isp_hourly" x="hour" y="$metric" series="isp"></al-lines>
<al-kpi view="isp_now" value="isp_provider"></al-kpi>
<al-lines view="isp_now" x="isp_provider" y="p99_ttfb"></al-lines>
<al-kpi view="quality_overall" value="p99_ttfb" compare="from_minutes_ago=abc"></al-kpi>
<al-text view="isp_now">Worst: {isp}</al-text>
<al-bars view="isp_now" category="isp_provider" value="p99_ttfb" sort="up"></al-bars>
<al-table view="isp_now" columns="isp_provider,$nope"></al-table>
<al-freshness dataset="telemetri"></al-freshness>
<al-kpi view="needs_day" value="plays"></al-kpi>
"""


def test_the_example_dashboard_passes_the_check():
    platform = load_platform(EXAMPLE)
    assert check_dashboards(EXAMPLE / "dashboards", platform.views, ["telemetry"]) == []


def test_the_check_finds_each_mistake_with_its_line(tmp_path):
    platform = load_platform(EXAMPLE)
    needs_day = parse_view(
        "needs_day",
        "/*\ndescription: d\nparams: {day: {type: timestamp}}\n*/\n"
        "SELECT unique_sessions AS plays FROM playback_telemetry WHERE event_ts >= :day",
        platform.ontology,
        platform.table_schemas(),
    )
    (tmp_path / "broken.html").write_text(BROKEN)
    problems = [str(p).removeprefix(f"{tmp_path.name}/broken.html:") for p in check_dashboards(tmp_path, platform.views | {"needs_day": needs_day}, ["telemetry"])]
    assert problems == [
        '1: <script> the page uses al-* tags but never loads them: add <script type="module" src="/al.js"></script>',
        '4: <al-param name="hours"> value="12" isn\'t one of its options (24, 9999)',
        "4: <al-param name=\"hours\"> option \"9999\" must be at most 2160, not 9999, as view isp_hourly's parameter hours",
        '5: <al-line view="isp_hourly"> no such tag; did you mean <al-lines>? Tags: '
        "al-bars, al-drift, al-freshness, al-gates, al-kpi, al-lines, al-live, al-param, al-table, al-text, al-theme",
        '6: <al-bars view="isp_nwo"> no view named "isp_nwo"; did you mean "isp_now"? views: isp_hourly, isp_now, quality_overall, needs_day',
        '7: <al-lines view="isp_hourly"> y: view isp_hourly has no column "rebuffer_rate"; did you mean "rebuffer_incidence_rate"? '
        "columns: hour, isp_provider, rebuffer_incidence_rate, p99_ttfb, unique_sessions, avg_bitrate_kbps",
        '7: <al-lines view="isp_hourly"> series: view isp_hourly has no column "isp"; '
        "columns: hour, isp_provider, rebuffer_incidence_rate, p99_ttfb, unique_sessions, avg_bitrate_kbps",
        '8: <al-kpi view="isp_now"> value: column "isp_provider" holds string values; it needs numbers',
        '9: <al-lines view="isp_now"> x: column "isp_provider" holds string values; lines run over a time column',
        "10: <al-kpi view=\"quality_overall\"> compare: parameter 'from_minutes_ago' must be a whole number, not 'abc'",
        '11: <al-text view="isp_now"> text: view isp_now has no column "isp"; columns: isp_provider, rebuffer_incidence_rate, p99_ttfb, unique_sessions',
        '12: <al-bars view="isp_now"> sort="up" must be one of asc, desc, view',
        '13: <al-table view="isp_now"> $nope names no page parameter: add <al-param name="nope" ...> or the value itself',
        '14: <al-freshness dataset="telemetri"> no dataset named "telemetri"; datasets: telemetry',
        '15: <al-kpi view="needs_day"> view needs_day needs parameter day: set it in params="day=..." or with a <al-param name="day">',
    ]


def test_run_py_check_reports_problems_and_exits_nonzero(tmp_path):
    root = Path(__file__).parents[1]
    project = tmp_path / "config"
    shutil.copytree(EXAMPLE, project)

    def check():
        return subprocess.run([sys.executable, "-m", "agentic_lakehouse", "--config", str(project), "check"], capture_output=True, text=True, cwd=root)

    ok = check()
    assert (ok.returncode, ok.stdout.strip()) == (0, f"{project}: 3 views, 1 dashboard, 0 problems")
    page = project / "dashboards" / "playback.html"
    page.write_text(page.read_text().replace('value="p99_ttfb" highlight', 'value="p99" highlight'))
    broken = check()
    assert broken.returncode == 1
    assert 'dashboards/playback.html:46: <al-bars view="isp_now"> value: view isp_now has no column "p99"' in broken.stdout
    (project / "views" / "isp_now.sql").write_text("/* description: d */ SELECT nope FROM gold.hourly_isp_quality")
    assert "views/isp_now.sql: Column 'nope' could not be resolved" in check().stdout


# ---------------------------------------------------------------- the agent's tools

def project(tmp_path) -> Project:
    shutil.copytree(EXAMPLE, tmp_path / "config")
    return Project(tmp_path / "config", gateway=standin_gateway)


def test_mcp_checks_the_project_as_it_is_on_disk(tmp_path):
    server = create_mcp_server(standin_gateway(load_platform(EXAMPLE)), project=project(tmp_path))
    result = call_mcp(server, "call_tool", "check_dashboards", {}).structured_content
    assert (result["config_error"], result["views"]) == (None, ["isp_hourly", "isp_now", "quality_overall"])
    [dashboard] = result["dashboards"]
    assert (dashboard["name"], dashboard["title"], dashboard["url"], dashboard["problems"]) == ("playback", "Playback Quality", "/d/playback", [])
    assert dashboard["views"] == ["quality_overall", "isp_now", "isp_hourly"]
    (tmp_path / "config" / "views" / "isp_now.sql").write_text("SELECT 1")  # an edit since the server started
    result = call_mcp(server, "call_tool", "check_dashboards", {}).structured_content
    assert "views/isp_now.sql: start the file with a /* ... */ comment" in result["config_error"]


def test_the_build_dashboard_prompt_carries_the_workflow_and_tag_reference(tmp_path):
    server = create_mcp_server(standin_gateway(load_platform(EXAMPLE)), project=project(tmp_path))
    prompt = call_mcp(server, "get_prompt", "build_dashboard", {"goal": "Show bitrate by device"})
    text = prompt.messages[0].content.text
    assert text.startswith("Goal: Show bitrate by device")
    assert "check_dashboards after every edit" in text
    assert "<al-lines view x y [series] [colors] [label] [height]>" in text  # from the top of al.js


@needs_chrome
def test_mcp_screenshots_a_dashboard(tmp_path):
    server = create_mcp_server(standin_gateway(load_platform(EXAMPLE)), project=project(tmp_path))
    result = call_mcp(server, "call_tool", "screenshot_dashboard", {"name": "playback", "width": 400})
    assert result.is_error is False
    text, image = result.content
    assert text.text.startswith("Dashboard playback (/d/playback), light, 400 px wide.\nEvery tag drew its data or an error.")
    assert "Problems the page shows: none" in text.text
    assert (image.type, image.mime_type) == ("image", "image/png")
    assert base64.b64decode(image.data).startswith(b"\x89PNG")
    missing = call_mcp(server, "call_tool", "screenshot_dashboard", {"name": "nope"})
    assert missing.is_error and "no dashboard named 'nope'. Dashboards: playback" in missing.content[0].text


def test_open_pages_hear_about_edits(tmp_path):
    """Edit a dashboard: pages reload. Break a view: pages say what broke and keep the last good
    version. Fix it: they reload."""
    proj = project(tmp_path)
    gateway = standin_gateway(load_platform(proj.config_dir))
    with serving(create_app(gateway, dashboards_dir=proj.dashboards_dir, project=proj)) as url, httpx.Client(timeout=15) as client:
        with client.stream("GET", f"{url}/live") as response:
            lines = response.iter_lines()

            def next_event() -> tuple[str, dict]:
                event = None
                for line in lines:
                    if line.startswith("event: "):
                        event = line.removeprefix("event: ")
                    elif line.startswith("data: "):
                        return event, json.loads(line.removeprefix("data: "))
                raise AssertionError("the stream ended")

            assert next_event()[0] == "hello"
            page = proj.dashboards_dir / "playback.html"
            page.write_text(page.read_text().replace("Playback quality", "Playback health"))
            assert next_event() == ("reload", {})
            view = proj.config_dir / "views" / "isp_now.sql"
            good = view.read_text()
            view.write_text(good.replace("isp_provider,", "isp_providr,"))
            event, data = next_event()
            assert event == "problem" and "views/isp_now.sql: 'isp_providr' isn't a metric or dimension" in data["detail"]
            assert "isp_provider" in [c.name for c in gateway.views["isp_now"].columns]  # the last good version
            view.write_text(good.replace("unique_sessions", "unique_sessions, avg_bitrate_kbps"))
            assert next_event() == ("reload", {})
    assert "avg_bitrate_kbps" in [c.name for c in gateway.views["isp_now"].columns]
