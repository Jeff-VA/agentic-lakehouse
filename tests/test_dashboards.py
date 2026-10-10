"""Dashboards in a real browser: male.js draws the example's views, and says what's wrong."""

import shutil
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from standin import serving, standin_gateway

from core.api_gateway import create_app
from core.browser import capture, find_chrome
from core.config import load_platform
from core.live import LiveViews

EXAMPLE = Path(__file__).parents[1] / "examples" / "playback" / "config"
needs_chrome = pytest.mark.skipif(find_chrome() is None, reason="needs Chrome or Chromium")


@pytest.fixture(scope="module")
def gateway():
    return standin_gateway(load_platform(EXAMPLE))


def app(gateway, dashboards: Path = EXAMPLE / "dashboards"):
    return create_app(gateway, dashboards_dir=dashboards, live=LiveViews(gateway, refresh_seconds=1))


def test_male_js_is_served(gateway):
    with TestClient(app(gateway)) as client:
        response = client.get("/male.js")
    assert response.headers["content-type"].startswith("text/javascript")
    assert response.headers["cache-control"] == "no-cache"  # edits show up on reload
    assert "customElements.define(\"male-lines\"" in response.text


FACTS = """(async () => {
  const text = selector => [...document.querySelectorAll(selector)].map(e => e.textContent.trim());
  const facts = {
    kpis: text('male-kpi .male-value'),
    deltas: text('male-kpi .male-delta'),
    bars: [...document.querySelectorAll('male-bars')].map(b => b.querySelectorAll('.bar-row').length),
    highlighted: text('male-bars .strong').filter((t, i) => i % 2 === 0),
    caption: text('male-text')[0],
    legend: text('male-lines .male-legend button'),
    lines: document.querySelectorAll('male-lines svg path[stroke^="var(--series"]').length,
    title: text('male-lines .male-title')[0],
  };
  const select = document.querySelector('male-param[name=metric] select');
  select.value = 'p99_ttfb';
  select.dispatchEvent(new Event('change'));
  [...document.querySelectorAll('male-param[name=hours] button')].find(b => b.textContent === '24 h').click();
  await new Promise(resolve => setTimeout(resolve, 100));
  while (!male.settled()) await new Promise(resolve => setTimeout(resolve, 50));
  facts.after = { title: text('male-lines .male-title')[0], url: location.search };
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
        'male-bars view=isp_nwo: No view named "isp_nwo". Views: isp_hourly, isp_now, quality_overall.',
        'male-lines view=isp_hourly: View "isp_hourly" has no column "rebuffer_rate".',
    ]
