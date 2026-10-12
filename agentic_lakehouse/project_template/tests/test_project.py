"""The project's config, views and dashboards, checked without the Docker stack."""

from pathlib import Path

import pytest

from agentic_lakehouse.api_gateway import Project, create_app
from agentic_lakehouse.browser import capture, find_chrome, serving
from agentic_lakehouse.config import load_platform
from agentic_lakehouse.dashboards import dashboard_files
from agentic_lakehouse.testing import standin_gateway

CONFIG = Path(__file__).parents[1] / "config"


@pytest.fixture(scope="module")
def gateway():
    return standin_gateway(load_platform(CONFIG))


def test_the_config_views_and_dashboards_check_out():
    result = Project(CONFIG).check()
    assert result.config_error is None
    assert {d.name: d.problems for d in result.dashboards} == {d.name: [] for d in result.dashboards}


def test_every_view_returns_rows(gateway):
    for name in gateway.views:
        assert gateway.view(name).rows, f"view {name} returned no rows on simulated events"


@pytest.mark.skipif(find_chrome() is None, reason="needs Chrome or Chromium")
@pytest.mark.parametrize("name", sorted(dashboard_files(CONFIG / "dashboards")))
def test_every_dashboard_draws_without_problems(gateway, name):
    with serving(create_app(gateway, dashboards_dir=CONFIG / "dashboards")) as url:
        shot = capture(f"{url}/d/{name}", timeout=30)
    assert shot.settled and shot.problems == []
