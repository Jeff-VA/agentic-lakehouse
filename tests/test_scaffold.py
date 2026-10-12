"""agentic-lakehouse init: a new project that runs, checks out and tests itself from the start."""

import subprocess
import sys
from pathlib import Path

import pytest
import yaml

from agentic_lakehouse.api_gateway import Project, create_app
from agentic_lakehouse.browser import capture, find_chrome, serving
from agentic_lakehouse.config import load_platform
from agentic_lakehouse.scaffold import TEMPLATE, create_project, engine_version, release_requirement
from agentic_lakehouse.testing import standin_gateway

ROOT = Path(__file__).parents[1]


@pytest.fixture(scope="module")
def project(tmp_path_factory) -> Path:
    path = tmp_path_factory.mktemp("projects") / "demo-shop"
    create_project(path)
    return path


def test_a_new_project_has_everything_and_pins_this_release(project):
    files = {str(p.relative_to(project)) for p in project.rglob("*") if p.is_file()}
    assert {
        ".gitignore", ".dockerignore", ".mcp.json", ".github/workflows/ci.yml", "AGENTS.md", "CLAUDE.md",
        "README.md", "Dockerfile", "requirements.txt", "pytest.ini", "tests/test_project.py",
        "config/ontology.yaml", "config/views/activity_now.sql", "config/dashboards/overview.html",
        "docker/docker-compose.yml", "docker/config/deployment.yaml", "docker/trino/catalog/iceberg.properties",
    } <= files
    assert all("@@" not in (project / f).read_text() for f in files)  # every placeholder filled
    assert release_requirement(engine_version()) in (project / "requirements.txt").read_text()
    assert f"/releases/download/v{engine_version()}/agentic_lakehouse-{engine_version()}-py3-none-any.whl" in release_requirement(engine_version())
    stack = yaml.safe_load((project / "docker" / "docker-compose.yml").read_text())
    assert (stack["name"], stack["services"]["api"]["image"]) == ("demo-shop", "demo-shop-app:latest")
    assert "<title>Demo shop</title>" in (project / "config" / "dashboards" / "overview.html").read_text()


def test_a_new_project_checks_out_and_every_view_returns_rows(project):
    assert Project(project / "config").check().ok
    gateway = standin_gateway(load_platform(project / "config"))
    assert {name: bool(gateway.view(name).rows) for name in gateway.views} == {
        "activity_now": True, "activity_total": True, "hourly_activity": True,
    }


@pytest.mark.skipif(find_chrome() is None, reason="needs Chrome or Chromium")
def test_a_new_projects_dashboard_draws(project):
    gateway = standin_gateway(load_platform(project / "config"))
    with serving(create_app(gateway, dashboards_dir=project / "config" / "dashboards")) as url:
        shot = capture(f"{url}/d/overview", timeout=30)
    assert shot.settled and shot.problems == []


def test_a_new_projects_own_tests_pass(project):
    """What the project's CI will run, with the engine installed here."""
    result = subprocess.run([sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider"], cwd=project, capture_output=True, text=True)
    assert result.returncode == 0, result.stdout[-2000:]
    assert "passed" in result.stdout


def test_init_refuses_a_folder_with_files_or_a_bad_name(tmp_path):
    (tmp_path / "taken").mkdir()
    (tmp_path / "taken" / "notes.txt").write_text("mine")
    with pytest.raises(FileExistsError, match="isn't empty"):
        create_project(tmp_path / "taken")
    with pytest.raises(ValueError, match="lowercase letters, digits and hyphens"):
        create_project(tmp_path / "My Project")


def test_init_on_the_command_line(tmp_path):
    result = subprocess.run(
        [sys.executable, "-m", "agentic_lakehouse", "init", str(tmp_path / "web-traffic")], capture_output=True, text=True, cwd=ROOT
    )
    assert result.returncode == 0, result.stdout
    assert result.stdout.startswith(f"Created {tmp_path / 'web-traffic'}:")
    assert (tmp_path / "web-traffic" / "config" / "ontology.yaml").exists()


def test_the_template_stack_matches_the_engines():
    """The engine develops against docker/; projects get the same stack from the template."""
    engine = yaml.safe_load((ROOT / "docker" / "docker-compose.yml").read_text())
    template = yaml.safe_load((TEMPLATE / "docker" / "docker-compose.yml").read_text())
    for stack in (engine, template):
        stack.pop("name")
        stack.pop("x-app")
        stack["services"]["kafka"]["environment"].pop("CLUSTER_ID")
        for service in stack["services"].values():
            if service.get("build"):  # the app's processes, built from each repository's Dockerfile
                service.pop("image")
    assert template == engine
    for name in ("docker/config/deployment.yaml", "docker/trino/catalog/iceberg.properties"):
        assert (TEMPLATE / name).read_text() == (ROOT / name).read_text(), name
    assert (TEMPLATE / "config" / "deployment.yaml").read_text() == (ROOT / "config" / "deployment.yaml").read_text()
