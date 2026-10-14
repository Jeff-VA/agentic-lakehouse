"""DuckDB as a SQL client: the setup `agentic-lakehouse duckdb` and `init` write, and that it runs.

The macros' answers from /views/<name> and /sql in Parquet are tested in test_duckdb_end_to_end.py.
"""

import sys
from dataclasses import replace
from pathlib import Path

import duckdb
import pytest
from fastapi import FastAPI

from agentic_lakehouse import cli
from agentic_lakehouse.api_gateway import create_app
from agentic_lakehouse.browser import serving
from agentic_lakehouse.config import Platform, load_platform
from agentic_lakehouse.duckdb_client import render
from agentic_lakehouse.scaffold import create_project
from agentic_lakehouse.testing import standin_gateway

EXAMPLE = Path(__file__).parents[1] / "examples" / "playback" / "config"


@pytest.fixture(scope="module")
def platform() -> Platform:
    return load_platform(EXAMPLE)


@pytest.fixture
def con():
    """An in-memory DuckDB with the extensions setup.sql installs."""
    con = duckdb.connect()
    try:
        con.execute("INSTALL httpfs; INSTALL iceberg")  # a no-op once they're in DuckDB's folder
    except duckdb.Error as e:
        pytest.skip(f"DuckDB's httpfs and iceberg extensions aren't installed, and can't be downloaded here: {e}")
    yield con
    con.close()


def run_setup(con: duckdb.DuckDBPyConnection, platform: Platform, gateway_url: str) -> None:
    """Run setup.sql, then session.sql without its ATTACH (there's no Iceberg catalog in tests). The
    secret isn't persistent: a persistent one would be written into ~/.duckdb."""
    files = render(platform, gateway_url, persistent_secret=False)
    con.execute(files["setup.sql"])
    con.execute("\n".join(line for line in files["session.sql"].splitlines() if not line.startswith("ATTACH")))


def with_s3(platform: Platform, **s3) -> Platform:
    deployment = platform.deployment
    return replace(platform, deployment=deployment.model_copy(update={"s3": deployment.s3.model_copy(update=s3)}))


def test_the_setup_uses_the_deployment(platform):
    files = render(platform)
    setup, session = files["setup.sql"], files["session.sql"]
    assert "\nCREATE OR REPLACE PERSISTENT SECRET playback_s3 (\n" in setup  # named for the project's folder
    for line in ["KEY_ID 'lakehouse',", "SECRET 'lakehouse-local-secret',", "ENDPOINT '127.0.0.1:9100',", "USE_SSL false,"]:
        assert f"\n    {line}\n" in setup
    assert "read_parquet('http://127.0.0.1:8000/views/' || name || '?format=parquet&errors=inline&' || params)" in setup
    assert "read_parquet('http://127.0.0.1:8000/sql?format=parquet&errors=inline&q=' || url_encode(sql))" in setup
    assert "ENDPOINT 'http://127.0.0.1:8181'" in session
    assert "localhost" not in setup + session  # deployment.yaml says localhost, which can resolve to IPv6
    # DBeaver's bootstrap queries take one statement each
    statements = [line for line in session.splitlines() if not line.startswith("--")]
    assert [len(duckdb.extract_statements(line)) for line in statements] == [1, 1]
    assert statements[0] == "SET force_download = true;"
    assert statements[1].startswith("ATTACH IF NOT EXISTS 'warehouse' AS lake (TYPE iceberg, ")


def test_the_setup_follows_other_deployments(platform):
    elsewhere = with_s3(platform, endpoint="https://s3.example.com", secret_key="it's")
    setup = render(elsewhere, "http://localhost:9000/", persistent_secret=False)["setup.sql"]
    assert "\nCREATE OR REPLACE SECRET playback_s3 (\n" in setup
    for line in ["ENDPOINT 's3.example.com',", "USE_SSL true,", "SECRET 'it''s',"]:
        assert f"\n    {line}\n" in setup
    assert "read_json('http://127.0.0.1:9000/views'," in setup
    with pytest.raises(ValueError, match="s3.endpoint in deployment.yaml should be a URL like http://127.0.0.1:9100"):
        render(with_s3(platform, endpoint="minio:9000"))


def test_the_same_config_gives_the_same_text(platform):
    assert render(platform) == render(load_platform(EXAMPLE))
    assert render(platform, "http://127.0.0.1:8001") == render(load_platform(EXAMPLE), "http://127.0.0.1:8001")


def test_the_command_writes_the_files(platform, tmp_path, monkeypatch, capsys):
    out = tmp_path / "duckdb"
    argv = ["agentic-lakehouse", "--config", str(EXAMPLE), "duckdb", "--gateway", "http://127.0.0.1:8001", "--out", str(out)]
    monkeypatch.setattr(sys, "argv", argv)
    cli.main()
    assert {p.name: p.read_text() for p in out.iterdir()} == render(platform, "http://127.0.0.1:8001")
    printed = capsys.readouterr().out
    assert f"duckdb my.duckdb < {out / 'setup.sql'}" in printed
    assert "Bootstrap queries" in printed


def test_init_writes_them_for_the_new_project(tmp_path):
    files = create_project(tmp_path / "demo-shop")
    project = tmp_path / "demo-shop"
    folder = project / "tools" / "duckdb"
    assert {folder / "setup.sql", folder / "session.sql"} <= set(files)
    assert {p.name: p.read_text() for p in folder.iterdir()} == render(load_platform(project / "config"))
    assert "\nCREATE OR REPLACE PERSISTENT SECRET demo_shop_s3 (\n" in (folder / "setup.sql").read_text()
    assert {"*.duckdb", "*.duckdb.wal"} <= set((project / ".gitignore").read_text().splitlines())


def test_the_setup_runs_in_duckdb_against_the_gateway(platform, con):
    with serving(create_app(standin_gateway(platform))) as url:
        run_setup(con, platform, url)
        views = con.execute("SELECT name, answers_from, params FROM gateway_views()").fetchall()
    assert views == [
        ("isp_hourly", "tables (history)", "hours=48"),
        ("isp_now", "live + history", "minutes=60"),
        ("quality_overall", "live + history", "from_minutes_ago=60&to_minutes_ago=0"),
    ]
    persistent, secret = con.execute("SELECT persistent, secret_string FROM duckdb_secrets() WHERE name = 'playback_s3'").fetchone()
    assert not persistent
    assert "endpoint=127.0.0.1:9100;" in secret and "key_id=lakehouse;" in secret and "url_style=path;" in secret


def test_gateway_views_says_what_each_view_answers_from(platform, con):
    """Composed views, and catalogs that leave nothing for DuckDB to infer types from: no views yet,
    a view without parameters, defaults of several types."""
    views = []
    app = FastAPI()
    app.get("/views")(lambda: {"views": views})
    with serving(app) as url:
        run_setup(con, platform, url)  # creating gateway_views() reads the catalog: here, an empty one
        assert con.execute("SELECT * FROM gateway_views()").fetchall() == []
        views += [
            {"name": "top_wikis", "source": "model", "reads": ["edits"], "params": [], "description": "Top wikis.\n"},
            {"name": "hot", "source": "composed", "reads": ["edits", "gold.baselines"], "description": "Hot.", "params": [
                {"name": "hours", "default": 24}, {"name": "wiki", "default": None}, {"name": "bots", "default": False}]},
            {"name": "daily", "source": "composed", "reads": ["gold.daily", "gold.baselines"], "description": "Days.", "params": [
                {"name": "share", "default": 0.5}, {"name": "label", "default": "en wiki"}]},
        ]
        rows = con.execute("SELECT * FROM gateway_views()").fetchall()
    assert rows == [
        ("daily", "tables (history)", "share=0.5&label=en wiki", "Days."),
        ("hot", "live + history", "hours=24&wiki=&bots=false", "Hot."),
        ("top_wikis", "live + history", "", "Top wikis."),
    ]
