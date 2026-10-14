"""Project macros in views and steps (docs/design/project-macros.md): their SQL is rendered with
Trino's version of each macro before anything parses it, the catalog lists the macros, and an
edited macro file reloads the views, the queries and open pages. Ad-hoc SQL can't call them."""

import json
import shutil
from collections.abc import Iterator
from datetime import timedelta
from pathlib import Path

import httpx
import pytest
import yaml
from fastapi.testclient import TestClient
from test_api_gateway import call_mcp
from test_compiler import ONTOLOGY
from test_router import router
from test_view_api import warehouse
from test_views import NOW, TABLES

from agentic_lakehouse.api_gateway import Gateway, Project, create_app, create_mcp_server
from agentic_lakehouse.browser import serving
from agentic_lakehouse.compiler import Filter, SqlCompiler
from agentic_lakehouse.config import ConfigError, load_platform
from agentic_lakehouse.macros import ProjectMacros
from agentic_lakehouse.ontology import Ontology
from agentic_lakehouse.router import Router
from agentic_lakehouse.scaffold import create_project
from agentic_lakehouse.testing import standin_gateway
from agentic_lakehouse.views import ComposedSource, ModelSource, TableSource, View, ViewError, load_views, parse_view

EXAMPLE = Path(__file__).parents[1] / "examples" / "playback" / "config"

DEFAULT = """\
{# The ISPs most plays come through. #}
{% macro big_isps() %}'comcast', 'att'{% endmacro %}

{# The last `hours` hours of plays. #}
{% macro last_hours(hours) %}ts >= now() - INTERVAL '{{ hours }}' HOUR{% endmacro %}

{# An edit by a person with an account: not a bot, not anonymous. #}
{% macro human_edit() %}is_bot = false AND is_anonymous = false{% endmacro %}

{# The host of a URL, e.g. en.wikipedia.org. #}
{% macro url_host(column) %}regexp_extract({{ column }}, '^https?://([^/]+)', 1){% endmacro %}
"""
TRINO = "{% macro url_host(column) %}url_extract_host({{ column }}){% endmacro %}\n"
EDITS = {"raw.edits": {"ts": "TIMESTAMP", "url": "VARCHAR", "is_bot": "BOOLEAN", "is_anonymous": "BOOLEAN"}}


@pytest.fixture
def ontology(tmp_path) -> Ontology:
    """test_compiler's ontology, with the project macros above."""
    (tmp_path / "macros").mkdir()
    (tmp_path / "macros" / "default.sql").write_text(DEFAULT)
    (tmp_path / "macros" / "trino.sql").write_text(TRINO)
    ontology = ONTOLOGY.model_copy()
    ontology._macros = ProjectMacros(tmp_path / "macros")
    return ontology


def view(ontology: Ontology, sql: str) -> View:
    return parse_view("test", f"/*\ndescription: A test view.\n*/\n{sql}", ontology, TABLES | EDITS)


# ---------------------------------------------------------------- views


def test_a_view_over_tables_calls_trinos_version_of_each_macro(ontology):
    v = view(ontology, "SELECT {{ url_host('url') }} AS host, count(*) AS edits FROM raw.edits WHERE {{ human_edit() }} GROUP BY 1")
    assert isinstance(v.source, TableSource) and v.reads == ["raw.edits"]
    assert [c.name for c in v.columns] == ["host", "edits"]
    assert v.sql.startswith("SELECT {{ url_host('url') }} AS host")  # as written: what people and agents read
    assert v.source.sql({}, dialect="trino") == (  # project/trino.sql's url_host, not the one for every engine
        "SELECT URL_EXTRACT_HOST(url) AS host, COUNT(*) AS edits FROM raw.edits WHERE is_bot = FALSE AND is_anonymous = FALSE GROUP BY 1"
    )


def test_a_view_over_a_model_can_take_its_time_range_and_filters_from_macros(ontology):
    v = view(ontology, "SELECT isp, plays FROM playback WHERE {{ last_hours(6) }} AND isp IN ({{ big_isps() }})")
    assert isinstance(v.source, ModelSource)
    query = v.source.query({}, NOW)
    assert (query.start, query.end) == (NOW - timedelta(hours=6), NOW)
    assert query.filters == [Filter(dimension="isp", op="in", value=["comcast", "att"])]
    assert {row["isp"] for row in Gateway(router()).run_view(v).rows} == {"comcast", "att"}


def test_a_composed_view_renders_its_parts_and_the_sql_over_them(ontology):
    v = view(
        ontology,
        """
        WITH live AS (SELECT isp, plays FROM playback WHERE {{ last_hours(6) }} AND isp IN ({{ big_isps() }})),
        hourly AS (SELECT isp, max(plays) AS busiest_hour FROM gold.hourly WHERE isp IN ({{ big_isps() }}) GROUP BY isp)
        SELECT live.isp, live.plays, hourly.busiest_hour
        FROM live JOIN hourly ON hourly.isp = live.isp
        WHERE live.isp IN ({{ big_isps() }})
        ORDER BY live.isp
        """,
    )
    assert isinstance(v.source, ComposedSource)
    assert {name: type(part) for name, part in v.source.parts.items()} == {"live": ModelSource, "hourly": TableSource}
    assert "IN ('comcast', 'att')" in v.source.stage2_sql({})  # translated to DuckDB from the rendered SQL
    rows = Gateway(router(), warehouse=warehouse()).run_view(v).rows
    assert [(row["isp"], row["busiest_hour"]) for row in rows] == [("att", 3), ("comcast", 8)]


@pytest.mark.parametrize(
    ("sql", "message"),
    [
        ("SELECT plays FROM playback WHERE {{ last_hour(1) }}", "'last_hour' is undefined (macros: avg, big_isps, count,"),
        ("SELECT {{ url_host() }} AS host FROM raw.edits", "parameter 'column' was not provided"),  # an argument left out
        ("SELECT plays FROM playback WHERE {{ last_hours(1) }", "line 1: unexpected '}'"),
    ],
)
def test_macro_mistakes_in_a_view_name_the_file_and_the_macros(ontology, tmp_path, sql, message):
    (tmp_path / "views").mkdir()
    (tmp_path / "views" / "recent.sql").write_text(f"/* description: Recent plays. */\n{sql}")
    with pytest.raises(ViewError) as raised:
        load_views(tmp_path / "views", ontology, TABLES | EDITS)
    assert f"views/recent.sql: macros: {message}" in str(raised.value)


# ---------------------------------------------------------------- steps

STEP_MACROS = {
    "default.sql": """\
{# The hourly activity rollup, which daily steps start from. #}
{% macro activity() %}gold.hourly_activity{% endmacro %}

{# The day a time falls in. #}
{% macro day(ts) %}CAST({{ ts }} AS DATE){% endmacro %}
""",
    "trino.sql": "{% macro day(ts) %}date_trunc('day', {{ ts }}){% endmacro %}\n",
}
DAILY = {
    "name": "daily_activity",
    "description": "Events per category and day.",
    "sql": "SELECT {{ day('event_ts') }} AS day, category, sum(event_count) AS events FROM {{ activity() }} GROUP BY 1, 2",
    "output": "gold.daily_activity",
    "time_column": "day",
    "every_minutes": 60,
}


@pytest.fixture
def starter(tmp_path) -> Path:
    """The starter project, with macros, a SQL step that calls them, and a view of the step's table."""
    create_project(tmp_path / "demo")
    config = tmp_path / "demo" / "config"
    (config / "macros").mkdir(exist_ok=True)
    for name, text in STEP_MACROS.items():
        (config / "macros" / name).write_text(text)
    (config / "pipelines.yaml").write_text(yaml.safe_dump({"version": "1.0.0", "steps": [DAILY]}, sort_keys=False))
    (config / "views" / "daily.sql").write_text("/*\ndescription: Events per category and day.\n*/\nSELECT day, category, events FROM gold.daily_activity")
    return config


def test_a_steps_sql_is_rendered_with_trinos_macros_when_the_platform_loads(starter):
    platform = load_platform(starter)
    step = platform.step("daily_activity")
    assert step.sql == "SELECT date_trunc('day', event_ts) AS day, category, sum(event_count) AS events FROM gold.hourly_activity GROUP BY 1, 2"
    assert step.inputs() == {"gold.hourly_activity"}  # the table a macro names
    # Trino's day() keeps a timestamp, where the one for every engine would have made it a DATE.
    assert platform.table_schemas()["gold.daily_activity"] == {"day": "TIMESTAMP", "category": "VARCHAR", "events": "BIGINT"}
    assert standin_gateway(platform).view("daily").rows  # the stand-in ran the rendered step, so its table has rows


def test_a_macro_mistake_in_a_step_names_the_step(starter):
    pipelines = yaml.safe_load((starter / "pipelines.yaml").read_text())
    pipelines["steps"][0]["sql"] = pipelines["steps"][0]["sql"].replace("activity()", "activities()")
    (starter / "pipelines.yaml").write_text(yaml.safe_dump(pipelines, sort_keys=False))
    with pytest.raises(ConfigError, match=r"^step 'daily_activity': macros: 'activities' is undefined \(macros: activity, "):
        load_platform(starter)


# ---------------------------------------------------------------- the catalog and MCP


def test_the_catalog_and_list_metrics_list_the_projects_macros_first(ontology):
    gateway = Gateway(Router(SqlCompiler(ontology), router().placements))
    with TestClient(create_app(gateway)) as client:
        macros = client.get("/catalog").json()["macros"]
    assert macros[:4] == [
        {"name": "big_isps", "args": [], "description": "The ISPs most plays come through.", "files": ["project/default.sql"]},
        {
            "name": "human_edit",
            "args": [],
            "description": "An edit by a person with an account: not a bot, not anonymous.",
            "files": ["project/default.sql"],
        },
        {"name": "last_hours", "args": ["hours"], "description": "The last `hours` hours of plays.", "files": ["project/default.sql"]},
        {
            "name": "url_host",
            "args": ["column"],
            "description": "The host of a URL, e.g. en.wikipedia.org.",
            "files": ["project/default.sql", "project/trino.sql"],
        },
    ]
    engine = {m["name"]: m for m in macros[4:]}  # then the engine's, which a project can override
    assert {"percentile", "ratio"} <= engine.keys() and all(f.startswith("engine/") for m in engine.values() for f in m["files"])
    assert call_mcp(create_mcp_server(gateway), "call_tool", "list_metrics", {}).structured_content["macros"] == macros


def test_the_dashboard_guide_says_views_can_call_macros(tmp_path):
    server = create_mcp_server(Gateway(router()), project=Project(tmp_path))
    text = call_mcp(server, "get_prompt", "build_dashboard", {"goal": "Show plays by ISP"}).messages[0].content.text
    assert "View files can call the project's SQL macros, {{ name(...) }}," in text
    assert "list_metrics lists them" in text


def test_ad_hoc_sql_cant_call_macros_since_rendering_runs_code(ontology):
    """GET /sql and MCP's query_sql take SQL from clients: Jinja in it is refused, never rendered."""
    gateway = Gateway(Router(SqlCompiler(ontology), router().placements))
    payload = "SELECT {{ adapter.__init__.__globals__['__builtins__']['len']('abc') }} AS n"  # Python, in a template
    refused = "only the project's files can call macros ({{ ... }}): save the SQL as a view, views/<name>.sql, and run that"
    with TestClient(create_app(gateway)) as client:
        for sql in (payload, "SELECT isp, plays FROM playback WHERE ts >= now() - INTERVAL '1' HOUR AND isp IN ({{ big_isps() }})"):
            response = client.get("/sql", params={"q": sql})
            assert (response.status_code, response.json()) == (400, {"detail": refused})
    result = call_mcp(create_mcp_server(gateway), "call_tool", "query_sql", {"sql": payload})
    assert result.is_error and refused in result.content[0].text


# ---------------------------------------------------------------- live reload

PLAYBACK_MACROS = """\
{# The ISPs most viewers watch through. #}
{% macro big_isps() %}'comcast', 'att'{% endmacro %}
"""
# An override of a built-in for every engine (DuckDB has no ratio of its own): queries compile with it.
RATIO = "{% macro ratio(numerator, denominator) %}1.0 * {{ numerator }} / NULLIF({{ denominator }}, 0){% endmacro %}\n"
BIG_ISPS = """/*
description: Rebuffer rate at the big ISPs over the last hour.
*/
SELECT isp_provider, rebuffer_incidence_rate
FROM playback_telemetry
WHERE event_ts >= now() - INTERVAL '1' HOUR AND isp_provider IN ({{ big_isps() }})
"""


def events(response: httpx.Response) -> Iterator[tuple[str, dict]]:
    """A Server-Sent Events stream, as (event, data)."""
    event = None
    for line in response.iter_lines():
        if line.startswith("event: "):
            event = line.removeprefix("event: ")
        elif line.startswith("data: "):
            yield event, json.loads(line.removeprefix("data: "))


def test_editing_a_macro_reloads_the_views_the_queries_and_open_pages(tmp_path):
    """Edit a macro: the views calling it render again, queries compile with it, and pages reload.
    Break it: pages say what broke, and the last good views and macros stay."""
    config = tmp_path / "config"
    shutil.copytree(EXAMPLE, config)
    (config / "macros").mkdir(exist_ok=True)
    macros = config / "macros" / "default.sql"
    macros.write_text(PLAYBACK_MACROS)
    (config / "views" / "big_isps.sql").write_text(BIG_ISPS)
    project = Project(config, gateway=standin_gateway)
    gateway = standin_gateway(load_platform(config))

    def isps(view: View) -> set[str]:
        return {row["isp_provider"] for row in gateway.run_view(view).rows}

    assert isps(gateway.views["big_isps"]) == {"comcast", "att"}
    assert "1.0 * " not in str(gateway.view("big_isps", include_sql=True).sql)
    with serving(create_app(gateway, dashboards_dir=project.dashboards_dir, project=project)) as url, httpx.Client(timeout=15) as client:
        with client.stream("GET", f"{url}/live") as response:
            stream = events(response)
            assert next(stream)[0] == "hello"
            macros.write_text(PLAYBACK_MACROS.replace("'comcast', 'att'", "'cox', 'frontier'") + RATIO)
            assert next(stream) == ("reload", {})
            assert isps(gateway.views["big_isps"]) == {"cox", "frontier"}  # rendered again
            sql = gateway.view("big_isps", include_sql=True).sql
            assert sql and all("1.0 * " in text for text in sql.values())  # compiled with the new ratio
            macros.write_text("{% macro big_isps( %}")
            event, data = next(stream)
            assert event == "problem" and "macros/default.sql, line 1: " in data["detail"]
            assert isps(gateway.views["big_isps"]) == {"cox", "frontier"}  # the last good views and macros
