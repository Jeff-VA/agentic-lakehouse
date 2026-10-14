"""Project macros where projects start and grow (docs/design/project-macros.md): `init` writes
config/macros/ with an example, and the drift agent offers Claude the project's own macros."""

import shutil
from datetime import datetime
from pathlib import Path

import duckdb
import pyarrow as pa
import pytest
import yaml
from sqlglot import exp
from test_agent_sync import claude

from agentic_lakehouse.agent_sync import (
    ColumnProfile,
    DriftReport,
    DriftSamples,
    Proposal,
    ProposedDimension,
    ProposedMetric,
    new_column_signal,
    propose_changes,
)
from agentic_lakehouse.compiler import SQLGLOT_DIALECTS
from agentic_lakehouse.config import load_platform
from agentic_lakehouse.macros import ENGINES, ProjectMacros
from agentic_lakehouse.ontology import parse_fragment
from agentic_lakehouse.scaffold import create_project
from agentic_lakehouse.schema_compiler import arrow_schema
from agentic_lakehouse.simulate import generate_events

# A project's own macros: two to offer Claude, one with a Trino version. Trino.sql also overrides
# a built-in and has a macro only Trino knows; ontology SQL can call neither, so they aren't offered.
DEFAULT = """\
{# A large event: an amount of 500 or more. #}
{% macro large_event() %}amount >= 500{% endmacro %}

{# The host of a URL, e.g. en.wikipedia.org. #}
{% macro url_host(column) %}regexp_extract({{ column }}, '^https?://([^/]+)', 1){% endmacro %}
"""
TRINO = """\
{% macro url_host(column) %}url_extract_host({{ column }}){% endmacro %}

{# Without the cast to DOUBLE. #}
{% macro percentile(expr, fraction) %}approx_percentile({{ expr }}, {{ fraction }}){% endmacro %}

{# The ISO day of the week, 1 (Monday) to 7. #}
{% macro weekday(ts) %}day_of_week({{ ts }}){% endmacro %}
"""

CHANNEL = ProposedDimension(
    signals=["S1"], model="activity", name="channel", type="string", description="Where the event came from: web or app.", sql=None, label=None
)
LARGE_APP_EVENTS = ProposedMetric(
    signals=["S1"], model="activity", name="large_app_events", description="Large events from the app.", type="aggregate",
    agg="count", sql=None, percentile=None, filters=["{{ large_event() }}", "channel = 'app'"], numerator=None, denominator=None,
    label=None, format=None, unit=None, better=None,
)


def proposal(*metrics: ProposedMetric) -> Proposal:
    return Proposal(
        summary="Upstream added the channel each event came from.", dimensions=[CHANNEL], metrics=list(metrics), changes=[], macros=[], unchanged=[]
    )


@pytest.fixture(scope="module")
def project(tmp_path_factory) -> Path:
    path = tmp_path_factory.mktemp("projects") / "demo-shop"
    create_project(path)
    return path


@pytest.fixture
def config(project, tmp_path) -> Path:
    """A copy of the new project's config, to change."""
    return shutil.copytree(project / "config", tmp_path / "config")


@pytest.fixture(scope="module")
def drift(project) -> tuple[DriftReport, DriftSamples]:
    """Producers start sending a `channel` field with the starter events: the report and a sample."""
    dataset = load_platform(project / "config").dataset("events")
    events = generate_events(dataset, datetime(2026, 1, 1), datetime(2026, 1, 1, 0, 10))
    for i, event in enumerate(events):
        event["channel"] = ("web", "app")[i % 2]
    sample = pa.Table.from_pylist(events, schema=arrow_schema(dataset).append(pa.field("channel", pa.string())))
    profile = ColumnProfile(name="channel", type="string", rows_sampled=len(events), rows_set=len(events), distinct_values=2)
    return DriftReport(dataset="events", table="raw.events", signals=[new_column_signal("events", profile)]), DriftSamples(sample, sample.slice(0, 0))


def test_init_writes_a_macro_whose_example_works_on_every_engine(project):
    macros = ProjectMacros(project / "config" / "macros")
    [example] = [m for m in macros.catalog() if m.files == ["project/default.sql"]]
    dataset = load_platform(project / "config").dataset("events")
    for engine in ENGINES:
        sql = macros.render(f"{{{{ {example.name}() }}}}", engine)
        columns = {c.name for c in parse_fragment(sql, read=SQLGLOT_DIALECTS[engine]).find_all(exp.Column)}
        assert columns and columns <= {c.name for c in dataset.columns}, engine  # reads the starter dataset

    con = duckdb.connect()
    con.register("events", pa.Table.from_pylist(generate_events(dataset, datetime(2026, 1, 1), datetime(2026, 1, 1, 1)), schema=arrow_schema(dataset)))
    sql = macros.render(f"{{{{ {example.name}() }}}}", "duckdb")
    matching, total = con.execute(f"SELECT count(*) FILTER (WHERE {sql}), count(*) FROM events").fetchone()
    assert 0 < matching < total  # a definition that picks out some of the simulated events


def test_a_new_project_loads_with_its_macros(project):
    assert load_platform(project / "config").ontology.macros.files == ["default"]


def test_the_examples_comment_describes_it(project):
    [example] = [m for m in ProjectMacros(project / "config" / "macros").catalog() if m.files == ["project/default.sql"]]
    assert example.description == "A large event: an amount of 500 or more."


def test_the_drift_agent_offers_claude_the_projects_macros_and_validates_with_them(config, drift):
    (config / "macros" / "default.sql").write_text(DEFAULT)
    (config / "macros" / "trino.sql").write_text(TRINO)
    requests = []
    # One attempt: the metric calls large_event(), so this fails unless validation knows the project's macros.
    result = propose_changes(*drift, config, claude(proposal(LARGE_APP_EVENTS), requests=requests), max_attempts=1, log=lambda m: None)

    prompt = requests[0]["body"]["messages"][0]["content"]
    assert "can call them with Jinja" in prompt
    listed = prompt.split("<project_macros>\n")[1].split("\n</project_macros>")[0]
    assert listed.splitlines() == [
        "- large_event(): A large event: an amount of 500 or more.",
        "- url_host(column): The host of a URL, e.g. en.wikipedia.org.",
        "  Engine-specific versions: trino.",
    ]
    assert "large_app_events" in result.patched["ontology.yaml"] and "{{ large_event() }}" in result.patched["ontology.yaml"]


@pytest.mark.parametrize("files", [None, {"trino.sql": TRINO.split("\n\n")[1]}], ids=["no folder", "only an override"])
def test_the_drift_agent_says_nothing_of_macros_when_the_project_has_none_of_its_own(config, drift, files):
    shutil.rmtree(config / "macros")
    ontology = yaml.safe_load((config / "ontology.yaml").read_text())
    for model in ontology["models"]:  # the starter's metric that calls large_event() goes too
        model["metrics"] = [m for m in model["metrics"] if m["name"] != "large_events"]
    (config / "ontology.yaml").write_text(yaml.safe_dump(ontology, sort_keys=False))
    if files:
        (config / "macros").mkdir()
        for name, text in files.items():
            (config / "macros" / name).write_text(text)
    requests = []
    propose_changes(*drift, config, claude(proposal(), requests=requests), log=lambda m: None)
    assert "macro" not in requests[0]["body"]["messages"][0]["content"]
