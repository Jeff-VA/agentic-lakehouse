"""Project macros in ontology snippets (docs/design/project-macros.md): rendered for each engine,
checked when the config loads, and proven to compile when they override the engine's built-ins."""

import shutil
from datetime import datetime
from pathlib import Path

import pytest
import yaml

from agentic_lakehouse.compiler import MetricQuery, SqlCompiler
from agentic_lakehouse.config import ConfigError, load_platform
from agentic_lakehouse.macros import ProjectMacros
from agentic_lakehouse.ontology import Ontology
from agentic_lakehouse.testing import standin_gateway

EXAMPLE = Path(__file__).parents[1] / "examples" / "playback" / "config"

MACROS = {
    "default.sql": """\
{# An edit by a person with an account: not a bot, not anonymous. #}
{% macro human_edit() %}is_bot = false AND is_anonymous = false{% endmacro %}

{# The host of a URL, e.g. en.wikipedia.org. #}
{% macro url_host(column) %}regexp_extract({{ column }}, '^https?://([^/]+)', 1){% endmacro %}
""",
    "trino.sql": "{% macro url_host(column) %}url_extract_host({{ column }}){% endmacro %}\n",
}
EDITS = Ontology.model_validate({
    "version": "1",
    "models": [{
        "name": "edits",
        "description": "Edits.",
        "source": "edits",
        "time_dimension": "event_ts",
        "dimensions": [
            {"name": "event_ts", "type": "timestamp"},
            {"name": "host", "type": "string", "sql": "{{ url_host('url') }}"},
        ],
        "metrics": [
            {"name": "edit_count", "description": "Edits.", "agg": "count"},
            {"name": "human_edits", "description": "Edits by people with accounts.", "agg": "count", "filters": ["{{ human_edit() }}"]},
            {"name": "human_share", "type": "ratio", "description": "Share of edits by people with accounts.", "numerator": "human_edits", "denominator": "edit_count"},
        ],
    }],
})


@pytest.fixture
def compiler(tmp_path) -> SqlCompiler:
    for name, text in MACROS.items():
        (tmp_path / name).write_text(text)
    ontology = EDITS.model_copy()
    ontology._macros = ProjectMacros(tmp_path)
    return SqlCompiler(ontology)


def test_snippets_that_call_macros_compile_for_each_engine(compiler):
    query = MetricQuery(metrics=["human_share"], dimensions=["host"], start=datetime(2026, 1, 1), end=datetime(2026, 1, 2))
    trino = compiler.compile(query, engine="trino", table="t")
    pinot = compiler.compile(query, engine="pinot", table="t")
    duckdb = compiler.compile(query, engine="duckdb", table="t")
    assert "URL_EXTRACT_HOST(url)" in trino  # the project's trino.sql
    assert "REGEXP_EXTRACT(\"url\", '^https?://([^/]+)', 1)" in pinot  # its default.sql, printed for Pinot
    assert "REGEXP_EXTRACT(url, '^https?://([^/]+)', 1)" in duckdb
    assert "is_bot = FALSE AND is_anonymous = FALSE" in trino and "is_bot = FALSE AND is_anonymous = FALSE" in duckdb
    assert '"is_bot" = FALSE AND "is_anonymous" = FALSE' in pinot  # the business definition, in every engine's SQL


def test_a_filter_on_a_templated_dimension_uses_the_engines_macro(compiler):
    query = MetricQuery(
        metrics=["edit_count"], start=datetime(2026, 1, 1), end=datetime(2026, 1, 2),
        filters=[{"dimension": "host", "op": "=", "value": "en.wikipedia.org"}],
    )
    assert "URL_EXTRACT_HOST(url) = 'en.wikipedia.org'" in compiler.compile(query, engine="trino", table="t")


# ---------------------------------------------------------------- checked when the config loads


def project(tmp_path, macros: dict[str, str], *metrics: dict) -> Path:
    """A copy of the example project with these macro files and extra metrics in its model."""
    config = tmp_path / "config"
    shutil.copytree(EXAMPLE, config)
    (config / "macros").mkdir()
    for name, text in macros.items():
        (config / "macros" / name).write_text(text)
    ontology = yaml.safe_load((config / "ontology.yaml").read_text())
    ontology["models"][0]["metrics"] += list(metrics)
    (config / "ontology.yaml").write_text(yaml.safe_dump(ontology, sort_keys=False))
    return config


SLOW_START = {"default.sql": "{# A stream start slower than a second. #}\n{% macro slow_start() %}stream_start_event = true AND request_latency_ms > 1000{% endmacro %}\n"}
SLOW_STARTS = {"name": "slow_starts", "description": "Stream starts slower than a second.", "agg": "count", "filters": ["{{ slow_start() }}"]}


def test_a_project_metric_using_a_macro_loads_and_answers(tmp_path):
    platform = load_platform(project(tmp_path, SLOW_START, SLOW_STARTS))
    gateway = standin_gateway(platform)
    query = MetricQuery(metrics=["slow_starts", "event_count"], start=datetime(2026, 1, 1), end=datetime(2100, 1, 1))
    [row] = gateway.query(query).rows
    assert 0 <= row["slow_starts"] <= row["event_count"]


@pytest.mark.parametrize(
    ("macros", "metric", "message"),
    [
        (SLOW_START, SLOW_STARTS | {"filters": ["{{ no_such_macro() }}"]},
         "ontology.yaml: model playback_telemetry, metric slow_starts, filters[0], for duckdb: 'no_such_macro' is undefined"),
        (SLOW_START | {"pinot.sql": "{% macro slow_start() %}request_latency_ms >{% endmacro %}\n"}, SLOW_STARTS,
         "ontology.yaml: model playback_telemetry, metric slow_starts, filters[0], for pinot: invalid SQL"),
        ({"default.sql": "{% macro slow_start() %}latency_seconds > 1{% endmacro %}\n"}, SLOW_STARTS,
         "model 'playback_telemetry' reads columns that dataset 'telemetry' doesn't have: latency_seconds"),
        (SLOW_START | {"duckdb.sql": "{% macro count(expr) %}COUNT_EVERYTHING({{ expr or '*' }}){% endmacro %}\n"}, SLOW_STARTS,
         "model playback_telemetry doesn't compile with the project's macros, for duckdb: Catalog Error"),
        (SLOW_START | {"duckdb.sql": "{% macro sum(expr) %}SUM({{ expr }}) +{% endmacro %}\n"}, SLOW_STARTS,
         "model playback_telemetry doesn't compile with the project's macros, for duckdb: Parser Error"),
    ],
)
def test_macro_mistakes_fail_the_config_with_their_place(tmp_path, macros, metric, message):
    with pytest.raises(ConfigError) as raised:
        load_platform(project(tmp_path, macros, metric))
    assert message in str(raised.value)


def test_an_override_that_works_passes_the_proof(tmp_path):
    exact = {"trino.sql": "{# Exact, where Trino's is approximate. #}\n{% macro percentile(expr, fraction) %}"
             "approx_percentile(CAST({{ expr }} AS DOUBLE), {{ fraction }}, 0.0001){% endmacro %}\n"}
    platform = load_platform(project(tmp_path, SLOW_START | exact, SLOW_STARTS))
    query = MetricQuery(metrics=["p99_ttfb"], start=datetime(2026, 1, 1), end=datetime(2026, 1, 2))
    assert ", 0.0001)" in SqlCompiler(platform.ontology).compile(query, engine="trino", table="t")
