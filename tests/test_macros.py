"""Project macros (agentic_lakehouse/macros.py): config/macros/ over the engine's templates/macros/."""

import shutil
from datetime import datetime
from pathlib import Path

import pytest
from test_compiler import ONTOLOGY

from agentic_lakehouse.compiler import MetricQuery, SqlCompiler
from agentic_lakehouse.config import ConfigError, load_platform
from agentic_lakehouse.macros import MacroError, ProjectMacros

DEFAULT = """\
{# An edit by a person with an account: not a bot, not anonymous. #}
{% macro human_edit() %}is_bot = false AND is_anonymous = false{% endmacro %}

{# The host of a URL, e.g. en.wikipedia.org. #}
{% macro url_host(column) %}regexp_extract({{ column }}, '^https?://([^/]+)', 1){% endmacro %}

{# Overrides of built-ins, for every engine: one with no engine-specific built-in, one with them. #}
{% macro ratio(numerator, denominator) %}({{ numerator }}) / NULLIF(({{ denominator }}), 0){% endmacro %}
{% macro percentile(expr, fraction) %}GENERIC_PERCENTILE({{ expr }}, {{ fraction }}){% endmacro %}
"""
TRINO = """\
{% macro url_host(column) %}url_extract_host({{ column }}){% endmacro %}

{# Exact, where the engine's is approximate. #}
{% macro percentile(expr, fraction) %}EXACT_PERCENTILE({{ expr }}, {{ fraction }}){% endmacro %}
"""


@pytest.fixture
def macros(tmp_path) -> ProjectMacros:
    (tmp_path / "default.sql").write_text(DEFAULT)
    (tmp_path / "trino.sql").write_text(TRINO)
    return ProjectMacros(tmp_path)


def test_an_engine_specific_macro_beats_a_generic_one_and_the_project_beats_the_engine(macros):
    percentile = {engine: macros.adapter(engine).percentile("x", 0.5) for engine in ("trino", "pinot", "duckdb")}
    assert percentile == {
        "trino": "EXACT_PERCENTILE(x, 0.5)",  # the project's trino.sql
        "pinot": "PERCENTILETDIGEST(x, 50)",  # the engine's pinot.sql beats the project's default.sql
        "duckdb": "quantile_cont(x, 0.5)",  # likewise the engine's duckdb.sql
    }
    assert {engine: macros.adapter(engine).ratio("a", "b") for engine in ("trino", "duckdb")} == {
        "trino": "(a) / NULLIF((b), 0)",  # no engine-specific built-in: the project's default.sql wins
        "duckdb": "(a) / NULLIF((b), 0)",
    }
    assert macros.adapter("trino").count(None) == "COUNT(*)"  # untouched built-ins still work


def test_sql_is_rendered_with_the_engines_macros(macros):
    assert macros.render("{{ human_edit() }}", "pinot") == "is_bot = false AND is_anonymous = false"
    assert macros.render("{{ url_host('url') }} = 'x'", "trino") == "url_extract_host(url) = 'x'"
    assert macros.render("{{ adapter.url_host('url') }}", "duckdb") == "regexp_extract(url, '^https?://([^/]+)', 1)"
    assert macros.render("plain SQL, untouched", "trino") == "plain SQL, untouched"


@pytest.mark.parametrize(
    ("text", "message"),
    [
        ("{{ no_such_macro() }}", "'no_such_macro' is undefined (macros: "),
        ("{{ url_host() }}", "parameter 'column' was not provided"),  # an argument left out
        ("{{ url_host('a') ", "line 1: unexpected end of template"),
    ],
)
def test_rendering_mistakes_are_explained(macros, text, message):
    with pytest.raises(MacroError, match=message.replace("(", r"\(")):
        macros.render(text, "trino")


def test_a_macro_file_that_isnt_jinja_names_its_file_and_line(tmp_path):
    (tmp_path / "pinot.sql").write_text("{% macro ok() %}1{% endmacro %}\n{% macro broken( %}\n")
    with pytest.raises(MacroError, match=r"macros/pinot.sql, line 2: "):
        ProjectMacros(tmp_path)


def test_the_catalog_lists_project_macros_first_with_descriptions(macros):
    catalog = {m.name: m for m in macros.catalog()}
    human = catalog["human_edit"]
    assert (human.args, human.description, human.files) == ([], "An edit by a person with an account: not a bot, not anonymous.", ["project/default.sql"])
    assert catalog["url_host"].files == ["project/default.sql", "project/trino.sql"]
    assert catalog["percentile"].files == [
        "project/default.sql", "project/trino.sql", "engine/macros/default.sql", "engine/macros/duckdb.sql", "engine/macros/trino.sql", "engine/macros/pinot.sql",
    ]
    assert catalog["percentile"].description == "Exact, where the engine's is approximate."  # the comment right before a definition
    assert catalog["ratio"].description == "Overrides of built-ins, for every engine: one with no engine-specific built-in, one with them."
    assert macros.catalog()[0].name == "human_edit"  # the project's own come first


def test_queries_compile_with_the_projects_overrides(macros):
    ontology = ONTOLOGY.model_copy()
    ontology._macros = macros
    query = MetricQuery(metrics=["median_latency_ms", "slow_rate"], start=datetime(2026, 1, 1), end=datetime(2026, 1, 2))
    compiler = SqlCompiler(ontology)
    trino = compiler.compile(query, engine="trino", table="t")
    pinot = compiler.compile(query, engine="pinot", table="t")
    assert "EXACT_PERCENTILE(latency_ms, 0.5)" in trino and "PERCENTILETDIGEST" in pinot
    assert "NULLIF((COUNT(*)), 0)" in trino and "NULLIF((COUNT(*)), 0)" in pinot  # the project's ratio, everywhere
    assert "approx_percentile" in SqlCompiler(ONTOLOGY).compile(query, engine="trino", table="t")  # other ontologies are unaffected


def test_load_platform_reads_the_projects_macros(tmp_path):
    config = tmp_path / "config"
    shutil.copytree(Path(__file__).parents[1] / "examples" / "playback" / "config", config)
    assert load_platform(config).ontology.macros.files == []  # no folder: the engine's macros
    (config / "macros").mkdir()
    (config / "macros" / "default.sql").write_text(DEFAULT)
    assert load_platform(config).ontology.macros.files == ["default"]
    (config / "macros" / "default.sql").write_text("{% macro broken( %}")
    with pytest.raises(ConfigError, match=r"macros/default.sql, line 1: "):
        load_platform(config)


def test_a_files_header_comment_isnt_a_description(tmp_path):
    (tmp_path / "default.sql").write_text(
        "{#- What these macros are for: a header, before any macro. -#}\n\n"
        "{% macro undescribed() %}1{% endmacro %}\n\n"
        "{# A described one. #}\n{% macro described() %}2{% endmacro %}\n"
    )
    catalog = {m.name: m.description for m in ProjectMacros(tmp_path).catalog()}
    assert (catalog["undescribed"], catalog["described"]) == ("", "A described one.")
    engine = {m.name: m.description for m in ProjectMacros(None).catalog()}
    assert engine["quote"].startswith("Output column names.")  # not the engine file's header
    assert engine["percentile"].startswith("Exact percentile")  # duckdb.sql's comment, not its header
