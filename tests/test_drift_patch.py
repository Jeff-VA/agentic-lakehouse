"""The drift agent's patchers (agentic_lakehouse/drift/patch.py): a proposal applied to the config
files' text, keeping their comments and styles, and the diff that `git apply` applies.

tests/fixtures/wiki_pulse/ holds copies of wiki-pulse's ontology.yaml and macro files: real files,
with flow-style entries, folded descriptions and an engine-specific macro."""

import difflib
import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

from agentic_lakehouse.config import load_platform
from agentic_lakehouse.drift.patch import LINE_WIDTH, add_ingestion_columns, apply_macros, apply_ontology, config_diff
from agentic_lakehouse.drift.proposal import DefinitionChange, MacroChange, Proposal, ProposedDimension, ProposedMetric
from agentic_lakehouse.drift.signals import ColumnProfile
from agentic_lakehouse.macros import ENGINES, NO_PROJECT_MACROS, ProjectMacros
from agentic_lakehouse.ontology import Ontology

WIKI = Path(__file__).parent / "fixtures" / "wiki_pulse"
WIKI_ONTOLOGY = (WIKI / "ontology.yaml").read_text()
WIKI_MACROS = {path.name: path.read_text() for path in (WIKI / "macros").glob("*.sql")}
PLAYBACK = Path(__file__).parents[1] / "examples" / "playback" / "config"
PLAYBACK_ONTOLOGY = (PLAYBACK / "ontology.yaml").read_text()
RESERVED = {macro.name for macro in NO_PROJECT_MACROS.catalog()}  # the engine's built-ins


def change(name: str, kind: str = "metric", model: str = "edits", **overrides) -> DefinitionChange:
    fields = {"signals": ["S1"], "model": model, "kind": kind, "name": name, "description": None, "sql": None, "filters": None, "reason": "Drift."}
    return DefinitionChange(**fields | overrides)


def dimension(name: str, model: str = "playback_telemetry", **overrides) -> ProposedDimension:
    fields = {"signals": ["S1"], "model": model, "name": name, "type": "string", "description": f"The {name}.", "sql": None, "label": None}
    return ProposedDimension(**fields | overrides)


def metric(name: str, model: str = "playback_telemetry", **overrides) -> ProposedMetric:
    fields = {
        "signals": ["S2"],
        "model": model,
        "name": name,
        "description": f"The {name}.",
        "type": "aggregate",
        "agg": "count",
        "sql": None,
        "percentile": None,
        "filters": [],
        "numerator": None,
        "denominator": None,
        "label": None,
        "format": None,
        "unit": None,
        "better": None,
    }
    return ProposedMetric(**fields | overrides)


def macro(name: str, sql: str, args: list[str] | None = None, description: str = "What it means.", **engines) -> MacroChange:
    versions = {engine: None for engine in ENGINES} | engines
    return MacroChange(signals=["S1"], name=name, args=args or [], description=description, sql=sql, reason="Drift.", **versions)


def proposal(changes=(), dimensions=(), metrics=(), macros=()) -> Proposal:
    return Proposal(
        summary="Upstream changed.", dimensions=list(dimensions), metrics=list(metrics), changes=list(changes), macros=list(macros), unchanged=[]
    )


def changed_lines(before: str, after: str) -> list[str]:
    """The lines a diff removes (-) and adds (+), in order."""
    diff = difflib.unified_diff(before.splitlines(), after.splitlines(), lineterm="", n=0)
    return [line for line in diff if line[:1] in "-+" and not line.startswith(("---", "+++"))]


# --- ontology.yaml: changes


def test_a_change_keeps_comments_and_quotes():
    patched = apply_ontology(WIKI_ONTOLOGY, proposal([
        change("editor_type", "dimension", description="Who made the edit, broadly; bot, temporary, anonymous or registered."),
        change("site", "dimension", sql="lower({{ url_host('url') }})"),
    ]))

    assert changed_lines(WIKI_ONTOLOGY, patched) == [
        "-        description: Who made the edit, broadly; bot, anonymous (an IP address or temporary account) or registered.",
        "+        description: Who made the edit, broadly; bot, temporary, anonymous or registered.",
        "-        sql: \"{{ url_host('url') }}\"  # Trino has its own version, in config/macros/trino.sql",
        "+        sql: \"lower({{ url_host('url') }})\"  # Trino has its own version, in config/macros/trino.sql",
    ]


def test_a_flow_entry_stays_flow_style():
    patched = apply_ontology(WIKI_ONTOLOGY, proposal([change("title", "dimension", description="The article's title.")]))
    assert changed_lines(WIKI_ONTOLOGY, patched) == [
        "-      - {name: title, type: string, label: Title}",
        "+      - {name: title, type: string, label: Title, description: The article's title.}",
    ]


@pytest.mark.parametrize("over", [0, 1])
def test_a_flow_entry_becomes_block_style_when_its_line_grows_past_the_width(over):
    start = "      - {name: title, type: string, label: Title, description: "
    description = "x" * (LINE_WIDTH - len(start) - len("}") + over)
    patched = apply_ontology(WIKI_ONTOLOGY, proposal([change("title", "dimension", description=description)]))

    lines = changed_lines(WIKI_ONTOLOGY, patched)
    if over:
        assert lines == [
            "-      - {name: title, type: string, label: Title}",
            "+      - name: title",
            "+        type: string",
            "+        label: Title",
            f"+        description: {description}",
        ]
    else:
        assert lines[1] == f"+{start}{description}}}"
        assert len(lines[1]) - 1 == LINE_WIDTH


def test_a_flow_entry_turned_block_style_keeps_its_comment_and_what_followed_it():
    text = """\
models:
  - name: m
    dimensions:
      - {name: t, type: string}  # the title

    metrics:
      - {name: n, description: Rows., agg: count}
"""
    description = "The title, as the page shows it: words separated by spaces, rather than by underscores, as in its URL."
    patched = apply_ontology(text, proposal([change("t", "dimension", model="m", description=description)]))
    assert patched == text.replace(
        "      - {name: t, type: string}  # the title\n",
        f"      - name: t  # the title\n        type: string\n        description: '{description}'\n",
    )


def test_a_folded_description_is_folded_again_to_fit():
    description = (
        "How far above usual a burst's edits were, in standard deviations (usual: for an article "
        "being edited on that wiki at that hour of the day, over the last four weeks)."
    )
    patched = apply_ontology(WIKI_ONTOLOGY, proposal([change("peak_surprise", model="trends", description=description)]))

    assert changed_lines(WIKI_ONTOLOGY, patched) == [
        "-          How far above usual a burst's edits were, in standard deviations",
        "-          (usual: for an article being edited on that wiki at that hour of the day).",
        "+          How far above usual a burst's edits were, in standard deviations (usual: for an article",
        "+          being edited on that wiki at that hour of the day, over the last four weeks).",
    ]
    [trends] = [m for m in yaml.safe_load(patched)["models"] if m["name"] == "trends"]
    assert next(m for m in trends["metrics"] if m["name"] == "peak_surprise")["description"] == description + "\n"


def test_sql_on_several_lines_is_a_literal_block_with_the_comment_above_it():
    sql = "CASE WHEN is_temporary = true THEN 'temporary'\n     ELSE {{ editor_type() }} END"
    patched = apply_ontology(WIKI_ONTOLOGY, proposal([change("editor_type", "dimension", sql=sql)]))

    assert changed_lines(WIKI_ONTOLOGY, patched) == [
        '-        sql: "{{ editor_type() }}"  # config/macros/default.sql',
        "+        # config/macros/default.sql",
        "+        sql: |-",
        "+          CASE WHEN is_temporary = true THEN 'temporary'",
        "+               ELSE {{ editor_type() }} END",
    ]
    [editor_type] = [d for d in yaml.safe_load(patched)["models"][0]["dimensions"] if d["name"] == "editor_type"]
    assert editor_type["sql"] == sql


def test_sql_moves_between_one_line_and_a_literal_block_keeping_what_followed_it():
    text = """\
models:
  - name: m
    dimensions:
      - name: a
        type: string
        sql: "x"  # was x

      # b is next
      - {name: b, type: string}
      - name: c
        type: string
        sql: |-
          CASE WHEN y THEN 1
          END

    metrics:
      - {name: n, description: Rows., agg: count}
"""
    sql = "CASE WHEN y THEN 1\n  ELSE 2 END"
    patched = apply_ontology(text, proposal([
        change("a", "dimension", model="m", sql=sql),
        change("b", "dimension", model="m", sql=sql),  # a flow entry can't hold a block: it becomes block style
        change("c", "dimension", model="m", sql="z"),
    ]))

    assert patched == """\
models:
  - name: m
    dimensions:
      - name: a
        type: string
        # was x
        sql: |-
          CASE WHEN y THEN 1
            ELSE 2 END

      # b is next
      - name: b
        type: string
        sql: |-
          CASE WHEN y THEN 1
            ELSE 2 END
      - name: c
        type: string
        sql: z

    metrics:
      - {name: n, description: Rows., agg: count}
"""


def test_filters_keep_their_list_style_until_the_line_is_too_long():
    patched = apply_ontology(WIKI_ONTOLOGY, proposal([
        change("human_edits", filters=["{{ human_edit() }}", "is_temporary = false"]),
        change("judged_bursts", model="trends", filters=["p_news IS NOT NULL", "claude_label IN ('news', 'edit war', 'routine', 'vandalism')"]),
    ]))

    assert changed_lines(WIKI_ONTOLOGY, patched) == [
        '-        filters: ["{{ human_edit() }}"]',
        '+        filters: ["{{ human_edit() }}", "is_temporary = false"]',
        "-        filters: [\"p_news IS NOT NULL\", \"claude_label IN ('news', 'edit war', 'routine')\"]",
        "+        filters:",
        '+          - "p_news IS NOT NULL"',
        "+          - \"claude_label IN ('news', 'edit war', 'routine', 'vandalism')\"",
    ]


def test_new_filters_follow_the_model_and_go_before_the_label():
    wiki = apply_ontology(WIKI_ONTOLOGY, proposal([change("edit_count", filters=["is_temporary IS NOT NULL"])]))
    playback = apply_ontology(PLAYBACK_ONTOLOGY, proposal([
        change("event_count", model="playback_telemetry", filters=["stream_start_event = true"]),
        change("p99_ttfb", model="playback_telemetry", filters=["stream_start_event = true", "request_latency_ms >= 0"]),
    ]))

    assert changed_lines(WIKI_ONTOLOGY, wiki) == ['+        filters: ["is_temporary IS NOT NULL"]']
    assert changed_lines(PLAYBACK_ONTOLOGY, playback) == [
        '+          - "request_latency_ms >= 0"',  # p99_ttfb's existing filter is kept as it was
        "+        filters:",
        '+          - "stream_start_event = true"',
    ]
    assert "        agg: count\n        filters:\n          - \"stream_start_event = true\"\n        label: Events\n" in playback


def test_empty_filters_remove_the_key_and_keep_what_followed_it():
    text = """\
models:
  - name: m
    metrics:
      - name: a
        description: A.
        agg: count
        filters:
          - "x = 1"  # why

      # b counts every row
      - name: b
        description: B.
        agg: count
"""
    patched = apply_ontology(text, proposal([change("a", model="m", filters=[])]))
    assert changed_lines(text, patched) == ["-        filters:", '-          - "x = 1"  # why']

    playback = apply_ontology(PLAYBACK_ONTOLOGY, proposal([change("rebuffer_event_count", model="playback_telemetry", filters=[])]))
    assert changed_lines(PLAYBACK_ONTOLOGY, playback) == ["-        filters:", '-          - "buffer_duration_ms > 200"']


def test_a_new_key_at_the_end_of_an_entry_goes_before_the_blank_line():
    patched = apply_ontology(PLAYBACK_ONTOLOGY, proposal([
        change("is_mobile_device", "dimension", model="playback_telemetry", description="Whether the client is a phone or tablet."),
    ]))
    assert "        label: Mobile device\n        description: Whether the client is a phone or tablet.\n\n    metrics:\n" in patched


@pytest.mark.parametrize(
    ("bad", "message"),
    [
        (change("edit_count", model="nope", description="x"), "change to metric 'edit_count': there is no model named 'nope'; models: edits, trends"),
        (change("edit_total", description="x"), "change to metric 'edit_total': model 'edits' has no metric named 'edit_total'; its metrics: edit_count, editors, articles,"),
        (change("edit_count", "dimension", description="x"), "model 'edits' has no dimension named 'edit_count'; its dimensions: event_ts, wiki,"),
        (change("wiki", "dimension", filters=["wiki = 'enwiki'"]), "change to dimension 'wiki': a dimension has no filters; give null"),
        (change("bot_share", sql="is_bot"), "change to metric 'bot_share': a ratio metric has no sql or filters; change the metrics it divides instead"),
        (change("bot_share", filters=["is_bot = true"]), "a ratio metric has no sql or filters"),
        (change("edit_count"), "change to metric 'edit_count': it changes nothing"),
        (change("edit_count", description="Number of edits."), "change to metric 'edit_count': it changes nothing"),
        (change("edit_count", filters=[]), "change to metric 'edit_count': it changes nothing"),
    ],
)
def test_a_change_that_cant_apply_is_explained(bad, message):
    with pytest.raises(ValueError) as raised:
        apply_ontology(WIKI_ONTOLOGY, proposal([bad]))
    assert message in str(raised.value)


def test_every_problem_is_reported_at_once():
    twice = [change("edit_count", description="Edits."), change("edit_count", sql="rev_id")]
    ratio = metric("odd_share", model="edits", type="ratio", agg=None, numerator="bot_edits", denominator="edit_count", filters=["x = 1"])
    with pytest.raises(ValueError) as raised:
        apply_ontology(WIKI_ONTOLOGY, proposal(twice, dimensions=[dimension("cdn", model="nope")], metrics=[ratio]))
    assert str(raised.value) == (
        "change to metric 'edit_count': another change already changes it; give one change with all of its edits; "
        "dimension 'cdn': there is no model named 'nope'; models: edits, trends; "
        "metric 'odd_share': a ratio metric has no sql or filters; put them on the metrics it divides"
    )


def test_empty_filters_on_a_dimension_say_what_is_already_true():
    patched = apply_ontology(WIKI_ONTOLOGY, proposal([change("wiki", "dimension", description="The wiki.", filters=[])]))
    assert changed_lines(WIKI_ONTOLOGY, patched)[1] == "+        description: The wiki."


# --- ontology.yaml: new definitions


def test_new_definitions_follow_the_layout_of_their_lists():
    patched = apply_ontology(PLAYBACK_ONTOLOGY, proposal(
        dimensions=[dimension("cdn_pop", label="CDN PoP", description="CDN point of presence\n that served the stream.")],
        metrics=[metric("avg_dropped_frames", agg="avg", sql="dropped_frames", filters=["is_live = true"], label="Dropped frames",
                        format="number", unit="frames", better="lower")],
    ))

    # Dimensions have no blank lines between them, but one before `metrics:`; metrics have one between each.
    assert changed_lines(PLAYBACK_ONTOLOGY, patched) == [
        "+      - name: cdn_pop",
        "+        type: string",
        "+        label: CDN PoP",
        "+        description: CDN point of presence that served the stream.",
        "+",
        "+      - name: avg_dropped_frames",
        "+        description: The avg_dropped_frames.",
        "+        agg: avg",
        "+        sql: dropped_frames",
        "+        filters:",
        '+          - "is_live = true"',
        "+        label: Dropped frames",
        "+        format: number",
        "+        unit: frames",
        "+        better: lower",
    ]
    assert "        label: Mobile device\n      - name: cdn_pop\n" in patched
    assert "        better: higher\n\n      - name: avg_dropped_frames\n" in patched


def test_a_new_metric_takes_its_models_filter_style_and_keeps_the_blank_line_after_the_model():
    patched = apply_ontology(WIKI_ONTOLOGY, proposal(metrics=[metric("temporary_edits", model="edits", filters=["is_temporary = true"], label="Temporary edits")]))
    assert changed_lines(WIKI_ONTOLOGY, patched) == [
        "+      - name: temporary_edits",
        "+        description: The temporary_edits.",
        "+        agg: count",
        '+        filters: ["is_temporary = true"]',
        "+        label: Temporary edits",
    ]
    assert "        label: Temporary edits\n\n  - name: trends\n" in patched


def test_new_ingestion_columns_go_after_the_last_column_and_before_what_followed_it():
    text = """\
datasets:
  - name: edits
    columns:
      - {name: event_ts, type: timestamp}
      - {name: url, type: string}  # the article

    realtime:
      table: edits
"""
    patched = add_ingestion_columns(text, "edits", [ColumnProfile(name="is_temporary", type="boolean", rows_sampled=10, rows_set=4, distinct_values=2)])
    assert patched == text.replace("# the article\n", "# the article\n      - {name: is_temporary, type: boolean}\n")


def test_apply_ontology_is_a_pure_function_whose_result_is_final():
    changes = [change("title", "dimension", description="The article's title, as Wikipedia shows it, with spaces rather than underscores.")]
    request = proposal(changes, metrics=[metric("temporary_edits", model="edits", filters=["is_temporary = true"])])
    dumped = request.model_dump()

    patched = apply_ontology(WIKI_ONTOLOGY, request)
    assert apply_ontology(WIKI_ONTOLOGY, request) == patched
    assert request.model_dump() == dumped
    assert apply_ontology(patched, proposal()) == patched  # nothing is left for a second pass
    Ontology.model_validate(yaml.safe_load(patched))


# --- macro files


def test_a_macro_is_replaced_together_with_its_description():
    patched = apply_macros(WIKI_MACROS, [macro(
        "human_edit", "is_bot = false AND is_anonymous = false AND is_temporary = false",
        description="An edit by a person with a permanent account:\n  not a bot, not anonymous, not temporary.",
    )], RESERVED)

    assert list(patched) == ["default.sql"]
    assert changed_lines(WIKI_MACROS["default.sql"], patched["default.sql"]) == [
        "-{# An edit by a person with an account: not a bot, not anonymous. #}",
        "-{% macro human_edit() %}is_bot = false AND is_anonymous = false{% endmacro %}",
        "+{# An edit by a person with a permanent account: not a bot, not anonymous, not temporary. #}",
        "+{% macro human_edit() %}is_bot = false AND is_anonymous = false AND is_temporary = false{% endmacro %}",
    ]


def test_a_new_macro_is_appended_after_a_blank_line():
    patched = apply_macros(WIKI_MACROS, [macro("temporary_account", "starts_with({{ editor }}, '~')", ["editor"], "A temporary account's name.")], RESERVED)
    assert patched == {
        "default.sql": WIKI_MACROS["default.sql"]
        + "\n{# A temporary account's name. #}\n{% macro temporary_account(editor) %}starts_with({{ editor }}, '~'){% endmacro %}\n"
    }


def test_an_engine_version_is_added_and_a_missing_file_created():
    patched = apply_macros(WIKI_MACROS, [macro(
        "temporary_account", "starts_with({{ editor }}, '~')", ["editor"], "A temporary account's name.",
        trino="{{ editor }} LIKE '~%'", pinot="STARTSWITH({{ editor }}, '~')",
    )], RESERVED)

    assert list(patched) == ["default.sql", "trino.sql", "pinot.sql"]
    assert patched["trino.sql"] == (
        WIKI_MACROS["trino.sql"] + "\n{# A temporary account's name. #}\n{% macro temporary_account(editor) %}{{ editor }} LIKE '~%'{% endmacro %}\n"
    )
    assert patched["pinot.sql"] == (
        "{#- Pinot's own versions of this project's macros, used instead of default.sql's on Pinot. -#}\n\n"
        "{# A temporary account's name. #}\n{% macro temporary_account(editor) %}STARTSWITH({{ editor }}, '~'){% endmacro %}\n"
    )


def test_an_engine_version_left_out_is_removed_with_its_description_and_a_blank_line():
    patched = apply_macros(WIKI_MACROS, [macro("url_host", "regexp_extract({{ column }}, '^https?://([^/]+)', 1)", ["column"], "The site a URL is on.")], RESERVED)

    assert patched["trino.sql"] == "{#- Trino's own versions of wiki-pulse's macros, used instead of default.sql's on Trino. -#}\n"
    assert changed_lines(WIKI_MACROS["default.sql"], patched["default.sql"]) == [
        "-{# The site a URL is on, e.g. en.wikipedia.org. #}",
        "+{# The site a URL is on. #}",
    ]


def test_blocks_are_found_with_whitespace_control_and_without_descriptions():
    text = """\
{#- A file's header, set off by a blank line: no macro's description. -#}

{#- Old. -#}
{%- macro a(x) -%}
  old({{ x }})
{%- endmacro -%}

{% macro b() %}1{% endmacro %}

{# Keep this one. #}
{% macro c() %}2{% endmacro %}
"""
    replaced = apply_macros({"duckdb.sql": text}, [macro("a", "x", duckdb="new({{ x }})", args=["x"], description="New.")], RESERVED)
    assert replaced["duckdb.sql"] == text.replace("{#- Old. -#}\n{%- macro a(x) -%}\n  old({{ x }})\n{%- endmacro -%}", "{# New. #}\n{% macro a(x) %}new({{ x }}){% endmacro %}")

    removed = apply_macros({"duckdb.sql": text}, [macro("a", "x", ["x"]), macro("b", "1")], RESERVED)["duckdb.sql"]
    assert removed == "{#- A file's header, set off by a blank line: no macro's description. -#}\n\n{# Keep this one. #}\n{% macro c() %}2{% endmacro %}\n"


def test_a_macro_defined_twice_ends_up_defined_once():
    text = "{% macro a() %}1{% endmacro %}\n\n{# Again. #}\n{% macro a() %}2{% endmacro %}\n"
    assert apply_macros({"default.sql": text}, [macro("a", "3", description="Once.")], RESERVED) == {
        "default.sql": "{# Once. #}\n{% macro a() %}3{% endmacro %}\n"
    }


def test_sql_on_several_lines_is_kept_as_given():
    sql = "CASE\n  WHEN is_bot = true THEN 'bot'\n  ELSE 'person'\nEND"
    patched = apply_macros({}, [macro("who", f"\n{sql}\n", description="Bot or person.")], RESERVED)
    assert patched["default.sql"].endswith(f"\n\n{{# Bot or person. #}}\n{{% macro who() %}}{sql}{{% endmacro %}}\n")


def test_unchanged_files_are_left_out():
    same = macro("human_edit", "is_bot = false AND is_anonymous = false", description="An edit by a person with an account: not a bot, not anonymous.")
    assert apply_macros(WIKI_MACROS, [same], RESERVED) == {}


def test_the_files_load_and_each_engine_renders_its_version(tmp_path):
    changes = [
        macro("human_edit", "is_bot = false AND {{ adapter.not_temporary('editor') }}", description="A person with a permanent account."),
        macro("not_temporary", "NOT starts_with({{ editor }}, '~')", ["editor"], "Not a temporary account.", trino="{{ editor }} NOT LIKE '~%'"),
        macro("url_host", "regexp_extract({{ column }}, '^https?://([^/]+)', 1)", ["column"], "The site a URL is on.", pinot="SPLIT_PART({{ column }}, '/', 3)"),
    ]
    patched = apply_macros(WIKI_MACROS, changes, RESERVED)

    folder = tmp_path / "macros"
    folder.mkdir()
    for name, text in (WIKI_MACROS | patched).items():
        (folder / name).write_text(text)
    macros = ProjectMacros(folder)
    rendered = {engine: macros.render("{{ human_edit() }} / {{ url_host('url') }}", engine) for engine in ENGINES}
    assert rendered == {
        "duckdb": "is_bot = false AND NOT starts_with(editor, '~') / regexp_extract(url, '^https?://([^/]+)', 1)",
        "trino": "is_bot = false AND editor NOT LIKE '~%' / regexp_extract(url, '^https?://([^/]+)', 1)",
        "pinot": "is_bot = false AND NOT starts_with(editor, '~') / SPLIT_PART(url, '/', 3)",
    }
    descriptions = {info.name: (info.description, info.files) for info in macros.catalog() if info.name in {c.name for c in changes}}
    assert descriptions == {
        "human_edit": ("A person with a permanent account.", ["project/default.sql"]),
        "not_temporary": ("Not a temporary account.", ["project/default.sql", "project/trino.sql"]),
        "url_host": ("The site a URL is on.", ["project/default.sql", "project/pinot.sql"]),
    }


@pytest.mark.parametrize("name", ["percentile", "quote", "time_bucket"])
def test_built_in_names_are_refused(name):
    with pytest.raises(ValueError, match=f"^macro '{name}': a built-in has that name; choose another$"):
        apply_macros(WIKI_MACROS, [macro(name, "1")], RESERVED)


@pytest.mark.parametrize(
    ("bad", "message"),
    [
        (macro("Human-Edit", "1"), "macro 'Human-Edit': a macro's name is lowercase letters, digits and underscores, not starting with a digit"),
        (macro("2nd", "1"), "macro '2nd': a macro's name is lowercase letters"),
        (macro("host", "1", ["Column"]), "macro 'host': argument 'Column' isn't a name; use lowercase letters, digits and underscores"),
        (macro("host", "1", ["a", "b", "a"]), "macro 'host': argument 'a' appears more than once"),
        (macro("host", "1", description=" \n "), "macro 'host': its description is empty; say what the macro means, for people and agents"),
        (macro("host", "1", description="Ends #} early."), "macro 'host': its description can't contain {# or #}"),
        (macro("host", " "), "macro 'host': its SQL is empty"),
        (macro("host", "1", trino=""), "macro 'host': its Trino version is empty; give null where Trino uses the SQL for every engine"),
        (macro("host", "1{% endmacro %}{% macro other() %}2"), "macro 'host': its SQL can't define macros ({% macro or {% endmacro); give each macro its own change"),
        (macro("host", "1", pinot="{%- macro x() %}"), "macro 'host': its Pinot version can't define macros"),
        (macro("host", "{{ column"), "macro 'host': its SQL isn't valid Jinja: "),
        (macro("host", "1", duckdb="{% if x %}2"), "macro 'host': its DuckDB version isn't valid Jinja: "),
        (macro("none", "1"), "macro 'none': Jinja reads that name as a constant; choose another"),
        (macro("host", "1", ["true"]), "macro 'host': argument 'true' is a name Jinja reads as a constant; choose another"),
    ],
)
def test_a_macro_change_that_cant_apply_is_explained(bad, message):
    with pytest.raises(ValueError) as raised:
        apply_macros(WIKI_MACROS, [bad], RESERVED)
    assert message in str(raised.value)


def test_a_macro_named_by_two_changes_is_refused_with_the_other_problems():
    changes = [macro("host", "1"), macro("host", "2"), macro("percentile", "3")]
    with pytest.raises(ValueError) as raised:
        apply_macros(WIKI_MACROS, changes, RESERVED)
    assert str(raised.value) == (
        "macro 'percentile': a built-in has that name; choose another; "
        "macro 'host': more than one change has that name; give it one change, with every version"
    )


# --- the diff, and the whole proposal


@pytest.fixture
def repo(tmp_path, monkeypatch):
    """A git checkout holding a copy of the playback example's config, as the working directory."""
    root = tmp_path / "repo"
    shutil.copytree(PLAYBACK, root / "config")
    subprocess.run(["git", "init", "-q"], cwd=root, check=True)
    monkeypatch.chdir(root)
    return root


def git_apply(repo: Path, patch: str) -> None:
    (repo / "drift.patch").write_text(patch)
    subprocess.run(["git", "apply", "--verbose", "drift.patch"], cwd=repo, check=True, capture_output=True)


def test_the_diff_creates_files_and_handles_missing_final_newlines(repo):
    (repo / "config" / "notes.txt").write_text("one\ntwo")  # no final newline
    patched = {
        "notes.txt": "one\nthree\n",
        "macros/default.sql": "{# A. #}\n{% macro a() %}1{% endmacro %}",  # a new file, in a new folder, without one
        "macros/trino.sql": "{% macro a() %}2{% endmacro %}\n",
    }
    patch = config_diff("config", patched)

    assert "--- /dev/null\n+++ b/config/macros/trino.sql\n" in patch
    assert "--- a/config/notes.txt\n+++ b/config/notes.txt\n" in patch
    git_apply(repo, patch)
    assert {name: (repo / "config" / name).read_text() for name in patched} == patched


def test_a_proposal_applies_with_git_apply_and_loads(repo):
    mobile = macro(
        "mobile_device", "{{ device }} IN ('ios', 'android')", ["device"], "A phone or tablet: an iOS or Android client.",
        trino="contains(ARRAY['ios', 'android'], {{ device }})",
    )
    request = proposal(
        changes=[
            change("is_mobile_device", "dimension", model="playback_telemetry",
                   sql="CASE WHEN {{ mobile_device('client_device') }} THEN true ELSE false END"),
            change("rebuffer_event_count", model="playback_telemetry", description="Playback events that buffered for more than 250 ms.",
                   filters=["buffer_duration_ms > 250"]),
        ],
        dimensions=[dimension("device", sql="client_device", label="Device")],
        metrics=[metric("mobile_events", filters=["{{ mobile_device('client_device') }}"], label="Mobile events")],
        macros=[mobile],
    )
    config = Path("config")
    sources = {path.name: path.read_text() for path in (config / "macros").glob("*.sql")}
    patched = {"ontology.yaml": apply_ontology((config / "ontology.yaml").read_text(), request)}
    patched |= {f"macros/{name}": text for name, text in apply_macros(sources, request.macros, RESERVED).items()}

    git_apply(repo, config_diff(config, patched))
    platform = load_platform(repo / "config")
    assert {name: (repo / "config" / name).read_text() for name in patched} == patched
    assert list(patched) == ["ontology.yaml", "macros/default.sql", "macros/trino.sql"]
    model = platform.ontology.models[0]
    assert {"device", "is_mobile_device"} <= {d.name for d in model.dimensions}
    assert platform.ontology.macros.render("{{ mobile_device('client_device') }}", "trino") == "contains(ARRAY['ios', 'android'], client_device)"
    rebuffers = next(m for m in model.metrics if m.name == "rebuffer_event_count")
    assert (rebuffers.filters, rebuffers.label) == (["buffer_duration_ms > 250"], "Rebuffer events")
    assert "# Semantic contract for the engine" in (repo / "config" / "ontology.yaml").read_text()
