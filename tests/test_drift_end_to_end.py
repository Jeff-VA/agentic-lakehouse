"""The drift agent end to end (docs/design/drift-agent.md): producers start flagging live streams;
the agent finds the new column in Iceberg, a canned Claude answer changes the project's rebuffer
macro and a metric's description, and the patch it writes shows the effect, applies with git
apply, and leaves a project that loads."""

import json
import shutil
import subprocess
from datetime import timedelta
from pathlib import Path

import pytest
from pyiceberg.catalog.sql import SqlCatalog
from test_agent_sync import claude
from test_flush import T0, FakeConsumer, event

from agentic_lakehouse.agent_sync import DefinitionChange, MacroChange, Proposal, ProposedDimension, check_drift
from agentic_lakehouse.config import load_platform
from agentic_lakehouse.flush import flush
from agentic_lakehouse.schema_compiler import iceberg_partition_spec, iceberg_schema

SAMPLE_CONFIG = Path(__file__).parents[1] / "examples" / "playback" / "config"
NOW = T0 + timedelta(days=8)  # the recent window is day 7, the earlier one days 0 to 6
MODEL = "playback_telemetry"
REBUFFER = (
    "{# A playback event that buffered long enough for viewers to notice. #}\n"
    "{% macro rebuffer() %}buffer_duration_ms > 200{% endmacro %}\n"
)


def answer(macro_sql: str) -> Proposal:
    """Claude's answer: model the flag, and change the rebuffer rule to `macro_sql`."""
    return Proposal(
        summary="Producers now flag live streams, which buffer by design while they wait for the live edge.",
        dimensions=[
            ProposedDimension(
                signals=["S1"], model=MODEL, name="is_live", type="boolean", description="Whether the stream was live.",
                sql=None, label="Live",
            )
        ],
        metrics=[],
        changes=[
            DefinitionChange(
                signals=["S1"], model=MODEL, kind="metric", name="rebuffer_event_count",
                description="Playback events that buffered for more than 200 ms, except on live streams.",
                sql=None, filters=None, reason="It counts what the rebuffer macro counts, which now leaves live streams out.",
            )
        ],
        macros=[
            MacroChange(
                signals=["S1"], name="rebuffer", args=[],
                description="A playback event that buffered long enough for viewers to notice. Live streams buffer by design, so they don't count.",
                sql=macro_sql, pinot=None, trino=None, duckdb=None,
                reason="Live streams wait for the live edge; counting that as rebuffering blames the network for it.",
            )
        ],
        unchanged=[],
    )  # fmt: skip


@pytest.fixture
def project(tmp_path, monkeypatch) -> Path:
    """The playback example in a git checkout, with its rebuffer rule named once, as a macro."""
    root = tmp_path / "repo"
    config = shutil.copytree(SAMPLE_CONFIG, root / "config")
    (config / "macros").mkdir()
    (config / "macros" / "default.sql").write_text(REBUFFER)
    ontology = config / "ontology.yaml"
    text = ontology.read_text()
    assert text.count('- "buffer_duration_ms > 200"') == 1
    ontology.write_text(text.replace('- "buffer_duration_ms > 200"', '- "{{ rebuffer() }}"'))
    subprocess.run(["git", "init", "-q"], cwd=root, check=True)
    subprocess.run(["git", "add", "config"], cwd=root, check=True)
    subprocess.run(["git", "-c", "user.name=test", "-c", "user.email=test@example.com", "commit", "-qm", "start"], cwd=root, check=True)
    monkeypatch.chdir(root)
    return root


@pytest.fixture
def catalog(project, tmp_path) -> SqlCatalog:
    """raw.telemetry: six days of events, one in ten rebuffering; then a day on which producers also
    send is_live (a third of the events), one in five rebuffering."""
    platform = load_platform(project / "config")
    dataset = platform.dataset("telemetry")
    catalog = SqlCatalog("test", uri=f"sqlite:///{tmp_path}/catalog.db", warehouse=f"file://{tmp_path}")
    catalog.create_namespace("raw")
    table = catalog.create_table(
        "raw.telemetry", schema=iceberg_schema(dataset), partition_spec=iceberg_partition_spec(dataset, platform.iceberg_table("telemetry"))
    )
    week = [event(60 + 10 * i, buffer_duration_ms=300 if i % 10 == 0 else 0) for i in range(800)]
    day = [event(7 * 1440 + 60 + 2 * i, buffer_duration_ms=300 if i % 5 == 0 else 0, is_live=i % 3 == 0) for i in range(600)]
    for published in (week, week + day):  # two flushes: the day's events in files of their own
        flush(table, FakeConsumer(dataset.topic, {0: published}), dataset, now=NOW, lateness=timedelta(0))
    return catalog


def test_a_macro_change_goes_from_drift_to_an_applied_patch(project, catalog):
    out = project / "proposals"
    platform = load_platform(project / "config")
    [patch] = check_drift(platform, "config", out, catalog=catalog, client=claude(answer("buffer_duration_ms > 200 AND NOT coalesce(is_live, false)")), log=print)
    text = patch.read_text()

    assert "#   S1 new column is_live (boolean)" in text
    assert "#      answered by: dimension is_live, change to metric rebuffer_event_count, macro rebuffer()" in text
    # Of the day's 120 rebuffers, 40 were on live streams; the week before had no flag, so it keeps its 80.
    for lines in (
        "#   metric playback_telemetry.rebuffer_event_count (changed)\n#     recent: 120 → 80\n#     earlier: unchanged (80)\n",
        "#   metric playback_telemetry.rebuffer_incidence_rate (divides rebuffer_event_count)\n#     recent: 20.0% → 13.3%\n",
        "#   views: isp_now (rebuffer_incidence_rate); quality_overall (rebuffer_incidence_rate)\n",
        "#   dashboards: playback (draws quality_overall, isp_now)\n",
    ):
        assert lines in text
    assert "rewrites history" not in text
    assert json.loads((out / "ledger.json").read_text())["telemetry:new_column:is_live"]["proposal"] == patch.name

    subprocess.run(["git", "apply", str(patch)], cwd=project, check=True)
    applied = load_platform(project / "config")
    assert applied.ontology.macros.render("{{ rebuffer() }}", "trino") == "buffer_duration_ms > 200 AND NOT coalesce(is_live, false)"
    assert "is_live" in {c.name for c in applied.dataset("telemetry").columns}
    model = applied.ontology.models[0]
    assert "is_live" in {d.name for d in model.dimensions}
    [metric] = [m for m in model.metrics if m.name == "rebuffer_event_count"]
    assert metric.description == "Playback events that buffered for more than 200 ms, except on live streams."
    assert check_drift(applied, "config", out, catalog=catalog, client=claude(), log=print) == []  # nothing left to ask


def test_a_change_that_rewrites_history_is_flagged(project, catalog):
    platform = load_platform(project / "config")
    [patch] = check_drift(platform, "config", project / "proposals", catalog=catalog, client=claude(answer("buffer_duration_ms > 400")), log=print)
    assert "#     earlier: 80 → 0  ← rewrites history\n" in patch.read_text()
