"""The drift agent around Claude's answer (docs/design/drift-agent.md): every signal answered,
the header a reviewer reads, the ledger that keeps it from paying twice, and dry runs."""

import json
import sys

import pytest
from test_agent_sync import PLATFORM, catalog, claude, dimension, metric, proposal, repo, table  # noqa: F401 (fixtures)

from agentic_lakehouse import cli
from agentic_lakehouse.agent_sync import (
    DriftAgentError,
    DriftReport,
    Proposal,
    Signal,
    Unchanged,
    check_drift,
    detect_drift,
    propose_changes,
    render_patch,
)

SIGNALS = {"telemetry:new_column:cdn_pop", "telemetry:new_column:dropped_frames", "telemetry:new_column:is_live"}


def quiet(message: str) -> None:
    pass


def feedback(requests: list, attempt: int = 1) -> str:
    """What the agent told Claude about the proposal it sent back on `attempt`."""
    return requests[attempt]["body"]["messages"][-1]["content"]


def test_a_signal_without_an_answer_goes_back_to_claude(table, repo):
    report, samples = detect_drift("telemetry", PLATFORM, table)
    unanswered = proposal().model_copy(update={"unchanged": []})  # nothing answers S3, is_live
    requests = []
    result = propose_changes(report, samples, "config", claude(unanswered, proposal(), requests=requests), log=quiet)
    assert result.attempts == 2
    assert "S3 (new column is_live (boolean): set in 8 of 8 sampled rows" in feedback(requests)
    assert "has no answer: answer it with a change, or list it in unchanged with the reason" in feedback(requests)


def test_an_answer_to_a_signal_the_request_didnt_have_goes_back(table, repo):
    report, samples = detect_drift("telemetry", PLATFORM, table)
    requests = []
    propose_changes(report, samples, "config", claude(proposal(metrics=[metric(signals=["S9"])]), proposal(), requests=requests), log=quiet)
    assert "S9: no such signal; the request's signals are S1, S2, S3" in feedback(requests)
    assert "S2 (" in feedback(requests)  # the metric was S2's only answer


def test_the_header_shows_each_definitions_effect_and_where_engines_disagree(table, repo):
    macros = repo / "config" / "macros"
    macros.mkdir(exist_ok=True)
    (macros / "default.sql").write_text("{# A PoP's code. #}\n{% macro pop_code(column) %}lower({{ column }}){% endmacro %}\n")
    (macros / "trino.sql").write_text("{% macro pop_code(column) %}upper({{ column }}){% endmacro %}\n")  # disagrees
    report, samples = detect_drift("telemetry", PLATFORM, table)
    answer = proposal(dimensions=[dimension(sql="{{ pop_code('cdn_pop') }}")])
    result = propose_changes(report, samples, "config", claude(answer), log=quiet)

    assert result.review["effect"] == [
        "dimension playback_telemetry.cdn_pop (new)",
        "  recent: new: 'den-1' 40.0%, 'sea-2' 40.0%, null 20.0%",  # the window's 10 events; 2 predate cdn_pop
        "metric playback_telemetry.avg_dropped_frames (new)",
        "  recent: new: 5.50",  # dropped_frames 2 to 9
    ]
    [engines] = result.review["engines"]
    assert engines.startswith("dimension playback_telemetry.cdn_pop sql: trino differs on 8 of 10 sampled rows, e.g. duckdb ")
    assert engines.endswith("; pinot agrees on all 10 sampled rows")  # Pinot has no version of its own
    assert result.review["impact"] == []  # new definitions: nothing uses them yet
    patch = render_patch(result, "config")
    assert "# Effect on sampled rows, before → after" in patch and "# Engines: where a definition goes through" in patch
    assert "#   dimension playback_telemetry.cdn_pop (new)\n#     recent: new: 'den-1' 40.0%, 'sea-2' 40.0%, null 20.0%\n" in patch


def test_a_dashboard_that_was_already_broken_doesnt_fail_proposals(table, repo):
    (repo / "config" / "dashboards" / "draft.html").write_text(
        '<script type="module" src="/al.js"></script>\n<al-kpi view="no_such_view" value="x"></al-kpi>\n'
    )
    report, samples = detect_drift("telemetry", PLATFORM, table)
    assert propose_changes(report, samples, "config", claude(proposal()), max_attempts=1, log=quiet).attempts == 1


def test_a_proposal_that_changes_nothing_says_so(table, repo):
    _, samples = detect_drift("telemetry", PLATFORM, table)
    fading = Signal(
        key="telemetry:fading_column:bitrate_kbps",
        kind="fading_column",
        column="bitrate_kbps",
        detail="column bitrate_kbps set on 0% of 5,000 events in the last 24 hours, 100% in the 7 days before",
    )
    report = DriftReport(dataset="telemetry", table="raw.telemetry", signals=[fading])
    answer = Proposal(
        summary="Bitrate stopped arriving a day ago.",
        dimensions=[],
        metrics=[],
        changes=[],
        macros=[],
        unchanged=[Unchanged(signal="S1", reason="Likely an outage upstream: nothing to change yet.")],
    )
    result = propose_changes(report, samples, "config", claude(answer), log=quiet)
    assert result.patched == {}
    patch = render_patch(result, "config")
    assert "#      unchanged: Likely an outage upstream" in patch
    assert patch.endswith("# Nothing to apply: this proposal changes no files. Delete it once you've read it.\n")


def test_the_ledger_stops_repeat_questions_even_after_a_patch_is_deleted(catalog, table, repo):
    out = repo / "proposals"
    [patch] = check_drift(PLATFORM, "config", out, catalog=catalog, client=claude(proposal()), log=quiet)
    entries = json.loads((out / "ledger.json").read_text())
    assert set(entries) == SIGNALS
    assert {(e["outcome"], e["proposal"]) for e in entries.values()} == {("proposed", patch.name)}

    patch.unlink()  # rejected; claude() has no replies, so asking again would fail
    assert check_drift(PLATFORM, "config", out, catalog=catalog, client=claude(), log=quiet) == []
    [again] = check_drift(PLATFORM, "config", out, catalog=catalog, client=claude(proposal()), log=quiet, again=True)
    assert again == patch and again.exists()


def test_a_failure_is_recorded_so_it_isnt_paid_for_every_hour(catalog, table, repo):
    out = repo / "proposals"
    bad = proposal(dimensions=[dimension(model="nope")])
    with pytest.raises(DriftAgentError, match="telemetry: no valid proposal after 3 attempts"):
        check_drift(PLATFORM, "config", out, catalog=catalog, client=claude(bad, bad, bad), log=quiet)
    entries = json.loads((out / "ledger.json").read_text())
    assert {e["outcome"] for e in entries.values()} == {"failed"}
    assert "there is no model named 'nope'" in entries["telemetry:new_column:cdn_pop"]["error"]
    assert check_drift(PLATFORM, "config", out, catalog=catalog, client=claude(), log=quiet) == []


def test_drift_that_never_reached_claude_isnt_recorded(catalog, table, repo, monkeypatch):
    for variable in ("ANTHROPIC_API_KEY", "ANTHROPIC_AUTH_TOKEN", "ANTHROPIC_PROFILE", "ANTHROPIC_CONFIG_DIR"):
        monkeypatch.delenv(variable, raising=False)
    monkeypatch.setenv("HOME", str(repo))
    monkeypatch.setenv("XDG_CONFIG_HOME", str(repo))
    with pytest.raises(DriftAgentError, match="no Claude API credentials"):
        check_drift(PLATFORM, "config", repo / "proposals", catalog=catalog, log=quiet)
    assert not (repo / "proposals" / "ledger.json").exists()  # nothing was paid for: the next run asks


def test_an_unreadable_ledger_stops_the_agent_rather_than_asking_about_everything(catalog, table, repo):
    (repo / "proposals").mkdir()
    (repo / "proposals" / "ledger.json").write_text("{not json")
    with pytest.raises(DriftAgentError, match="ledger.json can't be read"):
        check_drift(PLATFORM, "config", repo / "proposals", catalog=catalog, client=claude(), log=quiet)


def test_a_dry_run_shows_the_drift_and_asks_nothing(catalog, table, repo):
    logs = []
    assert check_drift(PLATFORM, "config", repo / "proposals", catalog=catalog, client=claude(), log=logs.append, dry_run=True) == []
    assert not (repo / "proposals").exists()
    assert "drift: telemetry: S1 new column cdn_pop (string): set in 8 of 8 sampled rows, most often den-1 (4), sea-2 (4)" in logs
    assert any("would ask claude-opus-5-5 about 3 signal(s)" in line for line in logs)


def test_the_command_runs_a_dry_run_without_prefect(repo, monkeypatch):
    calls = []
    monkeypatch.setattr("agentic_lakehouse.agent_sync.check_drift", lambda *args, **kwargs: calls.append(kwargs))
    monkeypatch.setattr(sys, "argv", ["agentic-lakehouse", "drift", "--dry-run"])
    cli.main()
    assert calls == [{"dry_run": True}]
    monkeypatch.setattr(sys, "argv", ["agentic-lakehouse", "drift", "--serve", "--again"])
    with pytest.raises(SystemExit):
        cli.main()
