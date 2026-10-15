import json
import shutil
import subprocess
from pathlib import Path

import anthropic
import httpx2
import pytest
from pyiceberg.catalog.sql import SqlCatalog
from test_flush import NOW, FakeConsumer, event

from agentic_lakehouse.agent_sync import (
    MODEL,
    Answer,
    DefinitionChange,
    DriftAgentError,
    MacroChange,
    Proposal,
    ProposedDimension,
    ProposedMetric,
    Unchanged,
    check_drift,
    detect_drift,
    propose_changes,
    render_patch,
    unmodeled_columns,
)
from agentic_lakehouse.config import load_platform
from agentic_lakehouse.flush import flush
from agentic_lakehouse.orchestrator import sync_iceberg_columns
from agentic_lakehouse.schema_compiler import iceberg_partition_spec, iceberg_schema

SAMPLE_CONFIG = Path(__file__).parents[1] / "examples" / "playback" / "config"
PLATFORM = load_platform(SAMPLE_CONFIG)
DATASET = PLATFORM.dataset("telemetry")
MODEL_NAME = "playback_telemetry"

# Upstream starts sending three new fields on half the events, plus one whose
# camelCase name config can't express.
EVENTS = [event(0), event(1)] + [
    event(minute, cdn_pop=["den-1", "sea-2"][minute % 2], dropped_frames=minute, is_live=minute > 5, droppedFrames=1)
    for minute in range(2, 10)
]


@pytest.fixture
def catalog(tmp_path):
    catalog = SqlCatalog("test", uri=f"sqlite:///{tmp_path}/catalog.db", warehouse=f"file://{tmp_path}")
    catalog.create_namespace("raw")
    return catalog


@pytest.fixture
def table(catalog):
    """raw.telemetry after the flush loaded EVENTS, adding columns for the new fields."""
    table = catalog.create_table(
        "raw.telemetry",
        schema=iceberg_schema(DATASET),
        partition_spec=iceberg_partition_spec(DATASET, PLATFORM.iceberg_table("telemetry")),
    )
    flush(table, FakeConsumer(DATASET.topic, {0: EVENTS}), DATASET, now=NOW, lateness=NOW - NOW)
    return table


@pytest.fixture
def repo(tmp_path, monkeypatch):
    """A git checkout holding a copy of the sample config, as the working directory."""
    root = tmp_path / "repo"
    shutil.copytree(SAMPLE_CONFIG, root / "config")
    subprocess.run(["git", "init", "-q"], cwd=root, check=True)
    monkeypatch.chdir(root)
    return root


def dimension(**overrides) -> ProposedDimension:
    fields = {
        "signals": ["S1"],
        "model": MODEL_NAME,
        "name": "cdn_pop",
        "type": "string",
        "description": "CDN point of presence that served the stream.",
        "sql": None,
        "label": "CDN PoP",
    }
    return ProposedDimension(**fields | overrides)


def metric(**overrides) -> ProposedMetric:
    fields = {
        "signals": ["S2"],
        "model": MODEL_NAME,
        "name": "avg_dropped_frames",
        "description": "Average video frames dropped per playback event.",
        "type": "aggregate",
        "agg": "avg",
        "sql": "dropped_frames",
        "percentile": None,
        "filters": [],
        "numerator": None,
        "denominator": None,
        "label": None,
        "format": None,
        "unit": "frames",
        "better": "lower",
    }
    return ProposedMetric(**fields | overrides)


def proposal(dimensions=None, metrics=None) -> Proposal:
    return Proposal(
        summary="Upstream added the serving CDN PoP, a dropped-frame count and a live-stream flag.",
        dimensions=[dimension()] if dimensions is None else dimensions,
        metrics=[metric()] if metrics is None else metrics,
        changes=[],
        macros=[],
        unchanged=[Unchanged(signal="S3", reason="Every sampled live event is also a late one; unclear.")],
    )


def claude(*replies, requests: list | None = None) -> anthropic.Anthropic:
    """A real SDK client whose HTTP calls get canned replies: a Proposal, or a raw response dict."""
    replies = iter(replies)

    def respond(request: httpx2.Request) -> httpx2.Response:
        if requests is not None:
            requests.append({"headers": dict(request.headers), "body": json.loads(request.content)})
        reply = next(replies)
        if isinstance(reply, Proposal):  # as Claude gives it: in the answer's schema
            reply = {"stop_reason": "end_turn", "content": [{"type": "text", "text": Answer.from_proposal(reply).model_dump_json()}]}
        return httpx2.Response(
            200,
            json={
                "id": "msg_test",
                "type": "message",
                "role": "assistant",
                "model": MODEL,
                "stop_sequence": None,
                "usage": {"input_tokens": 1, "output_tokens": 1},
                **reply,
            },
        )

    return anthropic.Anthropic(
        api_key="test", max_retries=0, http_client=anthropic.DefaultHttpxClient(transport=httpx2.MockTransport(respond))
    )


def test_detect_drift_profiles_the_new_columns(table):
    report, samples = detect_drift("telemetry", PLATFORM, table)

    profiles = {c.name: c for c in report.new_columns}
    assert list(profiles) == ["cdn_pop", "dropped_frames", "is_live"]
    assert report.unsupported_columns == ["droppedFrames: config needs lowercase snake_case names"]
    assert (profiles["cdn_pop"].type, profiles["cdn_pop"].top_values) == ("string", [("den-1", 4), ("sea-2", 4)])
    assert (profiles["dropped_frames"].type, profiles["dropped_frames"].minimum, profiles["dropped_frames"].maximum) == ("double", 2, 9)
    assert profiles["is_live"].top_values == [(False, 4), (True, 4)]  # ties in value order
    new_rows = {row["session_id"] for row in samples.recent.to_pylist() if row["cdn_pop"] is not None}
    assert new_rows == {f"s{minute}" for minute in range(2, 10)}  # includes the 8 rows where a new column is set
    assert [s.key for s in report.signals] == [f"telemetry:new_column:{name}" for name in profiles]


def test_matching_config_has_no_drift(catalog):
    table = catalog.create_table("raw.telemetry", schema=iceberg_schema(DATASET))
    report, samples = detect_drift("telemetry", PLATFORM, table)
    assert (report.has_drift, samples) == (False, None)


def test_type_mismatches_are_reported(catalog):
    columns = [c.model_copy(update={"type": "long"}) if c.name == "bitrate_kbps" else c for c in DATASET.columns]
    table = catalog.create_table("raw.telemetry", schema=iceberg_schema(DATASET.model_copy(update={"columns": columns})))
    report, _ = detect_drift("telemetry", PLATFORM, table)
    assert report.type_mismatches == ["bitrate_kbps: ingestion.yaml says int, the Iceberg table has long"]


def test_a_valid_proposal_becomes_a_patch_that_applies(table, repo):
    report, samples = detect_drift("telemetry", PLATFORM, table)
    result = propose_changes(report, samples, "config", claude(proposal()), log=lambda m: None)
    patch = render_patch(result, "config")

    assert result.attempts == 1
    assert "+      - {name: cdn_pop, type: string}" in patch
    assert "+      - name: avg_dropped_frames" in patch
    assert "+        unit: frames" in patch and "+        label: CDN PoP" in patch
    assert "#      unchanged: Every sampled live event" in patch
    assert "#      answered by: dimension cdn_pop" in patch
    (repo / "drift.patch").write_text(patch)
    subprocess.run(["git", "apply", "drift.patch"], cwd=repo, check=True)
    applied = load_platform(repo / "config")
    assert {"cdn_pop", "dropped_frames", "is_live"} <= {c.name for c in applied.dataset("telemetry").columns}
    assert "avg_dropped_frames" in {m.name for m in applied.ontology.models[0].metrics}
    assert "# Semantic contract for the engine" in (repo / "config" / "ontology.yaml").read_text()  # comments kept


def test_claude_is_asked_for_structured_output_with_fallback(table, repo):
    requests = []
    report, samples = detect_drift("telemetry", PLATFORM, table)
    propose_changes(report, samples, "config", claude(proposal(), requests=requests), log=lambda m: None)

    [request] = requests
    body = request["body"]
    assert body["model"] == "claude-opus-5-5"
    assert body["output_config"]["effort"] == "high"
    assert body["output_config"]["format"]["type"] == "json_schema"
    assert body["fallbacks"] == "default"
    assert body["thinking"] == {"type": "adaptive"}
    assert "server-side-fallback-2026-07-01" in request["headers"]["anthropic-beta"]
    prompt = body["messages"][0]["content"]
    assert '"name": "dropped_frames"' in prompt and "<ontology_yaml>" in prompt


def test_the_answer_schema_has_nothing_nullable_or_optional(table, repo):
    """Structured outputs compile the schema into a grammar. Each nullable field (an anyOf, or a
    type list) makes it exponentially larger: the API allows 16 in a request, and refused 17."""
    requests = []
    report, samples = detect_drift("telemetry", PLATFORM, table)
    propose_changes(report, samples, "config", claude(proposal(), requests=requests), log=lambda m: None)
    unions, optional = [], []

    def walk(node, path: str) -> None:
        if isinstance(node, dict):
            if "anyOf" in node or "oneOf" in node or isinstance(node.get("type"), list):
                unions.append(path)
            if node.get("type") == "object":
                optional.extend(f"{path}.{p}" for p in node.get("properties", {}) if p not in node.get("required", []))
            for key, value in node.items():
                walk(value, f"{path}.{key}")
        elif isinstance(node, list):
            for i, value in enumerate(node):
                walk(value, f"{path}[{i}]")

    schema = requests[0]["body"]["output_config"]["format"]["schema"]
    walk(schema, "$")
    assert (unions, optional) == ([], [])
    assert len(json.dumps(schema)) > 1000  # it is the answer's schema, not an empty one


def test_an_answer_and_a_proposal_carry_the_same_thing():
    full = proposal(metrics=[metric(), metric(name="p95_frames", agg="percentile", percentile=0.95, label="p95", format="number")])
    full.changes = [
        DefinitionChange(signals=["S2"], model=MODEL_NAME, kind="metric", name="rebuffer_event_count", description=None,
                         sql=None, filters=[], reason="Filters dropped."),
        DefinitionChange(signals=["S2"], model=MODEL_NAME, kind="dimension", name="isp_provider", description="Who.",
                         sql=None, filters=None, reason="Clearer."),
    ]  # fmt: skip
    full.macros = [
        MacroChange(signals=["S1"], name="pop", args=["c"], description="A PoP.", sql="lower({{ c }})", pinot=None,
                    trino="upper({{ c }})", duckdb=None, reason="Why not."),
    ]  # fmt: skip
    assert Answer.from_proposal(full).to_proposal() == full


@pytest.mark.parametrize(
    ("bad", "error"),
    [
        (proposal(metrics=[metric(sql="SUM(dropped_frames)")]), "must not contain an aggregate function"),
        (proposal(metrics=[metric(sql="cdn_pop")]), "metric 'avg_dropped_frames' failed on sampled data"),  # avg of text
        (proposal(dimensions=[dimension(model="nope")]), "there is no model named 'nope'"),
        (proposal(metrics=[metric(sql="frames_dropped")]), "doesn't have: frames_dropped"),  # no such column
    ],
)
def test_invalid_proposals_go_back_to_claude_with_the_errors(table, repo, bad, error):
    requests = []
    report, samples = detect_drift("telemetry", PLATFORM, table)
    result = propose_changes(report, samples, "config", claude(bad, proposal(), requests=requests), log=lambda m: None)

    assert result.attempts == 2
    retry = requests[1]["body"]["messages"]
    assert [m["role"] for m in retry] == ["user", "assistant", "user"]
    assert error in retry[2]["content"]


def test_the_agent_gives_up_after_max_attempts(table, repo):
    report, samples = detect_drift("telemetry", PLATFORM, table)
    bad = proposal(dimensions=[dimension(model="nope")])
    with pytest.raises(DriftAgentError, match="no valid proposal after 2 attempts"):
        propose_changes(report, samples, "config", claude(bad, bad), max_attempts=2, log=lambda m: None)


def test_a_refusal_stops_the_agent(table, repo):
    report, samples = detect_drift("telemetry", PLATFORM, table)
    refusal = {"stop_reason": "refusal", "content": [], "stop_details": {"type": "refusal", "category": None, "explanation": "x"}}
    with pytest.raises(DriftAgentError, match="Claude declined"):
        propose_changes(report, samples, "config", claude(refusal), log=lambda m: None)


def test_check_drift_proposes_each_drift_once(catalog, table, repo):
    out = repo / "proposals"
    [written] = check_drift(PLATFORM, "config", out, catalog=catalog, client=claude(proposal()), log=lambda m: None)
    assert written.parent == out and written.read_text().startswith("# Drift proposal for dataset 'telemetry'")
    # The same drift again: the pending patch is kept and Claude isn't asked (no replies left).
    assert check_drift(PLATFORM, "config", out, catalog=catalog, client=claude(), log=lambda m: None) == []


def test_setup_adds_new_ingestion_columns_to_an_existing_iceberg_table(catalog):
    table = catalog.create_table("raw.telemetry", schema=iceberg_schema(DATASET))
    grown = DATASET.model_copy(update={"columns": [*DATASET.columns, DATASET.columns[1].model_copy(update={"name": "cdn_pop"})]})
    sync_iceberg_columns(table, grown, log=lambda m: None)
    assert table.schema().find_field("cdn_pop").required is False


def test_missing_credentials_are_reported_before_calling_claude(table, repo, monkeypatch):
    for variable in ("ANTHROPIC_API_KEY", "ANTHROPIC_AUTH_TOKEN", "ANTHROPIC_PROFILE", "ANTHROPIC_CONFIG_DIR"):
        monkeypatch.delenv(variable, raising=False)
    monkeypatch.setenv("HOME", str(repo))  # an empty home: no `ant auth login` profile either
    monkeypatch.setenv("XDG_CONFIG_HOME", str(repo))
    report, samples = detect_drift("telemetry", PLATFORM, table)
    with pytest.raises(DriftAgentError, match="no Claude API credentials: put ANTHROPIC_API_KEY"):
        propose_changes(report, samples, "config", log=lambda m: None)


def test_unmodeled_columns_lists_every_field_the_config_lacks(table):
    assert unmodeled_columns(DATASET, table) == ["cdn_pop", "dropped_frames", "is_live", "droppedFrames"]
