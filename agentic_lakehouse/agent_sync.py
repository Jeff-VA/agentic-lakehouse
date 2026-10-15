"""The drift agent: keeps a project's ontology and macros in step with what producers send.

When producers start sending a new event field, the flush (agentic_lakehouse/flush.py) adds
it to the Iceberg table as a column; when they stop sending one, or send a value the
definitions don't expect, the data shows that too. For each dataset, this agent:

1. Detects drift (drift/signals.py) as signals: new columns, fading columns, and new values
   in columns that definitions compare with text. Type mismatches and columns config can't
   express are reported for a person.
2. Asks Claude what each signal should change (drift/proposal.py): new dimensions and
   metrics, changes to existing ones, new macros or new versions of the project's own. Every
   signal gets an answer: a change, or the reason for none. Adding new columns to
   ingestion.yaml is mechanical and done here.
3. Validates the proposal: the patched config must load (every snippet rendered for every
   engine), every dashboard must still check against its views, and every definition the
   proposal touches must run on DuckDB over sampled rows. Errors go back to Claude, up to
   `max_attempts` times.
4. Writes a patch (drift/patch.py) for a person to review and `git apply`, headed by what it
   would do (drift/review.py): the effect on sampled rows, whether the engines' versions of
   changed macros agree, and the views, dashboards and steps it affects. It never edits config
   in place.
5. Records each signal it asked about in proposals/ledger.json, so the same drift is never
   asked about, and paid for, twice.

Design: docs/design/drift-agent.md.
"""

import hashlib
import json
import re
import shutil
import tempfile
import textwrap
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path

import anthropic
import pyarrow.compute as pc
from pydantic import ValidationError
from pyiceberg.catalog import Catalog

from agentic_lakehouse.compiler import MetricQuery, QueryError, SqlCompiler
from agentic_lakehouse.config import ConfigError, Platform, load_platform
from agentic_lakehouse.dashboards import check_dashboards
from agentic_lakehouse.drift import review
from agentic_lakehouse.drift.patch import add_ingestion_columns, apply_macros, apply_ontology, config_diff
from agentic_lakehouse.drift.proposal import (  # noqa: F401
    DefinitionChange,
    MacroChange,
    Proposal,
    ProposedDimension,
    ProposedMetric,
    Unchanged,
)
from agentic_lakehouse.drift.signals import (  # noqa: F401
    ColumnProfile,
    DriftReport,
    DriftSamples,
    Signal,
    detect_drift,
    new_column_signal,
    unmodeled_columns,
)
from agentic_lakehouse.engines import DuckDBEngine, EngineError
from agentic_lakehouse.macros import MacroError, ProjectMacros
from agentic_lakehouse.orchestrator import iceberg_catalog

MODEL = "claude-opus-5-5"
LEDGER = "ledger.json"  # in the proposals folder: every signal Claude was asked about

Log = Callable[[str], None]


class DriftAgentError(RuntimeError):
    """The agent couldn't produce a valid proposal."""

    def __init__(self, message: str, *, asked: bool = False):
        super().__init__(message)
        self.asked = asked  # Claude was asked, and paid, before this failed


@dataclass
class DriftProposal:
    report: DriftReport
    proposal: Proposal | None  # None when Claude wasn't asked
    patched: dict[str, str]  # config file, relative to the config folder -> proposed text
    attempts: int
    review: dict[str, list[str]] = field(default_factory=dict)  # "effect", "engines", "impact": lines for the header

    @property
    def numbered(self) -> dict[str, Signal]:
        """The report's signals by the numbers the request gave them: S1, S2, ..."""
        return _numbered(self.report)


SYSTEM_PROMPT = """\
You keep a lakehouse's semantic layer in step with the data its producers send. The semantic \
layer is the ontology, config/ontology.yaml, which defines the dimensions and metrics that \
people and AI agents query, and the project's SQL macros, config/macros/, which name business \
definitions once and hold SQL that differs by engine. Checks of one dataset found drift. Each \
finding is a signal, numbered S1, S2, and so on. Decide what each signal should change, if \
anything, and propose those changes.

The signals:
- New column: producers started sending a field, which is being added to ingestion.yaml. Model \
it if someone would plausibly slice by it or measure it, or if an existing definition should \
use it.
- Fading column: a field that used to be set now is far less often, or not at all. Decide \
whether the definitions that read it should change: to read a field that replaced it, for \
example, or to say in their descriptions that it stopped arriving, and when.
- New value: a text column that definitions compare with literal values has a value they don't \
mention. Decide whether those definitions should handle it.

You can propose new dimensions and metrics; changes to existing ones (a description, a \
dimension's or metric's sql, a metric's filters); and new macros, or new versions of the \
project's own. Each item names the signals it answers, and every signal needs an answer: a \
signal that needs no change goes in `unchanged`, with the reason.

These rules are enforced; a proposal that breaks one comes back to you with the errors:
- Names are lowercase snake_case and unique within a model across dimensions and metrics. \
Metric names are unique across all models. Existing names never change, and nothing is \
removed: views and dashboards use the names, and history still holds the data.
- A definition applies to all of history, in the real-time tier and the historical one alike. \
A change must also be right for rows from before the drift: coalesce(new_name, old_name) for a \
renamed field, for example, or a column every row has rather than one only new rows have.
- A dimension has a type (string, number, boolean or timestamp) and an optional `sql` \
expression over source columns; leave `sql` null when the dimension is the column of the same \
name.
- An aggregate metric has `agg` (count, count_distinct, sum, avg, min, max or percentile), the \
`sql` expression it aggregates (null only for count, which then counts rows), `percentile` as a \
fraction such as 0.95 (only with agg percentile), and `filters`: SQL predicates, ANDed, that \
select which rows count.
- A ratio metric divides two aggregate metrics of the same model, named in `numerator` and \
`denominator`.
- Ontology SQL is DuckDB's dialect: one scalar expression over source columns, with no \
aggregate functions (`agg` applies the aggregation), subqueries or window functions. It can call \
macros with Jinja: {{ name() }}, or {{ name('column_name') }}, where each argument is SQL text \
in quotes. A snippet that calls a macro is rendered for each engine and read in that engine's \
dialect, so the SQL around the call must be plain SQL that every engine reads.
- A macro's body is a SQL expression in which {{ argument }} inserts an argument. Its SQL for \
every engine (`sql`) must be SQL that DuckDB, Trino and Pinot all read; give an engine its own \
version only where its SQL must differ. A macro change states every version the macro should \
have: an existing engine-specific version you leave null is removed. A macro can't take the \
name of one of the engine's built-ins, listed in the request.
- When a rule decides what several definitions count, name it once as a macro and call it. \
When a macro already defines what you need, call it rather than writing its SQL out again.
- Every dimension, metric and macro needs a description: what it means, in what unit, and \
anything surprising. Descriptions are how people and agents decide what to query. A label is \
how people say a name, in charts and tables; give metrics a format and unit where they help.

Low-cardinality text and booleans make good dimensions. Numeric measures make good metrics, \
with an aggregation that answers a real question (e.g. an average or p95 for latencies, a sum \
for totals); identifiers suit count_distinct, or nothing. Prefer a few well-described \
definitions to one per column, and don't duplicate existing ones. Use the profiles to choose \
types and infer units."""


def _numbered(report: DriftReport) -> dict[str, Signal]:
    return {f"S{i}": signal for i, signal in enumerate(report.signals, 1)}


def propose_changes(
    report: DriftReport,
    samples: DriftSamples,
    config_dir: str | Path,
    client: anthropic.Anthropic | None = None,
    *,
    max_attempts: int = 3,
    log: Log = print,
) -> DriftProposal:
    """Config changes for `report`'s signals: ingestion.yaml gets every new column, and the
    ontology and macros get the changes Claude proposes and the validation accepts."""
    config_dir = Path(config_dir)
    texts = {name: (config_dir / name).read_text() for name in ("ingestion.yaml", "ontology.yaml")}
    macros = ProjectMacros(config_dir / "macros")
    reserved = {m.name for m in macros.catalog() if any(f.startswith("engine/") for f in m.files)}
    base = {}
    if report.new_columns:
        base["ingestion.yaml"] = add_ingestion_columns(texts["ingestion.yaml"], report.dataset, report.new_columns)
    client = _client(client)
    before = load_platform(config_dir)
    numbered = _numbered(report)
    messages = [{"role": "user", "content": _request(report, texts, macros, reserved)}]
    errors: list[str] = []
    for attempt in range(1, max_attempts + 1):
        log(f"drift: asking {MODEL} for a proposal (attempt {attempt} of {max_attempts})")
        response = _ask(client, messages, log)
        proposal = response.parsed_output
        patched = dict(base)
        findings: dict[str, list[str]] = {}
        try:
            errors = _answer_errors(proposal, numbered)
            ontology = apply_ontology(texts["ontology.yaml"], proposal)
            if ontology != texts["ontology.yaml"]:
                patched["ontology.yaml"] = ontology
            for name, text in apply_macros(macros.sources, proposal.macros, reserved).items():
                patched[f"macros/{name}"] = text
            if not errors:
                errors, findings = validate(config_dir, patched, report.dataset, samples, proposal, before)
        except ValueError as e:
            errors = [*errors, str(e)]
        if not errors:
            return DriftProposal(report, proposal, patched, attempt, findings)
        log(f"drift: the proposal failed validation: {'; '.join(errors)[:300]}")
        messages += [
            {"role": "assistant", "content": response.content},
            {
                "role": "user",
                "content": "That proposal failed validation:\n"
                + "\n".join(f"- {e}" for e in errors)
                + "\n\nReturn a corrected, complete proposal.",
            },
        ]
    raise DriftAgentError(f"no valid proposal after {max_attempts} attempts; last errors: {errors}", asked=True)


def _client(client: anthropic.Anthropic | None) -> anthropic.Anthropic:
    try:
        client = client or anthropic.Anthropic()
    except anthropic.CredentialsError as e:  # e.g. a configured profile that can't be read
        raise DriftAgentError(f"Claude API credentials: {e}") from e
    if client.api_key is None and client.auth_token is None and client.credentials is None:
        raise DriftAgentError(
            "no Claude API credentials: put ANTHROPIC_API_KEY=... in .env (git-ignored) or the environment"
        )
    return client


def _ask(client: anthropic.Anthropic, messages: list[dict], log: Log):
    try:
        response = client.beta.messages.parse(
            model=MODEL,
            max_tokens=16000,
            system=SYSTEM_PROMPT,
            messages=messages,
            output_format=Proposal,
            # Changes can rewrite history, so this is where thinking pays.
            thinking={"type": "adaptive"},
            output_config={"effort": "high"},  # Claude Opus 5.5 defaults to medium
            cache_control={"type": "ephemeral"},  # retries resend the same prefix
            betas=["server-side-fallback-2026-07-01"],
            fallbacks="default",  # if a safety classifier declines, retry on the recommended model
        )
    except anthropic.AuthenticationError as e:
        raise DriftAgentError("Claude API rejected the credentials: set ANTHROPIC_API_KEY (e.g. in .env)") from e
    except anthropic.APIConnectionError as e:
        raise DriftAgentError(f"couldn't reach the Claude API: {e}") from e
    except anthropic.APIStatusError as e:  # rate limits and 5xx were already retried by the SDK
        raise DriftAgentError(f"Claude API error {e.status_code}: {e.message}") from e
    usage = response.usage
    log(
        f"drift: {response.model} used {usage.input_tokens:,} input tokens, "
        f"{usage.cache_creation_input_tokens or 0:,} written to the cache and {usage.cache_read_input_tokens or 0:,} "
        f"read from it, and {usage.output_tokens:,} output tokens"
    )
    if response.stop_reason == "refusal":
        raise DriftAgentError(f"Claude declined to propose changes: {response.stop_details}", asked=True)
    if response.stop_reason == "max_tokens":
        raise DriftAgentError("Claude's proposal was cut off at max_tokens", asked=True)
    return response


def _request(report: DriftReport, texts: dict[str, str], macros: ProjectMacros, reserved: set[str]) -> str:
    until = f", up to {report.window_end:%Y-%m-%d %H:%M} UTC" if report.window_end else ""
    signals = []
    for number, signal in _numbered(report).items():
        signals.append(f"{number} {signal.detail}")
        if signal.read_by:
            signals.append(f"   Read by: {'; '.join(signal.read_by)}")
    parts = [f"Dataset `{report.dataset}` (Iceberg table {report.table}). Drift found in its recent data{until}:"]
    parts.append("<signals>\n" + "\n".join(signals) + "\n</signals>")
    if report.notes:
        parts.append("What the checks couldn't judge:\n<notes>\n" + "\n".join(f"- {n}" for n in report.notes) + "\n</notes>")
    if report.new_columns:
        profiles = "[\n" + ",\n".join(p.model_dump_json(indent=2) for p in report.new_columns) + "\n]"
        parts.append(
            "The new columns are being added to ingestion.yaml. Their profiles, from sampled rows where at least "
            f"one of them is set:\n<new_columns>\n{profiles}\n</new_columns>"
        )
    entry = _dataset_entry(texts["ingestion.yaml"], report.dataset)
    parts.append(f"The dataset's entry in ingestion.yaml; its comments describe the columns:\n<ingestion_yaml>\n{entry}</ingestion_yaml>")
    parts.append(f"The current ontology:\n<ontology_yaml>\n{texts['ontology.yaml']}</ontology_yaml>")
    if macros.files:
        files = "\n".join(f'<macro_file name="{name}.sql">\n{macros.sources[f"{name}.sql"]}</macro_file>' for name in macros.files)
        parts.append(f"The project's macros, in config/macros/:\n{files}")
    else:
        parts.append("The project has no macros of its own yet. A new one goes in config/macros/default.sql.")
    parts.append(f"The engine's built-in macros, whose names a project macro can't take: {', '.join(sorted(reserved))}.")
    return "\n\n".join(parts)


def _dataset_entry(text: str, dataset: str) -> str:
    """The dataset's block of ingestion.yaml, comments included; the whole file if it can't be found."""
    lines = text.splitlines(keepends=True)
    heading = re.compile(rf"^(\s*)- name:\s*['\"]?{re.escape(dataset)}['\"]?\s*(#.*)?$")
    start = next((i for i, line in enumerate(lines) if heading.match(line.rstrip("\n"))), None)
    if start is None:
        return text
    indent = len(lines[start]) - len(lines[start].lstrip())
    end = start + 1
    while end < len(lines):
        line = lines[end]
        if line.strip() and not line.strip().startswith("#") and len(line) - len(line.lstrip()) <= indent:
            break
        end += 1
    return "".join(lines[start:end])


def _answer_errors(proposal: Proposal, numbered: dict[str, Signal]) -> list[str]:
    """Every signal needs an answer, and an answer can only name a signal the request had."""
    answers = proposal.answers()
    errors = []
    if unknown := [number for number in answers if number not in numbered]:
        errors.append(f"{', '.join(unknown)}: no such signal; the request's signals are {', '.join(numbered)}")
    for number, signal in numbered.items():
        if number not in answers:
            errors.append(
                f"{number} ({signal.detail[:100]}) has no answer: answer it with a change, or list it in unchanged with the reason"
            )
    return errors


def validate(
    config_dir: Path, patched: dict[str, str], dataset_name: str, samples: DriftSamples, proposal: Proposal, before: Platform
) -> tuple[list[str], dict[str, list[str]]]:
    """Problems with the patched config, and when there are none, what the reviewer reads about
    it. The patched config must load, with the project's macros; no dashboard may have a problem
    it didn't have before; and every definition the proposal touches must run on DuckDB over the
    sampled rows."""
    with tempfile.TemporaryDirectory() as tmp:
        copy = Path(tmp) / "config"
        shutil.copytree(config_dir, copy)  # views/ and dashboards/ too, so breaking one fails
        for name, text in patched.items():
            (copy / name).parent.mkdir(parents=True, exist_ok=True)
            (copy / name).write_text(text)
        try:
            after = load_platform(copy)
        except (ValidationError, ConfigError) as e:
            return [str(e)], {}
        datasets = [d.name for d in after.ingestion.datasets]
        # Only problems the proposal makes: a dashboard someone is still building isn't Claude's to fix.
        known = {str(problem) for problem in check_dashboards(config_dir / "dashboards", before.views, datasets)}
        errors = [str(problem) for problem in check_dashboards(copy / "dashboards", after.views, datasets) if str(problem) not in known]
        targets = review.targets(proposal, after)
        errors += _run(after, dataset_name, samples, targets)
        if errors:
            return errors, {}
        return [], {
            "effect": review.effect(before, after, dataset_name, samples, targets),
            "engines": review.engine_agreement(after, dataset_name, samples, targets),
            "impact": review.impact(after, targets, proposal),
        }


def _run(platform: Platform, dataset: str, samples: DriftSamples, targets: list[review.Target]) -> list[str]:
    """Each target in the dataset's models, run on DuckDB over each window's sampled rows."""
    models = {m.name: m for m in platform.ontology.models if m.source == dataset}
    compiler = SqlCompiler(platform.ontology)
    time_column = platform.dataset(dataset).time_column
    failed: dict[review.Target, str] = {}
    for label, rows in (("recent", samples.recent), ("earlier", samples.earlier)):
        if not rows.num_rows:
            continue
        engine = DuckDBEngine()
        engine.con.register("sample", rows)
        times = rows.column(time_column)
        window = {"start": pc.min(times).as_py(), "end": pc.max(times).as_py() + timedelta(milliseconds=1)}
        for target in targets:
            if target.model not in models or target in failed:
                continue
            model = models[target.model]
            if target.kind == "metric":
                fields = {"metrics": [target.name]}
            elif target.name == model.time_dimension:
                fields = {"metrics": [model.metrics[0].name], "grain": "hour"}
            else:
                fields = {"metrics": [model.metrics[0].name], "dimensions": [target.name]}
            try:
                engine.execute(compiler.compile(MetricQuery(**fields, **window), engine="duckdb", table="sample"))
            except (QueryError, EngineError, MacroError) as e:
                failed[target] = f"{target.kind} '{target.name}' failed on sampled data ({label} rows): {e}"
    return list(failed.values())


def render_patch(result: DriftProposal, config_dir: str | Path) -> str:
    """A patch of the config files, applied with `git apply` from the repository root, headed by
    the drift, the answers, and what the changes would do."""
    summary = result.proposal.summary if result.proposal else "New columns added to ingestion.yaml."
    lines = [f"# Drift proposal for dataset '{result.report.dataset}' ({result.report.table})\n"]
    lines += _comment(summary)
    if result.proposal:
        answers = result.proposal.answers()
        reasons = {u.signal: u.reason for u in result.proposal.unchanged}
        lines.append("#\n# The drift, and what answers it:\n")
        for number, signal in result.numbered.items():
            lines += _comment(f"{number} {signal.detail}", indent=2)
            if changes := [label for label in answers.get(number, []) if label != "unchanged"]:
                lines += _comment(f"answered by: {', '.join(changes)}", indent=5)
            if number in reasons:
                lines += _comment(f"unchanged: {reasons[number]}", indent=5)
        why = [(f"{c.kind} {c.model}.{c.name}", c.reason) for c in result.proposal.changes]
        why += [(f"macro {m.name}()", m.reason) for m in result.proposal.macros]
        if why:
            lines.append("#\n# Why each change:\n")
            for label, reason in why:
                lines += _comment(f"{label}: {reason}", indent=2)
    sections = [
        ("effect", "Effect on sampled rows, before → after (recent: the drift's window; earlier: the one before)"),
        ("engines", "Engines: where a definition goes through a macro with an engine's own version"),
        ("impact", "Affects"),
    ]
    for key, title in sections:
        if result.review.get(key):
            lines.append(f"#\n# {title}:\n")
            for line in result.review[key]:
                indent = len(line) - len(line.lstrip())
                lines += _comment(line.strip(), indent=2 + indent)
    if result.patched:
        lines.append("#\n# Review, then from the repository root: git apply <this file> && agentic-lakehouse setup\n\n")
        lines.append(config_diff(config_dir, result.patched))
    else:
        lines.append("#\n# Nothing to apply: this proposal changes no files. Delete it once you've read it.\n")
    return "".join(lines)


def _comment(text: str, indent: int = 0) -> list[str]:
    prefix = "# " + " " * indent
    return [f"{prefix}{line}\n" for line in textwrap.wrap(text, 98 - len(prefix))] or [f"{prefix.rstrip()}\n"]


def proposal_path(report: DriftReport, out_dir: str | Path) -> Path:
    """Where the proposal for this drift goes. Named after its signals, so the
    same drift isn't proposed (and paid for) again while its patch is pending."""
    key = ",".join(sorted(s.key for s in report.signals))
    return Path(out_dir) / f"{report.dataset}-{hashlib.sha256(key.encode()).hexdigest()[:8]}.patch"


@dataclass
class Ledger:
    """The signals Claude was asked about (proposals/ledger.json), so none is asked about twice:
    not while its patch is pending, not after the patch is applied or deleted, and not after a
    failure. `agentic-lakehouse drift --again` asks anyway."""

    path: Path
    entries: dict[str, dict[str, str]]  # signal key -> outcome, proposal, at, error

    @classmethod
    def load(cls, out_dir: str | Path) -> "Ledger":
        path = Path(out_dir) / LEDGER
        if not path.exists():
            return cls(path, {})
        try:
            entries = json.loads(path.read_text())
            if not isinstance(entries, dict):
                raise ValueError("it isn't a JSON object")
        except ValueError as e:  # fail closed: ignoring it would ask about everything again
            raise DriftAgentError(f"{path} can't be read ({e}); fix it, or delete it to ask about every current signal again") from e
        return cls(path, entries)

    def record(self, signals: list[Signal], outcome: str, *, proposal: str | None = None, error: str | None = None) -> None:
        at = datetime.now(UTC).isoformat(timespec="seconds")
        for signal in signals:
            entry = {"outcome": outcome, "at": at}
            if proposal:
                entry["proposal"] = proposal
            if error:
                entry["error"] = error[:500]
            self.entries[signal.key] = entry
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(self.entries, indent=2, sort_keys=True) + "\n")


def check_drift(
    platform: Platform,
    config_dir: str | Path,
    out_dir: str | Path,
    *,
    catalog: Catalog | None = None,
    client: anthropic.Anthropic | None = None,
    log: Log = print,
    dry_run: bool = False,
    again: bool = False,
) -> list[Path]:
    """Look for drift in every dataset, and write a proposal patch about the signals Claude
    hasn't been asked about. Returns the patches written. `dry_run` reports what would be asked
    and asks nothing; `again` asks about every current signal, even ones in the ledger. Raises
    DriftAgentError, after trying every dataset, if any failed."""
    catalog = catalog or iceberg_catalog(platform)
    ledger = Ledger.load(out_dir)
    written, failures = [], []
    for dataset in platform.ingestion.datasets:
        table = catalog.load_table(platform.iceberg_table(dataset.name).table)
        report, samples = detect_drift(dataset.name, platform, table)
        for problem in report.unsupported_columns + report.type_mismatches:
            log(f"drift: {dataset.name}: needs a person: {problem}")
        for note in report.notes:
            log(f"drift: {dataset.name}: {note}")
        if not report.signals:
            log(f"drift: {dataset.name}: no drift")
            continue
        fresh = [s for s in report.signals if again or s.key not in ledger.entries]
        if len(fresh) < len(report.signals):
            log(f"drift: {dataset.name}: {len(report.signals) - len(fresh)} signal(s) already asked about, in {ledger.path}")
        if not fresh:
            continue
        report = report.model_copy(update={"signals": fresh})
        path = proposal_path(report, out_dir)
        if dry_run:
            for number, signal in _numbered(report).items():
                log(f"drift: {dataset.name}: {number} {signal.detail}")
            log(f"drift: {dataset.name}: would ask {MODEL} about {len(fresh)} signal(s) and write {path}; a dry run asks nothing")
            continue
        if path.exists() and not again:
            log(f"drift: {dataset.name}: proposal already pending: {path}")
            continue
        try:
            result = propose_changes(report, samples, config_dir, client, log=log)
        except DriftAgentError as e:
            if e.asked:
                ledger.record(fresh, "failed", error=str(e))
            log(f"drift: {dataset.name}: {e}")
            failures.append(f"{dataset.name}: {e}")
            continue
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(render_patch(result, config_dir))
        ledger.record(fresh, "proposed" if result.patched else "unchanged", proposal=path.name)
        log(f"drift: {dataset.name}: wrote {path} ({result.attempts} attempt(s)); review it, then git apply it")
        written.append(path)
    if failures:
        raise DriftAgentError("; ".join(failures))
    return written


@dataclass
class PendingProposal:
    file: str
    dataset: str
    summary: str
    created: datetime  # UTC


def pending_proposals(out_dir: str | Path) -> list[PendingProposal]:
    """Proposal patches waiting for review in `out_dir`, newest first."""
    proposals = []
    for path in Path(out_dir).glob("*.patch"):
        header = []
        for line in path.read_text().splitlines():
            if not line.startswith("#"):
                break
            header.append(line.removeprefix("#").strip())
        dataset = re.search(r"dataset '([^']+)'", header[0]) if header else None
        summary = " ".join(header[1 : header.index("")] if "" in header else header[1:])
        created = datetime.fromtimestamp(path.stat().st_mtime, UTC)
        proposals.append(PendingProposal(path.name, dataset.group(1) if dataset else "?", summary, created))
    return sorted(proposals, key=lambda p: p.created, reverse=True)
