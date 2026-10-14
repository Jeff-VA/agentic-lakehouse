"""The drift agent: keeps the config in step with what producers actually send.

When producers start sending a new event field, the flush (agentic_lakehouse/flush.py) adds
it to the Iceberg table as a column. This agent then:

1. Detects drift (agentic_lakehouse/drift/signals.py): Iceberg columns that
   ingestion.yaml doesn't list, as signals, plus type mismatches and columns
   config can't express (reported for a person).
2. Profiles the new columns from sampled rows: how often they're set, distinct
   values, ranges and most common values.
3. Proposes config changes. Adding the new columns to ingestion.yaml is
   mechanical and done here; Claude proposes which of them deserve ontology
   dimensions or metrics, with names and descriptions, as structured output
   (drift/proposal.py), and answers every signal. The prompt lists the
   project's own macros (config/macros/), so a proposal can reuse the business
   definitions they name instead of re-deriving them.
4. Validates the proposal: the patched config, with the project's macros, must
   pass load_platform's checks, and every proposed dimension and metric must
   compile and run on DuckDB over the sampled rows. Errors go back to Claude to
   fix, up to `max_attempts` times.
5. Writes a patch of the config directory (drift/patch.py) for a person to
   review and apply with `git apply`. It never edits config in place.

Design, and where this is going: docs/design/drift-agent.md.
"""

import hashlib
import re
import shutil
import tempfile
import textwrap
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

import anthropic
import pyarrow.compute as pc
from pydantic import ValidationError
from pyiceberg.catalog import Catalog

from agentic_lakehouse.compiler import MetricQuery, QueryError, SqlCompiler
from agentic_lakehouse.config import ConfigError, Platform, load_platform
from agentic_lakehouse.drift.patch import add_ingestion_columns, apply_macros, apply_ontology, config_diff  # noqa: F401
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
from agentic_lakehouse.macros import ENGINES, MacroInfo, ProjectMacros
from agentic_lakehouse.orchestrator import iceberg_catalog

MODEL = "claude-opus-5-5"

Log = Callable[[str], None]


class DriftAgentError(RuntimeError):
    """The agent couldn't produce a valid proposal."""


@dataclass
class DriftProposal:
    report: DriftReport
    proposal: Proposal | None  # None when no column could be modeled
    patched: dict[str, str]  # config file (relative to the config folder) -> proposed text
    attempts: int

    @property
    def numbered(self) -> dict[str, Signal]:
        """The report's signals by the numbers the request gave them: S1, S2, ..."""
        return _numbered(self.report)


SYSTEM_PROMPT = """\
You maintain the semantic ontology of a lakehouse: the YAML file defining which dimensions and \
metrics people and AI agents can query. Producers have started sending new event fields, which \
now exist as columns. Decide which of them deserve new dimensions or metrics, and define those.

Each new column is a signal, numbered S1, S2, and so on. Every dimension and metric you propose \
names the signals it answers, and every signal you don't act on goes in `unchanged`, with the \
reason. Leave `changes` and `macros` empty.

The ontology's rules are enforced; a proposal that breaks one is sent back to you with the error:
- Names are lowercase snake_case and unique within a model across dimensions and metrics. Metric \
names are unique across all models.
- A dimension has a type (string, number, boolean or timestamp) and an optional `sql` expression \
over source columns; leave `sql` null when the dimension is the column of the same name.
- An aggregate metric has `agg` (count, count_distinct, sum, avg, min, max or percentile), the \
`sql` expression it aggregates (null only for count, which then counts rows), `percentile` as a \
fraction such as 0.95 (only with agg percentile), and `filters`: SQL predicates, ANDed, that \
select which rows count.
- A ratio metric divides two aggregate metrics of the same model, named in `numerator` and \
`denominator`.
- SQL is DuckDB dialect: a single scalar expression over source columns, with no aggregate \
functions (`agg` applies the aggregation), subqueries or window functions.
- Every dimension and metric needs a description. Descriptions are how people and agents decide \
what to query: say what it measures, in what unit, and anything surprising. A label is how \
people say the name, for charts and tables; give metrics a format and unit where they help.

Model a column only if someone would plausibly slice by it or measure it. Low-cardinality text \
and booleans make good dimensions; numeric measures make good metrics, with an aggregation that \
answers a real question (e.g. an average or p95 for latencies, a sum for totals); identifiers \
suit count_distinct, or nothing. Prefer a few well-described definitions over one per column, and \
don't duplicate existing ones. Use the column profiles to choose types and infer units."""

# Added to the request when the project has macros of its own, followed by the list of them.
MACROS_PROMPT = """\
The project names its business definitions, and SQL that differs by engine, as macros in \
config/macros/, listed below. A dimension's or metric's `sql` and a metric's `filters` can call \
them with Jinja: `{{ macro_name() }}`, or `{{ macro_name('column_name') }}`, where each argument \
is SQL text in quotes. When a macro already defines what you need, call it rather than writing \
its SQL out again, so the definition stays in one place. A snippet that calls a macro is \
rendered for each engine, with the engine's own version where it has one, then read in that \
engine's SQL dialect, so the SQL around the call must be plain SQL that every engine reads."""


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
    """Config changes for `report`'s signals: ingestion.yaml gets every new
    column, ontology.yaml gets the dimensions and metrics Claude proposes and
    the validation accepts."""
    config_dir = Path(config_dir)
    texts = {name: (config_dir / name).read_text() for name in ("ingestion.yaml", "ontology.yaml")}
    patched = {}
    if report.new_columns:
        patched["ingestion.yaml"] = add_ingestion_columns(texts["ingestion.yaml"], report.dataset, report.new_columns)
    try:
        client = client or anthropic.Anthropic()
    except anthropic.CredentialsError as e:  # e.g. a configured profile that can't be read
        raise DriftAgentError(f"Claude API credentials: {e}") from e
    if client.api_key is None and client.auth_token is None and client.credentials is None:
        raise DriftAgentError(
            "no Claude API credentials: put ANTHROPIC_API_KEY=... in .env (git-ignored) or the environment"
        )
    macros = _own_macros(ProjectMacros(config_dir / "macros"))
    messages = [{"role": "user", "content": _request(report, texts["ontology.yaml"], macros)}]
    for attempt in range(1, max_attempts + 1):
        log(f"drift: asking {MODEL} for a proposal (attempt {attempt} of {max_attempts})")
        response = _ask(client, messages)
        proposal = response.parsed_output
        try:
            patched["ontology.yaml"] = apply_ontology(texts["ontology.yaml"], proposal)
            errors = validate(config_dir, patched, report.dataset, samples, proposal)
        except ValueError as e:
            errors = [str(e)]
        if not errors:
            return DriftProposal(report, proposal, patched, attempt)
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
    raise DriftAgentError(f"no valid proposal after {max_attempts} attempts; last errors: {errors}")


def _ask(client: anthropic.Anthropic, messages: list[dict]):
    try:
        response = client.beta.messages.parse(
            model=MODEL,
            max_tokens=16000,
            system=SYSTEM_PROMPT,
            messages=messages,
            output_format=Proposal,
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
    if response.stop_reason == "refusal":
        raise DriftAgentError(f"Claude declined to propose changes: {response.stop_details}")
    if response.stop_reason == "max_tokens":
        raise DriftAgentError("Claude's proposal was cut off at max_tokens")
    return response


def _request(report: DriftReport, ontology_text: str, macros: list[MacroInfo]) -> str:
    signals = "\n".join(f"{number} {signal.detail}" for number, signal in _numbered(report).items())
    columns = "[\n" + ",\n".join(profile.model_dump_json(indent=2) for profile in report.new_columns) + "\n]"
    request = (
        f"Dataset `{report.dataset}` (Iceberg table {report.table}) has new columns. They are being added "
        f"to ingestion.yaml.\n\n<signals>\n{signals}\n</signals>\n\n"
        f"Profiles of the new columns, from rows where at least one of them is set:\n\n"
        f"<new_columns>\n{columns}\n</new_columns>\n\n"
        f"The current ontology:\n\n<ontology_yaml>\n{ontology_text}</ontology_yaml>"
    )
    if macros:
        listed = "\n".join(_macro_line(macro) for macro in macros)
        request += f"\n\n{MACROS_PROMPT}\n\n<project_macros>\n{listed}\n</project_macros>"
    return request


def _own_macros(macros: ProjectMacros) -> list[MacroInfo]:
    """The project's macros that ontology SQL can call: its own, defined for every engine. The
    engine's built-ins (count, percentile, quote, ...) and a project's overrides of them are left
    out: the compiler applies those itself, for example through a metric's `agg`."""
    own = []
    for macro in macros.catalog():
        if any(f.startswith("engine/") for f in macro.files):
            continue
        if "project/default.sql" in macro.files or all(f"project/{e}.sql" in macro.files for e in ENGINES):
            own.append(macro)  # an ontology snippet is rendered for all three engines
    return own


def _macro_line(macro: MacroInfo) -> str:
    """The macro as the prompt lists it: how to call it, what it means, and where its SQL differs."""
    line = f"- {macro.name}({', '.join(macro.args)})" + (f": {macro.description}" if macro.description else "")
    if engines := [e for e in ENGINES if f"project/{e}.sql" in macro.files]:
        line += f"\n  Engine-specific versions: {', '.join(engines)}."
    return line


def validate(config_dir: Path, patched: dict[str, str], dataset_name: str, samples: DriftSamples, proposal: Proposal) -> list[str]:
    """Problems with the patched config, empty if none: it must load, with the
    project's macros, and each proposed dimension and metric must run on DuckDB
    over the sampled rows."""
    sample = samples.recent
    with tempfile.TemporaryDirectory() as tmp:
        for path in config_dir.glob("*.yaml"):
            shutil.copy(path, tmp)
        if (config_dir / "macros").is_dir():  # the proposal may call them
            shutil.copytree(config_dir / "macros", Path(tmp) / "macros")
        for name, text in patched.items():
            (Path(tmp) / name).parent.mkdir(parents=True, exist_ok=True)
            (Path(tmp) / name).write_text(text)
        try:
            platform = load_platform(tmp)
        except (ValidationError, ConfigError) as e:
            return [str(e)]

        # Compiled while the copy exists: macro files are read when they're first used.
        time_column = platform.dataset(dataset_name).time_column
        times = sample.column(time_column)
        window = {"start": pc.min(times).as_py(), "end": pc.max(times).as_py() + timedelta(milliseconds=1)}
        engine = DuckDBEngine()
        engine.con.register("sample", sample)
        compiler = SqlCompiler(platform.ontology)
        models = {m.name: m for m in platform.ontology.models}
        checks = [(f"metric '{m.name}'", {"metrics": [m.name]}) for m in proposal.metrics] + [
            (f"dimension '{d.name}'", {"metrics": [models[d.model].metrics[0].name], "dimensions": [d.name]})
            for d in proposal.dimensions
        ]
        errors = []
        for label, fields in checks:
            try:
                engine.execute(compiler.compile(MetricQuery(**fields, **window), engine="duckdb", table="sample"))
            except (QueryError, EngineError) as e:
                errors.append(f"{label} failed on sampled data: {e}")
        return errors


def render_patch(result: DriftProposal, config_dir: str | Path) -> str:
    """A patch of the config files, applied with `git apply` from the repository root."""
    summary = result.proposal.summary if result.proposal else "New columns added to ingestion.yaml."
    lines = [f"# Drift proposal for dataset '{result.report.dataset}' ({result.report.table})\n"]
    lines += [f"# {line}\n" for line in textwrap.wrap(summary, 96)]
    if result.proposal:
        answers = result.proposal.answers()
        reasons = {u.signal: u.reason for u in result.proposal.unchanged}
        lines.append("#\n# Drift and answers:\n")
        for number, signal in result.numbered.items():
            lines += [f"#   {line}\n" for line in textwrap.wrap(f"{number} {signal.detail}", 94)]
            if number in reasons:
                lines += [f"#      {line}\n" for line in textwrap.wrap(f"unchanged: {reasons[number]}", 91)]
            changes = [label for label in answers.get(number, []) if label != "unchanged"]
            if changes:
                lines.append(f"#      answered by: {', '.join(changes)}\n")
    lines.append("#\n# Review, then from the repository root: git apply <this file> && agentic-lakehouse setup\n\n")
    lines.append(config_diff(config_dir, result.patched))
    return "".join(lines)


def proposal_path(report: DriftReport, out_dir: str | Path) -> Path:
    """Where the proposal for this drift goes. Named after its signals, so the
    same drift isn't proposed (and paid for) again while its patch is pending."""
    key = ",".join(sorted(s.key for s in report.signals))
    return Path(out_dir) / f"{report.dataset}-{hashlib.sha256(key.encode()).hexdigest()[:8]}.patch"


def check_drift(
    platform: Platform,
    config_dir: str | Path,
    out_dir: str | Path,
    *,
    catalog: Catalog | None = None,
    client: anthropic.Anthropic | None = None,
    log: Log = print,
) -> list[Path]:
    """Look for drift in every dataset and write a proposal patch for each new
    one. Returns the patches written; drift with a pending patch is skipped."""
    catalog = catalog or iceberg_catalog(platform)
    written = []
    for dataset in platform.ingestion.datasets:
        table = catalog.load_table(platform.iceberg_table(dataset.name).table)
        report, samples = detect_drift(dataset.name, platform, table)
        for problem in report.unsupported_columns + report.type_mismatches:
            log(f"drift: {dataset.name}: needs a person: {problem}")
        if not report.signals:
            log(f"drift: {dataset.name}: no new columns")
            continue
        path = proposal_path(report, out_dir)
        if path.exists():
            log(f"drift: {dataset.name}: proposal already pending: {path}")
            continue
        result = propose_changes(report, samples, config_dir, client, log=log)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(render_patch(result, config_dir))
        log(f"drift: {dataset.name}: wrote {path} ({result.attempts} attempt(s)); review it, then git apply it")
        written.append(path)
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
