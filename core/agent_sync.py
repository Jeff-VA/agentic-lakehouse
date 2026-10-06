"""The drift agent: keeps the config in step with what producers actually send.

When producers start sending a new event field, the flush (core/flush.py) adds
it to the Iceberg table as a column. This agent then:

1. Detects drift: Iceberg columns that ingestion.yaml doesn't list, plus type
   mismatches and columns config can't express (reported for a person).
2. Profiles the new columns from sampled rows: how often they're set, distinct
   values, ranges and most common values.
3. Proposes config changes. Adding the new columns to ingestion.yaml is
   mechanical and done here; Claude proposes which of them deserve ontology
   dimensions or metrics, with names and descriptions, as structured output.
4. Validates the proposal: the patched config must pass load_platform's checks,
   and every proposed dimension and metric must compile and run on DuckDB over
   the sampled rows. Errors go back to Claude to fix, up to `max_attempts` times.
5. Writes a patch of the config directory for a person to review and apply with
   `git apply`. It never edits config in place.
"""

import difflib
import hashlib
import io
import os
import re
import shutil
import tempfile
import textwrap
from collections.abc import Callable
from dataclasses import dataclass
from datetime import timedelta
from functools import reduce
from pathlib import Path
from typing import Literal

import anthropic
import duckdb
import pyarrow as pa
import pyarrow.compute as pc
from pydantic import BaseModel, Field, ValidationError
from pyiceberg.catalog import Catalog
from pyiceberg.expressions import NotNull, Or
from pyiceberg.table import Table
from pyiceberg.types import BooleanType, DoubleType, FloatType, IntegerType, LongType, StringType, TimestampType
from ruamel.yaml import YAML
from ruamel.yaml.comments import CommentedMap

from core.compiler import MetricQuery, QueryError, SqlCompiler
from core.config import ColumnType, ConfigError, Platform, load_platform
from core.engines import DuckDBEngine, EngineError
from core.orchestrator import iceberg_catalog

MODEL = "claude-opus-5-5"

Log = Callable[[str], None]

_SNAKE_CASE = re.compile(r"^[a-z][a-z0-9_]*$")
_COLUMN_TYPES = {
    IntegerType: "int",
    LongType: "long",
    FloatType: "double",
    DoubleType: "double",
    BooleanType: "boolean",
    StringType: "string",
    TimestampType: "timestamp",
}


class DriftAgentError(RuntimeError):
    """The agent couldn't produce a valid proposal."""


class ColumnProfile(BaseModel):
    name: str
    type: ColumnType
    rows_sampled: int
    rows_set: int  # sampled rows where the column isn't NULL
    distinct_values: int  # approximate
    minimum: int | float | str | None = None
    maximum: int | float | str | None = None
    mean: float | None = None
    top_values: list[tuple[str | bool, int]] = []  # most common values with their counts (text and booleans)


class DriftReport(BaseModel):
    dataset: str
    table: str
    new_columns: list[ColumnProfile] = []  # can be added to config
    unsupported_columns: list[str] = []  # new columns config can't express, with the reason
    type_mismatches: list[str] = []

    @property
    def has_drift(self) -> bool:
        return bool(self.new_columns or self.unsupported_columns or self.type_mismatches)


# Claude's answer. The schema is sent as structured output, so the response always parses.
class ProposedDimension(BaseModel):
    model: str = Field(description="Name of the ontology model to add it to")
    name: str
    type: Literal["string", "number", "boolean", "timestamp"]
    description: str
    sql: str | None = Field(description="SQL over source columns; null when it is the column of the same name")


class ProposedMetric(BaseModel):
    model: str = Field(description="Name of the ontology model to add it to")
    name: str
    description: str
    type: Literal["aggregate", "ratio"]
    agg: Literal["count", "count_distinct", "sum", "avg", "min", "max", "percentile"] | None = Field(
        description="Aggregate metrics only"
    )
    sql: str | None = Field(description="Aggregate metrics: the expression to aggregate; null counts rows")
    percentile: float | None = Field(description="Only with agg percentile, as a fraction, e.g. 0.95")
    filters: list[str] = Field(description="Aggregate metrics: SQL predicates selecting the rows that count")
    numerator: str | None = Field(description="Ratio metrics only: an aggregate metric of the same model")
    denominator: str | None = Field(description="Ratio metrics only: an aggregate metric of the same model")


class NotModeled(BaseModel):
    column: str
    reason: str


class Proposal(BaseModel):
    summary: str = Field(description="A short paragraph for the reviewer: what changed upstream and what you propose")
    dimensions: list[ProposedDimension]
    metrics: list[ProposedMetric]
    not_modeled: list[NotModeled] = Field(description="Every new column you chose not to model, with the reason")


@dataclass
class DriftProposal:
    report: DriftReport
    proposal: Proposal | None  # None when no column could be modeled
    patched: dict[str, str]  # config file name -> proposed text
    attempts: int


SYSTEM_PROMPT = """\
You maintain the semantic ontology of a lakehouse: the YAML file defining which dimensions and \
metrics people and AI agents can query. Producers have started sending new event fields, which \
now exist as columns. Decide which of them deserve new dimensions or metrics, and define those.

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
what to query: say what it measures, in what unit, and anything surprising.

Model a column only if someone would plausibly slice by it or measure it. Low-cardinality text \
and booleans make good dimensions; numeric measures make good metrics, with an aggregation that \
answers a real question (e.g. an average or p95 for latencies, a sum for totals); identifiers \
suit count_distinct, or nothing. Prefer a few well-described definitions over one per column, and \
don't duplicate existing ones. Use the column profiles to choose types and infer units. List \
every new column you don't model in not_modeled, with the reason."""


def detect_drift(dataset_name: str, platform: Platform, table: Table, sample_rows: int = 10_000) -> tuple[DriftReport, pa.Table | None]:
    """Compare `table`'s live schema with the dataset in ingestion.yaml.

    Returns the report and, when there are new columns, a sample of rows in
    which at least one of them is set (all columns).
    """
    dataset = platform.dataset(dataset_name)
    configured = {c.name: c.type for c in dataset.columns}
    report = DriftReport(dataset=dataset.name, table=".".join(table.name()))
    candidates = {}
    for field in table.schema().fields:
        live_type = _COLUMN_TYPES.get(type(field.field_type))
        if field.name in configured:
            if live_type != configured[field.name]:
                report.type_mismatches.append(
                    f"{field.name}: ingestion.yaml says {configured[field.name]}, the Iceberg table has {field.field_type}"
                )
        elif not _SNAKE_CASE.match(field.name):
            report.unsupported_columns.append(f"{field.name}: config needs lowercase snake_case names")
        elif live_type is None:
            report.unsupported_columns.append(f"{field.name}: Iceberg type {field.field_type} has no config equivalent")
        else:
            candidates[field.name] = live_type
    if not candidates:
        return report, None

    sample = table.scan(row_filter=reduce(Or, [NotNull(name) for name in candidates]), limit=sample_rows).to_arrow()
    con = duckdb.connect()
    con.register("sample", sample)
    for name, column_type in candidates.items():
        column = '"' + name.replace('"', '""') + '"'
        rows_set, distinct = con.execute(f"SELECT count({column}), approx_count_distinct({column}) FROM sample").fetchone()
        profile = ColumnProfile(
            name=name, type=column_type, rows_sampled=sample.num_rows, rows_set=rows_set, distinct_values=distinct
        )
        if column_type in ("int", "long", "double"):
            profile.minimum, profile.maximum, profile.mean = con.execute(
                f"SELECT min({column}), max({column}), avg({column}) FROM sample"
            ).fetchone()
        elif column_type == "timestamp":
            low, high = con.execute(f"SELECT min({column}), max({column}) FROM sample").fetchone()
            profile.minimum, profile.maximum = str(low), str(high)
        else:
            profile.top_values = [
                (value if isinstance(value, bool) else str(value)[:80], count)
                for value, count in con.execute(
                    f"SELECT {column}, count(*) FROM sample WHERE {column} IS NOT NULL GROUP BY 1 ORDER BY 2 DESC, 1 LIMIT 5"
                ).fetchall()
            ]
        report.new_columns.append(profile)
    return report, sample


def propose_changes(
    report: DriftReport,
    sample: pa.Table,
    config_dir: str | Path,
    client: anthropic.Anthropic | None = None,
    *,
    max_attempts: int = 3,
    log: Log = print,
) -> DriftProposal:
    """Config changes for `report`'s new columns: ingestion.yaml gets every one,
    ontology.yaml gets the dimensions and metrics Claude proposes and the
    validation accepts."""
    config_dir = Path(config_dir)
    texts = {name: (config_dir / name).read_text() for name in ("ingestion.yaml", "ontology.yaml")}
    patched = {"ingestion.yaml": add_ingestion_columns(texts["ingestion.yaml"], report.dataset, report.new_columns)}
    try:
        client = client or anthropic.Anthropic()
    except anthropic.CredentialsError as e:  # e.g. a configured profile that can't be read
        raise DriftAgentError(f"Claude API credentials: {e}") from e
    if client.api_key is None and client.auth_token is None and client.credentials is None:
        raise DriftAgentError(
            "no Claude API credentials: put ANTHROPIC_API_KEY=... in .env (git-ignored) or the environment"
        )
    messages = [{"role": "user", "content": _request(report, texts["ontology.yaml"])}]
    for attempt in range(1, max_attempts + 1):
        log(f"drift: asking {MODEL} for a proposal (attempt {attempt} of {max_attempts})")
        response = _ask(client, messages)
        proposal = response.parsed_output
        try:
            patched["ontology.yaml"] = apply_proposal(texts["ontology.yaml"], proposal)
            errors = validate(config_dir, patched, report.dataset, sample, proposal)
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


def _request(report: DriftReport, ontology_text: str) -> str:
    columns = report.model_dump_json(include={"new_columns"}, indent=2)
    return (
        f"Dataset `{report.dataset}` (Iceberg table {report.table}) has new columns. They are being added "
        f"to ingestion.yaml. Profiles from rows where at least one of them is set:\n\n"
        f"<new_columns>\n{columns}\n</new_columns>\n\n"
        f"The current ontology:\n\n<ontology_yaml>\n{ontology_text}</ontology_yaml>"
    )


def _yaml() -> YAML:
    yaml = YAML()  # round-trip mode: keeps comments and formatting
    yaml.preserve_quotes = True
    yaml.width = 4096
    yaml.indent(mapping=2, sequence=4, offset=2)
    return yaml


def _dump(yaml: YAML, data) -> str:
    out = io.StringIO()
    yaml.dump(data, out)
    return out.getvalue()


def add_ingestion_columns(text: str, dataset: str, columns: list[ColumnProfile]) -> str:
    yaml = _yaml()
    data = yaml.load(text)
    [entry] = [d for d in data["datasets"] if d["name"] == dataset]
    for column in columns:
        item = CommentedMap(name=column.name, type=column.type)
        item.fa.set_flow_style()  # one line, like the existing columns
        entry["columns"].append(item)
    return _dump(yaml, data)


def apply_proposal(text: str, proposal: Proposal) -> str:
    """ontology.yaml text with the proposal's dimensions and metrics appended to their models."""
    yaml = _yaml()
    data = yaml.load(text)
    models = {m["name"]: m for m in data["models"]}

    def model(kind: str, item) -> CommentedMap:
        if item.model not in models:
            raise ValueError(f"{kind} '{item.name}': there is no model named '{item.model}'")
        return models[item.model]

    for dimension in proposal.dimensions:
        entry = CommentedMap(name=dimension.name, type=dimension.type, description=dimension.description)
        if dimension.sql:
            entry["sql"] = dimension.sql
        model("dimension", dimension)["dimensions"].append(entry)
    for metric in proposal.metrics:
        entry = CommentedMap(name=metric.name, description=metric.description)
        if metric.type == "ratio":
            entry.update(type="ratio", numerator=metric.numerator, denominator=metric.denominator)
        else:
            entry["agg"] = metric.agg
            for key in ("percentile", "sql"):
                if getattr(metric, key) is not None:
                    entry[key] = getattr(metric, key)
            if metric.filters:
                entry["filters"] = list(metric.filters)
        model("metric", metric)["metrics"].append(entry)
    return _dump(yaml, data)


def validate(config_dir: Path, patched: dict[str, str], dataset_name: str, sample: pa.Table, proposal: Proposal) -> list[str]:
    """Problems with the patched config, empty if none: it must load, and each
    proposed dimension and metric must run on DuckDB over the sampled rows."""
    with tempfile.TemporaryDirectory() as tmp:
        for path in config_dir.glob("*.yaml"):
            shutil.copy(path, tmp)
        for name, text in patched.items():
            (Path(tmp) / name).write_text(text)
        try:
            platform = load_platform(tmp)
        except (ValidationError, ConfigError) as e:
            return [str(e)]

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
    config_dir = Path(config_dir)
    prefix = os.path.relpath(config_dir).replace(os.sep, "/")
    summary = result.proposal.summary if result.proposal else "New columns added to ingestion.yaml."
    lines = [f"# Drift proposal for dataset '{result.report.dataset}' ({result.report.table})\n"]
    lines += [f"# {line}\n" for line in textwrap.wrap(summary, 96)]
    if result.proposal and result.proposal.not_modeled:
        lines.append("#\n# Not modeled:\n")
        lines += [f"#   {n.column}: {n.reason}\n" for n in result.proposal.not_modeled]
    lines.append("#\n# Review, then from the repository root: git apply <this file> && python run.py setup\n\n")
    for name, text in result.patched.items():
        original = (config_dir / name).read_text()
        lines += difflib.unified_diff(
            original.splitlines(keepends=True),
            text.splitlines(keepends=True),
            fromfile=f"a/{prefix}/{name}",
            tofile=f"b/{prefix}/{name}",
        )
    return "".join(lines)


def proposal_path(report: DriftReport, out_dir: str | Path) -> Path:
    """Where the proposal for this drift goes. Named after the new columns, so the
    same drift isn't proposed (and paid for) again while its patch is pending."""
    key = ",".join(sorted(c.name for c in report.new_columns))
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
        report, sample = detect_drift(dataset.name, platform, table)
        for problem in report.unsupported_columns + report.type_mismatches:
            log(f"drift: {dataset.name}: needs a person: {problem}")
        if not report.new_columns:
            log(f"drift: {dataset.name}: no new columns")
            continue
        path = proposal_path(report, out_dir)
        if path.exists():
            log(f"drift: {dataset.name}: proposal already pending: {path}")
            continue
        result = propose_changes(report, sample, config_dir, client, log=log)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(render_patch(result, config_dir))
        log(f"drift: {dataset.name}: wrote {path} ({result.attempts} attempt(s)); review it, then git apply it")
        written.append(path)
    return written
