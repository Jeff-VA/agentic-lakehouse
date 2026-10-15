"""What a reviewer reads about a drift proposal before its diff (docs/design/drift-agent.md).

The proposal has passed validation by now; this says what it would do:

- effect(): each new or changed definition, and each one that calls a changed macro or
  divides a changed metric, before and after, on the sampled rows of the recent window and
  of the window before it. A definition whose value on the earlier rows changes rewrites
  history; sometimes that's the point, and the reviewer decides.
- engine_agreement(): where such a definition goes through a macro with an engine's own
  version, the Trino and Pinot renderings, translated to DuckDB by sqlglot, against DuckDB's,
  on the sampled rows. A warning, not an error: regex and URL functions differ at the edges,
  and a function DuckDB doesn't have can't be compared at all.
- impact(): the views that use what changed, the dashboards that draw them, and the views and
  steps that call a changed macro.
"""

import re
from dataclasses import dataclass
from datetime import timedelta
from typing import Literal

import duckdb
import pyarrow as pa
import pyarrow.compute as pc
import sqlglot
import yaml
from jinja2 import nodes

from agentic_lakehouse.compiler import SQLGLOT_DIALECTS, MetricQuery, QueryError, SqlCompiler
from agentic_lakehouse.config import Platform
from agentic_lakehouse.dashboards import dashboard_files, describe_dashboard
from agentic_lakehouse.drift.proposal import Proposal
from agentic_lakehouse.drift.signals import DriftSamples
from agentic_lakehouse.engines import DuckDBEngine, EngineError
from agentic_lakehouse.macros import ENGINES, MacroError, ProjectMacros
from agentic_lakehouse.ontology import AggregateMetric, SemanticModel
from agentic_lakehouse.views import ComposedSource, ModelSource

# A macro called in a snippet, view or step: {{ name( or {{ adapter.name(
MACRO_CALL = re.compile(r"\{\{-?\s*(?:adapter\.)?([A-Za-z_]\w*)\s*\(")
_SHOWN_VALUES = 6  # a dimension's most common values, compared before and after


@dataclass(frozen=True)
class Target:
    """A definition the review covers, and why."""

    model: str
    kind: Literal["dimension", "metric"]
    name: str
    why: str  # "new", "changed", "calls editor_type()", "divides human_edits"

    def __str__(self) -> str:
        return f"{self.kind} {self.model}.{self.name}"


def callers(seed: set[str], macros: ProjectMacros) -> set[str]:
    """`seed` and every project macro that calls one of them, directly or through others."""
    calls: dict[str, set[str]] = {}
    for text in macros.sources.values():
        for macro in macros.env.parse(text).find_all(nodes.Macro):
            calls.setdefault(macro.name, set()).update(_called(macro))
    found = set(seed)
    while more := {name for name, called in calls.items() if called & found} - found:
        found |= more
    return found


def _called(node: nodes.Node) -> set[str]:
    """The names a template calls: name(...) or adapter.name(...)."""
    names = set()
    for call in node.find_all(nodes.Call):
        if isinstance(call.node, nodes.Name):
            names.add(call.node.name)
        elif isinstance(call.node, nodes.Getattr) and isinstance(call.node.node, nodes.Name) and call.node.node.name == "adapter":
            names.add(call.node.attr)
    return names


def snippets(model: SemanticModel, kind: str, name: str) -> list[tuple[str, str]]:
    """(where, SQL) for each piece of SQL a definition is written in, e.g. ("filters[0]", "...")."""
    if kind == "dimension":
        dimension = next(d for d in model.dimensions if d.name == name)
        return [("sql", dimension.sql)] if dimension.sql else []
    metric = next(m for m in model.metrics if m.name == name)
    if not isinstance(metric, AggregateMetric):
        return []
    return ([("sql", metric.sql)] if metric.sql else []) + [(f"filters[{i}]", f) for i, f in enumerate(metric.filters)]


def targets(proposal: Proposal, after: Platform) -> list[Target]:
    """The definitions a proposal touches: new ones, changed ones, ones that call a changed macro,
    and ratios that divide any of those."""
    found: dict[tuple[str, str, str], str] = {}
    for dimension in proposal.dimensions:
        found[(dimension.model, "dimension", dimension.name)] = "new"
    for metric in proposal.metrics:
        found[(metric.model, "metric", metric.name)] = "new"
    for change in proposal.changes:
        found.setdefault((change.model, change.kind, change.name), "changed")
    changed = callers({m.name for m in proposal.macros}, after.ontology.macros) if proposal.macros else set()
    for model in after.ontology.models:
        definitions = [("dimension", d.name) for d in model.dimensions] + [("metric", m.name) for m in model.metrics]
        for kind, name in definitions:
            called = sorted({c for _, sql in snippets(model, kind, name) for c in MACRO_CALL.findall(sql)} & changed)
            if called:
                found.setdefault((model.name, kind, name), "calls " + ", ".join(f"{c}()" for c in called))
        for metric in model.metrics:
            if not isinstance(metric, AggregateMetric):
                parts = [p for p in (metric.numerator, metric.denominator) if (model.name, "metric", p) in found]
                if parts:
                    found.setdefault((model.name, "metric", metric.name), "divides " + " and ".join(parts))
    return [Target(model, kind, name, why) for (model, kind, name), why in found.items()]


class _Sample:
    """One window's sampled rows, and a platform's definitions computed over them."""

    def __init__(self, platform: Platform, dataset: str, rows: pa.Table):
        self.models = {m.name: m for m in platform.ontology.models}
        self.compiler = SqlCompiler(platform.ontology)
        self.engine = DuckDBEngine()
        self.engine.con.register("sample", rows)
        times = rows.column(platform.dataset(dataset).time_column)
        self.window = {"start": pc.min(times).as_py(), "end": pc.max(times).as_py() + timedelta(milliseconds=1)}

    def value(self, target: Target) -> dict | float | str | None:
        """A dimension's shares of its most common values, a metric's value, None if the platform
        has no such definition, or why it couldn't be computed."""
        model = self.models.get(target.model)
        names = [d.name for d in model.dimensions] if model and target.kind == "dimension" else [m.name for m in model.metrics] if model else []
        if target.name not in names:
            return None
        try:
            if target.kind == "metric":
                sql = self.compiler.compile(MetricQuery(metrics=[target.name], **self.window), engine="duckdb", table="sample")
                return self.engine.execute(sql).column(0)[0].as_py()
            dimension = next(d for d in model.dimensions if d.name == target.name)
            expression = self.compiler._dimension_tree(dimension, "duckdb").sql(dialect="duckdb")
            counts = self.engine.execute(
                f"SELECT {expression} AS value, count(*) AS n FROM sample GROUP BY 1 ORDER BY 2 DESC, 1 LIMIT {_SHOWN_VALUES}"
            ).to_pylist()
            total = self.engine.execute("SELECT count(*) AS n FROM sample").column(0)[0].as_py()
            return {row["value"]: row["n"] / total for row in counts}
        except (QueryError, EngineError, MacroError) as e:
            return f"couldn't compute: {str(e).splitlines()[0]}"


def effect(before: Platform, after: Platform, dataset: str, samples: DriftSamples, targets: list[Target]) -> list[str]:
    """For each target in a model of `dataset`, before -> after on each window's sampled rows."""
    windows = [(label, rows) for label, rows in (("recent", samples.recent), ("earlier", samples.earlier)) if rows.num_rows]
    computed = [(label, _Sample(before, dataset, rows), _Sample(after, dataset, rows)) for label, rows in windows]
    sources = {m.name: m.source for m in after.ontology.models}
    lines = []
    for target in targets:
        lines.append(f"{target} ({target.why})")
        if sources.get(target.model) != dataset:
            lines.append(f"  not sampled: model {target.model} reads dataset {sources.get(target.model)}")
            continue
        for label, old, new in computed:
            lines.append(f"  {label}: {_compare(old.value(target), new.value(target), history=label == 'earlier')}")
    return lines


def _compare(then, now, *, history: bool) -> str:
    if isinstance(now, str) or isinstance(then, str):
        return now if isinstance(now, str) else then
    if then is None:
        return "new: " + (_shares(now) if isinstance(now, dict) else _number(now))
    if isinstance(now, dict):
        keys = list(dict.fromkeys([*now, *then]))
        moved = [k for k in keys if abs(now.get(k, 0) - then.get(k, 0)) >= 0.0005]
        if not moved:
            return "unchanged"
        text = ", ".join(f"{_value(k)} {_share(then.get(k))} → {_share(now.get(k))}" for k in moved)
    elif then == now or (then is not None and now is not None and abs(then - now) <= 1e-9 * max(abs(then), abs(now), 1)):
        return f"unchanged ({_number(now)})"
    else:
        text = f"{_number(then)} → {_number(now)}"
    return text + ("  ← rewrites history" if history else "")


def _shares(shares: dict) -> str:
    return ", ".join(f"{_value(k)} {_share(v)}" for k, v in shares.items())


def _value(value) -> str:
    if value is None:
        return "null"
    if not isinstance(value, str):
        return str(value)
    return repr(value if len(value) <= 30 else value[:30] + "…")


def _share(share: float | None) -> str:
    return "—" if not share else f"{share:.1%}"


def _number(value) -> str:
    if value is None:
        return "null"
    if isinstance(value, int):
        return f"{value:,}"
    return f"{value:,.2f}" if abs(value) >= 1 else f"{value:.4g}"


def engine_agreement(after: Platform, dataset: str, samples: DriftSamples, targets: list[Target]) -> list[str]:
    """Where a target's SQL goes through a project macro with an engine's own version: whether the
    Trino and Pinot renderings give what DuckDB's gives, on the recent sampled rows."""
    macros = after.ontology.macros
    own = {m.name for m in macros.catalog() if any(f in m.files for f in (f"project/{e}.sql" for e in ENGINES))}
    varying = callers(own, macros) if own else set()
    rows = samples.recent
    if not varying or not rows.num_rows:
        return []
    con = duckdb.connect()
    con.register("sample", rows)
    models = {m.name: m for m in after.ontology.models if m.source == dataset}
    lines = []
    for target in targets:
        if target.model not in models:
            continue
        for where, sql in snippets(models[target.model], target.kind, target.name):
            if not set(MACRO_CALL.findall(sql)) & varying:
                continue
            reference = macros.render(sql, "duckdb")
            results = [_agreement(con, reference, macros, sql, engine) for engine in ENGINES if engine != "duckdb"]
            lines.append(f"{target} {where}: " + "; ".join(results))
    return lines


def _agreement(con: duckdb.DuckDBPyConnection, reference: str, macros: ProjectMacros, sql: str, engine: str) -> str:
    try:
        translated = sqlglot.transpile(macros.render(sql, engine), read=SQLGLOT_DIALECTS[engine], write="duckdb")[0]
    except (MacroError, sqlglot.errors.SqlglotError):
        return f"{engine}'s version can't be translated to DuckDB, so it isn't compared"
    differs = f"({reference}) IS DISTINCT FROM ({translated})"
    try:
        different, total, ours, theirs = con.execute(
            f"SELECT count(*) FILTER (WHERE {differs}), count(*), any_value({reference}) FILTER (WHERE {differs}), "
            f"any_value({translated}) FILTER (WHERE {differs}) FROM sample"
        ).fetchone()
    except duckdb.Error as e:
        return f"{engine}'s version doesn't run on DuckDB ({str(e).splitlines()[0][:80]}), so it isn't compared"
    if not different:
        return f"{engine} agrees on all {total:,} sampled rows"
    return f"{engine} differs on {different:,} of {total:,} sampled rows, e.g. duckdb {_value(ours)}, {engine} {_value(theirs)}"


def impact(after: Platform, targets: list[Target], proposal: Proposal) -> list[str]:
    """The views that use a changed definition or call a changed macro, the dashboards that draw
    them, and the steps that call a changed macro. New definitions affect nothing yet."""
    affected = {(t.model, t.name) for t in targets if t.why != "new"}
    changed = callers({m.name for m in proposal.macros}, after.ontology.macros) if proposal.macros else set()
    views: dict[str, list[str]] = {}
    for name, view in sorted(after.views.items()):
        uses = []
        sources = list(view.source.parts.values()) if isinstance(view.source, ComposedSource) else [view.source]
        for source in sources:
            if isinstance(source, ModelSource):
                fields = [o.field for o in source.outputs] + [c.dimension for c in source.conditions]
                uses += [field for field in fields if (source.model, field) in affected]
        uses += [f"{c}()" for c in MACRO_CALL.findall(view.sql) if c in changed]
        if uses:
            views[name] = list(dict.fromkeys(uses))
    lines = []
    if views:
        lines.append("views: " + "; ".join(f"{name} ({', '.join(uses)})" for name, uses in views.items()))
    if after.config_dir is not None:
        dashboards = {}
        for name, path in dashboard_files(after.config_dir / "dashboards").items():
            if drawn := [v for v in describe_dashboard(path)[1] if v in views]:
                dashboards[name] = drawn
        if dashboards:
            lines.append("dashboards: " + "; ".join(f"{name} (draws {', '.join(drawn)})" for name, drawn in dashboards.items()))
        pipelines = after.config_dir / "pipelines.yaml"
        if changed and pipelines.exists():
            steps = []
            for step in (yaml.safe_load(pipelines.read_text()) or {}).get("steps") or []:
                if called := list(dict.fromkeys(c for c in MACRO_CALL.findall(step.get("sql") or "") if c in changed)):
                    steps.append(f"{step.get('name')} (calls {', '.join(f'{c}()' for c in called)})")
            if steps:
                lines.append("steps: " + "; ".join(steps))
    return lines
