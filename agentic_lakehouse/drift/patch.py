"""A proposal applied to the text of the config files, and the diff a person reviews.

Edits go through ruamel's round-trip mode, so ontology.yaml and ingestion.yaml keep their
comments and formatting. Every function takes and returns text: nothing here writes files.
"""

import difflib
import io
import os
from pathlib import Path

from ruamel.yaml import YAML
from ruamel.yaml.comments import CommentedMap

from agentic_lakehouse.drift.proposal import MacroChange, Proposal
from agentic_lakehouse.drift.signals import ColumnProfile


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


def apply_ontology(text: str, proposal: Proposal) -> str:
    """ontology.yaml text with the proposal's new dimensions and metrics appended to their models.
    Raises ValueError, with a message for Claude, for a proposal that can't apply."""
    if proposal.changes:
        raise ValueError("changes to existing dimensions and metrics aren't supported yet")
    yaml = _yaml()
    data = yaml.load(text)
    models = {m["name"]: m for m in data["models"]}

    def model(kind: str, item) -> CommentedMap:
        if item.model not in models:
            raise ValueError(f"{kind} '{item.name}': there is no model named '{item.model}'")
        return models[item.model]

    for dimension in proposal.dimensions:
        entry = CommentedMap(name=dimension.name, type=dimension.type)
        if dimension.label:
            entry["label"] = dimension.label
        entry["description"] = dimension.description
        if dimension.sql:
            entry["sql"] = dimension.sql
        model("dimension", dimension)["dimensions"].append(entry)
    for metric in proposal.metrics:
        entry = CommentedMap(name=metric.name)
        if metric.label:
            entry["label"] = metric.label
        entry["description"] = metric.description
        if metric.type == "ratio":
            entry.update(type="ratio", numerator=metric.numerator, denominator=metric.denominator)
        else:
            entry["agg"] = metric.agg
            for key in ("percentile", "sql"):
                if getattr(metric, key) is not None:
                    entry[key] = getattr(metric, key)
            if metric.filters:
                entry["filters"] = list(metric.filters)
        for key in ("format", "unit", "better"):
            if getattr(metric, key):
                entry[key] = getattr(metric, key)
        model("metric", metric)["metrics"].append(entry)
    return _dump(yaml, data)


def apply_macros(sources: dict[str, str], changes: list[MacroChange], reserved: set[str]) -> dict[str, str]:
    """The project's macro files after `changes`: only the files that change, by name
    ("default.sql", "trino.sql", ...), new files included. `sources` holds the files that exist;
    `reserved` the built-in macro names a change can't use. Raises ValueError, with a message for
    Claude, for changes that can't apply."""
    if changes:
        raise ValueError("macro changes aren't supported yet")
    return {}


def config_diff(config_dir: str | Path, patched: dict[str, str]) -> str:
    """A unified diff of `patched` (path relative to the config folder -> new text) against the
    files on disk, for `git apply` from the repository root. A file that doesn't exist yet is
    created."""
    config_dir = Path(config_dir)
    prefix = os.path.relpath(config_dir).replace(os.sep, "/")
    lines: list[str] = []
    for name, text in patched.items():
        path = config_dir / name
        original = path.read_text() if path.exists() else None
        diff = difflib.unified_diff(
            _lines(original or ""),
            _lines(text),
            fromfile=f"a/{prefix}/{name}" if original is not None else "/dev/null",
            tofile=f"b/{prefix}/{name}",
        )
        for line in diff:
            # git apply needs the marker when a file doesn't end with a newline
            lines.append(line if line.endswith("\n") else line + "\n\\ No newline at end of file\n")
    return "".join(lines)


def _lines(text: str) -> list[str]:
    return text.splitlines(keepends=True)
