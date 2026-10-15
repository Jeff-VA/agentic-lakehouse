"""A proposal applied to the text of the config files, and the diff a person reviews.

ontology.yaml and ingestion.yaml are edited through ruamel's round-trip mode, so they keep their
comments, blank lines and styles. Macro files are edited as text, one macro's block at a time.
Every function takes and returns text: nothing here writes files.
"""

import difflib
import io
import json
import os
import re
import textwrap
from pathlib import Path
from typing import Any

import jinja2
from ruamel.yaml import YAML
from ruamel.yaml.comments import CommentedMap, CommentedSeq
from ruamel.yaml.error import CommentMark
from ruamel.yaml.scalarstring import (
    DoubleQuotedScalarString,
    FoldedScalarString,
    LiteralScalarString,
    PlainScalarString,
    SingleQuotedScalarString,
)
from ruamel.yaml.tokens import CommentToken

from agentic_lakehouse.drift.proposal import DefinitionChange, MacroChange, Proposal, ProposedDimension, ProposedMetric
from agentic_lakehouse.drift.signals import ColumnProfile
from agentic_lakehouse.macros import _DESCRIBED, ENGINES

LINE_WIDTH = 100  # a flow-style entry or list stays on one line while that line fits

# The order a definition's keys are written in. A key new to an existing entry goes after the last
# of its keys that comes before it in this order.
DIMENSION_KEYS = ("name", "type", "label", "description", "sql")
METRIC_KEYS = ("name", "description", "type", "numerator", "denominator", "agg", "percentile", "sql", "filters", "label", "format", "unit", "better")

_COLUMN_NAME = re.compile(r"[a-z_][a-z0-9_]*")
_BLOCK_SCALARS = (FoldedScalarString, LiteralScalarString)
_QUOTED = (SingleQuotedScalarString, DoubleQuotedScalarString)


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
    """ingestion.yaml text with `columns` appended to `dataset`'s, one line each."""
    yaml = _yaml()
    data = yaml.load(text)
    [entry] = [d for d in data["datasets"] if d["name"] == dataset]
    for column in columns:
        item = CommentedMap(name=column.name, type=column.type)
        item.fa.set_flow_style()  # one line, like the existing columns
        _append(entry["columns"], item)
    return _dump(yaml, data)


# --- ontology.yaml


def apply_ontology(text: str, proposal: Proposal) -> str:
    """ontology.yaml text with the proposal applied: its changes made to existing dimensions and
    metrics, and its new ones appended to their models. Comments, blank lines, quotes and each
    entry's style are kept; a flow-style entry or list whose line no longer fits in LINE_WIDTH
    becomes block style. Raises ValueError, with a message for Claude naming every problem, for a
    proposal that can't apply."""
    yaml = _yaml()
    data = yaml.load(text)
    problems: list[str] = []
    flows: list[tuple] = []  # where flow-style collections were written, to check that they fit
    changed: set[tuple[str, str, str]] = set()
    for change in proposal.changes:
        try:
            flows += _change(data, change, changed)
        except ValueError as e:
            problems.append(str(e))
    for item in [*proposal.dimensions, *proposal.metrics]:
        try:
            flows += _add(data, item)
        except ValueError as e:
            problems.append(str(e))
    if problems:
        raise ValueError("; ".join(problems))
    return _fitted(yaml, data, flows)


def _change(data, change: DefinitionChange, changed: set) -> list[tuple]:
    """Makes `change` to its definition. Returns where it wrote flow-style collections."""
    what = f"change to {change.kind} '{change.name}'"
    path = _definition(data, change, what)
    entry, model = _at(data, path), _at(data, path[:2])
    if (change.model, change.kind, change.name) in changed:
        raise ValueError(f"{what}: another change already changes it; give one change with all of its edits")
    changed.add((change.model, change.kind, change.name))
    # [] on a dimension or a ratio metric says what is already true: it has no filters
    if change.kind == "dimension" and change.filters:
        raise ValueError(f"{what}: a dimension has no filters; give null")
    if entry.get("type") == "ratio" and (change.sql is not None or change.filters):
        raise ValueError(f"{what}: a ratio metric has no sql or filters; change the metrics it divides instead")

    before = json.dumps(entry, default=str)
    keys = DIMENSION_KEYS if change.kind == "dimension" else METRIC_KEYS
    if change.description is not None:
        indent = (entry.lc.col or 0) + 2  # where a folded description's lines start
        _set(entry, "description", _prose(change.description, like=entry.get("description"), indent=indent), keys)
    if change.sql is not None:
        _set(entry, "sql", _sql(change.sql, like=entry.get("sql")), keys)
    flows = [path] if entry.fa.flow_style() else []
    if change.filters:
        old = entry.get("filters")
        flow = entry.fa.flow_style() or (old.fa.flow_style() if isinstance(old, CommentedSeq) else _flow_filters(model))
        _set(entry, "filters", _filters(change.filters, old, flow), keys)
        flows += [(*path, "filters")] if flow else []
    elif change.filters == [] and "filters" in entry:
        _delete(entry, "filters")
    if json.dumps(entry, default=str) == before:
        raise ValueError(f"{what}: it changes nothing; give a new description, sql or filters, or answer its signals in `unchanged`")
    return flows


def _definition(data, change: DefinitionChange, what: str) -> tuple:
    """The path to the dimension or metric `change` names."""
    m = _model_index(data, change.model, what)
    entries = data["models"][m][f"{change.kind}s"]
    names = [entry["name"] for entry in entries]
    if change.name not in names:
        raise ValueError(
            f"{what}: model '{change.model}' has no {change.kind} named '{change.name}'; its {change.kind}s: {', '.join(names)}"
        )
    return ("models", m, f"{change.kind}s", names.index(change.name))


def _model_index(data, model: str, what: str) -> int:
    names = [m["name"] for m in data["models"]]
    if model not in names:
        raise ValueError(f"{what}: there is no model named '{model}'; models: {', '.join(names)}")
    return names.index(model)


def _add(data, item: ProposedDimension | ProposedMetric) -> list[tuple]:
    """Appends a new dimension or metric to its model. Returns where it wrote flow-style collections."""
    kind = "dimension" if isinstance(item, ProposedDimension) else "metric"
    m = _model_index(data, item.model, f"{kind} '{item.name}'")
    model = data["models"][m]
    entry = _new_dimension(item) if kind == "dimension" else _new_metric(item, _flow_filters(model))
    _append(model[f"{kind}s"], entry)
    filters = entry.get("filters")
    if isinstance(filters, CommentedSeq) and filters.fa.flow_style():
        return [("models", m, f"{kind}s", len(model[f"{kind}s"]) - 1, "filters")]
    return []


def _new_dimension(dimension: ProposedDimension) -> CommentedMap:
    entry = CommentedMap(name=dimension.name, type=dimension.type)
    if dimension.label:
        entry["label"] = dimension.label
    entry["description"] = _prose(dimension.description)
    if dimension.sql:
        entry["sql"] = _sql(dimension.sql)
    return entry


def _new_metric(metric: ProposedMetric, flow_filters: bool) -> CommentedMap:
    entry = CommentedMap(name=metric.name, description=_prose(metric.description))
    if metric.type == "ratio":
        if metric.sql is not None or metric.filters:
            raise ValueError(f"metric '{metric.name}': a ratio metric has no sql or filters; put them on the metrics it divides")
        entry.update(type="ratio", numerator=metric.numerator, denominator=metric.denominator)
    else:
        entry["agg"] = metric.agg
        if metric.percentile is not None:
            entry["percentile"] = metric.percentile
        if metric.sql is not None:
            entry["sql"] = _sql(metric.sql)
        if metric.filters:
            entry["filters"] = _filters(metric.filters, None, flow_filters)
    for key in ("label", "format", "unit", "better"):
        if getattr(metric, key):
            entry[key] = getattr(metric, key)
    return entry


def _flow_filters(model: CommentedMap) -> bool:
    """Whether the model writes filters in flow style, filters: ["..."], as its first list of them is."""
    lists = [m["filters"] for m in model["metrics"] if isinstance(m.get("filters"), CommentedSeq) and m["filters"]]
    return bool(lists and lists[0].fa.flow_style())


def _filters(filters: list[str], old: Any, flow: bool) -> CommentedSeq:
    """A list of filters. One that was already in `old` keeps its quotes and its comment."""
    kept = {str(item).strip(): i for i, item in enumerate(old)} if isinstance(old, CommentedSeq) else {}
    seq = CommentedSeq()
    for sql in filters:
        i = kept.get(sql.strip())
        if i is None:
            seq.append(_sql(sql))
            continue
        seq.append(old[i])
        eol, _, mark = _after(old, i)
        if eol:
            _set_after(seq, len(seq) - 1, eol, "", mark)
    if flow:
        seq.fa.set_flow_style()
    return seq


def _prose(text: str, like: Any = None, indent: int = 0) -> str:
    """A description, on one line, in the style of `like`, the value it replaces: quoted the same
    way, or for a block scalar, folded (>) to fit in LINE_WIDTH with its lines starting at `indent`."""
    text = " ".join(text.split())
    if isinstance(like, _BLOCK_SCALARS):
        folded = FoldedScalarString(text + ("\n" if like.endswith("\n") else ""))
        folded.fold_pos = _folds(text, LINE_WIDTH - indent)
        return folded
    if isinstance(like, _QUOTED):
        return type(like)(text)
    return text


def _folds(text: str, width: int) -> list[int]:
    """Where to fold single-spaced `text` so each line fits in `width`: the spaces that break."""
    folds, position = [], 0
    for line in textwrap.wrap(text, width, break_long_words=False, break_on_hyphens=False)[:-1]:
        position += len(line)
        folds.append(position)
        position += 1
    return folds


def _sql(text: str, like: Any = None) -> str:
    """SQL as the config writes it: a column name plain, other SQL in double quotes (or the quotes
    of `like`, the value it replaces), and SQL on several lines as a literal block (|). Always a
    ScalarString: assigned over one, a plain str would take on the old value's style."""
    text = text.strip()
    if "\n" in text:
        return LiteralScalarString(text)
    if isinstance(like, DoubleQuotedScalarString) or (isinstance(like, SingleQuotedScalarString) and "'" not in text):
        return type(like)(text)
    return PlainScalarString(text) if _COLUMN_NAME.fullmatch(text) else DoubleQuotedScalarString(text)


def _set(entry: CommentedMap, key: str, value: Any, order: tuple[str, ...]) -> None:
    """entry[key] = value: in place of its old value, or as a new key in its place in `order`."""
    if key in entry:
        _replace(entry, key, value)
        return
    keys = list(entry)
    earlier = [k for k in keys if k in order and order.index(k) < order.index(key)]
    position = keys.index(earlier[-1]) + 1 if earlier else len(keys)
    if position < len(keys):
        entry.insert(position, key, value)
        return
    eol, rest, mark = _after(entry, keys[-1])  # the new last key: what followed the entry follows it
    _set_after(entry, keys[-1], eol, "", mark)
    entry.insert(position, key, value)
    _set_after(entry, key, "", rest)


def _delete(entry: CommentedMap, key: str) -> None:
    """Removes entry[key] and its comments. The blank lines and comment lines after it stay."""
    keys = list(entry)
    _, rest, _ = _after(entry, key)
    del entry[key]
    entry.ca.items.pop(key, None)
    if rest and keys.index(key):
        previous = keys[keys.index(key) - 1]
        eol, lines, mark = _after(entry, previous)
        _set_after(entry, previous, eol, lines + rest, mark)


def _append(items: CommentedSeq, entry: Any) -> None:
    """Appends to a list in the list's layout: what followed its last item (a blank line, a comment)
    follows the new one, and items are set apart by blank lines if the list's are."""
    last = len(items) - 1
    if last < 0:
        items.append(entry)
        return
    eol, rest, mark = _after(items, last)
    between = ""  # the blank lines between the last two items, if any; not their comments
    if last:
        between = "\n" * re.match(r"(?:[ \t]*\n)*", _after(items, last - 1)[1]).group().count("\n")
    _set_after(items, last, eol, between, mark)
    items.append(entry)
    _set_after(items, last + 1, "", rest)


# ruamel keeps what follows a value in its parent's comment slot for it: slot 2 of ca.items[key] in
# a mapping, slot 0 in a sequence. The token there holds the comment ending the value's line, a
# newline, then the blank lines and comment lines that follow. After a block scalar (| or >),
# which ends its own line, it holds just those lines. A block collection's is its last item's.


def _end(node: Any, key: Any) -> tuple[Any, Any]:
    """The (parent, key) of the scalar or flow-style collection that node[key]'s text ends with."""
    value = node[key]
    if isinstance(value, (CommentedMap, CommentedSeq)) and value and not value.fa.flow_style():
        return _end(value, list(value)[-1] if isinstance(value, CommentedMap) else len(value) - 1)
    return node, key


def _slot(parent: Any) -> int:
    return 2 if isinstance(parent, CommentedMap) else 0


def _after(node: Any, key: Any) -> tuple[str, str, Any]:
    """What follows node[key]: the comment ending its line, the lines after that, and the mark
    placing the comment."""
    parent, key = _end(node, key)
    slots = parent.ca.items.get(key) or []
    token = slots[_slot(parent)] if len(slots) > _slot(parent) else None
    if token is None:
        return "", "", None
    if isinstance(parent[key], _BLOCK_SCALARS):
        return "", token.value, token.start_mark
    eol, _, rest = token.value.partition("\n")
    return eol.strip(), rest, token.start_mark


def _set_after(node: Any, key: Any, eol: str, rest: str, mark: Any = None) -> None:
    """Sets what follows node[key] (see _after)."""
    parent, key = _end(node, key)
    if isinstance(parent[key], _BLOCK_SCALARS):  # nothing can follow it on its line; _eol moves a comment
        token = CommentToken(rest, CommentMark(0)) if rest else None
    else:
        token = _token(eol, rest, mark)
    if token is not None or key in parent.ca.items:
        parent.ca.items.setdefault(key, [None] * 4)[_slot(parent)] = token


def _token(eol: str, rest: str, mark: Any) -> CommentToken | None:
    """What follows a value on its line and after it. A comment ending a line is never at column 0,
    so without a mark there, it goes two spaces after the value."""
    if not eol:
        return CommentToken(f"\n{rest}", CommentMark(0)) if rest else None
    if not getattr(mark, "column", 0):
        return CommentToken(f" {eol}\n{rest}", CommentMark(0))  # the emitter adds the other space
    return CommentToken(f"{eol}\n{rest}", mark)


def _eol(node: CommentedMap, key: str, comment: str, mark: Any) -> None:
    """Puts `comment` at the end of node[key]'s first line or, for a block scalar, whose first line
    is its indicator (| or >), on a line of its own above the key."""
    keys = list(node)
    if isinstance(node[key], _BLOCK_SCALARS):
        if keys.index(key):
            previous = keys[keys.index(key) - 1]
            eol, rest, previous_mark = _after(node, previous)
            _set_after(node, previous, eol, f"{rest}{' ' * (node.lc.col or 0)}{comment}\n", previous_mark)
    elif _end(node, key)[0] is node:  # on one line, before the lines that follow it
        _set_after(node, key, comment, _after(node, key)[1], mark)
    else:  # a block list: on the key's line, above the items
        node.ca.items.setdefault(key, [None] * 4)[2] = _token(comment, "", mark)


def _replace(node: CommentedMap, key: str, value: Any) -> None:
    """node[key] = value, keeping the comment at the end of the key's line, two spaces after the new
    value, and what followed the old value."""
    parent, _ = _end(node, key)
    eol, rest, _ = _after(node, key)
    _set_after(node, key, "", "")
    node[key] = value
    if eol and parent is node:  # not the comment on the last item of an old block list
        _eol(node, key, eol, None)
    end_eol, _, end_mark = _after(node, key)
    _set_after(node, key, end_eol, rest, end_mark)


def _to_block(parent: Any, key: Any) -> None:
    """parent[key], a flow-style collection, in block style. The comment at the end of its line goes
    on its first line; the lines after it, after its last."""
    eol, rest, _ = _after(parent, key)
    _set_after(parent, key, "", "")
    node = parent[key]
    node.fa.set_block_style()
    if eol and isinstance(parent, CommentedSeq):  # - {name: x}  # c  ->  - name: x  # c
        _eol(node, next(iter(node)), eol, None)
    elif eol:  # filters: [...]  # c  ->  filters:  # c
        _eol(parent, key, eol, None)
    end_eol, _, end_mark = _after(parent, key)
    _set_after(parent, key, end_eol, rest, end_mark)


def _fitted(yaml: YAML, data: Any, flows: list[tuple]) -> str:
    """`data` as text, with each flow-style collection at `flows` in block style if its line doesn't
    fit in LINE_WIDTH or it holds text on several lines."""
    while True:
        text = _dump(yaml, data)
        lines, shown = text.splitlines(), yaml.load(text)
        unfit = [
            path
            for path in flows
            if _at(data, path).fa.flow_style()
            and (len(lines[_at(shown, path).lc.line]) > LINE_WIDTH or _multiline(_at(data, path)))
        ]
        # Outermost first: a list inside an entry may fit on a line of its own once the entry doesn't.
        unfit = [path for path in unfit if not any(path[: len(other)] == other for other in unfit if other != path)]
        if not unfit:
            return text
        for path in unfit:
            _to_block(_at(data, path[:-1]), path[-1])


def _multiline(node: Any) -> bool:
    values = node.values() if isinstance(node, CommentedMap) else node
    return any(_multiline(v) if isinstance(v, (CommentedMap, CommentedSeq)) else isinstance(v, str) and "\n" in v for v in values)


def _at(node: Any, path: tuple) -> Any:
    for step in path:
        node = node[step]
    return node


# --- macro files

_ENGINE_NAMES = {"duckdb": "DuckDB", "trino": "Trino", "pinot": "Pinot"}
_MACRO_NAME = re.compile(r"[a-z_][a-z0-9_]*")
_JINJA_CONSTANTS = {"true", "false", "none"}  # names Jinja can't define
_MACRO_TAG = re.compile(r"\{%[-+]?\s*(?:end)?macro\b")
# A macro file's comments, skipped, and its macro tags, which bound each macro's block
_TAGS = re.compile(r"\{#.*?#\}|\{%[-+]?\s*(?:macro\s+(?P<name>\w+)|(?P<end>endmacro)\b).*?%\}", re.DOTALL)
# What a macro file this creates starts with
_HEADERS = {
    "default.sql": """\
{#-
  This project's SQL macros, for every engine. pinot.sql, trino.sql and duckdb.sql hold an
  engine's own versions, used instead of these on that engine. The comment directly above a
  macro, with no blank line between, describes it to people and agents.
-#}""",
    **{
        f"{engine}.sql": f"{{#- {name}'s own versions of this project's macros, used instead of default.sql's on {name}. -#}}"
        for engine, name in _ENGINE_NAMES.items()
    },
}


def apply_macros(sources: dict[str, str], changes: list[MacroChange], reserved: set[str]) -> dict[str, str]:
    """The project's macro files after `changes`: only the files that change, by name
    ("default.sql", "trino.sql", ...), new files included. `sources` holds the files that exist;
    `reserved` the built-in macro names a change can't use.

    Each change writes its macro to default.sql and its engine-specific versions to <engine>.sql,
    as a block with its description directly above it, in place of the macro's old block or after
    the others. An engine's version the change leaves out (null) is removed. Raises ValueError,
    with a message for Claude naming every problem, for changes that can't apply."""
    problems = [problem for change in changes for problem in _macro_problems(change, reserved)]
    names = [change.name for change in changes]
    problems += [
        f"macro '{name}': more than one change has that name; give it one change, with every version"
        for name in dict.fromkeys(name for name in names if names.count(name) > 1)
    ]
    if problems:
        raise ValueError("; ".join(problems))
    files = {name: sources.get(name) for name in ("default.sql", *(f"{engine}.sql" for engine in ENGINES))}
    for change in changes:
        versions = {"default.sql": change.sql} | {f"{engine}.sql": getattr(change, engine) for engine in ENGINES}
        for name, sql in versions.items():
            if sql is not None:
                files[name] = _put_macro(files[name], change.name, _macro_block(change, sql), name)
            elif files[name] is not None:
                files[name] = _drop_macro(files[name], change.name)
    return {name: text for name, text in files.items() if text is not None and text != sources.get(name)}


def _macro_problems(change: MacroChange, reserved: set[str]) -> list[str]:
    what = f"macro '{change.name}'"
    problems = []
    if not _MACRO_NAME.fullmatch(change.name):
        problems.append(f"{what}: a macro's name is lowercase letters, digits and underscores, not starting with a digit")
    elif change.name in reserved:
        problems.append(f"{what}: a built-in has that name; choose another")
    elif change.name in _JINJA_CONSTANTS:
        problems.append(f"{what}: Jinja reads that name as a constant; choose another")
    for arg in change.args:
        if not _MACRO_NAME.fullmatch(arg):
            problems.append(f"{what}: argument '{arg}' isn't a name; use lowercase letters, digits and underscores")
        elif arg in _JINJA_CONSTANTS:
            problems.append(f"{what}: argument '{arg}' is a name Jinja reads as a constant; choose another")
    problems += [
        f"{what}: argument '{arg}' appears more than once"
        for arg in dict.fromkeys(arg for arg in change.args if change.args.count(arg) > 1)
    ]
    description = " ".join(change.description.split())
    if not description:
        problems.append(f"{what}: its description is empty; say what the macro means, for people and agents")
    elif "{#" in description or "#}" in description:
        problems.append(f"{what}: its description can't contain {{# or #}}, which Jinja reads as a comment's ends")
    parses = not problems  # a block with a bad name or description fails to parse for that reason
    for engine, sql in [(None, change.sql), *((engine, getattr(change, engine)) for engine in ENGINES)]:
        version = f"its {_ENGINE_NAMES[engine]} version" if engine else "its SQL"
        if sql is None:
            continue
        if not sql.strip():
            fix = f"; give null where {_ENGINE_NAMES[engine]} uses the SQL for every engine" if engine else ""
            problems.append(f"{what}: {version} is empty{fix}")
        elif _MACRO_TAG.search(sql):
            problems.append(f"{what}: {version} can't define macros ({{% macro or {{% endmacro); give each macro its own change")
        elif parses:
            try:
                jinja2.Environment().parse(_macro_block(change, sql))
            except jinja2.TemplateSyntaxError as e:
                problems.append(f"{what}: {version} isn't valid Jinja: {e.message}")
    return problems


def _macro_block(change: MacroChange, sql: str) -> str:
    """A macro as the project's files write it, with its description directly above it."""
    description = " ".join(change.description.split())
    return f"{{# {description} #}}\n{{% macro {change.name}({', '.join(change.args)}) %}}{sql.strip()}{{% endmacro %}}"


def _put_macro(text: str | None, name: str, block: str, file: str) -> str:
    """Macro file `file`'s text with `block` in place of macro `name`'s block, or after the others."""
    if text is None:
        return f"{_HEADERS[file]}\n\n{block}\n"
    spans = _macro_spans(text, name)
    if not spans:
        return f"{text.rstrip()}\n\n{block}\n" if text.strip() else f"{block}\n"
    (start, end), *others = spans
    for span in reversed(others):  # a macro defined twice keeps one definition, in the first's place
        text = _cut(text, *span)
    return text[:start] + block + text[end:]


def _drop_macro(text: str, name: str) -> str:
    for span in reversed(_macro_spans(text, name)):
        text = _cut(text, *span)
    return text


def _macro_spans(text: str, name: str) -> list[tuple[int, int]]:
    """Where macro `name` is defined in a macro file, at the top level: from its description, the
    comment directly above it as macros.py reads it, through its {% endmacro %}."""
    described = {text.index("{%", m.end(1)): m.start() for m in _DESCRIBED.finditer(text)}
    spans, depth, start, current = [], 0, 0, ""
    for tag in _TAGS.finditer(text):
        if tag["name"]:
            if not depth:
                start, current = tag.start(), tag["name"]
            depth += 1
        elif tag["end"] and depth:
            depth -= 1
            if not depth and current == name:
                spans.append((described.get(start, start), tag.end()))
    return spans


def _cut(text: str, start: int, end: int) -> str:
    """`text` without the block text[start:end]. A block with its lines to itself goes with its
    lines and one blank line beside them: the one after it, or else the one before."""
    first = text.rfind("\n", 0, start) + 1
    last = text.find("\n", end) + 1 or len(text)
    if text[first:start].strip() or text[end:last].strip():
        return text[:start] + text[end:]
    before, after = text[:first], text[last:]
    if blank := re.match(r"[ \t]*\r?\n", after):
        after = after[blank.end() :]
    elif blank := re.search(r"(?:\A|(?<=\n))[ \t]*\r?\n\Z", before):
        before = before[: blank.start()]
    return before + after


# --- the diff


def config_diff(config_dir: str | Path, patched: dict[str, str]) -> str:
    """A unified diff of `patched` (path relative to the config folder, e.g. "macros/trino.sql" ->
    new text) against the files on disk, for `git apply` from the repository root, which is taken to
    be the working directory. A file that doesn't exist yet is created."""
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
