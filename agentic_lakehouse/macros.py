"""SQL macros: the engine's templates, extended and overridden by each project.

The compiler renders every query from Jinja templates (templates/query.sql and
merge.sql) whose engine-specific parts are macros, like dbt's adapter.dispatch.
A project adds its own in config/macros/, the same layout as the engine's
templates/macros/: default.sql for every engine, and pinot.sql, trino.sql and
duckdb.sql for engine-specific versions. For each engine, a macro's name is
looked up in

    1. the project's <engine>.sql      2. the engine's macros/<engine>.sql
    3. the project's default.sql       4. the engine's macros/default.sql

so an engine-specific macro always beats a generic one, and at the same level
the project's beats the engine's: a project can override a built-in such as
`percentile` for one engine or all of them, and a generic project macro never
silently replaces an engine-specific built-in. Macros call each other as
adapter.<name>, which looks them up the same way.

Projects use macros in ontology SQL snippets (`filters: ["{{ human_edit() }}"]`),
in views and in steps; a comment right before a macro describes it to people
and agents. Design: docs/design/project-macros.md.
"""

import re
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import jinja2
from jinja2 import nodes

ENGINES = ("duckdb", "trino", "pinot")
TEMPLATES = Path(__file__).parent / "templates"

_JINJA = re.compile(r"\{\{|\{%")
# A macro's description: the comment right before its definition
_DESCRIBED = re.compile(r"\{#-?\s*(.*?)\s*-?#\}\s*\{%-?\s*macro\s+([A-Za-z_]\w*)", re.DOTALL)
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


class MacroError(ValueError):
    """A macro file isn't valid Jinja, or rendering with the macros failed."""


@dataclass(frozen=True)
class MacroInfo:
    """A macro people and agents can use: what it takes, what it's for, and where it's defined."""

    name: str
    args: list[str]
    description: str
    files: list[str] = field(default_factory=list)  # e.g. ["project/default.sql", "project/trino.sql"]


def templated(text: str) -> bool:
    """Whether `text` has Jinja in it, so it's rendered with the macros before use."""
    return bool(_JINJA.search(text))


def _environment(project: Path | None) -> jinja2.Environment:
    loaders = {"engine": jinja2.FileSystemLoader(TEMPLATES)}
    if project is not None:
        loaders["project"] = jinja2.FileSystemLoader(project)
    env = jinja2.Environment(
        loader=jinja2.PrefixLoader(loaders),
        undefined=jinja2.StrictUndefined,  # a typo fails instead of rendering ""
        trim_blocks=True,
        lstrip_blocks=True,
        autoescape=False,  # this renders SQL, not HTML; values are already escaped by sqlglot
    )
    env.filters["sql_timestamp"] = lambda ts: ts.replace(tzinfo=None).isoformat(sep=" ")
    # Rounded up: Pinot holds whole milliseconds, and for an integer x, x >= 12.0005 is x >= 13
    # (likewise for <). Flooring would put an event at 12 ms on both sides of a tier split.
    env.filters["epoch_millis"] = lambda ts: -((_EPOCH - ts) // timedelta(milliseconds=1))
    return env


class ProjectMacros:
    """The macros a project's SQL is rendered with: its own (config/macros/), over the engine's.
    With no folder, just the engine's."""

    def __init__(self, directory: str | Path | None = None):
        directory = Path(directory) if directory is not None else None
        self.directory = directory if directory is not None and directory.is_dir() else None
        self.env = _environment(self.directory)
        self.files = [name for name in ("default", *ENGINES) if self.directory and (self.directory / f"{name}.sql").is_file()]
        for name in self.files:  # a syntax error fails now, naming the file and line
            path = self.directory / f"{name}.sql"
            try:
                self.env.parse(path.read_text(), name=f"{name}.sql", filename=str(path))
            except jinja2.TemplateSyntaxError as e:
                raise MacroError(f"macros/{name}.sql, line {e.lineno}: {e.message}") from None
        self._adapters: dict[str, Adapter] = {}

    def adapter(self, engine: str) -> "Adapter":
        """The macros for `engine`, looked up in the four places, in order."""
        if engine not in self._adapters:
            self._adapters[engine] = Adapter(self, engine)
        return self._adapters[engine]

    def render(self, text: str, engine: str) -> str:
        """`text` (a SQL snippet, a view, a step) with its Jinja rendered for `engine`. Macros are
        called by name, like {{ human_edit() }}, or as {{ adapter.human_edit() }}."""
        if not templated(text):
            return text
        adapter = self.adapter(engine)
        try:
            return self.env.from_string(text).render(adapter.callables()).strip()
        except jinja2.TemplateSyntaxError as e:
            raise MacroError(f"line {e.lineno}: {e.message}") from None
        except jinja2.UndefinedError as e:
            raise MacroError(f"{e.message} (macros: {', '.join(sorted(adapter.names())) or 'none'})") from None
        except TypeError as e:  # e.g. a macro called with an argument it doesn't take
            raise MacroError(str(e)) from None

    def catalog(self) -> list[MacroInfo]:
        """Every macro, the project's and the engine's, with where each is defined."""
        sources = [(f"project/{name}.sql", self.directory / f"{name}.sql") for name in self.files]
        sources += [(f"engine/macros/{name}.sql", TEMPLATES / "macros" / f"{name}.sql") for name in ("default", *ENGINES)]
        found: dict[str, MacroInfo] = {}
        for label, path in sources:
            text = path.read_text()
            described = {name: " ".join(comment.split()) for comment, name in _DESCRIBED.findall(text)}
            for macro in self.env.parse(text).find_all(nodes.Macro):
                info = found.get(macro.name)
                if info is None:
                    info = found[macro.name] = MacroInfo(macro.name, [arg.name for arg in macro.args], described.get(macro.name, ""))
                elif not info.description and described.get(macro.name):
                    info = found[macro.name] = MacroInfo(info.name, info.args, described[macro.name], info.files)
                info.files.append(label)
        return sorted(found.values(), key=lambda m: (not any(f.startswith("project/") for f in m.files), m.name))


class Adapter:
    """One engine's macros, dbt-style: attribute access finds a macro in the project's file for
    the engine, the engine's file for it, the project's default.sql, then the engine's. Each
    macro returns a stripped SQL string."""

    def __init__(self, macros: ProjectMacros, engine: str):
        self.engine = engine
        context = {"adapter": self}
        names = [
            *([f"project/{engine}.sql"] if engine in macros.files else []),
            f"engine/macros/{engine}.sql",
            *(["project/default.sql"] if "default" in macros.files else []),
            "engine/macros/default.sql",
        ]
        self._modules = [macros.env.get_template(name).make_module(context) for name in names]

    def __getattr__(self, name: str):
        if name.startswith("_"):  # not a macro; also stops recursion before _modules exists
            raise AttributeError(name)
        for module in self._modules:
            if (macro := getattr(module, name, None)) is not None:
                return lambda *args, **kwargs: str(macro(*args, **kwargs)).strip()
        raise AttributeError(f"no SQL macro '{name}' for engine '{self.engine}'")

    def names(self) -> set[str]:
        return {name for module in self._modules for name, value in vars(module).items() if isinstance(value, jinja2.runtime.Macro)}

    def callables(self) -> dict[str, Any]:
        """The render context: every macro by name, and `adapter`."""
        return {name: getattr(self, name) for name in self.names()} | {"adapter": self}


NO_PROJECT_MACROS = ProjectMacros(None)
