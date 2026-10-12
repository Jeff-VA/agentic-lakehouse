"""Dashboards: the project's HTML pages in dashboards/, drawn with al.js tags.

check_dashboards() reads every page and checks each al-* tag against the
views it draws, without a browser: the tag exists, its view exists, the
columns it draws are in that view and of a type it can draw, and the
parameters it sets, or that a <al-param> sets for the whole page, are ones
the view takes, with values it accepts. A $name in an attribute is checked
for every value the page parameter `name` can take.

What only a browser can tell (a query that fails, a view with no rows yet) is
what agentic_lakehouse/browser.py reports when it renders a page.
"""

import difflib
import itertools
import re
from collections.abc import Mapping
from dataclasses import dataclass
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import parse_qsl

from agentic_lakehouse.views import View, ViewError

DASHBOARD_NAME = re.compile(r"^[A-Za-z0-9_-]+$")
VIEW_TAGS = {"al-kpi", "al-bars", "al-lines", "al-table", "al-text"}
TAGS = VIEW_TAGS | {"al-param", "al-freshness", "al-gates", "al-drift", "al-live", "al-theme"}
# The attributes each tag needs, and those that name columns of its view.
REQUIRED = {"al-kpi": ["view", "value"], "al-bars": ["view", "category", "value"], "al-lines": ["view", "x", "y"],
            "al-table": ["view"], "al-text": ["view"], "al-param": ["name", "options"]}
COLUMNS = {"al-kpi": ["value"], "al-bars": ["category", "value"], "al-lines": ["x", "y", "series"]}
NUMERIC = {("al-kpi", "value"), ("al-bars", "value"), ("al-lines", "y")}
CHOICES = {("al-bars", "sort"): {"view", "asc", "desc"}, ("al-param", "kind"): {"buttons", "select"}}
PARAM = re.compile(r"\$([a-z][a-z0-9_]*)")


@dataclass(frozen=True)
class Problem:
    file: str  # e.g. "dashboards/playback.html"
    line: int
    tag: str  # the tag, e.g. 'al-lines view="isp_hourly"'
    message: str

    def __str__(self) -> str:
        return f"{self.file}:{self.line}: <{self.tag}> {self.message}"


def dashboard_files(directory: str | Path | None) -> dict[str, Path]:
    """The dashboards in `directory`, by name (the file name without .html)."""
    directory = Path(directory) if directory else None
    if directory is None or not directory.is_dir():
        return {}
    return {p.stem: p for p in sorted(directory.glob("*.html")) if DASHBOARD_NAME.match(p.stem)}


def check_dashboards(directory: str | Path, views: Mapping[str, View], datasets: list[str] | None = None) -> list[Problem]:
    """Every problem in every dashboard in `directory`, in file and line order."""
    return [problem for path in dashboard_files(directory).values() for problem in check_dashboard(path, views, datasets)]


@dataclass
class _Tag:
    name: str
    attrs: dict[str, str]
    line: int
    text: str = ""  # al-text: the template inside it

    def __str__(self) -> str:
        key = next((k for k in ("view", "name", "dataset") if k in self.attrs), None)
        return f'{self.name} {key}="{self.attrs[key]}"' if key else self.name


class _Reader(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.tags: list[_Tag] = []
        self.scripts: list[str] = []
        self.title = ""
        self._text: _Tag | None = None
        self._in_title = False

    def handle_starttag(self, tag, attrs):
        self._in_title = tag == "title"
        if tag == "script":
            self.scripts.append(dict(attrs).get("src") or "")
        if tag.startswith("al-"):
            found = _Tag(tag, {k: v or "" for k, v in attrs}, self.getpos()[0])
            self.tags.append(found)
            if tag == "al-text":
                self._text = found

    def handle_endtag(self, tag):
        if tag == "al-text":
            self._text = None
        self._in_title = False

    def handle_data(self, data):
        if self._text is not None:
            self._text.text += data
        if self._in_title:
            self.title += data


def describe_dashboard(path: Path) -> tuple[str, list[str]]:
    """A dashboard's <title>, and the views its tags draw."""
    reader = _Reader()
    reader.feed(path.read_text())
    views = [tag.attrs["view"] for tag in reader.tags if tag.name in VIEW_TAGS and tag.attrs.get("view")]
    return reader.title.strip(), list(dict.fromkeys(views))


def check_dashboard(path: Path, views: Mapping[str, View], datasets: list[str] | None = None) -> list[Problem]:
    reader = _Reader()
    reader.feed(path.read_text())
    file = f"{path.parent.name}/{path.name}"
    problems: list[Problem] = []

    def problem(tag: _Tag, message: str) -> None:
        problems.append(Problem(file, tag.line, str(tag), message))

    if reader.tags and not any(src.rstrip("/").endswith("al.js") for src in reader.scripts):
        problems.append(Problem(file, 1, "script", 'the page uses al-* tags but never loads them: add <script type="module" src="/al.js"></script>'))

    # Page parameters: every value each one can take.
    page: dict[str, list[str]] = {}
    for tag in reader.tags:
        if tag.name == "al-param" and tag.attrs.get("name"):
            options = [o.split("=")[0].strip() for o in tag.attrs.get("options", "").split(",") if o.strip()]
            page[tag.attrs["name"]] = options
            default = tag.attrs.get("value")
            if default is not None and options and default not in options:
                problem(tag, f'value="{default}" isn\'t one of its options ({", ".join(options)})')

    for tag in reader.tags:
        if tag.name not in TAGS:
            close = difflib.get_close_matches(tag.name, TAGS, n=1)
            problem(tag, f"no such tag{f'; did you mean <{close[0]}>?' if close else ''} Tags: {', '.join(sorted(TAGS))}")
            continue
        for attribute in REQUIRED.get(tag.name, []):
            if not tag.attrs.get(attribute):
                problem(tag, f"needs a {attribute} attribute")
        for (name, attribute), allowed in CHOICES.items():
            if tag.name == name and attribute in tag.attrs and tag.attrs[attribute] not in allowed:
                problem(tag, f'{attribute}="{tag.attrs[attribute]}" must be one of {", ".join(sorted(allowed))}')
        unknown_params = sorted({p for value in tag.attrs.values() for p in PARAM.findall(value)} - page.keys())
        for name in unknown_params:
            problem(tag, f"${name} names no page parameter: add <al-param name=\"{name}\" ...> or the value itself")
        if tag.name == "al-freshness" and datasets is not None and tag.attrs.get("dataset") not in (None, *datasets):
            problem(tag, f'no dataset named "{tag.attrs["dataset"]}"; datasets: {", ".join(datasets)}')
        if tag.name == "al-param" and tag.attrs.get("view"):
            _check_view_name(tag, tag.attrs["view"], views, problem)
        if tag.name in VIEW_TAGS and tag.attrs.get("view"):
            for view_name in _expand(tag.attrs["view"], page):
                if (view := _check_view_name(tag, view_name, views, problem)) is not None:
                    _check_view_tag(tag, view, page, problem)

    # A page parameter fills the parameter of the same name in every view the page draws.
    drawn = {view_name for tag in reader.tags if tag.name in VIEW_TAGS for view_name in _expand(tag.attrs.get("view", ""), page)}
    for tag in reader.tags:
        name = tag.attrs.get("name")
        if tag.name != "al-param" or not name:
            continue
        for view_name in sorted(drawn & views.keys()):
            param = views[view_name].params.get(name)
            for option in page.get(name, []) if param else []:
                try:
                    param.convert(option, f'option "{option}"')
                except ViewError as e:
                    problem(tag, f"{e}, as view {view_name}'s parameter {name}")
    return sorted(problems, key=lambda p: p.line)


def _expand(value: str, page: Mapping[str, list[str]]) -> list[str]:
    """Every value an attribute can take as its $parameters take each of their options."""
    names = sorted(set(PARAM.findall(value)) & page.keys())
    if not names:
        return [value]
    expanded = []
    for combination in itertools.product(*(page[name] for name in names)):
        choice = dict(zip(names, combination))
        expanded.append(PARAM.sub(lambda m: choice.get(m[1], m[0]), value))
    return list(dict.fromkeys(expanded))


def _check_view_name(tag: _Tag, name: str, views: Mapping[str, View], problem) -> View | None:
    if name in views:
        return views[name]
    close = difflib.get_close_matches(name, views, n=1)
    hint = f'did you mean "{close[0]}"? ' if close else ""
    problem(tag, f'no view named "{name}"; {hint}views: {", ".join(views) or "none"}')
    return None


def _check_view_tag(tag: _Tag, view: View, page: Mapping[str, list[str]], problem) -> None:
    columns = {c.name: c for c in view.columns}

    def check_column(attribute: str, name: str) -> None:
        if PARAM.search(name):  # a $name that names no page parameter, reported already
            return
        column = columns.get(name)
        if column is None:
            close = difflib.get_close_matches(name, columns, n=1)
            hint = f'did you mean "{close[0]}"? ' if close else ""
            problem(tag, f'{attribute}: view {view.name} has no column "{name}"; {hint}columns: {", ".join(columns)}')
        elif (tag.name, attribute) in NUMERIC and column.type != "number":
            problem(tag, f'{attribute}: column "{name}" holds {column.type} values; it needs numbers')
        elif tag.name == "al-lines" and attribute == "x" and column.type not in ("timestamp", "date"):
            problem(tag, f'x: column "{name}" holds {column.type} values; lines run over a time column')

    for attribute in COLUMNS.get(tag.name, []):
        for value in _expand(tag.attrs.get(attribute, ""), page) if tag.attrs.get(attribute) else []:
            check_column(attribute, value)
    if tag.name == "al-table":
        for value in _expand(tag.attrs.get("columns", ""), page):
            for name in filter(None, (s.strip() for s in value.split(","))):
                check_column("columns", name)
    if tag.name == "al-text":
        for name in dict.fromkeys(re.findall(r"\{([a-z][a-z0-9_]*)\}", tag.text)):
            check_column("text", name)

    # Parameters: the tag's own, then the page's, must be ones the view takes, with values it accepts.
    for attribute in ("params", "compare"):
        for value in _expand(tag.attrs.get(attribute, ""), page) if tag.attrs.get(attribute) else []:
            try:
                view.bind(dict(parse_qsl(value, keep_blank_values=True)))
            except ViewError as e:
                if "needs parameter" not in str(e):
                    problem(tag, f"{attribute}: {e}")
    own = {k for value in (tag.attrs.get("params"), tag.attrs.get("compare")) if value for k, _ in parse_qsl(value)}
    for name, param in view.params.items():
        if param.required and name not in own and name not in page:
            problem(tag, f"view {view.name} needs parameter {name}: set it in params=\"{name}=...\" or with a <al-param name=\"{name}\">")
