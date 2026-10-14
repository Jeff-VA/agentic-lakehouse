# Design: project macros

Status: shipped in v0.5.0, 2026-10-14.
Tracking: the checklist at the end; update it as work lands.

## The problem

A project owns its ontology (`config/ontology.yaml`). The drift agent
proposes changes to it as new fields arrive, so the ontology already evolves
with the data. The SQL templates are another matter: they live only in the
engine (`agentic_lakehouse/templates/`). So a project can't:

- **name a business definition once and reuse it.** "A human edit" is
  `is_bot = false AND is_anonymous = false`. It appears in several metric
  filters, dimensions and views, written out each time.
- **use engine-specific SQL in its definitions.** A URL's host is
  `url_extract_host(url)` on Trino and a regular expression on Pinot and
  DuckDB. Ontology snippets are parsed once in DuckDB's dialect and translated
  by sqlglot, which knows no project-specific functions.
- **change how the engine compiles a construct for its data.** For example,
  an exact percentile on Trino, or an approximate distinct count on Pinot.

Templates and ontology are meant to evolve together with the business
context of each data source. Projects need macros of their own.

## Decisions (with the user, 2026-10-14)

- Projects can **add** macros and **override** the engine's built-ins.
- Design doc first, then build, test, release, and adopt in wiki-pulse.

## Design

### Where macros live

The project's `config/macros/` folder mirrors the engine's
`templates/macros/`:

| File | Holds |
|---|---|
| `default.sql` | macros for every engine |
| `pinot.sql`, `trino.sql`, `duckdb.sql` | engine-specific versions, optional |

These are ordinary Jinja macro files, the same format as the engine's. The
comment directly above a macro, with no blank line between, is its description,
shown to people and agents (a file's header comment, set off by a blank line,
describes no macro):

```sql
{# An edit by a person with an account: not a bot, not anonymous. #}
{% macro human_edit() %}is_bot = false AND is_anonymous = false{% endmacro %}

{# The host of a URL, e.g. en.wikipedia.org. #}
{% macro url_host(column) %}regexp_extract({{ column }}, '^https?://([^/]+)', 1){% endmacro %}
```

and in `config/macros/trino.sql`:

```sql
{% macro url_host(column) %}url_extract_host({{ column }}){% endmacro %}
```

### Lookup order

For each engine, a macro name is looked up in this order:

1. the project's `<engine>.sql`;
2. the engine's `macros/<engine>.sql`;
3. the project's `default.sql`;
4. the engine's `macros/default.sql`.

**An engine-specific macro always beats a generic one. At the same level,
the project's beats the engine's.** So:

- a project can override a built-in for one engine (put it in that engine's
  file) or for all engines (put it in `default.sql`);
- a generic project override never silently replaces a built-in that is
  engine-specific. Pinot's percentile stays Pinot's unless the project writes
  `pinot.sql`.

Macros call each other as `adapter.<name>`, as engine macros do today, with
the same lookup.

### Where macros can be used

- **The engine's own templates.** `query.sql` and `merge.sql` call macros
  through the adapter, so a project override of `percentile`, `count_distinct`,
  `time_bucket`, `ratio`, `quote`, `timestamp` or `limit` applies to every
  query compiled for that engine.
- **Ontology SQL snippets:** a dimension's `sql`, a metric's `sql` and
  `filters`. For example, `filters: ["{{ human_edit() }}"]`.
  - A snippet without `{{` or `{%` behaves exactly as today: parsed in
    DuckDB's dialect and translated by sqlglot.
  - A snippet with Jinja is **rendered for each engine** with that engine's
    lookup, then parsed and printed in that engine's dialect, so
    engine-specific macros reach each engine. The text around the macro calls
    must be portable SQL. Simple expressions are.
  - Values still never enter a template as raw text: rendered snippets are
    parsed by sqlglot before use, as today.
- **Views** (`config/views/*.sql`): the SQL after the header is a Jinja
  template, rendered with the **Trino** lookup (views are written in Trino's
  dialect) before it's parsed. A composed view's stage 2 is translated to
  DuckDB from the rendered SQL, as today.
- **Steps** (`pipelines.yaml`, `sql:`): rendered with the Trino lookup when
  the config loads. Steps run on Trino, and the rendered SQL is what's parsed,
  typed and run.
- **Not yet:** quality-rule `expression` checks. They run in DuckDB; a later
  release can render them with the DuckDB lookup.

### Validation, when the config loads (`load_platform`)

- **Macro files** must be valid Jinja: a syntax error names the file and line.
- **Every templated ontology snippet** is rendered for all three engines and
  parsed in each dialect. An unknown macro, a missing argument, or SQL that
  doesn't parse fails with the place: `ontology.yaml: model edits, metric
  human_edits, filters[0], for pinot: …`. The columns a snippet reads (for
  the dataset check) come from its DuckDB rendering.
- **Overrides of built-ins are proven by compiling every model for every
  engine.** That's a sample query with all its metrics, all its dimensions, an
  hourly grain and a filter, plus the partial queries and merge SQL used when
  a range spans the tiers.
  - The DuckDB SQL is run against an empty table with the dataset's columns,
    which catches binder errors.
  - Pinot and Trino SQL are parsed; there's no engine to run them on at load
    time.
- **Views and steps** are rendered first, then checked exactly as today.

`agentic-lakehouse check` and the tests surface all of this. Open dashboards
and the live gateway reload when a macro file changes, as they do for views.

### Discoverability, for people and agents

- **The catalog lists the macros.** `GET /catalog`, and MCP's
  `list_metrics`, gain a `macros` list: name, arguments, description, and the
  engines with their own version.
- **Agents are told about macros.** The MCP dashboard guide says views can
  call them. The drift agent's prompt lists them, so a proposed metric can
  reuse a business definition, for example `filters: ["{{ human_edit() }}"]`.
  Proposals are validated with the project's macros, through `load_platform`.
- **New projects start with the folder.** `agentic-lakehouse init` writes
  `config/macros/default.sql` with a commented example. The template's
  README and AGENTS.md explain the four files and the lookup order.

### API inside the engine

- **`agentic_lakehouse/macros.py`** (new) holds `ProjectMacros`:
  - `ProjectMacros(directory | None)` loads the project's macro files. None
    means the engine's built-ins only.
  - `.adapter(engine)` returns the dispatch object templates use, with the
    four-level lookup.
  - `.render(text, engine)` renders a snippet or SQL text with that engine's
    lookup.
  - `.templated(text)` says whether text has Jinja in it.
  - `.catalog()` returns `[MacroInfo(name, args, description, engines)]`.
- **The ontology carries its macros.** `Ontology` gets a private `_macros`
  attribute, set by `load_platform`, and a `macros` property (default: no
  project macros). `SqlCompiler(ontology)` reads it, so none of the seven
  call sites change.
- **`parse_fragment(sql, read=FRAGMENT_DIALECT)`** gains the read dialect.
- **The compiler gets `SqlCompiler._fragment(sql, engine) -> exp.Expression`.**
  It is used for dimension trees, metric expressions, metric filters and
  query filters on dimensions with `sql`.

## Implementation plan

| | Workstream | Owner | Files |
|---|---|---|---|
| WS0 | `macros.py` (loading, four-level adapter, render, catalog); `Ontology.macros`; the compiler's adapter built from the ontology's macros; `parse_fragment(read=)`; `load_platform` loads `config/macros/` | lead, on `main`, first | `macros.py`, `ontology.py`, `compiler.py`, `config.py` |
| WS1 | Templated ontology snippets: per-engine rendering in the compiler; skip pydantic-time parsing for templated snippets; load-time validation of snippets and the sample-compile proof of overrides; `source_columns` through the DuckDB rendering; tests | lead | `compiler.py`, `ontology.py`, `config.py`, `tests/test_project_macros.py` |
| WS2 | Views and steps: render view SQL (Trino lookup) in `parse_view`; render steps' SQL at load; reload on macro file changes; `macros` in `/catalog` and MCP `list_metrics`; the MCP guide; tests | subagent, worktree | `views.py` (`parse_view` only), `config.py` (step loading only), `api_gateway.py` (catalog, file watcher, guide), `tests/test_macros_in_views.py` |
| WS3 | The drift agent's prompt lists macros; `init` scaffolds `config/macros/default.sql`; template README and AGENTS.md; the engine README's "Macros" section; tests | subagent, worktree | `agent_sync.py`, `scaffold.py`, `project_template/**`, README "Macros", `tests/test_macros_scaffold.py` |
| WS4 | Merge; end-to-end (the stand-in gateway with project macros and an override; views, steps and live); `docs/architecture.md` and the architecture page; v0.5.0; wiki-pulse adoption: `human_edit()`, `url_host()` with a Trino version, metrics and dimensions using them; verified on the live stack | lead | various |

Order: WS0 lands first. WS1, WS2 and WS3 then run in parallel against its
API, and WS4 merges them.

## Testing

- **Macros module:**
  - the four-level lookup, including a generic project macro that doesn't
    replace an engine-specific built-in;
  - Jinja errors with file and line;
  - the catalog, with descriptions and engines.
- **Compiler:**
  - a templated filter, dimension and metric expression compiles per
    engine, with the engine-specific macro on its engine;
  - plain snippets compile exactly as before, against the golden SQL files in
    `tests/golden/`;
  - a built-in override, such as an exact Trino percentile, appears in Trino
    SQL only.
- **Validation:**
  - an unknown macro, a bad argument, SQL that doesn't parse for one engine,
    and an override that breaks DuckDB execution each fail with their
    location;
  - the columns a templated snippet reads are checked against the dataset.
- **Views and steps:**
  - a table view, a model view's filter, a composed view and a SQL step
    calling macros;
  - a macro file edit reloads open pages.
- **End to end:** the stand-in gateway answers views over a project with
  macros, and the DuckDB merge works with a project override.
- **Live (WS4):** wiki-pulse's new metrics on the real tiers.

## Open questions and risks

- **Portability of snippet text around macro calls.** A templated snippet is
  parsed in each engine's dialect, so DuckDB-only syntax around a macro call
  fails for Trino or Pinot at load. That's caught early, and the message says
  to move engine-specific SQL into a macro.
- **Built-in overrides can break routing assumptions.** Pinot's `timestamp`
  must stay epoch milliseconds, for example. The sample-compile proof catches
  syntax and DuckDB errors, not wrong semantics, so the docs list what each
  built-in must keep true.
- **Jinja in view and step SQL** makes `{{` and `{%` special there. That's
  rare in SQL; `{% raw %}` escapes it.

## What implementation found

- **Rendering is sandboxed.** Jinja could otherwise reach Python's
  internals: `{{ adapter.__init__.__globals__ }}` ran. Project files are
  trusted, but the drift agent's proposals are written by a model and
  rendered when they're checked. So every macro rendering uses Jinja's
  `SandboxedEnvironment`, and any failure while rendering becomes a
  `MacroError` that the config reports.
- **Ad-hoc SQL can't call macros.** `/sql` and MCP `query_sql` refuse
  `{{ … }}`; saving the SQL as a view lets it use them. Clients reach the
  gateway without authentication, and even sandboxed Jinja can exhaust
  memory.
- **Macro files are read once, at load.** A file broken on disk changes
  nothing until the next reload, and the live reload keeps the last good
  macros when an edit is broken.
- **A description is the comment directly above a macro**, with no blank line
  between. A file's header comment describes no macro.
- **The drift agent lists a project's own macros.** It leaves out
  overrides of built-ins, and macros that aren't defined for every engine.
  Its validation copies `config/macros/`.
- **Trino has no exact percentile aggregate**, so the README's override
  example is an exact percentile on Pinot.

## Checklist

- [x] WS0: `macros.py`, `Ontology.macros`, the compiler's adapter from the ontology, `parse_fragment(read=)`, loading
- [x] WS1: templated ontology snippets, load-time validation, the override proof, tests
- [x] WS2: macros in views and steps, reload, catalog and MCP, tests
- [x] WS3: drift agent context, `init` scaffold, docs, tests
- [x] WS4: merge, end-to-end, architecture docs, v0.5.0 release
- [x] WS4: wiki-pulse adoption, verified on the live stack (2026-10-14): a query of human_share by site since Oct 12 ran Trino's url_extract_host on Iceberg and default.sql's regex on Pinot, merged; /catalog lists the project's macros
