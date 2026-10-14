# Design: SQL access to live and historical data

Status: approved for implementation, 2026-10-14. Target release: v0.4.0.
Tracking: the checklist at the end; update it as work lands.

## The problem

The gateway is the one place where live data (Pinot) and history (Iceberg,
through Trino) are combined correctly. It cuts each question's time range
at the flush watermark, asks each tier for its side, and merges the parts.
Clients use that today through dashboards, `GET /views/<name>`,
`POST /query` and MCP.

People who work in SQL can't reach the combined data, for three reasons:

1. **SQL tools see one tier.** DBeaver on Trino sees Iceberg only, minutes
   behind. Pinot's console sees the last days only.
2. **The gateway answers fixed questions.** It runs only the shapes its
   views define. A view over a model accepts metrics × dimensions × a time
   range, because that's the shape the router knows how to cut without
   duplicates or gaps. Joins, rankings, comparisons between windows and
   window functions can't be expressed.
3. **SQL clients can't reach the gateway well.** A project-level DuckDB macro
   over `GET /views/<name>` works (wiki-pulse's `tools/duckdb/setup.sql`), but:
   - the gateway doesn't answer DuckDB's `HEAD` probe, so sessions need
     `force_download`;
   - JSON loses the columns of an empty result;
   - errors reach DuckDB as a bare "HTTP 400";
   - the macro isn't part of the engine.

## Goals

- **Two-stage views.** A view may compose semantic queries and then use any
  SQL on their merged results. Every client gets this: dashboards, live
  feeds, MCP and the REST API.
- **Ad-hoc SQL.** The same SQL can be sent without saving a view file:
  `GET`/`POST /sql`, and an MCP tool for agents.
- **DuckDB clients are first-class.** The engine generates the DuckDB setup
  for a project: macros for views and ad-hoc SQL, plus direct Iceberg access.
  It works in DBeaver, the DuckDB CLI and notebooks.
- **Correctness is unchanged.** Metrics are still cut across tiers only by
  the router, using the ontology's definitions. Everything else happens
  after the merge, on deduplicated rows.

## Non-goals

- **Arbitrary SQL pushed down across tiers**, such as raw-row queries spanning
  Pinot and Iceberg, or joins evaluated inside each tier. Moving raw rows is
  the cost the two tiers exist to avoid. Raw history stays available through
  direct Iceberg access.
- **Authentication.** It remains future work. See Security.
- **A Postgres wire-protocol endpoint.**

## Design

### 1. Two-stage (composed) views

A view file, or ad-hoc SQL, may be a `WITH` query whose CTEs are of three
kinds:

| CTE reads | Kind | Runs where |
|---|---|---|
| exactly one ontology model (`FROM edits`) | **model part**, stage 1 | the router: Pinot, Trino or both, merged. The same rules as a view over a model today |
| `namespace.table` names only | **table part**, stage 1 | Trino, as written, as table views run today |
| other CTE names only | stage 2 | DuckDB, in memory |

The final `SELECT` reads CTE names only, so it is stage 2. For example:

```sql
/*
description: Wikis with more reverts than usual in the last `hours`.
params:
  hours: {type: int, default: 1, min: 1, max: 24}
columns:
  above_usual: {label: Above usual, format: percent, better: lower}
*/
WITH recent AS (
  SELECT wiki, revert_share FROM edits WHERE event_ts >= now() - INTERVAL :hours HOUR
),
usual AS (
  SELECT wiki, revert_share FROM edits WHERE event_ts >= now() - INTERVAL '7' DAY
)
SELECT recent.wiki, recent.revert_share, recent.revert_share - usual.revert_share AS above_usual
FROM recent JOIN usual USING (wiki)
ORDER BY above_usual DESC
LIMIT 10
```

**Classification, so existing views don't change meaning:**

- A plain `SELECT … FROM <model>` with no `WITH` is a model view, as today.
- Any query that reads only tables, including with CTEs, is a table view and
  runs on Trino as written, as today.
- A query that reads a model inside a `WITH` is a **composed** view.
- In a composed view, a CTE that mixes kinds (a model and a table, or a model
  and another CTE) is an error: "a CTE reads one model, or tables, or other
  CTEs, not a mix". A model part may not have its own `WITH`. A table part may.

**SQL dialect.** All view SQL stays in Trino's dialect, as it is today.
Stage 2 is transpiled to DuckDB by sqlglot.

**Validation, when views load:**

- Each model part is checked as a model view, by the existing
  `_model_source`.
- Each table part is checked as a table view, by the existing
  `_table_source`.
- Stage 2 must:
  - be a `SELECT`;
  - read only CTE names;
  - use no table functions (`read_csv`, `read_parquet`, `glob`, …);
  - avoid the statements views already forbid.
- **Stage 2 is then run on empty tables.** Create in-memory DuckDB tables
  with each stage-1 part's column names and types, and execute the
  transpiled stage-2 SQL against them. That catches unknown functions, type
  errors and bad references with the view file's name. The result's
  description gives the view's output columns and types.

**Output column metadata.**

- An output column named like a stage-1 column that holds a metric or
  dimension takes that field's label, format, unit and better.
- Any column can be described in the header's `columns:` (v0.3.2).
- Anything else gets words(name) and no format.

**Parameters.** `:name` placeholders may appear in any part. They are bound
as literals: through `ModelSource` for model parts, `TableSource.sql` for
table parts, and literal substitution in the stage-2 SQL.

**Execution, in `Gateway.run_view`:**

1. Run every stage-1 part:
   - a model part through `ModelSource.query` and `router.run`;
   - a table part through `warehouse.engine`, with the same missing-table
     handling as table views.

   Parts get an internal cap of `part_max_rows`, default 100,000. If a part
   exceeds it, that's an error naming the part, not a silent truncation:
   stage 2 over a cut-off part would be wrong.
2. Open a fresh in-memory DuckDB with the sandbox settings (see Security),
   register each part's Arrow table under its CTE name, and run stage 2.
3. Cap the final rows by the response format's limit. A truncated result is
   reported in `truncated` and a note, as today.

**Result.** `ViewResult` gains `parts: dict[str, PartInfo] | None`. A
`PartInfo` holds the part's kind (`"model"` or `"tables"`), its coverage or
data-through time, its notes, and its row count. At the top level:

- `coverage` spans the model parts: earliest start to latest end, or `None`
  if there are none.
- `data_through` is the minimum over the table parts.
- `notes` are each part's notes, prefixed by the part's name.

Dashboards keep working: `al.js` shows "Live" when `coverage` is set.

**Live feeds.**

- A composed view with any model part reruns every `refresh_seconds`, like a
  model view.
- With only table parts, it reruns when one of their tables changes, like a
  table view.
- Results are pushed only on change, as today.

**Catalog.** `ViewInfo.source` gains `"composed"`, and `reads` lists the
models and tables. `gateway_views()` reports a composed view with a model
part as "live + history".

### 2. Ad-hoc SQL: `/sql`

- **`POST /sql`** takes the JSON body `{"sql": "…", "params": {…}}`.
- **`GET /sql?q=<url-encoded SQL>&<params>`** exists because DuckDB, and
  other HTTP readers, can only `GET`.
- **Either SQL form is accepted.** A view header comment (description,
  params, columns) is optional. Without one, the description is "ad-hoc
  query", and `:name` placeholders are an error that says to add a header.
- **It runs like a view:** `parse_view("sql", text, …)` then
  `Gateway.run_view`, with the same rules, validation, limits and result.
  Model views, table views and composed views are all allowed.
- **MCP gets a `query_sql(sql)` tool for agents.** Its description says to
  prefer metric names over models and that stage 2 runs after the merge.

### 3. DuckDB-friendly HTTP

- **Formats.** `format=json|csv|parquet` on `GET /views/<name>` and `/sql`.
  JSON is the default and keeps today's `ViewResult` shape.
  - CSV always has a header row, so columns survive an empty result.
    Timestamps are ISO 8601 UTC.
  - Parquet uses the view's column types, so the schema survives an empty
    result.
- **Row caps.** JSON keeps `max_rows` (5,000). CSV and Parquet use
  `export_max_rows`, default 100,000.
- **Inline errors.** With `errors=inline`, for CSV and Parquet only, a client
  error is answered 200 with one row and one column, `error`. That covers a
  bad view, parameter or SQL, and an unknown view. DuckDB users then see the
  message as a result row instead of a bare "HTTP 400". JSON clients keep
  real status codes.
- **`HEAD`.** `HEAD` is answered on `/views`, `/views/<name>` and `/sql`:
  200 with the content type and no body, without running anything. To
  verify: if DuckDB still needs `Content-Length` and range support, the
  generated session keeps `SET force_download = true`, which works today.

### 4. DuckDB client setup, generated by the engine

`agentic-lakehouse duckdb [--gateway http://127.0.0.1:8000] [--out tools/duckdb]`
writes two files from the project's `deployment.yaml`.

`setup.sql`, run once per DuckDB database:

- `INSTALL iceberg; INSTALL httpfs;`
- a persistent S3 secret from `deployment.s3`;
- the macros:
  - `gateway_view(name, params)`: `read_parquet` of
    `{gateway}/views/{name}?format=parquet&errors=inline&{params}`;
  - `gateway_views()`: the catalog, saying which views are live + history;
  - `lakehouse_sql(sql)`: `read_parquet` of
    `{gateway}/sql?format=parquet&errors=inline&q={url_encode(sql)}`.

`session.sql`, run at the start of each session (DBeaver: bootstrap
queries):

- `SET force_download = true`, unless `HEAD` makes it unnecessary;
- `ATTACH IF NOT EXISTS … AS lake (TYPE iceberg, ENDPOINT {deployment.iceberg.catalog_uri})`
  for read-only, direct access to the raw and gold tables.

`agentic-lakehouse init` runs it for new projects, and the template's
README and AGENTS.md describe it. wiki-pulse replaces its hand-written files
with the generated ones.

### 5. Security

- **Stage 2 is sandboxed.** It runs in a fresh in-memory DuckDB per
  execution, with these settings:
  - `enable_external_access = false`: no files, network or extensions;
  - `lock_configuration = true`;
  - a `memory_limit` (default 256 MB);
  - `threads = 2`.

  It is also validated to read only CTE names. A watchdog interrupts it
  after `sql_timeout_seconds` (default 30).
- **Stage 1 can't write.** Model parts are compiled by the engine:
  identifiers are validated and values bound as literals. Table parts are
  checked by the existing `_analyze`, which forbids anything but reading.
- **`/sql` has no authentication**, like every endpoint today. It adds ad-hoc
  reads and compute to what port 8000 already serves. Follow-up, not in
  this release: bind the stack's published ports to 127.0.0.1 (verify with
  Colima's port forwarding) and design API authentication.

## Implementation plan

Workstreams, with file ownership chosen to keep merges clean:

| | Workstream | Owner | Files |
|---|---|---|---|
| WS0 | Refactor `Gateway.view(name)` into `Gateway.run_view(view, params, …)`, which `view` calls. Add `ViewResult.parts` and `PartInfo`, and `ViewInfo.source` "composed", so the other streams code against the final shapes | lead, on `main`, first | `api_gateway.py` |
| WS1 | Composed views: parsing, classification, validation on empty tables, typing, metadata; execution in `run_view`; the stage-2 sandbox; live refresh; catalog | lead | `views.py`, a new `compose.py` (the stage-2 sandbox and runner), `api_gateway.py` (`run_view` only), `live.py`, `tests/test_compose.py` |
| WS2 | HTTP I/O: `format=` (JSON, CSV, Parquet) and inline errors on `/views/<name>` and `/sql`; `HEAD`; `GET`/`POST /sql` via `parse_view` and `run_view`; the MCP `query_sql` tool; README endpoint docs | subagent, in a worktree | `api_gateway.py` (`create_app` routes and MCP only), a new `formats.py`, README "Endpoints", `tests/test_sql_endpoint.py`, `tests/test_formats.py` |
| WS3 | DuckDB client: a new `duckdb_client.py` that renders the setup and session SQL from `Platform` and the gateway URL; the `cli duckdb` command; `init` integration; template docs; the README's "SQL access" section; generation tests | subagent, in a worktree | `duckdb_client.py`, `cli.py`, `scaffold.py`, `project_template/**`, README "SQL access", `tests/test_duckdb_client.py` |
| WS4 | Integration: merge WS1–WS3; end-to-end tests (DuckDB macros against a served stand-in app, covering a composed view, `lakehouse_sql` and inline errors); `docs/architecture.md`; version 0.4.0; release; wiki-pulse adoption (generated DuckDB files, a composed view, a long-range daily view); verify against the live stack | lead | various |

Order: WS0 lands on `main` first. WS1, WS2 and WS3 then run in parallel.
WS2 and WS3 can be built against the WS0 interfaces: before WS1 lands,
`/sql` already works for single-source SQL. WS4 runs last.

## Testing

- **Views:**
  - classification, so existing views keep their kind;
  - composed parsing and every error message;
  - validation of stage 2 on empty tables;
  - output columns, types and metadata inheritance;
  - parameters in every part.
- **Execution, on the stand-in gateway:**
  - a composed view joining two model parts, and one joining a model part
    with a table part;
  - a part over its cap is an error;
  - parts' notes and coverage;
  - the sandbox refuses external access, even if validation were bypassed.
- **Live:** a composed view's feed reruns on its timer and pushes on change.
- **HTTP:**
  - each format, including empty results;
  - inline errors;
  - `HEAD`;
  - `/sql` `GET` and `POST`, with and without a header;
  - MCP `query_sql`.
- **DuckDB client:**
  - the generated SQL is deterministic and uses the project's deployment
    values;
  - end to end: a real DuckDB, using the generated macros against
    `serving(create_app(standin_gateway(…)))`, returns rows for
    `gateway_view`, a composed view and `lakehouse_sql`, and an error row for
    a bad parameter.
- **Live stack (WS4):** the wiki-pulse composed view and DuckDB in DBeaver
  against the real tiers.

## Open questions and risks

- **Does DuckDB need `Content-Length` and range reads** for `read_parquet`
  over HTTP after a `HEAD`? If so, the session keeps `force_download`.
- **Is `url_encode` available** in the DuckDB versions DBeaver ships? If
  not, `lakehouse_sql` takes a pre-encoded string, or `/sql` accepts
  base64url in `q64`.
- **sqlglot's Trino-to-DuckDB transpilation of stage 2** is covered by the
  empty-tables validation, which reports what doesn't translate.
- **Memory.** Parts up to 100,000 rows in the 512 MiB `api` container. The
  caps and the DuckDB `memory_limit` bound it. Revisit if real views need
  more.

## Checklist

- [ ] WS0: `run_view` refactor, `ViewResult.parts`, `ViewInfo.source` "composed"
- [ ] WS1: composed views (parse, validate, execute, sandbox, live, catalog) with tests
- [ ] WS2: formats, inline errors, `HEAD`, `/sql`, MCP `query_sql`, docs, tests
- [ ] WS3: `duckdb_client.py`, `agentic-lakehouse duckdb`, `init` integration, docs, tests
- [ ] WS4: merge, end-to-end tests, architecture docs, v0.4.0 release
- [ ] WS4: wiki-pulse adoption, verified on the live stack
