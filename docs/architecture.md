# Architecture

How the platform works, end to end: how events come in, how pipelines and
models build on them, how a question is answered, how a SQL view becomes a
live dashboard, and which storage tier answers which time range. Every part is driven by the files in `config/`; the
engine in `agentic_lakehouse/` knows nothing about any one project.

## End to end

![Producers and live feeds send events to Kafka. Pinot ingests them live and keeps 7 days; the flush appends them to Iceberg every 5 minutes, exactly once. Rollups and pipeline steps read Iceberg and write quality-gated gold tables back; the drift agent proposes config patches for new columns. The gateway reads the config, answers recent ranges from Pinot and history from Iceberg through Trino, and serves dashboards over SSE, AI agents over MCP and apps over REST.](images/architecture/overview.svg)

Events land in two tiers. **Pinot** ingests each event within seconds of its
arrival on Kafka and keeps the last 7 days. **Iceberg**, on MinIO and read
through **Trino**, keeps all of history; a flush appends new events to it every
5 minutes. Events come from your producers, from **live feeds** the platform
reads itself, or from **processors** that compute one dataset from another as
it streams. Background jobs work from Iceberg: **rollups** and **pipeline
steps** turn history into gold tables (including models) that must pass
quality checks before they're published, and the **drift agent** turns new
event fields into config patches for a person to review.

Every question, from a dashboard, an app or an AI agent, goes through one
**gateway**. It compiles the question from the definitions in the config,
sends each part of the time range to the tier that holds it, and says what the
answer covers. The [README](../README.md#endpoints) documents each endpoint.

## Data in

![Events reach Kafka from your producers, or from live feeds that ingest reads. Processors read one dataset's topic and publish another dataset. Pinot ingests every topic live; a flush appends events to Iceberg every 5 minutes, exactly once, and records how far Iceberg is complete by event time. From Iceberg, rollups build gold tables and steps write tables in SQL or Python, both behind quality gates; the drift agent proposes config patches for new columns.](images/architecture/data-in.svg)

- **Kafka** gets one JSON message per event, on a topic per dataset
  (`ingestion.yaml`). Pinot consumes the topic directly.
- **Live feeds** ([`sources.py`](../agentic_lakehouse/sources.py)): a dataset
  with a `source` is read by `agentic-lakehouse ingest` from a Server-Sent
  Events stream, through your transform. After a restart it resumes after the
  last event it published, so a feed that keeps history (Wikimedia's does)
  replays what was missed.
- **Processors** ([`processors.py`](../agentic_lakehouse/processors.py)): a
  dataset with a `processor` is computed by your code from another dataset's
  stream, by `agentic-lakehouse process`. After a restart it replays a warm-up
  window to rebuild its state, without publishing anything twice.
- **The flush** ([`agentic_lakehouse/flush.py`](../agentic_lakehouse/flush.py), a Prefect flow) reads
  every message published before it started and appends the events to the
  dataset's Iceberg table. The same Iceberg commit records the Kafka offsets it
  read up to and the new **watermark**: the time before which Iceberg holds
  every event, the newest event flushed minus the allowed lateness. It follows
  event time, not the clock, so while a feed catches up after an outage, hours
  aren't declared complete before their events arrive. A crash can't load a
  batch twice or skip one. A field the config doesn't know becomes a new
  optional column.
- **Rollups** ([`agentic_lakehouse/rollups.py`](../agentic_lakehouse/rollups.py)) recompute a window of
  a semantic query, run the checks in `quality_rules.yaml` on the new rows, and
  publish them as a gold table only if no `error` check failed
  (write-audit-publish).
- **Steps** ([`steps.py`](../agentic_lakehouse/steps.py)) in
  `pipelines.yaml` write tables with SQL (run on Trino) or Python (handed SQL
  over the lakehouse), in the order their reads require, behind the same
  quality checks. A step waits while a table it reads doesn't exist yet.
- **The drift agent** ([`agentic_lakehouse/agent_sync.py`](../agentic_lakehouse/agent_sync.py)) compares
  the Iceberg schema with the config, profiles new columns, and asks Claude
  which deserve dimensions or metrics. The proposal must pass the platform's
  validation and run on sampled rows before it's written to `proposals/` as a
  git patch.

## Pipelines and models

![In wiki-pulse, a rollup counts each article's edits per hour, and a SQL step turns those counts into baselines. The burst detector, a stream processor, reads every edit, loads the baselines and the news classifier's weights every 15 minutes, asks Claude about fresh bursts, and publishes the bursts as a dataset. A Python step trains the classifier on the bursts Claude labeled. Every model is a gold table.](images/architecture/pipelines.svg)

A model is a table. A step fits it from other tables and publishes it behind
quality checks; a processor loads it to score a stream as events arrive; and
the dataset the processor publishes can train the next version. There is no
model store or feature pipeline apart from the lakehouse: training, scoring
and dashboards read the same versioned tables. The diagram shows
[wiki-pulse](https://github.com/Jeff-VA/wiki-pulse), a project on this engine:

- **A rollup and a SQL step** compute what's usual: each wiki's typical
  edits per article and hour, over the last week.
- **The burst detector** (a processor) keeps each article's last 30 minutes
  of edits and flags those far above usual, seconds after the edit that tips
  them over. It asks Claude about the freshest, within a budget.
- **A Python step** retrains a logistic regression hourly on Claude's labels.
  Its gate requires it to beat guessing the commoner label, and the detector
  applies it to every burst, including the ones Claude never reads.

## A question, answered

![A question from a dashboard, an agent or the API goes through the gateway to the compiler, which turns metric names from ontology.yaml into SQL. The router sends recent ranges to Pinot and older ones to Trino; DuckDB merges a split. The rows return with the time range they cover and notes.](images/architecture/question.svg)

- **The compiler** ([`agentic_lakehouse/compiler.py`](../agentic_lakehouse/compiler.py)) turns a
  question in metric names into SQL for one engine. Metrics are defined once in
  `ontology.yaml`; each engine's dialect lives in a Jinja macro file under
  [`agentic_lakehouse/templates/macros/`](../agentic_lakehouse/templates/macros/).
- **The router** ([`agentic_lakehouse/router.py`](../agentic_lakehouse/router.py)) picks the tier, or
  splits the range across both, using Pinot's retention and Iceberg's
  watermark.
- **DuckDB** merges a split: buckets from each tier, or partial aggregates (an
  average travels as a sum and a count).
- Every answer carries the time range it actually covers and notes on anything
  it left out, so neither a person nor an agent mistakes a partial answer for a
  complete one.

## Which tier answers

![Iceberg holds everything up to the last flush; Pinot holds the last 7 days, so the two overlap. A last-hour query goes to Pinot, a query for August to Trino, and a last-30-days query is split at the last flush and merged.](images/architecture/tiers.svg)

| The question's time range | Answered by |
|---|---|
| inside Pinot's 7 days | Pinot |
| ends before the last flush | Trino over Iceberg |
| spans both, with a time grain | split at a bucket boundary, so every bucket comes from one tier and every metric is exact |
| spans both, counts, sums, averages or ratios only | partial aggregates from each tier, merged in DuckDB |
| spans both, with percentiles or distinct counts | Trino up to the last flush, with a note that the newest data isn't included |

## From a view to a live page

![Pinot and Trino answer a feed that the gateway keeps for each view and set of parameters. The feed pushes new results to every dashboard page watching it over Server-Sent Events. You or an agent edit the view and dashboard files, which define the feeds; a check and a screenshot report problems back to the agent.](images/architecture/live-page.svg)

- **A view** ([`agentic_lakehouse/views.py`](../agentic_lakehouse/views.py)) is one SELECT in
  `config/views/<name>.sql`. Over an ontology model it names metrics and
  dimensions, becomes a semantic query, and is routed across both tiers, so it
  includes events from seconds ago. Over Iceberg tables (a gold rollup, say)
  it's plain Trino SQL. Views are checked against the config when it loads.
  A **composed** view puts semantic queries and table queries in a `WITH`
  clause, then runs any SQL over their answers in a sandboxed DuckDB, for
  example to compare two time windows or join a model with a gold table.
- **A feed** ([`agentic_lakehouse/live.py`](../agentic_lakehouse/live.py)) keeps one view with one set
  of parameters fresh while anyone watches, and pushes a new result only when
  it changed. A view over a model reruns every 10 seconds; a view over tables
  reruns when one of its tables gets a new Iceberg snapshot. A hundred open
  pages cost one query per change.
- **A dashboard** is an HTML file in `config/dashboards/` written with the
  chart tags in [`agentic_lakehouse/static/al.js`](../agentic_lakehouse/static/al.js). The page
  watches all its views over one Server-Sent Events connection, and labels and
  number formats come from the ontology.
- **The authoring loop**: you or an agent edit the files;
  [`agentic_lakehouse/dashboards.py`](../agentic_lakehouse/dashboards.py) checks every tag against the
  views it names, and [`agentic_lakehouse/browser.py`](../agentic_lakehouse/browser.py) renders the
  page in headless Chrome with live data. Open pages reload when a file is
  saved; an edit that breaks a view keeps the last good version and says what
  broke.

## SQL access

People who work in SQL have two ways in. Both work from DBeaver, the DuckDB
CLI or a notebook.

| Way in | What it reads | Fresh to |
|---|---|---|
| `lake.<namespace>.<table>`: DuckDB attaches the Iceberg catalog | raw and gold tables, any SQL | the last flush or rollup run |
| `gateway_view(name, params)`, `lakehouse_sql('…')`: DuckDB reads the gateway's answers as Parquet | views, and SQL written like one, composed views included | the second, for anything that reads a model |

`agentic-lakehouse duckdb` writes the setup from the project's config. The
gateway does the cutting and merging in both cases, so a DuckDB user gets
the same numbers as a dashboard. The design is in
[design/sql-access.md](design/sql-access.md).

## What you edit, what it drives

| Your project: `config/` | Drives |
|---|---|
| `ingestion.yaml` | Kafka topics, Pinot and Iceberg tables, live feeds, processors, synthetic data |
| `ontology.yaml` | every query: metrics and dimensions, with their labels and formats |
| `storage.yaml` | the flush schedule and the rollups |
| `pipelines.yaml` | steps: SQL or Python that writes tables, models among them |
| `quality_rules.yaml` | the checks a rollup or step must pass before it publishes |
| `views/*.sql` | named queries for dashboards and agents |
| `dashboards/*.html` | pages of live chart tags, at `/d/<name>` |
| `deployment.yaml` | where each engine runs |

| The engine: `agentic_lakehouse/` | Where |
|---|---|
| Compiler | `compiler.py`, `templates/` |
| Router | `router.py` |
| Flush | `flush.py` |
| Rollups and quality gates | `rollups.py`, `quality.py` |
| Drift agent | `agent_sync.py` |
| Gateway: REST, MCP, dashboards | `api_gateway.py` |
| Views | `views.py` |
| Live push | `live.py` |
| Chart tags | `static/al.js` |
| Dashboard checks and screenshots | `dashboards.py`, `browser.py` |
| Setup and Kafka publishing | `orchestrator.py` |
| Live feeds, processors, pipeline steps | `sources.py`, `processors.py`, `steps.py` |
| A new project (`agentic-lakehouse init`) | `scaffold.py`, `project_template/` |
| Views and dashboards without Docker, for tests | `testing.py` |

One `docker compose -f docker/docker-compose.yml up -d --build` runs all of
it: the engines (Kafka, Pinot, Trino, the Iceberg REST catalog, MinIO,
Prefect) and the app's processes (setup, stream or ingest, processors, flush,
rollups, steps, drift agent, gateway). The diagrams are SVG files in [`images/architecture/`](images/architecture/),
and they follow your light or dark theme.
