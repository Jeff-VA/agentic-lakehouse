# Architecture

How the platform works, end to end: how events come in, how a question is
answered, how a SQL view becomes a live dashboard, and which storage tier
answers which time range. Every part is driven by the files in `config/`; the
engine in `agentic_lakehouse/` knows nothing about any one project.

## End to end

![Producers send events to Kafka. Pinot ingests them live and keeps 7 days; the flush appends them to Iceberg every 5 minutes, exactly once. Rollups read Iceberg and write quality-gated gold tables back; the drift agent proposes config patches for new columns. The gateway reads the config, answers recent ranges from Pinot and history from Iceberg through Trino, and serves dashboards over SSE, AI agents over MCP and apps over REST.](images/architecture/overview.svg)

Events land in two tiers. **Pinot** ingests each event within seconds of its
arrival on Kafka and keeps the last 7 days. **Iceberg**, on MinIO and read
through **Trino**, keeps all of history; a flush appends new events to it every
5 minutes. Two background jobs work from Iceberg: **rollups** turn history into
gold tables that must pass quality checks before they're published, and the
**drift agent** turns new event fields into config patches for a person to
review.

Every question, from a dashboard, an app or an AI agent, goes through one
**gateway**. It compiles the question from the definitions in the config,
sends each part of the time range to the tier that holds it, and says what the
answer covers. The [README](../README.md#endpoints) documents each endpoint.

## Data in

![Events flow from producers into Kafka. Pinot ingests them live; a flush job appends them to Iceberg every 5 minutes. From Iceberg, rollups build quality-gated gold tables and the drift agent proposes config patches for new columns.](images/architecture/data-in.svg)

- **Kafka** gets one JSON message per event, on a topic per dataset
  (`ingestion.yaml`). Pinot consumes the topic directly.
- **The flush** ([`agentic_lakehouse/flush.py`](../agentic_lakehouse/flush.py), a Prefect flow) reads
  every message published before it started and appends the events to the
  dataset's Iceberg table. The same Iceberg commit records the Kafka offsets it
  read up to and the new **watermark**: the time before which Iceberg holds
  every event. A crash can't load a batch twice or skip one. A field the config
  doesn't know becomes a new optional column.
- **Rollups** ([`agentic_lakehouse/rollups.py`](../agentic_lakehouse/rollups.py)) recompute a window of
  a semantic query, run the checks in `quality_rules.yaml` on the new rows, and
  publish them as a gold table only if no `error` check failed
  (write-audit-publish).
- **The drift agent** ([`agentic_lakehouse/agent_sync.py`](../agentic_lakehouse/agent_sync.py)) compares
  the Iceberg schema with the config, profiles new columns, and asks Claude
  which deserve dimensions or metrics. The proposal must pass the platform's
  validation and run on sampled rows before it's written to `proposals/` as a
  git patch.

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

## What you edit, what it drives

| Your project: `config/` | Drives |
|---|---|
| `ingestion.yaml` | Kafka topics, Pinot and Iceberg tables, synthetic data |
| `ontology.yaml` | every query: metrics and dimensions, with their labels and formats |
| `storage.yaml` | the flush schedule and the rollups |
| `quality_rules.yaml` | the checks a rollup must pass before it publishes |
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
| A new project (`agentic-lakehouse init`) | `scaffold.py`, `project_template/` |
| Views and dashboards without Docker, for tests | `testing.py` |

One `docker compose -f docker/docker-compose.yml up -d --build` runs all of
it: the engines (Kafka, Pinot, Trino, the Iceberg REST catalog, MinIO,
Prefect) and the app's processes (setup, stream, flush, rollups, drift agent,
gateway). The diagrams are SVG files in [`images/architecture/`](images/architecture/),
and they follow your light or dark theme.
