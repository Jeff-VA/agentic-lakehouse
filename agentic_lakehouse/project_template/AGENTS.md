# Working in this project

Notes for coding agents, and for people who work the same way. Claude Code
reads this through `CLAUDE.md`.

This project runs on [agentic-lakehouse](https://github.com/Jeff-VA/agentic-lakehouse),
pinned in `requirements.txt`. The engine is a dependency: when something
needs to change in it, change it there and upgrade the pin, not here.

## Where things are

- `config/ingestion.yaml`: the datasets that arrive on Kafka, and their columns.
- `config/ontology.yaml`: what the data means: dimensions and metrics, with
  their labels and formats.
- `config/storage.yaml` and `config/quality_rules.yaml`: the Iceberg tables,
  the rollups, and the checks a rollup must pass before it publishes.
- `config/views/*.sql`: named queries that dashboards and agents read.
- `config/dashboards/*.html`: pages of live chart tags, at `/d/<name>`.
- Python modules next to `config/`: the project's own code, such as a
  dataset's `simulator: module:function`.

## Building a dashboard

Start the stack (`docker compose -f docker/docker-compose.yml up -d --build`).
The MCP tools below come from `agentic-lakehouse mcp`, which `.mcp.json`
registers for Claude Code; each has a command-line twin.

1. **Find the data.** `list_metrics` lists the metrics and dimensions with
   what they mean; `list_views` shows the views that exist, with their
   parameters, columns and SQL. Reuse a view when one fits.
2. **Write a view** for each question: `config/views/<name>.sql`, a YAML
   comment, then one SELECT.
   - Reading a model (`FROM activity`), select metrics and dimensions by
     name; never re-derive a metric in SQL (add it to `ontology.yaml`). Group
     by time with `date_trunc('hour', event_ts)` and bound the time dimension
     with `>=` and `<`. These views are live to the second.
   - Reading tables (`FROM gold.hourly_activity`), write any Trino SELECT.
   - Give it a `description` a stranger could act on, and parameters
     (`:hours`) instead of hard-coded windows. `query_view` runs it.
3. **Write the page** in plain HTML with the chart tags listed at the top of
   http://127.0.0.1:8000/al.js (the `build_dashboard` MCP prompt includes
   them). Names are enough: labels and formats come from the ontology. Copy
   the layout of `config/dashboards/overview.html`.
4. **Check** after every edit: `agentic-lakehouse check` (`check_dashboards`)
   must report 0 problems.
5. **Look**: `agentic-lakehouse screenshot <name>` (`screenshot_dashboard`)
   renders the page with live data and lists what it shows as wrong. Read the
   image critically. Open pages reload on every save.

## Tests

`pytest` runs `tests/`, which use `agentic_lakehouse.testing`: every view and
dashboard on DuckDB stand-ins for the stack, with no Docker needed. The
dashboard test needs Chrome and skips without it.
