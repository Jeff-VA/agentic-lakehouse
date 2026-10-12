# Working in this repository

Notes for coding agents, and for people who work the same way. Claude Code
reads this through `CLAUDE.md`.

## Where things are

- `agentic_lakehouse/` is the engine. It knows nothing about any one project.
- `config/` is the project: the YAML config (`ontology.yaml`,
  `ingestion.yaml`, `storage.yaml`, `quality_rules.yaml`, `deployment.yaml`),
  `views/*.sql` and `dashboards/*.html`.
- `examples/playback/` is the reference project. The tests run against it, so
  changing `config/` never breaks them.

## Building a dashboard

Dashboards are files: views in `config/views/`, pages in `config/dashboards/`,
reviewed like code. Start the stack first
(`docker compose -f docker/docker-compose.yml up -d`). The MCP tools named
below come from `agentic-lakehouse mcp`, which `.mcp.json` registers for Claude
Code; each has a command-line twin.

1. **Find the data.** `config/ontology.yaml` defines the metrics and
   dimensions; `list_metrics` lists them with what they mean. `list_views`
   shows the views that exist, with their parameters, columns and SQL. Reuse
   a view when one fits.
2. **Write a view** for each question: `config/views/<name>.sql`, a YAML
   comment, then one SELECT (format and rules: `agentic_lakehouse/views.py`, and the
   examples in `examples/playback/config/views/`).
   - Reading a model (`FROM playback_telemetry`), select metrics and
     dimensions by name. Never re-derive a metric in SQL: if one is missing,
     add it to `ontology.yaml`. Group by time with
     `date_trunc('hour', event_ts)` and bound the time dimension with `>=` and
     `<`. These views are live to the second.
   - Reading tables (`FROM gold.hourly_isp_quality`), write any Trino SELECT.
     Prefer gold rollups for long ranges.
   - Give the view a `description` a stranger could act on, and parameters
     (`:hours`) instead of hard-coded windows.
   - `query_view` runs it, so you can read the rows before drawing them.
3. **Write the page** in plain HTML with the chart tags listed at the top of
   `agentic_lakehouse/static/al.js`. Names are enough: labels and number formats come
   from the ontology, so set `label`, `format`, `unit` and `better` there
   rather than in the page. Copy the layout of
   `examples/playback/config/dashboards/playback.html`.
4. **Check** after every edit: `agentic-lakehouse check` (`check_dashboards`)
   must report 0 problems. It catches unknown views, columns, parameters and
   tags, with the file and line.
5. **Look**: `agentic-lakehouse screenshot <name>` (`screenshot_dashboard`)
   renders the page with live data and lists what it shows as wrong. Read the
   image critically: empty charts, crowded labels, a title that doesn't say
   what the chart shows. A person watching `http://127.0.0.1:8000/d/<name>`
   sees every saved edit, because open pages reload.

The MCP prompt `build_dashboard` hands an agent this workflow with the tag
reference.

## Changing the engine

- `pytest` runs the unit tests without Docker. They include browser tests of
  the example dashboard when Chrome is installed.
- `pytest -m integration` runs against the stack.
- Changes to the config's YAML need the platform restarted. Changes to views
  and dashboards don't.
