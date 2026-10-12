# @@TITLE@@

A project on [agentic-lakehouse](https://github.com/Jeff-VA/agentic-lakehouse)
(@@VERSION@@): events arrive on Kafka, land in Pinot (live) and Iceberg (all
history), and are served as governed metrics to dashboards and AI agents.

## Run it

You need Docker with about 12 GB of memory for its VM, and Python 3.11+.

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt pytest
docker compose -f docker/docker-compose.yml up -d --build
```

Then open http://127.0.0.1:8000 for the dashboards, `/docs` for the API, and
http://127.0.0.1:4200 for the pipelines. Setup publishes 24 hours of synthetic
events, and a stream keeps them coming until your producers write to Kafka.

## Make it yours

1. Describe your events in `config/ingestion.yaml` and what they mean in
   `config/ontology.yaml`.
2. Write views in `config/views/` and dashboards in `config/dashboards/`.
   With Claude Code, open this folder and ask for a dashboard: `.mcp.json` and
   `AGENTS.md` give it the tools and the workflow.
3. Check everything with `.venv/bin/agentic-lakehouse check` and `.venv/bin/pytest`.

The engine's README documents every endpoint, the view format and the chart tags.
