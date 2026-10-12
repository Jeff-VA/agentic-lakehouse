"""The agentic-lakehouse command: each subcommand reaches the code it runs."""

import sys

import yaml

from agentic_lakehouse import cli
from agentic_lakehouse.scaffold import create_project


def test_ingest_reads_the_feed_of_the_dataset_with_a_source(tmp_path, monkeypatch):
    create_project(tmp_path / "feed")
    config = tmp_path / "feed" / "config"
    ingestion = yaml.safe_load((config / "ingestion.yaml").read_text())
    ingestion["datasets"][0]["source"] = {"kind": "sse", "url": "https://example.org/stream"}
    (config / "ingestion.yaml").write_text(yaml.safe_dump(ingestion))
    calls = []
    monkeypatch.setattr(cli, "ingest", lambda dataset, publisher, resume_after: calls.append((dataset.name, publisher, resume_after)))
    monkeypatch.setattr(cli, "EventPublisher", lambda platform, dataset: "publisher")
    monkeypatch.setattr(cli, "last_header", lambda platform, dataset, header: "event 41")
    monkeypatch.setattr(sys, "argv", ["agentic-lakehouse", "--config", str(config), "ingest"])
    cli.main()
    assert calls == [("events", "publisher", "event 41")]  # resumes after the last event it published
