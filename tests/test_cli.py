"""The agentic-lakehouse command: each subcommand reaches the code it runs."""

import sys
from datetime import timedelta

import yaml
from prefect.client.schemas.objects import ConcurrencyLimitStrategy

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


def test_scheduled_flows_run_one_at_a_time():
    """When a sleeping laptop wakes, Prefect would start every run it missed at once: three
    flushes from the same offsets. The flush survives that (tests/test_flush.py), but none of
    the extra runs has anything to do, so a run that comes due while one is going is cancelled."""
    calls = []

    class Flow:
        def __call__(self, config_dir):
            calls.append(("run", config_dir))

        def serve(self, **kwargs):
            calls.append(("serve", kwargs))

    cli.run_then_serve(Flow(), "flush", timedelta(minutes=5), "config")

    [run, (_, served)] = calls
    assert run == ("run", "config")  # first, straight away
    assert (served["name"], served["interval"]) == ("flush", timedelta(minutes=5))
    limit = served["global_limit"]
    assert (limit.limit, limit.collision_strategy) == (1, ConcurrencyLimitStrategy.CANCEL_NEW)
