"""Pinot stores and aggregates missing fields as NULL, like Iceberg, on a throwaway topic and table."""

import time
from datetime import datetime
from uuid import uuid4

import httpx
import pytest
from confluent_kafka.admin import AdminClient, NewTopic

from core.engines import PinotEngine
from core.events import generate_events
from core.orchestrator import publish_events, sync_pinot_schema
from core.schema_compiler import pinot_table_config

pytestmark = pytest.mark.integration


def test_a_missing_field_is_null_in_pinot_aggregates(platform):
    name = f"nulls_{uuid4().hex[:8]}"
    telemetry = platform.dataset("telemetry")
    dataset = telemetry.model_copy(
        update={"topic": name.replace("_", "-"), "realtime": telemetry.realtime.model_copy(update={"table": name})}
    )
    controller = platform.deployment.pinot.controller_url
    kafka = AdminClient({"bootstrap.servers": platform.deployment.kafka.bootstrap_servers})
    [created] = kafka.create_topics([NewTopic(dataset.topic, num_partitions=1, replication_factor=1)]).values()
    created.result(timeout=30)
    try:
        sync_pinot_schema(controller, dataset, log=lambda m: None)
        config = pinot_table_config(dataset, platform.deployment.kafka.internal_bootstrap_servers)
        httpx.post(f"{controller}/tables", json=config).raise_for_status()
        events = generate_events(datetime(2026, 1, 1), datetime(2026, 1, 1, 0, 1), seed=5)[:2]
        events[0]["bitrate_kbps"], events[1]["bitrate_kbps"] = 3000, None  # the second event lacks it
        events[1].pop("bitrate_kbps")
        publish_events(platform, dataset, events, log=lambda m: None)

        engine = PinotEngine(platform.deployment.pinot.broker_url)
        deadline = time.monotonic() + 60
        while True:
            try:
                [row] = engine.execute(
                    f'SELECT COUNT(*) AS n, COUNT("bitrate_kbps") AS with_bitrate, AVG("bitrate_kbps") AS avg_bitrate FROM "{name}"'
                ).to_pylist()
                if row["n"] == 2:
                    break
            except Exception:  # the table may not be queryable yet
                pass
            assert time.monotonic() < deadline, "Pinot didn't ingest the events"
            time.sleep(1)
    finally:
        httpx.delete(f"{controller}/tables/{name}", params={"type": "realtime"})
        httpx.delete(f"{controller}/schemas/{name}")
        kafka.delete_topics([dataset.topic])
    assert row == {"n": 2, "with_bitrate": 1, "avg_bitrate": 3000.0}  # not (3000 + 0) / 2
