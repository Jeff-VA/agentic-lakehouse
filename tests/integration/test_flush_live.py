"""The flush on real Kafka and Iceberg, using a throwaway topic and table."""

from datetime import datetime, timedelta
from uuid import uuid4

import pytest
from confluent_kafka.admin import AdminClient, NewTopic

from core.engines import TrinoEngine
from core.events import generate_events
from core.flush import flush, kafka_consumer
from core.orchestrator import iceberg_catalog, publish_events
from core.schema_compiler import iceberg_schema

pytestmark = pytest.mark.integration


@pytest.fixture
def scratch(platform):
    """A copy of the telemetry dataset with its own Kafka topic and Iceberg table, deleted afterwards."""
    name = f"flush_test_{uuid4().hex[:8]}"
    dataset = platform.dataset("telemetry").model_copy(update={"name": name, "topic": name.replace("_", "-")})
    kafka = AdminClient({"bootstrap.servers": platform.deployment.kafka.bootstrap_servers})
    [created] = kafka.create_topics([NewTopic(dataset.topic, num_partitions=2, replication_factor=1)]).values()
    created.result()
    catalog = iceberg_catalog(platform)
    catalog.create_namespace_if_not_exists("test")
    table = catalog.create_table(f"test.{name}", schema=iceberg_schema(dataset))
    yield dataset, table
    [deleted] = kafka.delete_topics([dataset.topic]).values()
    deleted.result()
    catalog.purge_table(f"test.{name}")


def run_flush(platform, dataset, table):
    consumer = kafka_consumer(platform.deployment.kafka.bootstrap_servers)
    try:
        return flush(table, consumer, dataset, now=datetime(2026, 1, 2), lateness=timedelta(minutes=2), timeout=10)
    finally:
        consumer.close()


def test_flush_evolves_the_table_and_loads_each_event_once(platform, scratch):
    dataset, table = scratch
    events = generate_events(datetime(2026, 1, 1), datetime(2026, 1, 1, 0, 10))
    for event in events[::2]:  # upstream starts sending a new field on some events
        event["dropped_frames_count"] = 3
    publish_events(platform, dataset, events, log=lambda message: None)

    first = run_flush(platform, dataset, table)
    second = run_flush(platform, dataset, table)

    assert (first.events, first.new_columns) == (len(events), {"dropped_frames_count": "double"})
    assert second.events == 0
    [counts] = TrinoEngine(platform.deployment.trino).execute(
        f"SELECT count(*) AS n, count(dropped_frames_count) AS with_frames FROM iceberg.test.{dataset.name}"
    ).to_pylist()
    assert counts == {"n": len(events), "with_frames": len(events[::2])}
