"""Bootstraps the platform's infrastructure from config and loads data into it.

setup() is idempotent: it creates whatever is missing (Kafka topics, Pinot
schemas and tables, Iceberg namespaces and tables) and leaves the rest alone.
"""

import json
from collections.abc import Callable
from datetime import datetime, timedelta

import httpx
import pyarrow as pa
from confluent_kafka import KafkaError, KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, TableAlreadyExistsError

from core.config import Dataset, Platform
from core.schema_compiler import arrow_schema, iceberg_partition_spec, iceberg_schema, pinot_schema, pinot_table_config

Log = Callable[[str], None]

_EPOCH = datetime(1970, 1, 1)


def iceberg_catalog(platform: Platform) -> Catalog:
    s3 = platform.deployment.s3
    return load_catalog(
        "lakehouse",
        type="rest",
        uri=platform.deployment.iceberg.catalog_uri,
        **{
            "s3.endpoint": s3.endpoint,
            "s3.region": s3.region,
            "s3.access-key-id": s3.access_key,
            "s3.secret-access-key": s3.secret_key,
        },
    )


def setup(platform: Platform, log: Log = print) -> None:
    """Create every dataset's Kafka topic, Pinot table and Iceberg table if missing."""
    kafka = AdminClient({"bootstrap.servers": platform.deployment.kafka.bootstrap_servers})
    catalog = iceberg_catalog(platform)
    for dataset in platform.ingestion.datasets:
        _create_topic(kafka, dataset, log)
        _create_pinot_table(platform, dataset, log)
        _create_iceberg_table(platform, catalog, dataset, log)


def _create_topic(kafka: AdminClient, dataset: Dataset, log: Log) -> None:
    [future] = kafka.create_topics([NewTopic(dataset.topic, num_partitions=1, replication_factor=1)]).values()
    try:
        future.result()
        log(f"kafka: created topic {dataset.topic}")
    except KafkaException as e:
        if e.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
            raise
        log(f"kafka: topic {dataset.topic} exists")


def _create_pinot_table(platform: Platform, dataset: Dataset, log: Log) -> None:
    controller = platform.deployment.pinot.controller_url
    table = dataset.realtime.table
    if httpx.get(f"{controller}/tables/{table}").json().get("REALTIME"):
        log(f"pinot: table {table} exists")
        return
    httpx.post(f"{controller}/schemas", json=pinot_schema(dataset)).raise_for_status()
    config = pinot_table_config(dataset, platform.deployment.kafka.internal_bootstrap_servers)
    httpx.post(f"{controller}/tables", json=config).raise_for_status()
    log(f"pinot: created table {table}, consuming topic {dataset.topic}")


def _create_iceberg_table(platform: Platform, catalog: Catalog, dataset: Dataset, log: Log) -> None:
    table = platform.iceberg_table(dataset.name)
    try:
        catalog.create_namespace(table.namespace)
    except NamespaceAlreadyExistsError:
        pass
    try:
        catalog.create_table(
            table.table, schema=iceberg_schema(dataset), partition_spec=iceberg_partition_spec(dataset, table)
        )
        log(f"iceberg: created table {table.table}, partitioned by {table.partition_by}")
    except TableAlreadyExistsError:
        log(f"iceberg: table {table.table} exists")


def teardown(platform: Platform, log: Log = print) -> None:
    """Delete every dataset's Kafka topic, Pinot table and schema, and Iceberg table, with their data."""
    kafka = AdminClient({"bootstrap.servers": platform.deployment.kafka.bootstrap_servers})
    catalog = iceberg_catalog(platform)
    controller = platform.deployment.pinot.controller_url
    for dataset in platform.ingestion.datasets:
        table = dataset.realtime.table
        if httpx.get(f"{controller}/tables/{table}").json().get("REALTIME"):
            httpx.delete(f"{controller}/tables/{table}", params={"type": "realtime"}).raise_for_status()
            log(f"pinot: deleted table {table}")
        if httpx.get(f"{controller}/schemas/{table}").status_code == 200:
            httpx.delete(f"{controller}/schemas/{table}").raise_for_status()
        if dataset.topic in kafka.list_topics(timeout=10).topics:
            [future] = kafka.delete_topics([dataset.topic]).values()
            future.result()
            log(f"kafka: deleted topic {dataset.topic}")
        iceberg_table = platform.iceberg_table(dataset.name).table
        if catalog.table_exists(iceberg_table):
            catalog.purge_table(iceberg_table)
            log(f"iceberg: dropped table {iceberg_table}")


def load_events(platform: Platform, dataset: Dataset, events: list[dict], log: Log = print) -> None:
    """Append events to the dataset's Iceberg table and publish the same events
    to its Kafka topic (Pinot ingests them), so both tiers hold the batch.

    Iceberg goes first: it checks the events against the table schema, so a bad
    batch fails before anything reaches Kafka.
    """
    table = iceberg_catalog(platform).load_table(platform.iceberg_table(dataset.name).table)
    table.append(pa.Table.from_pylist(events, schema=arrow_schema(dataset)))
    log(f"iceberg: appended {len(events)} events to {table.name()}")

    producer = Producer({"bootstrap.servers": platform.deployment.kafka.bootstrap_servers, "linger.ms": 50})
    for event in events:
        message = {**event, dataset.time_column: (event[dataset.time_column] - _EPOCH) // timedelta(milliseconds=1)}
        while True:
            try:
                producer.produce(dataset.topic, json.dumps(message).encode())
                break
            except BufferError:  # local queue full: let deliveries drain, then retry
                producer.poll(0.5)
        producer.poll(0)
    if undelivered := producer.flush(30):
        raise RuntimeError(f"kafka: {undelivered} events not delivered to {dataset.topic}")
    log(f"kafka: published {len(events)} events to {dataset.topic}")
