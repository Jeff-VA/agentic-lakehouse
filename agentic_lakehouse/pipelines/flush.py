"""Prefect flow that flushes every dataset's new Kafka events into Iceberg.

One task per dataset, retried on failure; runs, logs and failures show in the
Prefect UI (http://127.0.0.1:4200 with the Docker stack). The work itself is
agentic_lakehouse/flush.py. Run it with `agentic-lakehouse flush` (once) or
`agentic-lakehouse flush --serve` (on each table's flush_every_minutes schedule).
"""

from dataclasses import asdict
from datetime import UTC, datetime, timedelta

from prefect import flow, get_run_logger, task

from agentic_lakehouse.config import load_platform
from agentic_lakehouse.flush import flush, kafka_consumer
from agentic_lakehouse.orchestrator import iceberg_catalog


@task(retries=2, retry_delay_seconds=10)
def flush_dataset(config_dir: str, dataset_name: str) -> dict:
    platform = load_platform(config_dir)
    dataset = platform.dataset(dataset_name)
    settings = platform.iceberg_table(dataset_name)
    table = iceberg_catalog(platform).load_table(settings.table)
    consumer = kafka_consumer(platform.deployment.kafka.bootstrap_servers)
    try:
        result = flush(
            table,
            consumer,
            dataset,
            now=datetime.now(UTC).replace(tzinfo=None),
            lateness=timedelta(minutes=settings.allowed_lateness_minutes),
        )
    finally:
        consumer.close()
    logger = get_run_logger()
    logger.info(
        "%s: flushed %d events into %s, complete through %s UTC",
        dataset_name, result.events, settings.table, f"{result.watermark:%Y-%m-%d %H:%M:%S}",
    )
    if result.new_columns:
        logger.warning("%s: new event fields added as Iceberg columns: %s", dataset_name, result.new_columns)
    return asdict(result)


@flow(name="flush-to-iceberg")
def flush_all(config_dir: str = "config") -> list[dict]:
    """Flush each dataset in ingestion.yaml from its Kafka topic into its Iceberg table."""
    platform = load_platform(config_dir)
    return [flush_dataset(config_dir, dataset.name) for dataset in platform.ingestion.datasets]
