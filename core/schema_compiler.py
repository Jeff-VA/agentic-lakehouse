"""Derive every engine's table definition from one dataset in ingestion.yaml.

The raw schema is written once; Pinot, Iceberg and Arrow (the in-memory format
used to write Iceberg and to query with DuckDB) all get their definitions from
it, so the tiers can't drift apart by hand-editing.
"""

import pyarrow as pa
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import DayTransform, HourTransform, MonthTransform
from pyiceberg.types import (
    BooleanType,
    DoubleType,
    IcebergType,
    IntegerType,
    LongType,
    NestedField,
    StringType,
    TimestampType,
)

from core.config import Dataset, IcebergTable

# Timestamps are UTC by convention and stored without a zone everywhere, so
# every engine compares them to the same plain UTC literals.
_PINOT_TYPES = {
    "string": "STRING",
    "int": "INT",
    "long": "LONG",
    "double": "DOUBLE",
    "boolean": "BOOLEAN",
    "timestamp": "TIMESTAMP",
}
_ICEBERG_TYPES = {
    "string": StringType,
    "int": IntegerType,
    "long": LongType,
    "double": DoubleType,
    "boolean": BooleanType,
    "timestamp": TimestampType,
}
_ARROW_TYPES = {
    "string": pa.string(),
    "int": pa.int32(),
    "long": pa.int64(),
    "double": pa.float64(),
    "boolean": pa.bool_(),
    "timestamp": pa.timestamp("us"),
}
_PARTITION_TRANSFORMS = {"hour": HourTransform, "day": DayTransform, "month": MonthTransform}

PINOT_KAFKA_CONSUMER = "org.apache.pinot.plugin.stream.kafka30.KafkaConsumerFactory"
PINOT_JSON_DECODER = "org.apache.pinot.plugin.inputformat.json.JSONMessageDecoder"


def pinot_schema(dataset: Dataset) -> dict:
    """Pinot schema. Numbers become metric fields, timestamps date-time fields, the rest dimensions."""
    dimensions, metrics, date_times = [], [], []
    for column in dataset.columns:
        spec = {"name": column.name, "dataType": _PINOT_TYPES[column.type]}
        if column.type == "timestamp":
            date_times.append({**spec, "format": "1:MILLISECONDS:EPOCH", "granularity": "1:MILLISECONDS"})
        elif column.type in ("int", "long", "double"):
            metrics.append(spec)
        else:
            dimensions.append(spec)
    return {
        "schemaName": dataset.realtime.table,
        "dimensionFieldSpecs": dimensions,
        "metricFieldSpecs": metrics,
        "dateTimeFieldSpecs": date_times,
    }


def pinot_table_config(dataset: Dataset, kafka_bootstrap_servers: str) -> dict:
    """Pinot REALTIME table that consumes the dataset's topic. Messages are JSON
    objects keyed by column name, with timestamps in epoch milliseconds."""
    return {
        "tableName": dataset.realtime.table,
        "tableType": "REALTIME",
        "segmentsConfig": {
            "schemaName": dataset.realtime.table,
            "timeColumnName": dataset.time_column,
            "replication": "1",
            "retentionTimeUnit": "DAYS",
            "retentionTimeValue": str(dataset.realtime.retention_days),
        },
        "tenants": {},
        "tableIndexConfig": {"loadMode": "MMAP"},
        "ingestionConfig": {
            "streamIngestionConfig": {
                "streamConfigMaps": [
                    {
                        "streamType": "kafka",
                        "stream.kafka.topic.name": dataset.topic,
                        "stream.kafka.broker.list": kafka_bootstrap_servers,
                        "stream.kafka.consumer.type": "lowlevel",
                        "stream.kafka.consumer.factory.class.name": PINOT_KAFKA_CONSUMER,
                        "stream.kafka.decoder.class.name": PINOT_JSON_DECODER,
                        "stream.kafka.consumer.prop.auto.offset.reset": "smallest",
                        # Seal a consuming segment at ~100 MB or after 6 hours, whichever comes first.
                        "realtime.segment.flush.threshold.rows": "0",
                        "realtime.segment.flush.threshold.segment.size": "100M",
                        "realtime.segment.flush.threshold.time": "6h",
                    }
                ]
            }
        },
        "metadata": {},
    }


def iceberg_type(column_type: str) -> IcebergType:
    return _ICEBERG_TYPES[column_type]()


def iceberg_schema(dataset: Dataset) -> Schema:
    # Optional fields: a dataset gaining a column must not break older files.
    return Schema(
        *(
            NestedField(field_id=i, name=c.name, field_type=iceberg_type(c.type), required=False)
            for i, c in enumerate(dataset.columns, start=1)
        )
    )


def iceberg_partition_spec(dataset: Dataset, table: IcebergTable) -> PartitionSpec:
    source = iceberg_schema(dataset).find_field(dataset.time_column)
    return PartitionSpec(
        PartitionField(
            source_id=source.field_id,
            field_id=1000,  # Iceberg numbers partition fields from 1000
            transform=_PARTITION_TRANSFORMS[table.partition_by](),
            name=f"{dataset.time_column}_{table.partition_by}",
        )
    )


def arrow_schema(dataset: Dataset) -> pa.Schema:
    return pa.schema([pa.field(c.name, _ARROW_TYPES[c.type]) for c in dataset.columns])
