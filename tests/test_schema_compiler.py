from pathlib import Path

import pyarrow as pa

from agentic_lakehouse.config import load_platform
from agentic_lakehouse.schema_compiler import arrow_schema, iceberg_partition_spec, iceberg_schema, pinot_schema, pinot_table_config

PLATFORM = load_platform(Path(__file__).parents[1] / "examples" / "playback" / "config")
DATASET = PLATFORM.dataset("telemetry")


def test_pinot_schema_assigns_columns_by_type():
    schema = pinot_schema(DATASET)
    assert [f["name"] for f in schema["dateTimeFieldSpecs"]] == ["event_ts"]
    assert [f["name"] for f in schema["metricFieldSpecs"]] == ["request_latency_ms", "buffer_duration_ms", "bitrate_kbps"]
    assert {f["name"]: f["dataType"] for f in schema["dimensionFieldSpecs"]} == {
        "session_id": "STRING",
        "client_isp": "STRING",
        "client_device": "STRING",
        "stream_start_event": "BOOLEAN",
    }


def test_pinot_stores_missing_fields_as_null():
    assert pinot_schema(DATASET)["enableColumnBasedNullHandling"] is True


def test_pinot_table_consumes_the_dataset_topic():
    config = pinot_table_config(DATASET, "kafka:19092")
    [stream] = config["ingestionConfig"]["streamIngestionConfig"]["streamConfigMaps"]
    assert stream["stream.kafka.topic.name"] == "playback-telemetry"
    assert stream["stream.kafka.broker.list"] == "kafka:19092"
    assert config["segmentsConfig"]["timeColumnName"] == "event_ts"
    assert config["segmentsConfig"]["retentionTimeValue"] == "7"


def test_iceberg_schema_has_every_column_optional():
    schema = iceberg_schema(DATASET)
    assert [f.name for f in schema.fields] == [c.name for c in DATASET.columns]
    assert not any(f.required for f in schema.fields)
    assert str(schema.find_field("event_ts").field_type) == "timestamp"


def test_iceberg_table_is_partitioned_on_the_time_column():
    [field] = iceberg_partition_spec(DATASET, PLATFORM.iceberg_table("telemetry")).fields
    assert (field.source_id, str(field.transform), field.name) == (1, "day", "event_ts_day")


def test_arrow_schema_types():
    schema = arrow_schema(DATASET)
    assert schema.field("event_ts").type == pa.timestamp("us")
    assert schema.field("request_latency_ms").type == pa.int32()
    assert schema.field("stream_start_event").type == pa.bool_()
