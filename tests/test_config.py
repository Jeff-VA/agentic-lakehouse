import shutil
from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

from core.config import ConfigError, load_platform

SAMPLE_CONFIG = Path(__file__).parents[1] / "config"


@pytest.fixture
def config_dir(tmp_path):
    """A copy of the sample config that a test can break."""
    for path in SAMPLE_CONFIG.glob("*.yaml"):
        shutil.copy(path, tmp_path)
    return tmp_path


def edit(path: Path, change) -> None:
    data = yaml.safe_load(path.read_text())
    change(data)
    path.write_text(yaml.safe_dump(data))


def test_sample_config_loads():
    platform = load_platform(SAMPLE_CONFIG)
    assert platform.table_for("telemetry", "trino") == "iceberg.raw.telemetry"
    assert platform.table_for("telemetry", "pinot") == "telemetry"
    with pytest.raises(ValueError, match="holds no datasets"):
        platform.table_for("telemetry", "duckdb")


def test_model_reading_a_column_its_dataset_lacks_is_rejected(config_dir):
    edit(config_dir / "ingestion.yaml", lambda d: d["datasets"][0]["columns"].pop())  # bitrate_kbps
    with pytest.raises(ConfigError, match="dataset 'telemetry' doesn't have: bitrate_kbps"):
        load_platform(config_dir)


def test_model_with_unknown_source_is_rejected(config_dir):
    edit(config_dir / "ontology.yaml", lambda d: d["models"][0].update(source="nowhere"))
    with pytest.raises(ConfigError, match="reads unknown dataset 'nowhere'"):
        load_platform(config_dir)


def test_storage_and_ingestion_must_cover_the_same_datasets(config_dir):
    edit(config_dir / "storage.yaml", lambda d: d["tables"][0].update(dataset="other"))
    with pytest.raises(ConfigError, match="unknown dataset 'other'") as error:
        load_platform(config_dir)
    assert "dataset 'telemetry' has no Iceberg table" in str(error.value)


def test_time_column_must_be_a_timestamp(config_dir):
    edit(config_dir / "ingestion.yaml", lambda d: d["datasets"][0].update(time_column="session_id"))
    with pytest.raises(ValidationError, match="time_column 'session_id' must be a timestamp column"):
        load_platform(config_dir)


def test_rollup_queries_are_checked_against_the_ontology(config_dir):
    edit(config_dir / "storage.yaml", lambda d: d["rollups"][0]["query"].update(metrics=["p99_ttfbb"]))
    with pytest.raises(ConfigError, match="rollup 'gold.hourly_isp_quality': unknown metrics: p99_ttfbb"):
        load_platform(config_dir)


def test_quality_rules_must_name_a_rollup(config_dir):
    edit(config_dir / "quality_rules.yaml", lambda d: d["rules"][0].update(table="gold.nowhere"))
    with pytest.raises(ConfigError, match="quality rules for 'gold.nowhere', which isn't a rollup"):
        load_platform(config_dir)


def test_quality_checks_must_read_the_rollups_columns(config_dir):
    edit(config_dir / "quality_rules.yaml", lambda d: d["rules"][0]["checks"].append({"type": "not_null", "columns": ["client_isp"]}))
    with pytest.raises(ConfigError, match="not_null on 'gold.hourly_isp_quality' reads columns the rollup doesn't have: client_isp"):
        load_platform(config_dir)


def test_a_rollup_cannot_reuse_a_table_name(config_dir):
    edit(config_dir / "storage.yaml", lambda d: d["rollups"][0].update(table="raw.telemetry"))
    with pytest.raises(ValidationError, match="tables defined more than once: raw.telemetry"):
        load_platform(config_dir)
