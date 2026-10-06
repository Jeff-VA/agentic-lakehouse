"""`setup` on the real Pinot controller, using a throwaway schema."""

from uuid import uuid4

import httpx
import pytest

from core.orchestrator import sync_pinot_schema

pytestmark = pytest.mark.integration


def test_setup_adds_new_columns_to_an_existing_pinot_schema(platform):
    dataset = platform.dataset("telemetry")
    name = f"sync_test_{uuid4().hex[:8]}"
    dataset = dataset.model_copy(update={"realtime": dataset.realtime.model_copy(update={"table": name})})
    before = dataset.model_copy(update={"columns": dataset.columns[:-1]})  # without bitrate_kbps
    controller = platform.deployment.pinot.controller_url
    logs = []
    try:
        sync_pinot_schema(controller, before, log=logs.append)
        sync_pinot_schema(controller, dataset, log=logs.append)
        schema = httpx.get(f"{controller}/schemas/{name}").json()
    finally:
        httpx.delete(f"{controller}/schemas/{name}")
    assert "bitrate_kbps" in {f["name"] for f in schema["metricFieldSpecs"]}
    assert logs == [f"pinot: added bitrate_kbps to schema {name}"]
