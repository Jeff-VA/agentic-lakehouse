"""A rollup on the real historical tier, written to a throwaway gold table."""

from dataclasses import replace
from uuid import uuid4

import pytest
from compare import assert_same_result

from agentic_lakehouse.compiler import SqlCompiler
from agentic_lakehouse.orchestrator import flushed_through, iceberg_catalog
from agentic_lakehouse.quality import QualityConfig
from agentic_lakehouse.rollups import materialize

pytestmark = pytest.mark.integration


def test_rollup_matches_the_query_it_materializes(platform, engines, window):
    table = f"test.rollup_{uuid4().hex[:8]}"
    rollup = platform.rollup("gold.hourly_isp_quality").model_copy(update={"table": table})
    checks = [{"type": "row_count", "min": 1}, {"type": "unique", "columns": ["event_ts", "isp_provider"]}]
    scratch = replace(platform, quality=QualityConfig.model_validate({"version": "1", "rules": [{"table": table, "checks": checks}]}))
    catalog = iceberg_catalog(platform)
    try:
        run = materialize(
            rollup,
            scratch,
            engine=engines["trino"],
            source_table=platform.table_for("telemetry", "trino"),
            catalog=catalog,
            watermark=flushed_through(platform, "telemetry"),
            log=lambda message: None,
        )
        gold = engines["trino"].execute(f"SELECT * FROM iceberg.{table} ORDER BY event_ts, isp_provider").to_pylist()
    finally:
        catalog.purge_table(table)

    assert all(result.passed for result in run.checks)
    query = rollup.with_window(run.start, run.end)
    expected = engines["duckdb"].execute(SqlCompiler(platform.ontology).compile(query, engine="duckdb", table="telemetry"))
    assert_same_result(platform.ontology, engines["duckdb"], query, expected.to_pylist(), gold)
