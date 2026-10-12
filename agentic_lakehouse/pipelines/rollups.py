"""Prefect flow that refreshes every rollup in storage.yaml (agentic_lakehouse/rollups.py).

Each rollup is its own task, so one failed quality gate doesn't hold up the
others; the flow fails if any rollup did. Each task attaches its check results
to the run as a table in the Prefect UI.
"""

from dataclasses import asdict

from prefect import flow, get_run_logger, task
from prefect.artifacts import create_table_artifact

from agentic_lakehouse.config import load_platform
from agentic_lakehouse.engines import TrinoEngine
from agentic_lakehouse.orchestrator import flushed_through, iceberg_catalog
from agentic_lakehouse.quality import CheckResult
from agentic_lakehouse.rollups import QualityGateError, materialize


@task
def refresh_rollup(config_dir: str, table: str) -> dict:
    platform = load_platform(config_dir)
    rollup = platform.rollup(table)
    source = platform.rollup_model(rollup).source
    logger = get_run_logger()
    try:
        run = materialize(
            rollup,
            platform,
            engine=TrinoEngine(platform.deployment.trino),
            source_table=platform.table_for(source, "trino"),
            catalog=iceberg_catalog(platform),
            watermark=flushed_through(platform, source),
            log=logger.info,
        )
    except QualityGateError as e:
        _report(table, e.results, published=False)
        raise
    if run.skipped:
        logger.info("rollup %s: skipped, %s", table, run.skipped)
    if run.checks:
        _report(table, run.checks, published=True)
    for check in run.checks:
        if not check.passed:  # failed warn checks; failed error checks raised above
            logger.warning("rollup %s: %s %s: %s", table, check.severity, check.check, check.detail)
    return asdict(run)


def _report(table: str, results: list[CheckResult], *, published: bool) -> None:
    create_table_artifact(
        key="quality-" + table.replace(".", "-").replace("_", "-"),
        table=[asdict(r) for r in results],
        description=f"Quality checks for {table}: {'published' if published else 'blocked, nothing published'}",
    )


@flow(name="rollups")
def refresh_rollups(config_dir: str = "config") -> list[dict]:
    """Refresh each rollup's gold table from the historical tier."""
    platform = load_platform(config_dir)
    states = [refresh_rollup(config_dir, rollup.table, return_state=True) for rollup in platform.storage.rollups]
    if failed := [state for state in states if state.is_failed()]:
        raise RuntimeError(f"{len(failed)} of {len(states)} rollups failed; see their task runs")
    return [state.result() for state in states]
