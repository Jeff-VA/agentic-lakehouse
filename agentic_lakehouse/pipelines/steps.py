"""Prefect flow that runs the steps in pipelines.yaml (agentic_lakehouse/steps.py).

Each step is its own task, run in the order their reads require, so the
Prefect UI shows the pipeline as it ran. A step whose quality gate blocks, or
that fails, doesn't stop the others; the flow fails if any did.
"""

from dataclasses import asdict

from prefect import flow, get_run_logger, task

from agentic_lakehouse.config import load_platform
from agentic_lakehouse.engines import TrinoEngine
from agentic_lakehouse.orchestrator import iceberg_catalog
from agentic_lakehouse.pipelines.rollups import _report
from agentic_lakehouse.rollups import QualityGateError
from agentic_lakehouse.steps import IcebergStepStore, run_step
from agentic_lakehouse.views import qualify_tables


@task
def run_one_step(config_dir: str, name: str) -> dict:
    platform = load_platform(config_dir)
    step = platform.step(name)
    trino = TrinoEngine(platform.deployment.trino)
    logger = get_run_logger()
    try:
        run = run_step(
            platform,
            step,
            query=lambda sql: trino.execute(qualify_tables(sql, platform.deployment.trino.catalog)),
            store=IcebergStepStore(iceberg_catalog(platform)),
            log=logger.info,
        )
    except QualityGateError as e:
        _report(step.output, e.results, published=False)
        raise
    if run.skipped:
        logger.info("step %s: skipped, %s", name, run.skipped)
    elif run.checks:
        _report(step.output, run.checks, published=True)
    for check in run.checks:
        if not check.passed:  # failed warn checks; failed error checks raised above
            logger.warning("step %s: %s %s: %s", name, check.severity, check.check, check.detail)
    return asdict(run)


@flow(name="steps")
def run_steps(config_dir: str = "config") -> list[dict]:
    """Run each due step, in the order their reads require."""
    platform = load_platform(config_dir)
    states = [run_one_step(config_dir, step.name, return_state=True) for step in platform.ordered_steps()]
    if failed := [state for state in states if state.is_failed()]:
        raise RuntimeError(f"{len(failed)} of {len(states)} steps failed; see their task runs")
    return [state.result() for state in states]
