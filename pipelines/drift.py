"""Prefect flow that runs the drift agent (core/agent_sync.py) over every dataset.

`python run.py drift` runs it once; `python run.py drift --serve` hourly. Claude
is only called for drift that has no pending proposal in proposals/.
"""

from prefect import flow, get_run_logger

from core.agent_sync import check_drift
from core.config import load_platform


@flow(name="drift-check")
def drift_check(config_dir: str = "config", out_dir: str = "proposals") -> list[str]:
    """Write a config patch to `out_dir` for each dataset whose events gained new fields."""
    logger = get_run_logger()
    return [str(path) for path in check_drift(load_platform(config_dir), config_dir, out_dir, log=logger.info)]
