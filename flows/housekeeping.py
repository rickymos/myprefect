"""Local Prefect housekeeping flows."""
from __future__ import annotations

from prefect import flow

from config.envs import PREFECT
from flows.shared.subprocess_task import make_cli_task


_cleanup_old_logs_task = make_cli_task(
    venv_bin=PREFECT["venv_bin"],
    cwd=PREFECT["cwd"],
    task_name="prefect-cleanup-old-logs",
    retries=0,
    timeout_seconds=7200,
    cmd=["python", "tools/cleanup_old_logs.py", "--apply"],
)

_cleanup_temp_files_task = make_cli_task(
    venv_bin=PREFECT["venv_bin"],
    cwd=PREFECT["cwd"],
    task_name="prefect-cleanup-temp-files",
    retries=0,
    timeout_seconds=1800,
    cmd=["python", "tools/cleanup_temp_files.py", "--apply", "--older-than-hours", "24"],
)


@flow(name="prefect-housekeeping", log_prints=True)
def prefect_housekeeping_flow() -> None:
    """Prune old Prefect logs using the local retention policy."""
    _cleanup_old_logs_task()


@flow(name="prefect-temp-cleanup", log_prints=True)
def prefect_temp_cleanup_flow() -> None:
    """Prune stale project-owned temporary files using an allow-listed policy."""
    _cleanup_temp_files_task()
