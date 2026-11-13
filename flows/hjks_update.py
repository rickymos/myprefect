"""Daily HJKS plant/unit and outage snapshot import."""
from __future__ import annotations

from prefect import flow

from config.envs import JAPANMARKET
from config.limits import MAX_RUN_SECONDS, TASK_TIMEOUT_SECONDS
from flows.shared.run_names import descriptive_run_name
from flows.shared.subprocess_task import make_cli_task


def _build_hjks_command(params: dict) -> list[str]:
    command = ["python", "-m", "japanmarket.hjks.cli", "update"]
    if params.get("force"):
        command.append("--force")
    return command


_hjks_task = make_cli_task(
    venv_bin=JAPANMARKET["venv_bin"], cwd=JAPANMARKET["cwd"],
    task_name="hjks-update", cmd_builder=_build_hjks_command,
    retries=0, timeout_seconds=TASK_TIMEOUT_SECONDS, skip_if_running_key="hjks-update",
)


@flow(name="hjks-update", flow_run_name=descriptive_run_name, log_prints=True,
      timeout_seconds=MAX_RUN_SECONDS)
def hjks_update_flow(force: bool = False) -> None:
    """Download the complete HJKS snapshots; content hashes make unchanged runs cheap."""
    _hjks_task({"force": force})
