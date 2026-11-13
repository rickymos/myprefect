"""Daily regional TSO supply/demand import for all nine mainland grid areas."""
from __future__ import annotations

from prefect import flow

from config.envs import JAPANMARKET
from config.limits import MAX_RUN_SECONDS, TASK_TIMEOUT_SECONDS
from flows.shared.run_names import descriptive_run_name
from flows.shared.subprocess_task import make_cli_task


def _build_tso_command(params: dict) -> list[str]:
    command = ["python", "-m", "japanmarket.tso_supply.cli", "update"]
    command += ["--months", str(int(params.get("months", 2)))]
    for area in params.get("areas") or []:
        command += ["--area", str(area).upper()]
    if params.get("force"):
        command.append("--force")
    return command


_tso_task = make_cli_task(
    venv_bin=JAPANMARKET["venv_bin"], cwd=JAPANMARKET["cwd"],
    task_name="tso-supply-update", cmd_builder=_build_tso_command,
    retries=0, timeout_seconds=TASK_TIMEOUT_SECONDS, skip_if_running_key="tso-supply-update",
)


@flow(name="tso-supply-update", flow_run_name=descriptive_run_name, log_prints=True,
      timeout_seconds=MAX_RUN_SECONDS)
def tso_supply_update_flow(areas: list[str] | None = None, months: int = 2, force: bool = False) -> None:
    """Refresh current and previous month so late TSO revisions are captured."""
    _tso_task({"areas": areas or [], "months": months, "force": force})
