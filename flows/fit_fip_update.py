"""FIT/FIP certification register update (METI 事業計画認定情報).

Runs ``python -m japanmarket.fit_fip.cli update`` in the japanmarket environment. METI refreshes the
register about monthly; a run imports only prefecture workbooks that changed since the last import,
then refreshes operator totals and the links to JEMI companies. ``max_files`` keeps a run inside the
30-minute limit (a full re-import of all 47 prefectures is better run by hand).
"""
from __future__ import annotations

from prefect import flow

from config.envs import JAPANMARKET
from config.limits import MAX_RUN_SECONDS, TASK_TIMEOUT_SECONDS
from flows.shared.run_names import descriptive_run_name
from flows.shared.subprocess_task import make_cli_task


def _build_fit_fip_command(params: dict) -> list[str]:
    command = ["python", "-m", "japanmarket.fit_fip.cli", "update"]
    for prefecture in params.get("prefectures") or []:
        command += ["--prefecture", str(prefecture)]
    if params.get("force"):
        command.append("--force")
    if params.get("max_files"):
        command += ["--max-files", str(int(params["max_files"]))]
    return command


_fit_fip_update_task = make_cli_task(
    venv_bin=JAPANMARKET["venv_bin"],
    cwd=JAPANMARKET["cwd"],
    task_name="fit-fip-update",
    cmd_builder=_build_fit_fip_command,
    retries=0,
    timeout_seconds=TASK_TIMEOUT_SECONDS,
    skip_if_running_key="fit-fip-update",
)


@flow(name="fit-fip-update", flow_run_name=descriptive_run_name, log_prints=True,
      timeout_seconds=MAX_RUN_SECONDS)
def fit_fip_update_flow(
    prefectures: list[str] | None = None, force: bool = False, max_files: int = 47,
) -> None:
    """Import changed prefecture workbooks (all 47 when METI publishes a new snapshot)."""
    _fit_fip_update_task({"prefectures": prefectures or [], "force": force, "max_files": max_files})
