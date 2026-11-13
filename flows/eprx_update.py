"""Daily EPRX balancing-market prompt/final result import."""
from __future__ import annotations

from prefect import flow

from config.envs import JAPANMARKET
from config.limits import MAX_RUN_SECONDS, TASK_TIMEOUT_SECONDS
from flows.shared.run_names import descriptive_run_name
from flows.shared.subprocess_task import make_cli_task


def _build_eprx_command(params: dict) -> list[str]:
    command = ["python", "-m", "japanmarket.eprx.cli", "update", "--months", str(int(params.get("months", 2)))]
    for product in params.get("products") or []:
        command += ["--product", str(product)]
    if params.get("force"):
        command.append("--force")
    return command


_eprx_task = make_cli_task(
    venv_bin=JAPANMARKET["venv_bin"], cwd=JAPANMARKET["cwd"],
    task_name="eprx-update", cmd_builder=_build_eprx_command,
    retries=0, timeout_seconds=TASK_TIMEOUT_SECONDS, skip_if_running_key="eprx-update",
)


@flow(name="eprx-update", flow_run_name=descriptive_run_name, log_prints=True,
      timeout_seconds=MAX_RUN_SECONDS)
def eprx_update_flow(products: list[str] | None = None, months: int = 2, force: bool = False) -> None:
    """Refresh the latest two months of all prompt/final balancing products."""
    _eprx_task({"products": products or [], "months": months, "force": force})
