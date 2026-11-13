"""JEPX market data update (spot, intraday, FIP reference/avoidable cost, baseload, non-fossil, imbalance).

Runs ``python -m japanmarket.jepx.cli update`` in the japanmarket environment. Each run downloads the
current fiscal-year CSV of every JEPX market plus the imbalance month list, skips unchanged files and
upserts the rest into ``japan_energy``. ``full`` re-imports every published year (backfill).
"""
from __future__ import annotations

from prefect import flow

from config.envs import JAPANMARKET
from config.limits import MAX_RUN_SECONDS, TASK_TIMEOUT_SECONDS
from flows.shared.run_names import descriptive_run_name
from flows.shared.subprocess_task import make_cli_task


def _build_jepx_command(params: dict) -> list[str]:
    command = ["python", "-m", "japanmarket.jepx.cli", "update"]
    for dataset in params.get("datasets") or []:
        command += ["--dataset", str(dataset)]
    if params.get("full"):
        command.append("--full")
    return command


_jepx_update_task = make_cli_task(
    venv_bin=JAPANMARKET["venv_bin"],
    cwd=JAPANMARKET["cwd"],
    task_name="jepx-update",
    cmd_builder=_build_jepx_command,
    # A missed day is picked up by the next run (files are re-read by fiscal year), so no retry.
    retries=0,
    timeout_seconds=TASK_TIMEOUT_SECONDS,
    skip_if_running_key="jepx-update",
)


@flow(name="jepx-update", flow_run_name=descriptive_run_name, log_prints=True,
      timeout_seconds=MAX_RUN_SECONDS)
def jepx_update_flow(datasets: list[str] | None = None, full: bool = False) -> None:
    """Import changed JEPX files; ``datasets`` limits the run (spot, intraday, fip, baseload, rec, imbalance)."""
    _jepx_update_task({"datasets": datasets or [], "full": full})
