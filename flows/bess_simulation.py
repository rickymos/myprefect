"""BESS simulation refresh: solve and save the standard optimiser runs (japanmarket.simulation).

Runs ``python -m japanmarket.simulation.cli refresh`` in the japanmarket environment, after the JEPX import.
Saved runs are kept; only missing ones and the current fiscal year (when newer prices arrived) are solved,
about 4 s each: a normal day re-solves ~30 runs, a model change ~240 (about 16 minutes).
"""
from __future__ import annotations

from prefect import flow

from config.envs import JAPANMARKET
from config.limits import MAX_RUN_SECONDS, TASK_TIMEOUT_SECONDS
from flows.shared.run_names import descriptive_run_name
from flows.shared.subprocess_task import make_cli_task


def _build_command(params: dict) -> list[str]:
    command = ["python", "-m", "japanmarket.simulation.cli", "refresh"]
    for market in params.get("markets") or []:
        command += ["--market", str(market)]
    for year in params.get("years") or []:
        command += ["--year", str(int(year))]
    if params.get("force"):
        command.append("--force")
    return command


_refresh_task = make_cli_task(
    venv_bin=JAPANMARKET["venv_bin"],
    cwd=JAPANMARKET["cwd"],
    task_name="bess-simulation",
    cmd_builder=_build_command,
    # Missing runs are picked up by the next day's run, so no retry.
    retries=0,
    timeout_seconds=TASK_TIMEOUT_SECONDS,
    skip_if_running_key="bess-simulation",
)


@flow(name="bess-simulation", flow_run_name=descriptive_run_name, log_prints=True,
      timeout_seconds=MAX_RUN_SECONDS)
def bess_simulation_flow(markets: list[str] | None = None, years: list[int] | None = None, force: bool = False) -> None:
    """Solve missing or out-of-date standard runs; ``markets`` spot/imbalance, ``years`` fiscal years."""
    _refresh_task({"markets": markets or [], "years": years or [], "force": force})
