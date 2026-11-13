"""BESS project research flow (Gemini research, document fetch, Claude verification, import preview).

Runs ``bess-research cycle`` in the japanmarket environment. Each run starts at most ``per_run``
paid research calls and never more than ``daily_limit`` per day (Japan time); fetch and verification
also finish work left by earlier runs. The monthly budget is a hard stop kept in the local ledger
(``japanmarket/.japanmarket/bess_research/state``). Results are saved as import previews only;
a person reviews and applies them.
"""
from __future__ import annotations

from prefect import flow

from config.envs import JAPANMARKET
from config.limits import MAX_RUN_SECONDS, TASK_TIMEOUT_SECONDS
from flows.shared.run_names import descriptive_run_name
from flows.shared.subprocess_task import make_cli_task


def _build_research_command(params: dict) -> list[str]:
    return [
        "bess-research",
        "--monthly-budget-usd", str(float(params["monthly_budget_usd"])),
        "cycle",
        "--daily-limit", str(int(params["daily_limit"])),
        "--per-run", str(int(params["per_run"])),
        "--allow-paid",
        "--allow-db-write",
    ]


_bess_research_task = make_cli_task(
    venv_bin=JAPANMARKET["venv_bin"],
    cwd=JAPANMARKET["cwd"],
    task_name="bess-research",
    cmd_builder=_build_research_command,
    retries=0,
    timeout_seconds=TASK_TIMEOUT_SECONDS,
    skip_if_running_key="bess-research",
)


@flow(name="bess-research", flow_run_name=descriptive_run_name, log_prints=True,
      timeout_seconds=MAX_RUN_SECONDS)
def bess_research_flow(daily_limit: int = 5, per_run: int = 1, monthly_budget_usd: float = 100.0) -> None:
    """Research up to ``per_run`` projects now, at most ``daily_limit`` per Japan day."""
    _bess_research_task({"daily_limit": daily_limit, "per_run": per_run, "monthly_budget_usd": monthly_budget_usd})
