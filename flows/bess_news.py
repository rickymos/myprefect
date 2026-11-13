"""BESS news page upkeep: AI summaries for new news items, and removal of items older than one month.

Runs ``python -m japanmarket.bess.news daily`` in the japanmarket environment. News items are sources too
incomplete for the registry (see japanmarket.bess.news); the monitor and discovery flows create them.
"""
from __future__ import annotations

from prefect import flow

from config.envs import JAPANMARKET
from config.limits import MAX_RUN_SECONDS, TASK_TIMEOUT_SECONDS
from flows.shared.run_names import descriptive_run_name
from flows.shared.subprocess_task import make_cli_task


def _build_news_command(params: dict) -> list[str]:
    return ["python", "-m", "japanmarket.bess.news", "daily", "--limit", str(int(params["limit"]))]


_bess_news_task = make_cli_task(
    venv_bin=JAPANMARKET["venv_bin"],
    cwd=JAPANMARKET["cwd"],
    task_name="bess-news",
    cmd_builder=_build_news_command,
    retries=0,
    timeout_seconds=TASK_TIMEOUT_SECONDS,
    skip_if_running_key="bess-news",
)


@flow(name="bess-news", flow_run_name=descriptive_run_name, log_prints=True,
      timeout_seconds=MAX_RUN_SECONDS)
def bess_news_flow(limit: int = 100) -> None:
    """Summarize up to ``limit`` new news items, then purge items older than the retention window."""
    _bess_news_task({"limit": limit})
