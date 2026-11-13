"""Twice-daily flight discovery and route optimisation.

Runs the independent SeKnowledgeBank flight engine in its own virtual environment.
The engine reads its provider and database settings from ``seknowledgebank/api/.env``.
"""
from __future__ import annotations

from prefect import flow

from config.envs import SEKNOWLEDGEBANK_FLIGHT_ENGINE
from config.limits import MAX_RUN_SECONDS, TASK_TIMEOUT_SECONDS
from flows.shared.run_names import descriptive_run_name
from flows.shared.subprocess_task import make_cli_task


_flight_search_task = make_cli_task(
    venv_bin=SEKNOWLEDGEBANK_FLIGHT_ENGINE["venv_bin"],
    cwd=SEKNOWLEDGEBANK_FLIGHT_ENGINE["cwd"],
    task_name="flight-route-search",
    cmd=["python", "-m", "flight_engine.runner", "run"],
    # Provider-level request failures are recorded by the engine. Avoid repeating
    # a full search batch automatically and consuming the provider budget twice.
    retries=0,
    timeout_seconds=TASK_TIMEOUT_SECONDS,
    skip_if_running_key="flight-route-search",
)


@flow(
    name="flight-route-search",
    flow_run_name=descriptive_run_name,
    log_prints=True,
    timeout_seconds=MAX_RUN_SECONDS,
)
def flight_route_search_flow() -> None:
    """Refresh all active flight scenarios and store their ranked results."""
    _flight_search_task()
