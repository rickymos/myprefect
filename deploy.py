"""Register local Prefect deployments.

Run after install and whenever flows or schedules change:

    cd /home/tdm/Documents/prefect
    ./bin/deploy-local
"""
from __future__ import annotations

import asyncio
import os
from pathlib import Path

from prefect.client.orchestration import get_client
from prefect.client.schemas.objects import ConcurrencyLimitConfig, ConcurrencyLimitStrategy
from prefect.exceptions import ObjectNotFound
from prefect.flows import Flow

from config.schedules import (
    BESS_MONITOR_EVERY_SIX_HOURS,
    DAILY_PREFECT_TEMP_CLEANUP,
    ETORO_ACTIVITIES_DAILY,
    ETORO_HOLDINGS_EVERY_FOUR_HOURS,
    JOBS_DAILY,
    WEEKLY_PREFECT_HOUSEKEEPING,
)

WORK_POOL_NAME = os.getenv("PREFECT_WORK_POOL_NAME", "default-agent-pool")
# One active run per deployment. A run that becomes due while another is still running is
# cancelled instead of queued, so backlogs never pile up behind a slow or stuck run.
SINGLE_ACTIVE_RUN_LIMIT = ConcurrencyLimitConfig(limit=1, collision_strategy=ConcurrencyLimitStrategy.CANCEL_NEW)
PREFECT_ROOT = Path(__file__).resolve().parent
EXPECTED_PREFECT_HOME = str(PREFECT_ROOT / ".prefect")
EXPECTED_PREFECT_API_URL = "http://127.0.0.1:4200/prefect/api"


def require_local_prefect_environment() -> None:
    """Fail fast unless deployment registration targets this local server."""
    prefect_api_url = os.getenv("PREFECT_API_URL")
    prefect_home = os.getenv("PREFECT_HOME")

    if prefect_api_url == EXPECTED_PREFECT_API_URL and prefect_home == EXPECTED_PREFECT_HOME:
        return

    raise SystemExit(
        "deploy.py must target the managed local Prefect environment.\n"
        f"Expected PREFECT_API_URL={EXPECTED_PREFECT_API_URL}\n"
        f"Expected PREFECT_HOME={EXPECTED_PREFECT_HOME}\n\n"
        "Run ./bin/deploy-local from /home/tdm/Documents/prefect."
    )


def source_flow(entrypoint: str) -> Flow:
    """Load a flow from this Prefect project so workers import fresh code."""
    return Flow.from_source(source=str(PREFECT_ROOT), entrypoint=entrypoint)


async def reconcile_deployment_global_limit(deployment_id: str) -> None:
    async with get_client() as client:
        deployment = await client.read_deployment(deployment_id)
        print(f"Deployment concurrency active: {deployment.name}")


deployments = [
    source_flow("flows/bess_monitor.py:bess_monitor_flow").to_deployment(
        name="every-six-hours",
        schedule=BESS_MONITOR_EVERY_SIX_HOURS,
        paused=False,
        parameters={"gmail_limit": 500, "fetch_limit": 100, "process_limit": 60},
        concurrency_limit=SINGLE_ACTIVE_RUN_LIMIT,
        work_pool_name=WORK_POOL_NAME,
    ),
    source_flow("flows/bess_monitor.py:bess_monitor_flow").to_deployment(
        name="manual",
        parameters={"gmail_limit": 500, "fetch_limit": 100, "process_limit": 60},
        concurrency_limit=SINGLE_ACTIVE_RUN_LIMIT,
        work_pool_name=WORK_POOL_NAME,
    ),
    source_flow("flows/etoro_snaptrade.py:etoro_snaptrade_holdings_flow").to_deployment(
        name="every-four-hours",
        schedule=ETORO_HOLDINGS_EVERY_FOUR_HOURS,
        concurrency_limit=SINGLE_ACTIVE_RUN_LIMIT,
        work_pool_name=WORK_POOL_NAME,
    ),
    source_flow("flows/etoro_snaptrade.py:etoro_snaptrade_holdings_flow").to_deployment(
        name="manual",
        concurrency_limit=SINGLE_ACTIVE_RUN_LIMIT,
        work_pool_name=WORK_POOL_NAME,
    ),
    source_flow("flows/etoro_snaptrade.py:etoro_snaptrade_activities_flow").to_deployment(
        name="daily",
        schedule=ETORO_ACTIVITIES_DAILY,
        concurrency_limit=SINGLE_ACTIVE_RUN_LIMIT,
        work_pool_name=WORK_POOL_NAME,
    ),
    source_flow("flows/etoro_snaptrade.py:etoro_snaptrade_activities_flow").to_deployment(
        name="manual",
        concurrency_limit=SINGLE_ACTIVE_RUN_LIMIT,
        work_pool_name=WORK_POOL_NAME,
    ),
    source_flow("flows/jobs_daily.py:job_watch_flow").to_deployment(
        name="daily",
        schedule=JOBS_DAILY,
        parameters={
            "limit": 80,
            "save_policy": "relevant",
            "playwright": False,
            "rss": False,
        },
        concurrency_limit=SINGLE_ACTIVE_RUN_LIMIT,
        work_pool_name=WORK_POOL_NAME,
    ),
    source_flow("flows/jobs_daily.py:job_watch_flow").to_deployment(
        name="manual",
        parameters={
            "limit": 80,
            "save_policy": "relevant",
            "playwright": False,
            "rss": False,
        },
        concurrency_limit=SINGLE_ACTIVE_RUN_LIMIT,
        work_pool_name=WORK_POOL_NAME,
    ),
    source_flow("flows/housekeeping.py:prefect_housekeeping_flow").to_deployment(
        name="daily",
        schedule=WEEKLY_PREFECT_HOUSEKEEPING,
        concurrency_limit=SINGLE_ACTIVE_RUN_LIMIT,
        work_pool_name=WORK_POOL_NAME,
    ),
    source_flow("flows/housekeeping.py:prefect_housekeeping_flow").to_deployment(
        name="manual",
        concurrency_limit=SINGLE_ACTIVE_RUN_LIMIT,
        work_pool_name=WORK_POOL_NAME,
    ),
    source_flow("flows/housekeeping.py:prefect_temp_cleanup_flow").to_deployment(
        name="daily",
        schedule=DAILY_PREFECT_TEMP_CLEANUP,
        concurrency_limit=SINGLE_ACTIVE_RUN_LIMIT,
        work_pool_name=WORK_POOL_NAME,
    ),
    source_flow("flows/housekeeping.py:prefect_temp_cleanup_flow").to_deployment(
        name="manual",
        concurrency_limit=SINGLE_ACTIVE_RUN_LIMIT,
        work_pool_name=WORK_POOL_NAME,
    ),
]


if __name__ == "__main__":
    require_local_prefect_environment()

    from prefect import deploy

    deployment_ids = deploy(*deployments, work_pool_name=WORK_POOL_NAME)
    print("Applied deployments:")
    for deployment_id in deployment_ids:
        print(f" - {deployment_id}")
        try:
            asyncio.run(reconcile_deployment_global_limit(deployment_id))
        except ObjectNotFound:
            pass
