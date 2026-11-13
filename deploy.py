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
    BESS_DISCOVERY_DAILY,
    BESS_SIMULATION_DAILY,
    BESS_DUPLICATE_AUDIT_WEEKLY,
    BESS_MONITOR_EVERY_SIX_HOURS,
    BESS_NEWS_EVERY_SIX_HOURS,
    BESS_RESEARCH_EVERY_FOUR_HOURS,
    DAILY_PREFECT_TEMP_CLEANUP,
    ETORO_ACTIVITIES_DAILY,
    ETORO_HOLDINGS_EVERY_FOUR_HOURS,
    EPRX_UPDATE_DAILY,
    FIT_FIP_UPDATE_WEEKLY,
    FLIGHT_SEARCH_TWICE_DAILY,
    GRID_TARIFFS_WEEKLY,
    HJKS_UPDATE_DAILY,
    JEPX_UPDATE_DAILY,
    JOBS_DAILY,
    TSO_SUPPLY_UPDATE_DAILY,
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


def deployment(entrypoint: str, cadence: str, **kwargs):
    """Name each deployment ``<flow>-<cadence>`` so the Deployments page tells them apart."""
    flow = source_flow(entrypoint)
    return flow.to_deployment(name=f"{flow.name}-{cadence}", **kwargs)


async def reconcile_deployment_global_limit(deployment_id: str) -> None:
    async with get_client() as client:
        deployment = await client.read_deployment(deployment_id)
        print(f"Deployment concurrency active: {deployment.name}")


deployments = [
    deployment("flows/bess_monitor.py:bess_monitor_flow", "every-six-hours",
        schedule=BESS_MONITOR_EVERY_SIX_HOURS,
        paused=False,
        parameters={"gmail_limit": 500, "fetch_limit": 100, "process_limit": 60},
        concurrency_limit=SINGLE_ACTIVE_RUN_LIMIT,
        work_pool_name=WORK_POOL_NAME,
    ),
    deployment("flows/bess_monitor.py:bess_monitor_flow", "manual",
        parameters={"gmail_limit": 500, "fetch_limit": 100, "process_limit": 60},
        concurrency_limit=SINGLE_ACTIVE_RUN_LIMIT,
        work_pool_name=WORK_POOL_NAME,
    ),
    deployment("flows/bess_discovery.py:bess_discovery_flow", "daily",
        schedule=BESS_DISCOVERY_DAILY,
        paused=False,
        parameters={"promote": 20},
        concurrency_limit=SINGLE_ACTIVE_RUN_LIMIT,
        work_pool_name=WORK_POOL_NAME,
    ),
    deployment("flows/bess_duplicate_audit.py:bess_duplicate_audit_flow", "weekly",
        schedule=BESS_DUPLICATE_AUDIT_WEEKLY,
        paused=False,
        parameters={"min_score": 0.60},
        concurrency_limit=SINGLE_ACTIVE_RUN_LIMIT,
        work_pool_name=WORK_POOL_NAME,
    ),
    deployment("flows/bess_duplicate_audit.py:bess_duplicate_audit_flow", "manual",
        parameters={"min_score": 0.60},
        concurrency_limit=SINGLE_ACTIVE_RUN_LIMIT,
        work_pool_name=WORK_POOL_NAME,
    ),
    deployment("flows/bess_discovery.py:bess_discovery_flow", "manual",
        parameters={"promote": 0},
        concurrency_limit=SINGLE_ACTIVE_RUN_LIMIT,
        work_pool_name=WORK_POOL_NAME,
    ),
    deployment("flows/bess_news.py:bess_news_flow", "every-six-hours",
        schedule=BESS_NEWS_EVERY_SIX_HOURS,
        paused=False,
        parameters={"limit": 100},
        concurrency_limit=SINGLE_ACTIVE_RUN_LIMIT,
        work_pool_name=WORK_POOL_NAME,
    ),
    deployment("flows/bess_news.py:bess_news_flow", "manual",
        parameters={"limit": 100},
        concurrency_limit=SINGLE_ACTIVE_RUN_LIMIT,
        work_pool_name=WORK_POOL_NAME,
    ),
    deployment("flows/bess_research.py:bess_research_flow", "every-four-hours",
        schedule=BESS_RESEARCH_EVERY_FOUR_HOURS,
        paused=False,
        parameters={"daily_limit": 5, "per_run": 1, "monthly_budget_usd": 100.0},
        concurrency_limit=SINGLE_ACTIVE_RUN_LIMIT,
        work_pool_name=WORK_POOL_NAME,
    ),
    # Same flow, run by hand; the daily cap still counts scheduled starts. Raise per_run for a bigger batch.
    deployment("flows/bess_research.py:bess_research_flow", "manual",
        parameters={"daily_limit": 5, "per_run": 1, "monthly_budget_usd": 100.0},
        concurrency_limit=SINGLE_ACTIVE_RUN_LIMIT,
        work_pool_name=WORK_POOL_NAME,
    ),
    deployment("flows/jepx_update.py:jepx_update_flow", "daily",
        schedule=JEPX_UPDATE_DAILY,
        paused=False,
        concurrency_limit=SINGLE_ACTIVE_RUN_LIMIT,
        work_pool_name=WORK_POOL_NAME,
    ),
    deployment("flows/jepx_update.py:jepx_update_flow", "manual",
        concurrency_limit=SINGLE_ACTIVE_RUN_LIMIT,
        work_pool_name=WORK_POOL_NAME,
    ),
    deployment("flows/hjks_update.py:hjks_update_flow", "daily",
        schedule=HJKS_UPDATE_DAILY,
        paused=False,
        concurrency_limit=SINGLE_ACTIVE_RUN_LIMIT,
        work_pool_name=WORK_POOL_NAME,
    ),
    deployment("flows/hjks_update.py:hjks_update_flow", "manual",
        concurrency_limit=SINGLE_ACTIVE_RUN_LIMIT,
        work_pool_name=WORK_POOL_NAME,
    ),
    deployment("flows/tso_supply_update.py:tso_supply_update_flow", "daily",
        schedule=TSO_SUPPLY_UPDATE_DAILY,
        paused=False,
        parameters={"months": 2},
        concurrency_limit=SINGLE_ACTIVE_RUN_LIMIT,
        work_pool_name=WORK_POOL_NAME,
    ),
    deployment("flows/tso_supply_update.py:tso_supply_update_flow", "manual",
        parameters={"months": 2},
        concurrency_limit=SINGLE_ACTIVE_RUN_LIMIT,
        work_pool_name=WORK_POOL_NAME,
    ),
    deployment("flows/eprx_update.py:eprx_update_flow", "daily",
        schedule=EPRX_UPDATE_DAILY,
        paused=False,
        parameters={"months": 2},
        concurrency_limit=SINGLE_ACTIVE_RUN_LIMIT,
        work_pool_name=WORK_POOL_NAME,
    ),
    deployment("flows/eprx_update.py:eprx_update_flow", "manual",
        parameters={"months": 2},
        concurrency_limit=SINGLE_ACTIVE_RUN_LIMIT,
        work_pool_name=WORK_POOL_NAME,
    ),
    deployment("flows/bess_simulation.py:bess_simulation_flow", "daily",
        schedule=BESS_SIMULATION_DAILY,
        paused=False,
        concurrency_limit=SINGLE_ACTIVE_RUN_LIMIT,
        work_pool_name=WORK_POOL_NAME,
    ),
    deployment("flows/bess_simulation.py:bess_simulation_flow", "manual",
        concurrency_limit=SINGLE_ACTIVE_RUN_LIMIT,
        work_pool_name=WORK_POOL_NAME,
    ),
    deployment("flows/fit_fip_update.py:fit_fip_update_flow", "weekly",
        schedule=FIT_FIP_UPDATE_WEEKLY,
        paused=False,
        parameters={"max_files": 47},
        concurrency_limit=SINGLE_ACTIVE_RUN_LIMIT,
        work_pool_name=WORK_POOL_NAME,
    ),
    deployment("flows/fit_fip_update.py:fit_fip_update_flow", "manual",
        parameters={"max_files": 47},
        concurrency_limit=SINGLE_ACTIVE_RUN_LIMIT,
        work_pool_name=WORK_POOL_NAME,
    ),
    deployment("flows/grid_tariffs.py:grid_tariffs_flow", "weekly",
        schedule=GRID_TARIFFS_WEEKLY,
        paused=False,
        concurrency_limit=SINGLE_ACTIVE_RUN_LIMIT,
        work_pool_name=WORK_POOL_NAME,
    ),
    deployment("flows/grid_tariffs.py:grid_tariffs_flow", "manual",
        concurrency_limit=SINGLE_ACTIVE_RUN_LIMIT,
        work_pool_name=WORK_POOL_NAME,
    ),
    deployment("flows/flight_search.py:flight_route_search_flow", "twice-daily",
        schedule=FLIGHT_SEARCH_TWICE_DAILY,
        paused=False,
        concurrency_limit=SINGLE_ACTIVE_RUN_LIMIT,
        work_pool_name=WORK_POOL_NAME,
    ),
    deployment("flows/flight_search.py:flight_route_search_flow", "manual",
        concurrency_limit=SINGLE_ACTIVE_RUN_LIMIT,
        work_pool_name=WORK_POOL_NAME,
    ),
    deployment("flows/etoro_snaptrade.py:etoro_snaptrade_holdings_flow", "every-four-hours",
        schedule=ETORO_HOLDINGS_EVERY_FOUR_HOURS,
        concurrency_limit=SINGLE_ACTIVE_RUN_LIMIT,
        work_pool_name=WORK_POOL_NAME,
    ),
    deployment("flows/etoro_snaptrade.py:etoro_snaptrade_holdings_flow", "manual",
        concurrency_limit=SINGLE_ACTIVE_RUN_LIMIT,
        work_pool_name=WORK_POOL_NAME,
    ),
    deployment("flows/etoro_snaptrade.py:etoro_snaptrade_activities_flow", "daily",
        schedule=ETORO_ACTIVITIES_DAILY,
        concurrency_limit=SINGLE_ACTIVE_RUN_LIMIT,
        work_pool_name=WORK_POOL_NAME,
    ),
    deployment("flows/etoro_snaptrade.py:etoro_snaptrade_activities_flow", "manual",
        concurrency_limit=SINGLE_ACTIVE_RUN_LIMIT,
        work_pool_name=WORK_POOL_NAME,
    ),
    deployment("flows/jobs_daily.py:job_watch_flow", "daily",
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
    deployment("flows/jobs_daily.py:job_watch_flow", "manual",
        parameters={
            "limit": 80,
            "save_policy": "relevant",
            "playwright": False,
            "rss": False,
        },
        concurrency_limit=SINGLE_ACTIVE_RUN_LIMIT,
        work_pool_name=WORK_POOL_NAME,
    ),
    deployment("flows/housekeeping.py:prefect_housekeeping_flow", "daily",
        schedule=WEEKLY_PREFECT_HOUSEKEEPING,
        concurrency_limit=SINGLE_ACTIVE_RUN_LIMIT,
        work_pool_name=WORK_POOL_NAME,
    ),
    deployment("flows/housekeeping.py:prefect_housekeeping_flow", "manual",
        concurrency_limit=SINGLE_ACTIVE_RUN_LIMIT,
        work_pool_name=WORK_POOL_NAME,
    ),
    deployment("flows/housekeeping.py:prefect_temp_cleanup_flow", "daily",
        schedule=DAILY_PREFECT_TEMP_CLEANUP,
        concurrency_limit=SINGLE_ACTIVE_RUN_LIMIT,
        work_pool_name=WORK_POOL_NAME,
    ),
    deployment("flows/housekeeping.py:prefect_temp_cleanup_flow", "manual",
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
