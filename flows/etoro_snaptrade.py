"""Scheduled SnapTrade ingestion for the eToro tracker."""
from __future__ import annotations

from prefect import flow

from config.envs import ETORO
from flows.shared.subprocess_task import make_cli_task


_holdings_task = make_cli_task(
    venv_bin=ETORO["venv_bin"],
    cwd=ETORO["cwd"],
    task_name="etoro-snaptrade-holdings",
    cmd=["etoro-tracker", "snaptrade-sync", "--scope", "holdings"],
    retries=3,
    retry_delay_seconds=300,
    timeout_seconds=900,
    skip_if_running_key="etoro-snaptrade-sync",
)

_activities_task = make_cli_task(
    venv_bin=ETORO["venv_bin"],
    cwd=ETORO["cwd"],
    task_name="etoro-snaptrade-activities",
    cmd=["etoro-tracker", "snaptrade-sync", "--scope", "activities"],
    retries=3,
    retry_delay_seconds=900,
    timeout_seconds=1800,
    skip_if_running_key="etoro-snaptrade-sync",
)


@flow(name="etoro-snaptrade-holdings", log_prints=True)
def etoro_snaptrade_holdings_flow() -> None:
    """Snapshot account balances and open positions every four hours."""
    _holdings_task()


@flow(name="etoro-snaptrade-activities", log_prints=True)
def etoro_snaptrade_activities_flow() -> None:
    """Ingest daily trades, dividends, fees, taxes, and cash movements."""
    _activities_task()
