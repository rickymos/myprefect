"""Japanese BESS Google Alert ingestion and extraction flow."""
from __future__ import annotations

from prefect import flow

from config.envs import SEKNOWLEDGEBANK_API
from flows.shared.subprocess_task import make_cli_task


def _build_bess_command(params: dict) -> list[str]:
    command = [
        "python",
        "scripts/bess_monitor.py",
        "--gmail-limit",
        str(int(params.get("gmail_limit") or 500)),
        "--fetch-limit",
        str(int(params.get("fetch_limit") or 100)),
        "--process-limit",
        str(int(params.get("process_limit") or 100)),
    ]
    if params.get("skip_gmail"):
        command.append("--skip-gmail")
    if params.get("skip_fetch"):
        command.append("--skip-fetch")
    if params.get("skip_extraction"):
        command.append("--skip-extraction")
    return command


_bess_monitor_task = make_cli_task(
    venv_bin=SEKNOWLEDGEBANK_API["venv_bin"],
    cwd=SEKNOWLEDGEBANK_API["cwd"],
    task_name="bess-japan-monitor",
    cmd_builder=_build_bess_command,
    retries=2,
    retry_delay_seconds=900,
    timeout_seconds=14400,
    skip_if_running_key="bess-japan-monitor",
)


@flow(name="bess-japan-monitor", log_prints=True)
def bess_monitor_flow(
    gmail_limit: int = 500,
    fetch_limit: int = 100,
    process_limit: int = 100,
    skip_gmail: bool = False,
    skip_fetch: bool = False,
    skip_extraction: bool = False,
) -> None:
    """Ingest Gmail alerts, fetch Japanese sources, and update canonical projects."""
    _bess_monitor_task({
        "gmail_limit": gmail_limit,
        "fetch_limit": fetch_limit,
        "process_limit": process_limit,
        "skip_gmail": skip_gmail,
        "skip_fetch": skip_fetch,
        "skip_extraction": skip_extraction,
    })
