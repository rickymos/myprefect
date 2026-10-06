"""Daily job-watch orchestration flow."""
from __future__ import annotations

from datetime import UTC, datetime

from prefect import flow

from config.envs import JOBS
from flows.shared.subprocess_task import make_cli_task


def _build_job_watch_command(params: dict) -> list[str]:
    today = params.get("today") or datetime.now(UTC).date().isoformat()
    limit = int(params.get("limit") or 80)
    save_policy = params.get("save_policy") or "relevant"
    command = [
        "job-watch",
        "--today",
        today,
        "--limit",
        str(limit),
        "--llm-review",
        "--save-db",
        "--save-policy",
        save_policy,
    ]
    if params.get("playwright"):
        command.append("--playwright")
    if params.get("rss"):
        command.append("--rss")
    return command


_job_watch_task = make_cli_task(
    venv_bin=JOBS["venv_bin"],
    cwd=JOBS["cwd"],
    task_name="job-watch-daily",
    retries=1,
    retry_delay_seconds=900,
    timeout_seconds=3600,
    cmd_builder=_build_job_watch_command,
    skip_if_running_key="job-watch-daily",
)


@flow(name="job-watch", log_prints=True)
def job_watch_flow(
    today: str | None = None,
    limit: int = 80,
    save_policy: str = "relevant",
    playwright: bool = False,
    rss: bool = False,
) -> None:
    """Collect, AI-review, and save relevant job postings once per day."""
    _job_watch_task(
        {
            "today": today,
            "limit": limit,
            "save_policy": save_policy,
            "playwright": playwright,
            "rss": rss,
        }
    )
