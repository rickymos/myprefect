"""Human-readable, unique names for Prefect flow runs."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from prefect import runtime


def _slug(value: str | None, fallback: str) -> str:
    cleaned = "-".join((value or "").strip().casefold().replace("_", "-").split())
    return cleaned or fallback


def format_run_name(
    *,
    flow_name: str | None,
    deployment_name: str | None,
    scheduled_start_time: datetime | None,
    run_id: Any,
) -> str:
    """Build ``flow__deployment__UTC timestamp__short id`` for quick scanning."""
    started = scheduled_start_time or datetime.now(timezone.utc)
    if started.tzinfo is None:
        started = started.replace(tzinfo=timezone.utc)
    timestamp = started.astimezone(timezone.utc).strftime("%Y%m%d-%H%M")
    compact_id = str(run_id or "unknown").replace("-", "")
    short_id = compact_id[-8:]
    return "__".join(
        (
            _slug(flow_name, "flow"),
            _slug(deployment_name, "ad-hoc"),
            timestamp,
            short_id,
        )
    )


def descriptive_run_name() -> str:
    """Resolve a run name from the active Prefect runtime context."""
    return format_run_name(
        flow_name=runtime.flow_run.flow_name,
        deployment_name=runtime.deployment.name,
        scheduled_start_time=runtime.flow_run.scheduled_start_time,
        run_id=runtime.flow_run.id,
    )
