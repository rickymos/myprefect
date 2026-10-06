"""Clear flow runs that can no longer make progress.

Nothing may run longer than config.limits.MAX_RUN_SECONDS, so:

* a run still RUNNING after OVERDUE_RUNNING_SECONDS has lost its process (for example the
  worker restarted) and is marked CRASHED, which releases its deployment's concurrency slot;
* a run waiting for a concurrency slot (AwaitingConcurrencySlot) or stuck in PENDING for
  longer than --stale-minutes is CANCELLED, so backlogs do not run back to back later;
* a deployment concurrency slot that is held while none of that deployment's runs is
  pending or running (a leaked slot) is released.

Dry run by default; pass --apply to change states. Meant to run every few minutes from cron,
independently of the Prefect worker.
"""
from __future__ import annotations

import argparse
import asyncio
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from prefect.client.orchestration import get_client  # noqa: E402
from prefect.client.schemas.actions import GlobalConcurrencyLimitUpdate  # noqa: E402
from prefect.client.schemas.filters import (  # noqa: E402
    FlowRunFilter,
    FlowRunFilterState,
    FlowRunFilterStateName,
    FlowRunFilterStateType,
)
from prefect.states import Cancelled, Crashed  # noqa: E402

from config.limits import MAX_RUN_SECONDS, OVERDUE_RUNNING_SECONDS  # noqa: E402


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--apply", action="store_true", help="Change states (default: dry run).")
    parser.add_argument("--stale-minutes", type=int, default=60,
                        help="Cancel runs waiting for a slot or pending longer than this. Default: 60.")
    return parser


async def _main(apply: bool, stale_minutes: int) -> int:
    now = datetime.now(timezone.utc)
    overdue_before = now - timedelta(seconds=OVERDUE_RUNNING_SECONDS)
    stale_before = now - timedelta(minutes=stale_minutes)
    changed = 0
    async with get_client() as client:
        running = await client.read_flow_runs(
            flow_run_filter=FlowRunFilter(state=FlowRunFilterState(type=FlowRunFilterStateType(any_=["RUNNING"]))),
            limit=200,
        )
        for run in running:
            started = run.start_time or (run.state.timestamp if run.state else None)
            if started and started < overdue_before:
                minutes = int((now - started).total_seconds() // 60)
                print(f"overdue running: {run.name} ({run.id}) running {minutes} min -> CRASHED")
                if apply:
                    await client.set_flow_run_state(
                        run.id,
                        Crashed(message=f"Still running after {minutes} min (limit {MAX_RUN_SECONDS // 60} min); "
                                        "no process is expected to be alive. Marked crashed by reap_overdue_runs."),
                        force=True,
                    )
                changed += 1

        waiting = await client.read_flow_runs(
            flow_run_filter=FlowRunFilter(state=FlowRunFilterState(
                name=FlowRunFilterStateName(any_=["AwaitingConcurrencySlot", "Pending"]))),
            limit=200,
        )
        for run in waiting:
            since = run.expected_start_time or (run.state.timestamp if run.state else None)
            if since and since < stale_before:
                print(f"stale {run.state_name}: {run.name} ({run.id}) due {since:%Y-%m-%d %H:%M} -> CANCELLED")
                if apply:
                    await client.set_flow_run_state(
                        run.id,
                        Cancelled(message="Superseded: waited too long for its turn. Cancelled by reap_overdue_runs."),
                        force=True,
                    )
                changed += 1

        active = await client.read_flow_runs(
            flow_run_filter=FlowRunFilter(state=FlowRunFilterState(
                type=FlowRunFilterStateType(any_=["RUNNING", "PENDING", "CANCELLING"]))),
            limit=200,
        )
        busy = {str(run.deployment_id) for run in active if run.deployment_id}
        for limit in await client.read_global_concurrency_limits(limit=200):
            if not limit.name.startswith("deployment:") or not limit.active_slots:
                continue
            deployment_id = limit.name.split(":", 1)[1]
            if deployment_id in busy:
                continue
            print(f"leaked slot: {limit.name} holds {limit.active_slots} with no active run -> released")
            if apply:
                await client.update_global_concurrency_limit(limit.name, GlobalConcurrencyLimitUpdate(active_slots=0))
            changed += 1

    print(f"{'changed' if apply else 'would change'} {changed} item(s)")
    return 0


if __name__ == "__main__":
    args = _parser().parse_args()
    raise SystemExit(asyncio.run(_main(args.apply, args.stale_minutes)))
