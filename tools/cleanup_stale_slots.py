from __future__ import annotations

import argparse
import asyncio
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from prefect.client.orchestration import get_client
from prefect.client.schemas.filters import FlowRunFilter, FlowRunFilterState, FlowRunFilterStateType
from prefect.exceptions import ObjectNotFound


@dataclass
class StaleCancellingRun:
    flow_run_id: str
    flow_run_name: str
    deployment_id: str
    deployment_name: str
    flow_name: str
    state_timestamp: datetime
    expected_start_time: datetime | None
    age: timedelta
    reason: str


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="prefect-local cleanup-stale-slots",
        description=(
            "Delete stale never-started flow runs that can keep Prefect concurrency slots occupied. "
            "Handles both stuck Cancelling runs and superseded AwaitingConcurrencySlot runs."
        ),
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Delete matching flow runs. Without this flag, the command is a dry run.",
    )
    parser.add_argument(
        "--older-than-minutes",
        type=int,
        default=10,
        help="Only target candidate runs older than this many minutes. Default: 10.",
    )
    parser.add_argument(
        "--deployment",
        action="append",
        default=[],
        help="Optional deployment name filter. Can be provided multiple times.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=200,
        help="Maximum number of Cancelling runs to inspect. Prefect caps this at 200. Default: 200.",
    )
    return parser


def _effective_state_timestamp(run) -> datetime | None:
    if run.state is None:
        return None
    if run.state.timestamp is not None:
        return run.state.timestamp
    return run.expected_start_time


def _has_newer_started_or_terminal_run(*, run, runs_for_deployment: list) -> bool:
    current_expected = run.expected_start_time
    if current_expected is None:
        return False

    for candidate in runs_for_deployment:
        if candidate.id == run.id:
            continue
        candidate_expected = candidate.expected_start_time
        if candidate_expected is None or candidate_expected <= current_expected:
            continue

        if candidate.start_time is not None:
            return True

        state_type = getattr(candidate.state, "type", None)
        if state_type in {
            "COMPLETED",
            "FAILED",
            "CANCELLED",
            "CANCELLING",
            "CRASHED",
        }:
            return True
        if getattr(state_type, "value", None) in {
            "COMPLETED",
            "FAILED",
            "CANCELLED",
            "CANCELLING",
            "CRASHED",
        }:
            return True

    return False


def _pid_is_alive(pid_text: str | None) -> bool:
    if not pid_text:
        return False
    try:
        pid = int(pid_text)
    except (TypeError, ValueError):
        return False
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


async def _find_stale_runs(*, older_than: timedelta, limit: int, deployment_filters: set[str]) -> list[StaleCancellingRun]:
    now = datetime.now(timezone.utc)
    stale_runs: list[StaleCancellingRun] = []
    limit = min(limit, 200)

    async with get_client() as client:
        runs = await client.read_flow_runs(limit=limit)

        deployment_cache: dict[str, tuple[str, str]] = {}
        runs_by_deployment: dict[str, list] = {}
        for run in runs:
            if run.deployment_id is None:
                continue
            runs_by_deployment.setdefault(str(run.deployment_id), []).append(run)

        for run in runs:
            if run.deployment_id is None:
                continue

            state_name = getattr(run.state, "name", None)
            if state_name not in {"CANCELLING", "AwaitingConcurrencySlot"}:
                continue

            state_timestamp = _effective_state_timestamp(run)
            if state_timestamp is None:
                continue

            age = now - state_timestamp
            if age < older_than:
                continue

            reason: str | None = None
            if state_name == "CANCELLING":
                if run.start_time is None:
                    reason = "never-started cancelling run"
                elif not _pid_is_alive(getattr(run, "infrastructure_pid", None)):
                    reason = "started cancelling run with no live infrastructure pid"
            elif state_name == "AwaitingConcurrencySlot":
                if run.start_time is not None:
                    continue
                deployment_runs = runs_by_deployment.get(str(run.deployment_id), [])
                if _has_newer_started_or_terminal_run(run=run, runs_for_deployment=deployment_runs):
                    reason = "superseded awaiting-concurrency run"
                else:
                    continue
            if reason is None:
                continue

            deployment_id = str(run.deployment_id)
            if deployment_id not in deployment_cache:
                deployment = await client.read_deployment(run.deployment_id)
                flow = await client.read_flow(deployment.flow_id)
                deployment_cache[deployment_id] = (deployment.name, flow.name)

            deployment_name, flow_name = deployment_cache[deployment_id]
            if deployment_filters and deployment_name not in deployment_filters:
                continue

            stale_runs.append(
                StaleCancellingRun(
                    flow_run_id=str(run.id),
                    flow_run_name=run.name,
                    deployment_id=deployment_id,
                    deployment_name=deployment_name,
                    flow_name=flow_name,
                    state_timestamp=state_timestamp,
                    expected_start_time=run.expected_start_time,
                    age=age,
                    reason=reason,
                )
            )

    return stale_runs


async def _print_slot_state(deployment_ids: set[str]) -> None:
    if not deployment_ids:
        return

    async with get_client() as client:
        for deployment_id in sorted(deployment_ids):
            try:
                limit = await client.read_global_concurrency_limit_by_name(f"deployment:{deployment_id}")
            except ObjectNotFound:
                print("slot", deployment_id, "active_slots=none", "limit=none")
                continue
            print(
                "slot",
                deployment_id,
                f"active_slots={limit.active_slots}",
                f"limit={limit.limit}",
            )


async def _delete_runs(stale_runs: list[StaleCancellingRun]) -> None:
    async with get_client() as client:
        for run in stale_runs:
            await client.delete_flow_run(run.flow_run_id)
            print(
                "deleted",
                run.flow_run_id,
                run.deployment_name,
                run.flow_run_name,
            )


async def _main_async(args: argparse.Namespace) -> int:
    deployment_filters = {name.strip() for name in args.deployment if name.strip()}
    older_than = timedelta(minutes=args.older_than_minutes)
    stale_runs = await _find_stale_runs(
        older_than=older_than,
        limit=args.limit,
        deployment_filters=deployment_filters,
    )

    if not stale_runs:
        print("No stale never-started concurrency-blocking flow runs found.")
        return 0

    for run in stale_runs:
        print(
            "candidate",
            run.flow_run_id,
            run.deployment_name,
            run.flow_name,
            run.flow_run_name,
            f"reason={run.reason}",
            f"age={run.age}",
            f"expected_start={run.expected_start_time}",
        )

    deployment_ids = {run.deployment_id for run in stale_runs}
    await _print_slot_state(deployment_ids)

    if not args.apply:
        print("Dry run only. Re-run with --apply to delete these flow runs.")
        return 0

    await _delete_runs(stale_runs)
    await _print_slot_state(deployment_ids)
    return 0


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()
    return asyncio.run(_main_async(args))


if __name__ == "__main__":
    raise SystemExit(main())
