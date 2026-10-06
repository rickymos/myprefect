"""Clean up old Prefect flow run, task run, log, and event rows from PostgreSQL.

Deletion order ensures FK integrity:
  1. Logs linked to eligible flow_runs and their task_runs (log has no CASCADE from flow_run).
  2. flow_run rows — cascades task_run → task_run_state and flow_run_state automatically.
  3. event_resources then events (by occurred cutoff).
"""
from __future__ import annotations

import argparse
import asyncio
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import asyncpg
from pathlib import Path


DB_URL_ENV = "PREFECT_API_DATABASE_CONNECTION_URL"
_SERVER_ENV_FILE = Path.home() / ".config/prefect-server.env"

COMPLETED_STATES = ("COMPLETED",)
TERMINAL_STATES = ("FAILED", "CANCELLED", "CRASHED")
STALE_NEVER_STARTED_STATES = ("SCHEDULED", "PENDING", "CANCELLING")


@dataclass
class CleanupPlan:
    completed_flow_runs: int = 0
    terminal_flow_runs: int = 0
    stale_never_started_flow_runs: int = 0
    flow_logs: int = 0
    task_logs: int = 0
    orphan_logs: int = 0
    events: int = 0
    event_resources: int = 0

    @property
    def run_total(self) -> int:
        return (
            self.completed_flow_runs
            + self.terminal_flow_runs
            + self.stale_never_started_flow_runs
        )

    @property
    def log_total(self) -> int:
        return self.flow_logs + self.task_logs + self.orphan_logs


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="prefect-local cleanup-old-logs",
        description=(
            "Delete old Prefect history using retention-by-state rules against PostgreSQL. "
            "Successful completed runs are kept for 2 days by default. "
            "Failed/cancelled/crashed runs are kept for 7 days by default. "
            "Old never-started scheduled/pending/cancelling runs are kept for 7 days. "
            "Deletions are committed in batches so the Prefect server remains responsive. "
            "Deleting flow_run rows automatically cascades to task_run, "
            "flow_run_state, and task_run_state via PostgreSQL FK constraints."
        ),
    )
    parser.add_argument(
        "--completed-days",
        type=int,
        default=2,
        help="Retention in days for successful completed runs/logs. Default: 2.",
    )
    parser.add_argument(
        "--terminal-days",
        type=int,
        default=7,
        help="Retention in days for failed/cancelled/crashed runs/logs. Default: 7.",
    )
    parser.add_argument(
        "--stale-never-started-days",
        type=int,
        default=7,
        help="Retention in days for scheduled/pending/cancelling runs that never started. Default: 7.",
    )
    parser.add_argument(
        "--events-days",
        type=int,
        default=7,
        help="Retention in days for events and event_resources rows. Default: 7.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=10_000,
        help="Rows deleted per commit. Smaller = more server-friendly. Default: 10000.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Delete matching rows. Without this flag, the command is a dry run.",
    )
    parser.add_argument(
        "--vacuum",
        "--vacuum-analyze",
        action="store_true",
        dest="vacuum",
        help=(
            "Run VACUUM ANALYZE on affected tables after deletion. "
            "Safe to run while the Prefect server is active — "
            "PostgreSQL VACUUM ANALYZE does not hold an exclusive lock."
        ),
    )
    parser.add_argument(
        "--db-url",
        default=None,
        help=(
            f"PostgreSQL connection URL. Defaults to the {DB_URL_ENV} environment variable. "
            "Both postgresql+asyncpg:// and plain postgresql:// forms are accepted."
        ),
    )
    return parser


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _cutoff(days: int) -> datetime:
    return _utc_now() - timedelta(days=days)


def _pg_dsn(url: str) -> str:
    """Strip the SQLAlchemy dialect prefix so asyncpg can use the URL directly."""
    return url.replace("postgresql+asyncpg://", "postgresql://")


def _resolve_db_url(cli_url: str | None) -> str | None:
    """Return the first DB URL found: CLI arg > env var > server env file."""
    if cli_url:
        return cli_url
    url = os.environ.get(DB_URL_ENV)
    if url:
        return url
    if _SERVER_ENV_FILE.exists():
        for line in _SERVER_ENV_FILE.read_text().splitlines():
            line = line.strip()
            if line.startswith(f"{DB_URL_ENV}="):
                return line.split("=", 1)[1].strip()
    return None


def _in_clause(values: tuple[str, ...]) -> str:
    return ", ".join(f"'{v}'" for v in values)


async def _count(conn: asyncpg.Connection, sql: str, *args: object) -> int:
    row = await conn.fetchrow(sql, *args)
    return int(row[0])


async def _batched_delete(
    conn: asyncpg.Connection,
    sql: str,
    args: tuple,
    batch_size: int,
    label: str,
) -> int:
    """Run a DELETE ... WHERE id IN (... LIMIT $N) loop until no rows remain."""
    total = 0
    while True:
        result = await conn.execute(sql, *args, batch_size)
        n = int(result.split()[-1])
        total += n
        if n > 0:
            print(f"  {label} batch={n} total_so_far={total}", flush=True)
        if n < batch_size:
            break
    return total


async def _eligible_counts(
    conn: asyncpg.Connection,
    *,
    completed_cutoff: datetime,
    terminal_cutoff: datetime,
    stale_never_started_cutoff: datetime,
    events_cutoff: datetime,
) -> CleanupPlan:
    c = _in_clause(COMPLETED_STATES)
    t = _in_clause(TERMINAL_STATES)
    s = _in_clause(STALE_NEVER_STARTED_STATES)

    plan = CleanupPlan(
        completed_flow_runs=await _count(
            conn,
            f"SELECT COUNT(*) FROM flow_run WHERE state_type::text IN ({c}) AND end_time IS NOT NULL AND end_time < $1",
            completed_cutoff,
        ),
        terminal_flow_runs=await _count(
            conn,
            f"SELECT COUNT(*) FROM flow_run WHERE state_type::text IN ({t}) AND COALESCE(end_time, state_timestamp, updated, created) < $1",
            terminal_cutoff,
        ),
        stale_never_started_flow_runs=await _count(
            conn,
            f"SELECT COUNT(*) FROM flow_run WHERE state_type::text IN ({s}) AND start_time IS NULL AND COALESCE(expected_start_time, state_timestamp, updated, created) < $1",
            stale_never_started_cutoff,
        ),
        flow_logs=await _count(
            conn,
            f"""
            SELECT COUNT(*) FROM log
            WHERE flow_run_id IN (
                SELECT id FROM flow_run
                WHERE state_type::text IN ({c}) AND end_time IS NOT NULL AND end_time < $1
                UNION ALL
                SELECT id FROM flow_run
                WHERE state_type::text IN ({t}) AND COALESCE(end_time, state_timestamp, updated, created) < $2
                UNION ALL
                SELECT id FROM flow_run
                WHERE state_type::text IN ({s}) AND start_time IS NULL AND COALESCE(expected_start_time, state_timestamp, updated, created) < $3
            )
            """,
            completed_cutoff,
            terminal_cutoff,
            stale_never_started_cutoff,
        ),
        task_logs=await _count(
            conn,
            f"""
            SELECT COUNT(*) FROM log
            WHERE task_run_id IN (
                SELECT tr.id FROM task_run tr
                JOIN flow_run fr ON tr.flow_run_id = fr.id
                WHERE fr.state_type::text IN ({c}) AND fr.end_time IS NOT NULL AND fr.end_time < $1
                UNION ALL
                SELECT tr.id FROM task_run tr
                JOIN flow_run fr ON tr.flow_run_id = fr.id
                WHERE fr.state_type::text IN ({t}) AND COALESCE(fr.end_time, fr.state_timestamp, fr.updated, fr.created) < $2
                UNION ALL
                SELECT tr.id FROM task_run tr
                JOIN flow_run fr ON tr.flow_run_id = fr.id
                WHERE fr.state_type::text IN ({s}) AND fr.start_time IS NULL AND COALESCE(fr.expected_start_time, fr.state_timestamp, fr.updated, fr.created) < $3
            )
            """,
            completed_cutoff,
            terminal_cutoff,
            stale_never_started_cutoff,
        ),
        orphan_logs=await _count(
            conn,
            """
            SELECT COUNT(*) FROM log
            WHERE timestamp < $1
              AND (flow_run_id IS NULL OR flow_run_id NOT IN (SELECT id FROM flow_run))
              AND (task_run_id IS NULL OR task_run_id NOT IN (SELECT id FROM task_run))
            """,
            terminal_cutoff,
        ),
        events=await _count(
            conn,
            "SELECT COUNT(*) FROM events WHERE occurred < $1",
            events_cutoff,
        ),
        event_resources=await _count(
            conn,
            "SELECT COUNT(*) FROM event_resources WHERE occurred < $1",
            events_cutoff,
        ),
    )
    return plan


async def _delete_rows(
    conn: asyncpg.Connection,
    *,
    completed_cutoff: datetime,
    terminal_cutoff: datetime,
    stale_never_started_cutoff: datetime,
    events_cutoff: datetime,
    batch_size: int,
) -> CleanupPlan:
    deleted = CleanupPlan()
    c = _in_clause(COMPLETED_STATES)
    t = _in_clause(TERMINAL_STATES)
    s = _in_clause(STALE_NEVER_STARTED_STATES)

    # --- Logs: delete BEFORE flow_run rows (log has no CASCADE from flow_run) ---

    deleted.flow_logs += await _batched_delete(
        conn,
        f"""
        DELETE FROM log WHERE id IN (
            SELECT log.id FROM log
            JOIN flow_run ON log.flow_run_id = flow_run.id
            WHERE flow_run.state_type::text IN ({c})
              AND flow_run.end_time IS NOT NULL
              AND flow_run.end_time < $1
            LIMIT $2
        )
        """,
        (completed_cutoff,),
        batch_size,
        "flow_logs[completed]",
    )

    deleted.flow_logs += await _batched_delete(
        conn,
        f"""
        DELETE FROM log WHERE id IN (
            SELECT log.id FROM log
            JOIN flow_run ON log.flow_run_id = flow_run.id
            WHERE flow_run.state_type::text IN ({t})
              AND COALESCE(flow_run.end_time, flow_run.state_timestamp, flow_run.updated, flow_run.created) < $1
            LIMIT $2
        )
        """,
        (terminal_cutoff,),
        batch_size,
        "flow_logs[terminal]",
    )

    deleted.flow_logs += await _batched_delete(
        conn,
        f"""
        DELETE FROM log WHERE id IN (
            SELECT log.id FROM log
            JOIN flow_run ON log.flow_run_id = flow_run.id
            WHERE flow_run.state_type::text IN ({s})
              AND flow_run.start_time IS NULL
              AND COALESCE(flow_run.expected_start_time, flow_run.state_timestamp, flow_run.updated, flow_run.created) < $1
            LIMIT $2
        )
        """,
        (stale_never_started_cutoff,),
        batch_size,
        "flow_logs[stale]",
    )

    deleted.task_logs += await _batched_delete(
        conn,
        f"""
        DELETE FROM log WHERE id IN (
            SELECT log.id FROM log
            JOIN task_run ON log.task_run_id = task_run.id
            JOIN flow_run ON task_run.flow_run_id = flow_run.id
            WHERE flow_run.state_type::text IN ({c})
              AND flow_run.end_time IS NOT NULL
              AND flow_run.end_time < $1
            LIMIT $2
        )
        """,
        (completed_cutoff,),
        batch_size,
        "task_logs[completed]",
    )

    deleted.task_logs += await _batched_delete(
        conn,
        f"""
        DELETE FROM log WHERE id IN (
            SELECT log.id FROM log
            JOIN task_run ON log.task_run_id = task_run.id
            JOIN flow_run ON task_run.flow_run_id = flow_run.id
            WHERE flow_run.state_type::text IN ({t})
              AND COALESCE(flow_run.end_time, flow_run.state_timestamp, flow_run.updated, flow_run.created) < $1
            LIMIT $2
        )
        """,
        (terminal_cutoff,),
        batch_size,
        "task_logs[terminal]",
    )

    deleted.task_logs += await _batched_delete(
        conn,
        f"""
        DELETE FROM log WHERE id IN (
            SELECT log.id FROM log
            JOIN task_run ON log.task_run_id = task_run.id
            JOIN flow_run ON task_run.flow_run_id = flow_run.id
            WHERE flow_run.state_type::text IN ({s})
              AND flow_run.start_time IS NULL
              AND COALESCE(flow_run.expected_start_time, flow_run.state_timestamp, flow_run.updated, flow_run.created) < $1
            LIMIT $2
        )
        """,
        (stale_never_started_cutoff,),
        batch_size,
        "task_logs[stale]",
    )

    deleted.orphan_logs += await _batched_delete(
        conn,
        """
        DELETE FROM log WHERE id IN (
            SELECT id FROM log
            WHERE timestamp < $1
              AND (flow_run_id IS NULL OR flow_run_id NOT IN (SELECT id FROM flow_run))
              AND (task_run_id IS NULL OR task_run_id NOT IN (SELECT id FROM task_run))
            LIMIT $2
        )
        """,
        (terminal_cutoff,),
        batch_size,
        "orphan_logs",
    )

    # --- flow_run: cascades task_run → task_run_state and flow_run_state ---

    deleted.completed_flow_runs = await _batched_delete(
        conn,
        f"""
        DELETE FROM flow_run WHERE id IN (
            SELECT id FROM flow_run
            WHERE state_type::text IN ({c})
              AND end_time IS NOT NULL
              AND end_time < $1
            LIMIT $2
        )
        """,
        (completed_cutoff,),
        batch_size,
        "completed_flow_runs",
    )

    deleted.terminal_flow_runs = await _batched_delete(
        conn,
        f"""
        DELETE FROM flow_run WHERE id IN (
            SELECT id FROM flow_run
            WHERE state_type::text IN ({t})
              AND COALESCE(end_time, state_timestamp, updated, created) < $1
            LIMIT $2
        )
        """,
        (terminal_cutoff,),
        batch_size,
        "terminal_flow_runs",
    )

    deleted.stale_never_started_flow_runs = await _batched_delete(
        conn,
        f"""
        DELETE FROM flow_run WHERE id IN (
            SELECT id FROM flow_run
            WHERE state_type::text IN ({s})
              AND start_time IS NULL
              AND COALESCE(expected_start_time, state_timestamp, updated, created) < $1
            LIMIT $2
        )
        """,
        (stale_never_started_cutoff,),
        batch_size,
        "stale_never_started_flow_runs",
    )

    # --- Events ---

    deleted.event_resources = await _batched_delete(
        conn,
        """
        DELETE FROM event_resources WHERE id IN (
            SELECT id FROM event_resources WHERE occurred < $1 LIMIT $2
        )
        """,
        (events_cutoff,),
        batch_size,
        "event_resources",
    )

    deleted.events = await _batched_delete(
        conn,
        """
        DELETE FROM events WHERE id IN (
            SELECT id FROM events WHERE occurred < $1 LIMIT $2
        )
        """,
        (events_cutoff,),
        batch_size,
        "events",
    )

    return deleted


def _print_plan(prefix: str, plan: CleanupPlan) -> None:
    print(
        prefix,
        f"completed_flow_runs={plan.completed_flow_runs}",
        f"terminal_flow_runs={plan.terminal_flow_runs}",
        f"stale_never_started_flow_runs={plan.stale_never_started_flow_runs}",
        f"flow_logs={plan.flow_logs}",
        f"task_logs={plan.task_logs}",
        f"orphan_logs={plan.orphan_logs}",
        f"events={plan.events}",
        f"event_resources={plan.event_resources}",
        f"run_total={plan.run_total}",
        f"log_total={plan.log_total}",
    )


async def _async_main() -> int:
    parser = _build_parser()
    args = parser.parse_args()

    url = _resolve_db_url(args.db_url)
    if not url:
        print(
            f"ERROR: no database URL provided. "
            f"Set {DB_URL_ENV} or pass --db-url.",
            flush=True,
        )
        return 1

    dsn = _pg_dsn(url)
    completed_cutoff = _cutoff(args.completed_days)
    terminal_cutoff = _cutoff(args.terminal_days)
    stale_cutoff = _cutoff(args.stale_never_started_days)
    events_cutoff = _cutoff(args.events_days)

    conn = await asyncpg.connect(dsn=dsn, timeout=60)
    try:
        plan = await _eligible_counts(
            conn,
            completed_cutoff=completed_cutoff,
            terminal_cutoff=terminal_cutoff,
            stale_never_started_cutoff=stale_cutoff,
            events_cutoff=events_cutoff,
        )
        print(
            "policy",
            f"completed_cutoff={completed_cutoff.isoformat()}",
            f"terminal_cutoff={terminal_cutoff.isoformat()}",
            f"stale_never_started_cutoff={stale_cutoff.isoformat()}",
            f"events_cutoff={events_cutoff.isoformat()}",
            f"batch_size={args.batch_size}",
        )
        _print_plan("candidates", plan)

        if not args.apply:
            print("Dry run only. Re-run with --apply to delete these old rows.")
            return 0

        deleted = await _delete_rows(
            conn,
            completed_cutoff=completed_cutoff,
            terminal_cutoff=terminal_cutoff,
            stale_never_started_cutoff=stale_cutoff,
            events_cutoff=events_cutoff,
            batch_size=args.batch_size,
        )
        _print_plan("deleted", deleted)

        if args.vacuum:
            tables = (
                "log",
                "flow_run",
                "task_run",
                "flow_run_state",
                "task_run_state",
                "events",
                "event_resources",
            )
            print(
                "Running VACUUM ANALYZE on affected tables "
                "(non-blocking — safe while Prefect server is active).",
                flush=True,
            )
            for table in tables:
                print(f"  VACUUM ANALYZE {table} ...", flush=True)
                await conn.execute(f"VACUUM ANALYZE {table}")
            print("VACUUM ANALYZE complete.", flush=True)

    finally:
        await conn.close()

    return 0


def main() -> int:
    return asyncio.run(_async_main())


if __name__ == "__main__":
    raise SystemExit(main())
