"""
Factory that produces a Prefect @task wrapping a CLI command from a specific venv.

Usage:
    from flows.shared.subprocess_task import make_cli_task

    my_task = make_cli_task(
        venv_bin="/path/to/.venv/bin",
        cmd=["job-watch", "--save-db"],
        cwd="/path/to/project",
        task_name="job-watch",
    )

    @flow
    def my_flow():
        my_task()
"""
from __future__ import annotations

import os
import selectors
import signal
import subprocess
import time
import fcntl
from pathlib import Path
from typing import Any, Callable, Iterable, Sequence

from prefect import get_run_logger, task
from prefect.runtime import flow_run, task_run

from flows.shared.notifications import send_failure_email

CommandBuilder = Callable[[dict[str, Any]], list[str]]


def _terminate_process_group(
    process: subprocess.Popen[str],
    *,
    grace_seconds: int = 15,
) -> int:
    """Terminate a CLI process and every child in its process group."""
    returncode = process.poll()
    if returncode is not None:
        return returncode

    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        pass

    try:
        return process.wait(timeout=grace_seconds)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        return process.wait()


def make_cli_task(
    venv_bin: str,
    cwd: str,
    task_name: str,
    cmd: Sequence[str] | None = None,
    retries: int = 1,
    retry_delay_seconds: int = 900,
    timeout_seconds: int | None = 1800,
    tags: Iterable[str] | None = None,
    cmd_builder: CommandBuilder | None = None,
    skip_if_running_key: str | None = None,
    skip_if_db_unavailable: bool = False,
) -> callable:
    """
    Return a Prefect @task that runs a CLI command using the binary in ``venv_bin``.

    Either provide a static ``cmd`` or a ``cmd_builder`` that derives the CLI
    arguments from flow-provided parameters at runtime.

    Raises RuntimeError (which Prefect surfaces as a FAILED task) when the
    subprocess exits with a non-zero return code.

    Retry policy intentionally stays minimal for operational safety:
    one delayed retry only, then fail.
    """
    if (cmd is None) == (cmd_builder is None):
        raise ValueError("Provide exactly one of cmd or cmd_builder")

    def _lock_name(value: str) -> str:
        return "".join(ch if ch.isalnum() or ch in {"-", "_", "."} else "_" for ch in value)

    def _db_is_available(*, exe: str, cwd: str, env: dict[str, str], task_name: str) -> tuple[bool, str]:
        probe = r"""
from __future__ import annotations
import sys
import psycopg
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    PGURL: str | None = None
    pg_url: str | None = None
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

settings = Settings()
dsn = settings.PGURL or settings.pg_url
if not dsn:
    print("no PGURL configured")
    sys.exit(0)
try:
    with psycopg.connect(dsn, connect_timeout=5, application_name="prefect-db-preflight"):
        pass
except Exception as exc:
    print(str(exc))
    sys.exit(2)
"""
        python = str(Path(exe).with_name("python"))
        try:
            completed = subprocess.run(
                [python, "-c", probe],
                cwd=cwd,
                env=env,
                text=True,
                capture_output=True,
                timeout=10,
            )
        except Exception as exc:
            return False, f"DB preflight failed to run for {task_name}: {exc}"

        detail = (completed.stdout or completed.stderr or "").strip()
        if completed.returncode == 0:
            return True, detail or "ok"
        return False, detail or f"preflight exited with code {completed.returncode}"

    @task(
        name=task_name,
        retries=retries,
        retry_delay_seconds=retry_delay_seconds,
        timeout_seconds=timeout_seconds,
        tags=tags,
    )
    def _task(cli_kwargs: dict[str, Any] | None = None) -> None:
        params = cli_kwargs or {}
        resolved_cmd = list(cmd_builder(params) if cmd_builder is not None else cmd)
        if not resolved_cmd:
            raise RuntimeError(f"Task '{task_name}' resolved to an empty command")

        exe = str(Path(venv_bin) / resolved_cmd[0])
        full_cmd = [exe] + resolved_cmd[1:]
        logger = get_run_logger()
        logger.info("Running: %s", " ".join(full_cmd))
        env = os.environ.copy()
        env.setdefault("PYTHONUNBUFFERED", "1")
        if env.get("DEBUG", "").strip().lower() not in {"", "0", "1", "true", "false", "yes", "no", "on", "off"}:
            env["DEBUG"] = "false"

        lock_file = None
        if skip_if_running_key:
            lock_dir = Path(os.getenv("PREFECT_CLI_LOCK_DIR", "/tmp/prefect_cli_locks"))
            lock_dir.mkdir(parents=True, exist_ok=True)
            lock_path = lock_dir / f"{_lock_name(skip_if_running_key)}.lock"
            lock_file = open(lock_path, "w", encoding="utf-8")
            try:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                lock_file.write(
                    f"pid={os.getpid()} task={task_name} command={' '.join(full_cmd)}\n"
                )
                lock_file.flush()
            except BlockingIOError:
                logger.warning(
                    "Skipping %s because another copy is still running (lock=%s)",
                    task_name,
                    lock_path,
                )
                lock_file.close()
                return

        if skip_if_db_unavailable:
            ok, detail = _db_is_available(exe=exe, cwd=cwd, env=env, task_name=task_name)
            if not ok:
                logger.warning("Skipping %s because Postgres is unavailable: %s", task_name, detail)
                if lock_file is not None:
                    lock_file.close()
                return

        def _notify_failure(reason: str) -> None:
            try:
                sent = send_failure_email(
                    subject=f"Prefect task failed: {task_name}",
                    body=(
                        f"Reason: {reason}\n"
                        f"Task: {task_name}\n"
                        f"Flow run: {getattr(flow_run, 'name', None) or getattr(flow_run, 'id', 'unknown')}\n"
                        f"Task run: {getattr(task_run, 'name', None) or getattr(task_run, 'id', 'unknown')}\n"
                        f"Working directory: {cwd}\n"
                        f"Command: {' '.join(full_cmd)}\n"
                    ),
                )
                if sent:
                    logger.warning("Failure email sent for %s", task_name)
            except Exception as exc:
                logger.warning("Failed to send failure email for %s: %s", task_name, exc)

        try:
            with subprocess.Popen(
                full_cmd,
                cwd=cwd,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                start_new_session=True,
            ) as process:
                assert process.stdout is not None
                deadline = time.monotonic() + timeout_seconds if timeout_seconds else None
                selector = selectors.DefaultSelector()
                selector.register(process.stdout, selectors.EVENT_READ)

                try:
                    while True:
                        if deadline is not None and time.monotonic() >= deadline:
                            _terminate_process_group(process)
                            reason = f"Command exceeded {timeout_seconds} seconds and was terminated"
                            _notify_failure(reason)
                            raise TimeoutError(reason)

                        events = selector.select(timeout=1)
                        if events:
                            line = process.stdout.readline()
                            if line:
                                message = line.rstrip()
                                if message:
                                    logger.info("%s", message)
                                continue

                        returncode = process.poll()
                        if returncode is not None:
                            while True:
                                line = process.stdout.readline()
                                if not line:
                                    break
                                message = line.rstrip()
                                if message:
                                    logger.info("%s", message)
                            break
                finally:
                    selector.close()
                    if process.poll() is None:
                        logger.warning(
                            "Terminating %s because the task exited before the command completed",
                            task_name,
                        )
                        _terminate_process_group(process)

            if returncode != 0:
                _notify_failure(f"Command exited with code {returncode}")
                raise RuntimeError(
                    f"Command '{resolved_cmd[0]}' exited with code {returncode}"
                )
        finally:
            if lock_file is not None:
                lock_file.close()

    # Give the inner function the task name so it's readable in tracebacks
    _task.__name__ = task_name
    return _task
