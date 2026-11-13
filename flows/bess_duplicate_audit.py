"""Weekly human-review audit for duplicate Japanese BESS projects.

Runs the Japan Market duplicate audit in that application's own environment. The audit
re-scores discovery proposals and reports suspected registry duplicates, but never merges
or archives projects.
"""
from __future__ import annotations

from prefect import flow

from config.envs import JAPANMARKET
from config.limits import MAX_RUN_SECONDS, TASK_TIMEOUT_SECONDS
from flows.shared.run_names import descriptive_run_name
from flows.shared.subprocess_task import make_cli_task


def _build_duplicate_audit_command(params: dict) -> list[str]:
    return [
        "python",
        "-m",
        "japanmarket.bess.duplicate_audit",
        "--min-score",
        str(float(params.get("min_score", 0.60))),
    ]


_bess_duplicate_audit_task = make_cli_task(
    venv_bin=JAPANMARKET["venv_bin"],
    cwd=JAPANMARKET["cwd"],
    task_name="bess-duplicate-audit",
    cmd_builder=_build_duplicate_audit_command,
    retries=0,
    timeout_seconds=TASK_TIMEOUT_SECONDS,
    skip_if_running_key="bess-duplicate-audit",
    skip_if_db_unavailable=True,
)


@flow(name="bess-duplicate-audit", flow_run_name=descriptive_run_name, log_prints=True,
      timeout_seconds=MAX_RUN_SECONDS)
def bess_duplicate_audit_flow(min_score: float = 0.60) -> None:
    """Publish suspected duplicates for review; never merge or archive projects."""
    _bess_duplicate_audit_task({"min_score": min_score})
