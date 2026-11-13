"""Grid-tariff documents for the BESS simulation (japanmarket.grid_tariffs).

Runs ``python -m japanmarket.grid_tariffs.cli`` in the japanmarket environment: ``collect`` (new documents linked
from the T&D companies' and METI's pages, copied to Cloudflare R2), ``upload`` (anything not in R2 yet), ``redate``
(effective dates from the PDF cover; R2 copies renamed to their dated folder) and ``extract`` (page text and
candidate pages for review). Rates are never entered automatically. Pages already seen
are skipped, so a weekly run takes a few minutes; ``recheck`` downloads every known URL again to spot changed files.
"""
from __future__ import annotations

from prefect import flow

from config.envs import JAPANMARKET
from config.limits import MAX_RUN_SECONDS, TASK_TIMEOUT_SECONDS
from flows.shared.run_names import descriptive_run_name
from flows.shared.subprocess_task import make_cli_task

# collect / upload / redate / extract share the run's limit.
_STEP_TIMEOUT = TASK_TIMEOUT_SECONDS // 4


def _collect_command(params: dict) -> list[str]:
    command = ["python", "-m", "japanmarket.grid_tariffs.cli", "collect"]
    for source in params.get("sources") or []:
        command += ["--source", str(source)]
    if params.get("recheck"):
        command.append("--recheck")
    return command


def _task(name: str, builder) -> callable:
    return make_cli_task(venv_bin=JAPANMARKET["venv_bin"], cwd=JAPANMARKET["cwd"], task_name=f"grid-tariffs-{name}",
                         cmd_builder=builder, retries=0, timeout_seconds=_STEP_TIMEOUT,
                         skip_if_running_key=f"grid-tariffs-{name}")


_collect = _task("collect", _collect_command)
_upload = _task("upload", lambda params: ["python", "-m", "japanmarket.grid_tariffs.cli", "upload"])
_redate = _task("redate", lambda params: ["python", "-m", "japanmarket.grid_tariffs.cli", "redate"])
_extract = _task("extract", lambda params: ["python", "-m", "japanmarket.grid_tariffs.cli", "extract"])


@flow(name="grid-tariffs", flow_run_name=descriptive_run_name, log_prints=True, timeout_seconds=MAX_RUN_SECONDS)
def grid_tariffs_flow(sources: list[str] | None = None, recheck: bool = False) -> None:
    """Collect new tariff documents, copy them to R2 and extract candidate pages; a failed source fails the run."""
    params = {"sources": sources or [], "recheck": recheck}
    try:
        _collect(params)
    finally:
        # Upload and extract what was collected even when a source failed.
        _upload(params)
        _redate(params)
        _extract(params)
