"""Japanese BESS project discovery flow (official datasets, press feeds, TSO queue figures).

Runs ``scripts/bess_discovery.py run`` in seknowledgebank's API environment. Only sources whose
crawl policy is 'allowed' are polled; each source has its own poll interval, so a daily run only
fetches what is due. Promoted candidates become articles that the next bess-japan-monitor run
fetches and extracts.
"""
from __future__ import annotations

from prefect import flow

from config.envs import SEKNOWLEDGEBANK_API
from config.limits import MAX_RUN_SECONDS, TASK_TIMEOUT_SECONDS
from flows.shared.subprocess_task import make_cli_task


def _build_discovery_command(params: dict) -> list[str]:
    command = ["python", "scripts/bess_discovery.py", "run", "--promote", str(int(params.get("promote") or 0))]
    for source in params.get("sources") or []:
        command += ["--source", str(source)]
    if params.get("force"):
        command.append("--force")
    if params.get("all_pages"):
        command += ["--all-pages", "--max-pages", str(int(params.get("max_pages") or 50))]
    return command


_bess_discovery_task = make_cli_task(
    venv_bin=SEKNOWLEDGEBANK_API["venv_bin"],
    cwd=SEKNOWLEDGEBANK_API["cwd"],
    task_name="bess-discovery",
    cmd_builder=_build_discovery_command,
    retries=0,
    timeout_seconds=TASK_TIMEOUT_SECONDS,
    skip_if_running_key="bess-discovery",
)


@flow(name="bess-discovery", log_prints=True, timeout_seconds=MAX_RUN_SECONDS)
def bess_discovery_flow(
    promote: int = 20,
    sources: list[str] | None = None,
    force: bool = False,
    all_pages: bool = False,
    max_pages: int = 50,
) -> None:
    """Poll allowed discovery sources and queue up to ``promote`` new candidates for extraction.

    ``all_pages`` follows every page of each source (full history, e.g. all PR TIMES releases).
    """
    _bess_discovery_task({"promote": promote, "sources": sources or [], "force": force,
                          "all_pages": all_pages, "max_pages": max_pages})
