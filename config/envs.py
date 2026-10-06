"""Paths to project virtual environments used by Prefect flows."""
from pathlib import Path

DOCS = Path("/home/tdm/Documents")

JOBS = {
    "venv_bin": str(DOCS / "jobs/.venv/bin"),
    "cwd": str(DOCS / "jobs"),
}

ETORO = {
    "venv_bin": str(DOCS / "etoro/.venv/bin"),
    "cwd": str(DOCS / "etoro"),
}

PREFECT = {
    "venv_bin": str(DOCS / "prefect/.venv/bin"),
    "cwd": str(DOCS / "prefect"),
}

SEKNOWLEDGEBANK_API = {
    "venv_bin": str(DOCS / "seknowledgebank/api/.venv/bin"),
    "cwd": str(DOCS / "seknowledgebank/api"),
}
