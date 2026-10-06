"""Prune stale project-owned temporary files.

This intentionally avoids a broad /tmp cleanup. Only explicit Prefect/job-watch
temp patterns are considered, and each candidate must be older than the
retention cutoff before it is removed.
"""
from __future__ import annotations

import argparse
import shutil
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path


DEFAULT_PATTERNS = (
    "/tmp/job-watch-*",
    "/tmp/tmp*prefect",
)
ALLOWED_ROOTS = (
    Path("/home/tdm/tmp"),
    Path("/tmp"),
)


@dataclass
class CleanupStats:
    candidates: int = 0
    skipped_new: int = 0
    skipped_outside_roots: int = 0
    deleted: int = 0
    errors: int = 0
    bytes_candidate: int = 0
    bytes_deleted: int = 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="prefect-local cleanup-temp-files",
        description=(
            "Delete stale project-owned temp files from explicit allow-listed "
            "patterns only. Dry-run by default."
        ),
    )
    parser.add_argument(
        "--older-than-hours",
        type=float,
        default=24.0,
        help="Only remove candidates older than this many hours. Default: 24.",
    )
    parser.add_argument(
        "--pattern",
        action="append",
        default=[],
        help=(
            "Additional glob pattern to clean. Must resolve under /tmp or "
            "/home/tdm/tmp. Can be provided multiple times."
        ),
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually delete candidates. Without this flag, run as dry-run.",
    )
    return parser


def _format_bytes(size: int) -> str:
    value = float(size)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if value < 1024.0 or unit == "TB":
            return f"{value:.1f}{unit}"
        value /= 1024.0
    return f"{size}B"


def _is_under_allowed_root(path: Path) -> bool:
    try:
        resolved = path.resolve(strict=False)
    except OSError:
        return False

    for root in ALLOWED_ROOTS:
        root_resolved = root.resolve(strict=False)
        try:
            resolved.relative_to(root_resolved)
        except ValueError:
            continue
        return resolved != root_resolved
    return False


def _path_size(path: Path) -> int:
    try:
        if path.is_file() or path.is_symlink():
            return path.lstat().st_size
        if path.is_dir():
            total = 0
            for child in path.rglob("*"):
                try:
                    if child.is_file() or child.is_symlink():
                        total += child.lstat().st_size
                except OSError:
                    continue
            return total
    except OSError:
        return 0
    return 0


def _newest_mtime(path: Path) -> float:
    """Return newest mtime inside a path.

    Directory mtimes alone can be misleading, so for directories we inspect
    children and skip deletion if anything inside is newer than the cutoff.
    """
    try:
        newest = path.lstat().st_mtime
    except OSError:
        return datetime.now(timezone.utc).timestamp()

    if path.is_dir() and not path.is_symlink():
        for child in path.rglob("*"):
            try:
                newest = max(newest, child.lstat().st_mtime)
            except OSError:
                continue
    return newest


def _iter_candidates(patterns: list[str]) -> list[Path]:
    seen: set[Path] = set()
    candidates: list[Path] = []
    for pattern in patterns:
        for path in Path("/").glob(pattern.lstrip("/")):
            try:
                key = path.resolve(strict=False)
            except OSError:
                key = path
            if key in seen:
                continue
            seen.add(key)
            candidates.append(path)
    return candidates


def _delete_path(path: Path) -> None:
    if path.is_dir() and not path.is_symlink():
        shutil.rmtree(path)
    else:
        path.unlink(missing_ok=True)


def cleanup_temp_files(*, patterns: list[str], older_than_hours: float, apply: bool) -> CleanupStats:
    cutoff = datetime.now(timezone.utc) - timedelta(hours=older_than_hours)
    cutoff_ts = cutoff.timestamp()
    stats = CleanupStats()

    print(
        "policy",
        f"older_than_hours={older_than_hours:g}",
        f"cutoff={cutoff.isoformat()}",
        f"mode={'apply' if apply else 'dry-run'}",
    )

    for path in _iter_candidates(patterns):
        if not _is_under_allowed_root(path):
            stats.skipped_outside_roots += 1
            print("skip_outside_roots", path)
            continue

        newest = _newest_mtime(path)
        if newest >= cutoff_ts:
            stats.skipped_new += 1
            continue

        size = _path_size(path)
        stats.candidates += 1
        stats.bytes_candidate += size
        print("candidate", f"size={_format_bytes(size)}", path)

        if not apply:
            continue

        try:
            _delete_path(path)
        except OSError as exc:
            stats.errors += 1
            print("error", path, exc)
            continue

        stats.deleted += 1
        stats.bytes_deleted += size
        print("deleted", f"size={_format_bytes(size)}", path)

    print(
        "summary",
        f"candidates={stats.candidates}",
        f"candidate_bytes={_format_bytes(stats.bytes_candidate)}",
        f"deleted={stats.deleted}",
        f"deleted_bytes={_format_bytes(stats.bytes_deleted)}",
        f"skipped_new={stats.skipped_new}",
        f"skipped_outside_roots={stats.skipped_outside_roots}",
        f"errors={stats.errors}",
    )
    if not apply:
        print("Dry run only. Re-run with --apply to delete these temp files.")
    return stats


def main() -> int:
    args = _build_parser().parse_args()
    patterns = list(DEFAULT_PATTERNS) + list(args.pattern)
    stats = cleanup_temp_files(
        patterns=patterns,
        older_than_hours=args.older_than_hours,
        apply=args.apply,
    )
    return 1 if stats.errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
