#!/usr/bin/env bash
set -euo pipefail

LOCK_FILE="/home/tdm/Documents/prefect/.prefect/cron-disk-cleanup.lock"
LOG_DIR="/home/tdm/Documents/prefect/.prefect/cleanup-logs"
LOG_FILE="$LOG_DIR/disk-cleanup.log"
PREFECT_PY="/home/tdm/Documents/prefect/.venv/bin/python"
PREFECT_ROOT="/home/tdm/Documents/prefect"

mkdir -p "$LOG_DIR"

exec 9>"$LOCK_FILE"
if ! flock -n 9; then
  exit 0
fi

rotate_log() {
  if [ -f "$LOG_FILE" ] && [ "$(wc -c < "$LOG_FILE")" -gt 5242880 ]; then
    mv "$LOG_FILE" "$LOG_FILE.$(date -u +%Y%m%dT%H%M%SZ)"
  fi
  find "$LOG_DIR" -type f -name 'disk-cleanup.log.*' -mtime +14 -delete
}

log() {
  printf '%s %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*"
}

usage_pct() {
  df -P / | awk 'NR==2 {gsub(/%/, "", $5); print $5}'
}

delete_path() {
  local target="$1"
  if [ -e "$target" ] || [ -L "$target" ]; then
    du -sh "$target" 2>/dev/null | sed 's/^/delete /' || true
    rm -rf -- "$target"
  fi
}

delete_children_older_than_days() {
  local dir="$1"
  local days="$2"
  [ -d "$dir" ] || return 0
  find "$dir" -mindepth 1 -maxdepth 1 -mtime +"$days" -print0 2>/dev/null |
    while IFS= read -r -d '' item; do
      delete_path "$item"
    done
}

cleanup_project_temp() {
  if [ -x "$PREFECT_PY" ]; then
    "$PREFECT_PY" "$PREFECT_ROOT/tools/cleanup_temp_files.py" \
      --older-than-hours 24 \
      --apply || true
  fi
}

cleanup_rebuildable_caches() {
  delete_children_older_than_days "/home/tdm/.cache/pip" 7
  delete_children_older_than_days "/home/tdm/.cache/pypoetry" 7
  delete_children_older_than_days "/home/tdm/.npm/_cacache" 7
  delete_children_older_than_days "/home/tdm/.npm/_npx" 3
  delete_children_older_than_days "/home/tdm/.vscode-server/data/CachedExtensionVSIXs" 7
  delete_children_older_than_days "/home/tdm/.codex/.tmp" 2
  delete_path "/home/tdm/Documents/seknowledgebank/web/.next/cache"
}

emergency_cleanup() {
  local pct="$1"
  if [ "$pct" -lt 90 ]; then
    return 0
  fi

  log "root usage ${pct}% >= 90%; running emergency cache cleanup"
  delete_path "/home/tdm/.cache/pip"
  delete_path "/home/tdm/.cache/pypoetry"
  delete_path "/home/tdm/.npm/_cacache"
  delete_path "/home/tdm/.vscode-server/data/CachedExtensionVSIXs"
}

main() {
  rotate_log
  {
    log "start root_usage=$(usage_pct)%"
    cleanup_project_temp
    cleanup_rebuildable_caches
    emergency_cleanup "$(usage_pct)"
    sync || true
    log "finish root_usage=$(usage_pct)%"
    df -h / 2>/dev/null || true
  } >> "$LOG_FILE" 2>&1
}

main "$@"
