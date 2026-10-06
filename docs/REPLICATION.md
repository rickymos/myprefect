# Replicating This Prefect Orchestration Layer On A New VM

This document describes how to stand up **the orchestration layer only** —
Prefect server, worker, systemd services, nginx exposure, the `deploy.py`
registration pattern, and the housekeeping/cleanup tooling — on a fresh VM.

It intentionally excludes:
- The AEMO data-pipeline scripts (`aemo_update` repo).
- The Carwarp ingestion/settlement scripts (`carwarp` repo).
- Any business-logic flow bodies in `flows/*.py` that call into those repos.

Those live in their own repos and are not being copied as part of this
exercise (some of their scripts may be reused, but that is a separate,
later decision). What follows is the reusable "how Prefect is wired up"
pattern: folder layout, systemd units, nginx block, deployment registration
script, schedule config, concurrency-limit pattern, and the housekeeping
jobs that keep the Prefect DB and disk from filling up.

---

## 1. Architecture Overview

```
                        ┌─────────────────────────────┐
 nginx (basic auth) ───▶│ prefect server (systemd)     │  127.0.0.1:4200
   /prefect/            │  serves UI + API             │  UI base:  /prefect/
   /prefect/api/        │  backed by PostgreSQL         │  API base: /prefect/api
                        └───────────────┬─────────────┘
                                        │
                        ┌───────────────▼─────────────┐
                        │ prefect worker (systemd)     │
                        │  pool: default-agent-pool     │
                        │  type: process, limit 8       │
                        └───────────────┬─────────────┘
                                        │ subprocess.Popen
                        ┌───────────────▼─────────────┐
                        │ flows/*.py                   │
                        │  make_cli_task() wraps a CLI  │
                        │  binary from another repo's   │
                        │  own .venv/bin                │
                        └───────────────────────────────┘
```

Key design decisions worth preserving on the new VM:

- **One repo per data source, one Prefect repo for orchestration only.**
  Each source repo (e.g. `aemo_update`, `carwarp`) keeps its own
  Poetry/venv and its own installed CLI console-scripts. The Prefect repo
  never installs those dependencies itself — it just shells out to
  `<other_repo>/.venv/bin/<entrypoint>` as a subprocess.
- **Prefect stays bound to `127.0.0.1`.** Public access goes through nginx
  with HTTP basic auth. Prefect itself never listens on a public interface.
- **Prefect API/UI are served under a sub-path** (`/prefect/`, `/prefect/api/`)
  so it can share a host with other services instead of owning its own domain.
- **The local Prefect server uses PostgreSQL, not the default SQLite file**,
  because SQLite does not handle this run volume well under concurrent
  worker load. See §4.
- **Deployments are grouped into a small number of daily/weekly/manual
  buckets with staggered cron minutes** rather than one Prefect
  `IntervalSchedule` per job, to avoid many jobs firing in the same second
  and to make manual backfills a separate, un-scheduled deployment of the
  same flow.
- **A tag-based concurrency limit (`heavy-batch`, limit 1)** prevents two
  expensive nightly batch loaders from running at the same time, while a
  per-deployment concurrency limit (default 1) prevents a slow run from
  overlapping with its own next scheduled run. A small allow-list of
  high-frequency/lightweight deployments is exempted from the
  per-deployment limit (see `UNCAPPED_LIGHTWEIGHT_DEPLOYMENTS` in
  `deploy.py`).

---

## 2. What To Copy From This Repo

Copy the whole `prefect/` repo structure as the template, **except** the
flow *bodies* that are specific to AEMO/Carwarp business logic. Concretely:

| Path | Copy as-is? | Notes |
| --- | --- | --- |
| `pyproject.toml`, `poetry.toml`, `poetry.lock` | Yes | Only dependency is `prefect`. Update `poetry.lock` if you change the pin. |
| `deploy.py` | Adapt | Keep the pattern (`source_flow(...).to_deployment(...)`, concurrency-limit reconciliation, `require_local_prefect_environment` guard). Replace the actual deployment list with your new flows. |
| `config/envs.py` | Adapt | Keep the pattern of one dict per source repo with `venv_bin`/`cwd`. Replace the repo paths. |
| `config/schedules.py` | Adapt | Keep the pattern (one `CronSchedule`/`IntervalSchedule` constant per job, staggered minutes, AU_TZ constant). Replace the actual schedules. |
| `flows/shared/subprocess_task.py` | **Copy as-is** | Generic CLI-subprocess-as-Prefect-task wrapper. Not AEMO-specific. See §6. |
| `flows/shared/notifications.py` | **Copy as-is** | Generic SMTP failure-email helper. Not AEMO-specific. |
| `flows/housekeeping.py` | **Copy as-is** | Wraps the two `tools/*.py` cleanup scripts as flows. |
| `flows/aemo_*.py`, `flows/carwarp.py` | **Do not copy** | Business logic for the excluded repos. |
| `tools/cleanup_old_logs.py` | **Copy as-is** | Generic Postgres-backed Prefect history pruner. |
| `tools/cleanup_stale_slots.py` | **Copy as-is** | Generic stale-flow-run/concurrency-slot janitor, talks only to the Prefect API. |
| `tools/cleanup_temp_files.py` | Adapt | Generic allow-listed temp-file pruner; the `DEFAULT_PATTERNS` list is AEMO/Carwarp-specific and should be replaced with your own paths (or left empty). |
| `tools/cron_disk_cleanup.sh` | Adapt | Generic disk-usage janitor; strip the AEMO/Carwarp-specific path list and keep the structure (lock file, log rotation, emergency-cleanup-at-90%). |
| `bin/deploy-local` | **Copy as-is** | Thin wrapper that sets `PYTHONPATH`/`PREFECT_HOME`/`PREFECT_API_URL` and runs `deploy.py`. |
| `bin/prefect-local` | **Do not copy — see known issue in §9** | On this VM the file is broken (29 bytes, literal text, not a script). Rebuild it properly on the new VM; the intended contract is documented in §8. |
| `services/prefect-server.service` | **Copy as-is**, edit paths/user | systemd unit for the server. |
| `services/prefect-worker.service` | **Copy as-is**, edit paths/pool name | systemd unit for the worker. |
| `services/.prefect.env.example` | **Copy as-is** | Template for the non-secret env file. |
| `sitecustomize.py` | **Copy as-is** | Silences known noisy Prefect/Pydantic warnings. Only takes effect if `PYTHONPATH` includes the repo root (it does, via the systemd units and `bin/*` wrappers). |
| `.gitignore` | Copy as-is | Keeps `.prefect/` (local state/db) out of git. |

---

## 3. Host Prerequisites

```bash
sudo apt-get update
sudo apt-get install -y \
    python3 python3-venv python3-pip \
    nginx apache2-utils \
    postgresql postgresql-contrib \
    curl git

curl -sSL https://install.python-poetry.org | python3 -
export PATH="$HOME/.local/bin:$PATH"
poetry --version
```

Clone/place the repo:

```bash
mkdir -p /home/<user>/Documents
cd /home/<user>/Documents
git clone <new-repo-url> prefect   # or copy the template files in directly
cd /home/<user>/Documents/prefect
poetry install
./.venv/bin/prefect version
```

---

## 4. PostgreSQL Backend For The Prefect Server

The live system does **not** use Prefect's default local SQLite file. It
points the server at a dedicated PostgreSQL database via
`PREFECT_API_DATABASE_CONNECTION_URL`, set in an **out-of-repo** env file
loaded by the systemd unit:

```
/home/<user>/.config/prefect-server.env   (mode 600, owned by the service user)
```

Content pattern (replace with a freshly generated password — do not reuse
any password from another environment):

```
PREFECT_API_DATABASE_CONNECTION_URL=postgresql+asyncpg://<db_user>:<db_password>@127.0.0.1:5432/<db_name>
```

Create the database and role once:

```bash
sudo -u postgres psql -c "CREATE ROLE prefect WITH LOGIN PASSWORD '<generate-a-strong-password>';"
sudo -u postgres psql -c "CREATE DATABASE prefect OWNER prefect;"
```

Why Postgres instead of the SQLite default: this deployment runs enough
concurrent flow/task runs (many 5–10 minute schedules plus manual runs)
that SQLite's single-writer lock becomes a bottleneck and a source of
"database is locked" errors. Point at Postgres from day one on a new VM
rather than migrating later.

`tools/cleanup_old_logs.py` (see §7) talks to this same Postgres database
directly via `asyncpg`, bypassing the Prefect API, so that history pruning
can run in batches without loading the API server.

---

## 5. Repo-Local Prefect Home And Env Files

```bash
mkdir -p /home/<user>/Documents/prefect/.prefect
cp services/.prefect.env.example services/.prefect.env
```

Edit `services/.prefect.env`:

```
PREFECT_HOME=/home/<user>/Documents/prefect/.prefect
PYTHONPATH=/home/<user>/Documents/prefect
PREFECT_UI_SERVE_BASE=/prefect/
PREFECT_SERVER_API_BASE_PATH=/prefect/api
PREFECT_API_URL=http://127.0.0.1:4200/prefect/api
PREFECT_UI_API_URL=https://<your-hostname>/prefect/api
PREFECT_UI_URL=https://<your-hostname>/prefect
PREFECT_WORK_POOL_NAME=default-agent-pool
```

This file is loaded by both systemd units and is safe to commit as a
`.example` (no secrets). The real `services/.prefect.env` should stay out
of git if it ever contains anything host-specific/sensitive — on this VM
it doesn't, but treat it as host config, not shared template.

Secrets and per-host config that must **not** live in the repo, ever:

```
/home/<user>/.config/prefect-server.env   # PREFECT_API_DATABASE_CONNECTION_URL (has a DB password)
/home/<user>/.config/prefect-worker.env   # any cloud creds a flow's subprocess needs (e.g. AZURE_CLIENT_ID/TENANT_ID/CLIENT_SECRET)
```

Create them with restrictive permissions:

```bash
install -m 600 /dev/null /home/<user>/.config/prefect-server.env
install -m 600 /dev/null /home/<user>/.config/prefect-worker.env
```

Optional failure-email settings (consumed by `flows/shared/notifications.py`,
put in `prefect-worker.env` since that's the process that actually runs
tasks):

```
PREFECT_FAILURE_EMAIL_TO=ops@example.com,alerts@example.com
PREFECT_FAILURE_EMAIL_FROM=prefect@example.com
PREFECT_FAILURE_SMTP_HOST=smtp.example.com
PREFECT_FAILURE_SMTP_PORT=587
PREFECT_FAILURE_SMTP_USERNAME=prefect@example.com
PREFECT_FAILURE_SMTP_PASSWORD=...
PREFECT_FAILURE_SMTP_USE_TLS=true
```

---

## 6. systemd Services

Copy the two unit files, editing `User=`, `WorkingDirectory=`, and every
absolute path for the new host/user:

```bash
sudo cp services/prefect-server.service /etc/systemd/system/
sudo cp services/prefect-worker.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now prefect-server.service
sudo systemctl enable --now prefect-worker.service
```

Server unit highlights (`services/prefect-server.service`):
- Runs `prefect server start --host 127.0.0.1 --port 4200`.
- Sets `PREFECT_UI_SERVE_BASE=/prefect/`, `PREFECT_SERVER_API_BASE_PATH=/prefect/api`
  so the UI/API are served under a sub-path (matches the nginx block in §8).
- Loads `services/.prefect.env` and `~/.config/prefect-server.env` as
  optional (`EnvironmentFile=-...`) so a missing file doesn't block startup.
- `Restart=on-failure`, `RestartSec=30`.

Worker unit highlights (`services/prefect-worker.service`):
- `Requires=`/`After=prefect-server.service`.
- `ExecStartPre` polls `http://127.0.0.1:4200/prefect/api/health` for up to
  60 seconds before starting, so the worker doesn't crash-loop while the
  server is still coming up.
- Runs `prefect worker start --pool default-agent-pool --type process --limit 8`.
  `--limit 8` caps concurrent subprocess-backed flow runs on this host —
  size it to the VM's CPU/IO budget, not to the number of deployments.

**The `default-agent-pool` work pool is not created automatically by
`deploy.py`.** Create it once, before the first `deploy.py` run:

```bash
cd /home/<user>/Documents/prefect
./.venv/bin/prefect work-pool create --type process default-agent-pool
```

(This step was missing from the original `SETUP.md` on this VM — the pool
already existed here from an earlier manual step. Don't skip it on a fresh
host or `deploy.py`/the worker will have nothing to attach to.)

---

## 7. Housekeeping / Disk And DB Hygiene

Two independent layers keep this system healthy long-term. Replicate both.

### 7a. Prefect-native housekeeping flows (`flows/housekeeping.py`)

Two scheduled deployments, registered like any other flow in `deploy.py`:
- `prefect-housekeeping-daily` → runs `tools/cleanup_old_logs.py --apply`
  (default retention baked into the script, see below).
- `prefect-temp-cleanup-daily` → runs
  `tools/cleanup_temp_files.py --apply --older-than-hours 24`.

Both also have a `-manual` deployment (no schedule) for on-demand runs from
the UI.

### 7b. Host crontab (outside Prefect entirely)

This exists so cleanup still happens even if the Prefect server itself is
down or the DB has grown large enough to make the flow-based path slow to
start. Reproduce with `crontab -e` for the service user:

```cron
# prefect tools cron cleanup start
# Keep user-owned temp/cache files from filling root even if Prefect is down.
17 * * * * /home/<user>/Documents/prefect/tools/cron_disk_cleanup.sh
# Prune old Prefect DB history independently of Prefect schedules. No vacuum here; commit and exit.
37 2 * * * /usr/bin/flock -n /home/<user>/Documents/prefect/.prefect/cleanup-old-logs.lock /bin/bash -lc 'set -a; . /home/<user>/.config/prefect-server.env; set +a; /home/<user>/Documents/prefect/.venv/bin/python /home/<user>/Documents/prefect/tools/cleanup_old_logs.py --completed-days 2 --terminal-days 7 --stale-never-started-days 2 --events-days 7 --batch-size 5000 --apply >> /home/<user>/Documents/prefect/.prefect/cleanup-logs/old-logs-cleanup.log 2>&1'
# prefect tools cron cleanup end
```

Notes:
- `tools/cron_disk_cleanup.sh` uses a `flock` on
  `.prefect/cron-disk-cleanup.lock` so overlapping cron fires no-op instead
  of piling up. It rotates its own log past 5MB and prunes rotated logs
  older than 14 days.
- The retention numbers passed on the crontab line (`--completed-days 2
  --terminal-days 7 --stale-never-started-days 2`) are **tighter** than the
  script's own defaults (2/7/7). Both the cron invocation and the Prefect
  flow's default-args invocation are running the same script with
  different arguments — this is intentional belt-and-suspenders, not a
  bug, but worth being aware of if you tune one and expect it to affect
  the other.
- `tools/cleanup_stale_slots.py` (dry-run by default, `--apply` to act) is
  not currently scheduled anywhere on this VM — it's invoked manually via
  `./bin/prefect-local cleanup-stale-slots` when a deployment's
  concurrency slot gets stuck behind a never-started run. Decide on the
  new VM whether to leave it manual or wire it into cron/Prefect too.

### 7c. What `cleanup_old_logs.py` actually assumes

It connects **directly to PostgreSQL** via `asyncpg` (not through the
Prefect API), resolving the connection string in this order: `--db-url` CLI
flag → `PREFECT_API_DATABASE_CONNECTION_URL` env var → parsed out of
`~/.config/prefect-server.env`. This only works because the server uses
Postgres (§4) — if you ever run this against a SQLite-backed server it will
fail to find a usable URL. It deletes in FK-safe order (logs → flow_run,
which cascades to task_run/state tables → events/event_resources), commits
in batches (`--batch-size`, default 10,000) so the server stays responsive,
and supports `--vacuum` to run `VACUUM ANALYZE` afterward (non-locking,
safe to run live).

---

## 8. nginx Reverse Proxy

There is no nginx template file in this repo (`SETUP.md` refers to one at
`services/nginx/prefect.nginx.conf.tmpl` that does not actually exist on
this VM — that's a documentation gap in the current setup, not something to
replicate). In practice the Prefect block lives directly inside the
existing site config (here, `/etc/nginx/sites-available/recenplex`,
alongside other unrelated proxied services). On the new VM, add an
equivalent block to whatever site file you're serving Prefect from:

```nginx
location /prefect/ {
    auth_basic "Prefect";
    auth_basic_user_file /etc/nginx/.htpasswd-prefect;
    proxy_pass http://127.0.0.1:4200;   # note: no trailing slash
    proxy_http_version 1.1;
    proxy_set_header Host $host;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    proxy_set_header X-Forwarded-Proto $scheme;
    proxy_set_header X-Forwarded-Host $host;
    proxy_set_header X-Forwarded-Prefix /prefect;
    proxy_set_header Upgrade $http_upgrade;
    proxy_set_header Connection $http_connection;
    proxy_read_timeout 3600;
}

location /prefect/api/ {
    auth_basic "Prefect";
    auth_basic_user_file /etc/nginx/.htpasswd-prefect;
    proxy_pass http://127.0.0.1:4200/prefect/api/;
    proxy_http_version 1.1;
    proxy_set_header Host $host;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    proxy_set_header X-Forwarded-Proto $scheme;
    proxy_set_header Upgrade $http_upgrade;
    proxy_set_header Connection $http_connection;
    proxy_read_timeout 3600;
}
```

Set up basic auth and reload:

```bash
sudo htpasswd -c /etc/nginx/.htpasswd-prefect <prefect_username>
sudo nginx -t
sudo systemctl reload nginx
```

Smoke check:

```bash
curl -I http://127.0.0.1:4200/prefect/api/health
curl -I https://<your-hostname>/prefect/
```

Optional TLS via Certbot if this host has public DNS:

```bash
sudo apt-get install -y certbot python3-certbot-nginx
sudo certbot --nginx -d <your-hostname>
systemctl status certbot.timer --no-pager
```

---

## 9. The `deploy.py` Pattern

`deploy.py` is the single place deployments get registered. The pattern to
replicate for a new set of flows:

1. **`source_flow(entrypoint)`** loads a flow via
   `Flow.from_source(source=PREFECT_ROOT, entrypoint="flows/x.py:flow_fn")`
   so the worker (which runs `--type process`) can import it fresh per run
   rather than requiring a baked container image.
2. **`.to_deployment(name=..., schedule=..., parameters=..., concurrency_limit=...)`**
   — one call per deployment. The same flow function is frequently
   registered twice: once with a `schedule=` for the scheduled cadence, and
   once with no schedule and a name like `*-manual` or `backfill-range` for
   ad hoc/parameterized runs from the UI.
3. **`require_local_prefect_environment()`** is a fail-fast guard at the
   top of `if __name__ == "__main__":` that refuses to run unless
   `PREFECT_API_URL`/`PREFECT_HOME` exactly match the expected local
   values. This exists to stop `deploy.py` from being run against the
   wrong Prefect instance by accident (e.g. a stray global env var pointing
   at a different server). Keep this guard on the new VM — cheap insurance
   against silently registering deployments to the wrong place.
4. **Tag-based concurrency**: `ensure_tag_concurrency_limit(tag, limit)` is
   called once at deploy time to create/update a Prefect task-tag
   concurrency limit (used here for `"heavy-batch"` → 1, so two expensive
   loaders tagged `heavy-batch` never run simultaneously regardless of
   which deployment triggered them).
5. **Per-deployment concurrency**: every `to_deployment(...)` call defaults
   effectively to `concurrency_limit=1` unless the deployment name is in
   `UNCAPPED_LIGHTWEIGHT_DEPLOYMENTS` (fast, high-frequency, safe-to-overlap
   jobs). `reconcile_deployment_global_limit()` runs after `.apply()` and
   actively **deletes** the auto-created global concurrency limit for any
   deployment on that allow-list, since Prefect 3 creates one by default
   whenever `concurrency_limit` is set on `to_deployment`.
6. Run it via the wrapper so the env guard always passes:
   ```bash
   ./bin/deploy-local
   ```
   or manually:
   ```bash
   PREFECT_HOME=/home/<user>/Documents/prefect/.prefect \
   PREFECT_API_URL=http://127.0.0.1:4200/prefect/api \
   PYTHONPATH=/home/<user>/Documents/prefect \
   .venv/bin/python deploy.py
   ```

Verify:

```bash
./.venv/bin/prefect deployment ls
./.venv/bin/prefect work-pool inspect default-agent-pool
```

---

## 10. `bin/prefect-local` — Known Issue On This VM

`OPERATING_MODEL.md` documents a `./bin/prefect-local cleanup-stale-slots`
and `./bin/prefect-local cleanup-old-logs` interface. On this VM,
**`bin/prefect-local` is broken** — it is a 29-byte plain-text file
containing the literal string `work-pool: command not found`, not an
executable script. It is not currently callable.

Do not copy this file as-is to the new VM. Rebuild it following the same
pattern as `bin/deploy-local` (set `PYTHONPATH`/`PREFECT_HOME`/`PREFECT_API_URL`,
then dispatch to the right `tools/*.py` script based on the first CLI
argument, e.g. `cleanup-stale-slots` → `tools/cleanup_stale_slots.py`,
`cleanup-old-logs` → `tools/cleanup_old_logs.py`). Until then, on this VM,
call the tools directly:

```bash
cd /home/<user>/Documents/prefect
PREFECT_HOME=.prefect PREFECT_API_URL=http://127.0.0.1:4200/prefect/api \
  .venv/bin/python tools/cleanup_stale_slots.py --older-than-minutes 10

PREFECT_HOME=.prefect PREFECT_API_URL=http://127.0.0.1:4200/prefect/api \
  .venv/bin/python tools/cleanup_old_logs.py --apply --vacuum
```

---

## 11. Day-0 Smoke Tests

Run these before treating the new VM as live:

```bash
cd /home/<user>/Documents/prefect
systemctl status prefect-server.service prefect-worker.service --no-pager
./.venv/bin/prefect work-pool inspect default-agent-pool
./.venv/bin/prefect deployment ls
./.venv/bin/prefect deployment run '<some-flow>/<some-deployment>'
journalctl -u prefect-worker.service -f   # watch the run get picked up
```

---

## 12. Operating Commands Reference

| Task | Command |
| --- | --- |
| Check services | `systemctl status prefect-server.service prefect-worker.service` |
| Tail server log | `journalctl -u prefect-server.service -f` |
| Tail worker log | `journalctl -u prefect-worker.service -f` |
| List deployments | `./.venv/bin/prefect deployment ls` |
| Inspect pool | `./.venv/bin/prefect work-pool inspect default-agent-pool` |
| Trigger deployment | `./.venv/bin/prefect deployment run 'FLOW/DEPLOYMENT'` |
| Re-register deployments | `./bin/deploy-local` |
| Reload nginx | `sudo nginx -t && sudo systemctl reload nginx` |
| Restart worker after code/env change | `sudo systemctl restart prefect-worker.service` |
| Dry-run stale concurrency-slot cleanup | `.venv/bin/python tools/cleanup_stale_slots.py` |
| Apply stale concurrency-slot cleanup | `.venv/bin/python tools/cleanup_stale_slots.py --apply` |
| Dry-run old-history cleanup | `.venv/bin/python tools/cleanup_old_logs.py` |
| Apply old-history cleanup + vacuum | `.venv/bin/python tools/cleanup_old_logs.py --apply --vacuum` |

---

## 13. Security Note From This Audit

While preparing this guide, `~/.config/prefect-server.env` on this VM was
found to contain a live PostgreSQL connection string with a plaintext
password, and `~/.config/prefect-worker.env` contains an Azure client/tenant
ID. Both files are correctly kept out of the git repo (mode 600, outside
`Documents/prefect`), which is the right pattern — **keep doing this on the
new VM**: generate a fresh, unique DB password there rather than reusing
this one, and never let either file be copied into the repo or into this
document.
