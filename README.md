# SeKnowledgeBank Prefect

Local Prefect orchestration for SeKnowledgeBank jobs.

## What Runs

- `job-watch/daily`: runs every day at `07:15 Africa/Nairobi`.
- `bess-japan-monitor/every-six-hours`: ingests Japanese Google Alerts, fetches sources, and extracts canonical projects every six hours (JST).
- `etoro-snaptrade-holdings/every-four-hours`: snapshots balances and open positions at minute 17 every four hours (UTC).
- `etoro-snaptrade-activities/daily`: ingests transactions, dividends, fees, and cash movements daily at `06:30 UTC`.
- Both eToro flows also have `manual` deployments for ad hoc runs.
- `job-watch/manual`: same flow without a schedule for ad hoc runs.
- `prefect-housekeeping/*`: Prefect DB/log cleanup helpers.
- `prefect-temp-cleanup/*`: conservative temp-file cleanup.

The daily job command is:

```bash
/home/tdm/Documents/jobs/.venv/bin/job-watch \
  --today <YYYY-MM-DD> \
  --limit 80 \
  --llm-review \
  --save-db \
  --save-policy relevant
```

## Access

- UI: `https://seknowledgebank.com/prefect/`
- API health: `https://seknowledgebank.com/prefect/api/health`
- Local API: `http://127.0.0.1:4200/prefect/api`

## Common Commands

```bash
cd /home/tdm/Documents/prefect

sudo systemctl status prefect-server.service prefect-worker.service --no-pager
sudo journalctl -u prefect-worker.service -f

./bin/prefect-local prefect deployment ls
./bin/prefect-local prefect deployment run 'job-watch/manual' \
  -p today='"2026-07-06"' \
  -p limit=80 \
  -p save_policy='"relevant"' \
  --watch

./bin/prefect-local prefect deployment run 'etoro-snaptrade-holdings/manual' --watch
./bin/prefect-local prefect deployment run 'bess-japan-monitor/manual' \
  -p gmail_limit=500 -p fetch_limit=100 -p process_limit=100 --watch
./bin/prefect-local prefect deployment run 'etoro-snaptrade-activities/manual' --watch

./bin/deploy-local
```

Bootstrap the read-only Gmail token in an interactive desktop session, then apply the enabled deployment:

```bash
cd /home/tdm/Documents/seknowledgebank/api
.venv/bin/python scripts/bess_monitor.py --gmail-limit 1 --skip-fetch --skip-extraction

cd /home/tdm/Documents/prefect
./bin/deploy-local
```

## Host Files

Prefect secrets and host-specific env live outside the repo:

- `/home/tdm/.config/prefect-server.env`
- `/home/tdm/.config/prefect-worker.env`

The server uses a dedicated local PostgreSQL database named `prefect`.
