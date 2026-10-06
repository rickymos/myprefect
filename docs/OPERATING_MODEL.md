# Prefect Operating Model

The intent is:

- keep each source repo in `Documents`
- keep one Poetry-managed `.venv` per repo checkout
- use Prefect for schedules, retries, visibility, and manual triggering
- keep complex per-workflow sequencing in Prefect flows rather than in ad hoc shell timers

## Repository Ownership

| Repo | Working directory | Runtime env | Notes |
| --- | --- | --- | --- |
| AEMO update | `/home/azureuser/Documents/aemo_update` | `/home/azureuser/Documents/aemo_update/.venv/bin` | Owns `aemo-update`, `aemo-update-larger`, `aemo-current`, `aemo-web` |
| Carwarp | `/home/azureuser/Documents/carwarp` | `/home/azureuser/Documents/carwarp/.venv/bin` | Owns `sftp`, `dispatch-log`, `processed-log` |
| Prefect | `/home/azureuser/Documents/prefect` | `/home/azureuser/Documents/prefect/.venv/bin` | Owns orchestration only |

## Callable Inventory

### AEMO repo

CLI entry points:

- `aemo-update`
- `aemo-update-larger`
- `aemo-current`
- `aemo-web`

Historical MMSDM domains:

- `constraints`
- `network`
- `static`
- `dispatch`
- `tradingprice`
- `bids`

Large MMSDM datasets:

- `dispatchconstraint`
- `dispatchload`
- `dispatchoffertrk`
- `biddayoffer`
- `bidperoffer_d`
- `biddayoffer_d`

Current/intraday jobs:

- `dispatch_unit_scada`
- `trading_price`
- `market_notices`

Web downloaders:

- `generation-information`
- `generating-unit-expected-closure`
- `key-connection-information`
- `congestion-information`
- `mlf-forward-looking`
- `transmission-augmentation-information`
- `co2eii`
- `nem-registration-list`
- `network-outage-schedule`
- `nem-generation-maps`

### Carwarp repo

CLI entry points:

- `sftp`
- `dispatch-log`
- `processed-log`

SFTP feed targets:

- `dispatch`
- `generation_summary`
- `met_average`
- `all`

Source containers:

- `carwarp`
- `mondo`

Operational modes:

- normal ingestion
- path discovery
- repair dry-run
- repair execute
- processed audit/log upload

## Scheduled vs Manual

### Implemented as scheduled Prefect deployments

| Deployment | Schedule | Command intent |
| --- | --- | --- |
| `aemo-current-dispatch-unit-scada/dispatch-unit-scada` | every 10 minutes at `:00, :10, :20, :30, :40, :50` | `aemo-current dispatch_unit_scada --incremental` |
| `aemo-current-trading-price/trading-price` | every 10 minutes at `:02, :12, :22, :32, :42, :52` | `aemo-current trading_price --incremental` |
| `aemo-update/every-3-days` | every 3 days | grouped MMSDM historical top-up |
| `aemo-update-larger/every-4-days` | every 4 days | split large-loader top-up with one deployment per dataset and one-hour staggering where two share a day |
| `carwarp-sftp-carwarp-ingest/carwarp-ingest` | every 10 minutes | `sftp --source-container carwarp --all` |
| `carwarp-daily-audit/daily` | daily | `processed-log --all-feeds --write-logs` |
| `carwarp-weekly-repair/weekly` | weekly | `sftp --source-container carwarp --all --repair-missing --dry-run` |
| `prefect-housekeeping/weekly` | weekly | prune old Prefect logs and vacuum the local SQLite DB |

### Implemented as scheduled web deployments

| Deployment family | Schedule |
| --- | --- |
| `aemo-web-network-outage-schedule/daily` | daily |
| `aemo-web-generation-information/daily` | weekly |
| `aemo-web-generating-unit-expected-closure/daily` | weekly |
| `aemo-web-key-connection-information/daily` | weekly |
| `aemo-web-mlf-forward-looking/daily` | weekly |
| `aemo-web-transmission-augmentation-information/daily` | weekly |
| `aemo-web-co2eii/daily` | weekly |
| `aemo-web-nem-registration-list/daily` | weekly |
| `aemo-web-nem-generation-maps/daily` | weekly |
| `aemo-web-congestion-information/daily` | monthly |

### Implemented but left manual for now

| Deployment | Reason |
| --- | --- |
| `aemo-current-market-notices/market-notices-manual` | keep manual until market notices backfill is complete |
| `carwarp-sftp-mondo-ingest/mondo-manual` | keep manual until mondo ingestion is implemented and path mapping is stable |

### Still intentionally manual outside the first cut

| Item | Reason |
| --- | --- |
| `aemo-update ... --from/--to` backfills | backfills should be deliberate and parameterized |
| `aemo-update ... --create` | onboarding/DDL only |
| `sftp --discover-paths` | troubleshooting and onboarding |
| `sftp --repair-missing` without `--dry-run` | recovery action, not normal schedule |
| single-feed ad hoc runs in either repo | operational troubleshooting |

## Implementation Notes

- Prefect flows call the installed console scripts directly from each repo’s `.venv/bin` directory.
- The source repos remain under `Documents`; no move to `/srv` is required.
- Existing systemd timers can continue to run in parallel during the migration window.
- Once Prefect runs are stable, disable the overlapping systemd timers family by family.
- Prefect itself stays bound to localhost; public access should go through nginx with basic auth.
- Heavy nightly AEMO batch tasks share the Prefect task tag `heavy-batch` with a concurrency limit of `1`, so `aemo-update` and `aemo-update-larger` cannot run their expensive loaders at the same time.
- The large-loader family now rotates on a 4-day cycle:
  `dispatchconstraint` at 1:30 AM and `dispatchload` at 2:30 AM on day 1,
  `dispatchoffertrk` at 1:30 AM on day 2,
  `biddayoffer` at 1:30 AM on day 3,
  `bidperoffer_d` at 1:30 AM and `biddayoffer_d` at 2:30 AM on day 4.

## Files Updated In This Pass

- `config/schedules.py`
- `deploy.py`
- `flows/aemo_current.py`
- `flows/aemo_large.py`
- `flows/aemo_web.py`
- `flows/carwarp.py`

## Next Steps

1. Run `poetry install` in `/home/azureuser/Documents/prefect` if needed.

## Useful Commands

- Clean stale never-started flow runs that can block deployment concurrency slots:
  `./bin/prefect-local cleanup-stale-slots`
- Apply the cleanup instead of doing a dry run:
  `./bin/prefect-local cleanup-stale-slots --apply`
- Limit cleanup to a specific deployment:
  `./bin/prefect-local cleanup-stale-slots --deployment carwarp-sftp-generation-summary-every-10-min`
- Show old Prefect logs eligible for deletion under the retention policy:
  completed = 14 days, completed `every-5-min` deployments = 5 days, failed/cancelled/crashed = 60 days:
  `./bin/prefect-local cleanup-old-logs`
- Delete eligible old logs and reclaim SQLite space:
  `./bin/prefect-local cleanup-old-logs --apply --vacuum`
2. Re-register deployments with `PREFECT_HOME=/home/azureuser/Documents/prefect/.prefect PREFECT_API_URL=http://127.0.0.1:4200/prefect/api .venv/bin/python deploy.py`.
3. Start by enabling Prefect in parallel with existing systemd timers.
4. Decide when the `mondo` ingestion should move from manual deployment to a scheduled deployment.

## TODO 1 We need to add variables so that I can change the variable when doing a manual run on the web.
## TODO Carwarp is having less logger infor and therefore have no IDEA what is happening. 
## TODO convert Carwarp to 20 minutes for the item 
