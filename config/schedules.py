"""All Prefect schedule definitions in one place."""

from prefect.client.schemas.schedules import CronSchedule

KENYA_TZ = "Africa/Nairobi"

# Run once every morning after most job boards have settled overnight.
JOBS_DAILY = CronSchedule(cron="15 7 * * *", timezone=KENYA_TZ)

# Prefect hygiene jobs.
WEEKLY_PREFECT_HOUSEKEEPING = CronSchedule(cron="30 2 * * *", timezone=KENYA_TZ)
DAILY_PREFECT_TEMP_CLEANUP = CronSchedule(cron="45 2 * * *", timezone=KENYA_TZ)

# SnapTrade holdings change intraday; transaction activities refresh daily.
ETORO_HOLDINGS_EVERY_FOUR_HOURS = CronSchedule(cron="17 */4 * * *", timezone="UTC")
ETORO_ACTIVITIES_DAILY = CronSchedule(cron="30 6 * * *", timezone="UTC")

# Google Alerts can arrive throughout the day; run after each six-hour block in JST.
BESS_MONITOR_EVERY_SIX_HOURS = CronSchedule(cron="25 */6 * * *", timezone="Asia/Tokyo")

# BESS discovery once a day, shortly before the 06:25 JST monitor run that extracts promoted pages.
BESS_DISCOVERY_DAILY = CronSchedule(cron="40 5 * * *", timezone="Asia/Tokyo")

# Re-score proposed and registered projects after the week's identity/research changes.
BESS_DUPLICATE_AUDIT_WEEKLY = CronSchedule(cron="0 3 * * 1", timezone="Asia/Tokyo")

# BESS project research every four hours (JST); the flow itself caps paid research starts per day.
BESS_RESEARCH_EVERY_FOUR_HOURS = CronSchedule(cron="50 */4 * * *", timezone="Asia/Tokyo")

# News page upkeep half an hour after each BESS monitor run (which creates news items).
BESS_NEWS_EVERY_SIX_HOURS = CronSchedule(cron="55 */6 * * *", timezone="Asia/Tokyo")

# JEPX market data at midnight JST (spot results for the coming day are published the previous morning).
JEPX_UPDATE_DAILY = CronSchedule(cron="0 0 * * *", timezone="Asia/Tokyo")

# HJKS outages shortly after midnight; the complete snapshot is content-hash idempotent.
HJKS_UPDATE_DAILY = CronSchedule(cron="20 0 * * *", timezone="Asia/Tokyo")

# Regional supply files after the completed operating day has propagated to every TSO.
TSO_SUPPLY_UPDATE_DAILY = CronSchedule(cron="15 2 * * *", timezone="Asia/Tokyo")

# EPRX prompt results change daily; final values arrive monthly and are picked up by the same run.
EPRX_UPDATE_DAILY = CronSchedule(cron="0 3 * * *", timezone="Asia/Tokyo")

# BESS simulation runs at 01:00 JST, after the JEPX import has brought in the new day's prices.
BESS_SIMULATION_DAILY = CronSchedule(cron="0 1 * * *", timezone="Asia/Tokyo")

# FIT/FIP register once a week, Saturday 00:00 JST; METI refreshes it about monthly.
FIT_FIP_UPDATE_WEEKLY = CronSchedule(cron="0 0 * * 6", timezone="Asia/Tokyo")

# Refresh flight candidates at midnight and noon Melbourne time.
FLIGHT_SEARCH_TWICE_DAILY = CronSchedule(cron="0 0,12 * * *", timezone="Australia/Melbourne")

# Grid-tariff documents once a week, Sunday 02:00 JST; tariffs change a few times a year.
GRID_TARIFFS_WEEKLY = CronSchedule(cron="0 2 * * 0", timezone="Asia/Tokyo")
