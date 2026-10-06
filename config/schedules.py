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
