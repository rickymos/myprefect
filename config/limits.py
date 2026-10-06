"""Run-time limits shared by every flow and deployment."""

# No flow run may take longer than this; Prefect fails it when the limit is reached.
MAX_RUN_SECONDS = 30 * 60

# CLI tasks stop their subprocess a minute earlier so it is terminated cleanly
# (process group killed) before the flow-level limit fires.
TASK_TIMEOUT_SECONDS = MAX_RUN_SECONDS - 60

# Anything still "Running" after this long has lost its process (e.g. worker restart)
# and is marked Crashed by tools/reap_overdue_runs.py.
OVERDUE_RUNNING_SECONDS = MAX_RUN_SECONDS + 5 * 60
