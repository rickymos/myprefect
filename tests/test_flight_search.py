import unittest

from config.schedules import FLIGHT_SEARCH_TWICE_DAILY
from flows.flight_search import _flight_search_task


class FlightSearchTests(unittest.TestCase):
    def test_schedule_runs_at_midnight_and_noon_melbourne(self):
        self.assertEqual(FLIGHT_SEARCH_TWICE_DAILY.cron, "0 0,12 * * *")
        self.assertEqual(FLIGHT_SEARCH_TWICE_DAILY.timezone, "Australia/Melbourne")

    def test_flow_task_is_defined(self):
        self.assertEqual(_flight_search_task.name, "flight-route-search")
