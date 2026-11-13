from datetime import datetime, timezone
import unittest
from uuid import UUID

from flows.shared.run_names import format_run_name


class RunNameTests(unittest.TestCase):
    def test_identifies_flow_deployment_time_and_run(self):
        name = format_run_name(
            flow_name="BESS Japan Monitor",
            deployment_name="every-six-hours",
            scheduled_start_time=datetime(2026, 10, 22, 3, 25, tzinfo=timezone.utc),
            run_id=UUID("01a16079-2503-7150-b642-ef2712345678"),
        )

        self.assertEqual(
            name,
            "bess-japan-monitor__every-six-hours__20261022-0325__12345678",
        )

    def test_labels_non_deployment_runs_as_ad_hoc(self):
        name = format_run_name(
            flow_name="JEPX Update",
            deployment_name=None,
            scheduled_start_time=datetime(2026, 10, 22, 9, 5),
            run_id="run-1234",
        )

        self.assertEqual(name, "jepx-update__ad-hoc__20261022-0905__run1234")
