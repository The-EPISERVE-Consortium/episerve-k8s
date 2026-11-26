import os
import sys
import unittest
from datetime import date, datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import collect  # noqa: E402

NOW = datetime(2026, 11, 26, 12, 0, 0, tzinfo=timezone.utc)


def flow(cron="20 * * * *", ago=timedelta(minutes=30), state="COMPLETED",
         nxt=NOW + timedelta(minutes=30), name="x"):
    last = None if ago is None else NOW - ago
    return collect.flow_entry(name, cron, last, state, nxt, NOW)


class WorkloadTests(unittest.TestCase):
    def test_workload_ready_all_replicas(self):
        self.assertTrue(collect.workload_ready(
            {"spec": {"replicas": 2}, "status": {"readyReplicas": 2}}))

    def test_workload_not_ready_partial(self):
        self.assertFalse(collect.workload_ready(
            {"spec": {"replicas": 2}, "status": {"readyReplicas": 1}}))

    def test_workload_scaled_to_zero_is_down(self):
        self.assertFalse(collect.workload_ready(
            {"spec": {"replicas": 0}, "status": {"readyReplicas": 0}}))

    def test_workload_missing_ready_field(self):
        self.assertFalse(collect.workload_ready(
            {"spec": {"replicas": 1}, "status": {}}))


class CronTests(unittest.TestCase):
    def test_cron_interval_hourly(self):
        self.assertEqual(collect.cron_interval("20 * * * *"), timedelta(hours=1))

    def test_cron_interval_daily(self):
        self.assertEqual(collect.cron_interval("0 1 * * *"), timedelta(hours=24))

    def test_cron_interval_weekly(self):
        self.assertEqual(collect.cron_interval("0 3 * * 1"), timedelta(days=7))

    def test_cron_interval_unknown_falls_back_to_day(self):
        self.assertEqual(collect.cron_interval("*/15 * * * *"), timedelta(hours=24))
        self.assertEqual(collect.cron_interval("0 1 1 * *"), timedelta(hours=24))


class FlowEntryTests(unittest.TestCase):
    def test_flow_entry_on_time(self):
        e = flow(ago=timedelta(hours=1, minutes=59))
        self.assertTrue(e["ok"])
        self.assertEqual(e["last_state"], "ok")

    def test_flow_entry_late(self):
        self.assertFalse(flow(ago=timedelta(hours=2, minutes=1))["ok"])

    def test_flow_entry_failed(self):
        e = flow(state="FAILED")
        self.assertEqual(e["last_state"], "failed")
        self.assertFalse(e["ok"])

    def test_flow_entry_no_next_run(self):
        e = flow(nxt=None)
        self.assertIsNone(e["next_run"])
        self.assertFalse(e["ok"])

    def test_flow_entry_never_ran(self):
        e = flow(ago=None, state=None)
        self.assertEqual(e["last_state"], "none")
        self.assertIsNone(e["last_run"])
        self.assertFalse(e["ok"])

    def test_flow_entry_download_group(self):
        self.assertEqual(flow(name="download__grippeweb")["group"], "downloads")
        self.assertIsNone(flow(name="sync")["group"])

    def test_flow_entry_iso_format(self):
        e = flow()
        self.assertTrue(e["last_run"].endswith("Z"))
        self.assertTrue(e["next_run"].endswith("Z"))

    def test_scheduler_ok(self):
        self.assertTrue(collect.scheduler_ok([flow(), flow()]))

    def test_scheduler_down_when_no_flows(self):
        self.assertFalse(collect.scheduler_ok([]))

    def test_scheduler_down_when_any_flow_has_no_next_run(self):
        self.assertFalse(collect.scheduler_ok([flow(), flow(nxt=None)]))


class AlertTests(unittest.TestCase):
    def test_count_alerts_ignores_other_severities(self):
        def a(sev):
            return {"labels": {"severity": sev}}
        alerts = [a("none"), a("none"), a("warning"), a("warning"), a("critical")]
        self.assertEqual(collect.count_alerts(alerts),
                         {"warning": 2, "critical": 1})


class HistoryTests(unittest.TestCase):
    today = date(2026, 11, 26)

    def test_merge_history_adds_sample(self):
        prev = {"schema": 1, "history": {"web.api": {"2026-11-26": [3, 4]}}}
        h = collect.merge_history(prev, {"web.api": {"ok": True}}, self.today)
        self.assertEqual(h["web.api"]["2026-11-26"], [4, 5])

    def test_merge_history_adds_failed_sample(self):
        prev = {"schema": 1, "history": {"web.api": {"2026-11-26": [3, 4]}}}
        h = collect.merge_history(prev, {"web.api": {"ok": False}}, self.today)
        self.assertEqual(h["web.api"]["2026-11-26"], [3, 5])

    def test_merge_history_drops_old_days(self):
        prev = {"schema": 1, "history": {"web.api": {
            "2026-10-27": [1, 1], "2026-10-28": [1, 1]}}}
        h = collect.merge_history(prev, {"web.api": {"ok": True}}, self.today)
        self.assertNotIn("2026-10-27", h["web.api"])
        self.assertIn("2026-10-28", h["web.api"])

    def test_merge_history_without_previous(self):
        h = collect.merge_history(None, {"web.api": {"ok": False}}, self.today)
        self.assertEqual(h, {"web.api": {"2026-11-26": [0, 1]}})

    def test_merge_history_ignores_other_schema(self):
        prev = {"schema": 2, "history": {"web.api": {"2026-11-26": [3, 4]}}}
        h = collect.merge_history(prev, {"web.api": {"ok": True}}, self.today)
        self.assertEqual(h, {"web.api": {"2026-11-26": [1, 1]}})


class BuildStatusTests(unittest.TestCase):
    def test_build_status_shape(self):
        s = collect.build_status(NOW, {"web.api": {"ok": True}}, [], None, None)
        self.assertEqual(set(s), {"schema", "generated_at", "alerts", "items",
                                  "flows", "history"})
        self.assertEqual(s["schema"], 1)
        self.assertTrue(s["generated_at"].endswith("Z"))
        self.assertIsNone(s["alerts"])
        self.assertEqual(s["history"]["web.api"], {"2026-11-26": [1, 1]})


if __name__ == "__main__":
    unittest.main()
