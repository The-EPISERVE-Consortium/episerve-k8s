import json
import os
import sys
import tempfile
import unittest
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import collect  # noqa: E402

NOW = datetime(2026, 11, 26, 12, 0, tzinfo=timezone.utc)


def ready(path):
    return {"spec": {"replicas": 1}, "status": {"readyReplicas": 1}}


def boom(*a, **k):
    raise OSError("secret-host:1234 refused")


def deployments():
    return [
        {"id": "a", "name": "hourly", "paused": False,
         "schedules": [{"active": True, "schedule": {"cron": "5 * * * *"}}]},
        {"id": "b", "name": "daily", "paused": False,
         "schedules": [{"active": True, "schedule": {"cron": "0 3 * * *"}}]},
    ]


def prefect_post(path, body):
    if path == "/deployments/filter":
        return deployments()
    ids = body["flow_runs"]["deployment_id"]["any_"]
    states = body["flow_runs"]["state"]["type"]["any_"]
    if "SCHEDULED" in states:
        return [{"expected_start_time": "2026-11-26T13:05:00Z"}]
    return [{"start_time": "2026-11-26T11:30:00Z", "state_type": "COMPLETED",
             "state": {"type": "COMPLETED"}}]


class MainTest(unittest.TestCase):
    def run_main(self, sources):
        with tempfile.TemporaryDirectory() as d:
            out = os.path.join(d, "status.json")
            rc = collect.main({"OUTPUT": out}, sources)
            with open(out) as f:
                text = f.read()
        return rc, text, json.loads(text)

    def test_main_writes_complete_status(self):
        src = collect.Sources(
            k8s_get=ready, prefect_post=prefect_post,
            alerts_get=lambda: [{"labels": {"severity": "warning"}}],
            previous_get=lambda: None, check_url=lambda u: True)
        rc, _, data = self.run_main(src)
        self.assertEqual(rc, 0)
        keys = {k for k, *_ in collect.PUBLIC_SERVICES}
        keys |= {w[0] for w in collect.WORKLOADS} | {"prefect.scheduler"}
        self.assertEqual(set(data["items"]), keys)
        self.assertEqual(len(data["items"]), 18)
        self.assertTrue(all(i["ok"] for i in data["items"].values()))
        self.assertEqual(len(data["flows"]), 2)
        self.assertEqual(data["alerts"], {"warning": 1, "critical": 0})

    def test_main_survives_failing_sources(self):
        src = collect.Sources(
            k8s_get=boom, prefect_post=boom, alerts_get=boom,
            previous_get=boom, check_url=boom)
        rc, text, data = self.run_main(src)
        self.assertEqual(rc, 0)
        self.assertEqual(len(data["items"]), 18)
        self.assertFalse(any(i["ok"] for i in data["items"].values()))
        self.assertIsNone(data["alerts"])
        self.assertEqual(data["flows"], [])
        self.assertNotIn("secret-host", text)

    def test_malformed_previous_starts_fresh(self):
        src = collect.Sources(
            k8s_get=ready, prefect_post=prefect_post,
            alerts_get=lambda: [], previous_get=lambda: {"schema": 1, "history": 5},
            check_url=lambda u: True)
        rc, _, data = self.run_main(src)
        self.assertEqual(rc, 0)
        self.assertEqual(len(data["history"]), 18)

    def test_collect_flows_skips_paused_and_unscheduled(self):
        def post(path, body):
            if path == "/deployments/filter":
                return [
                    {"id": "m", "name": "manual", "paused": False, "schedules": []},
                    {"id": "x", "name": "x", "paused": True,
                     "schedules": [{"active": True, "schedule": {"cron": "0 * * * *"}}]},
                    {"id": "y", "name": "y", "paused": False,
                     "schedules": [{"active": False, "schedule": {"cron": "0 * * * *"}}]},
                    {"id": "a", "name": "ok", "paused": False,
                     "schedules": [{"active": True, "schedule": {"cron": "0 * * * *"}}]},
                ]
            return prefect_post(path, body)
        flows = collect.collect_flows(post, NOW)
        self.assertEqual([f["name"] for f in flows], ["ok"])

    def test_check_url_retries_once(self):
        calls = []

        class Resp:
            status = 200
            def __enter__(self): return self
            def __exit__(self, *a): return False

        def opener(url, timeout=None):
            calls.append(url)
            if len(calls) == 1:
                raise OSError("x")
            return Resp()

        old = collect.SLEEP
        collect.SLEEP = lambda s: None
        try:
            self.assertTrue(collect.check_url("https://x", opener))
        finally:
            collect.SLEEP = old
        self.assertEqual(len(calls), 2)


if __name__ == "__main__":
    unittest.main()
