import json
import os
import sys
import tempfile
import time
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
            old = collect.SLEEP
            if collect.SLEEP is time.sleep:
                collect.SLEEP = lambda s: None
            try:
                rc = collect.main({"OUTPUT": out}, sources)
            finally:
                collect.SLEEP = old
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
        keys |= {w[0] for w in collect.WORKLOADS} | {"prefect.scheduler", "prefect.components"}
        self.assertEqual(set(data["items"]), keys)
        self.assertEqual(len(data["items"]), 9)
        self.assertTrue(all(i["ok"] for i in data["items"].values()))
        self.assertEqual(len(data["flows"]), 2)
        self.assertEqual(data["alerts"], {"warning": 1, "critical": 0})

    def test_main_survives_failing_sources(self):
        src = collect.Sources(
            k8s_get=boom, prefect_post=boom, alerts_get=boom,
            previous_get=boom, check_url=boom)
        rc, text, data = self.run_main(src)
        self.assertEqual(rc, 0)
        self.assertEqual(len(data["items"]), 9)
        self.assertFalse(any(i["ok"] for i in data["items"].values()))
        self.assertIsNone(data["alerts"])
        self.assertEqual(data["flows"], [])
        self.assertNotIn("secret-host", text)

    def test_components_down_when_one_workload_is_down(self):
        def k8s(path):
            if path.endswith("/prefect-worker"):
                return {"spec": {"replicas": 1}, "status": {"readyReplicas": 0}}
            return ready(path)
        src = collect.Sources(
            k8s_get=k8s, prefect_post=prefect_post, alerts_get=lambda: [],
            previous_get=lambda: None, check_url=lambda u: True)
        _, _, data = self.run_main(src)
        comp = data["items"]["prefect.components"]
        self.assertEqual((comp["group"], comp["name"], comp["ok"]), ("prefect", "Components", False))
        self.assertTrue(data["items"]["prefect.scheduler"]["ok"])

    def test_malformed_previous_starts_fresh(self):
        src = collect.Sources(
            k8s_get=ready, prefect_post=prefect_post,
            alerts_get=lambda: [], previous_get=lambda: {"schema": 1, "history": 5},
            check_url=lambda u: True)
        rc, _, data = self.run_main(src)
        self.assertEqual(rc, 0)
        self.assertEqual(len(data["history"]), 9)

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

    def failing_post(self, fail_ids):
        def post(path, body):
            if path != "/deployments/filter":
                ids = body["flow_runs"]["deployment_id"]["any_"]
                if ids[0] in fail_ids:
                    raise OSError("secret-host:1234 refused")
            return prefect_post(path, body)
        return post

    def test_next_run_query_filters_future(self):
        bodies = []

        def post(path, body):
            bodies.append(body)
            return prefect_post(path, body)
        collect.collect_flows(post, NOW)
        sched = [b for b in bodies if "flow_runs" in b
                 and "SCHEDULED" in b["flow_runs"]["state"]["type"]["any_"]]
        self.assertEqual(len(sched), 2)
        for b in sched:
            self.assertEqual(b["flow_runs"]["expected_start_time"],
                             {"after_": "2026-11-26T12:00:00Z"})

    def test_one_failed_runs_query_keeps_other_flows(self):
        src = collect.Sources(
            k8s_get=ready, prefect_post=self.failing_post({"b"}),
            alerts_get=lambda: [], previous_get=lambda: None,
            check_url=lambda u: True)
        rc, text, data = self.run_main(src)
        flows = {f["name"]: f for f in data["flows"]}
        self.assertEqual(set(flows), {"hourly", "daily"})
        self.assertEqual(flows["hourly"]["last_state"], "ok")
        self.assertIsNotNone(flows["hourly"]["next_run"])
        bad = flows["daily"]
        self.assertFalse(bad["ok"])
        self.assertIsNone(bad["last_run"])
        self.assertIsNone(bad["next_run"])
        self.assertEqual(bad["last_state"], "none")
        self.assertTrue(data["items"]["prefect.scheduler"]["ok"])
        self.assertNotIn("secret-host", text)

    def test_all_failed_runs_queries_scheduler_down(self):
        src = collect.Sources(
            k8s_get=ready, prefect_post=self.failing_post({"a", "b"}),
            alerts_get=lambda: [], previous_get=lambda: None,
            check_url=lambda u: True)
        rc, text, data = self.run_main(src)
        self.assertEqual(len(data["flows"]), 2)
        self.assertFalse(data["items"]["prefect.scheduler"]["ok"])

    def test_previous_fetch_retried_once(self):
        prev = {"schema": 1, "history": {"web.api": {"2026-11-20": [3, 4]}}}
        calls = []

        def previous():
            calls.append(1)
            if len(calls) == 1:
                raise OSError("x")
            return prev
        src = collect.Sources(
            k8s_get=ready, prefect_post=prefect_post, alerts_get=lambda: [],
            previous_get=previous, check_url=lambda u: True)
        sleeps = []
        old = collect.SLEEP
        collect.SLEEP = sleeps.append
        try:
            rc, _, data = self.run_main(src)
        finally:
            collect.SLEEP = old
        self.assertEqual(len(calls), 2)
        self.assertEqual(sleeps, [5])
        self.assertEqual(data["history"]["web.api"]["2026-11-20"], [3, 4])

    def test_previous_fetch_fails_twice_starts_empty(self):
        src = collect.Sources(
            k8s_get=ready, prefect_post=prefect_post, alerts_get=lambda: [],
            previous_get=boom, check_url=lambda u: True)
        old = collect.SLEEP
        collect.SLEEP = lambda s: None
        try:
            rc, _, data = self.run_main(src)
        finally:
            collect.SLEEP = old
        self.assertEqual(rc, 0)
        self.assertEqual(len(data["history"]["web.api"]), 1)

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
