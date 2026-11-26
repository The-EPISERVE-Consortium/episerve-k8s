"""Hourly status collector for the EPISERVE status page (stdlib only)."""
import json
import os
import ssl
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Callable, Optional

PUBLIC_SERVICES = [
    ("web.website", "EPISERVE website", "https://episerve.zib.de/"),
    ("web.datahub", "Data hub", "https://data.episerve.zib.de/"),
    ("web.api", "API server", "https://api.episerve.zib.de/health"),
    ("web.doip", "DOIP server", "https://doip.episerve.zib.de/"),
]

# (key, group, name, namespace, kind, workload)
WORKLOADS = [
    ("prefect.server", "prefect", "Server", "default", "deployments", "prefect-server"),
    ("prefect.worker", "prefect", "Worker", "default", "deployments", "prefect-worker"),
    ("prefect.database", "prefect", "Database", "default", "statefulsets", "prefect-postgresql"),
    ("pods.api", "pods", "API server", "default", "deployments", "episerve-api-server"),
    ("pods.doip", "pods", "DOIP server", "default", "deployments", "doip-server"),
    ("pods.ckan", "pods", "CKAN", "ckan", "deployments", "ckan"),
    ("pods.ckan_worker_default", "pods", "CKAN worker (default)", "ckan", "deployments", "ckan-worker-default"),
    ("pods.ckan_worker_priority", "pods", "CKAN worker (priority)", "ckan", "deployments", "ckan-worker-priority"),
    ("pods.ckan_worker_bulk", "pods", "CKAN worker (bulk)", "ckan", "deployments", "ckan-worker-bulk"),
    ("pods.solr", "pods", "Solr", "ckan", "statefulsets", "solr"),
    ("pods.ckan_db", "pods", "CKAN database", "ckan", "statefulsets", "postgres"),
    ("pods.redis", "pods", "Redis", "ckan", "statefulsets", "redis-master"),
    ("pods.mariadb", "pods", "MariaDB", "default", "statefulsets", "mariadb"),
]

ISO = "%Y-%m-%dT%H:%M:%SZ"
HISTORY_DAYS = 30


def workload_ready(obj):
    replicas = (obj.get("spec") or {}).get("replicas") or 0
    ready = (obj.get("status") or {}).get("readyReplicas") or 0
    return replicas > 0 and ready == replicas


def cron_interval(cron):
    parts = cron.split()
    if len(parts) == 5:
        minute, hour, dom, month, dow = parts
        if minute.isdigit() and (hour, dom, month, dow) == ("*", "*", "*", "*"):
            return timedelta(hours=1)
        if minute.isdigit() and hour.isdigit() and (dom, month, dow) == ("*", "*", "*"):
            return timedelta(hours=24)
        if (minute.isdigit() and hour.isdigit() and dom == "*" and month == "*"
                and dow.isdigit()):
            return timedelta(days=7)
    return timedelta(hours=24)


def final_state(state_type):
    if state_type == "COMPLETED":
        return "ok"
    if state_type in ("FAILED", "CRASHED", "CANCELLED"):
        return "failed"
    return "none"


def _iso(dt):
    return dt.strftime(ISO) if dt else None


def flow_entry(name, cron, last_run, last_state_type, next_run, now):
    state = final_state(last_state_type)
    ok = (
        state == "ok"
        and next_run is not None
        and last_run is not None
        and now - last_run <= cron_interval(cron) + timedelta(hours=1)
    )
    return {
        "name": name,
        "group": "downloads" if name.startswith("download__") else None,
        "last_run": _iso(last_run),
        "last_state": state,
        "next_run": _iso(next_run),
        "ok": ok,
    }


def scheduler_ok(flows):
    return all(f["next_run"] is not None for f in flows)


def count_alerts(alerts):
    counts = {"warning": 0, "critical": 0}
    for alert in alerts:
        sev = (alert.get("labels") or {}).get("severity")
        if sev in counts:
            counts[sev] += 1
    return counts


def merge_history(previous, items, today):
    old = {}
    if previous and previous.get("schema") == 1:
        old = previous.get("history") or {}
    cutoff = (today - timedelta(days=HISTORY_DAYS)).isoformat()
    day = today.isoformat()
    history = {}
    for key, item in items.items():
        days = {d: list(v) for d, v in (old.get(key) or {}).items() if d > cutoff}
        up, total = days.get(day, [0, 0])
        days[day] = [up + (1 if item.get("ok") else 0), total + 1]
        history[key] = days
    return history


def build_status(now, items, flows, alerts, previous):
    return {
        "schema": 1,
        "generated_at": _iso(now),
        "alerts": alerts,
        "items": items,
        "flows": flows,
        "history": merge_history(previous, items, now.date()),
    }


SLEEP = time.sleep
SA_DIR = "/var/run/secrets/kubernetes.io/serviceaccount"
FINAL_TYPES = ["COMPLETED", "FAILED", "CRASHED", "CANCELLED"]


def http_json(method, url, body=None, headers=None, timeout=10, context=None):
    hdrs = dict(headers or {})
    data = None
    if body is not None:
        data = json.dumps(body).encode()
        hdrs.setdefault("Content-Type", "application/json")
    req = urllib.request.Request(url, data=data, headers=hdrs, method=method)
    with urllib.request.urlopen(req, timeout=timeout, context=context) as resp:
        return json.loads(resp.read().decode())


def check_url(url, opener=urllib.request.urlopen):
    for attempt in range(2):
        try:
            with opener(url, timeout=10) as resp:
                if 200 <= resp.status < 400:
                    return True
        except urllib.error.HTTPError as err:
            if 200 <= err.code < 400:
                return True
        except Exception:
            pass
        if attempt == 0:
            SLEEP(5)
    return False


def _log(source, exc):
    print(f"{source}: {type(exc).__name__}", file=sys.stderr)


def collect_workloads(get):
    result = {}
    for key, group, name, ns, kind, workload in WORKLOADS:
        try:
            ok = workload_ready(get(f"/apis/apps/v1/namespaces/{ns}/{kind}/{workload}"))
        except Exception as exc:
            _log(f"kubernetes {key}", exc)
            ok = False
        result[key] = {"group": group, "name": name, "ok": ok}
    return result


def _parse(ts):
    if not ts:
        return None
    return datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(timezone.utc)


def _active_cron(dep):
    for sched in dep.get("schedules") or []:
        cron = (sched.get("schedule") or {}).get("cron")
        if sched.get("active") and cron:
            return cron
    return None


def collect_flows(post, now):
    flows = []
    for dep in post("/deployments/filter", {"limit": 200}):
        cron = _active_cron(dep)
        if cron is None or dep.get("paused") is not False:
            continue
        dep_id = {"any_": [dep["id"]]}
        last = post("/flow_runs/filter", {
            "flow_runs": {"deployment_id": dep_id,
                          "state": {"type": {"any_": FINAL_TYPES}}},
            "sort": "START_TIME_DESC", "limit": 1})
        nxt = post("/flow_runs/filter", {
            "flow_runs": {"deployment_id": dep_id,
                          "state": {"type": {"any_": ["SCHEDULED"]}}},
            "sort": "EXPECTED_START_TIME_ASC", "limit": 1})
        last_run = last[0] if last else {}
        state_type = last_run.get("state_type") or (last_run.get("state") or {}).get("type")
        flows.append(flow_entry(
            dep["name"], cron, _parse(last_run.get("start_time")), state_type,
            _parse(nxt[0].get("expected_start_time")) if nxt else None, now))
    return sorted(flows, key=lambda f: f["name"])


@dataclass
class Sources:
    k8s_get: Callable[[str], dict]
    prefect_post: Callable[[str, dict], object]
    alerts_get: Callable[[], list]
    previous_get: Callable[[], Optional[dict]]
    check_url: Callable[[str], bool]


def default_sources(env):
    kube = env.get("KUBE_API", "https://kubernetes.default.svc")
    prefect = env.get("PREFECT_API_URL", "http://prefect-server.default.svc:4200/api")
    am = env.get("ALERTMANAGER_URL",
                 "http://kube-prometheus-kube-prome-alertmanager.kube-prometheus.svc:9093")
    prev = env.get("PREVIOUS_URL",
                   "https://raw.githubusercontent.com/The-EPISERVE-Consortium/episerve-status/data/status.json")

    def k8s_get(path):
        with open(f"{SA_DIR}/token") as f:
            token = f.read().strip()
        ctx = ssl.create_default_context(cafile=f"{SA_DIR}/ca.crt")
        return http_json("GET", kube + path,
                         headers={"Authorization": f"Bearer {token}"}, context=ctx)

    return Sources(
        k8s_get=k8s_get,
        prefect_post=lambda path, body: http_json("POST", prefect + path, body),
        alerts_get=lambda: http_json(
            "GET", am + "/api/v2/alerts?active=true&silenced=false&inhibited=false"),
        previous_get=lambda: http_json("GET", prev),
        check_url=check_url,
    )


def main(env=os.environ, sources=None):
    src = sources or default_sources(env)
    now = datetime.now(timezone.utc).replace(microsecond=0)
    items = {}
    for key, name, url in PUBLIC_SERVICES:
        try:
            ok = bool(src.check_url(url))
        except Exception as exc:
            _log(f"web {key}", exc)
            ok = False
        items[key] = {"group": "public", "name": name, "ok": ok}
    items.update(collect_workloads(src.k8s_get))

    try:
        flows = collect_flows(src.prefect_post, now)
        sched_ok = scheduler_ok(flows)
    except Exception as exc:
        _log("prefect", exc)
        flows, sched_ok = [], False
    items["prefect.scheduler"] = {"group": "prefect", "name": "Scheduler", "ok": sched_ok}

    try:
        alerts = count_alerts(src.alerts_get())
    except Exception as exc:
        _log("alertmanager", exc)
        alerts = None

    try:
        previous = src.previous_get()
    except Exception as exc:
        _log("previous", exc)
        previous = None
    try:
        status = build_status(now, items, flows, alerts, previous)
    except Exception as exc:
        _log("history", exc)
        status = build_status(now, items, flows, alerts, None)

    try:
        with open(env.get("OUTPUT", "/work/status.json"), "w") as f:
            json.dump(status, f, indent=2)
    except Exception as exc:
        _log("output", exc)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
