"""Hourly status collector for the EPISERVE status page (stdlib only)."""
from datetime import timedelta

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
