"""
Register a Prefect automation that triggers the sync-ckan-with-lakefs-modelruns
deployment whenever a model-runner deployment run completes successfully.

Run from the repo root after setting PREFECT_API_URL (and optionally
PREFECT_API_KEY and the SOURCE_DEPLOYMENT_NAME / TARGET_DEPLOYMENT_NAME env vars):

    python maintenance/register_automation.py

PREFECT_API_URL defaults to https://prefect.episerve.zib.de/api (the cluster
gateway). Override via env var if needed. No API key is required (Prefect OSS).
"""

import json
import os
import sys

import httpx

PREFECT_API_URL = os.environ.get(
    "PREFECT_API_URL", "https://prefect.episerve.zib.de/api"
).rstrip("/")
PREFECT_API_KEY = os.environ.get("PREFECT_API_KEY", "")

# Name of the deployment whose completion triggers the automation
SOURCE_DEPLOYMENT_NAME = os.getenv("SOURCE_DEPLOYMENT_NAME", "model-runner")

# Name of the deployment to trigger
TARGET_DEPLOYMENT_FLOW = os.getenv("TARGET_DEPLOYMENT_FLOW", "sync-ckan-with-lakefs")
TARGET_DEPLOYMENT_NAME = os.getenv("TARGET_DEPLOYMENT_NAME", "sync-ckan-with-lakefs-modelruns")

AUTOMATION_NAME = os.getenv(
    "AUTOMATION_NAME",
    f"trigger-{TARGET_DEPLOYMENT_NAME}-on-{SOURCE_DEPLOYMENT_NAME}-completed",
)


def _headers() -> dict:
    h = {"Content-Type": "application/json"}
    if PREFECT_API_KEY:
        h["Authorization"] = f"Bearer {PREFECT_API_KEY}"
    return h


def _get(path: str) -> dict:
    r = httpx.get(f"{PREFECT_API_URL}{path}", headers=_headers())
    r.raise_for_status()
    return r.json()


def _post(path: str, payload: dict) -> dict:
    r = httpx.post(f"{PREFECT_API_URL}{path}", headers=_headers(), content=json.dumps(payload))
    r.raise_for_status()
    return r.json()


def _put(path: str, payload: dict) -> dict:
    r = httpx.put(f"{PREFECT_API_URL}{path}", headers=_headers(), content=json.dumps(payload))
    r.raise_for_status()
    return r.json()


def resolve_deployment_id(deployment_name: str) -> str:
    result = _post("/deployments/filter", {
        "deployments": {"name": {"any_": [deployment_name]}},
        "limit": 10,
    })
    if not result:
        print(f"ERROR: deployment '{deployment_name}' not found in Prefect.", file=sys.stderr)
        sys.exit(1)
    return result[0]["id"]


def find_existing_automation(name: str) -> str | None:
    result = _post("/automations/filter", {})
    for auto in result:
        if auto.get("name") == name:
            return auto["id"]
    return None


def main():
    print(f"Resolving source deployment '{SOURCE_DEPLOYMENT_NAME}'...")
    source_deployment_id = resolve_deployment_id(SOURCE_DEPLOYMENT_NAME)
    print(f"  source deployment id: {source_deployment_id}")

    print(f"Resolving target deployment '{TARGET_DEPLOYMENT_NAME}'...")
    deployment_id = resolve_deployment_id(TARGET_DEPLOYMENT_NAME)
    print(f"  target deployment id: {deployment_id}")

    automation = {
        "name": AUTOMATION_NAME,
        "description": (
            f"Run {TARGET_DEPLOYMENT_NAME} whenever a {SOURCE_DEPLOYMENT_NAME} "
            "deployment run completes successfully."
        ),
        "enabled": True,
        "trigger": {
            "type": "event",
            "posture": "Reactive",
            "threshold": 1,
            "within": 0,
            "match": {
                "prefect.resource.id": "prefect.flow-run.*"
            },
            "match_related": {
                "prefect.resource.id": [f"prefect.deployment.{source_deployment_id}"],
                "prefect.resource.role": "deployment",
            },
            "for_each": ["prefect.resource.id"],
            "after": [],
            "expect": ["prefect.flow-run.Completed"],
        },
        "actions": [
            {
                "type": "run-deployment",
                "source": "selected",
                "deployment_id": deployment_id,
                "parameters": {},
            }
        ],
    }

    existing_id = find_existing_automation(AUTOMATION_NAME)
    if existing_id:
        print(f"Updating existing automation '{AUTOMATION_NAME}' ({existing_id})...")
        _put(f"/automations/{existing_id}", automation)
        print("Done.")
    else:
        print(f"Creating automation '{AUTOMATION_NAME}'...")
        result = _post("/automations/", automation)
        print(f"Done — id: {result['id']}")


if __name__ == "__main__":
    main()
