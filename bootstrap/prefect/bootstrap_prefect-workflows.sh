#!/bin/bash
# Bootstrap script for registering Prefect deployments on cluster startup.
# Clones each flow repo, installs dependencies, and runs deploy.py to register
# its deployments with the Prefect server, then registers the automation that
# starts the model-runs sync after each model run. Assumes Prefect server and
# work pool are already running. scrape-x is left out on purpose: it is
# experimental and only started by hand.
set -euo pipefail

PREFECT_API_URL="${PREFECT_API_URL:-http://prefect-server.default.svc.cluster.local:4200/api}"
WORK_POOL_NAME="${WORK_POOL_NAME:-kubernetes-pool}"

TMPDIR=$(mktemp -d)
trap "rm -rf $TMPDIR" EXIT

REPOS=(
  "https://github.com/The-EPISERVE-Consortium/workflow-prefect__model-runner"
  "https://github.com/The-EPISERVE-Consortium/workflow-prefect__sync-lakefs-ckan"
  "https://github.com/The-EPISERVE-Consortium/workflow-prefect__dataset-downloader"
  "https://github.com/The-EPISERVE-Consortium/workflow-prefect__ai-blackboard-task-runner"
  "https://github.com/The-EPISERVE-Consortium/workflow-prefect__ai-blackboard-orchestrator"
)

for repo in "${REPOS[@]}"; do
  name=$(basename "$repo")
  dir="$TMPDIR/$name"
  echo "=== Deploying: $name ==="
  git clone --depth 1 "$repo" "$dir"
  pip install -r "$dir/requirements.txt" -q
  python "$dir/deploy.py"
  echo "=== Done: $name ==="
done

echo "=== Registering automation ==="
PREFECT_API_URL="$PREFECT_API_URL" python "$(dirname "$0")/register_automation.py"

echo "Bootstrap complete."
