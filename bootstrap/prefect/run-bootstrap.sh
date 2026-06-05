#!/bin/bash
# Runs the Prefect bootstrap job inside the cluster.
# Usage: ./run-bootstrap.sh
# Requires: kubectl configured and pointing at the right cluster.
set -euo pipefail

NAMESPACE="${NAMESPACE:-default}"
PREFECT_API_URL="${PREFECT_API_URL:-http://prefect-server.default.svc.cluster.local:4200/api}"
WORK_POOL_NAME="${WORK_POOL_NAME:-kubernetes-pool}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "==> Creating ConfigMap..."
kubectl delete configmap prefect-bootstrap -n "$NAMESPACE" 2>/dev/null || true
kubectl create configmap prefect-bootstrap \
  --from-file=bootstrap_prefect-workflows.sh="$SCRIPT_DIR/bootstrap_prefect-workflows.sh" \
  -n "$NAMESPACE"

echo "==> Deleting old pod if exists..."
kubectl delete pod prefect-bootstrap -n "$NAMESPACE" 2>/dev/null || true

echo "==> Running bootstrap job..."
kubectl run prefect-bootstrap \
  --image=python:3.11 \
  --restart=Never \
  --namespace="$NAMESPACE" \
  --overrides="{
    \"spec\": {
      \"containers\": [{
        \"name\": \"prefect-bootstrap\",
        \"image\": \"python:3.11\",
        \"command\": [\"bash\", \"/scripts/bootstrap_prefect-workflows.sh\"],
        \"env\": [
          {\"name\": \"PREFECT_API_URL\", \"value\": \"$PREFECT_API_URL\"},
          {\"name\": \"WORK_POOL_NAME\", \"value\": \"$WORK_POOL_NAME\"},
          {\"name\": \"CKAN_HOST\", \"value\": \"dummy\"},
          {\"name\": \"CKAN_API_TOKEN\", \"value\": \"dummy\"},
          {\"name\": \"LAKEFS_ACCESS_KEY\", \"value\": \"dummy\"},
          {\"name\": \"LAKEFS_SECRET_KEY\", \"value\": \"dummy\"}
        ],
        \"volumeMounts\": [{\"name\": \"scripts\", \"mountPath\": \"/scripts\"}]
      }],
      \"volumes\": [{\"name\": \"scripts\", \"configMap\": {\"name\": \"prefect-bootstrap\", \"defaultMode\": 493}}]
    }
  }"

echo "==> Waiting for pod to start..."
kubectl wait pod/prefect-bootstrap \
  --for=jsonpath='{.status.phase}'=Running \
  --timeout=120s \
  -n "$NAMESPACE" 2>/dev/null || true

echo "==> Streaming logs..."
kubectl logs -f pod/prefect-bootstrap -n "$NAMESPACE"

echo "==> Cleaning up..."
kubectl delete pod prefect-bootstrap -n "$NAMESPACE"
kubectl delete configmap prefect-bootstrap -n "$NAMESPACE"
