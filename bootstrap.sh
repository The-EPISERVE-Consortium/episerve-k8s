#!/bin/bash
set -e

GITHUB_REPO="https://github.com/The-EPISERVE-Consortium/episerve-k8s"
GITHUB_USER=""
GITHUB_TOKEN=""

echo "==> Switching to episerve01 context"
kubectl config use-context episerve01

echo "==> Installing ArgoCD"
kubectl create namespace argocd --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -n argocd -f https://raw.githubusercontent.com/argoproj/argo-cd/stable/manifests/install.yaml \
    --server-side --force-conflicts

echo "==> Waiting for ArgoCD pods to be ready"
kubectl wait --for=condition=ready pod \
    -l app.kubernetes.io/name=argocd-server \
    -n argocd --timeout=120s

echo "==> ArgoCD is ready"
echo ""
echo "Next steps (manual):"
echo "  1. Port-forward: kubectl port-forward svc/argocd-server -n argocd 8080:443"
echo "  2. Get password: kubectl get secret argocd-initial-admin-secret -n argocd -o jsonpath='{.data.password}' | base64 -d"
echo "  3. Open https://localhost:8080 and log in"
echo "  4. Settings → Repositories → Connect Repo: $GITHUB_REPO"
echo "  5. Create Application: infrastructure → path: infrastructure"
echo "  6. Create Application: apps → path: apps"
