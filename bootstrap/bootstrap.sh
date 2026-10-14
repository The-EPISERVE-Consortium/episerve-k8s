#!/bin/bash
# Installs ArgoCD and prepares it for this repo.
#
# Greenfield install: run this, then follow "Next steps" below.
# Restore after total cluster loss: do NOT start here. Follow DISASTER-RECOVERY.md;
# it runs this script only after Velero has restored the data (step 6), because ArgoCD
# would otherwise create the data apps on empty volumes that Velero cannot restore into.
set -e
GITHUB_REPO="https://github.com/The-EPISERVE-Consortium/episerve-k8s"
# Pinned to the version the cluster runs; "stable" would pull whatever is newest.
ARGOCD_VERSION="v3.4.2"

echo "==> Switching to episerve01 context"
kubectl config use-context episerve01

echo "==> Installing ArgoCD ${ARGOCD_VERSION}"
kubectl create namespace argocd --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -n argocd -f "https://raw.githubusercontent.com/argoproj/argo-cd/${ARGOCD_VERSION}/manifests/install.yaml" \
    --server-side --force-conflicts

echo "==> Waiting for ArgoCD pods to be ready"
kubectl wait --for=condition=ready pod \
    -l app.kubernetes.io/name=argocd-server \
    -n argocd --timeout=120s

echo "==> Configuring ArgoCD server for Gateway API (TLS terminated at gateway)"
kubectl patch configmap argocd-cmd-params-cm -n argocd \
    --patch '{"data": {"server.insecure": "true"}}'
kubectl rollout restart deployment argocd-server -n argocd
kubectl rollout status deployment argocd-server -n argocd

echo "==> Enabling Helm rendering for Kustomize (required before any Application is created)"
kubectl patch configmap argocd-cm -n argocd \
    --patch '{"data": {"kustomize.buildOptions": "--enable-helm"}}'
kubectl rollout restart deployment argocd-repo-server -n argocd
kubectl rollout status deployment argocd-repo-server -n argocd

echo ""
echo "==> ArgoCD is ready"
echo ""
echo "Next steps (manual):"
echo "  1. Greenfield only: restore the Sealed Secrets key set BEFORE step 4, so the controller"
echo "     starts with the existing keys (no restart needed):"
echo "       kubectl apply -f sealed-secrets-master-keys.yaml     # from Bitwarden"
echo "     (After a total loss this was already done in DISASTER-RECOVERY.md step 2.)"
echo "  2. Open https://argocd.episerve.zib.de and log in (ZIB network only — use VPN if off-site)"
echo "     Password: kubectl get secret argocd-initial-admin-secret -n argocd -o jsonpath='{.data.password}' | base64 -d"
echo "  3. Settings → Repositories → Connect Repo: $GITHUB_REPO (public, no credentials needed)"
echo "  4. Create Application: infrastructure → path: infrastructure"
echo "  5. Apply ApplicationSet: kubectl apply -f applicationset.yaml -n argocd"
echo "  6. Register Prefect workflow deployments: ./bootstrap/prefect/run-bootstrap.sh"
