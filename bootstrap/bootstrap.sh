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

echo "==> Configuring ArgoCD server for Gateway API (TLS terminated at gateway)"
kubectl patch configmap argocd-cmd-params-cm -n argocd \
    --patch '{"data": {"server.insecure": "true"}}'
kubectl rollout restart deployment argocd-server -n argocd
kubectl rollout status deployment argocd-server -n argocd

echo "==> Restoring Sealed Secrets master key"
echo "    Copy sealed-secrets-master-key.yaml from Bitwarden and run:"
echo "    kubectl apply -f sealed-secrets-master-key.yaml"
echo "    kubectl rollout restart deployment sealed-secrets -n kube-system"
echo ""
echo "==> ArgoCD is ready"
echo ""
echo "Next steps (manual):"
echo "  1. Open https://argo.medicalbioinformatics.de and log in"
echo "  2. Get password: kubectl get secret argocd-initial-admin-secret -n argocd -o jsonpath='{.data.password}' | base64 -d"
echo "  3. Settings → Repositories → Connect Repo: $GITHUB_REPO"
echo "  4. Create Application: infrastructure → path: infrastructure"
echo "     Enable Helm rendering for Kustomize (required for infrastructure/sealed-secrets):"
echo "     kubectl patch configmap argocd-cm -n argocd --patch '{\"data\":{\"kustomize.buildOptions\":\"--enable-helm\"}}'"
echo "     kubectl rollout restart deployment argocd-repo-server -n argocd"
echo "  5. Apply ApplicationSet: kubectl apply -f applicationset.yaml -n argocd"
echo "  6. Register Prefect workflow deployments: ./bootstrap/prefect/run-bootstrap.sh"
