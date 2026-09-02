# episerve-k8s

Kubernetes infrastructure-as-code for the EPISERVE platform. Uses ArgoCD with a GitOps approach: an ApplicationSet auto-syncs everything in `apps/` to the cluster whenever `main` is pushed.

GitHub: `https://github.com/The-EPISERVE-Consortium/episerve-k8s`

## How it works

`applicationset.yaml` defines an ArgoCD ApplicationSet that watches the `apps/` directory. Each subdirectory becomes an ArgoCD Application with `automated` sync (prune + self-heal). CKAN is excluded from the ApplicationSet and managed as a separate ArgoCD Application in `infrastructure/argocd-apps/ckan-application.yaml`.

## Directory structure

```
apps/
  ckan/                    # CKAN open data catalog (Helm values + patches)
  doip-server/             # DOIP 2.0 FDO access server
  episerve-api-server/     # FastAPI + NiceGUI platform API
  mariadb/                 # MariaDB (episerve-raw-data database)
  metabase/                # Metabase dashboards (reads MariaDB)
  openproject/             # Project management (project.episerve.zib.de)
  prefect/                 # Prefect server
  prefect-worker/          # Prefect Kubernetes work pool worker
  prefect-secrets/         # Sealed secrets for Prefect flows (lakeFS, CKAN)

infrastructure/
  argocd-apps/             # CKAN ArgoCD Application
  base/gateway/            # Gateway API: ListenerSets, HTTPRoutes, cert-manager Certificate
  base/gateway-zib-only/   # Traffic policy restricting access to ZIB network
  cert-manager/            # Cluster issuer (ACME/Let's Encrypt)
  gateways/                # Per-service HTTPRoute kustomizations
    argocd/, ckan/, doip-server/, episerve-api-server/
    metabase/, openproject/, prefect/
  sealed-secrets/          # Sealed Secrets controller
  gateway-websocket-policy.yaml  # HTTPListenerPolicy enabling WebSocket for NiceGUI

bootstrap/
  bootstrap.sh             # Cluster bootstrap script
  ckan/                    # CKAN post-install setup
  prefect/                 # Prefect deployment bootstrap
```

## Services and their URLs

| Service | External URL | Notes |
|---|---|---|
| EPISERVE API | `api.episerve.zib.de` | FastAPI + NiceGUI |
| CKAN | `data.episerve.zib.de` | Open data catalog |
| DOIP server | `doip.episerve.zib.de` | DOIP 2.0 TCP + HTTP gateway |
| Prefect | `prefect.episerve.zib.de` | Workflow server UI + API |
| Metabase | `metabase.episerve.zib.de` | Dashboards |
| OpenProject | `project.episerve.zib.de` | Project management |
| ArgoCD | cluster-internal | GitOps controller |

## Making changes

1. Edit the relevant files in `apps/` or `infrastructure/`.
2. Commit and push to `main`.
3. ArgoCD detects the change and syncs the affected Application automatically (within ~3 minutes, or force-sync via the ArgoCD UI).

For CKAN changes: the ApplicationSet excludes `apps/ckan/` — sync it manually via the ArgoCD UI or `argocd app sync ckan`.

## Secrets

Secrets are managed with [Sealed Secrets](https://github.com/bitnami-labs/sealed-secrets). The sealed secret files (`.yaml`) in each `apps/*/` directory are safe to commit. To rotate a secret, re-encrypt with `kubeseal` and commit the updated file.

Key secrets:
- `lakefs-credentials` (in `default` namespace) — used by Prefect flows and the API server
- `ckan-credentials` (in `default` namespace) — CKAN API token for the API server and sync flows
- `apps/prefect-secrets/` — lakeFS and CKAN credentials injected into Prefect flow runs

The Sealed Secrets controller's master key is **not** in this repo or any backup — it
is held in Bitwarden (`sealed-secrets-master-key.yaml`) and is required before any
secret (including Velero's S3 credentials) can be decrypted on a rebuilt cluster.

## Backups

Persistent data is backed up by [Velero](apps/velero/) to S3
(`episerve-backups` at `rise-s3.zib.de`). Coverage:

| Namespace | Daily / weekly | Retention | Notes |
|---|---|---|---|
| `ckan` | ✅ / ✅ | 14 d / 90 d | Postgres (`pg_dumpall`), filestore, Solr, Zookeeper |
| `default` | ✅ / ✅ | 14 d / 90 d | MariaDB, Metabase Postgres, Prefect Postgres (dump hooks) |
| `openproject` | ✅ / ✅ | 14 d / 90 d | Postgres (`pg_dumpall`), app PVC |

**Not backed up:** lakeFS object data (external infra), `kube-prometheus` PVCs
(metrics history), etcd / control-plane state.

Adding a stateful app means **adding a Velero `Schedule`** in
[`apps/velero/values.yaml`](apps/velero/values.yaml). Full restore procedure:
[`DISASTER-RECOVERY.md`](DISASTER-RECOVERY.md).
