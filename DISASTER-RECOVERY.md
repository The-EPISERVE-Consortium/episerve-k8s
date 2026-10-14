# Disaster recovery

How to restore the EPISERVE platform after data loss or a full cluster loss.

The platform follows a standard DR pattern:

- **Cluster state** (Deployments, Services, config) — reproduced from this repo by ArgoCD (GitOps).
- **Persistent data** (databases, file stores) — backed up by [Velero](apps/velero/) to external S3.
- **Secret material** — Sealed Secrets in the repo, decryptable only with the controller's
  master key, which is held **out-of-band in Bitwarden** (not in any backup).

> A backup you have never restored is a hypothesis. See [Testing restores](#testing-restores).

## Recovery objectives

| | Value |
|---|---|
| Backup target | S3 bucket `episerve-backups` at `https://rise-s3.zib.de` |
| RPO (daily backups) | ≤ 24 h, retained **14 days** |
| RPO (weekly backups) | ≤ 7 days, retained **90 days** |
| RTO (full cluster) | hours — mostly manual, see below |

## What is backed up

Velero `Schedule`s are defined in [`apps/velero/values.yaml`](apps/velero/values.yaml).
Each runs a pre-hook that dumps the database to a file on its data PVC
(`/bitnami/{postgresql,mariadb}/velero-backup.sql.gz`), then file-system-backs-up
the volume with Kopia (`defaultVolumesToFsBackup: true`, `snapshotVolumes: false` —
no dependency on CSI snapshots surviving).

| Namespace | Schedules (daily / weekly cron) | State captured | DB dump hook |
|---|---|---|---|
| `ckan` | `0 2 * * *` / `0 4 * * 0` | filestore PVC, Postgres, Solr, Zookeeper | `pg_dumpall` (roles + all DBs) |
| `default` | `0 3 * * *` / `0 5 * * 0` | MariaDB, Metabase Postgres, **Prefect Postgres** | `mysqldump --all-databases`; `pg_dumpall` (Metabase); `pg_dump prefect` (single DB) |
| `openproject` | `30 2 * * *` / `30 4 * * 0` | Postgres, app PVC | `pg_dumpall` (roles + all DBs) |

The **Prefect work-pool base job template** (the ~24 env vars injected into every flow
run — lakeFS / CKAN / MariaDB / DOIP / GitHub / blackboard credentials and endpoints)
lives only in the Prefect Postgres DB and is recovered via the `prefect` dump. It is
**not** in this repo and **not** in the Prefect bootstrap script.

## What is NOT backed up

| Item | Why it matters | Mitigation |
|---|---|---|
| **Sealed Secrets keys** | Without them, every `*/sealed-secret.yaml` in this repo is undecryptable — including `velero-s3-credentials`, so you cannot even read the backups. | Stored in Bitwarden as `sealed-secrets-master-keys.yaml`. It must contain **every** key the controller has ever generated — currently 8 and growing by one roughly every 30 days. Each `SealedSecret` is decryptable only by the key that was current when it was sealed, and keys cannot be derived from one another, so a partial set is unrecoverable. Verify access quarterly and re-export after each rotation; see the setup runbook for the export command. |
| **lakeFS object data** (`data-raw`, `data-processed`, `model-runs`) | The actual datasets and model outputs. lakeFS runs on external infra (`lake-episerve.zib.de`), not this cluster. | Backed up by whoever operates lakeFS — confirm separately. Out of scope here. |
| **Velero Kopia repository password** (`velero-repo-credentials`) | Encrypts all backup data; without it the backups in S3 cannot be read. | Not stored anywhere because it is Velero's built-in default (`static-passw0rd`), which a fresh Velero recreates automatically. If it is ever changed, store the new value in Bitwarden. See [step 3](#full-cluster-restore-total-loss). |
| `kube-prometheus` PVCs | Prometheus TSDB + Alertmanager state — metrics/alert history is lost on restore. | Accepted. History is not business-critical and re-accumulates. |
| etcd / control-plane state | Node and control-plane rebuild is not Velero's job. | Handled by cluster provisioning tooling. |
| Any **new stateful namespace** added later | Not backed up unless a `Schedule` is added for it. | See the backup-coverage note in [`README.md`](README.md#backups). Adding a stateful app = add a Velero schedule. |

## Full cluster restore (total loss)

> **Order matters.** Restore the data with Velero *before* ArgoCD creates any
> application. If ArgoCD runs first, CKAN, `default` and OpenProject start on new,
> empty PVCs. Velero skips PVCs that already exist and only copies backed-up files
> into pods it creates itself, so the data would silently not be restored. Scaling
> the workloads down does not help, because the empty PVCs remain.
>
> That is why steps 2–3 install the sealed-secrets controller and Velero **by hand**
> from this repo (normally ArgoCD installs both), and ArgoCD only arrives in step 6,
> where it adopts what is already running.

> **Do not run `bootstrap/ckan/run-bootstrap.sh` during a restore.** It is for a
> fresh install only: it generates *new* passwords and secrets, overwrites
> `apps/ckan/values.yaml` and `apps/ckan/sealed-secret.yaml`, and re-initialises the
> databases. Run during a restore, it would leave the restored data unusable.

Tools needed on the operator machine: `kubectl` (episerve01 context, OIDC login,
LDAP group `login_k8s_episerve`), `kubeseal`, the `velero` CLI (v1.18.x), and a
clone of this repo.

1. **Rebuild the cluster and confirm ZIB's platform layer is back.** The control
   plane, nodes and the cluster-wide platform are operated by ZIB (Flux repo
   `rise-flux`), not by this repo. Before continuing, confirm with ZIB, or check
   directly, that these exist:

   | Prerequisite | Needed for | Check |
   |---|---|---|
   | Ceph CSI storage classes `csi-rbd-sc`, `csi-cephfs-sc-hdd` | every PVC | `kubectl get storageclass` |
   | kgateway + `shared-public-gateway` on IP **130.73.131.120** | all service URLs | `kubectl get svc -n kgateway-system shared-public-gateway` |
   | WebSocket `ListenerPolicy` on that gateway | API server UI (NiceGUI) | `kubectl get listenerpolicy -n kgateway-system` |
   | DNS CNAMEs `*.episerve.zib.de` → `lb223.zib.de` | all service URLs | `getent hosts data.episerve.zib.de` |
   | cert-manager (CRDs + controller) | TLS certificates, `ClusterIssuer` | `kubectl get crd certificates.cert-manager.io` |
   | kube-prometheus-stack (CRDs `PrometheusRule`, `ServiceMonitor`, `AlertmanagerConfig`) | `monitoring-alerts` app, Velero `ServiceMonitor` | `kubectl get crd servicemonitors.monitoring.coreos.com` |

   The gateway IP is pinned by ZIB (`lbipam.cilium.io/ips`), so DNS stays valid. The
   other LoadBalancer IPs are **not** pinned: MariaDB (`130.73.130.213`) and
   `doip-server-lb` will most likely get new IPs. Tell external MariaDB users.

2. **Restore the Sealed Secrets keys, then install the controller by hand.**

   ```bash
   kubectl apply -f sealed-secrets-master-keys.yaml          # from Bitwarden
   kubectl kustomize --enable-helm infrastructure/sealed-secrets \
     | kubectl apply --server-side -f -
   kubectl -n kube-system rollout status deploy/sealed-secrets
   ```

   Applying the keys *before* the controller starts means it loads them at startup,
   so no restart is needed. The file is a Kubernetes `List` holding all key secrets;
   confirm it holds every key the cluster used (8 as of 2026-10). A set that is
   missing a key fails silently later, when that key's `SealedSecret` is unsealed.
   Do **not** apply the upstream `controller.yaml`: it installs a second controller
   whose keys are incompatible (see the setup runbook).

3. **Install Velero by hand and check that it sees the old backups.**

   ```bash
   kubectl kustomize --enable-helm apps/velero | kubectl apply --server-side -f -
   kubectl -n velero rollout status deploy/velero
   kubectl -n velero get secret velero-s3-credentials   # must exist = decryption works
   velero backup-location get                          # PHASE: Available
   velero backup get                                   # daily-*/weekly-* synced from S3
   ```

   The Velero server runs with `--schedule-skip-immediately`
   (`configuration.extraArgs` in `apps/velero/values.yaml`), so creating the
   Schedules does not back up the still-empty namespaces; each one waits for its
   next cron time. The `velero-upgrade-crds` Job (a Helm hook) runs once; that is
   harmless.

   **Kopia repository password:** Velero encrypts the backup data with the password
   in the Secret `velero-repo-credentials`. Velero creates that Secret automatically
   with its built-in default (`static-passw0rd`), which is what the existing backups
   use (checked 2026-10-14). A fresh Velero therefore reads them without any input.
   If this password is ever changed, the new value **must** be stored in Bitwarden,
   or every backup made after the change becomes unreadable after a total loss.

4. **Restore the data namespaces into the empty cluster.** Pick the most recent
   good backup of each (`velero backup describe <name>`: phase `Completed`, no hook
   errors):

   ```bash
   velero restore create --from-backup daily-ckan-<timestamp>
   velero restore create --from-backup daily-default-<timestamp>
   velero restore create --from-backup daily-openproject-<timestamp>
   velero restore describe <restore-name> --details   # wait for Completed; read warnings
   ```

   Velero recreates the PVCs, StatefulSets, Deployments, Services and Secrets and
   copies the file-system backups into the new volumes before the pods start. The
   `default` restore also brings back `doip-server`, `episerve-api-server` and
   Prefect. That is fine: they match git, and ArgoCD adopts them in step 6.

5. **Verify / repair each database.** The restored PVC contains both the live
   `PGDATA`/datadir (crash-consistent, possibly torn) *and* the logical dump.
   If the DB pod comes up clean and the data looks right, you are done. If it will
   not start or the data is inconsistent, reload from the dump. See
   [Reload a database from its dump](#reload-a-database-from-its-dump).

6. **Only now install ArgoCD and hand everything over to it.**

   ```bash
   ./bootstrap/bootstrap.sh     # ArgoCD (pinned version), --enable-helm, server.insecure
   ```

   Then follow its "Next steps": connect the repo, create the `infrastructure`
   Application, and apply `applicationset.yaml`. Skip the key-restore step there;
   it was done in step 2. ArgoCD finds the hand-installed sealed-secrets and Velero
   and the restored workloads already matching git, and adopts them. The
   `infrastructure` app also creates the auto-syncing `ckan` Application
   (`infrastructure/argocd-apps/ckan-application.yaml`). Wait until everything is
   `Synced` / `Healthy`:

   ```bash
   kubectl get applications -n argocd
   argocd app wait ckan --sync --health   # force with `argocd app sync ckan` only if it stalls
   ```

7. **Prefect deployments.** The deployments, the automation and the work-pool
   base job template live in the Prefect DB and come back with the `default`
   restore. Re-registering them from the flow repos is optional and refreshes them
   to the repos' current state:

   ```bash
   ./bootstrap/prefect/run-bootstrap.sh
   ```

   If the Prefect DB was lost beyond backup, also recreate the work-pool template
   by hand: `prefect work-pool update kubernetes-pool --base-job-template <file.json>`.

8. **CKAN search index.** If search is empty, rebuild it from Postgres:
   `ckan search-index rebuild`.

9. **Smoke test** every service URL in [`README.md`](README.md), trigger one
   Prefect flow run, and confirm it can reach lakeFS / CKAN / MariaDB. OpenProject
   users have to log in again: its chart regenerates `secret_key_base`. Passwords
   and API keys are unaffected.

## Single namespace / single DB restore

For losing one database (not the whole cluster):

```bash
velero backup get
velero restore create --from-backup weekly-default-<timestamp> \
  --include-namespaces default
```

Then follow [Reload a database from its dump](#reload-a-database-from-its-dump) if
you only need the logical data and not the whole namespace.

### Reload a database from its dump

The dump file is at `/bitnami/postgresql/velero-backup.sql.gz` (Postgres) or
`/bitnami/mariadb/velero-backup.sql.gz` (MariaDB) on the restored data PVC.

Postgres (e.g. Prefect — stop the server first so nothing writes):

```bash
kubectl -n default scale deploy prefect-server --replicas=0
POD=prefect-postgresql-0
kubectl -n default exec -it $POD -- bash -c \
  'gunzip -c /bitnami/postgresql/velero-backup.sql.gz | psql -U prefect -d prefect'
kubectl -n default scale deploy prefect-server --replicas=1
```

> The `prefect` dump is `pg_dump` of a single database — it carries no roles or
> globals. That is fine because the Helm chart recreates the `prefect` role. The
> CKAN / OpenProject / Metabase dumps are `pg_dumpall` and include roles.

MariaDB:

```bash
POD=data-mariadb-0   # pod is 'mariadb-0'; adjust as needed
kubectl -n default exec -it mariadb-0 -- bash -c \
  'gunzip -c /bitnami/mariadb/velero-backup.sql.gz | mysql -u root -p"$(cat $MARIADB_ROOT_PASSWORD_FILE)"'
```

## Testing restores

Do this at least once per quarter, and after any change to `apps/velero/`:

1. `velero backup get` — confirm daily + weekly backups are recent and `Completed`.
2. Restore one namespace into a scratch namespace or a throwaway cluster and reload
   a DB from its dump.
3. Confirm the Bitwarden `sealed-secrets-master-keys.yaml` entry is present, that at
   least one person on the team can retrieve it, and that it contains the same number
   of keys the cluster reports:

   ```bash
   # items in the backup file must equal this count
   kubectl get secret -n kube-system \
     -l sealedsecrets.bitnami.com/sealed-secrets-key --no-headers | wc -l
   ```

   A backup holding fewer keys than the cluster looks fine until a restore is attempted,
   at which point the affected `SealedSecret`s cannot be decrypted.
4. Check that every stateful namespace (`kubectl get pvc -A`) is covered by a
   `Schedule` in `apps/velero/values.yaml`.
