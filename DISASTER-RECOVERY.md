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
| **Sealed Secrets master key** | Without it, every `*/sealed-secret.yaml` in this repo is undecryptable — including `velero-s3-credentials`, so you cannot even read the backups. | Stored in Bitwarden as `sealed-secrets-master-key.yaml`. Keep it current; verify access quarterly. |
| **lakeFS object data** (`data-raw`, `data-processed`, `model-runs`) | The actual datasets and model outputs. lakeFS runs on external infra (`lake-episerve.zib.de`), not this cluster. | Backed up by whoever operates lakeFS — confirm separately. Out of scope here. |
| `kube-prometheus` PVCs | Prometheus TSDB + Alertmanager state — metrics/alert history is lost on restore. | Accepted. History is not business-critical and re-accumulates. |
| etcd / control-plane state | Node and control-plane rebuild is not Velero's job. | Handled by cluster provisioning tooling. |
| Any **new stateful namespace** added later | Not backed up unless a `Schedule` is added for it. | See the backup-coverage note in [`README.md`](README.md#backups). Adding a stateful app = add a Velero schedule. |

## Full cluster restore (total loss)

1. **Rebuild the cluster** — nodes, control plane, CNI, and the Ceph CSI storage
   classes (`csi-rbd-sc`, CephFS). Not covered here.

2. **Restore the Sealed Secrets master key** (from Bitwarden), then bootstrap ArgoCD:

   ```bash
   ./bootstrap/bootstrap.sh
   # When prompted:
   kubectl apply -f sealed-secrets-master-key.yaml
   kubectl rollout restart deployment sealed-secrets -n kube-system
   ```

3. **Sync infrastructure and apps** via ArgoCD (see `bootstrap.sh` "Next steps"):
   connect the repo, create the `infrastructure` Application, apply
   `applicationset.yaml`. Wait for `sealed-secrets`, `cert-manager`, gateways, and
   **`velero`** to become healthy. Confirm secrets decrypt:

   ```bash
   kubectl get sealedsecret -A
   kubectl -n velero get secret velero-s3-credentials    # must exist
   ```

4. **Verify Velero sees the backups:**

   ```bash
   velero backup get                 # lists daily-*/weekly-* from S3
   velero backup-location get        # PHASE should be Available
   ```

5. **Restore data namespaces** from the most recent good backup of each. Restore
   into the empty namespaces *before* their ArgoCD Application recreates the
   workloads, or scale the workloads down first so the DB starts on restored data:

   ```bash
   velero restore create --from-backup daily-ckan-<timestamp>
   velero restore create --from-backup daily-default-<timestamp>
   velero restore create --from-backup daily-openproject-<timestamp>
   velero restore describe <restore-name>   # watch to completion
   ```

6. **Verify / repair each database.** The restored PVC contains both the live
   `PGDATA`/datadir (crash-consistent, possibly torn) *and* the logical dump.
   Start the DB pod; if it comes up clean and data looks right, you are done.
   If it will not start or the data is inconsistent, reload from the dump — see
   [Reload a database from its dump](#reload-a-database-from-its-dump).

7. **Re-register Prefect deployments:**

   ```bash
   ./bootstrap/prefect/run-bootstrap.sh
   ```

   The work-pool base job template is restored with the `prefect` DB (step 5–6).
   If the Prefect DB was lost beyond backup, recreate it by hand:
   `prefect work-pool update kubernetes-pool --base-job-template <file.json>`.

8. **Sync CKAN** (excluded from the ApplicationSet):

   ```bash
   argocd app sync ckan
   ```

   Rebuild the Solr index from Postgres if search is empty:
   `ckan search-index rebuild`.

9. **Smoke test** every service URL in [`README.md`](README.md), trigger one
   Prefect flow run, and confirm it can reach lakeFS / CKAN / MariaDB.

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
3. Confirm the Bitwarden `sealed-secrets-master-key.yaml` entry is present and that
   at least one person on the team can retrieve it.
4. Check that every stateful namespace (`kubectl get pvc -A`) is covered by a
   `Schedule` in `apps/velero/values.yaml`.
