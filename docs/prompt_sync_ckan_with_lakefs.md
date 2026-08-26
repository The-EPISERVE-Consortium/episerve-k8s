# Coding Agent Prompt: sync-ckan-with-lakefs Prefect Project

## Task

Create a Python Prefect project that periodically scans a lakeFS repository for model runs and registers any new ones in a CKAN data catalog.

A guide file already exists at `src/sync_ckan_with_lakefs.py`. It contains:
- **Fully implemented**: `create_model()` and `create_model_run()` — these are correct and complete, copy them verbatim into `flows/sync_ckan_with_lakefs.py`
- **Stubs**: `_lakefs_client()`, `_list_lakefs_runs()`, `_list_run_files()`, `_get_run_metadata()`, `_ckan_run_exists()`, `sync_run` task, `sync_ckan_with_lakefs` flow — implement these

Do not modify the logic of `create_model()` or `create_model_run()`.

---

## Project structure to create

```
.github/workflows/ci.yaml
flows/sync_ckan_with_lakefs.py
tests/test_sync_ckan_with_lakefs.py
Dockerfile
README.md
deploy.py
requirements.txt
```

---

## Context

- **lakeFS** (`https://lake-episerve.zib.de`) stores model run files
- **CKAN** (`https://ckan.episerve.zib.de`) is the data catalog
- The Prefect worker runs in Kubernetes on the `kubernetes-pool` work pool
- Prefect server: `http://prefect-server.default.svc.cluster.local:4200/api`
- Docker images are pushed to `ghcr.io/the-episerve-consortium/`

---

## lakeFS run folder structure

Every completed model run produces a folder in the `model-runs` repository, `main` branch:

```
<run_id>/
  metadata.json      ← run provenance
  input/             ← all input files (one or more)
  output/            ← all output files (one or more)
```

`metadata.json` schema:
```json
{
  "model_name":    "ct-segmentation",
  "git_commit":    "a3f9c12",
  "docker_tag":    "2.1.0",
  "run_timestamp": "2026-05-15T11:00:00Z",
  "status":        "success",
  "domain":        "imaging",
  "modality":      "ct-scan"
}
```

---

## Implementation details

### `flows/sync_ckan_with_lakefs.py`

**`_lakefs_client()`**
Return a `lakefs.client.Client` using:
- `host`: env var `LAKEFS_HOST`, default `https://lake-episerve.zib.de/`
- `username`: env var `LAKEFS_ACCESS_KEY`
- `password`: env var `LAKEFS_SECRET_KEY`

**`_list_lakefs_runs()`**
List all top-level run folders in `model-runs/main`. Use the lakeFS Python SDK:
```python
import lakefs
repo   = lakefs.repository("model-runs", client=_lakefs_client())
branch = repo.branch("main")
# list with delimiter to get common prefixes (top-level dirs only)
runs = [entry.path.rstrip("/") for entry in branch.objects.list(delimiter="/")]
```
Return a list of `run_id` strings. Skip entries without a `/` (bare files).

**`_list_run_files(run_id, subdir)`**
List all objects under `<run_id>/<subdir>/` and return full lakeFS URIs:
```
lakefs://model-runs/main/<run_id>/<subdir>/<filename>
```

**`_get_run_metadata(run_id)`**
Read and JSON-parse `<run_id>/metadata.json` from lakeFS. Return the dict directly.

**`_ckan_run_exists(run_id)`**
Search CKAN for a dataset where `extras_run_id` equals `run_id`:
```python
r = requests.get(f"{CKAN_URL}/api/3/action/package_search",
    params={"q": f"extras_run_id:{run_id}", "rows": 1})
return r.json()["result"]["count"] > 0
```

**`sync_run` task**
Already stubbed. Implement it:
1. Call `_ckan_run_exists(run_id)` — if True, log and return
2. Call `_get_run_metadata(run_id)`
3. Call `_list_run_files(run_id, "input")` and `_list_run_files(run_id, "output")`
4. Call `create_model_run(...)` with all gathered data
5. Log success

**`sync_ckan_with_lakefs` flow**
Already stubbed. Implement it:
1. Call `_list_lakefs_runs()`
2. Submit each run_id to `sync_run` as a Prefect task

### `requirements.txt`
```
prefect>=3.0
lakefs
requests
```

### `Dockerfile`
- Base image: `python:3.12-slim`
- Install `requirements.txt`
- Copy `flows/` into the image
- No entrypoint needed (Prefect worker pulls and executes flows)

### `deploy.py`
Create a Prefect deployment using the `prefect` Python API:
- Flow: `sync_ckan_with_lakefs` from `flows/sync_ckan_with_lakefs.py`
- Deployment name: `sync-ckan-with-lakefs`
- Work pool: `kubernetes-pool`
- Schedule: every 1 hour (cron: `"0 * * * *"`)
- Image: `ghcr.io/the-episerve-consortium/sync-ckan-with-lakefs:latest`
- Pass through env vars: `CKAN_API_TOKEN`, `LAKEFS_ACCESS_KEY`, `LAKEFS_SECRET_KEY`

### `.github/workflows/ci.yaml`
Trigger on push to `main`. Two jobs:

**build-and-push**
- Check out code
- Log in to GHCR using `GITHUB_TOKEN`
- Build and push `ghcr.io/the-episerve-consortium/sync-ckan-with-lakefs:<sha>` and `:latest`

**deploy** (depends on build-and-push)
- Run `python deploy.py`
- Needs secrets: `PREFECT_API_URL`, `PREFECT_API_KEY`, `CKAN_API_TOKEN`, `LAKEFS_ACCESS_KEY`, `LAKEFS_SECRET_KEY`

### `tests/test_sync_ckan_with_lakefs.py`
Write unit tests using `pytest` and `unittest.mock`. Cover:
- `create_model()` is idempotent (returns existing dataset without calling `package_create` again)
- `create_model_run()` calls `package_create` once and `resource_create` once per file
- `sync_run()` skips a run when `_ckan_run_exists()` returns `True`
- `sync_run()` calls `create_model_run()` when `_ckan_run_exists()` returns `False`
- `_ckan_run_exists()` returns `True` when CKAN search count > 0

### `README.md`
Cover: what the project does, environment variables required, how to run locally, how to deploy.

---

## Environment variables

| Variable | Description |
|---|---|
| `CKAN_API_TOKEN` | CKAN API token |
| `LAKEFS_ACCESS_KEY` | lakeFS access key |
| `LAKEFS_SECRET_KEY` | lakeFS secret key |
| `LAKEFS_HOST` | lakeFS endpoint (default: `https://lake-episerve.zib.de/`) |
| `PREFECT_API_URL` | Prefect server URL (for deploy.py and CI) |
| `PREFECT_API_KEY` | Prefect API key (for deploy.py and CI) |
