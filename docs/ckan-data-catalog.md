# CKAN Data Catalog

CKAN at `https://ckan.episerve.zib.de` serves as the central data catalog for EPISERVE. It tracks what data exists, where it lives, who produced it, and how it was generated — without storing the files themselves.

---

## Core Concept

Four CKAN building blocks, each with a distinct role:

| Building block | Role | Example |
|---|---|---|
| **Organization** | Ownership / access control | `episerve`, `zib`, `charite` |
| **Group `type-*`** | Browses by content type | `type-model`, `type-model-run`, `type-report`, `type-raw-data` |
| **Dataset** | One catalogued item — model descriptor, run, report, or raw data | `ct-segmentation`, `ct-segmentation-run-20260515-f1a8b99` |
| **Resource** | A file attached to a dataset | `lakefs://sandbox/main/runs/f1a8b99/metrics.json` |

**Tag vocabularies** add cross-cutting facets to every dataset, driving faceted search in the UI:

| Vocabulary | Values |
|---|---|
| `domain` | `genomics`, `clinical`, `imaging`, `metabolomics` |
| `status` | `success`, `failed`, `running` |
| `modality` | `rna-seq`, `wgs`, `ct-scan`, `mri` |

There are exactly four groups regardless of how many models or datasets exist. Runs carry `extras.model` pointing to their model descriptor name — this is the primary link between runs and models.

```
episerve (org)
├── type-model      (group)  ← all model descriptors
├── type-model-run  (group)  ← all runs across all models
├── type-report     (group)  ← all reports
└── type-raw-data   (group)  ← all raw datasets

ct-segmentation (dataset)                ← MODEL DESCRIPTOR
  groups: [type-model]
  url:    https://github.com/episerve/ct-segmentation
  extras: docker_image, algorithm, input_format, ...

ct-segmentation-run-20260515-f1a8b99 (dataset)   ← RUN
  groups: [type-model-run]
  extras: model=ct-segmentation, git_commit, docker_tag, status, ...
  resources: config.yaml, segmentation.nii.gz, metrics.json
```

**Navigation at scale:**

| Question | How |
|---|---|
| Browse all models | `/group/type-model` |
| All runs of model X (UI) | Search: `extras_model:X` |
| All runs of model X (API) | `package_search?q=extras_model:X` |
| All runs across all models | `/group/type-model-run` |
| All imaging datasets | facet: `domain=imaging` |
| All failed runs | facet: `status=failed` |

---

## Workflow: Add a New Model

A model is code-only (Python + Docker). In CKAN it gets a single **model descriptor Dataset** placed in the `type-model` group.

```python
import requests

CKAN_URL = "https://ckan.episerve.zib.de"
headers  = {"Authorization": "<api-token>"}

vocabs = {v["name"]: v["id"] for v in
    requests.get(f"{CKAN_URL}/api/3/action/vocabulary_list").json()["result"]}

# Model descriptor dataset — carries all metadata, no group needed per model
requests.post(f"{CKAN_URL}/api/3/action/package_create", headers=headers, json={
    "name":      "<name>",
    "title":     "<name>",
    "notes":     f"Full description of what the model does.\n\n### [Browse all runs →]({CKAN_URL}/dataset?q=extras_model:<name>)",
    "owner_org": "episerve",
    "url":       "https://github.com/episerve/<name>",
    "groups":    [{"name": "type-model"}],
    "tags": [
        {"name": "<domain>",   "vocabulary_id": vocabs["domain"]},    # genomics | clinical | imaging | metabolomics
        {"name": "<modality>", "vocabulary_id": vocabs["modality"]},  # rna-seq | wgs | ct-scan | mri
    ],
    "extras": [
        {"key": "dataset_type",    "value": "model"},
        {"key": "docker_image",    "value": "ghcr.io/episerve/<name>"},
        {"key": "algorithm",       "value": "<algorithm>"},
        {"key": "input_format",    "value": "<input formats>"},
        {"key": "output_format",   "value": "<output formats>"},
        {"key": "lead_researcher", "value": "<name>"},
        {"key": "paper_doi",       "value": "<doi>"},          # optional
    ],
})
```

> If the model produces trained artifacts (weights, ONNX files) stored in lakeFS, add them as resources on the descriptor dataset.

---

## Workflow: Add a New Run Result

Each execution of a model produces a **Dataset** in `type-model-run` with `extras.model` pointing back to the model descriptor. Resources are the input config and all output files, referenced as lakeFS URLs.

```python
import requests

CKAN_URL = "https://ckan.episerve.zib.de"
headers  = {"Authorization": "<api-token>"}

# fetch vocabulary ids once
vocabs = {v["name"]: v["id"] for v in
    requests.get(f"{CKAN_URL}/api/3/action/vocabulary_list").json()["result"]}

def vtag(vocab, value):
    return {"name": value, "vocabulary_id": vocabs[vocab]}

GIT_COMMIT  = "a3f9c12"
MODEL_NAME  = "my-example-model1"
RUN_DATE    = "20260528"

# 1. Create the run dataset
pkg = requests.post(f"{CKAN_URL}/api/3/action/package_create", headers=headers, json={
    "name":      f"{MODEL_NAME}-run-{RUN_DATE}-{GIT_COMMIT}",
    "title":     f"{MODEL_NAME} · run {GIT_COMMIT}",
    "notes":     "Short description of this run.",
    "owner_org": "episerve",
    "groups": [{"name": "type-model-run"}],
    "tags": [
        vtag("domain",   "genomics"),   # genomics | clinical | imaging | metabolomics
        vtag("status",   "success"),    # success | failed | running
        vtag("modality", "rna-seq"),    # rna-seq | wgs | ct-scan | mri
    ],
    "extras": [
        {"key": "model",         "value": MODEL_NAME},
        {"key": "git_commit",    "value": GIT_COMMIT},
        {"key": "docker_tag",    "value": "1.4.2"},
        {"key": "run_timestamp", "value": "2026-05-28T09:00:00Z"},
        {"key": "status",        "value": "success"},
    ],
}).json()["result"]

# 2. Attach input config and output files
base = f"lakefs://sandbox/main/runs/{GIT_COMMIT}"
for name, url, desc in [
    ("config.yaml",      f"{base}/config.yaml",      "Input configuration"),
    ("results.parquet",  f"{base}/results.parquet",  "Model output"),
    ("metrics.json",     f"{base}/metrics.json",      "Run metrics"),
]:
    requests.post(f"{CKAN_URL}/api/3/action/resource_create", headers=headers, json={
        "package_id":  pkg["id"],
        "name":        name,
        "url":         url,
        "format":      name.split(".")[-1].upper(),
        "description": desc,
    })
```

> Avoid using `type` or `author` as extra keys — these clash with CKAN built-in fields. Use `dataset_type` and `created_by` instead.

---

## Workflow: Add a Plain Dataset

Reports, raw data, and other items that are not model runs follow the same pattern but use `type-report` or `type-raw-data` as their group.

```python
import requests

CKAN_URL = "https://ckan.episerve.zib.de"
headers  = {"Authorization": "<api-token>"}

vocabs = {v["name"]: v["id"] for v in
    requests.get(f"{CKAN_URL}/api/3/action/vocabulary_list").json()["result"]}

# Raw data example
pkg = requests.post(f"{CKAN_URL}/api/3/action/package_create", headers=headers, json={
    "name":      "cohort-b-ct-batch-1",
    "title":     "Cohort B — CT Batch 1",
    "notes":     "CT scans for Cohort B, first batch.",
    "owner_org": "episerve",
    "groups":    [{"name": "type-raw-data"}],
    "tags": [
        {"name": "imaging",  "vocabulary_id": vocabs["domain"]},
        {"name": "ct-scan",  "vocabulary_id": vocabs["modality"]},
    ],
    "extras": [
        {"key": "dataset_type", "value": "raw-data"},
        {"key": "cohort",       "value": "cohort-b"},
        {"key": "n_samples",    "value": "64"},
    ],
}).json()["result"]

requests.post(f"{CKAN_URL}/api/3/action/resource_create", headers=headers, json={
    "package_id": pkg["id"],
    "name":       "scans.tar.gz",
    "url":        "lakefs://sandbox/main/raw/cohort-b/batch1/scans.tar.gz",
    "format":     "GZ",
})
```

For a **report**, use `"groups": [{"name": "type-report"}]` and add extras like `dataset_type: report`, `period: 2026-Q1`.

---

## Connection to lakeFS

CKAN and lakeFS have complementary roles:

| | CKAN | lakeFS |
|---|---|---|
| **Stores** | Metadata, catalog entries, provenance | Actual files |
| **Answers** | What exists? Who made it? How? | Where is it? What changed? |
| **Links** | Resource URLs pointing into lakeFS | Versioned file storage |

### URL convention

All files referenced in CKAN resources use lakeFS URLs:

```
lakefs://<repository>/<branch>/<path>
```

| Content | lakeFS path |
|---|---|
| Run input config | `lakefs://sandbox/main/runs/<git_commit>/config.yaml` |
| Run output files | `lakefs://sandbox/main/runs/<git_commit>/<filename>` |
| Raw data | `lakefs://sandbox/main/raw/<cohort>/<batch>/<filename>` |
| Reports | `lakefs://sandbox/main/reports/<period>/<filename>` |
| Model artifacts | `lakefs://sandbox/main/models/<model>/<version>/<filename>` |

### Branching model

- **`main` branch** — stable, reviewed data. All CKAN resources point here.
- **Feature branches** — in-progress runs. Commit to `main` and register in CKAN only after a run completes successfully.

```
run starts  → write outputs to lakefs://sandbox/run-<id>/...
run succeeds → merge branch into main
              → create CKAN dataset pointing to lakefs://sandbox/main/runs/<id>/...
run fails   → mark dataset status=failed (or skip CKAN registration)
```

### Traceability chain

```
CKAN dataset (model descriptor)       e.g. ct-segmentation
  └── url (Source)                    → GitHub repository
  └── extras.docker_image             → GHCR image
        ↑ linked via extras.model
CKAN dataset (run)                    e.g. ct-segmentation-run-20260515-f1a8b99
  └── extras.model                    → ct-segmentation (model descriptor name)
  └── extras.git_commit               → exact code version in GitHub
  └── extras.docker_tag               → exact image tag in GHCR
  └── resource: config                → lakefs://sandbox/main/runs/<id>/config.yaml
  └── resource: output                → lakefs://sandbox/main/runs/<id>/results.parquet
                                            └── lakeFS commit history → who, when, from what branch
```

From any output file in lakeFS you can navigate up to the CKAN run entry, then to the model descriptor, and from there to the exact git commit and Docker image that produced it.
