# HOWTO: Run a Databricks Notebook on a Cluster (Agent Handoff)

Audience: another agent that needs to execute a notebook in Databricks and read its result.
This is the verified pattern used in this workspace (real-estate, banking-news, customs, NSO pipelines).

---

## 0. Credentials (do this first)

Credentials live in a private local file — **never print the token**:

```
C:\Users\Genius\.openclaw\workspace\dbx config.txt
```

Load them into the current PowerShell process, then verify:

```powershell
$cfg='C:\Users\Genius\.openclaw\workspace\dbx config.txt'
Get-Content $cfg | ForEach-Object {
  if ($_ -match '^\s*(DATABRICKS_[A-Z0-9_]+)\s*=\s*"?([^"#]+)"?\s*$') {
    Set-Item -Path "Env:$($matches[1])" -Value $matches[2].Trim()
  }
}

databricks --version                                   # expect Version 0.18.0
databricks workspace ls "$env:DATABRICKS_DEFAULT_FOLDER_PATH"
```

The file sets at least `DATABRICKS_HOST`, `DATABRICKS_TOKEN`, `DATABRICKS_DEFAULT_FOLDER_PATH`.
Workspace host in use: `https://dbc-5a6b7518-84a8.cloud.databricks.com` (org `o=7474657041045699`).

Treat `dbx config.txt` as secret. If you must show it, redact `DATABRICKS_TOKEN` and anything token/password-like.

---

## 1. Decide the compute target

There are two working paths. **Check for an all-purpose cluster first:**

```powershell
databricks clusters list --output JSON
```

- Non-empty result → you have an all-purpose cluster; use **Path A** (`existing_cluster_id`).
- Returns `{}` / empty → no all-purpose cluster exists. Use **Path B (serverless)**. This is the common case in this account, and it is the pattern that has been used successfully across projects.

> Notebooks in Databricks cannot run "on nothing" — they need either an all-purpose cluster, a job cluster, or serverless compute attached to the run.

---

## Path A — Run on an existing (all-purpose or job) cluster

Grab the `cluster_id` from the list above. Two options:

### A1. Quick one-off notebook run via Jobs "runs submit"

```powershell
$payload = @{
  run_name = "adhoc_notebook_run"
  tasks = @(
    @{
      task_key = "run_nb"
      existing_cluster_id = "REPLACE_WITH_CLUSTER_ID"
      notebook_task = @{
        notebook_path = "/Users/tuckeyhue@gmail.com/.../01_my_notebook"
        base_parameters = @{ catalog = "market_data"; schema = "real_estate"; year = "2026"; month = "6" }
      }
    }
  )
} | ConvertTo-Json -Depth 10

databricks jobs submit --json $payload
```

This returns a `run_id`. Poll it (see section 3).

### A2. Multi-task workflow job (notebook DAG)

Use a job template like `real_estate_price_sprint007_databricks/06_jobs/monthly_workflow_job_template.json`.
Each task points at a notebook and carries `existing_cluster_id`. Replace every `REPLACE_WITH_CLUSTER_ID`, then:

```powershell
databricks jobs create --json @path\to\job_template.json     # returns job_id
databricks jobs run-now --job-id <JOB_ID> --json '{ "job_parameters": { "year": "2026", "month": "6", "project_code": "ALL" } }'
```

Reference working job: `real_estate_price_monthly_manual_pipeline`, job id `256911246994174` (manual monthly, no schedule).

---

## Path B — Run on Serverless compute (default when no cluster exists)

This is the reliable pattern used for banking-news, customs, hyundai/vinfast, and real-estate runs.
It uses **Jobs API 2.1 `/jobs/runs/submit`** with a serverless environment instead of a cluster id.

Required serverless knobs:

- `environment_key = "Default"`
- `environment_version = "5"` (spec: `environments[].spec.environment_version`)
- `performance_target = "PERFORMANCE_OPTIMIZED"`

Example (raw REST via curl; works the same through the CLI's `--json`):

```bash
curl -sS -X POST "$DATABRICKS_HOST/api/2.1/jobs/runs/submit" \
  -H "Authorization: Bearer $DATABRICKS_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "run_name": "adhoc_serverless_run",
    "performance_target": "PERFORMANCE_OPTIMIZED",
    "environments": [
      { "environment_key": "Default",
        "spec": { "client": "1", "environment_version": "5" } }
    ],
    "tasks": [
      {
        "task_key": "run_nb",
        "environment_key": "Default",
        "notebook_task": {
          "notebook_path": "/Users/tuckeyhue@gmail.com/.../01_my_notebook",
          "base_parameters": { "catalog": "market_data", "schema": "customs", "year": "2026", "month": "6" }
        }
      }
    ]
  }'
```

Response contains `run_id`. Notes:
- No `existing_cluster_id` / no `new_cluster` when using serverless — the `environment_key` supplies compute.
- For a multi-task DAG, give every task its own `task_key`, `depends_on`, and `environment_key = "Default"`.

---

## 2. How notebooks read parameters

Notebooks receive `base_parameters` as widgets:

```python
dbutils.widgets.text("year", "2026")
dbutils.widgets.text("month", "6")
year  = dbutils.widgets.get("year")
month = dbutils.widgets.get("month")
```

Common run-id convention in these pipelines: `YYYY-MM-PROJECT_CODE` (e.g. `2026-06-ALL`).

---

## 3. Poll the run and get status / output

```powershell
# Overall run
databricks jobs get-run <RUN_ID> --output JSON
# Look at: state.life_cycle_state (PENDING/RUNNING/TERMINATED) and state.result_state (SUCCESS/FAILED)

# Per-task notebook output (dbutils.notebook.exit(...) value + truncation flag)
databricks jobs get-run-output <TASK_RUN_ID> --output JSON
```

Equivalent REST: `GET /api/2.1/jobs/runs/get?run_id=...` and `GET /api/2.1/jobs/runs/get-output?run_id=<task_run_id>`.

Poll with backoff (don't tight-loop). Terminal states: `TERMINATED`, `SKIPPED`, `INTERNAL_ERROR`.
`result_state = SUCCESS` means the notebook finished cleanly.

To surface a result to the caller, have the notebook end with:

```python
dbutils.notebook.exit(json.dumps({"rows": n, "status": "SUCCESS"}))
```

Then read it from `get-run-output`.

---

## 4. Importing / updating the notebook itself

- **Notebooks** (SOURCE objects): `databricks workspace import --language PYTHON --format SOURCE --overwrite <path> <local.py>`
  or `databricks workspace import-dir <local_dir> <workspace_dir> --overwrite`.
- **Plain workspace FILE objects** (e.g. app.py, data.py): use the **workspace-files API**, not notebook import.
- **Vietnamese-text gotcha:** the old Windows Databricks CLI mangles Vietnamese diacritics on import. For runtime-critical Vietnamese, write it as ASCII-safe Python `\uXXXX` unicode escapes in the notebook source, or import via the files API with explicit UTF-8.

---

## 5. Quick decision tree

1. Load creds (section 0), `databricks clusters list --output JSON`.
2. Cluster exists → **Path A** with its `existing_cluster_id`.
3. No cluster → **Path B serverless** (`environment_key=Default`, `environment_version=5`, `PERFORMANCE_OPTIMIZED`).
4. Submit → capture `run_id` → poll `get-run` → read `get-run-output`.
5. `result_state=SUCCESS` = done. On `FAILED`, read the task error in `get-run-output` and patch the notebook.

---

## Reference facts (this account)

- Host: `https://dbc-5a6b7518-84a8.cloud.databricks.com`, org `o=7474657041045699`
- Notebook root: `/Users/tuckeyhue@gmail.com/...`
- CLI version: `0.18.0`
- Known job: `real_estate_price_monthly_manual_pipeline` (id `256911246994174`), manual monthly, target schema `market_data.real_estate`
- Serverless run pattern is the default here because `clusters list` typically returns empty.
