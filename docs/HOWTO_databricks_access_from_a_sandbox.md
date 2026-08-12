# Connecting to Databricks from a cloud sandbox

Notes from a cloud sandbox session (2026-07-26) that had to get from "no visible
credentials" to running SQL and pushing a notebook. Read this before concluding a sandbox has no
Databricks access; the first pass wasted several steps on that assumption and was wrong.

## 1. The credentials are there, under lowercase names

`~/.databrickscfg` does **not** exist in the sandbox, and `DATABRICKS_HOST` / `DATABRICKS_TOKEN` are
**not** set. Some environments inject them as lowercase `host` and `token` instead:

```python
os.environ["host"]   # https://dbc-5a6b7518-84a8.cloud.databricks.com
os.environ["token"]  # dapi... (36 chars); never print it
```

A `env | grep -i databricks` finds them only by accident (the *value* matches, not the name), so
scan for both spellings:

```python
[k for k in os.environ if k.lower() in ("host", "token") or "DATABRICKS" in k.upper()]
```

`scripts/dbsql.py` resolves all three sources (uppercase env, lowercase env, `~/.databrickscfg`)
so you should not have to think about this again.

For the SDK-based tooling, map them explicitly; it only reads the uppercase names:

```bash
DATABRICKS_HOST="$host" DATABRICKS_TOKEN="$token" python scripts/databricks_sync.py diff
```

## 2. `databricks-sdk` is not preinstalled

```bash
pip install databricks-sdk    # pulls google-auth + protobuf as dependencies
```

## 3. If the SDK import aborts the interpreter

Twice in that session, importing the SDK killed the process outright:

```
File ".../google/auth/crypt/es.py", line 21, in <module>
    import cryptography.exceptions
File "/usr/lib/python3/dist-packages/cryptography/exceptions.py", line 9, in <module>
    from cryptography.hazmat.bindings._rust import exceptions as rust_exceptions
pyo3_runtime.PanicException: Python API call failed
```

`databricks-sdk` imports `google.auth` eagerly, which imports `cryptography`'s Rust bindings. It is
a hard abort, not a catchable `ImportError`.

**Be sceptical of the "fix" that appears to work.** Running `pip install --upgrade cryptography`
was followed by a successful import, which looked causal, but the pip command had actually
**failed** (`Cannot uninstall cryptography 41.0.7, RECORD file not found. Hint: The package was
installed by debian.`) and left the Debian 41.0.7 in place, a single copy, unchanged. Afterwards the
panic could not be reproduced at all: the bare failing import succeeded on every subsequent attempt,
with and without importing `cryptography` first. Disk was not short (30 GB free).

So: **the trigger is unidentified and the panic appears transient.** What to actually do:

1. **Retry once** — it may simply not recur.
2. **If it recurs, use `scripts/dbsql.py`.** It talks to the REST API with only
   `requests` and never touches `google.auth`. Everything the 2026-07-26 data quality assessment
   needed, including SQL, notebook export/import, job submit, and polling, was done through it.

Do not add `pip install --upgrade cryptography` to a setup script on the strength of this. It did
nothing, and on a Debian-managed `cryptography` it cannot succeed.

## 4. `scripts/dbsql.py` - the SDK-free path

```bash
python scripts/dbsql.py "SELECT count(*) FROM curated_indicators_long"
python scripts/dbsql.py --file docs/qc_gap_checks.sql       # runs every statement, reports failures
```

```python
import sys; sys.path.insert(0, "scripts")
from dbsql import run, show, export_notebook, import_notebook, wait_for_run

show("SELECT * FROM qc_semantic_validation", "semantic QC")
import_notebook("4_Curated", "notebooks/4_Curated.py")     # verifies the round-trip
life, result, msg = wait_for_run(run_id)                    # waits on the PARENT run
```

The warehouse (`7eb5fd2336243915`) auto-resumes from STOPPED, so the first query after an idle
period legitimately takes ~30s. That is not a hang.

## 5. `databricks_sync.py diff` and `push` disagree, and both are right

`diff` normalises line endings; `push` compares raw bytes (`fetch(w, remote[rel]) == data`). A
notebook differing only by a trailing newline shows as **identical** under `diff` and **needs
upload** under `push`. On this repo, `00_specs_nso_socioeconomic_reports.sql` is in exactly that
state and `push` will always want to re-upload it.

That matters when deploying one notebook during a production fix: `push` would also rewrite an
unrelated one. Use `import_notebook()` from `dbsql.py` to deploy exactly the file you changed, and
keep `push` for the deliberate full sync.

## 6. Order of operations that worked

```bash
# 1. confirm credentials exist (redact!)
python -c "import os; t=os.environ.get('token',''); print(bool(t), len(t))"
# 2. connectivity
python scripts/dbsql.py "SELECT current_catalog(), current_schema(), current_user()"
# 3. confirm the repo mirrors the workspace before editing
DATABRICKS_HOST="$host" DATABRICKS_TOKEN="$token" python scripts/databricks_sync.py diff
# 4. edit, deploy the single notebook, verify round-trip
python -c "import sys;sys.path.insert(0,'scripts');from dbsql import import_notebook as i;i('4_Curated','notebooks/4_Curated.py')"
# 5. run, then wait on the PARENT run
python -c "import sys;sys.path.insert(0,'scripts');from dbsql import wait_for_run;print(wait_for_run(<run_id>))"
```
