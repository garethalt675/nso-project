#!/usr/bin/env python
"""Export NSO Databricks job settings to jobs/*.json.

    python scripts/export_jobs.py

Only job settings are written; run state stays out of the repo.
"""
import json
import os
import re
import sys

from databricks.sdk import WorkspaceClient

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
JOBS_DIR = os.path.join(REPO_ROOT, "jobs")

JOB_NAME_PREFIX = "nso"


def main():
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    os.makedirs(JOBS_DIR, exist_ok=True)

    profile = os.environ.get("DATABRICKS_CONFIG_PROFILE", "DEFAULT")
    w = WorkspaceClient() if os.environ.get("DATABRICKS_TOKEN") else WorkspaceClient(profile=profile)

    found = 0
    for job in w.jobs.list():
        name = job.settings.name if job.settings else ""
        if not name or not name.lower().startswith(JOB_NAME_PREFIX.lower()):
            continue
        settings = w.jobs.get(job.job_id).settings.as_dict()
        slug = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")
        path = os.path.join(JOBS_DIR, f"{slug}.json")
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(settings, fh, indent=2, ensure_ascii=False)
            fh.write("\n")
        print(f"  {slug}.json   ({len(settings.get('tasks', []))} tasks)  <- job {job.job_id}")
        found += 1

    print(f"\n{found} job definition(s) exported to {JOBS_DIR}")
    if not found:
        print("  (nothing matched the prefix "
              f"{JOB_NAME_PREFIX!r} - check you are on the right workspace)")


if __name__ == "__main__":
    main()
