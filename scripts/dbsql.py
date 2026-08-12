"""Small Databricks SQL client that only depends on `requests`.

Credential lookup:
  1. DATABRICKS_HOST / DATABRICKS_TOKEN
  2. host / token
  3. ~/.databrickscfg [DEFAULT]

Usage:
    from dbsql import run, show
    cols, rows = run("SELECT count(*) FROM curated_indicators_long")
    show("SELECT * FROM qc_semantic_validation", "semantic QC")

CLI:
    python scripts/dbsql.py "SELECT count(*) FROM market_data.nso.curated_indicators_long"
    python scripts/dbsql.py --file docs/qc_gap_checks.sql     # runs every statement
"""

from __future__ import annotations

import argparse
import configparser
import json
import os
import pathlib
import sys
import time

import requests

WAREHOUSE = os.environ.get("DATABRICKS_WAREHOUSE_ID", "7eb5fd2336243915")
CATALOG = os.environ.get("DATABRICKS_CATALOG", "market_data")
SCHEMA = os.environ.get("DATABRICKS_SCHEMA", "nso")
WS_BASE = os.environ.get(
    "DATABRICKS_NOTEBOOK_PATH",
    "/Users/tuckeyhue@gmail.com/Market Research/1. Data ETL/3. NSO",
)


def resolve_auth():
    """Return (host, token)."""
    host = os.environ.get("DATABRICKS_HOST") or os.environ.get("host")
    token = os.environ.get("DATABRICKS_TOKEN") or os.environ.get("token")
    if not (host and token):
        cfg_path = pathlib.Path.home() / ".databrickscfg"
        if cfg_path.exists():
            cfg = configparser.ConfigParser()
            cfg.read(cfg_path)
            profile = os.environ.get("DATABRICKS_CONFIG_PROFILE", "DEFAULT")
            if cfg.has_section(profile) or profile == "DEFAULT":
                host = host or cfg.get(profile, "host", fallback=None)
                token = token or cfg.get(profile, "token", fallback=None)
    if not (host and token):
        raise SystemExit(
            "No Databricks credentials. Set DATABRICKS_HOST/DATABRICKS_TOKEN, or lowercase "
            "host/token, or add a [DEFAULT] profile to ~/.databrickscfg."
        )
    return host.rstrip("/"), token


HOST, TOKEN = resolve_auth()
HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}


def run(sql, catalog=CATALOG, schema=SCHEMA, warehouse=WAREHOUSE, wait="50s", timeout=120):
    """Execute one SQL statement and return (columns, rows)."""
    resp = requests.post(
        f"{HOST}/api/2.0/sql/statements",
        headers=HEADERS,
        json={
            "statement": sql,
            "warehouse_id": warehouse,
            "wait_timeout": wait,
            "catalog": catalog,
            "schema": schema,
            "format": "JSON_ARRAY",
            "disposition": "INLINE",
        },
        timeout=timeout,
    )
    resp.raise_for_status()
    payload = resp.json()
    statement_id = payload["statement_id"]
    while payload["status"]["state"] in ("PENDING", "RUNNING"):
        time.sleep(3)
        payload = requests.get(
            f"{HOST}/api/2.0/sql/statements/{statement_id}", headers=HEADERS, timeout=60
        ).json()
    state = payload["status"]["state"]
    if state != "SUCCEEDED":
        raise RuntimeError(f"{state}: {json.dumps(payload['status'])[:800]}")
    columns = [c["name"] for c in payload["manifest"]["schema"]["columns"]]
    return columns, payload.get("result", {}).get("data_array", []) or []


def show(sql, title=None, maxrows=60, width=48):
    """Run a statement and print it as a padded table. Returns (columns, rows)."""
    if title:
        print(f"\n=== {title} ===")
    try:
        columns, rows = run(sql)
    except Exception as exc:  # noqa: BLE001 - surface the server message, keep going
        print(f"  !! ERROR: {str(exc)[:500]}")
        return None, None
    widths = [
        min(max(len(str(c)), *(len(str(r[i])) for r in rows[:maxrows])) if rows else len(str(c)), width)
        for i, c in enumerate(columns)
    ]
    print("  " + " | ".join(str(c)[:width].ljust(widths[i]) for i, c in enumerate(columns)))
    print("  " + "-+-".join("-" * w for w in widths))
    for row in rows[:maxrows]:
        print("  " + " | ".join(
            (str(v)[:width] if v is not None else "NULL").ljust(widths[i]) for i, v in enumerate(row)
        ))
    if len(rows) > maxrows:
        print(f"  ... {len(rows) - maxrows} more rows")
    return columns, rows


def split_statements(text):
    """Split a .sql file into executable statements."""
    body = "\n".join(l for l in text.splitlines() if not l.strip().startswith("--"))
    out = []
    for stmt in body.split(";"):
        stmt = stmt.strip()
        if stmt and not stmt.upper().startswith("USE "):
            out.append(stmt)
    return out


def export_notebook(name, fmt="SOURCE"):
    """Export one workspace notebook as source text. `name` is the bare name, no extension."""
    import base64

    resp = requests.get(
        f"{HOST}/api/2.0/workspace/export",
        headers=HEADERS,
        params={"path": f"{WS_BASE}/{name}", "format": fmt},
        timeout=120,
    )
    resp.raise_for_status()
    return base64.b64decode(resp.json()["content"]).decode("utf-8", errors="replace")


def import_notebook(name, local_path, language="PYTHON"):
    """Overwrite one workspace notebook from a local file, then verify the round-trip."""
    import base64

    data = pathlib.Path(local_path).read_bytes().replace(b"\xef\xbb\xbf", b"")
    resp = requests.post(
        f"{HOST}/api/2.0/workspace/import",
        headers=HEADERS,
        json={
            "path": f"{WS_BASE}/{name}",
            "format": "SOURCE",
            "language": language,
            "content": base64.b64encode(data).decode(),
            "overwrite": True,
        },
        timeout=120,
    )
    resp.raise_for_status()
    back = export_notebook(name)
    ok = back.replace("\r\n", "\n").strip() == data.decode("utf-8", "replace").replace("\r\n", "\n").strip()
    print(f"  uploaded {name}: round-trip identical = {ok}")
    return ok


def wait_for_run(run_id, poll=20, timeout=3600):
    """Block until a job run's PARENT reaches a terminal life_cycle_state.

    Per-task result_states appear earlier; stopping on those reports success too soon.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        d = requests.get(
            f"{HOST}/api/2.1/jobs/runs/get", headers=HEADERS, params={"run_id": run_id}, timeout=60
        ).json()
        state = d.get("state", {})
        life = state.get("life_cycle_state")
        if life in ("TERMINATED", "INTERNAL_ERROR", "SKIPPED"):
            return life, state.get("result_state"), state.get("state_message")
        time.sleep(poll)
    raise TimeoutError(f"run {run_id} still running after {timeout}s")


def main():
    ap = argparse.ArgumentParser(description="Run SQL against the NSO Databricks warehouse.")
    ap.add_argument("sql", nargs="?", help="statement to run")
    ap.add_argument("--file", help="path to a .sql file; every statement in it is run")
    ap.add_argument("--maxrows", type=int, default=60)
    args = ap.parse_args()

    if args.file:
        statements = split_statements(pathlib.Path(args.file).read_text())
        failed = 0
        for i, stmt in enumerate(statements, 1):
            cols, rows = show(stmt, f"[{i}/{len(statements)}] {' '.join(stmt.split())[:70]}", args.maxrows)
            failed += cols is None
        print(f"\n=== {len(statements) - failed} OK, {failed} failed ===")
        return 1 if failed else 0
    if not args.sql:
        ap.error("give a statement or --file")
    show(args.sql, maxrows=args.maxrows)
    return 0


if __name__ == "__main__":
    sys.exit(main())
