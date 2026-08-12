#!/usr/bin/env python
"""Sync local NSO notebooks with the Databricks workspace.

    python scripts/databricks_sync.py pull    # workspace -> repo
    python scripts/databricks_sync.py push    # repo -> workspace
    python scripts/databricks_sync.py diff    # show what differs

The workspace copy is what runs. Use `diff` before pushing, especially after
manual notebook edits in the Databricks UI. Set NSO_WS_BASE to target another
workspace folder.
"""
import argparse
import base64
import difflib
import os
import sys

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ExportFormat, ImportFormat, Language

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
NOTEBOOK_DIR = os.path.join(REPO_ROOT, "notebooks")

WS_BASE = os.environ.get(
    "NSO_WS_BASE",
    "/Workspace/Users/tuckeyhue@gmail.com/Market Research/1. Data ETL/3. NSO",
)

EXT_TO_LANG = {".py": Language.PYTHON, ".sql": Language.SQL, ".scala": Language.SCALA, ".r": Language.R}
LANG_TO_EXT = {"PYTHON": ".py", "SQL": ".sql", "SCALA": ".scala", "R": ".r"}

BOM = b"\xef\xbb\xbf"


def client():
    profile = os.environ.get("DATABRICKS_CONFIG_PROFILE", "DEFAULT")
    if os.environ.get("DATABRICKS_HOST") and os.environ.get("DATABRICKS_TOKEN"):
        return WorkspaceClient()
    return WorkspaceClient(profile=profile)


def workspace_files(w, ws_path=WS_BASE, rel=""):
    """Yield (relative_path_with_ext, workspace_path, is_notebook)."""
    for obj in sorted(w.workspace.list(ws_path), key=lambda o: o.path):
        name = obj.path.rsplit("/", 1)[-1]
        kind = obj.object_type.value
        if kind == "DIRECTORY":
            yield from workspace_files(w, obj.path, os.path.join(rel, name))
        elif kind == "NOTEBOOK":
            ext = LANG_TO_EXT.get(obj.language.value if obj.language else "", ".py")
            yield os.path.join(rel, name + ext), obj.path, True
        elif kind == "FILE":
            yield os.path.join(rel, name), obj.path, False


def repo_files():
    for root, _, files in os.walk(NOTEBOOK_DIR):
        for name in sorted(files):
            full = os.path.join(root, name)
            yield os.path.relpath(full, NOTEBOOK_DIR), full


def fetch(w, ws_path):
    res = w.workspace.export(ws_path, format=ExportFormat.SOURCE)
    return base64.b64decode(res.content).replace(BOM, b"")


def cmd_pull(w, args):
    changed = 0
    for rel, ws_path, _ in workspace_files(w):
        data = fetch(w, ws_path)
        local = os.path.join(NOTEBOOK_DIR, rel)
        os.makedirs(os.path.dirname(local), exist_ok=True)
        old = open(local, "rb").read() if os.path.exists(local) else None
        if old == data:
            continue
        if not args.dry_run:
            with open(local, "wb") as fh:
                fh.write(data)
        print(f"  {'would update' if args.dry_run else 'updated'}  {rel}")
        changed += 1
    print(f"\n{changed} file(s) {'would change' if args.dry_run else 'changed'}")


def cmd_push(w, args):
    remote = {rel: ws for rel, ws, _ in workspace_files(w)}
    changed = 0
    for rel, local in repo_files():
        stem, ext = os.path.splitext(rel)
        is_notebook = ext.lower() in EXT_TO_LANG
        ws_path = f"{WS_BASE}/" + (stem if is_notebook else rel).replace("\\", "/")

        data = open(local, "rb").read().replace(BOM, b"")
        if rel in remote:
            if fetch(w, remote[rel]) == data:
                continue
        if args.dry_run:
            print(f"  would upload  {rel}")
            changed += 1
            continue
        if is_notebook:
            w.workspace.import_(
                path=ws_path, format=ImportFormat.SOURCE,
                language=EXT_TO_LANG[ext.lower()],
                content=base64.b64encode(data).decode("ascii"), overwrite=True,
            )
        else:
            w.workspace.upload(path=ws_path, content=data, overwrite=True)
        print(f"  uploaded  {rel}")
        changed += 1
    print(f"\n{changed} file(s) {'would upload' if args.dry_run else 'uploaded'}")


def cmd_diff(w, args):
    remote = {rel: ws for rel, ws, _ in workspace_files(w)}
    local = dict(repo_files())

    only_remote = sorted(set(remote) - set(local))
    only_local = sorted(set(local) - set(remote))
    both = sorted(set(remote) & set(local))

    for rel in only_remote:
        print(f"  workspace only : {rel}")
    for rel in only_local:
        print(f"  repo only      : {rel}")

    differing = 0
    for rel in both:
        rdata = fetch(w, remote[rel]).decode("utf-8", "replace").splitlines()
        ldata = open(local[rel], "rb").read().replace(BOM, b"").decode("utf-8", "replace").splitlines()
        if rdata == ldata:
            continue
        differing += 1
        print(f"\n  differs        : {rel}")
        if args.verbose:
            for line in difflib.unified_diff(rdata, ldata, "workspace", "repo", lineterm="", n=1):
                print("      " + line)

    print(f"\n{len(only_remote)} workspace-only, {len(only_local)} repo-only, {differing} differing")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("command", choices=["pull", "push", "diff"])
    parser.add_argument("--dry-run", action="store_true", help="report changes without writing")
    parser.add_argument("-v", "--verbose", action="store_true", help="show line diffs (diff only)")
    args = parser.parse_args()

    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    w = client()
    print(f"workspace : {WS_BASE}")
    print(f"repo      : {NOTEBOOK_DIR}")
    print(f"host      : {w.config.host}\n")

    {"pull": cmd_pull, "push": cmd_push, "diff": cmd_diff}[args.command](w, args)


if __name__ == "__main__":
    main()
