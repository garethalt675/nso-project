# -*- coding: utf-8 -*-
"""Load the non-Spark functions from the Step 3 notebook for local tests."""
import ast
import hashlib
import json
import os
import re
import unicodedata
from datetime import date, datetime

HERE = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(HERE)
DEFAULT_SRC = os.path.join(REPO_ROOT, "notebooks", "3_Extract_Tables.py")


def load(src_path=DEFAULT_SRC):
    with open(src_path, encoding="utf-8") as fh:
        text = fh.read()
    tree = ast.parse(text)
    chunks = []
    for node in tree.body:
        if not isinstance(node, (ast.FunctionDef, ast.Assign)):
            continue
        seg = ast.get_source_segment(text, node)
        if seg is None:
            continue
        if re.search(r"\bspark\b|\bdbutils\b|\bdisplay\(|StructType|StructField|\bF\.", seg):
            continue
        chunks.append(seg)
    namespace = {
        "re": re,
        "json": json,
        "hashlib": hashlib,
        "unicodedata": unicodedata,
        "datetime": datetime,
        "date": date,
        "os": os,
    }
    exec("\n\n".join(chunks), namespace)
    return namespace


if __name__ == "__main__":
    ns = load()
    funcs = sorted(k for k, v in ns.items() if callable(v) and getattr(v, "__module__", None) is None)
    print(f"loaded {len(funcs)} callables")
    for f in ["clean_cell", "to_number", "norm", "row_map", "infer_header_and_data_rows",
              "header_for_col", "infer_title", "parse_cells", "split_row_label",
              "classify_sheet", "infer_unit_raw"]:
        print(f"  {f}: {'OK' if f in ns else 'MISSING'}")
