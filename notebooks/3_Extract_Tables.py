# Databricks notebook source
# DBTITLE 1,NSO Step 3 - Deterministic Sheet-Specific Excel Extraction
# MAGIC %md
# MAGIC # Step 3: Deterministic sheet-specific extraction
# MAGIC
# MAGIC Extraction approach:
# MAGIC - Treat each Excel sheet as a topical table/source section.
# MAGIC - Classify sheets deterministically from sheet name + title/header text.
# MAGIC - Apply topic-specific routing rules for row dimensions (`geography`, `sector`, `product`, etc.).
# MAGIC - Preserve raw cell lineage and a unified long table.
# MAGIC - Also write manageable topic tables named like `industry_report`, `trade_prices_report`, etc.

# COMMAND ----------

import hashlib
import json
import re
import unicodedata
from datetime import datetime

from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

CATALOG = "market_data"
SCHEMA = "nso"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

PARSED_TABLE = f"{CATALOG}.{SCHEMA}.parsed_workbooks_raw"
REPORTS_TABLE = f"{CATALOG}.{SCHEMA}.nso_reports_url"
LOG_TABLE = f"{CATALOG}.{SCHEMA}.document_processing_log"
SHEET_REPORTS_TABLE = f"{CATALOG}.{SCHEMA}.sheet_reports"
SHEET_LOG_TABLE = f"{CATALOG}.{SCHEMA}.sheet_processing_log"

CELLS_TABLE = f"{CATALOG}.{SCHEMA}.extracted_cells_long"
INVENTORY_TABLE = f"{CATALOG}.{SCHEMA}.extracted_table_inventory"
LONG_TABLE = f"{CATALOG}.{SCHEMA}.extracted_indicators_long"

# Topic-specific report tables.
TOPIC_TABLES = {
    "national_accounts": "national_accounts_report",
    "agriculture_forestry_fishery": "agriculture_forestry_fishery_report",
    "industry": "industry_report",
    "investment_construction": "investment_construction_report",
    "enterprise_business_registration": "enterprise_business_registration_report",
    "retail_services_tourism": "retail_services_tourism_report",
    "trade_prices": "trade_prices_report",
    "transport_post_telecom": "transport_post_telecom_report",
    "population_labor_social": "population_labor_social_report",
    "environment_safety_disaster": "environment_safety_disaster_report",
    "other_or_unknown": "other_report",
}

# COMMAND ----------

# DBTITLE 1,Output tables
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CELLS_TABLE} (
  sheet_report_id STRING,
  report_id STRING NOT NULL,
  attachment_id STRING NOT NULL,
  report_url STRING,
  attachment_url STRING,
  filename STRING,
  report_year INT,
  report_month INT,
  report_quarter INT,
  period_type STRING,
  sub_category STRING,
  sheet_index INT,
  sheet_name_raw STRING,
  table_index INT,
  row_index INT,
  column_index INT,
  cell_value_raw STRING,
  cell_value_clean STRING,
  extracted_timestamp TIMESTAMP
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {INVENTORY_TABLE} (
  sheet_report_id STRING,
  table_id STRING NOT NULL,
  report_id STRING NOT NULL,
  attachment_id STRING NOT NULL,
  sheet_index INT,
  sheet_name_raw STRING,
  table_index INT,
  table_title_raw STRING,
  table_category STRING,
  indicator_domain STRING,
  indicator_subdomain STRING,
  header_rows_json STRING,
  data_start_row INT,
  data_end_row INT,
  classification_method STRING,
  classification_confidence DOUBLE,
  needs_review BOOLEAN,
  extracted_timestamp TIMESTAMP
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {LONG_TABLE} (
  sheet_report_id STRING,
  indicator_observation_id STRING NOT NULL,
  report_id STRING NOT NULL,
  attachment_id STRING NOT NULL,
  table_id STRING,
  source_row_index INT,
  source_column_index INT,
  report_year INT,
  report_month INT,
  report_quarter INT,
  period_type STRING,
  period_start_date DATE,
  period_end_date DATE,
  sub_category STRING,
  indicator_domain STRING,
  indicator_subdomain STRING,
  indicator_name_raw STRING,
  indicator_name_normalized STRING,
  geography_raw STRING,
  sector_raw STRING,
  product_raw STRING,
  unit_raw STRING,
  metric_name_raw STRING,
  metric_type STRING,
  value_numeric DOUBLE,
  value_text STRING,
  currency STRING,
  scale STRING,
  extraction_method STRING,
  extraction_confidence DOUBLE,
  needs_review BOOLEAN,
  source_filename STRING,
  source_sheet_name STRING,
  extracted_timestamp TIMESTAMP
) USING DELTA
""")

REPORT_SCHEMA_SQL = """
  sheet_report_id STRING,
  observation_id STRING NOT NULL,
  report_id STRING NOT NULL,
  attachment_id STRING NOT NULL,
  table_id STRING,
  report_year INT,
  report_month INT,
  report_quarter INT,
  period_type STRING,
  period_start_date DATE,
  period_end_date DATE,
  sub_category STRING,
  source_filename STRING,
  source_sheet_name STRING,
  sheet_index INT,
  source_row_index INT,
  source_column_index INT,
  table_title_raw STRING,
  row_label_raw STRING,
  row_label_normalized STRING,
  geography_raw STRING,
  sector_raw STRING,
  product_raw STRING,
  unit_raw STRING,
  metric_name_raw STRING,
  metric_type STRING,
  value_numeric DOUBLE,
  value_text STRING,
  currency STRING,
  scale STRING,
  extraction_method STRING,
  extraction_confidence DOUBLE,
  needs_review BOOLEAN,
  extracted_timestamp TIMESTAMP
"""

for table_name in TOPIC_TABLES.values():
    spark.sql(f"CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.{table_name} ({REPORT_SCHEMA_SQL}) USING DELTA")


spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SHEET_REPORTS_TABLE} (
  sheet_report_id STRING NOT NULL,
  report_id STRING NOT NULL,
  attachment_id STRING NOT NULL,
  sheet_index INT NOT NULL,
  sheet_name_raw STRING,
  sheet_name_normalized STRING,
  source_filename STRING,
  report_year INT,
  report_month INT,
  report_quarter INT,
  period_type STRING,
  period_start_date DATE,
  period_end_date DATE,
  indicator_domain STRING,
  indicator_subdomain STRING,
  table_title_raw STRING,
  parse_status STRING,
  extraction_status STRING,
  observation_count INT,
  needs_review BOOLEAN,
  warnings_json STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SHEET_LOG_TABLE} (
  sheet_report_id STRING NOT NULL,
  report_id STRING NOT NULL,
  attachment_id STRING NOT NULL,
  sheet_index INT NOT NULL,
  parse_status STRING,
  extraction_status STRING,
  rows_extracted INT,
  warnings_json STRING,
  error_message STRING,
  run_timestamp TIMESTAMP,
  updated_at TIMESTAMP
) USING DELTA
""")

# Existing Delta tables created by older versions need additive columns.
def add_column_if_missing(table_name, column_name, column_ddl):
    if not spark.catalog.tableExists(table_name):
        return
    cols = {c.name.lower() for c in spark.table(table_name).schema.fields}
    if column_name.lower() not in cols:
        spark.sql(f"ALTER TABLE {table_name} ADD COLUMNS ({column_ddl})")

for _tbl in [CELLS_TABLE, INVENTORY_TABLE, LONG_TABLE] + [f"{CATALOG}.{SCHEMA}.{t}" for t in TOPIC_TABLES.values()]:
    add_column_if_missing(_tbl, "sheet_report_id", "sheet_report_id STRING")

# COMMAND ----------

# DBTITLE 1,Helpers
def md5_16(value):
    return hashlib.md5(str(value).encode("utf-8")).hexdigest()[:16]

def clean_cell(value):
    return re.sub(r"\s+", " ", str(value or "").replace("\u00a0", " ")).strip()

def strip_accents(text):
    text = unicodedata.normalize("NFD", str(text or ""))
    text = "".join(ch for ch in text if unicodedata.category(ch) != "Mn")
    return text.replace("Đ", "D").replace("đ", "d")

def norm(value):
    return re.sub(r"\s+", " ", strip_accents(clean_cell(value)).lower()).strip()

# Known artifacts from legacy Vietnamese encodings. Keep corrections exact.
#
# Rules are (artifact, replacement) literals rather than regexes, restricted to letters,
# spaces, commas and semicolons. That restriction is what lets one list feed all three
# consumers -- the Python pass over incoming cells, the Spark SQL backfill, and the predicate
# that selects rows to repair -- since a literal with no metacharacters, quotes or backslashes
# needs no escaping anywhere. Anchors are added on the way out. A rule written outside the
# vocabulary is rejected below instead of turning into SQL that quietly matches nothing.
VIETNAMESE_ASCII_CORRECTIONS = [
    ("Hang hoo khoc", "Hang hoa khac"),
    ("hang hoo khoc", "hang hoa khac"),
    ("Hang hua", "Hang hoa"),
    ("hang hua", "hang hoa"),
    ("hua", "hoa"),
    ("Hua", "Hoa"),
    ("hoo", "hoa"),
    ("Hoo", "Hoa"),
    ("khoc", "khac"),
    ("Khoc", "Khac"),
    ("cho thuo", "cho thue"),
    ("Cho thuo", "Cho thue"),
    ("may muc", "may moc"),
    ("May muc", "May moc"),
    ("kho boi", "kho bai"),
    ("Kho boi", "Kho bai"),
    ("cu lion quan", "co lien quan"),
    ("Cu lion quan", "Co lien quan"),
    ("cu lien quan", "co lien quan"),
    ("Cu lien quan", "Co lien quan"),
    ("lion quan", "lien quan"),
    ("Lion quan", "Lien quan"),
    ("coc loai", "cac loai"),
    ("Coc loai", "Cac loai"),
    ("coc dich vu", "cac dich vu"),
    ("Coc dich vu", "Cac dich vu"),
    ("coc san pham", "cac san pham"),
    ("Coc san pham", "Cac san pham"),
    ("coc thiet bi", "cac thiet bi"),
    ("Coc thiet bi", "Cac thiet bi"),
    ("Bon buun", "Ban buon"),
    ("bon buun", "ban buon"),
    ("bon le", "ban le"),
    ("Bon le", "Ban le"),
    ("u tu", "o to"),
    ("U tu", "O to"),
    ("mu tu", "mo to"),
    ("Mu tu", "Mo to"),
    ("cu dong", "co dong"),
    ("Cu dong", "Co dong"),
    ("Nung, lom nghiep", "Nong, lam nghiep"),
    ("nung, lom nghiep", "nong, lam nghiep"),
    ("Nung nghiep", "Nong nghiep"),
    ("nung nghiep", "nong nghiep"),
    ("Lom nghiep", "Lam nghiep"),
    ("lom nghiep", "lam nghiep"),
    ("phEm", "pham"),
    ("thop", "thep"),
    ("tiou", "tieu"),
    ("Nghon", "Nghin"),
    ("nghon", "nghin"),
    ("tonh", "tinh"),
    ("Tonh", "Tinh"),
    ("toch", "tich"),
    ("Toch", "Tich"),
    ("moy", "may"),
    ("Moy", "May"),
    ("Tri gio", "Tri gia"),
    ("tri gio", "tri gia"),
    ("TriOu", "Trieu"),
    ("TRiOu", "Trieu"),
    ("triOu", "trieu"),
    ("Gioo duc va dao tao", "Giao duc va dao tao"),
    ("giai tro", "giai tri"),
    ("Bon buun; bon le; sua chua u tu, xe may", "Ban buon; ban le; sua chua o to, xe may"),
    ("Dich vu viec lam; du lich; cho thuo may muc thiet bi, do dung va coc dich vu ho tro khoc", "Dich vu viec lam; du lich; cho thue may moc thiet bi, do dung va cac dich vu ho tro khac"),
    ("Hua chat", "Hoa chat"),
    ("Kho dot hua long", "Kho dot hoa long"),
    ("Thanh Hua", "Thanh Hoa"),
    ("Kim loai thuong khoc", "Kim loai thuong khac"),
    ("Giay coc loai", "Giay cac loai"),
    ("Cung cap nuoc; hoat dong quan ly va xu ly roc thai, nuoc thai", "Cung cap nuoc; hoat dong quan ly va xu ly rac thai, nuoc thai"),
    ("San xuat da va coc san pham cu lion quan", "San xuat da va cac san pham co lien quan"),
    ("San xuat san pham thuoc lo", "San xuat san pham thuoc la"),
    ("San xuat thuoc, hoo duoc va duoc lieu", "San xuat thuoc, hoa duoc va duoc lieu"),
    ("Hoat dong thu gom, xu ly va tieu huy roc thai; toi che phe lieu", "Hoat dong thu gom, xu ly va tieu huy rac thai; tai che phe lieu"),
    ("Bonh Duong", "Binh Duong"),
    ("Thoi Bonh", "Thai Binh"),
    ("Bonh Dinh", "Binh Dinh"),
    ("Gioo", "Giao"),
    ("gioo", "giao"),
    ("thuoc lo", "thuoc la"),
    ("Thuoc lo", "Thuoc la"),
    ("roc thai", "rac thai"),
    ("Roc thai", "Rac thai"),
    ("toi che", "tai che"),
    ("cu lion quan", "co lien quan"),
    ("coc dich vu lion quan", "cac dich vu lien quan"),
    ("Dich vu kho boi va cac dich vu lien quan den ho tro van tai", "Dich vu kho bai va cac dich vu lien quan den ho tro van tai"),
    ("Bon buun va bon le; sua chua u tu, mu tu, xe may va xe cu dong co khoc", "Ban buon va ban le; sua chua o to, mo to, xe may va xe co dong co khac"),
    ("Cung cap nuoc, hoat dong quan ly va xu ly rac thai, nuoc thai", "Cung cap nuoc; hoat dong quan ly va xu ly rac thai, nuoc thai"),
    ("Nuoc tu nhion khai thoc", "Nuoc tu nhien khai thac"),
    ("Lom nghiep va dich vu co lien quan", "Lam nghiep va dich vu co lien quan"),
    ("Nung nghiep va dich vu co lien quan", "Nong nghiep va dich vu co lien quan"),
    ("Det, trang phuc, da va coc san pham co lien quan", "Det, trang phuc, da va cac san pham co lien quan"),
    ("Da va coc san pham co lien quan", "Da va cac san pham co lien quan"),
    ("Hoat dog thu gom", "Hoat dong thu gom"),
    ("Van hoo", "Van hoa"),

    ("Bonh keo", "Banh keo"),
    ("Bonh quon", "Banh quan"),
    ("bonh", "banh"),
    ("Bonh", "Banh"),
    ("Cung nghiep", "Cong nghiep"),
    ("cung nghiep", "cong nghiep"),
    ("cung nghe", "cong nghe"),
    ("Cung nghe", "Cong nghe"),
    ("chuyon mun", "chuyen mon"),
    ("Chuyon mun", "Chuyen mon"),
    ("chuyon", "chuyen"),
    ("Chuyon", "Chuyen"),
    ("hang khung", "hang khong"),
    ("Hang khung", "Hang khong"),
    ("khung kho", "khong khi"),
    ("Khung kho", "Khong khi"),
    ("nuoc nung", "nuoc nong"),
    ("Nuoc nung", "Nuoc nong"),
    ("Chi so gio", "Chi so gia"),
    ("chi so gio", "chi so gia"),
    ("hanh chonh", "hanh chinh"),
    ("Hanh chonh", "Hanh chinh"),
    ("gia donh", "gia dinh"),
    ("Gia donh", "Gia dinh"),
    ("coc cung viec", "cac cong viec"),
    ("Coc cung viec", "Cac cong viec"),

    ("lam thuo", "lam thue"),
    ("Lam thuo", "Lam thue"),
    ("coc ho", "cac ho"),
    ("Coc ho", "Cac ho"),
    ("khoong san", "khoang san"),
    ("Khoong san", "Khoang san"),
    ("Kho dot", "Khi dot"),
    ("kho dot", "khi dot"),

    ("Khai khoong", "Khai khoang"),
    ("khai khoong", "khai khoang"),
    ("Giay dop", "Giay dep"),
    ("giay dop", "giay dep"),
    ("Giau dep", "Giay dep"),
    ("giau dep", "giay dep"),
    ("Nguyon", "Nguyen"),
    ("nguyon", "nguyen"),
    ("mu nun", "mu non"),
    ("Mu nun", "Mu non"),
]

ASCII_RULE_VOCABULARY = re.compile(r"^[A-Za-z][A-Za-z ,;]*$")

def check_ascii_corrections():
    """Fail fast if a rule cannot be rendered as both a Python regex and a SQL literal."""
    bad = []
    for artifact, replacement in VIETNAMESE_ASCII_CORRECTIONS:
        for role, value in (("artifact", artifact), ("replacement", replacement)):
            if not ASCII_RULE_VOCABULARY.match(value):
                bad.append(f"{role} {value!r}: letters, spaces, commas and semicolons only")
    if bad:
        raise AssertionError("VIETNAMESE_ASCII_CORRECTIONS rejected:\n  " + "\n  ".join(bad))
    print(f"ASCII corrections self-test passed ({len(VIETNAMESE_ASCII_CORRECTIONS)} rules)")

check_ascii_corrections()

ASCII_CORRECTION_PATTERNS = [(re.compile(rf"\b{artifact}\b"), replacement)
                             for artifact, replacement in VIETNAMESE_ASCII_CORRECTIONS]

def fix_vietnamese_ascii_artifacts(value):
    text = clean_cell(value)
    for pattern, replacement in ASCII_CORRECTION_PATTERNS:
        text = pattern.sub(replacement, text)
    return text

# A cell counts as a value only if it is entirely numeric. Partial matching lets header
# text ("nam 2003", "5. Mot so san pham chu yeu", "#DIV/0!") reduce to its digits and read
# as a number, which pushes data_start into the header block.
NUMERIC_TOKEN_RE = re.compile(r"^[0-9 .,]+$")
YEAR_TOKEN_RE = re.compile(r"^(19|20)\d{2}$")

def looks_like_year(value):
    return bool(YEAR_TOKEN_RE.match(clean_cell(value)))

def to_number(value):
    s = clean_cell(value)
    if not s or s in {"-", "\u2013", "\u2014", "...", "."}:
        return None
    negative = False
    # Accounting-style negatives: "(1234)" means -1234.
    if len(s) > 2 and s.startswith("(") and s.endswith(")"):
        negative, s = True, s[1:-1].strip()
    if s[:1] in {"+", "-"}:
        negative = negative or s[0] == "-"
        s = s[1:].strip()
    s = s.rstrip("%").strip()
    if not s or not NUMERIC_TOKEN_RE.match(s):
        return None
    # Vietnamese decimal comma support when the number ends in comma-decimals.
    if re.search(r"\d,\d+$", s) and not re.search(r"\d,\d{3}(\D|$)", s):
        s2 = s.replace(".", "").replace(" ", "").replace(",", ".")
    else:
        s2 = s.replace(",", "").replace(" ", "")
    if not s2 or s2.count(".") > 1:
        return None
    try:
        parsed = float(s2)
    except Exception:
        return None
    return -parsed if negative else parsed

def parse_cells(raw_cells_json):
    cells = json.loads(raw_cells_json or "[]")
    out = []
    for c in cells:
        # Step 2 preserves the original raw value but also emits a deterministic no-accent
        # normalized value. Use normalized text for extraction so old .xls legacy Vietnamese
        # font values like `S¶n xuÊt` do not leak into report tables.
        v = fix_vietnamese_ascii_artifacts(c.get("cell_value_normalized") or c.get("cell_value_raw"))
        if not v:
            continue
        out.append({
            "row_index": int(c.get("row_index")),
            "column_index": int(c.get("column_index")),
            "cell_ref": c.get("cell_ref"),
            "cell_value_raw": v,
            # Present from the 2026-07-26 reparse onward; 0 for sheets parsed before that.
            "indent_level": int(c.get("indent_level") or 0),
        })
    return out

def indent_map(cells):
    """{row_index: {column_index: indent_level}} for the rows that carry any indentation."""
    m = {}
    for c in cells:
        lvl = int(c.get("indent_level") or 0)
        if lvl:
            m.setdefault(c["row_index"], {})[c["column_index"]] = lvl
    return m

def row_map(cells):
    m = {}
    for c in cells:
        m.setdefault(c["row_index"], {})[c["column_index"]] = c.get("cell_value_raw") or c.get("cell_value_normalized")
    return m

def row_values(rmap, r):
    return [v for _, v in sorted(rmap.get(r, {}).items()) if clean_cell(v)]

def sheet_text(sheet_name, rmap, max_rows=12):
    parts = [sheet_name or ""]
    for r in sorted(rmap)[:max_rows]:
        parts.extend(row_values(rmap, r))
    return " | ".join(parts)

def infer_title(rmap, sheet_name):
    titles = []
    for r in sorted(rmap)[:6]:
        vals = row_values(rmap, r)
        # Title rows in NSO files are usually text-heavy and not too wide.
        nums = sum(1 for v in vals if to_number(v) is not None)
        if vals and nums == 0 and len(vals) <= 5:
            titles.append(" | ".join(vals))
    return " | ".join(titles[:3]) or sheet_name or "unknown_sheet"

def infer_header_and_data_rows(rmap):
    rows = sorted(rmap)
    if not rows:
        return [], None, None
    data_rows = []
    for r in rows:
        vals = rmap.get(r, {})
        text_count = sum(1 for v in vals.values() if clean_cell(v) and to_number(v) is None)
        # A bare year is a header stamp ("nam 2003"), not a measurement, so it alone
        # must not promote a header row into the data block.
        num_count = sum(1 for v in vals.values()
                        if to_number(v) is not None and not looks_like_year(v))
        if text_count >= 1 and num_count >= 1:
            data_rows.append(r)
    if not data_rows:
        return rows[: min(5, len(rows))], None, None
    data_start = min(data_rows)
    data_end = max(data_rows)
    header_rows = [r for r in rows if r < data_start]
    return header_rows[-7:], data_start, data_end

def col_letters_to_index(letters):
    n = 0
    for ch in letters:
        n = n * 26 + (ord(ch) - ord("A") + 1)
    return n

def parse_merged_ranges(merged_ranges_json):
    """["C4:D4", ...] -> [(row_start, row_end, col_start, col_end)] (1-based, Excel order)."""
    out = []
    try:
        raw = json.loads(merged_ranges_json or "[]")
    except Exception:
        return out
    for item in raw or []:
        m = re.match(r"^([A-Z]+)(\d+):([A-Z]+)(\d+)$", str(item).strip().upper())
        if not m:
            continue
        c1, r1, c2, r2 = m.group(1), int(m.group(2)), m.group(3), int(m.group(4))
        out.append((min(r1, r2), max(r1, r2),
                    min(col_letters_to_index(c1), col_letters_to_index(c2)),
                    max(col_letters_to_index(c1), col_letters_to_index(c2))))
    return out

def column_header_rows(rmap, header_rows):
    """The header rows that actually label columns.

    A header row holding a single cell is a caption - the table number/title
    ("11. Thuc hien von dau tu...") or a units note ("Don vi tinh: Nghin tan").
    Spanning it across the table would stamp the same text on every metric name,
    so captions are dropped whenever the sheet has a real multi-cell header row.
    Their text still reaches the output through table_title_raw and context units.
    """
    counts = {r: sum(1 for v in rmap.get(r, {}).values() if clean_cell(v))
              for r in header_rows or []}
    multi = [r for r in (header_rows or []) if counts.get(r, 0) >= 2]
    return multi or list(header_rows or [])

def expand_header_spans(rmap, header_rows, merged_ranges_json=None):
    """Return a copy of rmap in which header cells are propagated across the columns
    they visually span.

    NSO headers are multi-row and heavily merged: "Uoc tinh thang 10 nam 2022" sits in
    one cell above the "Luong"/"Tri gia" pair, so the right-hand column inherits no
    header of its own. Merged ranges are authoritative when present, but legacy .xls
    parsed by xlrd reports none, so spans are otherwise inferred: a header cell extends
    rightwards only over columns that are empty on its own row AND carry header text on
    some other header row. That keeps genuinely unlabelled trailing columns unlabelled.
    """
    if not header_rows:
        return rmap
    hmap = {r: dict(vals) for r, vals in rmap.items()}
    header_set = set(header_rows)
    all_cols = sorted({c for vals in rmap.values() for c in vals})
    if not all_cols:
        return hmap
    sub_header_cols = {c for r in header_rows for c, v in rmap.get(r, {}).items()
                       if clean_cell(v) and to_number(v) is None}

    for r1, r2, c1, c2 in parse_merged_ranges(merged_ranges_json):
        for r in range(r1, r2 + 1):
            if r not in header_set:
                continue
            source = clean_cell(rmap.get(r, {}).get(c1))
            # A bare year is a legitimate header label (the year a block refers to), so it
            # must span its merged range like any other; every other numeric is stray data.
            if not source or (to_number(source) is not None and not looks_like_year(source)):
                continue
            for c in range(c1, c2 + 1):
                if not clean_cell(hmap.get(r, {}).get(c)):
                    hmap.setdefault(r, {})[c] = source

    for r in header_rows:
        row_vals = rmap.get(r, {})
        occupied = sorted(c for c in all_cols if clean_cell(row_vals.get(c)))
        for idx, c in enumerate(occupied):
            source = clean_cell(row_vals.get(c))
            if not source or to_number(source) is not None:
                continue
            stop = occupied[idx + 1] if idx + 1 < len(occupied) else all_cols[-1] + 1
            for c2 in range(c + 1, stop):
                if c2 not in sub_header_cols:
                    continue
                if clean_cell(hmap.get(r, {}).get(c2)):
                    continue
                hmap.setdefault(r, {})[c2] = source
    return hmap

def header_for_col(rmap, header_rows, col):
    """Join a column's header cells top-to-bottom into one metric name.

    Numeric header cells are skipped -- they are stray data, not labels -- but a bare
    four-digit year is a label, and dropping it loses the period the column refers to.
    NSO export tables write one block's year as text ("nam 2025") and the neighbouring
    block's as a bare "2024" on its own row, so the same table yielded
    "So bo / thang 01 / nam 2025 / Luong" next to "So bo / nam / Luong" -- the second
    column silently lost the year it belonged to. Bare years are kept and prefixed with
    "nam" so both blocks read the same way.
    """
    parts = []
    seen = set()
    for r in header_rows:
        v = clean_cell(rmap.get(r, {}).get(col))
        if not v:
            continue
        if to_number(v) is not None:
            if not looks_like_year(v):
                continue
            year = str(int(float(norm(v))))
            # "nam" alone on an earlier row is this year's label; absorb it rather than
            # emitting "nam / 2024".
            if parts and norm(parts[-1]) == "nam":
                parts[-1] = f"nam {year}"
                seen.add(norm(parts[-1]))
                continue
            v = f"nam {year}"
        if norm(v) not in seen:
            seen.add(norm(v))
            parts.append(v)
    return " / ".join(parts) or f"column_{col}"

def infer_metric_type(text):
    t = norm(text)
    if "so voi thang truoc" in t or "thang truoc" in t:
        return "mom_growth"
    if "cung ky" in t or "nam truoc" in t or "so voi" in t:
        return "yoy_growth"
    if "co cau" in t or "ty trong" in t:
        return "share"
    if "%" in text or "phan tram" in t:
        return "percent"
    if "chi so" in t or "index" in t:
        return "index"
    if "cong don" in t or "luy ke" in t or "thang dau nam" in t or re.search(r"\b\d+ thang\b", t):
        return "ytd_value"
    return "value"

def infer_scale(unit_or_metric):
    t = norm(unit_or_metric)
    raw = clean_cell(unit_or_metric)
    if "%" in raw or "phan tram" in t:
        return "percent"
    if "nghin ty" in t:
        return "trillion"
    if "ty" in t or "billion" in t:
        return "billion"
    if "trieu" in t or "million" in t:
        return "million"
    if "nghin" in t or "thousand" in t:
        return "thousand"
    if "chi so" in t:
        return "index"
    return None

def infer_currency(text):
    t = norm(text)
    if "usd" in t or "do la" in t:
        return "USD"
    if "dong" in t or "vnd" in t:
        return "VND"
    return None

UNIT_KEYWORDS = [
    "dong", "vnd", "usd", "ty dong", "trieu dong", "nghin dong", "nghin ty",
    "trieu usd", "nghin usd", "tan", "nghin tan", "trieu tan", "kg", "ta",
    "m2", "m3", "km", "km2", "ha", "nghin ha", "trieu ha",
    "nguoi", "nghin nguoi", "trieu nguoi", "lao dong", "doanh nghiep",
    "luot", "nghin luot", "trieu luot", "khach", "nghin khach", "trieu khach",
    "chiec", "cai", "bo", "con", "nghin con", "trieu con",
    "phan tram", "diem phan tram", "chi so", "index", "lan",
]

UNIT_PHRASE_PATTERNS = [
    (r"nghin\s+ty\s+dong", "Nghìn tỷ đồng"),
    (r"ty\s+dong", "Tỷ đồng"),
    (r"trieu\s+dong", "Triệu đồng"),
    (r"nghin\s+dong", "Nghìn đồng"),
    (r"vnd|dong", "Đồng"),
    (r"trieu\s+usd", "Triệu USD"),
    (r"nghin\s+usd", "Nghìn USD"),
    (r"usd", "USD"),
    (r"trieu\s+tan", "Triệu tấn"),
    (r"nghin\s+tan", "Nghìn tấn"),
    (r"\btan\b", "Tấn"),
    (r"trieu\s+ha", "Triệu ha"),
    (r"nghin\s+ha", "Nghìn ha"),
    (r"\bha\b", "Ha"),
    (r"trieu\s+nguoi", "Triệu người"),
    (r"nghin\s+nguoi", "Nghìn người"),
    (r"\bnguoi\b", "Người"),
    (r"trieu\s+luot", "Triệu lượt"),
    (r"nghin\s+luot", "Nghìn lượt"),
    (r"\bluot\b", "Lượt"),
    (r"trieu\s+khach", "Triệu khách"),
    (r"nghin\s+khach", "Nghìn khách"),
    (r"doanh\s+nghiep", "Doanh nghiệp"),
    (r"lao\s+dong", "Lao động"),
    (r"phan\s+tram|%", "%"),
]

# Canonical ASCII-safe labels replace legacy literals that were vulnerable to
# encoding corruption during old Databricks CLI imports.
UNIT_PHRASE_PATTERNS = [
    (r"nghin\s+ty\s+dong", "Thousand billion VND"),
    (r"ty\s+dong", "Billion VND"),
    (r"trieu\s+dong", "Million VND"),
    (r"nghin\s+dong", "Thousand VND"),
    (r"vnd|dong", "VND"),
    (r"trieu\s+usd", "Million USD"),
    (r"nghin\s+usd", "Thousand USD"),
    (r"usd", "USD"),
    (r"trieu\s+tan", "Million tonnes"),
    (r"nghin\s+tan", "Thousand tonnes"),
    (r"\btan\b", "Tonnes"),
    (r"trieu\s+ha", "Million ha"),
    (r"nghin\s+ha", "Thousand ha"),
    (r"\bha\b", "Ha"),
    (r"trieu\s+nguoi", "Million persons"),
    (r"nghin\s+nguoi", "Thousand persons"),
    (r"\bnguoi\b", "Persons"),
    (r"trieu\s+luot", "Million visits"),
    (r"nghin\s+luot", "Thousand visits"),
    (r"\bluot\b", "Visits"),
    (r"trieu\s+khach", "Million visitors"),
    (r"nghin\s+khach", "Thousand visitors"),
    (r"doanh\s+nghiep", "Enterprises"),
    (r"lao\s+dong", "Workers"),
    (r"phan\s+tram|%", "%"),
]

def unit_phrase_from_text(text):
    raw = clean_cell(text)
    if not raw:
        return None
    t = norm(raw)
    for pattern, label in UNIT_PHRASE_PATTERNS:
        if re.search(pattern, t) or (label == "%" and "%" in raw):
            return label
    return None

def looks_like_unit_text(text):
    raw = clean_cell(text)
    t = norm(raw)
    if not t:
        return False
    if raw == "%" or t in {"%", "phan tram", "diem phan tram"}:
        return True
    if len(t) > 50:
        return False
    # Accept compact unit-only text, but avoid broad titles such as
    # "Tổng mức bán lẻ hàng hóa (tỷ đồng)" being stamped verbatim as a unit.
    return unit_phrase_from_text(raw) is not None and len(t.split()) <= 5

def normalize_unit_text(text):
    raw = clean_cell(text)
    if not raw:
        return None
    if re.match(r"^(don vi tinh|don vi|unit)\b", norm(raw)):
        raw = re.split(r"[:：-]", raw, maxsplit=1)[-1] if re.search(r"[:：-]", raw) else raw
    raw = re.sub(r"^(don vi tinh|don vi|unit)\s*[:：-]?\s*", "", raw, flags=re.IGNORECASE)
    raw = clean_cell(raw.strip(" .;,-"))
    if looks_like_unit_text(raw):
        return raw
    return None

def unit_from_label(label):
    m = re.search(r"\(([^)]+)\)", clean_cell(label))
    if not m:
        return None
    return normalize_unit_text(m.group(1)) or m.group(1).strip()

def unit_from_text(text):
    raw = clean_cell(text)
    if not raw:
        return None
    # Prefer explicit parenthesized units, common in metric headers and titles.
    for candidate in re.findall(r"\(([^)]+)\)", raw):
        unit = normalize_unit_text(candidate) or unit_phrase_from_text(candidate)
        if unit:
            return unit
    # Use the cell verbatim only when it is already unit-like (short/compact).
    unit = normalize_unit_text(raw)
    if unit:
        return unit
    # For broader title/header context, return only the recognized unit phrase.
    return unit_phrase_from_text(raw)

def infer_explicit_unit_row(rmap, data_start):
    # Rows such as "Đơn vị tính: Tỷ đồng" or "Đơn vị tính" followed by a neighboring
    # cell describe the whole table and outrank generic title/header hints.
    for r in sorted(rmap):
        if data_start is not None and r >= data_start:
            continue
        vals = [clean_cell(v) for _, v in sorted(rmap.get(r, {}).items()) if clean_cell(v)]
        for i, v in enumerate(vals):
            if re.search(r"(don vi tinh|don vi|unit)", norm(v)):
                after = re.sub(r"^.*?(don vi tinh|don vi|unit)\s*[:：-]?\s*", "", v, flags=re.IGNORECASE).strip()
                unit = normalize_unit_text(after) or unit_phrase_from_text(after)
                if unit:
                    return unit
                # Accented labels will not be stripped by the raw regex above; split on
                # punctuation as a deterministic fallback.
                if re.search(r"[:：-]", v):
                    unit = normalize_unit_text(re.split(r"[:：-]", v, maxsplit=1)[-1])
                    if unit:
                        return unit
                for neighbor in vals[i + 1:i + 4]:
                    unit = normalize_unit_text(neighbor) or unit_phrase_from_text(neighbor)
                    if unit:
                        return unit
    return None

def infer_header_unit(rmap, header_rows, col):
    # Column headers sometimes include units directly, e.g. "Trị giá (Triệu USD)".
    for r in reversed(header_rows or []):
        unit = unit_from_text(rmap.get(r, {}).get(col))
        if unit:
            return unit
    return None

COMBINED_UNIT_CAPTION_RE = re.compile(
    r"([A-Za-z%][^|;,]{0,28}?)\s*(?:;|,|\bva\b)\s*([^|;,]{0,28}?(?:usd|vnd|dong)[^|;,]{0,12})",
    re.IGNORECASE)

CURRENCY_UNIT_LABELS = {"USD", "Thousand USD", "Million USD",
                        "VND", "Thousand VND", "Million VND", "Billion VND",
                        "Thousand billion VND"}

def find_combined_unit_caption(*texts):
    """Recover a two-part unit caption such as "Nghin tan; Trieu USD" from raw text.

    `unit_from_text` canonicalises a caption to a single label, and for a combined caption
    it returns whichever half it recognises first -- the currency. That loses the quantity
    half permanently, so it has to be picked up from the raw text before canonicalisation.

    Both halves are validated as real units rather than pattern-matched on the currency word
    alone: "dong" also strips out of `dong xuan` (the winter-spring crop) and `lao dong`, so
    captions like "nang suat; san luong lua dong xuan" would otherwise look like a unit pair.
    """
    for text in texts:
        raw = clean_cell(text)
        if not raw:
            continue
        for m in COMBINED_UNIT_CAPTION_RE.finditer(raw):
            left = re.sub(r"(?i)^\s*(don vi tinh|don vi|unit)\s*[:\-]?\s*", "",
                          m.group(1).strip(" .;,:")).strip(" .;,:")
            right = m.group(2).strip(" .;,:")
            if not left or not right:
                continue
            left_unit, right_unit = unit_phrase_from_text(left), unit_phrase_from_text(right)
            if (left_unit and right_unit
                    and right_unit in CURRENCY_UNIT_LABELS
                    and left_unit not in CURRENCY_UNIT_LABELS):
                return f"{left}; {right}"
    return None

# Sheet-level unit context only. Per-column header units are inferred separately by
# infer_header_unit(), which is what reads header_rows.
def infer_context_units(rmap, title, sheet_name, data_start):
    explicit = infer_explicit_unit_row(rmap, data_start)
    top_text = " | ".join(" | ".join(row_values(rmap, r)) for r in sorted(rmap)[:8])
    combined = find_combined_unit_caption(title, sheet_name, top_text)
    for text in [title, sheet_name]:
        unit = unit_from_text(text)
        if unit:
            return {"explicit": explicit, "title": unit, "combined": combined}
    return {"explicit": explicit, "title": unit_from_text(top_text), "combined": combined}

PROVINCE_HINTS = {
    "ha noi", "hai phong", "bac ninh", "vinh phuc", "da nang", "ho chi minh", "can tho", "dong nai", "binh duong",
    "ba ria", "long an", "tien giang", "dong thap", "an giang", "kien giang", "ca mau", "lam dong", "khanh hoa",
    "thanh hoa", "nghe an", "ha tinh", "quang ninh", "thai nguyen", "lao cai", "yen bai", "dien bien"
}

def looks_like_geography(label):
    t = norm(label)
    if not t:
        return False
    if t in {"ca nuoc", "mien bac", "mien nam", "dong bang song hong", "dong bang song cuu long"}:
        return True
    return any(p in t for p in PROVINCE_HINTS)

# COMMAND ----------

# DBTITLE 1,Sheet/topic rules
SHEET_RULES = [
    # national accounts
    # Specific price-basis codes outrank the shared "gdp" keyword.
    ("national_accounts", "gdp_current_price", ["gdp-hh", "gdp", "tong san pham trong nuoc theo gia hien hanh", "gia hien hanh"], "sector"),
    ("national_accounts", "gdp_constant_price", ["gdp-ss", "gdp", "gia so sanh", "tong san pham trong nuoc"], "sector"),

    # agriculture
    ("agriculture_forestry_fishery", "agriculture", ["nong", "nn", "nong nghiep", "lua", "gieo cay", "thu hoach", "dong xuan", "channuoi", "lam nghiep", "thuy san", "nuoi trong"], "mixed_agri"),

    # industry
    ("industry", "industrial_production_index", ["iip", "chi so san xuat cong nghiep", "gtcn", "gia tri san xuat cong nghiep", "cong nghiep"], "sector"),
    ("industry", "industrial_products", ["sp cn", "spcn", "san pham chu yeu", "san pham cong nghiep"], "product"),
    ("industry", "industrial_labor", ["ld cn", "ldcn", "lao dong cua doanh nghiep cong nghiep"], "mixed_geo_sector"),

    # investment / enterprise
    ("investment_construction", "investment", ["vdt", "von dau tu", "von nsnn", "xd", "xay dung", "dtnn", "fdi", "dt nn"], "mixed_geo_sector"),
    ("enterprise_business_registration", "enterprise_registration", ["dn", "doanh nghiep", "dang ky thanh lap", "quay lai", "ngung", "giai the"], "indicator"),

    # retail / tourism
    ("retail_services_tourism", "retail_services", ["tongmuc", "tong muc", "tong muc ban le", "ban le", "dich vu", "luu tru", "an uong"], "sector"),
    ("retail_services_tourism", "tourism", ["du lich", "khach quoc te", "kqt"], "geography"),

    # trade / prices
    ("trade_prices", "exports", ["xuat khau", " xk", "xk", "export"], "product"),
    ("trade_prices", "imports", ["nhap khau", " nk", "nk", "import"], "product"),
    ("trade_prices", "consumer_price_index", ["cpi", "gia tieu dung", "lam phat"], "price_group"),
    ("trade_prices", "producer_price_index", ["gia sx", "gia san xuat", "gia van tai", "gia nvl", "gia xk", "gia nk", "tygia", "ty gia"], "price_group"),

    # transport / labor / environment
    ("transport_post_telecom", "transport_passenger", ["van tai hk", "van tai hanh khach", "hanh khach", "vt hk", "vthk"], "transport_mode"),
    # Freight rules require a transport qualifier; "hang hoa" alone is ambiguous.
    ("transport_post_telecom", "transport_freight", ["van tai hh", "van tai hang hoa", "vt hh", "vthh"], "transport_mode"),
    # Generic transport spellings are evaluated after mode-specific rules.
    ("transport_post_telecom", "transport_freight", ["van tai", "vantai", "vt", "cuoc van tai", "buu chinh", "vien thong"], "transport_mode"),
    # Glued spellings ("Danso", "Vieclam") are as common as spaced ones in these workbooks.
    ("population_labor_social", "labor_social", ["laodong", "lao dong", "thatnghiep", "that nghiep", "danso", "dan so", "vieclam", "viec lam"], "indicator"),
    # Avoid the ambiguous bare keyword "no"; match explicit social/environment terms.
    ("environment_safety_disaster", "social_environment",
     ["xhmt", "moi truong", "thien tai", "tai nan", "chay", "chay no", "vu no",
      "giao duc", "hoc sinh", "tot nghiep", "thpt", "y te", "benh vien", "van hoa", "the thao"],
     "indicator"),
]

# Workbook sheet codes are more reliable than incidental keywords in cells.
SHEET_NAME_OVERRIDES = [
    (r"^\s*14\s*[.\-_]?\s*xk\b", "trade_prices", "exports", "product"),
    (r"^\s*15\s*[.\-_]?\s*nk\b", "trade_prices", "imports", "product"),
    (r"^\s*12\s*[.\-_]?\s*fdi\b", "investment_construction", "foreign_direct_investment", "sector"),
    (r"^\s*2\s*[.\-_]?\s*iip", "industry", "industrial_production_index", "sector"),
    (r"^\s*4\s*[.\-_]?\s*ldcn", "industry", "industrial_labor", "sector"),
]

SHORT_KEYWORD_MAX = 3

def keyword_hit(kw, text):
    """Substring match, except that very short keywords must begin at a non-letter boundary.

    'nn' as a plain substring fires inside 'dtnn' (investment) and 'vt' inside any word
    containing those letters. A LEFT boundary is required; a right-hand one deliberately is
    not, because NSO sheet names glue codes onto words -- '26.NKthang', 'VTHH', '18XKthang'
    all have to keep matching. Digits count as a boundary, so '06VT' and '5NN' match.
    """
    if not kw or not text:
        return False
    if len(kw) > SHORT_KEYWORD_MAX:
        return kw in text
    return re.search(r"(?<![a-z])" + re.escape(kw), text) is not None

def classify_sheet(sheet_name, title, rmap):
    sheet_key = norm(sheet_name or "")
    for pattern, domain, subdomain, dimension_strategy in SHEET_NAME_OVERRIDES:
        if re.search(pattern, sheet_key):
            return {
                "domain": domain,
                "subdomain": subdomain,
                "dimension_strategy": dimension_strategy,
                "method": f"sheet_name_override:{subdomain}",
                "confidence": 1.0,
                "needs_review": False,
            }
    # Prefer sheet name, then title, then body text. Rule order breaks equal ties.
    name_text = norm(sheet_name or "")
    title_text = norm(title or "")
    body_text = norm(sheet_text(sheet_name, rmap))
    # Longer keywords win within a tier.
    best = None
    for idx, (domain, subdomain, keywords, dimension_strategy) in enumerate(SHEET_RULES):
        score = 0
        span = 0
        for k in keywords:
            kk = norm(k)
            if not kk:
                continue
            if keyword_hit(kk, name_text):
                tier = 3
            elif keyword_hit(kk, title_text):
                tier = 2
            elif keyword_hit(kk, body_text):
                tier = 1
            else:
                continue
            if tier > score:
                score, span = tier, len(kk)
            elif tier == score and len(kk) > span:
                span = len(kk)
        if score and (best is None or (score, span) > (best[0], best[1])):
            best = (score, span, idx, domain, subdomain, dimension_strategy)
    if best:
        score, _span, _idx, domain, subdomain, dimension_strategy = best
        # A body-only match is a weak signal and is now surfaced as such instead of being
        # reported at the same 0.9 confidence as a match on the sheet's own name.
        confidence = {3: 0.95, 2: 0.90, 1: 0.70}[score]
        return {
            "domain": domain,
            "subdomain": subdomain,
            "dimension_strategy": dimension_strategy,
            "method": f"sheet_rule:{subdomain}:{'name' if score == 3 else 'title' if score == 2 else 'body'}",
            "confidence": confidence,
            "needs_review": score == 1,
        }
    return {
        "domain": "other_or_unknown",
        "subdomain": "unknown",
        "dimension_strategy": "indicator",
        "method": "sheet_rule:fallback",
        "confidence": 0.35,
        "needs_review": True,
    }

# Corpus sheet names used to catch classification regressions before extraction.
SHEET_RULE_SELFTEST = [
    ("Vantai", "transport_post_telecom"), ("VT", "transport_post_telecom"),
    ("06VT", "transport_post_telecom"), ("VTHH", "transport_post_telecom"),
    ("VTHK", "transport_post_telecom"), ("VT-Ok", "transport_post_telecom"),
    ("18XK", "trade_prices"), ("26.NKthang", "trade_prices"), ("CPI", "trade_prices"),
    ("xuat khau thang", "trade_prices"),
    ("03SPCN", "industry"), ("02GTCN", "industry"), ("IIP", "industry"),
    ("Cong nghiep", "industry"), ("5. LDCN_DP", "industry"),
    ("5NN", "agriculture_forestry_fishery"), ("01NN", "agriculture_forestry_fishery"),
    ("05DTNN", "investment_construction"), ("11.VDT", "investment_construction"),
    ("Von dau tu", "investment_construction"), ("XD", "investment_construction"),
    ("04XD", "investment_construction"),
    ("DN1", "enterprise_business_registration"),
    ("16.DN giai the", "enterprise_business_registration"),
    ("Tongmuc", "retail_services_tourism"), ("Tong muc", "retail_services_tourism"),
    ("Du lich", "retail_services_tourism"), ("KQT", "retail_services_tourism"),
    ("GDP", "national_accounts"), ("1.GDP-HH", "national_accounts"),
    ("2.GDP-SS", "national_accounts"),
    ("LAO DONG", "population_labor_social"), ("Danso", "population_labor_social"),
    ("XHMT", "environment_safety_disaster"), ("THPT", "environment_safety_disaster"),
]

def check_sheet_rules():
    """Fail fast if a rule family stops covering the sheet names it is supposed to own."""
    bad = []
    for name, want in SHEET_RULE_SELFTEST:
        got = classify_sheet(name, "", {})
        if got["domain"] != want:
            bad.append(f"{name!r}: expected {want}, got {got['domain']} via {got['method']}")
    # A one- or two-character keyword matches almost anything it is tested against. "no"
    # (explosion) matched inside "nong"/"khong" and quietly owned every education sheet in
    # the corpus; keyword_hit's boundary rule contains the damage but does not excuse it.
    for domain, subdomain, keywords, _strategy in SHEET_RULES:
        for k in keywords:
            if len(norm(k)) < 2:
                bad.append(f"{domain}/{subdomain}: keyword {k!r} is too short to be evidence")
    if bad:
        raise AssertionError("SHEET_RULES self-test failed:\n  " + "\n  ".join(bad))
    print(f"SHEET_RULES self-test passed ({len(SHEET_RULE_SELFTEST)} names)")

check_sheet_rules()

COMBINED_UNIT_SEP_RE = re.compile(r"\s*(?:;|,|\bva\b|\band\b)\s*", re.IGNORECASE)

def split_combined_unit(unit_text, measure):
    """Pick one side of a two-part unit caption that covers a Luong/Tri gia column pair.

    NSO trade tables print a single caption -- "Nghin tan; Trieu USD" -- above a repeating
    pair of quantity and value columns. `unit_phrase_from_text` scans that caption for the
    first pattern it recognises, which is the currency, so BOTH columns collapsed to
    "Million USD" and every quantity row was labelled with a currency unit.

    Returns the currency side for 'value' and the other side for 'quantity', or None when
    the caption is not a clean two-part pair.
    """
    raw = clean_cell(unit_text)
    if not raw or not measure:
        return None
    parts = [p.strip(" .;,:") for p in COMBINED_UNIT_SEP_RE.split(raw)]
    parts = [p for p in parts if p]
    if len(parts) != 2:
        return None
    def is_currency(p):
        return bool(re.search(r"usd|vnd|\bdong\b", norm(p)))

    currency = [p for p in parts if is_currency(p)]
    other = [p for p in parts if not is_currency(p)]
    if len(currency) != 1 or len(other) != 1:
        return None
    return currency[0] if measure == "value" else other[0]

def normalize_observation_semantics(sheet_name, subdomain, metric_name, unit_raw, metric_type, scale,
                                    context_units=None):
    """Apply narrow corrections after generic header/unit inference."""
    sheet_key = norm(sheet_name or "")
    metric_key = norm(metric_name or "")

    if subdomain == "industrial_production_index" or re.search(r"^\s*2\s*[.\-_]?\s*iip", sheet_key):
        metric_type = "index_yoy_base100" if ("cung ky" in metric_key or "nam truoc" in metric_key) else "index"
        unit_raw = "Index (comparison period=100)"
        scale = "index"
    elif subdomain in {"exports", "imports"}:
        if "tri gia" in metric_key or "value" in metric_key:
            unit_raw = "Million USD"
            scale = "million"
        elif "luong" in metric_key or "quantity" in metric_key:
            quantity_unit = unit_phrase_from_text(metric_name)
            if not (quantity_unit and quantity_unit not in {"Million USD", "Thousand USD", "USD", "%"}):
                # The metric name carries no unit of its own ("So bo / nam 2024 / Luong"), so
                # fall back to the quantity side of the table's combined caption. Without this
                # the row keeps a currency unit inherited from the caption's Trieu USD half.
                cu = context_units or {}
                for caption in (cu.get("combined"), unit_raw, cu.get("explicit"), cu.get("title")):
                    side = split_combined_unit(caption, "quantity")
                    if side:
                        quantity_unit = unit_phrase_from_text(side) or side
                        break
            if quantity_unit and quantity_unit not in {"Million USD", "Thousand USD", "USD", "%"}:
                unit_raw = quantity_unit
                scale = infer_scale(quantity_unit)
        if "so voi" in metric_key or "cung ky" in metric_key:
            metric_type = "index_yoy_base100"
            unit_raw = "Index (same period previous year=100)"
            scale = "index"
    return unit_raw, metric_type, scale

def route_dimensions(strategy, row_label, title):
    geo = sector = product = None
    label = clean_cell(row_label)
    context = norm(title + " | " + label)
    if strategy == "geography":
        geo = label
    elif strategy == "product":
        product = label
    elif strategy in {"sector", "price_group", "transport_mode"}:
        sector = label
    elif strategy == "mixed_geo_sector":
        if looks_like_geography(label) or "dia phuong" in context or "phan theo dia phuong" in context:
            geo = label
        else:
            sector = label
    elif strategy == "mixed_agri":
        if looks_like_geography(label) or label in {"Miền Bắc", "Miền Nam"}:
            geo = label
        else:
            product = label
    else:
        # Generic indicator table: keep label in indicator_name only.
        pass
    return geo, sector, product

# COMMAND ----------

# DBTITLE 1,Row context: ditto marks, unit column, parent propagation

# NSO workbooks put the unit in its own column and use the Excel ditto convention (a bare `"`)
# to mean "same unit as the row above". Sheet 03SPCN is the canonical shape:
#
#   c1=Than                  c2=Nghin tan   c3=3803.3   <- parent product, real unit
#   c1=Doanh nghiep Nha nuoc c2="           c3=3660.7   <- child sector, ditto unit
#   c1=Trung uong            c2="           c3=3650
#   c1=Sua hop               c2=Trieu hop   c3=25.2     <- next parent
#   c1=Doanh nghiep Nha nuoc c2="           c3=15.8     <- same child label, different parent
#
# A ditto is not numeric, so it has to be excluded from the label explicitly. Otherwise it
# joins on as a second label part (`Doanh nghiep Nha nuoc | "`) and every product's ownership
# breakdown collapses onto the same key.

DITTO_TOKENS = {'"', '""', "''", "'", '”', '“', '»', 'nt', '-nt-', '~', '،'}

def is_ditto(value):
    t = clean_cell(value)
    if not t:
        return False
    return t.strip(" .-_") .lower() in DITTO_TOKENS

def detect_unit_column(rmap, cols, data_start):
    """Column whose data cells are predominantly units or ditto marks, or None.

    The ditto mark is the load-bearing signal. Requiring it avoids the failure mode where a
    genuine LABEL column is mistaken for units because its text happens to contain them --
    'Dien tich (Nghin ha)', 'Nang suat (Ta/ha)', 'San luong (Nghin tan)' all look unit-like,
    and excluding that column from the label drops real agricultural measurements entirely.
    A label column never contains ditto marks; a unit column nearly always does.
    """
    best, best_score = None, 0.0
    for c in cols:
        vals = [clean_cell(rmap.get(r, {}).get(c)) for r in rmap if data_start is not None and r >= data_start]
        vals = [v for v in vals if v]
        if len(vals) < 4:
            continue
        ditto_hits = sum(1 for v in vals if is_ditto(v))
        if ditto_hits < 2:
            continue
        hits = sum(1 for v in vals if is_ditto(v) or (to_number(v) is None and normalize_unit_text(v)))
        score = hits / float(len(vals))
        # Demand a clear majority so a text label column is never mistaken for units.
        if score >= 0.6 and score > best_score:
            best, best_score = c, score
    return best

def split_row_label(vals, cols, unit_col):
    """Row label from the first two text cells, excluding the unit column and ditto marks."""
    text_cols = []
    for c in cols:
        if unit_col is not None and c == unit_col:
            continue
        v = clean_cell(vals.get(c))
        if v and to_number(v) is None and not is_ditto(v):
            text_cols.append((c, v))
    if not text_cols:
        return None, None, []
    first_col, first = text_cols[0]
    labels = [v for _, v in text_cols[:2] if v]
    return " | ".join(labels), first_col, text_cols

def row_indent(indent_map, r, label_col):
    """Indent level of a row's label cell, or 0 when unavailable."""
    if not indent_map:
        return 0
    return int((indent_map.get(r) or {}).get(label_col, 0) or 0)

def build_row_context(rmap, cols, data_start, indent_map=None):
    """Resolve, per data row, its unit (following ditto marks) and its parent group label.

    Grouping is deliberately independent of the sheet's dimension strategy: this decides WHICH
    rows form a group, and route_dimensions_with_parent() decides which dimension the resulting
    parent/child labels land in.

    Two independent signals, because NSO sheets use both:

    * Excel indent level (captured by Step 2 as `indent_level`). A province or product sits at
      indent 0 and its breakdown -- ownership sector, transport mode, age band, gender -- at
      indent 1+. This is the general signal and covers sheets with no unit column at all.
    * The unit column with ditto marks. A row whose unit cell holds a real unit opens a group;
      rows carrying a ditto belong to it. This covers sheets where everything sits at indent 0.
    """
    unit_col = detect_unit_column(rmap, cols, data_start)
    units, parents = {}, {}
    current_unit = None
    parent_by_level = {}
    for r in sorted(x for x in rmap if data_start is not None and x >= data_start):
        vals = rmap.get(r, {})
        label, label_col, _ = split_row_label(vals, cols, unit_col)
        raw_unit = clean_cell(vals.get(unit_col)) if unit_col is not None else None
        has_own_unit = bool(raw_unit) and not is_ditto(raw_unit)
        carries_ditto = bool(raw_unit) and is_ditto(raw_unit)

        if has_own_unit:
            current_unit = normalize_unit_text(raw_unit) or raw_unit
        units[r] = current_unit if (has_own_unit or carries_ditto) else None

        level = row_indent(indent_map, r, label_col)
        parent = None
        if level > 0:
            # Nearest ancestor at a shallower indent.
            for lv in range(level - 1, -1, -1):
                if parent_by_level.get(lv):
                    parent = parent_by_level[lv]
                    break
        elif carries_ditto:
            # No indent information, but the ditto says "same group as above".
            parent = parent_by_level.get(0)
        parents[r] = parent
        # A child must not become the parent of its own siblings. Without this, a ditto-marked
        # group at indent 0 chains -- the second child takes the first child as its parent
        # instead of the group head -- which splits groups wrongly and makes ambiguity worse.
        is_child = parent is not None
        if label and not is_child:
            parent_by_level[level] = label
            # A new group at this level invalidates anything deeper.
            for deeper in [k for k in parent_by_level if k > level]:
                parent_by_level.pop(deeper, None)
    return {"unit_col": unit_col, "units": units, "parents": parents}

def resolve_row(ctx, r, vals, cols):
    """Row label with the parent group folded in, plus the row's resolved unit.

    Returns (label, label_col, text_cols, parent_label, row_unit, child_label) — `child_label`
    is the row's own label before the parent is folded in, which is what the dimension routing
    needs so the breakdown lands in sector_raw rather than being duplicated into the name.
    """
    unit_col = ctx["unit_col"]
    child, label_col, text_cols = split_row_label(vals, cols, unit_col)
    parent = ctx["parents"].get(r)
    row_unit = ctx["units"].get(r)
    label = child
    if child and parent and norm(parent) != norm(child):
        # Self-describing and unique: "Than | Doanh nghiep Nha nuoc".
        label = f"{parent} | {child}"
    else:
        parent = None
    return label, label_col, text_cols, parent, row_unit, child

def route_dimensions_with_parent(strategy, row_label, title, parent_label, child_label=None):
    """route_dimensions, plus the parent group routed to product/sector where it applies."""
    geo, sector, product = route_dimensions(strategy, row_label, title)
    if not parent_label or not child_label:
        return geo, sector, product
    # The parent names the subject being measured; the child is the breakdown of it
    # (in NSO industrial/agricultural sheets the children are ownership sectors).
    if strategy in {"product", "mixed_agri"} and not looks_like_geography(child_label):
        return None, child_label, parent_label
    if strategy in {"sector", "price_group", "transport_mode", "mixed_geo_sector"}:
        return geo, child_label, product or parent_label
    return geo, sector, product

def reconcile_metric_type(metric_type, unit_raw, value, metric_name):
    """Demote a growth type that the unit and magnitude contradict.

    A header like "... cung ky nam 2023 (%)" can sit over a column of billion-VND levels, so
    the header text alone is not enough to type the column. Trust the unit and magnitude
    instead, falling back to the cumulative type when the header carries an "N thang" marker.
    """
    if metric_type not in {"yoy_growth", "mom_growth"}:
        return metric_type
    unit = (unit_raw or "").strip()
    if unit and unit != "%" and value is not None and abs(value) > 1000:
        return "ytd_value" if re.search(r"\b\d+\s*thang\b", norm(metric_name)) else "value"
    return metric_type

COMPARISON_METRIC_TYPES = {"yoy_growth", "mom_growth", "index", "index_yoy_base100", "share", "percent"}

def infer_unit_raw(row_label, metric_name, rmap, header_rows, col, context_units,
                   row_unit=None, metric_type=None):
    """Unit for one observation, preferring the row's own unit column over table context.

    Precedence: parenthesized unit in the row label, the row's ditto-resolved unit column,
    explicit "Don vi tinh:" context, the metric/header column, then title text.
    """
    unit = unit_from_label(row_label)
    if unit:
        return unit
    # The unit column describes the row's level measurement. A comparison column
    # ("so voi cung ky ...") on the same row is a percentage whatever that column says,
    # so the row unit must not reach comparison metric types.
    if row_unit and metric_type not in COMPARISON_METRIC_TYPES:
        return row_unit
    explicit = (context_units or {}).get("explicit")
    if explicit:
        return explicit
    unit = unit_from_text(metric_name)
    if unit:
        return unit
    unit = infer_header_unit(rmap, header_rows, col)
    if unit:
        return unit
    title_unit = (context_units or {}).get("title")
    # Percent is a per-column property, never a table-wide one, so it is not inherited
    # from the title the way other units are.
    if title_unit and title_unit.strip() == "%":
        return None
    return title_unit

# COMMAND ----------

# DBTITLE 1,Load pending parsed sheets
parsed = spark.sql(f"""
SELECT
  p.*,
  r.report_year,
  r.report_month,
  r.report_quarter,
  r.period_type,
  r.sub_category,
  r.period_start_date,
  r.period_end_date
FROM {PARSED_TABLE} p
INNER JOIN {REPORTS_TABLE} r ON p.report_id = r.report_id
INNER JOIN {LOG_TABLE} l ON p.attachment_id = l.attachment_id
WHERE l.parse_status = 'success'
  AND (l.extraction_status IS NULL OR l.extraction_status IN ('pending','failed'))
""").collect()

print(f"Pending parsed sheets for extraction: {len(parsed)}")

# COMMAND ----------

# DBTITLE 1,Extract cells, inventory, unified observations, and topic rows
now = datetime.utcnow()
cell_rows = []
table_rows = []
indicator_rows = []
topic_rows_by_domain = {domain: [] for domain in TOPIC_TABLES}
status_by_attachment = {}
errors_by_attachment = {}
processed_sheet_report_ids = set()

for i, row in enumerate(parsed, start=1):
    d = row.asDict()
    attachment_id = d["attachment_id"]
    try:
        if i == 1 or i % 100 == 0:
            print(f"Extracting sheet {i}/{len(parsed)}: {d.get('filename')} / {d.get('sheet_name_raw')}")

        cells = parse_cells(d.get("raw_cells_json"))
        rmap = row_map(cells)
        header_rows, data_start, data_end = infer_header_and_data_rows(rmap)
        # Resolve merged/spanned header cells before any header text is read.
        label_rows = column_header_rows(rmap, header_rows)
        hmap = expand_header_spans(rmap, label_rows, d.get("merged_ranges_json"))
        title = infer_title(rmap, d.get("sheet_name_raw"))
        context_units = infer_context_units(hmap, title, d.get("sheet_name_raw"), data_start)
        sheet_rule = classify_sheet(d.get("sheet_name_raw"), title, rmap)
        domain = sheet_rule["domain"]
        subdomain = sheet_rule["subdomain"]
        sheet_report_id = md5_16(f"{d['report_id']}|{attachment_id}|{d.get('sheet_index')}")
        processed_sheet_report_ids.add(sheet_report_id)
        table_id = md5_16(f"{sheet_report_id}|table|1")

        for c in cells:
            cell_rows.append({
                "sheet_report_id": sheet_report_id,
                "report_id": d["report_id"],
                "attachment_id": attachment_id,
                "report_url": d.get("report_url"),
                "attachment_url": d.get("attachment_url"),
                "filename": d.get("filename"),
                "report_year": d.get("report_year"),
                "report_month": d.get("report_month"),
                "report_quarter": d.get("report_quarter"),
                "period_type": d.get("period_type"),
                "sub_category": d.get("sub_category"),
                "sheet_index": d.get("sheet_index"),
                "sheet_name_raw": d.get("sheet_name_raw"),
                "table_index": 1,
                "row_index": c["row_index"],
                "column_index": c["column_index"],
                "cell_value_raw": c["cell_value_raw"],
                "cell_value_clean": clean_cell(c["cell_value_raw"]),
                "extracted_timestamp": now,
            })

        table_rows.append({
            "sheet_report_id": sheet_report_id,
            "table_id": table_id,
            "report_id": d["report_id"],
            "attachment_id": attachment_id,
            "sheet_index": d.get("sheet_index"),
            "sheet_name_raw": d.get("sheet_name_raw"),
            "table_index": 1,
            "table_title_raw": title,
            "table_category": "sheet_topic_report",
            "indicator_domain": domain,
            "indicator_subdomain": subdomain,
            "header_rows_json": json.dumps(header_rows, ensure_ascii=False),
            "data_start_row": data_start,
            "data_end_row": data_end,
            "classification_method": sheet_rule["method"],
            "classification_confidence": float(sheet_rule["confidence"]),
            "needs_review": bool(sheet_rule["needs_review"]),
            "extracted_timestamp": now,
        })

        if data_start is None:
            status_by_attachment[attachment_id] = status_by_attachment.get(attachment_id, 0)
            continue

        cols = sorted({c["column_index"] for c in cells})
        # Per-sheet row context: ditto-resolved units and parent/child grouping (from Excel
        # indent levels where present, falling back to unit-column ditto marks).
        row_ctx = build_row_context(rmap, cols, data_start, indent_map(cells))
        sheet_inserted = 0
        for r in sorted(x for x in rmap if x >= data_start):
            vals = rmap.get(r, {})
            row_label, label_col, text_cols, parent_label, row_unit, child_label = resolve_row(row_ctx, r, vals, cols)
            if not row_label:
                continue
            if len(row_label) <= 1 or norm(row_label) in {"don vi", "don vi tinh", "uoc tinh"}:
                continue
            geography, sector, product = route_dimensions_with_parent(
                sheet_rule["dimension_strategy"], row_label, title, parent_label, child_label
            )

            for col in cols:
                if col == label_col:
                    continue
                raw_value = clean_cell(vals.get(col))
                numeric_value = to_number(raw_value)
                if numeric_value is None:
                    continue
                metric_name = header_for_col(hmap, label_rows, col)
                # metric_type first: a comparison column is a percentage regardless of the
                # row's unit column, so unit inference needs to know which kind of column this is.
                metric_type = infer_metric_type(metric_name)
                unit_raw = infer_unit_raw(row_label, metric_name, hmap, header_rows, col,
                                             context_units, row_unit, metric_type)
                scale = infer_scale(metric_name) or infer_scale(unit_raw)
                unit_raw, metric_type, scale = normalize_observation_semantics(
                    d.get("sheet_name_raw"), subdomain, metric_name, unit_raw, metric_type, scale,
                    context_units
                )
                metric_type = reconcile_metric_type(metric_type, unit_raw, numeric_value, metric_name)
                currency = infer_currency(unit_raw or metric_name)
                obs_id = md5_16(f"{sheet_report_id}|{table_id}|{r}|{col}|{row_label}|{raw_value}")

                long_row = {
                    "sheet_report_id": sheet_report_id,
                    "indicator_observation_id": obs_id,
                    "report_id": d["report_id"],
                    "attachment_id": attachment_id,
                    "table_id": table_id,
                    "source_row_index": r,
                    "source_column_index": col,
                    "report_year": d.get("report_year"),
                    "report_month": d.get("report_month"),
                    "report_quarter": d.get("report_quarter"),
                    "period_type": d.get("period_type"),
                    "period_start_date": d.get("period_start_date"),
                    "period_end_date": d.get("period_end_date"),
                    "sub_category": d.get("sub_category"),
                    "indicator_domain": domain,
                    "indicator_subdomain": subdomain,
                    "indicator_name_raw": row_label,
                    "indicator_name_normalized": norm(fix_vietnamese_ascii_artifacts(row_label)),
                    "geography_raw": geography,
                    "sector_raw": sector,
                    "product_raw": product,
                    "unit_raw": unit_raw,
                    "metric_name_raw": metric_name,
                    "metric_type": metric_type,
                    "value_numeric": numeric_value,
                    "value_text": raw_value,
                    "currency": currency,
                    "scale": scale,
                    "extraction_method": sheet_rule["method"],
                    "extraction_confidence": float(sheet_rule["confidence"]),
                    "needs_review": bool(sheet_rule["needs_review"]),
                    "source_filename": d.get("filename"),
                    "source_sheet_name": d.get("sheet_name_raw"),
                    "extracted_timestamp": now,
                }
                indicator_rows.append(long_row)

                topic_rows_by_domain.setdefault(domain, []).append({
                    "sheet_report_id": sheet_report_id,
                    "observation_id": obs_id,
                    "report_id": d["report_id"],
                    "attachment_id": attachment_id,
                    "table_id": table_id,
                    "report_year": d.get("report_year"),
                    "report_month": d.get("report_month"),
                    "report_quarter": d.get("report_quarter"),
                    "period_type": d.get("period_type"),
                    "period_start_date": d.get("period_start_date"),
                    "period_end_date": d.get("period_end_date"),
                    "sub_category": d.get("sub_category"),
                    "source_filename": d.get("filename"),
                    "source_sheet_name": d.get("sheet_name_raw"),
                    "sheet_index": d.get("sheet_index"),
                    "source_row_index": r,
                    "source_column_index": col,
                    "table_title_raw": title,
                    "row_label_raw": row_label,
                    "row_label_normalized": norm(fix_vietnamese_ascii_artifacts(row_label)),
                    "geography_raw": geography,
                    "sector_raw": sector,
                    "product_raw": product,
                    "unit_raw": unit_raw,
                    "metric_name_raw": metric_name,
                    "metric_type": metric_type,
                    "value_numeric": numeric_value,
                    "value_text": raw_value,
                    "currency": currency,
                    "scale": scale,
                    "extraction_method": sheet_rule["method"],
                    "extraction_confidence": float(sheet_rule["confidence"]),
                    "needs_review": bool(sheet_rule["needs_review"]),
                    "extracted_timestamp": now,
                })
                sheet_inserted += 1

        status_by_attachment[attachment_id] = status_by_attachment.get(attachment_id, 0) + sheet_inserted
    except Exception as e:
        errors_by_attachment[attachment_id] = str(e)[:1000]

print(f"Prepared cells: {len(cell_rows)}")
print(f"Prepared inventory rows: {len(table_rows)}")
print(f"Prepared unified observations: {len(indicator_rows)}")
for domain, rows in topic_rows_by_domain.items():
    if rows:
        print(f"Prepared {domain} rows: {len(rows)} -> {TOPIC_TABLES[domain]}")

# COMMAND ----------

# DBTITLE 1,Schemas
cell_schema = StructType([
    StructField("sheet_report_id", StringType(), True),
    StructField("report_id", StringType(), False), StructField("attachment_id", StringType(), False),
    StructField("report_url", StringType(), True), StructField("attachment_url", StringType(), True), StructField("filename", StringType(), True),
    StructField("report_year", IntegerType(), True), StructField("report_month", IntegerType(), True), StructField("report_quarter", IntegerType(), True),
    StructField("period_type", StringType(), True), StructField("sub_category", StringType(), True), StructField("sheet_index", IntegerType(), True),
    StructField("sheet_name_raw", StringType(), True), StructField("table_index", IntegerType(), True), StructField("row_index", IntegerType(), True),
    StructField("column_index", IntegerType(), True), StructField("cell_value_raw", StringType(), True), StructField("cell_value_clean", StringType(), True),
    StructField("extracted_timestamp", TimestampType(), True),
])

table_schema = StructType([
    StructField("sheet_report_id", StringType(), True),
    StructField("table_id", StringType(), False), StructField("report_id", StringType(), False), StructField("attachment_id", StringType(), False),
    StructField("sheet_index", IntegerType(), True), StructField("sheet_name_raw", StringType(), True), StructField("table_index", IntegerType(), True),
    StructField("table_title_raw", StringType(), True), StructField("table_category", StringType(), True), StructField("indicator_domain", StringType(), True),
    StructField("indicator_subdomain", StringType(), True), StructField("header_rows_json", StringType(), True), StructField("data_start_row", IntegerType(), True),
    StructField("data_end_row", IntegerType(), True), StructField("classification_method", StringType(), True), StructField("classification_confidence", DoubleType(), True),
    StructField("needs_review", BooleanType(), True), StructField("extracted_timestamp", TimestampType(), True),
])

indicator_schema = StructType([
    StructField("sheet_report_id", StringType(), True),
    StructField("indicator_observation_id", StringType(), False), StructField("report_id", StringType(), False), StructField("attachment_id", StringType(), False),
    StructField("table_id", StringType(), True), StructField("source_row_index", IntegerType(), True), StructField("source_column_index", IntegerType(), True),
    StructField("report_year", IntegerType(), True), StructField("report_month", IntegerType(), True), StructField("report_quarter", IntegerType(), True),
    StructField("period_type", StringType(), True), StructField("period_start_date", DateType(), True), StructField("period_end_date", DateType(), True),
    StructField("sub_category", StringType(), True), StructField("indicator_domain", StringType(), True), StructField("indicator_subdomain", StringType(), True),
    StructField("indicator_name_raw", StringType(), True), StructField("indicator_name_normalized", StringType(), True), StructField("geography_raw", StringType(), True),
    StructField("sector_raw", StringType(), True), StructField("product_raw", StringType(), True), StructField("unit_raw", StringType(), True),
    StructField("metric_name_raw", StringType(), True), StructField("metric_type", StringType(), True), StructField("value_numeric", DoubleType(), True),
    StructField("value_text", StringType(), True), StructField("currency", StringType(), True), StructField("scale", StringType(), True),
    StructField("extraction_method", StringType(), True), StructField("extraction_confidence", DoubleType(), True), StructField("needs_review", BooleanType(), True),
    StructField("source_filename", StringType(), True), StructField("source_sheet_name", StringType(), True), StructField("extracted_timestamp", TimestampType(), True),
])

report_schema = StructType([
    StructField("sheet_report_id", StringType(), True),
    StructField("observation_id", StringType(), False), StructField("report_id", StringType(), False), StructField("attachment_id", StringType(), False),
    StructField("table_id", StringType(), True), StructField("report_year", IntegerType(), True), StructField("report_month", IntegerType(), True),
    StructField("report_quarter", IntegerType(), True), StructField("period_type", StringType(), True), StructField("period_start_date", DateType(), True),
    StructField("period_end_date", DateType(), True), StructField("sub_category", StringType(), True), StructField("source_filename", StringType(), True),
    StructField("source_sheet_name", StringType(), True), StructField("sheet_index", IntegerType(), True), StructField("source_row_index", IntegerType(), True),
    StructField("source_column_index", IntegerType(), True), StructField("table_title_raw", StringType(), True), StructField("row_label_raw", StringType(), True),
    StructField("row_label_normalized", StringType(), True), StructField("geography_raw", StringType(), True), StructField("sector_raw", StringType(), True),
    StructField("product_raw", StringType(), True), StructField("unit_raw", StringType(), True), StructField("metric_name_raw", StringType(), True),
    StructField("metric_type", StringType(), True), StructField("value_numeric", DoubleType(), True), StructField("value_text", StringType(), True),
    StructField("currency", StringType(), True), StructField("scale", StringType(), True), StructField("extraction_method", StringType(), True),
    StructField("extraction_confidence", DoubleType(), True), StructField("needs_review", BooleanType(), True), StructField("extracted_timestamp", TimestampType(), True),
])

# COMMAND ----------

# DBTITLE 1,Write outputs
if cell_rows:
    spark.createDataFrame(cell_rows, cell_schema).createOrReplaceTempView("new_extracted_cells")
    spark.sql(f"""
    MERGE INTO {CELLS_TABLE} AS target
    USING new_extracted_cells AS source
    ON target.sheet_report_id = source.sheet_report_id
       AND target.sheet_index = source.sheet_index
       AND target.row_index = source.row_index
       AND target.column_index = source.column_index
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """)

if table_rows:
    spark.createDataFrame(table_rows, table_schema).createOrReplaceTempView("new_table_inventory")
    spark.sql(f"""
    MERGE INTO {INVENTORY_TABLE} AS target
    USING new_table_inventory AS source
    ON target.table_id = source.table_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """)

if indicator_rows:
    spark.createDataFrame(indicator_rows, indicator_schema).createOrReplaceTempView("new_indicators")
    spark.sql(f"""
    MERGE INTO {LONG_TABLE} AS target
    USING new_indicators AS source
    ON target.indicator_observation_id = source.indicator_observation_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """)

for domain, rows in topic_rows_by_domain.items():
    if not rows:
        continue
    table_name = f"{CATALOG}.{SCHEMA}.{TOPIC_TABLES[domain]}"
    view_name = f"new_{TOPIC_TABLES[domain]}"
    spark.createDataFrame(rows, report_schema).createOrReplaceTempView(view_name)
    spark.sql(f"""
    MERGE INTO {table_name} AS target
    USING {view_name} AS source
    ON target.observation_id = source.observation_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """)

# COMMAND ----------

# DBTITLE 1,Drop superseded rows for reprocessed sheets
# indicator_observation_id / observation_id are derived from label text, so when a reprocessed
# sheet's labels change (e.g. an upstream encoding repair), the MERGEs above INSERT rows under
# new ids and the previous run's rows for that sheet are never matched again -- they linger as
# stale duplicates. Every row written this run carries extracted_timestamp == now, so for each
# sheet processed this run, anything older than `now` is superseded and safe to drop. Topic
# tables are swept for ALL processed sheets (not just the domains written this run) because a
# sheet's domain classification can change between runs, stranding rows in its previous table.
if processed_sheet_report_ids:
    spark.createDataFrame(
        [(sid,) for sid in sorted(processed_sheet_report_ids)], "sheet_report_id STRING"
    ).createOrReplaceTempView("reprocessed_sheets")
    for _stale_table in [LONG_TABLE] + [f"{CATALOG}.{SCHEMA}.{t}" for t in TOPIC_TABLES.values()]:
        spark.sql(f"""
        DELETE FROM {_stale_table}
        WHERE sheet_report_id IN (SELECT sheet_report_id FROM reprocessed_sheets)
          AND extracted_timestamp < timestamp'{now}'
        """)

# COMMAND ----------

# DBTITLE 1,Backfill conservative Vietnamese ASCII label corrections
# Existing extracted/topic rows include legacy Step 2 normalized artifacts from
# old Vietnamese font/codepage conversion. Correct only evidence-backed ASCII
# artifacts in label/metric/unit fields; do not fuzzy-match or translate source
# labels. Future rows are corrected in parse_cells() above.
#
# Word boundaries reach Spark SQL as \\b, not \b: the pattern sits inside a SQL string
# literal, which consumes one level of escaping before the regex engine ever sees it. A
# single \b arrives as a backspace character (U+0008) and the rule then matches nothing --
# silently, since a correction that never fires looks exactly like one with nothing to fix.
def vietnamese_ascii_sql_expr(column_name):
    expr = column_name
    for artifact, replacement in VIETNAMESE_ASCII_CORRECTIONS:
        expr = rf"regexp_replace({expr}, '\\b{artifact}\\b', '{replacement}')"
    return expr

# Unanchored on purpose: this only preselects rows worth rewriting, and a boundary check here
# would cost more than the no-op replacements it saves.
_suspicious_ascii_predicate = "RLIKE '({})'".format(
    "|".join(dict.fromkeys(artifact for artifact, _ in VIETNAMESE_ASCII_CORRECTIONS)))

spark.sql(f"""
UPDATE {LONG_TABLE}
SET
  indicator_name_raw = {vietnamese_ascii_sql_expr('indicator_name_raw')},
  indicator_name_normalized = lower(trim({vietnamese_ascii_sql_expr('indicator_name_raw')})),
  metric_name_raw = {vietnamese_ascii_sql_expr('metric_name_raw')},
  unit_raw = {vietnamese_ascii_sql_expr('unit_raw')},
  geography_raw = {vietnamese_ascii_sql_expr('geography_raw')},
  sector_raw = {vietnamese_ascii_sql_expr('sector_raw')},
  product_raw = {vietnamese_ascii_sql_expr('product_raw')},
  value_text = {vietnamese_ascii_sql_expr('value_text')}
WHERE concat_ws(' ', indicator_name_raw, metric_name_raw, unit_raw, geography_raw, sector_raw, product_raw, value_text) {_suspicious_ascii_predicate}
""")

for _domain, _topic_table in TOPIC_TABLES.items():
    _table_name = f"{CATALOG}.{SCHEMA}.{_topic_table}"
    spark.sql(f"""
    UPDATE {_table_name}
    SET
      table_title_raw = {vietnamese_ascii_sql_expr('table_title_raw')},
      row_label_raw = {vietnamese_ascii_sql_expr('row_label_raw')},
      row_label_normalized = lower(trim({vietnamese_ascii_sql_expr('row_label_raw')})),
      metric_name_raw = {vietnamese_ascii_sql_expr('metric_name_raw')},
      unit_raw = {vietnamese_ascii_sql_expr('unit_raw')},
      geography_raw = {vietnamese_ascii_sql_expr('geography_raw')},
      sector_raw = {vietnamese_ascii_sql_expr('sector_raw')},
      product_raw = {vietnamese_ascii_sql_expr('product_raw')},
      value_text = {vietnamese_ascii_sql_expr('value_text')}
    WHERE concat_ws(' ', table_title_raw, row_label_raw, metric_name_raw, unit_raw, geography_raw, sector_raw, product_raw, value_text) {_suspicious_ascii_predicate}
    """)

# Apply the same corrections to normalized display fields. Raw workbook cells remain unchanged.
spark.sql(f"""
UPDATE {CELLS_TABLE}
SET
  cell_value_raw = {vietnamese_ascii_sql_expr('cell_value_raw')},
  cell_value_clean = {vietnamese_ascii_sql_expr('cell_value_clean')}
WHERE concat_ws(' ', cell_value_raw, cell_value_clean) {_suspicious_ascii_predicate}
""")

spark.sql(f"""
UPDATE {INVENTORY_TABLE}
SET table_title_raw = {vietnamese_ascii_sql_expr('table_title_raw')}
WHERE table_title_raw {_suspicious_ascii_predicate}
""")

spark.sql(f"""
UPDATE {SHEET_REPORTS_TABLE}
SET table_title_raw = {vietnamese_ascii_sql_expr('table_title_raw')}
WHERE table_title_raw {_suspicious_ascii_predicate}
""")

display(spark.sql(f"""
SELECT field, value, COUNT(*) AS rows
FROM (
  SELECT 'extracted_indicator' AS field, indicator_name_raw AS value FROM {LONG_TABLE}
  WHERE indicator_name_raw {_suspicious_ascii_predicate}
  UNION ALL
  SELECT 'extracted_metric' AS field, metric_name_raw AS value FROM {LONG_TABLE}
  WHERE metric_name_raw {_suspicious_ascii_predicate}
  UNION ALL
  SELECT 'trade_row_label' AS field, row_label_raw AS value FROM {CATALOG}.{SCHEMA}.trade_prices_report
  WHERE row_label_raw {_suspicious_ascii_predicate}
  UNION ALL
  SELECT 'trade_metric' AS field, metric_name_raw AS value FROM {CATALOG}.{SCHEMA}.trade_prices_report
  WHERE metric_name_raw {_suspicious_ascii_predicate}
) q
GROUP BY field, value
ORDER BY rows DESC
LIMIT 100
"""))

# COMMAND ----------

# DBTITLE 1,Write sheet report/status grain
sheet_report_rows = []
sheet_log_rows = []

# The report/period fields below are constant within a sheet, so the sheet's first observation
# supplies all of them. One pass collects that row alongside the count; looking each field up by
# scanning indicator_rows instead meant seven walks of the whole run's observations per sheet.
obs_count_by_sheet = {}
first_obs_by_sheet = {}
for row in indicator_rows:
    sheet_report_id = row["sheet_report_id"]
    obs_count_by_sheet[sheet_report_id] = obs_count_by_sheet.get(sheet_report_id, 0) + 1
    first_obs_by_sheet.setdefault(sheet_report_id, row)

for tr in table_rows:
    sheet_report_id = tr["sheet_report_id"]
    obs = first_obs_by_sheet.get(sheet_report_id) or {}
    observation_count = obs_count_by_sheet.get(sheet_report_id, 0)
    extraction_status = "success" if observation_count > 0 else "empty"
    sheet_report_rows.append({
        "sheet_report_id": sheet_report_id,
        "report_id": tr["report_id"],
        "attachment_id": tr["attachment_id"],
        "sheet_index": tr["sheet_index"],
        "sheet_name_raw": tr["sheet_name_raw"],
        "sheet_name_normalized": norm(tr["sheet_name_raw"]),
        "source_filename": obs.get("source_filename"),
        "report_year": obs.get("report_year"),
        "report_month": obs.get("report_month"),
        "report_quarter": obs.get("report_quarter"),
        "period_type": obs.get("period_type"),
        "period_start_date": obs.get("period_start_date"),
        "period_end_date": obs.get("period_end_date"),
        "indicator_domain": tr["indicator_domain"],
        "indicator_subdomain": tr["indicator_subdomain"],
        "table_title_raw": tr["table_title_raw"],
        "parse_status": "success",
        "extraction_status": extraction_status,
        "observation_count": int(observation_count),
        "needs_review": bool(tr["needs_review"]),
        "warnings_json": None,
        "created_at": now,
        "updated_at": now,
    })
    sheet_log_rows.append({
        "sheet_report_id": sheet_report_id,
        "report_id": tr["report_id"],
        "attachment_id": tr["attachment_id"],
        "sheet_index": tr["sheet_index"],
        "parse_status": "success",
        "extraction_status": extraction_status,
        "rows_extracted": int(observation_count),
        "warnings_json": None,
        "error_message": None,
        "run_timestamp": now,
        "updated_at": now,
    })

sheet_report_schema = StructType([
    StructField("sheet_report_id", StringType(), False), StructField("report_id", StringType(), False), StructField("attachment_id", StringType(), False),
    StructField("sheet_index", IntegerType(), False), StructField("sheet_name_raw", StringType(), True), StructField("sheet_name_normalized", StringType(), True),
    StructField("source_filename", StringType(), True), StructField("report_year", IntegerType(), True), StructField("report_month", IntegerType(), True),
    StructField("report_quarter", IntegerType(), True), StructField("period_type", StringType(), True), StructField("period_start_date", DateType(), True),
    StructField("period_end_date", DateType(), True), StructField("indicator_domain", StringType(), True), StructField("indicator_subdomain", StringType(), True),
    StructField("table_title_raw", StringType(), True), StructField("parse_status", StringType(), True), StructField("extraction_status", StringType(), True),
    StructField("observation_count", IntegerType(), True), StructField("needs_review", BooleanType(), True), StructField("warnings_json", StringType(), True),
    StructField("created_at", TimestampType(), True), StructField("updated_at", TimestampType(), True),
])

sheet_log_schema = StructType([
    StructField("sheet_report_id", StringType(), False), StructField("report_id", StringType(), False), StructField("attachment_id", StringType(), False),
    StructField("sheet_index", IntegerType(), False), StructField("parse_status", StringType(), True), StructField("extraction_status", StringType(), True),
    StructField("rows_extracted", IntegerType(), True), StructField("warnings_json", StringType(), True), StructField("error_message", StringType(), True),
    StructField("run_timestamp", TimestampType(), True), StructField("updated_at", TimestampType(), True),
])

if sheet_report_rows:
    spark.createDataFrame(sheet_report_rows, sheet_report_schema).createOrReplaceTempView("new_sheet_reports")
    spark.sql(f"""
    MERGE INTO {SHEET_REPORTS_TABLE} AS target
    USING new_sheet_reports AS source
    ON target.sheet_report_id = source.sheet_report_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """)

# COMMAND ----------
# DBTITLE 1,Durable sheet-lineage backfill and reconciliation
# Older rows were produced before sheet_report_id existed in the long/topic tables.
# The current extractor generates sheet_report_id deterministically from
# report_id|attachment_id|sheet_index; parsed_workbooks_raw supplies sheet_index for
# every legacy row via (report_id, attachment_id, source_sheet_name).
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW nso_lineage_sheet_map AS
SELECT
  report_id,
  attachment_id,
  sheet_name_raw,
  sheet_index,
  substr(md5(concat(report_id, '|', attachment_id, '|', CAST(sheet_index AS STRING))), 1, 16) AS sheet_report_id
FROM {PARSED_TABLE}
""")

spark.sql(f"""
MERGE INTO {LONG_TABLE} AS target
USING nso_lineage_sheet_map AS source
ON target.sheet_report_id IS NULL
   AND target.report_id = source.report_id
   AND target.attachment_id = source.attachment_id
   AND target.source_sheet_name <=> source.sheet_name_raw
WHEN MATCHED THEN UPDATE SET target.sheet_report_id = source.sheet_report_id
""")

for _domain, _topic_table in TOPIC_TABLES.items():
    _table_name = f"{CATALOG}.{SCHEMA}.{_topic_table}"
    spark.sql(f"""
    MERGE INTO {_table_name} AS target
    USING nso_lineage_sheet_map AS source
    ON target.sheet_report_id IS NULL
       AND target.report_id = source.report_id
       AND target.attachment_id = source.attachment_id
       AND target.source_sheet_name <=> source.sheet_name_raw
    WHEN MATCHED THEN UPDATE SET target.sheet_report_id = source.sheet_report_id
    """)

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW nso_sheet_observation_counts AS
SELECT sheet_report_id, COUNT(*) AS observation_count
FROM {LONG_TABLE}
WHERE sheet_report_id IS NOT NULL
GROUP BY sheet_report_id
""")

spark.sql(f"""
MERGE INTO {SHEET_REPORTS_TABLE} AS target
USING nso_sheet_observation_counts AS source
ON target.sheet_report_id = source.sheet_report_id
WHEN MATCHED THEN UPDATE SET
  target.observation_count = CAST(source.observation_count AS INT),
  target.extraction_status = CASE WHEN source.observation_count > 0 THEN 'success' ELSE target.extraction_status END,
  target.updated_at = current_timestamp()
""")

display(spark.sql(f"""
SELECT
  COUNT(*) AS extracted_rows,
  SUM(CASE WHEN sheet_report_id IS NULL THEN 1 ELSE 0 END) AS null_sheet_report_id,
  COUNT(DISTINCT sheet_report_id) AS distinct_sheet_reports
FROM {LONG_TABLE}
"""))

if sheet_log_rows:
    spark.createDataFrame(sheet_log_rows, sheet_log_schema).createOrReplaceTempView("new_sheet_processing_log")
    spark.sql(f"""
    MERGE INTO {SHEET_LOG_TABLE} AS target
    USING new_sheet_processing_log AS source
    ON target.sheet_report_id = source.sheet_report_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """)

# COMMAND ----------

# DBTITLE 1,Final text cleanup for sheet/cell metadata
# Keep sheet/cell metadata synchronized with the same conservative artifact map
# after all Step 3 MERGEs and sheet-report reconciliation have completed.
spark.sql(f"""
UPDATE {CELLS_TABLE}
SET
  cell_value_raw = {vietnamese_ascii_sql_expr('cell_value_raw')},
  cell_value_clean = {vietnamese_ascii_sql_expr('cell_value_clean')}
WHERE concat_ws(' ', cell_value_raw, cell_value_clean) {_suspicious_ascii_predicate}
""")

spark.sql(f"""
UPDATE {INVENTORY_TABLE}
SET table_title_raw = {vietnamese_ascii_sql_expr('table_title_raw')}
WHERE table_title_raw {_suspicious_ascii_predicate}
""")

spark.sql(f"""
UPDATE {SHEET_REPORTS_TABLE}
SET table_title_raw = {vietnamese_ascii_sql_expr('table_title_raw')}
WHERE table_title_raw {_suspicious_ascii_predicate}
""")

# COMMAND ----------

# DBTITLE 1,Update extraction status
status_rows = []
all_attachment_ids = set(status_by_attachment) | set(errors_by_attachment)
for attachment_id in all_attachment_ids:
    if attachment_id in errors_by_attachment:
        status_rows.append({
            "attachment_id": attachment_id,
            "extraction_status": "failed",
            "extraction_timestamp": now,
            "extraction_error_message": errors_by_attachment[attachment_id],
            "extraction_rows_inserted": int(status_by_attachment.get(attachment_id, 0)),
            "updated_at": now,
        })
    else:
        status_rows.append({
            "attachment_id": attachment_id,
            "extraction_status": "success",
            "extraction_timestamp": now,
            "extraction_error_message": None,
            "extraction_rows_inserted": int(status_by_attachment.get(attachment_id, 0)),
            "updated_at": now,
        })

status_schema = StructType([
    StructField("attachment_id", StringType(), False),
    StructField("extraction_status", StringType(), True),
    StructField("extraction_timestamp", TimestampType(), True),
    StructField("extraction_error_message", StringType(), True),
    StructField("extraction_rows_inserted", IntegerType(), True),
    StructField("updated_at", TimestampType(), True),
])

if status_rows:
    spark.createDataFrame(status_rows, status_schema).createOrReplaceTempView("extraction_status_updates")
    spark.sql(f"""
    MERGE INTO {LOG_TABLE} AS target
    USING extraction_status_updates AS source
    ON target.attachment_id = source.attachment_id
    WHEN MATCHED THEN UPDATE SET
      extraction_status = source.extraction_status,
      extraction_timestamp = source.extraction_timestamp,
      extraction_error_message = source.extraction_error_message,
      extraction_rows_inserted = source.extraction_rows_inserted,
      updated_at = source.updated_at
    """)

# COMMAND ----------

# DBTITLE 1,QC summaries
display(spark.sql(f"""
SELECT indicator_domain, indicator_subdomain, extraction_method, COUNT(*) AS observations,
       SUM(CASE WHEN needs_review THEN 1 ELSE 0 END) AS needs_review
FROM {LONG_TABLE}
GROUP BY indicator_domain, indicator_subdomain, extraction_method
ORDER BY observations DESC
"""))

summary_sql = []
for table_name in TOPIC_TABLES.values():
    summary_sql.append(f"SELECT '{table_name}' AS table_name, COUNT(*) AS rows FROM {CATALOG}.{SCHEMA}.{table_name}")

display(spark.sql("\nUNION ALL\n".join(summary_sql) + "\nORDER BY rows DESC"))

display(spark.sql(f"""
SELECT sheet_name_raw, indicator_domain, indicator_subdomain, classification_method, COUNT(*) AS sheets,
       SUM(CASE WHEN needs_review THEN 1 ELSE 0 END) AS needs_review
FROM {INVENTORY_TABLE}
GROUP BY sheet_name_raw, indicator_domain, indicator_subdomain, classification_method
ORDER BY sheets DESC, sheet_name_raw
LIMIT 100
"""))
