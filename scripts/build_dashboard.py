#!/usr/bin/env python3
"""Build or update the NSO Data Output Review Lakeview dashboard.

The dashboard has overview, data-quality, coverage, and per-domain pages. Keeping
the layout as code makes schema changes reviewable and repeatable.

    python scripts/build_dashboard.py            # create or update, then publish
    python scripts/build_dashboard.py --dry-run  # print the payload summary only

Auth resolution is shared with scripts/dbsql.py.
"""
from __future__ import annotations

import argparse
import json
import os
import sys

import requests

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from dbsql import HOST, HEADERS, WAREHOUSE  # noqa: E402

TABLE = "market_data.nso.curated_indicators_long"
DASHBOARD_NAME = "NSO Data Output Review"
PARENT_PATH = os.environ.get("NSO_DASHBOARD_PARENT", "/Users/tuckeyhue@gmail.com")

# (domain value, page title). Ordered biggest first so the busiest pages are nearest the front.
DOMAINS = [
    ("agriculture_forestry_fishery", "Agriculture, Forestry & Fishery"),
    ("industry", "Industry"),
    ("investment_construction", "Investment & Construction"),
    ("retail_services_tourism", "Retail, Services & Tourism"),
    ("national_accounts", "National Accounts"),
    ("transport_post_telecom", "Transport, Post & Telecom"),
    ("other_or_unknown", "Unclassified"),
    ("trade_prices", "Trade & Prices"),
    ("environment_safety_disaster", "Environment, Safety & Disaster"),
    ("enterprise_business_registration", "Enterprise Registration"),
    ("population_labor_social", "Population, Labour & Social"),
]


def slug(domain: str) -> str:
    return "".join(ch if ch.isalnum() else "_" for ch in domain)[:28]


def ds(name: str, display: str, sql: str) -> dict:
    return {"name": name, "displayName": display, "queryLines": [l + "\n" for l in sql.strip().splitlines()]}


def q(dataset: str, fields: list[tuple[str, str]], *, disaggregated=False, filters=None, orders=None) -> dict:
    query = {
        "datasetName": dataset,
        "fields": [{"name": n, "expression": e} for n, e in fields],
        "disaggregated": disaggregated,
    }
    if filters:
        query["filters"] = [{"expression": f} for f in filters]
    if orders:
        query["orders"] = [{"direction": d, "expression": e} for d, e in orders]
    return {"name": "main_query", "query": query}


def text(name: str, lines: list[str], x, y, w, h) -> dict:
    return {
        "widget": {"name": name, "multilineTextboxSpec": {"lines": [l + "\n" for l in lines]}},
        "position": {"x": x, "y": y, "width": w, "height": h},
    }


def counter(name, dataset, field, title, x, y, w=4, h=3, description=None) -> dict:
    frame = {"showTitle": True, "title": title}
    if description:
        frame.update({"showDescription": True, "description": description})
    return {
        "widget": {
            "name": name,
            "queries": [q(dataset, [(field, f"`{field}`")], disaggregated=True)],
            "spec": {
                "version": 2, "widgetType": "counter", "frame": frame,
                "encodings": {"value": {"fieldName": field, "style": {"fontSize": 24}}},
                "data": {"queryName": "main_query"},
            },
        },
        "position": {"x": x, "y": y, "width": w, "height": h},
    }


def chart(name, kind, dataset, xf, yf, title, x, y, w, h, *, color=None, xtitle="", ytitle="",
          xscale="categorical", description=None, filters=None, agg="SUM") -> dict:
    fields = [(xf, f"`{xf}`")]
    yname = f"{agg.lower()}({yf})"
    fields.append((yname, f"{agg}(`{yf}`)"))
    if color:
        fields.append((color, f"`{color}`"))
    frame = {"showTitle": True, "title": title}
    if description:
        frame.update({"showDescription": True, "description": description})
    enc = {
        "x": {"fieldName": xf, "scale": {"type": xscale}, "axis": {"title": xtitle}},
        "y": {"fieldName": yname, "scale": {"type": "quantitative"}, "axis": {"title": ytitle}},
    }
    if color:
        enc["color"] = {"fieldName": color, "scale": {"type": "categorical"}, "axis": {"title": ""}}
    return {
        "widget": {
            "name": name,
            "queries": [q(dataset, fields, filters=filters)],
            "spec": {"version": 3, "widgetType": kind, "frame": frame, "encodings": enc,
                     "data": {"queryName": "main_query"}},
        },
        "position": {"x": x, "y": y, "width": w, "height": h},
    }


def table(name, dataset, cols, title, x, y, w, h, *, description=None, filters=None, orders=None) -> dict:
    frame = {"showTitle": True, "title": title}
    if description:
        frame.update({"showDescription": True, "description": description})
    columns = []
    for i, (field, label, typ) in enumerate(cols):
        columns.append({
            "fieldName": field, "title": label, "visible": True, "order": i,
            "type": typ, "displayAs": typ,
            "alignContent": "right" if typ in ("integer", "float") else "left",
            "booleanValues": ["false", "true"], "allowSearch": field in ("indicator", "metric", "unit"),
            "allowHTML": False, "highlightLinks": False, "useMonospaceFont": False,
            "preserveWhitespace": False,
        })
    return {
        "widget": {
            "name": name,
            "queries": [q(dataset, [(f, f"`{f}`") for f, _, _ in cols], disaggregated=True,
                          filters=filters, orders=orders)],
            "spec": {"version": 1, "widgetType": "table", "frame": frame,
                     "encodings": {"columns": columns}, "data": {"queryName": "main_query"}},
        },
        "position": {"x": x, "y": y, "width": w, "height": h},
    }


def page(name, display, layout) -> dict:
    return {"name": name, "displayName": display, "layout": layout,
            "pageType": "PAGE_TYPE_CANVAS", "layoutVersion": 2}


# --------------------------------------------------------------------------- datasets
datasets: list[dict] = []

datasets.append(ds("ds_ov_kpi", "Overview KPIs", f"""
SELECT
  format_number(COUNT(*), 0)                                   AS observations,
  format_number(COUNT(DISTINCT report_id), 0)                  AS reports,
  format_number(COUNT(DISTINCT sheet_report_id), 0)            AS sheets,
  format_number(COUNT(DISTINCT indicator_name_raw), 0)         AS indicators,
  CAST(MAX(period_end_date) AS STRING)                         AS latest_period,
  CAST(datediff(current_date(), MAX(published_date)) AS STRING) AS days_since_publish
FROM {TABLE}
"""))

datasets.append(ds("ds_ov_domains", "Rows by domain", f"""
SELECT indicator_domain AS domain, COUNT(*) AS observations,
       COUNT(DISTINCT indicator_name_raw) AS indicators,
       COUNT(DISTINCT sheet_report_id) AS sheets
FROM {TABLE} GROUP BY indicator_domain
"""))

datasets.append(ds("ds_ov_timeline", "Observations by year and domain", f"""
SELECT report_year, indicator_domain AS domain, COUNT(*) AS observations
FROM {TABLE} WHERE report_year IS NOT NULL GROUP BY report_year, indicator_domain
"""))

datasets.append(ds("ds_ov_metric", "Metric type mix", f"""
SELECT metric_type, COUNT(*) AS observations
FROM {TABLE} GROUP BY metric_type
"""))

datasets.append(ds("ds_qc_dash", "QC dashboard", """
SELECT check_name, value, threshold, known_open_issue, status, note
FROM market_data.nso.qc_dashboard
"""))

datasets.append(ds("ds_qc_amb", "Ambiguity by domain", """
SELECT indicator_domain AS domain, ambiguous_groups, rows_in_ambiguous_groups AS ambiguous_rows,
       sheets_affected
FROM market_data.nso.qc_business_key_uniqueness
"""))

datasets.append(ds("ds_qc_fields", "Field completeness by domain", f"""
SELECT indicator_domain AS domain,
  COUNT(*) AS observations,
  ROUND(100.0 * SUM(CASE WHEN unit_raw IS NULL THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_no_unit,
  ROUND(100.0 * SUM(CASE WHEN metric_name_raw RLIKE '^column_[0-9]+$' THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_placeholder_metric,
  ROUND(100.0 * SUM(CASE WHEN geography_raw IS NULL THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_no_geography,
  ROUND(100.0 * SUM(CASE WHEN sector_raw IS NULL THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_no_sector,
  ROUND(100.0 * SUM(CASE WHEN needs_review THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_needs_review
FROM {TABLE} GROUP BY indicator_domain
"""))

datasets.append(ds("ds_cov_reports", "Report coverage", """
SELECT report_year, period_type, COUNT(DISTINCT report_id) AS reports
FROM market_data.nso.nso_reports_url
WHERE report_year IS NOT NULL GROUP BY report_year, period_type
"""))

datasets.append(ds("ds_cov_month", "Observations by period end", f"""
SELECT period_end_date, period_type, COUNT(*) AS observations
FROM {TABLE} WHERE period_end_date IS NOT NULL
GROUP BY period_end_date, period_type
"""))

datasets.append(ds("ds_cov_status", "Attachment processing status", """
SELECT attachment_type, attachment_role, download_status, parse_status, extraction_status,
       COUNT(*) AS attachments
FROM market_data.nso.document_processing_log
GROUP BY attachment_type, attachment_role, download_status, parse_status, extraction_status
"""))

for dom, _title in DOMAINS:
    s = slug(dom)
    datasets.append(ds(f"ds_{s}_kpi", f"{dom} KPIs", f"""
SELECT
  format_number(COUNT(*), 0)                           AS observations,
  format_number(COUNT(DISTINCT indicator_name_raw), 0) AS indicators,
  format_number(COUNT(DISTINCT sheet_report_id), 0)    AS sheets,
  concat(CAST(MIN(report_year) AS STRING), ' - ', CAST(MAX(report_year) AS STRING)) AS year_range
FROM {TABLE} WHERE indicator_domain = '{dom}'
"""))
    datasets.append(ds(f"ds_{s}_cov", f"{dom} coverage by year", f"""
SELECT report_year, COUNT(*) AS observations,
       COUNT(DISTINCT indicator_name_raw) AS indicators
FROM {TABLE} WHERE indicator_domain = '{dom}' AND report_year IS NOT NULL
GROUP BY report_year
"""))
    datasets.append(ds(f"ds_{s}_unit", f"{dom} units", f"""
SELECT COALESCE(unit_raw, '(no unit)') AS unit, metric_type, COUNT(*) AS observations
FROM {TABLE} WHERE indicator_domain = '{dom}'
GROUP BY COALESCE(unit_raw, '(no unit)'), metric_type
"""))
    datasets.append(ds(f"ds_{s}_ind", f"{dom} indicators", f"""
SELECT indicator_name_raw AS indicator, metric_name_raw AS metric, metric_type,
       COALESCE(unit_raw, '(no unit)') AS unit,
       COALESCE(sector_raw, product_raw, geography_raw, '') AS breakdown,
       COUNT(*) AS observations,
       ROUND(percentile(value_numeric, 0.5), 2) AS median_value,
       ROUND(MIN(value_numeric), 2) AS min_value,
       ROUND(MAX(value_numeric), 2) AS max_value,
       CAST(MIN(period_end_date) AS STRING) AS first_period,
       CAST(MAX(period_end_date) AS STRING) AS last_period,
       ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) AS rn
FROM {TABLE} WHERE indicator_domain = '{dom}'
GROUP BY indicator_name_raw, metric_name_raw, metric_type, COALESCE(unit_raw, '(no unit)'),
         COALESCE(sector_raw, product_raw, geography_raw, '')
"""))

# --------------------------------------------------------------------------- pages
pages: list[dict] = []

pages.append(page("overview", "Overview", [
    text("ov_t", [
        "# NSO data output review",
        "Vietnam GSO/NSO monthly socio-economic reports, crawled and extracted into "
        "`market_data.nso.curated_indicators_long`. Use **Data quality** to judge whether the corpus "
        "is trustworthy, **Coverage** to see what periods exist, and the per-domain tabs to inspect "
        "the actual indicators.",
    ], 0, 0, 24, 2),
    counter("ov_obs", "ds_ov_kpi", "observations", "Observations", 0, 2, 4, 3),
    counter("ov_rep", "ds_ov_kpi", "reports", "Reports", 4, 2, 4, 3),
    counter("ov_sheet", "ds_ov_kpi", "sheets", "Sheets", 8, 2, 4, 3),
    counter("ov_ind", "ds_ov_kpi", "indicators", "Distinct indicators", 12, 2, 4, 3),
    counter("ov_latest", "ds_ov_kpi", "latest_period", "Latest period covered", 16, 2, 4, 3),
    counter("ov_fresh", "ds_ov_kpi", "days_since_publish", "Days since last publish", 20, 2, 4, 3,
            description="Alerts above 45 in qc_dashboard."),
    chart("ov_dom", "bar", "ds_ov_domains", "domain", "observations",
          "Observations by domain", 0, 5, 12, 7, ytitle="Observations",
          description="How the corpus splits across the 11 indicator domains."),
    chart("ov_metric", "pie", "ds_ov_metric", "metric_type", "observations",
          "Metric type mix", 12, 5, 12, 7,
          description="Levels vs growth vs index. 'value' and 'ytd_value' are levels."),
    chart("ov_time", "bar", "ds_ov_timeline", "report_year", "observations",
          "Observations by year", 0, 12, 24, 7, color="domain", xtitle="Report year",
          ytitle="Observations",
          description="Coverage runs 2000 to date. Volume grows with the number of statistical tables per report."),
]))

pages.append(page("quality", "Data quality", [
    text("q_t", [
        "# Data quality",
        "`qc_dashboard` is the contract: everything not flagged **known_open_issue** must stay at its "
        "threshold, and Step 4 fails the job if one regresses. Known open issues are tracked gaps whose "
        "thresholds sit just above the current value, so the board is green today and turns red only on "
        "regression.",
    ], 0, 0, 24, 2),
    table("q_dash", "ds_qc_dash", [
        ("check_name", "Check", "string"), ("value", "Value", "integer"),
        ("threshold", "Threshold", "integer"), ("status", "Status", "string"),
        ("known_open_issue", "Known open issue", "boolean"), ("note", "Note", "string"),
    ], "QC dashboard", 0, 2, 24, 8,
       description="Green today. A FAIL on a row that is not a known open issue fails the pipeline."),
    chart("q_amb", "bar", "ds_qc_amb", "domain", "ambiguous_rows",
          "Rows without a unique business key, by domain", 0, 10, 12, 7, ytitle="Rows",
          description="Rows sharing sheet+label+column-header while holding different values. "
                      "Residual after the 2026-07-27 hierarchy fix; concentrated where sheet indentation is uniform."),
    table("q_fields", "ds_qc_fields", [
        ("domain", "Domain", "string"), ("observations", "Observations", "integer"),
        ("pct_no_unit", "No unit %", "float"), ("pct_placeholder_metric", "Placeholder metric %", "float"),
        ("pct_no_geography", "No geography %", "float"), ("pct_no_sector", "No sector %", "float"),
        ("pct_needs_review", "Needs review %", "float"),
    ], "Field completeness by domain", 12, 10, 12, 7,
       description="'No unit %' is high by design since the unjustified percent fallback was removed — "
                   "no unit is honest where a wrong one is not.",
       orders=[("DESC", "`observations`")]),
]))

pages.append(page("coverage", "Coverage", [
    text("c_t", [
        "# Coverage and processing",
        "**Caution on `report_month`:** Q1/Q2/Q3 reports carry NULL, so a March filter returns nothing, "
        "and month 12 holds annual full-year data rather than December. Filter on `period_end_date` and "
        "`period_type` instead.",
    ], 0, 0, 24, 2),
    chart("c_rep", "bar", "ds_cov_reports", "report_year", "reports",
          "Reports crawled by year", 0, 2, 24, 6, color="period_type", xtitle="Report year",
          ytitle="Reports", description="Monthly, quarterly, semi-annual and annual releases."),
    chart("c_month", "line", "ds_cov_month", "period_end_date", "observations",
          "Observations by period covered", 0, 8, 24, 7, color="period_type",
          xscale="temporal", xtitle="Period end", ytitle="Observations"),
    table("c_status", "ds_cov_status", [
        ("attachment_type", "Type", "string"), ("attachment_role", "Role", "string"),
        ("download_status", "Download", "string"), ("parse_status", "Parse", "string"),
        ("extraction_status", "Extract", "string"), ("attachments", "Attachments", "integer"),
    ], "Attachment processing status", 0, 15, 24, 7,
       description="Only workbook roles are parsed — HTML and DOCX are narrative and intentionally skipped.",
       orders=[("DESC", "`attachments`")]),
]))

for dom, title in DOMAINS:
    s = slug(dom)
    pages.append(page(f"d_{s}", title, [
        text(f"{s}_t", [f"# {title}", f"`indicator_domain = '{dom}'`"], 0, 0, 24, 2),
        counter(f"{s}_obs", f"ds_{s}_kpi", "observations", "Observations", 0, 2, 6, 3),
        counter(f"{s}_ind", f"ds_{s}_kpi", "indicators", "Distinct indicators", 6, 2, 6, 3),
        counter(f"{s}_sheet", f"ds_{s}_kpi", "sheets", "Sheets", 12, 2, 6, 3),
        counter(f"{s}_yr", f"ds_{s}_kpi", "year_range", "Years covered", 18, 2, 6, 3),
        chart(f"{s}_cov", "bar", f"ds_{s}_cov", "report_year", "observations",
              "Observations by year", 0, 5, 12, 6, xtitle="Report year", ytitle="Observations"),
        chart(f"{s}_unit", "bar", f"ds_{s}_unit", "unit", "observations",
              "Units in use", 12, 5, 12, 6, color="metric_type", ytitle="Observations",
              description="A long tail of near-duplicate units is a known modelling gap."),
        table(f"{s}_tab", f"ds_{s}_ind", [
            ("indicator", "Indicator", "string"), ("breakdown", "Breakdown", "string"),
            ("metric", "Column header", "string"), ("metric_type", "Type", "string"),
            ("unit", "Unit", "string"), ("observations", "Obs", "integer"),
            ("median_value", "Median", "float"), ("min_value", "Min", "float"),
            ("max_value", "Max", "float"),
            ("first_period", "From", "string"), ("last_period", "To", "string"),
        ], "Indicators — median and range", 0, 11, 24, 12,
           description="The sanity check: an implausible median or an extreme min/max is where "
                       "extraction went wrong. Top 250 by observation count.",
           filters=["`rn` <= 250"], orders=[("DESC", "`observations`")]),
    ]))

dashboard = {"datasets": datasets, "pages": pages}


def find_existing():
    r = requests.get(f"{HOST}/api/2.0/lakeview/dashboards", headers=HEADERS,
                     params={"page_size": 100}, timeout=60)
    r.raise_for_status()
    for d in r.json().get("dashboards", []):
        if d.get("display_name") == DASHBOARD_NAME and d.get("lifecycle_state") != "TRASHED":
            return d
    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    payload = json.dumps(dashboard)
    print(f"datasets={len(datasets)} pages={len(pages)} serialized={len(payload):,} bytes")
    for p in pages:
        print(f"  page {p['displayName']:<34} widgets={len(p['layout'])}")
    if args.dry_run:
        return 0

    existing = find_existing()
    body = {"display_name": DASHBOARD_NAME, "warehouse_id": WAREHOUSE, "serialized_dashboard": payload}
    if existing:
        did = existing["dashboard_id"]
        # The list endpoint omits etag; re-read before PATCH.
        cur = requests.get(f"{HOST}/api/2.0/lakeview/dashboards/{did}", headers=HEADERS, timeout=60)
        cur.raise_for_status()
        etag = cur.json().get("etag", "")
        r = requests.patch(f"{HOST}/api/2.0/lakeview/dashboards/{did}", headers=HEADERS,
                           json={**body, "etag": etag}, timeout=120)
        action = "updated"
    else:
        r = requests.post(f"{HOST}/api/2.0/lakeview/dashboards", headers=HEADERS,
                          json={**body, "parent_path": PARENT_PATH}, timeout=120)
        action = "created"
    if r.status_code >= 300:
        print(f"FAILED ({r.status_code}): {r.text[:1200]}")
        return 1
    did = r.json()["dashboard_id"]
    pub = requests.post(f"{HOST}/api/2.0/lakeview/dashboards/{did}/published", headers=HEADERS,
                        json={"embed_credentials": False, "warehouse_id": WAREHOUSE}, timeout=120)
    print(f"{action}: {did} | publish {pub.status_code}")
    print(f"{HOST}/dashboardsv3/{did}/published")
    return 0


if __name__ == "__main__":
    sys.exit(main())
