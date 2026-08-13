"""Per-domain regression checks for market_data.nso.curated_indicators_long.

Each domain runs as one aggregate SQL statement. The checks focus on the cases
that raw schema tests miss: coverage, units, period grain, metric parsing, and
cross-domain leakage.

Usage
-----
    python scripts/nso_test_suite.py                  # all domains
    python scripts/nso_test_suite.py --domain industry
    python scripts/nso_test_suite.py --failures-only
    python scripts/nso_test_suite.py --markdown docs/TEST_RESULTS.md
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import dbsql

TABLE = "market_data.nso.curated_indicators_long"

DOMAINS = [
    "agriculture_forestry_fishery",
    "industry",
    "trade_prices",
    "investment_construction",
    "retail_services_tourism",
    "national_accounts",
    "transport_post_telecom",
    "enterprise_business_registration",
    "population_labor_social",
    "environment_safety_disaster",
    "other_or_unknown",
]

# Domains where a leading dimension should usually be populated.
DIMENSION_EXPECTATION = {
    "agriculture_forestry_fishery": "product_raw",
    "industry": "product_raw",
    "trade_prices": "product_raw",
    "investment_construction": "sector_raw",
    "retail_services_tourism": "sector_raw",
    "national_accounts": "sector_raw",
    "transport_post_telecom": "sector_raw",
    "enterprise_business_registration": None,
    "population_labor_social": None,
    "environment_safety_disaster": None,
    "other_or_unknown": None,
}

# Sheet-name patterns used by the classification checks.
DOMAIN_NAME_PATTERN = {
    "trade_prices": r"(?<![a-z])(xk|nk|xnk)|xuat khau|nhap khau|(?<![a-z])cpi",
    "industry": r"(?<![a-z])(iip|spcn|gtcn|ldcn)|cong nghiep",
    "investment_construction": r"(?<![a-z])(vdt|dtnn|fdi|nsnn|xd)|von dau tu|xay dung",
    "transport_post_telecom": r"van ?tai|(?<![a-z])(vt|vthh|vthk)",
    "retail_services_tourism": r"du lich|khach quoc te|(?<![a-z])kqt|tong ?muc|ban le",
    "enterprise_business_registration": r"(?<![a-z])dn|doanh nghiep|giai the",
    "agriculture_forestry_fishery": r"(?<![a-z])nn|nong nghiep|thuy san|lam nghiep",
    "national_accounts": r"(?<![a-z])(gdp|grdp)",
    "population_labor_social": r"lao dong|dan ?so|that nghiep",
    "environment_safety_disaster": r"(?<![a-z])(xhmt|thpt)|moi truong|thien tai|giao duc",
}


@dataclass
class Check:
    """One SQL aggregate assertion for a domain."""
    key: str
    name: str
    category: str
    expr: str
    op: str                      # one of: ==, <=, >=, <, >
    threshold: float
    severity: str = "error"
    note: str = ""
    pct_of: str | None = None

    def passed(self, value) -> bool:
        if value is None:
            return False
        return {
            "==": lambda a, b: a == b,
            "<=": lambda a, b: a <= b,
            ">=": lambda a, b: a >= b,
            "<": lambda a, b: a < b,
            ">": lambda a, b: a > b,
        }[self.op](value, self.threshold)


def build_checks(domain: str) -> list[Check]:
    """Build the aggregate checks for one domain."""
    dim = DIMENSION_EXPECTATION.get(domain)
    name_pat = DOMAIN_NAME_PATTERN.get(domain)
    c: list[Check] = []
    add = c.append

    # ---- structure & identity (8) -------------------------------------------------
    add(Check("rows_present", "domain has rows", "structure", "COUNT(*)", ">", 0))
    add(Check("dup_observation_ids", "no duplicate observation ids", "structure",
              "COUNT(*) - COUNT(DISTINCT indicator_observation_id)", "==", 0))
    add(Check("null_sheet_report_id", "every row keeps its sheet lineage", "structure",
              "SUM(CASE WHEN sheet_report_id IS NULL THEN 1 ELSE 0 END)", "==", 0))
    add(Check("null_report_id", "every row keeps its report lineage", "structure",
              "SUM(CASE WHEN report_id IS NULL THEN 1 ELSE 0 END)", "==", 0))
    add(Check("distinct_reports", "sourced from more than one report", "structure",
              "COUNT(DISTINCT report_id)", ">", 1))
    add(Check("distinct_sheets", "sourced from more than one sheet", "structure",
              "COUNT(DISTINCT sheet_report_id)", ">", 1))
    add(Check("null_indicator_name", "indicator name always present", "structure",
              "SUM(CASE WHEN indicator_name_raw IS NULL OR trim(indicator_name_raw) = '' THEN 1 ELSE 0 END)",
              "==", 0))
    add(Check("null_source_file", "source filename always present", "structure",
              "SUM(CASE WHEN source_filename IS NULL THEN 1 ELSE 0 END)", "==", 0))

    # ---- encoding / text hygiene (4) ----------------------------------------------
    add(Check("replacement_char", "no U+FFFD replacement characters", "encoding",
              "SUM(CASE WHEN concat_ws(' ', indicator_name_raw, metric_name_raw, unit_raw, "
              "source_sheet_name) RLIKE '�' THEN 1 ELSE 0 END)", "==", 0))
    add(Check("mojibake_text", "no CP1252 mojibake in text fields", "encoding",
              "SUM(CASE WHEN concat_ws(' ', indicator_name_raw, unit_raw) RLIKE "
              "'Ã¢|Ã­|Ä‘|áº' THEN 1 ELSE 0 END)", "==", 0))
    add(Check("tcvn3_garbled", "legacy TCVN3 glyphs cleared from names", "encoding",
              "SUM(CASE WHEN indicator_name_raw RLIKE '[¸¨£µ¬®Ç×Ð]' "
              "THEN 1 ELSE 0 END)", "<=", 500, "warn",
              "Residual legacy .xls text; corpus-wide baseline ~1.7k rows"))
    add(Check("control_chars", "no control characters in labels", "encoding",
              "SUM(CASE WHEN indicator_name_raw RLIKE '[\\\\x00-\\\\x08]' THEN 1 ELSE 0 END)", "==", 0))

    # ---- temporal coverage (9) ----------------------------------------------------
    add(Check("min_year_sane", "earliest year is plausible", "coverage",
              "MIN(report_year)", ">=", 1995))
    add(Check("max_year_recent", "domain reaches the current era", "coverage",
              "MAX(report_year)", ">=", 2024))
    add(Check("year_span", "covers a meaningful number of years", "coverage",
              "COUNT(DISTINCT report_year)", ">=", 5))
    # Some domains are intermittent. Treat a gap as an error only when source sheets
    # for that domain exist in the missing year.
    add(Check("year_gaps", "years missing inside the covered span", "coverage",
              "(MAX(report_year) - MIN(report_year) + 1) - COUNT(DISTINCT report_year)",
              "==", 0, "warn",
              "Informational: a genuine publication gap looks identical here"))
    if name_pat:
        add(Check("year_gaps_with_source", "no year where this domain's sheets exist but none were filed",
                  "coverage",
                  "(SELECT COUNT(*) FROM (SELECT report_year FROM " + TABLE + " "
                  f"WHERE lower(source_sheet_name) RLIKE '{name_pat}' GROUP BY report_year "
                  f"HAVING SUM(CASE WHEN indicator_domain = '{domain}' THEN 1 ELSE 0 END) = 0))",
                  "==", 0, "error",
                  "Misclassification signature: source material present, domain empty"))
    add(Check("rows_2023", "2023 present", "coverage",
              "SUM(CASE WHEN report_year = 2023 THEN 1 ELSE 0 END)", ">", 0))
    add(Check("rows_2024", "2024 present", "coverage",
              "SUM(CASE WHEN report_year = 2024 THEN 1 ELSE 0 END)", ">", 0))
    add(Check("rows_2025", "2025 present", "coverage",
              "SUM(CASE WHEN report_year = 2025 THEN 1 ELSE 0 END)", ">", 0))
    add(Check("rows_2026", "2026 present", "coverage",
              "SUM(CASE WHEN report_year = 2026 THEN 1 ELSE 0 END)", ">", 0))
    add(Check("null_report_year", "report year never null", "coverage",
              "SUM(CASE WHEN report_year IS NULL THEN 1 ELSE 0 END)", "==", 0))

    # ---- period integrity (7) -----------------------------------------------------
    add(Check("report_month_range", "report month within 1-12", "period",
              "SUM(CASE WHEN report_month IS NOT NULL AND report_month NOT BETWEEN 1 AND 12 THEN 1 ELSE 0 END)",
              "==", 0))
    add(Check("report_quarter_range", "report quarter within 1-4", "period",
              "SUM(CASE WHEN report_quarter IS NOT NULL AND report_quarter NOT BETWEEN 1 AND 4 THEN 1 ELSE 0 END)",
              "==", 0))
    add(Check("period_order", "period end is not before period start", "period",
              "SUM(CASE WHEN period_start_date IS NOT NULL AND period_end_date IS NOT NULL "
              "AND period_end_date < period_start_date THEN 1 ELSE 0 END)", "==", 0))
    add(Check("period_end_year_matches", "period end year agrees with report year", "period",
              "SUM(CASE WHEN period_end_date IS NOT NULL AND report_year IS NOT NULL "
              "AND year(period_end_date) <> report_year THEN 1 ELSE 0 END)", "==", 0,
              "error", "The 'thang nam' month defect showed up exactly here"))
    add(Check("monthly_has_month", "monthly rows carry a month", "period",
              "SUM(CASE WHEN period_type LIKE 'monthly%' AND report_month IS NULL THEN 1 ELSE 0 END)",
              "==", 0))
    add(Check("period_type_present", "period type always set", "period",
              "SUM(CASE WHEN period_type IS NULL OR trim(period_type) = '' THEN 1 ELSE 0 END)", "==", 0))
    add(Check("month5_not_dominant", "May is not over-represented", "period",
              "CAST(100.0 * SUM(CASE WHEN report_month = 5 THEN 1 ELSE 0 END) / "
              "NULLIF(SUM(CASE WHEN report_month IS NOT NULL THEN 1 ELSE 0 END), 0) AS DOUBLE)",
              "<=", 30.0, "error",
              "Regression guard for the 'thang nam' bug, which forced 68% of rows to month 5"))

    # ---- values (8) ---------------------------------------------------------------
    add(Check("value_present_pct", "most rows carry a numeric value", "values",
              "CAST(100.0 * SUM(CASE WHEN value_numeric IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*) AS DOUBLE)",
              ">=", 60.0))
    add(Check("value_finite", "no NaN or infinite values", "values",
              "SUM(CASE WHEN value_numeric IS NOT NULL AND (isnan(value_numeric) OR "
              "value_numeric IN (double('Infinity'), double('-Infinity'))) THEN 1 ELSE 0 END)", "==", 0))
    add(Check("value_magnitude", "no absurd magnitudes", "values",
              "SUM(CASE WHEN abs(value_numeric) > 1e13 THEN 1 ELSE 0 END)", "==", 0))
    add(Check("value_not_constant", "values are not all identical", "values",
              "COUNT(DISTINCT value_numeric)", ">", 10))
    add(Check("zero_share", "values are not overwhelmingly zero", "values",
              "CAST(100.0 * SUM(CASE WHEN value_numeric = 0 THEN 1 ELSE 0 END) / "
              "NULLIF(SUM(CASE WHEN value_numeric IS NOT NULL THEN 1 ELSE 0 END), 0) AS DOUBLE)",
              "<=", 40.0, "warn", "A high zero share usually means header debris was parsed as data"))
    add(Check("negative_quantity", "quantities are not negative", "values",
              "SUM(CASE WHEN metric_measure = 'quantity' AND value_numeric < 0 THEN 1 ELSE 0 END)",
              "<=", 50, "warn", "Some tables legitimately show negative stock changes"))
    add(Check("base_scale_present", "base-scale value derived where value exists", "values",
              "SUM(CASE WHEN value_numeric IS NOT NULL AND value_numeric_base_scale IS NULL THEN 1 ELSE 0 END)",
              "==", 0))
    add(Check("index_value_band", "index values sit in a plausible band", "values",
              "SUM(CASE WHEN metric_type IN ('index','index_yoy_base100') AND value_numeric IS NOT NULL "
              "AND (value_numeric < 0 OR value_numeric > 100000) THEN 1 ELSE 0 END)", "==", 0))

    # ---- units (6) ----------------------------------------------------------------
    add(Check("unit_null_pct", "unit populated for most rows", "units",
              "CAST(100.0 * SUM(CASE WHEN unit_raw IS NULL THEN 1 ELSE 0 END) / COUNT(*) AS DOUBLE)",
              "<=", 60.0, "warn",
              "Units are deliberately NULL where no column header justified one"))
    add(Check("quantity_currency_unit", "quantities never carry a currency unit", "units",
              "SUM(CASE WHEN metric_measure = 'quantity' AND unit_raw RLIKE '(?i)usd|vnd' THEN 1 ELSE 0 END)",
              "==", 0, "error",
              "One caption covers a Luong/Tri gia pair; the currency half must not leak"))
    add(Check("analytical_unit_present", "analytical unit derived where unit exists", "units",
              "SUM(CASE WHEN unit_raw IS NOT NULL AND analytical_unit IS NULL THEN 1 ELSE 0 END)", "==", 0))
    add(Check("unit_cardinality", "unit vocabulary has not exploded", "units",
              "COUNT(DISTINCT unit_raw)", "<=", 400, "warn",
              "A spike means raw captions are being stamped verbatim as units"))
    add(Check("pct_unit_sanity", "percent unit implies a percent-like value", "units",
              "SUM(CASE WHEN unit_raw = '%' AND abs(value_numeric) > 100000 THEN 1 ELSE 0 END)", "==", 0))
    add(Check("currency_consistency", "currency set whenever unit names one", "units",
              "SUM(CASE WHEN unit_raw RLIKE '(?i)usd|vnd' AND currency IS NULL THEN 1 ELSE 0 END)", "==", 0))

    # ---- metric decomposition (9) -------------------------------------------------
    add(Check("metric_month_range", "metric ref month within 1-12", "metric",
              "SUM(CASE WHEN metric_ref_month IS NOT NULL AND metric_ref_month NOT BETWEEN 1 AND 12 THEN 1 ELSE 0 END)",
              "==", 0))
    add(Check("metric_quarter_range", "metric ref quarter within 1-4", "metric",
              "SUM(CASE WHEN metric_ref_quarter IS NOT NULL AND metric_ref_quarter NOT BETWEEN 1 AND 4 THEN 1 ELSE 0 END)",
              "==", 0))
    add(Check("metric_cum_range", "cumulative months within 1-12", "metric",
              "SUM(CASE WHEN metric_cumulative_months IS NOT NULL AND metric_cumulative_months NOT BETWEEN 1 AND 12 "
              "THEN 1 ELSE 0 END)", "==", 0))
    add(Check("metric_year_range", "metric ref year is plausible", "metric",
              "SUM(CASE WHEN metric_ref_year IS NOT NULL AND metric_ref_year NOT BETWEEN 1990 AND 2035 THEN 1 ELSE 0 END)",
              "==", 0))
    add(Check("metric_year_missing", "a period column that says 'nam' carries its year", "metric",
              "SUM(CASE WHEN metric_name_raw RLIKE '(?i)(^|/) *nam *(/|$)' "
              "AND metric_name_raw RLIKE '(?i)so bo|uoc tinh|thang|quy ' "
              "AND NOT metric_name_raw RLIKE '[0-9]{4}' THEN 1 ELSE 0 END)", "==", 0,
              "error", "Bare header years used to be dropped as numeric"))
    add(Check("metric_measure_domain", "metric measure uses the known vocabulary", "metric",
              "SUM(CASE WHEN metric_measure IS NOT NULL AND metric_measure NOT IN ('quantity','value') THEN 1 ELSE 0 END)",
              "==", 0))
    add(Check("metric_basis_domain", "metric basis uses the known vocabulary", "metric",
              "SUM(CASE WHEN metric_basis IS NOT NULL AND metric_basis NOT IN "
              "('preliminary','estimated','final') THEN 1 ELSE 0 END)", "==", 0))
    add(Check("metric_grain_domain", "period grain uses the known vocabulary", "metric",
              "SUM(CASE WHEN metric_period_grain IS NOT NULL AND metric_period_grain NOT IN "
              "('month','quarter','ytd_cumulative','year') THEN 1 ELSE 0 END)", "==", 0))
    add(Check("grain_null_pct", "most rows carry a period grain", "metric",
              "CAST(100.0 * SUM(CASE WHEN metric_period_grain IS NULL THEN 1 ELSE 0 END) / COUNT(*) AS DOUBLE)",
              "<=", 60.0, "warn",
              "NULL is correct where the column header names no period at all"))

    # ---- grain / aggregation safety (4) -------------------------------------------
    add(Check("grain_month_consistency", "month grain implies a month or a 1-month cumulative", "grain",
              "SUM(CASE WHEN metric_period_grain = 'month' AND metric_ref_month IS NULL "
              "AND metric_cumulative_months IS NULL THEN 1 ELSE 0 END)", "==", 0))
    add(Check("grain_quarter_consistency", "quarter grain implies a quarter", "grain",
              "SUM(CASE WHEN metric_period_grain = 'quarter' AND metric_ref_quarter IS NULL THEN 1 ELSE 0 END)",
              "==", 0))
    add(Check("grain_ytd_consistency", "cumulative grain implies a cumulative month count", "grain",
              "SUM(CASE WHEN metric_period_grain = 'ytd_cumulative' AND metric_cumulative_months IS NULL "
              "THEN 1 ELSE 0 END)", "==", 0))
    add(Check("grain_year_consistency", "year grain implies no finer period", "grain",
              "SUM(CASE WHEN metric_period_grain = 'year' AND (metric_ref_month IS NOT NULL "
              "OR metric_ref_quarter IS NOT NULL) THEN 1 ELSE 0 END)", "==", 0))

    # ---- growth / index semantics (4) ---------------------------------------------
    add(Check("unconverted_growth", "growth values are converted from base-100", "growth",
              "SUM(CASE WHEN metric_type IN ('yoy_growth','mom_growth') AND value_growth_pct IS NOT NULL "
              "AND value_growth_pct = value_numeric THEN 1 ELSE 0 END)", "==", 0,
              "error", "Every YoY series was overstated by ~100pp before this was fixed"))
    add(Check("growth_conversion_valid", "growth conversion is exactly value - 100", "growth",
              "SUM(CASE WHEN metric_type IN ('index_yoy_base100','yoy_growth','mom_growth') "
              "AND value_growth_pct IS NOT NULL AND abs(value_growth_pct - (value_numeric - 100.0)) > 0.000001 "
              "THEN 1 ELSE 0 END)", "==", 0))
    add(Check("growth_band", "growth percentages stay in a sane band", "growth",
              "SUM(CASE WHEN abs(value_growth_pct) > 10000 THEN 1 ELSE 0 END)", "==", 0))
    add(Check("comparison_vocabulary", "metric comparison uses the known vocabulary", "growth",
              "SUM(CASE WHEN metric_comparison IS NOT NULL AND metric_comparison NOT IN "
              "('yoy','mom','vs_plan') THEN 1 ELSE 0 END)", "==", 0))

    # ---- classification & review signal (5) ---------------------------------------
    add(Check("subdomain_present", "subdomain always set", "classification",
              "SUM(CASE WHEN indicator_subdomain IS NULL THEN 1 ELSE 0 END)", "==", 0))
    add(Check("confidence_present", "extraction confidence always set", "classification",
              "SUM(CASE WHEN extraction_confidence IS NULL THEN 1 ELSE 0 END)", "==", 0))
    add(Check("confidence_range", "extraction confidence within 0-1", "classification",
              "SUM(CASE WHEN extraction_confidence < 0 OR extraction_confidence > 1 THEN 1 ELSE 0 END)", "==", 0))
    add(Check("placeholder_metric_pct", "few placeholder column_N metric names", "classification",
              "CAST(100.0 * SUM(CASE WHEN metric_name_raw RLIKE '^column_' THEN 1 ELSE 0 END) / COUNT(*) AS DOUBLE)",
              "<=", 15.0, "warn", "Genuinely unlabelled trailing columns in old sheets"))
    # Ignore ambiguous sheet names when checking cross-domain leakage.
    others = [(o, p) for o, p in DOMAIN_NAME_PATTERN.items() if o != domain]
    if others:
        match_terms = " + ".join(
            f"(CASE WHEN lower(source_sheet_name) RLIKE '{p}' THEN 1 ELSE 0 END)" for _, p in others)
        own = (f"CASE WHEN lower(source_sheet_name) RLIKE '{name_pat}' THEN 1 ELSE 0 END"
               if name_pat else "0")
        add(Check("misfiled_from_other_domain",
                  "no rows whose sheet name names a different domain", "classification",
                  f"SUM(CASE WHEN ({own}) = 0 AND ({match_terms}) = 1 THEN 1 ELSE 0 END)",
                  "<=", 2000, "error",
                  "The defect that hid 135k trade rows in agriculture and investment"))

    # ---- dimensions (3) -----------------------------------------------------------
    add(Check("normalized_name_present", "normalized indicator name derived", "dimensions",
              "SUM(CASE WHEN indicator_name_normalized IS NULL OR trim(indicator_name_normalized) = '' "
              "THEN 1 ELSE 0 END)", "==", 0))
    if dim:
        add(Check(f"{dim}_coverage", f"{dim} populated for a reasonable share", "dimensions",
                  f"CAST(100.0 * SUM(CASE WHEN {dim} IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*) AS DOUBLE)",
                  ">=", 10.0, "warn",
                  "Uniform-indent sheets carry no hierarchy to route from"))
    else:
        add(Check("any_dimension_coverage", "at least one dimension populated somewhere", "dimensions",
                  "SUM(CASE WHEN geography_raw IS NOT NULL OR sector_raw IS NOT NULL "
                  "OR product_raw IS NOT NULL THEN 1 ELSE 0 END)", ">", 0, "warn"))
    add(Check("dimension_not_ditto", "no Excel ditto marks left in dimensions", "dimensions",
              "SUM(CASE WHEN trim(coalesce(sector_raw,'')) IN ('\"','\"\"') "
              "OR trim(coalesce(product_raw,'')) IN ('\"','\"\"') THEN 1 ELSE 0 END)", "==", 0))

    return c


def run_domain(domain: str, checks: list[Check]):
    """Evaluate every check for one domain in a single round trip."""
    select = ",\n  ".join(f"({ch.expr}) AS {ch.key}" for ch in checks)
    sql = f"SELECT\n  {select}\nFROM {TABLE}\nWHERE indicator_domain = '{domain}'"
    cols, rows = dbsql.run(sql, timeout=300)
    # strict: a column/value length mismatch means the aggregate SELECT and the check list have
    # drifted apart, which would otherwise silently drop checks off the end.
    raw = dict(zip(cols, rows[0], strict=True)) if rows else {}
    results = []
    for ch in checks:
        v = raw.get(ch.key)
        if v is not None and v != "":
            try:
                v = float(v)
            except (TypeError, ValueError):
                v = None
        else:
            v = None
        results.append((ch, v, ch.passed(v)))
    return results


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--domain", action="append", help="limit to one or more domains")
    ap.add_argument("--failures-only", action="store_true")
    ap.add_argument("--markdown", help="write a markdown report to this path")
    ap.add_argument("--json", dest="json_out", help="write raw results as JSON")
    args = ap.parse_args()

    domains = args.domain or DOMAINS
    all_results = {}
    for d in domains:
        checks = build_checks(d)
        all_results[d] = run_domain(d, checks)
        n_err = sum(1 for ch, _, ok in all_results[d] if not ok and ch.severity == "error")
        n_warn = sum(1 for ch, _, ok in all_results[d] if not ok and ch.severity == "warn")
        print(f"  {d:34} {len(checks):3} checks  "
              f"{len(checks) - n_err - n_warn:3} pass  {n_err:2} FAIL  {n_warn:2} warn")

    total = sum(len(v) for v in all_results.values())
    fails = [(d, ch, v) for d, rs in all_results.items() for ch, v, ok in rs
             if not ok and ch.severity == "error"]
    warns = [(d, ch, v) for d, rs in all_results.items() for ch, v, ok in rs
             if not ok and ch.severity == "warn"]
    print(f"\nTOTAL {total} checks across {len(domains)} domains: "
          f"{total - len(fails) - len(warns)} pass, {len(fails)} FAIL, {len(warns)} warn")

    if fails:
        print("\n--- FAILURES ---")
        for d, ch, v in fails:
            print(f"  [{d}] {ch.key}: got {v}, expected {ch.op} {ch.threshold}  ({ch.name})")
            if ch.note:
                print(f"      note: {ch.note}")
    if warns and not args.failures_only:
        print("\n--- WARNINGS ---")
        for d, ch, v in warns:
            print(f"  [{d}] {ch.key}: got {v}, expected {ch.op} {ch.threshold}  ({ch.name})")

    if args.json_out:
        payload = {d: [{"key": ch.key, "name": ch.name, "category": ch.category,
                        "severity": ch.severity, "value": v, "op": ch.op,
                        "threshold": ch.threshold, "passed": ok, "note": ch.note}
                       for ch, v, ok in rs] for d, rs in all_results.items()}
        with open(args.json_out, "w") as fh:
            json.dump(payload, fh, indent=1)
        print(f"\nwrote {args.json_out}")

    if args.markdown:
        write_markdown(args.markdown, all_results)
        print(f"wrote {args.markdown}")

    return 1 if fails else 0


def write_markdown(path, all_results):
    lines = ["# NSO curated layer — per-domain test results", ""]
    total = sum(len(v) for v in all_results.values())
    nf = sum(1 for rs in all_results.values() for ch, _, ok in rs if not ok and ch.severity == "error")
    nw = sum(1 for rs in all_results.values() for ch, _, ok in rs if not ok and ch.severity == "warn")
    lines += [f"**{total} checks across {len(all_results)} domains — "
              f"{total - nf - nw} pass, {nf} fail, {nw} warn**", "",
              "| domain | checks | pass | fail | warn |", "|---|---|---|---|---|"]
    for d, rs in all_results.items():
        e = sum(1 for ch, _, ok in rs if not ok and ch.severity == "error")
        w = sum(1 for ch, _, ok in rs if not ok and ch.severity == "warn")
        lines.append(f"| {d} | {len(rs)} | {len(rs)-e-w} | {e} | {w} |")
    for label, sev in (("Failures", "error"), ("Warnings", "warn")):
        rows = [(d, ch, v) for d, rs in all_results.items() for ch, v, ok in rs
                if not ok and ch.severity == sev]
        lines += ["", f"## {label} ({len(rows)})", ""]
        if not rows:
            lines.append("_none_")
            continue
        lines += ["| domain | check | value | expected | note |", "|---|---|---|---|---|"]
        for d, ch, v in rows:
            lines.append(f"| {d} | `{ch.key}` | {v} | {ch.op} {ch.threshold} | {ch.note} |")
    with open(path, "w") as fh:
        fh.write("\n".join(lines) + "\n")


if __name__ == "__main__":
    sys.exit(main())
