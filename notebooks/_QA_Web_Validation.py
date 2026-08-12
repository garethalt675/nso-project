# Databricks notebook source
import json

def rows(sql):
    return [r.asDict(recursive=True) for r in spark.sql(sql).collect()]

payload = {
    "dashboard": rows("SELECT * FROM market_data.nso.qc_dashboard ORDER BY status DESC, check_name"),
    "freshness": rows("SELECT * FROM market_data.nso.qc_freshness"),
    "semantic_qc": rows("SELECT * FROM market_data.nso.qc_semantic_validation"),
    "quality_summary": rows("SELECT * FROM market_data.nso.qc_curated_quality_summary"),
    "unit_metric_coherence": rows("SELECT * FROM market_data.nso.qc_unit_metric_coherence"),
    "business_key_uniqueness": rows(
        "SELECT * FROM market_data.nso.qc_business_key_uniqueness ORDER BY rows_in_ambiguous_groups DESC"
    ),
    "recent_numeric_sample": rows("""
        SELECT indicator_domain AS domain, indicator_subdomain AS subdomain,
               indicator_name_raw AS indicator, metric_name_raw AS metric,
               unit_raw AS unit, metric_type, value_numeric, value_growth_pct,
               source_sheet_name AS sheet
        FROM market_data.nso.curated_indicators_long
        WHERE value_numeric IS NOT NULL
          AND report_year = 2026
          AND report_month = 4
          AND lower(trim(source_sheet_name)) RLIKE '^(2[. _-]*iip|14[. _-]*xk|15[. _-]*nk)'
          AND (lower(indicator_name_raw) RLIKE 'tong|toan nganh|cong nghiep che bien'
               OR abs(value_numeric-109.2)<0.01 OR abs(value_numeric-109.9)<0.01)
        ORDER BY indicator_domain, source_sheet_name, indicator_name_raw, metric_name_raw
        LIMIT 60
    """),
}

print(json.dumps(payload, ensure_ascii=False, default=str))
dbutils.notebook.exit(json.dumps(payload, ensure_ascii=False, default=str))
