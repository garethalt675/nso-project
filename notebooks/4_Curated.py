# Databricks notebook source
# DBTITLE 1,NSO Step 4 - Curated Layer
# MAGIC %md
# MAGIC # Step 4: Build curated NSO indicators and QC views

# COMMAND ----------

CATALOG = "market_data"
SCHEMA = "nso"

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

# COMMAND ----------
# DBTITLE 1,Mapping dimensions

# Rebuild rule-derived dimensions so corrected labels replace stale values.
spark.sql("DROP TABLE IF EXISTS dim_indicator")
spark.sql("DROP TABLE IF EXISTS dim_geography")
spark.sql("DROP TABLE IF EXISTS dim_unit")

spark.sql("""
CREATE TABLE IF NOT EXISTS dim_indicator (
  indicator_name_raw STRING,
  indicator_name_normalized STRING,
  indicator_name_en STRING,
  indicator_domain STRING,
  indicator_subdomain STRING,
  preferred_unit STRING,
  preferred_scale STRING,
  mapping_method STRING,
  confidence_score DOUBLE,
  needs_review BOOLEAN,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
) USING DELTA
""")

spark.sql("""
MERGE INTO dim_indicator AS target
USING (
  SELECT
    indicator_name_raw,
    indicator_name_normalized,
    indicator_name_en,
    indicator_domain,
    indicator_subdomain,
    preferred_unit,
    preferred_scale,
    mapping_method,
    confidence_score,
    needs_review,
    created_at,
    updated_at
  FROM (
    SELECT
      grouped.*,
      ROW_NUMBER() OVER (
        PARTITION BY indicator_name_normalized, indicator_domain, indicator_subdomain
        ORDER BY row_count DESC, indicator_name_raw, preferred_unit, preferred_scale
      ) AS rn
    FROM (
      SELECT
        indicator_name_raw,
        indicator_name_normalized,
        indicator_name_raw AS indicator_name_en,
        indicator_domain,
        indicator_subdomain,
        unit_raw AS preferred_unit,
        scale AS preferred_scale,
        'rule_normalized_raw' AS mapping_method,
        0.80D AS confidence_score,
        FALSE AS needs_review,
        current_timestamp() AS created_at,
        current_timestamp() AS updated_at,
        COUNT(*) AS row_count
      FROM extracted_indicators_long
      WHERE indicator_name_raw IS NOT NULL
      GROUP BY indicator_name_raw, indicator_name_normalized, indicator_domain, indicator_subdomain, unit_raw, scale
    ) grouped
  ) ranked
  WHERE rn = 1
) AS source
ON target.indicator_name_normalized <=> source.indicator_name_normalized
AND target.indicator_domain <=> source.indicator_domain
AND target.indicator_subdomain <=> source.indicator_subdomain
WHEN NOT MATCHED THEN INSERT *
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS dim_geography (
  geography_raw STRING,
  geography_normalized STRING,
  geography_en STRING,
  geography_type STRING,
  iso2 STRING,
  iso3 STRING,
  province_code STRING,
  mapping_method STRING,
  confidence_score DOUBLE,
  needs_review BOOLEAN,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
) USING DELTA
""")

spark.sql("""
MERGE INTO dim_geography AS target
USING (
  SELECT DISTINCT
    geography_raw,
    lower(trim(geography_raw)) AS geography_normalized,
    geography_raw AS geography_en,
    CASE WHEN geography_raw IS NULL THEN NULL ELSE 'unknown' END AS geography_type,
    CAST(NULL AS STRING) AS iso2,
    CAST(NULL AS STRING) AS iso3,
    CAST(NULL AS STRING) AS province_code,
    'rule_normalized_raw' AS mapping_method,
    0.80D AS confidence_score,
    FALSE AS needs_review,
    current_timestamp() AS created_at,
    current_timestamp() AS updated_at
  FROM extracted_indicators_long
  WHERE geography_raw IS NOT NULL
) AS source
ON target.geography_normalized <=> source.geography_normalized
WHEN NOT MATCHED THEN INSERT *
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS dim_unit (
  unit_raw STRING,
  unit_normalized STRING,
  unit_en STRING,
  scale STRING,
  multiplier_to_base DOUBLE,
  mapping_method STRING,
  confidence_score DOUBLE,
  needs_review BOOLEAN,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
) USING DELTA
""")

spark.sql("""
MERGE INTO dim_unit AS target
USING (
  SELECT DISTINCT
    unit_raw,
    lower(trim(unit_raw)) AS unit_normalized,
    unit_raw AS unit_en,
    scale,
    CASE scale
      WHEN 'thousand' THEN 1000D
      WHEN 'million' THEN 1000000D
      WHEN 'billion' THEN 1000000000D
      WHEN 'trillion' THEN 1000000000000D
      ELSE 1D
    END AS multiplier_to_base,
    'rule_normalized_raw' AS mapping_method,
    0.80D AS confidence_score,
    FALSE AS needs_review,
    current_timestamp() AS created_at,
    current_timestamp() AS updated_at
  FROM extracted_indicators_long
  WHERE unit_raw IS NOT NULL
) AS source
ON target.unit_normalized <=> source.unit_normalized
WHEN NOT MATCHED THEN INSERT *
""")

# COMMAND ----------
# DBTITLE 1,Curated canonical long table

spark.sql("DROP TABLE IF EXISTS curated_indicators_long")
spark.sql("""
CREATE TABLE curated_indicators_long USING DELTA AS
WITH e_dedup AS (
  SELECT * EXCEPT(rn)
  FROM (
    SELECT
      e.*,
      ROW_NUMBER() OVER (
        PARTITION BY indicator_observation_id
        ORDER BY extracted_timestamp DESC, report_id, attachment_id, sheet_report_id, source_row_index, source_column_index
      ) AS rn
    FROM extracted_indicators_long e
  )
  WHERE rn = 1
)
SELECT
  e.indicator_observation_id,
  e.report_id,
  e.attachment_id,
  e.sheet_report_id,
  e.table_id,
  r.report_url,
  r.title_raw AS report_title,
  r.published_date,
  e.report_year,
  e.report_month,
  e.report_quarter,
  e.period_type,
  e.period_start_date,
  e.period_end_date,
  e.sub_category,
  e.indicator_domain,
  e.indicator_subdomain,
  e.indicator_name_raw,
  COALESCE(di.indicator_name_en, e.indicator_name_raw) AS indicator_name_en,
  e.indicator_name_normalized,
  e.metric_name_raw,
  -- metric_name_raw is a composite of the source column's stacked header cells, joined with
  -- " / ": basis, the period the COLUMN refers to, and the measure --
  -- "So bo / thang 01 / nam 2025 / Tri gia". Summing one specific column previously meant
  -- string-matching that whole label, which is brittle and breaks whenever a block's header
  -- is laid out differently. These components break it into filterable parts.
  --
  -- Note metric_ref_year/month describe the COLUMN, which is not always report_year/month:
  -- a January 2025 report carries a "nam 2024" full-prior-year column alongside its
  -- "thang 01 nam 2025" one. That distinction is the whole point of these columns.
  -- Patterns match whole " / "-delimited segments and use explicit spaces rather than \\s or
  -- \\b: this SQL is a non-raw Python string, so a backslash escape would reach Spark's string
  -- parser first and \\b would become a backspace character, not a word boundary.
  CASE
    WHEN e.metric_name_raw RLIKE '(?i)so bo'                THEN 'preliminary'
    WHEN e.metric_name_raw RLIKE '(?i)uoc tinh|uoc thuc hien' THEN 'estimated'
    WHEN e.metric_name_raw RLIKE '(?i)chinh thuc'           THEN 'final'
  END AS metric_basis,
  CASE
    WHEN e.metric_name_raw RLIKE '(?i)(^|/) *luong *(/|$)' THEN 'quantity'
    WHEN e.metric_name_raw RLIKE '(?i)(^|/) *tri gia'      THEN 'value'
  END AS metric_measure,
  CAST(nullif(regexp_extract(lower(e.metric_name_raw), 'thang *([0-9]{1,2})(?![0-9])', 1), '') AS INT)
    AS metric_ref_month,
  CAST(nullif(regexp_extract(lower(e.metric_name_raw), 'nam *([0-9]{4})', 1), '') AS INT)
    AS metric_ref_year,
  -- Quarterly blocks ("Quy I", "Quy III") are as common as monthly ones in these tables.
  -- Tested longest-first so "Quy III" is not matched by the "Quy I" branch.
  CASE
    WHEN e.metric_name_raw RLIKE '(?i)quy *iv'          THEN 4
    WHEN e.metric_name_raw RLIKE '(?i)quy *iii'         THEN 3
    WHEN e.metric_name_raw RLIKE '(?i)quy *ii'          THEN 2
    WHEN e.metric_name_raw RLIKE '(?i)quy *i([^ivx]|$)' THEN 1
  END AS metric_ref_quarter,
  -- "9 thang" = cumulative year-to-date over N months (digits BEFORE "thang"), as opposed to
  -- "thang 09" = the single month of September (digits after).
  CAST(nullif(regexp_extract(lower(e.metric_name_raw), '([0-9]{1,2}) *thang', 1), '') AS INT)
    AS metric_cumulative_months,
  CASE
    WHEN e.metric_name_raw RLIKE '(?i)cung ky|nam truoc' THEN 'yoy'
    WHEN e.metric_name_raw RLIKE '(?i)thang truoc'       THEN 'mom'
    WHEN e.metric_name_raw RLIKE '(?i)ke hoach'          THEN 'vs_plan'
  END AS metric_comparison,
  -- The period GRAIN of the column, and the single most important filter for aggregation:
  -- one NSO table puts a full-year total ("So bo / nam 2024 / Tri gia"), year-to-date
  -- cumulatives ("9 thang / nam 2024") and single months ("thang 01 / nam 2025") side by
  -- side, all with the same metric_ref_year. Summing across them double-counts badly --
  -- an annual column plus its own twelve monthly columns plus every YTD subtotal.
  -- ALWAYS constrain metric_period_grain (and metric_comparison) when aggregating.
  --
  -- Precedence: an explicit "thang NN" wins, because 185 rows carry both forms (a
  -- month-vs-month comparison such as "Thang 9 nam 2001 so voi thang ..."). "12 thang" is
  -- a full year and "01 thang" a single month, so both are mapped to what they mean rather
  -- than left as cumulative.
  CASE
    WHEN e.metric_name_raw IS NULL THEN NULL
    WHEN regexp_extract(lower(e.metric_name_raw), 'thang *([0-9]{1,2})(?![0-9])', 1) <> '' THEN 'month'
    WHEN regexp_extract(lower(e.metric_name_raw), '([0-9]{1,2}) *thang', 1) = '12' THEN 'year'
    WHEN regexp_extract(lower(e.metric_name_raw), '([0-9]{1,2}) *thang', 1) = '1'  THEN 'month'
    WHEN regexp_extract(lower(e.metric_name_raw), '([0-9]{1,2}) *thang', 1) <> '' THEN 'ytd_cumulative'
    WHEN e.metric_name_raw RLIKE '(?i)quy *(i|ii|iii|iv)' THEN 'quarter'
    WHEN regexp_extract(lower(e.metric_name_raw), 'nam *([0-9]{4})', 1) <> '' THEN 'year'
  END AS metric_period_grain,
  -- Many headers name a month but not a year ("Uoc tinh / thang 4") because the year is
  -- implicit from the report. Falling back to report_year makes the components usable for
  -- aggregation without every query repeating the COALESCE. Prefer metric_ref_year when you
  -- need to know what the SOURCE actually stated -- this column cannot distinguish a header
  -- that named the year from one that did not.
  COALESCE(
    CAST(nullif(regexp_extract(lower(e.metric_name_raw), 'nam *([0-9]{4})', 1), '') AS INT),
    e.report_year
  ) AS metric_ref_year_effective,
  e.metric_type,
  e.unit_raw,
  COALESCE(du.unit_en, e.unit_raw) AS unit_en,
  e.scale,
  e.value_numeric,
  CASE
    WHEN du.multiplier_to_base IS NOT NULL AND e.value_numeric IS NOT NULL THEN e.value_numeric * du.multiplier_to_base
    ELSE e.value_numeric
  END AS value_numeric_base_scale,
  -- Vietnamese NSO "so voi cung ky nam truoc (%)" columns are expressed as a base-100 index
  -- (105.2 means +5.2%), not as a growth rate. Every growth metric_type therefore needs the
  -- same -100 conversion that index_yoy_base100 already applies. Convert only where the row is
  -- demonstrably index-like: a row typed as growth but carrying a level unit and a level-sized
  -- value is misclassified upstream in Step 3, and emitting NULL for it is honest where
  -- emitting value_numeric verbatim (the previous behaviour) silently overstated it by ~100pp.
  CASE
    WHEN e.metric_type = 'index_yoy_base100' AND e.value_numeric IS NOT NULL THEN e.value_numeric - 100.0D
    WHEN e.metric_type IN ('yoy_growth', 'mom_growth') AND e.value_numeric IS NOT NULL
         AND (e.unit_raw = '%' OR e.value_numeric BETWEEN 20.0D AND 400.0D) THEN e.value_numeric - 100.0D
    ELSE NULL
  END AS value_growth_pct,
  CASE
    WHEN e.metric_type IN ('index_yoy_base100', 'yoy_growth') THEN 'Percent change vs same period previous year'
    WHEN e.metric_type = 'mom_growth' THEN 'Percent change vs previous month'
    WHEN e.metric_type = 'index' THEN 'Index'
    ELSE COALESCE(du.unit_en, e.unit_raw)
  END AS analytical_unit,
  e.value_text,
  e.currency,
  e.geography_raw,
  COALESCE(dg.geography_en, e.geography_raw) AS geography_en,
  dg.geography_type,
  e.sector_raw,
  e.product_raw,
  e.extraction_method,
  e.extraction_confidence,
  (
    e.needs_review
    OR e.indicator_name_raw IS NULL OR trim(e.indicator_name_raw) = ''
    OR e.value_numeric IS NULL
    OR COALESCE(e.extraction_confidence, 1.0D) < 0.50D
    OR e.indicator_domain = 'other_or_unknown'
  ) AS needs_review,
  e.source_filename,
  e.source_sheet_name,
  e.source_row_index,
  e.source_column_index,
  e.extracted_timestamp
FROM e_dedup e
LEFT JOIN nso_reports_url r ON e.report_id = r.report_id
LEFT JOIN (
  SELECT * EXCEPT(rn)
  FROM (
    SELECT
      di.*,
      ROW_NUMBER() OVER (
        PARTITION BY indicator_name_normalized, indicator_domain, indicator_subdomain
        ORDER BY COALESCE(confidence_score, 0D) DESC, updated_at DESC, indicator_name_raw
      ) AS rn
    FROM dim_indicator di
  )
  WHERE rn = 1
) di ON e.indicator_name_normalized <=> di.indicator_name_normalized AND e.indicator_domain <=> di.indicator_domain AND e.indicator_subdomain <=> di.indicator_subdomain
LEFT JOIN (
  SELECT * EXCEPT(rn)
  FROM (
    SELECT
      du.*,
      ROW_NUMBER() OVER (
        PARTITION BY unit_normalized
        ORDER BY COALESCE(confidence_score, 0D) DESC, updated_at DESC, unit_raw
      ) AS rn
    FROM dim_unit du
  )
  WHERE rn = 1
) du ON lower(trim(e.unit_raw)) <=> du.unit_normalized
LEFT JOIN (
  SELECT * EXCEPT(rn)
  FROM (
    SELECT
      dg.*,
      ROW_NUMBER() OVER (
        PARTITION BY geography_normalized
        ORDER BY COALESCE(confidence_score, 0D) DESC, updated_at DESC, geography_raw
      ) AS rn
    FROM dim_geography dg
  )
  WHERE rn = 1
) dg ON lower(trim(e.geography_raw)) <=> dg.geography_normalized
""")

# COMMAND ----------
# DBTITLE 1,Domain convenience views

spark.sql("""
CREATE OR REPLACE VIEW curated_trade_indicators AS
SELECT * FROM curated_indicators_long
WHERE indicator_domain = 'trade_prices'
  AND indicator_subdomain IN ('exports', 'imports')
""")

spark.sql("""
CREATE OR REPLACE VIEW curated_price_indicators AS
SELECT * FROM curated_indicators_long
WHERE indicator_domain = 'trade_prices'
  AND indicator_subdomain IN ('consumer_price_index', 'producer_price_index')
""")

spark.sql("""
CREATE OR REPLACE VIEW curated_industry_indicators AS
SELECT * FROM curated_indicators_long
WHERE indicator_domain = 'industry'
""")

spark.sql("""
CREATE OR REPLACE VIEW curated_retail_tourism_indicators AS
SELECT * FROM curated_indicators_long
WHERE indicator_domain = 'retail_services_tourism'
""")

spark.sql("""
CREATE OR REPLACE VIEW curated_investment_enterprise_indicators AS
SELECT * FROM curated_indicators_long
WHERE indicator_domain IN ('investment_construction', 'enterprise_business_registration')
""")

spark.sql("""
CREATE OR REPLACE VIEW curated_macro_dashboard_monthly AS
SELECT * FROM curated_indicators_long
WHERE period_type IN ('monthly', 'monthly_single_month', 'monthly_cumulative')
  AND indicator_domain IN ('national_accounts','industry','trade_prices','retail_services_tourism','investment_construction','enterprise_business_registration')
""")

spark.sql("""
CREATE OR REPLACE VIEW curated_sheet_reports AS
SELECT
  sr.*,
  r.report_url,
  r.title_raw AS report_title,
  r.published_date
FROM sheet_reports sr
LEFT JOIN nso_reports_url r ON sr.report_id = r.report_id
""")

spark.sql("""
CREATE OR REPLACE VIEW curated_report_sheet_summary AS
SELECT
  c.report_id,
  COUNT(DISTINCT c.sheet_report_id) AS sheet_reports,
  COUNT(*) AS observations,
  COUNT(DISTINCT CASE WHEN c.needs_review THEN c.sheet_report_id END) AS sheets_needing_review
FROM curated_indicators_long c
GROUP BY c.report_id
""")

# COMMAND ----------
# DBTITLE 1,QC views

spark.sql("""
CREATE OR REPLACE VIEW qc_report_coverage AS
SELECT
  r.report_year,
  r.sub_category,
  COUNT(DISTINCT r.report_id) AS reports,
  COUNT(DISTINCT a.attachment_id) AS attachments,
  COUNT(DISTINCT CASE WHEN a.attachment_type IN ('xls','xlsx','xlsm') THEN a.attachment_id END) AS workbook_attachments,
  COUNT(DISTINCT CASE WHEN l.parse_status = 'success' THEN l.attachment_id END) AS parsed_attachments,
  COUNT(DISTINCT CASE WHEN l.extraction_status = 'success' THEN l.attachment_id END) AS extracted_attachments
FROM nso_reports_url r
LEFT JOIN nso_report_attachments a ON r.report_id = a.report_id
LEFT JOIN document_processing_log l ON a.attachment_id = l.attachment_id
GROUP BY r.report_year, r.sub_category
""")

spark.sql("""
CREATE OR REPLACE VIEW qc_attachment_status AS
SELECT attachment_type, attachment_role, download_status, parse_status, extraction_status, COUNT(*) AS attachments
FROM document_processing_log
GROUP BY attachment_type, attachment_role, download_status, parse_status, extraction_status
""")

spark.sql("""
CREATE OR REPLACE VIEW qc_extraction_review_queue AS
SELECT *
FROM curated_indicators_long
WHERE needs_review = true
""")

spark.sql("""
CREATE OR REPLACE VIEW qc_curated_quality_summary AS
SELECT
  COUNT(*) AS curated_rows,
  COUNT(DISTINCT indicator_observation_id) AS distinct_observation_ids,
  COUNT(*) - COUNT(DISTINCT indicator_observation_id) AS duplicate_observation_extra_rows,
  COUNT(DISTINCT report_id) AS reports,
  COUNT(DISTINCT attachment_id) AS attachments,
  COUNT(DISTINCT sheet_report_id) AS sheet_reports,
  SUM(CASE WHEN sheet_report_id IS NULL THEN 1 ELSE 0 END) AS rows_missing_sheet_report_id,
  SUM(CASE WHEN needs_review THEN 1 ELSE 0 END) AS rows_needing_review,
  ROUND(100.0 * SUM(CASE WHEN needs_review THEN 1 ELSE 0 END) / COUNT(*), 2) AS rows_needing_review_pct,
  SUM(CASE WHEN value_numeric IS NULL THEN 1 ELSE 0 END) AS rows_missing_numeric_value,
  SUM(CASE WHEN indicator_name_raw IS NULL OR trim(indicator_name_raw) = '' THEN 1 ELSE 0 END) AS rows_missing_indicator_name,
  SUM(CASE WHEN source_sheet_name RLIKE '�' OR indicator_name_raw RLIKE '�' OR metric_name_raw RLIKE '�' OR unit_raw RLIKE '�' OR geography_raw RLIKE '�' OR sector_raw RLIKE '�' OR product_raw RLIKE '�' THEN 1 ELSE 0 END) AS rows_with_replacement_character,
  MIN(period_start_date) AS min_period_start_date,
  MAX(period_end_date) AS max_period_end_date,
  MIN(extracted_timestamp) AS min_source_extracted_timestamp,
  MAX(extracted_timestamp) AS max_source_extracted_timestamp
FROM curated_indicators_long
""")

spark.sql("""
CREATE OR REPLACE VIEW qc_sheet_lineage_reconciliation AS
SELECT
  (SELECT COUNT(*) FROM extracted_indicators_long) AS extracted_rows,
  (SELECT SUM(CASE WHEN sheet_report_id IS NULL THEN 1 ELSE 0 END) FROM extracted_indicators_long) AS extracted_null_sheet_report_id,
  (SELECT COUNT(*) FROM curated_indicators_long) AS curated_rows,
  (SELECT SUM(CASE WHEN sheet_report_id IS NULL THEN 1 ELSE 0 END) FROM curated_indicators_long) AS curated_null_sheet_report_id,
  (SELECT SUM(observations) FROM curated_report_sheet_summary) AS curated_sheet_summary_observations,
  (SELECT COUNT(*) FROM curated_indicators_long) - (SELECT SUM(observations) FROM curated_report_sheet_summary) AS curated_minus_sheet_summary_observations
""")

spark.sql("""
CREATE OR REPLACE VIEW qc_semantic_validation AS
SELECT
  SUM(CASE WHEN lower(trim(source_sheet_name)) RLIKE '^14[. _-]*xk' AND indicator_domain <> 'trade_prices' THEN 1 ELSE 0 END) AS export_rows_wrong_domain,
  SUM(CASE WHEN lower(trim(source_sheet_name)) RLIKE '^15[. _-]*nk' AND indicator_domain <> 'trade_prices' THEN 1 ELSE 0 END) AS import_rows_wrong_domain,
  SUM(CASE WHEN lower(trim(source_sheet_name)) RLIKE '^2[. _-]*iip' AND metric_type NOT IN ('index','index_yoy_base100') THEN 1 ELSE 0 END) AS iip_rows_wrong_metric_type,
  -- Must cover every growth metric_type. Narrowing to index_yoy_base100 first would test only
  -- the subset already known to be correct and report 0 regardless of the other types.
  SUM(CASE WHEN metric_type IN ('index_yoy_base100', 'yoy_growth', 'mom_growth')
            AND value_growth_pct IS NOT NULL
            AND abs(value_growth_pct - (value_numeric - 100.0D)) > 0.000001D THEN 1 ELSE 0 END) AS invalid_growth_conversion,
  SUM(CASE WHEN metric_type IN ('yoy_growth', 'mom_growth')
            AND value_growth_pct = value_numeric THEN 1 ELSE 0 END) AS unconverted_growth_rows,
  SUM(CASE WHEN unit_raw RLIKE '[ÃÂÄ]' THEN 1 ELSE 0 END) AS rows_with_mojibake_unit,
  COUNT(*) AS curated_rows
FROM curated_indicators_long
""")

# COMMAND ----------
# DBTITLE 1,Data quality gap checks

# These views cover gaps the older qc_* checks missed during the 2026-07-26 audit
# (see docs/DATA_QUALITY_ASSESSMENT.md).

spark.sql("""
CREATE OR REPLACE VIEW qc_freshness AS
SELECT
  MAX(published_date) AS latest_published,
  MAX(period_end_date) AS latest_period_end,
  datediff(current_date(), MAX(published_date)) AS days_since_publish,
  COUNT(DISTINCT report_id) AS reports,
  COUNT(*) AS curated_rows
FROM curated_indicators_long
""")

spark.sql("""
CREATE OR REPLACE VIEW qc_business_key_uniqueness AS
WITH ambiguous AS (
  SELECT sheet_report_id, indicator_domain, indicator_name_raw, metric_name_raw, COUNT(*) AS n
  FROM curated_indicators_long
  GROUP BY 1, 2, 3, 4
  HAVING COUNT(*) > 1 AND COUNT(DISTINCT value_numeric) > 1
)
SELECT
  indicator_domain,
  COUNT(*) AS ambiguous_groups,
  SUM(n) AS rows_in_ambiguous_groups,
  COUNT(DISTINCT sheet_report_id) AS sheets_affected
FROM ambiguous
GROUP BY indicator_domain
""")

spark.sql("""
CREATE OR REPLACE VIEW qc_unit_metric_coherence AS
SELECT
  SUM(CASE WHEN metric_type IN ('yoy_growth','mom_growth')
            AND unit_raw IS NOT NULL AND unit_raw <> '%' THEN 1 ELSE 0 END) AS growth_type_with_level_unit,
  SUM(CASE WHEN metric_type IN ('yoy_growth','mom_growth')
            AND abs(value_numeric) > 1000 THEN 1 ELSE 0 END) AS growth_value_implausible,
  -- Percent is a per-column property; this counts rows that took '%' from the sheet-title
  -- fallback in Step 3's infer_unit_raw with nothing in the column header justifying it.
  SUM(CASE WHEN unit_raw = '%'
            AND NOT (metric_name_raw RLIKE '(?i)%|phan tram|co cau|chi so|tang|giam|so voi')
           THEN 1 ELSE 0 END) AS pct_unit_unjustified_by_header,
  SUM(CASE WHEN metric_type IN ('share','percent')
            AND (value_numeric < 0 OR value_numeric > 100) THEN 1 ELSE 0 END) AS share_out_of_range,
  SUM(CASE WHEN metric_name_raw RLIKE '^column_[0-9]+$' THEN 1 ELSE 0 END) AS placeholder_metric_rows
FROM curated_indicators_long
""")

spark.sql("""
CREATE OR REPLACE VIEW qc_calendar_model AS
-- report_month is overloaded and traps month-based filters: Q1/Q2/Q3 reports carry NULL (so
-- month 3 returns nothing in any year) and month 12 is annual full-year data, not December.
-- period_end_date + period_type is the only safe temporal filter today.
SELECT
  SUM(CASE WHEN report_month = 3 THEN 1 ELSE 0 END) AS rows_month_3,
  SUM(CASE WHEN report_month IS NULL THEN 1 ELSE 0 END) AS rows_null_month,
  SUM(CASE WHEN report_month = 12 AND period_type = 'annual' THEN 1 ELSE 0 END) AS month12_annual_rows,
  SUM(CASE WHEN report_month = 12 AND period_type = 'monthly' THEN 1 ELSE 0 END) AS month12_monthly_rows
FROM curated_indicators_long
""")

spark.sql("""
CREATE OR REPLACE VIEW qc_domain_classification AS
-- Does the sheet's own name agree with the domain it was filed under?
--
-- Two rules this check has to obey, both learned the hard way.
--
-- Cover all ten domains, and match the spellings sheets actually use. A pattern like
-- 'van tai|vt hh|vt hk' matches none of "Vantai", "VT", "VTHH" or "06VT", so it scores
-- transport clean no matter how badly transport is misfiled. A check narrowed to a subset
-- of names reports 0 by construction.
--
-- Abstain rather than guess: a name like "XNK Dich vu" legitimately suggests two domains,
-- so where more than one family matches, report nothing instead of a false error.
WITH flagged AS (
  SELECT indicator_domain, source_sheet_name,
    -- fold d-stroke so "VDTTXH" and "VDTTXH" normalize alike; \\b-style guards keep the
    -- two-letter codes from firing inside longer words ('nn' inside 'dtnn').
    lower(regexp_replace(source_sheet_name, '[^a-zA-Z0-9 ._-]', 'd')) AS nm
  FROM curated_indicators_long
), m AS (
  SELECT indicator_domain, source_sheet_name,
    CASE WHEN nm RLIKE '(?<![a-z])(xk|nk|xnk)|xuat khau|nhap khau|(?<![a-z])cpi|gia tieu dung' THEN 1 ELSE 0 END AS f_trade,
    CASE WHEN nm RLIKE '(?<![a-z])(iip|spcn|gtcn|ldcn)|sp cn|cong nghiep'                      THEN 1 ELSE 0 END AS f_industry,
    CASE WHEN nm RLIKE '(?<![a-z])(vdt|dtnn|fdi|nsnn)|von dau tu|xay dung'                     THEN 1 ELSE 0 END AS f_investment,
    CASE WHEN nm RLIKE 'van ?tai|(?<![a-z])(vt|vthh|vthk)|buu chinh|vien thong'                THEN 1 ELSE 0 END AS f_transport,
    CASE WHEN nm RLIKE 'du lich|khach quoc te|(?<![a-z])kqt|tong ?muc|ban le'                  THEN 1 ELSE 0 END AS f_retail,
    CASE WHEN nm RLIKE '(?<![a-z])dn|doanh nghiep|giai the'                                    THEN 1 ELSE 0 END AS f_enterprise,
    CASE WHEN nm RLIKE '(?<![a-z])nn|nong nghiep|thuy san|lam nghiep'                          THEN 1 ELSE 0 END AS f_agri,
    CASE WHEN nm RLIKE '(?<![a-z])(gdp|grdp)'                                                  THEN 1 ELSE 0 END AS f_accounts,
    CASE WHEN nm RLIKE 'lao dong|dan so|that nghiep|viec lam'                                  THEN 1 ELSE 0 END AS f_labor,
    CASE WHEN nm RLIKE '(?<![a-z])(xhmt|thpt)|moi truong|thien tai|giao duc|tot nghiep'        THEN 1 ELSE 0 END AS f_environment
  FROM flagged
), expected AS (
  SELECT indicator_domain, source_sheet_name,
    CASE WHEN f_trade + f_industry + f_investment + f_transport + f_retail
            + f_enterprise + f_agri + f_accounts + f_labor + f_environment <> 1 THEN NULL
         WHEN f_trade = 1       THEN 'trade_prices'
         WHEN f_industry = 1    THEN 'industry'
         WHEN f_investment = 1  THEN 'investment_construction'
         WHEN f_transport = 1   THEN 'transport_post_telecom'
         WHEN f_retail = 1      THEN 'retail_services_tourism'
         WHEN f_enterprise = 1  THEN 'enterprise_business_registration'
         WHEN f_agri = 1        THEN 'agriculture_forestry_fishery'
         WHEN f_accounts = 1    THEN 'national_accounts'
         WHEN f_labor = 1       THEN 'population_labor_social'
         WHEN f_environment = 1 THEN 'environment_safety_disaster'
    END AS expected_domain
  FROM m
)
SELECT
  COUNT(*) AS rows_with_named_domain,
  SUM(CASE WHEN expected_domain <> indicator_domain THEN 1 ELSE 0 END) AS misclassified_rows,
  ROUND(100.0 * SUM(CASE WHEN expected_domain <> indicator_domain THEN 1 ELSE 0 END) / COUNT(*), 1) AS misclassified_pct,
  COUNT(DISTINCT CASE WHEN expected_domain <> indicator_domain THEN source_sheet_name END) AS sheets_affected
FROM expected
WHERE expected_domain IS NOT NULL
""")

spark.sql("""
CREATE OR REPLACE VIEW qc_metric_decomposition AS
-- Guards the metric_* component columns and the two defects that motivated them.
--
-- metric_year_missing is the signature of the dropped-year bug: NSO trade tables write one
-- block's year as text ("nam 2025") and the next block's as a bare "2024" on its own header
-- row. header_for_col skipped the bare year as numeric, so the column's label collapsed to
-- "So bo / nam / Luong" and lost the period it referred to entirely -- indistinguishable, in
-- the output, from a column that genuinely had no year.
--
-- quantity_with_currency_unit is the paired unit defect: these tables print ONE caption
-- ("Nghin tan; Trieu USD") over a Luong/Tri gia column pair, and the caption's currency half
-- was being applied to both, labelling quantities in USD.
SELECT
  COUNT(*) AS rows_total,
  -- Anchored to the defect's signature: a period-structured column (a basis, month or quarter)
  -- carrying a bare "nam" segment and no year anywhere. A bare "nam" on its own is not
  -- evidence -- it is also "male" ("Phan theo gioi tinh / Nam", "Chia ra: / Nam") and part of
  -- the province Quang Nam, so an untargeted predicate here returns noise almost exclusively.
  SUM(CASE WHEN metric_name_raw RLIKE '(?i)(^|/) *nam *(/|$)'
            AND metric_name_raw RLIKE '(?i)so bo|uoc tinh|thang|quy '
            AND NOT metric_name_raw RLIKE '[0-9]{4}' THEN 1 ELSE 0 END) AS metric_year_missing,
  -- Informational: the header names a month but no year, which is normal -- the year is
  -- implicit from the report ("Uoc tinh / thang 4"). metric_ref_year_effective resolves it.
  SUM(CASE WHEN metric_ref_year IS NULL AND metric_ref_month IS NOT NULL THEN 1 ELSE 0 END) AS metric_month_without_year,
  -- Rows with no period grain cannot be aggregated safely: they are indistinguishable from
  -- an annual total when summing. Mostly legacy sheets whose metric names are still
  -- TCVN3-garbled ("2 th¸ng dau n¨m 2009"), so the period words do not match.
  SUM(CASE WHEN metric_period_grain IS NULL THEN 1 ELSE 0 END) AS metric_grain_unknown,
  COUNT(DISTINCT metric_period_grain) AS distinct_grain,
  SUM(CASE WHEN metric_measure IS NULL
            AND metric_name_raw RLIKE '(?i)(^|/) *(luong|tri gia)' THEN 1 ELSE 0 END) AS measure_unparsed,
  SUM(CASE WHEN metric_measure = 'quantity'
            AND unit_raw RLIKE '(?i)usd|vnd|dong' THEN 1 ELSE 0 END) AS quantity_with_currency_unit,
  SUM(CASE WHEN metric_ref_year IS NOT NULL AND metric_ref_year <> report_year THEN 1 ELSE 0 END) AS metric_year_differs_from_report,
  COUNT(DISTINCT metric_basis) AS distinct_basis,
  COUNT(DISTINCT metric_measure) AS distinct_measure
FROM curated_indicators_long
""")

spark.sql("""
CREATE OR REPLACE VIEW qc_dimension_coverage AS
SELECT
  COUNT(*) AS curated_rows,
  SUM(CASE WHEN geography_raw IS NULL THEN 1 ELSE 0 END) AS null_geography,
  SUM(CASE WHEN sector_raw IS NULL THEN 1 ELSE 0 END) AS null_sector,
  SUM(CASE WHEN product_raw IS NULL THEN 1 ELSE 0 END) AS null_product,
  SUM(CASE WHEN geography_raw IS NOT NULL AND geography_type = 'unknown' THEN 1 ELSE 0 END) AS geography_unclassified,
  COUNT(DISTINCT geography_type) AS distinct_geography_types,
  ROUND(100.0 * SUM(CASE WHEN indicator_name_en = indicator_name_raw THEN 1 ELSE 0 END) / COUNT(*), 1) AS indicator_en_untranslated_pct,
  ROUND(100.0 * SUM(CASE WHEN unit_en = unit_raw THEN 1 ELSE 0 END) / COUNT(*), 1) AS unit_en_untranslated_pct
FROM curated_indicators_long
""")

# COMMAND ----------
# DBTITLE 1,QC dashboard — one row per check, PASS/FAIL against a baseline

# Thresholds are set just above the 2026-07-26 measured baseline for defects that are known and
# still open, so the dashboard reads green today and turns red on REGRESSION. A check that is
# permanently red teaches people to ignore it. `known_open_issue` distinguishes "tracked gap,
# holding steady" from "must stay at zero".

spark.sql("""
CREATE OR REPLACE VIEW qc_dashboard AS
WITH f AS (SELECT * FROM qc_freshness),
     s AS (SELECT * FROM qc_semantic_validation),
     u AS (SELECT * FROM qc_unit_metric_coherence),
     l AS (SELECT * FROM qc_sheet_lineage_reconciliation),
     c AS (SELECT * FROM qc_curated_quality_summary),
     b AS (SELECT COALESCE(SUM(rows_in_ambiguous_groups), 0) AS rows_ambiguous FROM qc_business_key_uniqueness),
     dc AS (SELECT * FROM qc_domain_classification),
     md AS (SELECT * FROM qc_metric_decomposition),
     checks AS (
       SELECT 'freshness_days_since_publish' AS check_name, CAST(f.days_since_publish AS BIGINT) AS value, 45L AS threshold, FALSE AS known_open_issue, 'No schedule on the job; staleness has no other alarm' AS note FROM f
       UNION ALL SELECT 'unconverted_growth_rows', CAST(s.unconverted_growth_rows AS BIGINT), 0L, FALSE, 'Fixed 2026-07-26; must stay at zero' FROM s
       UNION ALL SELECT 'invalid_growth_conversion', CAST(s.invalid_growth_conversion AS BIGINT), 0L, FALSE, 'value_growth_pct must equal value_numeric - 100' FROM s
       UNION ALL SELECT 'rows_with_mojibake_unit', CAST(s.rows_with_mojibake_unit AS BIGINT), 0L, FALSE, 'TCVN3 remediation must hold' FROM s
       UNION ALL SELECT 'duplicate_observation_extra_rows', CAST(c.duplicate_observation_extra_rows AS BIGINT), 0L, FALSE, 'MERGE key integrity' FROM c
       UNION ALL SELECT 'rows_missing_sheet_report_id', CAST(c.rows_missing_sheet_report_id AS BIGINT), 0L, FALSE, 'Lineage completeness' FROM c
       UNION ALL SELECT 'curated_minus_sheet_summary', CAST(abs(l.curated_minus_sheet_summary_observations) AS BIGINT), 0L, FALSE, 'Curated must reconcile to the sheet summary' FROM l
       -- Thresholds retightened to the post-Phase-2 baseline (2026-07-27). Leaving them at the
       -- pre-Phase-2 values would let the fix silently regress by 40% before anything complained.
       UNION ALL SELECT 'rows_in_ambiguous_groups', b.rows_ambiguous, 85000L, TRUE, 'Residual hierarchy: sheets with uniform/absent indent (transport, agriculture)' FROM b
       UNION ALL SELECT 'pct_unit_unjustified_by_header', CAST(u.pct_unit_unjustified_by_header AS BIGINT), 12000L, TRUE, 'Residual after removing the table-wide percent fallback' FROM u
       UNION ALL SELECT 'growth_type_with_level_unit', CAST(u.growth_type_with_level_unit AS BIGINT), 70000L, TRUE, 'metric_type still keyword-driven; unit evidence only demotes implausible magnitudes' FROM u
       UNION ALL SELECT 'placeholder_metric_rows', CAST(u.placeholder_metric_rows AS BIGINT), 20000L, TRUE, 'Residual column_N on genuinely unlabelled columns' FROM u
       UNION ALL SELECT 'domain_misclassified_rows', CAST(dc.misclassified_rows AS BIGINT), 5000L, FALSE, 'Sheet name must agree with the domain it was filed under' FROM dc
       UNION ALL SELECT 'metric_year_missing', CAST(md.metric_year_missing AS BIGINT), 100L, FALSE, 'A metric column saying "nam" must carry the year it refers to' FROM md
       UNION ALL SELECT 'quantity_with_currency_unit', CAST(md.quantity_with_currency_unit AS BIGINT), 500L, FALSE, 'Luong columns must not inherit the Tri gia half of a combined unit caption' FROM md
     )
SELECT
  check_name,
  value,
  threshold,
  known_open_issue,
  CASE WHEN value > threshold THEN 'FAIL' ELSE 'PASS' END AS status,
  note
FROM checks
""")

# COMMAND ----------
# DBTITLE 1,Run summary

# Fail loudly rather than leaving a red row in a view nobody opens. Anything not flagged
# known_open_issue is either a fixed defect that must not come back or a structural invariant.
_failures = spark.sql("""
SELECT check_name, value, threshold, note
FROM qc_dashboard
WHERE status = 'FAIL' AND NOT known_open_issue
ORDER BY check_name
""").collect()

display(spark.sql("SELECT * FROM qc_dashboard ORDER BY status DESC, known_open_issue, check_name"))

if _failures:
    raise AssertionError(
        "QC dashboard regressions:\n"
        + "\n".join(f"  {r.check_name}: {r.value} > {r.threshold} ({r.note})" for r in _failures)
    )

display(spark.sql("SELECT * FROM qc_report_coverage ORDER BY report_year DESC, sub_category"))
display(spark.sql("SELECT indicator_domain, COUNT(*) AS rows, SUM(CASE WHEN needs_review THEN 1 ELSE 0 END) AS needs_review FROM curated_indicators_long GROUP BY indicator_domain ORDER BY rows DESC"))
