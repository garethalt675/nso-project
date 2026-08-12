-- Data quality checks covering the gaps the built-in qc_* views do not reach.
-- Companion to docs/DATA_QUALITY_ASSESSMENT.md (assessed 2026-07-26).
-- Run against market_data.nso on warehouse 7eb5fd2336243915.
--
-- The existing qc_* views validate STRUCTURE (lineage, key uniqueness, encoding) and all pass.
-- These validate MEANING. Expected values below are the 2026-07-26 baseline; any of them moving
-- up is a regression, and the P1/P2 checks should reach 0 once the fixes land.

USE CATALOG market_data;
USE SCHEMA nso;

-- ---------------------------------------------------------------------------
-- P1. Growth conversion. `index_yoy_base100` derives value_growth_pct as
-- (value_numeric - 100); `yoy_growth` copies value_numeric verbatim instead.
-- qc_semantic_validation.invalid_index_growth_conversion filters to
-- index_yoy_base100 BEFORE testing, so it validates only the correct subset.
-- FIXED 2026-07-26: unconverted_growth_rows 237,972 -> 0. This check must stay at 0;
-- growth_rows_missing_growth_pct is now 7,782 yoy/mom rows that are not index-like
-- (misclassified upstream in Step 3) plus 14,535 'index' rows, which carry no growth by design.
-- ---------------------------------------------------------------------------
SELECT
  'P1_growth_conversion' AS check_name,
  SUM(CASE WHEN metric_type IN ('yoy_growth','mom_growth')
            AND value_growth_pct IS NOT NULL
            AND value_growth_pct = value_numeric THEN 1 ELSE 0 END) AS unconverted_growth_rows,
  SUM(CASE WHEN metric_type = 'yoy_growth'
            AND value_numeric BETWEEN 50 AND 200 THEN 1 ELSE 0 END) AS index_like_yoy_rows,
  SUM(CASE WHEN metric_type IN ('yoy_growth','mom_growth','index')
            AND value_growth_pct IS NULL THEN 1 ELSE 0 END)         AS growth_rows_missing_growth_pct
FROM curated_indicators_long;

-- ---------------------------------------------------------------------------
-- P2.1 Business-key uniqueness. Rows sharing sheet + label + column header while
-- holding different values cannot be joined or aggregated on meaning; only
-- source_row_index separates them. Caused by literal ditto marks (") and
-- unpropagated hierarchical parent labels.
-- Baseline: 31,167 groups / 136,535 rows / 1,620 sheets (19.90% of corpus).
-- ---------------------------------------------------------------------------
WITH ambiguous AS (
  SELECT sheet_report_id, indicator_name_raw, metric_name_raw, COUNT(*) AS n
  FROM curated_indicators_long
  GROUP BY 1, 2, 3
  HAVING COUNT(*) > 1 AND COUNT(DISTINCT value_numeric) > 1
)
SELECT
  'P2_1_ambiguous_business_key' AS check_name,
  COUNT(*)                        AS ambiguous_groups,
  SUM(n)                          AS rows_in_ambiguous_groups,
  COUNT(DISTINCT sheet_report_id) AS sheets_affected,
  ROUND(100.0 * SUM(n) / (SELECT COUNT(*) FROM curated_indicators_long), 2) AS pct_of_corpus
FROM ambiguous;

-- Per-domain concentration: transport_post_telecom is 89.3% affected.
WITH ambiguous AS (
  SELECT sheet_report_id, indicator_name_raw, metric_name_raw
  FROM curated_indicators_long
  GROUP BY 1, 2, 3
  HAVING COUNT(*) > 1 AND COUNT(DISTINCT value_numeric) > 1
),
totals AS (SELECT indicator_domain, COUNT(*) AS domain_rows FROM curated_indicators_long GROUP BY 1)
SELECT c.indicator_domain, COUNT(*) AS rows_affected, t.domain_rows,
       ROUND(100.0 * COUNT(*) / t.domain_rows, 2) AS pct_of_domain
FROM curated_indicators_long c
JOIN ambiguous a USING (sheet_report_id, indicator_name_raw, metric_name_raw)
JOIN totals t ON t.indicator_domain = c.indicator_domain
GROUP BY c.indicator_domain, t.domain_rows
ORDER BY pct_of_domain DESC;

-- Label pathologies driving it. Baseline: 65,554 rows carry a ditto mark.
SELECT
  'P2_1_label_pathologies' AS check_name,
  SUM(CASE WHEN indicator_name_raw RLIKE '"'          THEN 1 ELSE 0 END) AS rows_with_ditto_mark,
  SUM(CASE WHEN length(trim(indicator_name_raw)) <= 3 THEN 1 ELSE 0 END) AS rows_label_len_le_3
FROM curated_indicators_long;

-- ---------------------------------------------------------------------------
-- P2.2 Timeliness. Baseline: 84 days since last publish (2026-05-03);
-- May + June 2026 monthly reports never crawled. Alert above ~45 days.
-- ---------------------------------------------------------------------------
SELECT
  'P2_2_freshness' AS check_name,
  MAX(published_date)                          AS latest_published,
  MAX(period_end_date)                         AS latest_period_end,
  datediff(current_date(), MAX(published_date)) AS days_since_publish
FROM curated_indicators_long;

-- ---------------------------------------------------------------------------
-- P2.3 Unit / metric coherence.
-- Baseline: 64,812 growth-typed rows with level units; 2,964 growth rows > 1000
-- (2,757 of them yoy_growth, max 1,120,438 — header-to-column misattribution);
-- 140,199 rows take '%' from the sheet-title fallback with nothing in the
-- header justifying it; 1,408 share/percent rows outside 0-100.
-- ---------------------------------------------------------------------------
SELECT
  'P2_3_unit_metric_coherence' AS check_name,
  SUM(CASE WHEN metric_type IN ('yoy_growth','mom_growth')
            AND unit_raw IS NOT NULL AND unit_raw <> '%' THEN 1 ELSE 0 END) AS growth_type_with_level_unit,
  SUM(CASE WHEN metric_type IN ('yoy_growth','mom_growth')
            AND abs(value_numeric) > 1000 THEN 1 ELSE 0 END)                AS growth_value_implausible,
  SUM(CASE WHEN unit_raw = '%'
            AND NOT (metric_name_raw RLIKE '(?i)%|phan tram|co cau|chi so|tang|giam|so voi')
           THEN 1 ELSE 0 END)                                               AS pct_unit_unjustified_by_header,
  SUM(CASE WHEN metric_type IN ('share','percent')
            AND (value_numeric < 0 OR value_numeric > 100) THEN 1 ELSE 0 END) AS share_out_of_range
FROM curated_indicators_long;

-- ---------------------------------------------------------------------------
-- P3.1 Calendar model. report_month = 3 returns nothing in any year; months 6
-- and 9 hold 1 report each; report_month = 12 is exclusively annual full-year
-- reports (double-count hazard if read as December).
-- period_end_date + period_type is the only safe temporal filter today.
-- ---------------------------------------------------------------------------
SELECT
  'P3_1_calendar_model' AS check_name,
  SUM(CASE WHEN report_month = 3  THEN 1 ELSE 0 END)                       AS rows_month_3,
  SUM(CASE WHEN report_month IS NULL THEN 1 ELSE 0 END)                    AS rows_null_month,
  SUM(CASE WHEN report_month = 12 AND period_type = 'annual'  THEN 1 ELSE 0 END) AS month12_annual_rows,
  SUM(CASE WHEN report_month = 12 AND period_type = 'monthly' THEN 1 ELSE 0 END) AS month12_monthly_rows
FROM curated_indicators_long;

-- Confirms the 2026-07-25 "tháng năm" backfill still holds: month 5 must not
-- spike relative to months 1/2/4/7/8, and month distribution must be flat.
SELECT report_month, COUNT(DISTINCT report_id) AS reports, COUNT(*) AS rows
FROM curated_indicators_long
GROUP BY report_month
ORDER BY report_month NULLS LAST;

-- ---------------------------------------------------------------------------
-- P3.2 Dimensional enrichment. geography_type is 'unknown' for 100% of the
-- 96,530 rows that carry a geography — the classifier is non-functional.
-- ---------------------------------------------------------------------------
SELECT
  'P3_2_dimensions' AS check_name,
  COUNT(*)                                                                       AS total_rows,
  SUM(CASE WHEN geography_raw IS NULL THEN 1 ELSE 0 END)                         AS null_geography,
  SUM(CASE WHEN sector_raw    IS NULL THEN 1 ELSE 0 END)                         AS null_sector,
  SUM(CASE WHEN product_raw   IS NULL THEN 1 ELSE 0 END)                         AS null_product,
  SUM(CASE WHEN geography_raw IS NOT NULL AND geography_type = 'unknown'
           THEN 1 ELSE 0 END)                                                    AS geography_unclassified,
  COUNT(DISTINCT geography_type)                                                 AS distinct_geography_types
FROM curated_indicators_long;

-- ---------------------------------------------------------------------------
-- P3.3 Curation columns. indicator_name_en is byte-identical to the raw label
-- on 96.2% of rows; unit_en on 90.1%. Diacritics are stripped corpus-wide, so
-- indicator_name_raw is ASCII-folded, not raw (accents survive in
-- parsed_workbooks_raw.raw_cells_json and are recoverable).
-- ---------------------------------------------------------------------------
SELECT
  'P3_3_translation_coverage' AS check_name,
  ROUND(100.0 * SUM(CASE WHEN indicator_name_en = indicator_name_raw THEN 1 ELSE 0 END) / COUNT(*), 1)
    AS indicator_en_untranslated_pct,
  ROUND(100.0 * SUM(CASE WHEN unit_en = unit_raw THEN 1 ELSE 0 END) / COUNT(*), 1)
    AS unit_en_untranslated_pct,
  SUM(CASE WHEN indicator_name_raw RLIKE '[\\u1EA0-\\u1EF9]' THEN 1 ELSE 0 END)
    AS rows_retaining_vietnamese_unicode
FROM curated_indicators_long;

-- ---------------------------------------------------------------------------
-- P3.4 Review signal. extraction_confidence takes 3 values and needs_review is
-- exactly (indicator_domain = 'other_or_unknown') — neither reflects anything
-- about the individual row, so qc_extraction_review_queue misses every defect
-- above. Expect > 3 distinct confidences once per-row evidence is wired in.
-- ---------------------------------------------------------------------------
SELECT extraction_confidence, needs_review, COUNT(*) AS rows,
       COUNT(DISTINCT indicator_domain) AS domains
FROM curated_indicators_long
GROUP BY extraction_confidence, needs_review
ORDER BY rows DESC;

-- ---------------------------------------------------------------------------
-- P4. Residual placeholders, failed downloads, unvalidated legacy encoding.
-- Baseline: 16,734 column_N rows (2.44%); 2 failed downloads; 21,071 rows from
-- attachments whose sheets were flagged possible_legacy_low_confidence.
-- ---------------------------------------------------------------------------
SELECT
  'P4_residuals' AS check_name,
  SUM(CASE WHEN metric_name_raw RLIKE '^column_[0-9]+$' THEN 1 ELSE 0 END) AS placeholder_metric_rows,
  SUM(CASE WHEN attachment_id IN (
        SELECT attachment_id FROM parsed_workbooks_raw
        WHERE detected_text_encoding = 'possible_legacy_low_confidence')
      THEN 1 ELSE 0 END) AS rows_from_unvalidated_legacy_sheets
FROM curated_indicators_long;

SELECT r.report_id, r.report_year, r.period_type, l.attachment_type, left(r.title_raw, 70) AS title
FROM document_processing_log l
JOIN nso_reports_url r ON l.report_id = r.report_id
WHERE l.download_status = 'failed';

-- ---------------------------------------------------------------------------
-- NOTE (2026-07-26): every check in this file is now also a view, created by
-- 4_Curated.py — qc_freshness, qc_business_key_uniqueness, qc_unit_metric_coherence,
-- qc_calendar_model, qc_dimension_coverage, and the qc_dashboard roll-up.
-- Prefer `SELECT * FROM qc_dashboard` for routine monitoring; keep this file for
-- ad-hoc drill-down and as the record of how each figure is derived.
-- ---------------------------------------------------------------------------
SELECT check_name, value, threshold, known_open_issue, status, note
FROM qc_dashboard ORDER BY status DESC, known_open_issue, check_name;
