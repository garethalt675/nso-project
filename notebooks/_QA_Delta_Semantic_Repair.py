# Databricks notebook source
# Deterministic in-place repair for already extracted NSO rows. The production
# Step 3 notebook contains the same rules for all future processing.

LONG = "market_data.nso.extracted_indicators_long"
INVENTORY = "market_data.nso.extracted_table_inventory"
SHEETS = "market_data.nso.sheet_reports"
LOG = "market_data.nso.document_processing_log"
TRADE = "market_data.nso.trade_prices_report"
INVESTMENT = "market_data.nso.investment_construction_report"

spark.sql(f"""
UPDATE {LONG}
SET indicator_domain = 'trade_prices',
    indicator_subdomain = CASE
      WHEN lower(trim(source_sheet_name)) RLIKE '^14[. _-]*xk' THEN 'exports'
      ELSE 'imports'
    END,
    extraction_method = CASE
      WHEN lower(trim(source_sheet_name)) RLIKE '^14[. _-]*xk' THEN 'sheet_name_override:exports'
      ELSE 'sheet_name_override:imports'
    END,
    extraction_confidence = 1.0D,
    needs_review = false,
    unit_raw = CASE
      WHEN lower(metric_name_raw) LIKE '%so voi%' OR lower(metric_name_raw) LIKE '%cung ky%' THEN 'Index (same period previous year=100)'
      WHEN lower(metric_name_raw) LIKE '%tri gia%' THEN 'Million USD'
      ELSE unit_raw
    END,
    metric_type = CASE
      WHEN lower(metric_name_raw) LIKE '%so voi%' OR lower(metric_name_raw) LIKE '%cung ky%' THEN 'index_yoy_base100'
      ELSE metric_type
    END,
    scale = CASE
      WHEN lower(metric_name_raw) LIKE '%so voi%' OR lower(metric_name_raw) LIKE '%cung ky%' THEN 'index'
      WHEN lower(metric_name_raw) LIKE '%tri gia%' THEN 'million'
      ELSE scale
    END,
    currency = CASE WHEN lower(metric_name_raw) LIKE '%tri gia%' THEN 'USD' ELSE currency END
WHERE lower(trim(source_sheet_name)) RLIKE '^(14[. _-]*xk|15[. _-]*nk)'
""")

spark.sql(f"""
UPDATE {LONG}
SET indicator_domain = 'industry',
    indicator_subdomain = 'industrial_production_index',
    extraction_method = 'sheet_name_override:industrial_production_index',
    extraction_confidence = 1.0D,
    needs_review = false,
    unit_raw = 'Index (comparison period=100)',
    metric_type = CASE
      WHEN lower(metric_name_raw) LIKE '%cung ky%' OR lower(metric_name_raw) LIKE '%nam truoc%' THEN 'index_yoy_base100'
      ELSE 'index'
    END,
    scale = 'index',
    currency = NULL
WHERE lower(trim(source_sheet_name)) RLIKE '^2[. _-]*iip'
""")

for table_name in [INVENTORY, SHEETS]:
    spark.sql(f"""
    UPDATE {table_name}
    SET indicator_domain = 'trade_prices',
        indicator_subdomain = CASE
          WHEN lower(trim(sheet_name_raw)) RLIKE '^14[. _-]*xk' THEN 'exports'
          ELSE 'imports'
        END
    WHERE lower(trim(sheet_name_raw)) RLIKE '^(14[. _-]*xk|15[. _-]*nk)'
    """)
    spark.sql(f"""
    UPDATE {table_name}
    SET indicator_domain = 'industry', indicator_subdomain = 'industrial_production_index'
    WHERE lower(trim(sheet_name_raw)) RLIKE '^2[. _-]*iip'
    """)

# Move any legacy trade-sheet rows out of the investment topic table.
spark.sql(f"""
MERGE INTO {TRADE} AS target
USING (
  SELECT * FROM {INVESTMENT}
  WHERE lower(trim(source_sheet_name)) RLIKE '^(14[. _-]*xk|15[. _-]*nk)'
) AS source
ON target.observation_id = source.observation_id
WHEN NOT MATCHED THEN INSERT *
""")
spark.sql(f"DELETE FROM {INVESTMENT} WHERE lower(trim(source_sheet_name)) RLIKE '^(14[. _-]*xk|15[. _-]*nk)'")

# Restore processing-log consistency after the cancelled full historical run.
spark.sql(f"""
MERGE INTO {LOG} AS target
USING (
  SELECT attachment_id, COUNT(*) AS extracted_rows
  FROM {LONG}
  GROUP BY attachment_id
) AS source
ON target.attachment_id = source.attachment_id
WHEN MATCHED THEN UPDATE SET
  target.extraction_status = 'success',
  target.extraction_error_message = NULL,
  target.extraction_rows_inserted = source.extracted_rows,
  target.updated_at = current_timestamp()
""")

summary = spark.sql(f"""
SELECT
  SUM(CASE WHEN lower(trim(source_sheet_name)) RLIKE '^14[. _-]*xk' AND indicator_domain <> 'trade_prices' THEN 1 ELSE 0 END) export_wrong_domain,
  SUM(CASE WHEN lower(trim(source_sheet_name)) RLIKE '^15[. _-]*nk' AND indicator_domain <> 'trade_prices' THEN 1 ELSE 0 END) import_wrong_domain,
  SUM(CASE WHEN lower(trim(source_sheet_name)) RLIKE '^2[. _-]*iip' AND metric_type NOT IN ('index','index_yoy_base100') THEN 1 ELSE 0 END) iip_wrong_metric_type
FROM {LONG}
""").first().asDict()

print(summary)
dbutils.notebook.exit(str(summary))
