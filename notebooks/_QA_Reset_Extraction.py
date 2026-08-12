# Databricks notebook source
target = "market_data.nso.document_processing_log"

before = spark.sql(f"""
SELECT COUNT(*) AS n
FROM {target}
WHERE parse_status = 'success'
  AND attachment_id IN (SELECT DISTINCT attachment_id FROM market_data.nso.parsed_workbooks_raw)
""").first()["n"]

spark.sql(f"""
UPDATE {target}
SET extraction_status = 'pending',
    extraction_error_message = NULL,
    updated_at = current_timestamp()
WHERE parse_status = 'success'
  AND attachment_id IN (SELECT DISTINCT attachment_id FROM market_data.nso.parsed_workbooks_raw)
""")

print(f"reset_successfully_parsed_workbooks={before}")
dbutils.notebook.exit(str(before))
