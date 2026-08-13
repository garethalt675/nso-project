# NSO ETL

Databricks ETL pipeline for Vietnam National Statistics Office (GSO/NSO)
monthly socio-economic reports from [nso.gov.vn](https://www.nso.gov.vn).

The pipeline crawls the public archive, downloads report attachments, parses
Excel workbooks across modern and legacy formats, extracts statistical tables,
and publishes a curated long-form dataset in Unity Catalog under
`market_data.nso`.

The main output is `market_data.nso.curated_indicators_long`, with roughly
690,000 observations sourced from workbook cells and linked back to the source
report, attachment, sheet, row, and column.

## Repository Layout

```text
notebooks/   Databricks notebooks for the pipeline and QA helpers
scripts/     Local tooling for workspace sync, job export, SQL checks, and tests
jobs/        Databricks workflow definitions
docs/        Architecture notes, quality assessment, remediation plan, and runbooks
```

## Pipeline

| # | Notebook | Purpose |
|---|----------|---------|
| 0 | `00_specs_nso_socioeconomic_reports` | Target schemas, taxonomy, IDs, and idempotency rules |
| 1 | `1_Crawl_Download_Documents` | Crawl the NSO archive, classify report periods, and download attachments |
| 2 | `2_Parse_Documents` | Parse Excel workbooks into sheet-level cell JSON |
| 3 | `3_Extract_Tables` | Extract table observations using deterministic parsing rules |
| 4 | `4_Curated` | Build dimensions, curated indicators, and QC views |

All stages use deterministic MD5-based IDs and Delta `MERGE` patterns so reruns
are idempotent. Processing status is tracked in `document_processing_log` and
`sheet_processing_log`.

## Data Quality

The corpus has several failure modes that do not show up as schema errors:
misclassified sheets, plausible values under the wrong metric, legacy encoding
issues, period parsing mistakes, and ambiguous row labels. The repository keeps
the relevant audits and open findings in version control:

- [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) describes the pipeline design.
- [`docs/DATA_QUALITY_ASSESSMENT.md`](docs/DATA_QUALITY_ASSESSMENT.md) summarizes the current quality state.
- [`docs/REMEDIATION_PLAN.md`](docs/REMEDIATION_PLAN.md) tracks known gaps and next fixes.
- [`docs/TEST_RESULTS.md`](docs/TEST_RESULTS.md) records the per-domain regression suite results.
- [`docs/qc_gap_checks.sql`](docs/qc_gap_checks.sql) contains additional SQL quality checks.

## Setup

Install local tooling:

```bash
pip install -r scripts/requirements.txt
```

Databricks authentication is read from either:

- `DATABRICKS_HOST` and `DATABRICKS_TOKEN`
- the `DEFAULT` profile in `~/.databrickscfg`

See `.env.example` for the expected environment variables. Credentials are not
committed to the repository.

## Working With Databricks

The Databricks workspace copy is the version that runs. Use the sync script to
compare and move notebook changes between the workspace and this repository.

```bash
python scripts/databricks_sync.py diff
python scripts/databricks_sync.py diff -v
python scripts/databricks_sync.py pull
python scripts/databricks_sync.py push
```

Both `pull` and `push` accept `--dry-run`. Pushing to GitHub does not deploy to
Databricks; use `scripts/databricks_sync.py push` for that.

Refresh workflow definitions with:

```bash
python scripts/export_jobs.py
```

The production workflow is `NSO Monthly Pipeline (manual)`, stored in
`jobs/nso-monthly-pipeline-manual.json`.

## Local Checks

Run the linter and Python syntax checks:

```bash
ruff check .
python -m compileall -q scripts notebooks
```

Both run in CI on every push and pull request (`.github/workflows/lint.yml`). Lint
configuration is in `pyproject.toml`. Do not run `ruff format` or `black`: notebooks
are uploaded to Databricks byte-for-byte, so a reformat is a full workspace re-push.

Run the curated-layer regression suite against Databricks:

```bash
python scripts/nso_test_suite.py
python scripts/nso_test_suite.py --domain industry
python scripts/nso_test_suite.py --failures-only
```

Run SQL checks through the lightweight REST client:

```bash
python scripts/dbsql.py "SELECT count(*) FROM market_data.nso.curated_indicators_long"
python scripts/dbsql.py --file docs/qc_gap_checks.sql
```

For Step 3 extraction changes, `scripts/step3_lib.py` can load the non-Spark
helper functions from the notebook for local replay tests:

```bash
python scripts/step3_lib.py
```
