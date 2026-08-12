# NSO data quality remediation plan

Companion to [`DATA_QUALITY_ASSESSMENT.md`](DATA_QUALITY_ASSESSMENT.md) (assessed 2026-07-26).
Every code reference below was read against the live workspace; `scripts/databricks_sync.py diff`
was clean at time of writing (all 8 notebooks identical).

## The 13 findings collapse to 8 root causes

Fixing them in finding order would be wasteful — several findings share a cause, and two of the
biggest share a single line of code.

| Root cause | Where | Findings it resolves |
|---|---|---|
| RC-1 Growth conversion missing for `yoy_growth` | `4_Curated.py:226` | P1 |
| RC-2 Unit column swept into row label; ditto marks unresolved; no parent propagation | `3_Extract_Tables.py:959` `split_row_label` | P2.1, part of P2.3, part of the 140k `%` |
| RC-3 Table-wide `%` fallback | `3_Extract_Tables.py:841` `infer_unit_raw` | P2.3 (140,199 rows) |
| RC-4 `metric_type` keyword match ignores unit/magnitude evidence | `3_Extract_Tables.py:615` `infer_metric_type` | P2.3 (64,812 + 2,964 rows) |
| RC-5 Dim tables are identity mappings, dropped and rebuilt every run | `4_Curated.py:20-22, 68, 115-116, 153` | P3.2, P3.3 |
| RC-6 Step 3 consumes accent-stripped text | `3_Extract_Tables.py:463` | P3.3 fidelity |
| RC-7 `needs_review` / `extraction_confidence` are constants | `3_Extract_Tables.py` + `4_Curated.py:243-249` | P3.4 |
| RC-8 No crawl since 2026-05-03 | operations | P2.2, P4 failed downloads |

## Sequencing constraint that drives everything

`4_Curated.py` does `DROP TABLE curated_indicators_long` and rebuilds it from
`extracted_indicators_long` — **a Step 4 change costs minutes**. A Step 3 change requires a full
re-extraction of ~5,900 sheets — **~50 min plus Step 4**, based on the latest production run timings.

So: ship every Step-4-only fix first (this includes the critical P1), then batch **all** Step 3
changes into a **single** re-extraction. Do not re-extract more than once.

---

## Phase 0 — Restore freshness (independent, do immediately)

> **DONE 2026-07-26 — but it needed a code change after all.** The job had 0 runs between May and
> today (staleness cause: nobody ran it), *and* a latent schema-drift bug blocked the first attempt —
> `DELTA_MERGE_UNRESOLVED_EXPRESSION`, because `document_processing_log` had no `content_hash` column
> and `CREATE TABLE IF NOT EXISTS` never added it. Last successful crawl was 2026-05-17. Fixed by
> reconciling the schema at startup and making downloads incremental — see
> §2.2 of the assessment. Result: freshness **84 → 23 days**, corpus 686,068 → 699,818 rows,
> 314 → 316 reports with data, 0 failed downloads (the 2 known holes recovered), all `qc_*` green,
> `unconverted_growth_rows` still 0 on the newly extracted rows (median 2026 YoY growth +7.0%).

No code change was *expected* here — just run the saved job to pick up the May and June 2026 reports.

```bash
curl -X POST "$DATABRICKS_HOST/api/2.1/jobs/run-now" \
  -H "Authorization: Bearer $DATABRICKS_TOKEN" -d '{"job_id": 116290042743527}'
# poll GET /api/2.1/jobs/runs/get?run_id=<id> until the PARENT run reaches life_cycle_state=TERMINATED
```

Also retry the 2 permanently failed downloads (`27f445bc6722c66b` 2009 annual,
`7c139e6a58786a6b` 2015 April monthly) by resetting `document_processing_log.download_status`
to `'pending'` for those `attachment_id`s before the run. If they fail again, record them as
known holes rather than retrying indefinitely — they are 1 report each out of 315.

**Effort:** ~1 h wall clock, mostly waiting. **Risk:** low. **Validates:** `days_since_publish` < 45.

---

## Phase 1 — P1 growth conversion (Step 4 only, highest value per line changed)

> **DONE 2026-07-26.** Deployed and validated: `unconverted_growth_rows` 237,972 → 0, median
> `value_growth_pct` for `yoy_growth` 105.17 → +5.4, 7,782 rows NULL as predicted, row count and
> lineage unchanged. Spot-checked against source (CPI gold +17.0%, Korea arrivals +17.4%,
> USD index −0.31%). See `docs/DATA_QUALITY_ASSESSMENT.md` for the durable write-up.

### The change

`4_Curated.py:224-228` currently reads:

```sql
CASE
  WHEN e.metric_type = 'index_yoy_base100' AND e.value_numeric IS NOT NULL THEN e.value_numeric - 100.0D
  WHEN e.metric_type = 'yoy_growth'        AND e.value_numeric IS NOT NULL THEN e.value_numeric
  ELSE NULL
END AS value_growth_pct,
```

The `yoy_growth` branch passes the base-100 index through unconverted. Replace with a branch that
converts when the row is index-like and **emits NULL when it cannot tell**:

```sql
CASE
  WHEN e.metric_type = 'index_yoy_base100' AND e.value_numeric IS NOT NULL
    THEN e.value_numeric - 100.0D
  -- Vietnamese NSO "so với cùng kỳ năm trước (%)" columns are base-100 indices.
  -- Convert only where the row is demonstrably index-like; NULL beats a wrong number.
  WHEN e.metric_type IN ('yoy_growth','mom_growth') AND e.value_numeric IS NOT NULL
       AND (e.unit_raw = '%' OR e.value_numeric BETWEEN 20D AND 400D)
    THEN e.value_numeric - 100.0D
  ELSE NULL
END AS value_growth_pct,
```

Emitting NULL where the row is not index-like is deliberate. Those rows are mostly *misclassified*
(Phase 2's RC-4 fix is what actually repairs them, e.g. `Von dang ky (Ty dong)` at 1,120,438 under a
`(%)` header); serving them a fabricated growth rate now would be worse than serving nothing.
Phase 2 shrinks the NULL set.

Measured against live data, this gate converts **243,898 of 251,680** growth rows (96.9%) and leaves
7,782 NULL.

Also update `analytical_unit` (line 229-233) so `yoy_growth` reports
`'Percent change vs same period previous year'` rather than falling through to the raw unit.

### Widen the QC that missed it

`qc_semantic_validation.invalid_index_growth_conversion` (line 434) filters to
`metric_type = 'index_yoy_base100'` before testing, so it only ever validated the correct subset.
Replace with a check over all growth types that asserts the conversion held **and** that no growth
row still carries its raw index:

```sql
SUM(CASE WHEN metric_type IN ('index_yoy_base100','yoy_growth','mom_growth')
          AND value_growth_pct IS NOT NULL
          AND abs(value_growth_pct - (value_numeric - 100.0D)) > 0.000001D
         THEN 1 ELSE 0 END) AS invalid_growth_conversion,
SUM(CASE WHEN metric_type IN ('yoy_growth','mom_growth')
          AND value_growth_pct = value_numeric THEN 1 ELSE 0 END) AS unconverted_growth_rows,
```

### Validate

Run `docs/qc_gap_checks.sql` check `P1_growth_conversion`. Target: `unconverted_growth_rows` 237,972 → 0.

The distribution is the real test, and it was checked against live data before writing this plan:
median `value_growth_pct` moves from **105.17 to +4.65**, with p05 −43.9% and p95 +63.2% — a
plausible spread for Vietnamese monthly YoY series across 2000-2026. Spot-check 20 rows against
their source workbooks before accepting.

**Effort:** ~30 min edit + ~10 min run. **Risk:** low — Step 4 is idempotent and rebuilds from
`extracted_indicators_long`, which is untouched. **Rollback:** Delta time travel on
`curated_indicators_long`; record the pre-change version first.

---

## Phase 2 — Step 3 batch (one re-extraction for all of it)

> **DONE 2026-07-27.** Re-extraction completed (68.5 min extract + 1.1 min curated). Ambiguous
> rows **138,097 → 78,080 (−43%)**, ditto labels **65,978 → 21**,
> `pct_unit_unjustified_by_header` **142,765 → 7,631 (−95%)**, null `sector_raw` −133,766, null
> `product_raw` −94,275. Rows 699,818 → 699,690, the 128 removed all debris. All `qc_*` green,
> lineage exact. Dashboard thresholds retightened to the new baseline so the fix cannot silently
> regress. Remaining: uniform-indent sheets (agriculture 32,281, transport 17,144) and the
> deferred diacritics work (2d).
>
> The history below is retained: the first attempt hit a credits stop, and the second completed
> but produced pre-fix output because the helpers were not wired into the notebook's loop.
>
> **(historical) 2026-07-26: code validated, deployed and pushed; re-extraction BLOCKED on
> Databricks credits.** The run failed 42.5 min in with *"Command failed because warehouse 7eb5fd2336243915
> was stopped. Warehouse was stopped because Workspace is out of Lighthouse credits."* This is a
> billing/capacity stop, not a code fault, and it also blocks SQL verification. Do not simply
> retry — restore credits first.
>
> **State left behind:**
> - `2_Parse_Documents.py` — deployed, reparse **completed successfully**. `indent_level` is
>   captured on 3,840 of 6,048 sheets (xlsx 2,853/3,237; xls 987/2,811, the rest safely falling
>   back to 0).
> - `3_Extract_Tables.py` — deployed, validated offline, but **the re-extraction did not finish**.
> - `document_processing_log.extraction_status` was reset to `'pending'` for all 319 workbook
>   attachments, so a re-run picks them all up automatically.
> - `curated_indicators_long` is **untouched** (Step 4 never ran — `UPSTREAM_CANCELED`) and still
>   serves the Phase 0/1 state: 699,818 rows, growth conversion correct. Nothing downstream is
>   broken by this.
> - `extracted_indicators_long` state is **unverified** — the failure may have landed before,
>   during or after its MERGE, and the warehouse is down so it cannot be checked. **Verify before
>   trusting it.** Rollback point recorded pre-run: **version 104**
>   (`RESTORE TABLE extracted_indicators_long TO VERSION AS OF 104`).
>
> **To resume once credits are restored:**
> 1. `python scripts/dbsql.py --file docs/qc_gap_checks.sql` — confirm the warehouse answers.
> 2. Check the extracted layer reconciles: `SELECT count(*), count(DISTINCT sheet_report_id),
>    max(extracted_timestamp) FROM extracted_indicators_long`. If it holds a partial mix of
>    timestamps, restore to version 104 first.
> 3. Re-run extract + curated (Step 3 then Step 4). Expect ~50 min for extract.
> 4. Validate against the replay predictions below, then `SELECT * FROM qc_dashboard`.
>
> **Replay predicted (186 sheets, 24,612 observations, 0 cells dropped/added/values changed):**
> ambiguous rows −30%, ditto marks in labels → 0, `unit_raw '%'` −45%, null `sector_raw` and
> `product_raw` both materially down, `unit_raw` NULL up (intended — the unjustified percent
> fallback is gone). Corpus-level `rows_in_ambiguous_groups` should fall from 138,097; the
> `qc_dashboard` threshold of 145,000 must not be breached.

All four changes below land together, are replayed offline together, and deploy in one run.

### 2a. RC-2 — labels, ditto marks, hierarchy

Inspecting sheet `03SPCN` in `parsed_workbooks_raw.raw_cells_json` shows the real structure, and it
is not what the extractor assumes:

```
r10 | c1=Than                  | c2=Nghìn tấn | c3=3803.3     <- parent product, real unit
r11 | c1=Doanh nghiệp Nhà nước | c2="         | c3=3660.7     <- child sector, ditto unit
r12 | c1=Trung ương            | c2="         | c3=3650
r16 | c1=Dầu thô khai thác     | c2=Nghìn tấn | c3=1366       <- next parent
r20 | c1=Doanh nghiệp Nhà nước | c2="         | c3=11877.5    <- same child label, different parent
```

Column 2 is the **unit** column, and `"` is the Excel ditto convention meaning "same unit as above".
`split_row_label` (line 959) collects "the first two non-numeric cells" and joins them with `" | "`.
Because `to_number('"')` is None, the ditto is treated as a second label part — producing the
observed `Doanh nghiệp Nhà nước | "` and destroying the distinction between coal, milk, sugar and
beer sub-rows. That is the entire 136,535-row ambiguity.

Three coordinated changes:

1. **Detect the unit column and exclude it from the label.** A column qualifies when a majority of
   its populated data-row cells are either ditto tokens or match `unit_from_text`. Feed it to
   `infer_unit_raw` as a new highest-precedence source instead.
2. **Resolve ditto tokens** (`"`, `''`, `“`, `”`, `nt`, `-nt-`, `~`) against the nearest preceding
   data row in the same column, carrying the resolved value forward.
3. **Propagate the parent label.** A data row whose unit cell held a *real* unit opens a new group;
   subsequent rows with a ditto unit are its children. Emit the parent into the dimension the
   sheet's `dimension_strategy` designates (`product` for `industrial_products`) and the child label
   into `sector_raw`, leaving `indicator_name_raw` as the child. Key becomes
   (`Than`, `Doanh nghiệp Nhà nước`) vs (`Sữa hộp`, `Doanh nghiệp Nhà nước`) — unique.

This also fills `product_raw`/`sector_raw` on exactly the rows that are currently null on both,
which is a direct improvement to P3.2.

### 2b. RC-3 — remove the table-wide `%` fallback

`infer_unit_raw` (line 841) ends `return (context_units or {}).get("title")`. Percent is a
*per-column* property; inheriting it from the sheet title is what stamps `%` on 140,199 rows whose
column header says nothing about percent. Restrict the title fallback so it never supplies `%`
(other title-derived units may stay), and let the unit column from 2a fill the gap instead.
Expect `unit_raw` null to rise — that is correct, and honest.

### 2c. RC-4 — make `metric_type` respect unit and magnitude

`infer_metric_type` (line 615) matches header keywords only, so a level column under a
`"... cùng kỳ năm 2023 (%)"` header is typed `yoy_growth` while carrying `Ty dong` and a value of
1,120,438. After classification, add a reconciliation pass: if the type is a growth type but the
resolved unit is a level unit **and** `abs(value)` exceeds a plausible index bound, demote to
`value` (or `ytd_value` when the header carries a `N tháng` cumulative marker).

### 2d. RC-6 — stop discarding diacritics

Line 463 takes `cell_value_normalized` because of the legacy TCVN3 font problem. That problem was
root-caused and fixed on 2026-07-13, and `cell_value_unicode` in `raw_cells_json` is verifiably
clean (`Nghìn tấn`, `Doanh nghiệp Nhà nước` above). Carry **both**: use `cell_value_unicode` for
display fields (`indicator_name_raw`, `unit_raw`, `geography_raw`, `sector_raw`, `product_raw`) and
keep `cell_value_normalized` for `indicator_name_normalized`, keyword matching, and all MD5 ID
derivation.

**Keeping IDs on normalized text is essential** — it is what stops this change from re-keying all
686,068 rows. Restrict to sheets where `detected_text_encoding <> 'possible_legacy_low_confidence'`
(17 sheets, 21,071 rows) so the unvalidated legacy set keeps today's behaviour.

### Validate before deploying — mandatory

Previous extraction fixes have moved aggregate shares in the wrong direction while improving the
rows that mattered. Use
`scripts/step3_lib.py` to load current and proposed helpers side by side, replay both over real
`raw_cells_json` pulled from Databricks for **≥150 sheets** stratified across all 11 domains
(over-sample `transport_post_telecom`, 89.3% affected, and `industry`, which owns `03SPCN`), and
diff the resulting observations.

**Enumerate every row that changes** — do not accept an aggregate. Specifically confirm:
- ambiguous-key groups fall sharply and no *new* ones appear;
- rows that stop being extracted are header/unit debris, not measurements;
- `unit_raw` null rises only where the `%` fallback was removed;
- no `indicator_observation_id` changes (proves the ID-on-normalized-text guarantee).

Then deploy: `python scripts/databricks_sync.py push`, reset the target attachments'
`extraction_status` to `'pending'`, and run the job.

**Effort:** 1-2 days including replay validation; ~60 min pipeline. **Risk:** medium-high — this is
the ~1,500-line rule engine. **Rollback:** Delta time travel; record `extracted_indicators_long`
and all 11 topic table versions before the run.

**Expected:** ambiguous rows 136,535 → target <15,000; unjustified `%` 140,199 → ~0;
growth-with-level-unit 64,812 → <5,000; `column_N` 16,734 → lower; accents restored corpus-wide.

---

## Phase 3 — Dimensional enrichment (Step 4, after Phase 2)

### The constraint that shapes the design

`4_Curated.py:20-22` **drops** `dim_indicator`, `dim_geography` and `dim_unit` at the start of every
run, so the `MERGE ... WHEN NOT MATCHED THEN INSERT` that follows always inserts into an empty
table. **Any hand-curated mapping written into those tables is destroyed on the next run.** The
MERGE is vestigial and reads as if it preserves, which it does not. Worth a comment in the notebook.

So curated mappings must live in **new seed tables that Step 4 reads but never writes**:

- `ref_geography` — `geography_normalized`, `geography_en`, `geography_type`
  (`country`/`region`/`province`/`city`), `province_code`, `iso3`. Seed the 63 provinces plus the
  8 economic regions and `cả nước`; that covers the bulk of the 251 observed distinct values.
- `ref_unit` — `unit_normalized`, `unit_en`, `scale`, `multiplier_to_base`. Seed the top ~60
  `unit_raw` values, which cover the large majority of rows. Fold the case and language variants
  (`Ty dong` / `Ty Dong` / `Billion VND` → one canonical entry).
- `ref_indicator` — optional, ~200 highest-frequency labels; 5,235 distinct is too many to hand-map,
  so accept partial coverage.

Then change the identity mappings to left joins against these:

- line 116 `CASE WHEN geography_raw IS NULL THEN NULL ELSE 'unknown' END` → `ref_geography.geography_type`,
  defaulting to `'unknown'` only on a genuine miss (P3.2).
- line 115 / 68 / 153 `x_raw AS x_en` → `COALESCE(ref.x_en, x_raw)` (P3.3).

**Handle compound units separately.** `'Nghin tan, trieu USD'` (40,778 rows) encodes two units in one
string and cannot be mapped to a single `multiplier_to_base`; it needs splitting per column in
Phase 2, or explicit exclusion from `value_numeric_base_scale` so it does not silently produce
wrong base-scale numbers.

Rename the `_en` columns only if translation coverage stays below ~50% — a column called
`indicator_name_en` holding Vietnamese is worse than one honestly called
`indicator_name_display`.

**Effort:** ~1 day, mostly compiling reference lists. **Risk:** low.

---

## Phase 4 — Calendar model and QC hardening

### 4a. Make month filtering safe (P3.1)

Today `report_month = 3` returns nothing in any year, and `report_month = 12` is exclusively annual
full-year reports — a 12-month total that reads as "December". Both are traps in a BI layer.

Preferred fix: **stop overloading `report_month`.** Add explicit, unambiguous columns to
`curated_indicators_long` and document `period_end_date` + `period_type` as the only safe temporal
filter:

- `period_month_end` — `month(period_end_date)`, populated for **every** row regardless of period type.
- `is_cumulative` — true for `quarterly`, `semi_annual`, `annual` and cumulative monthlies, derived
  from the existing `period_months_covered`.

Leave `report_month` as-is for backward compatibility. Do **not** back-fill quarterly reports with a
synthetic month — that would create the double-count the current NULL correctly avoids.

Add a `curated_macro_dashboard_monthly` guard so it selects only `period_months_covered = 1`.

### 4b. Give `needs_review` real signal (P3.4)

`extraction_confidence` takes three values and `needs_review` is exactly
`indicator_domain = 'other_or_unknown'`, so `qc_extraction_review_queue` flags **none** of the
136,535 ambiguous rows. Replace the constant in Step 3 with a score that subtracts per-row evidence:

| condition | penalty |
|---|---|
| `metric_name_raw` is `column_N` | −0.30 |
| unit came from the title fallback, not the column | −0.20 |
| business key not unique within the sheet | −0.25 |
| metric_type contradicts the unit | −0.20 |
| domain is `other_or_unknown` | −0.30 |
| sheet encoding `possible_legacy_low_confidence` | −0.15 |

Then `needs_review := confidence < 0.60`, and simplify `4_Curated.py:243-249`, whose extra
disjuncts are all currently dead (0 rows have a null name or value).

### 4c. Promote the gap checks into views

Materialise `docs/qc_gap_checks.sql` as `qc_growth_conversion`, `qc_business_key_uniqueness`,
`qc_unit_metric_coherence` and `qc_freshness` alongside the existing `qc_*` views, and add them to
`_QA_Web_Validation.py`. **A defect on 35% of rows should not have needed a manual audit to find.**

**Effort:** ~1 day. **Risk:** low, additive.

---

## Summary

| Phase | Fixes | Re-extract? | Effort | Risk |
|---|---|---|---|---|
| 0 Crawl | P2.2, P4 downloads | no | ~1 h | low |
| 1 Growth conversion | **P1 (34.7% of rows)** | no | ~40 min | low |
| 2 Step 3 batch | P2.1, P2.3, P3.3 fidelity | **yes, once** | 1-2 days | med-high |
| 3 Dimensions | P3.2, P3.3 | no | ~1 day | low |
| 4 Calendar + QC | P3.1, P3.4, QC gaps | no | ~1 day | low |

Roughly a week of focused work. Phases 0 and 1 are independent of each other and of everything
else — they can ship today and between them clear the critical finding and the staleness. Phase 2
carries essentially all the risk and is the only one requiring re-extraction.

**Do not skip the Phase 2 offline replay.** The earlier units regression moved the wrong way for
the right reason, and only per-row enumeration caught it.
