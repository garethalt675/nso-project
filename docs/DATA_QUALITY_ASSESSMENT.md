# NSO data quality assessment

**Assessed:** 2026-07-26 · **Target:** `market_data.nso` (live, warehouse `7eb5fd2336243915`)
**Corpus:** 686,068 curated observations · 314 attachments · 5,756 sheets · 315 reports · periods 2000-01-01 → 2026-04-30
**Pipeline state at assessment:** last extraction run `2026-07-25T09:42:09Z` (the post-header-fix re-extraction)

## Verdict

The pipeline is **structurally sound but semantically unreliable**. Every one of the five built-in
`qc_*` views reports clean, and the structural guarantees genuinely hold — lineage reconciles exactly,
there are no duplicate keys, no encoding corruption, and no period inconsistencies. But the built-in
QC only checks structure. Once you check *meaning*, three defects affect between 20% and 35% of rows
each, and the largest one is invisible to the existing checks by construction.

**Amended 2026-07-27:** the largest defect turned out not to be in this list at all. Domain
misclassification affected **53.3%** of rows on sheets whose name names a domain (P1b), and was found
by the data owner noticing missing years — not by this assessment, which had accepted a green
`qc_semantic_validation` domain check whose predicate matched only a handful of sheets. Encoding
integrity is scored ✅ below on the *data*, which remains true; the Step 3 *source* was CP1252
mojibake throughout, which is where the `đ`-folding failure behind P1b came from.

**Fitness for use:** safe for row-level lookup and lineage-traced inspection; **not currently safe
for unattended analytics or BI** without the P1 fix and the P2 caveats.

| Dimension | Score | Note |
|---|---|---|
| Completeness (pipeline) | ✅ Strong | 314/314 workbooks parsed + extracted; 2 permanently failed downloads |
| Consistency / lineage | ✅ Strong | extracted = curated = sheet summary = 686,068; 0 orphans |
| Uniqueness (technical) | ✅ Strong | 0 duplicate observation IDs, 0 duplicate cell coordinates |
| Encoding integrity | ✅ Strong | 0 mojibake, 0 replacement chars, 0 legacy-garble suspects |
| **Derived-value correctness** | ✅ Fixed 2026-07-26 | was: `value_growth_pct` wrong on 237,972 rows (34.7%) |
| **Domain classification** | ⚠️ Fixed 2026-07-27 | was: 53.3% of domain-named sheets misfiled; see P1b |
| **Uniqueness (business key)** | ⚠️ Improved 2026-07-27 | 138,097 → 78,080 rows (−43%); residual is uniform-indent sheets |
| **Timeliness** | ✅ Fixed 2026-07-26 | was: 84 days stale — crawl failing silently since 2026-05-17 |
| Unit / metric coherence | ⚠️ High | 64,812 growth-typed rows carry level units |
| Dimensional enrichment | ⚠️ Medium | geography 86% null; `geography_type` 100% `unknown` |
| Curation (`_en` columns) | ⚠️ Medium | 96.2% untranslated — column promises what it doesn't deliver |

---

## P1 — Critical: `value_growth_pct` is off by +100 pp on 34.7% of the corpus

> **RESOLVED 2026-07-26.** Fixed in `4_Curated.py` and deployed; `unconverted_growth_rows`
> 237,972 → **0**, median `value_growth_pct` for `yoy_growth` **105.17 → +5.4** (p05 −46.3,
> p95 +65.8), 7,782 rows now NULL rather than wrong. `qc_semantic_validation` widened to every
> growth type. Row count, lineage and all other QC unchanged. The analysis below is retained as
> the record of what was wrong and how it was found.

**All 237,972 `metric_type='yoy_growth'` rows have `value_growth_pct` set to a verbatim copy of
`value_numeric`, with no conversion applied.**

```
metric_type          rows      growth_pct == value   growth_pct == value-100   p50(value)
yoy_growth         237,972            237,972 (100%)                       0       105.17
index_yoy_base100   28,405                      0            28,405 (100%)       108.31
```

`index_yoy_base100` demonstrates the intended contract — subtract 100 from a base-100 index to get a
growth rate — and applies it to every row. `yoy_growth` never does. And the underlying values *are*
base-100 indices: **89.4% (212,798 rows) fall in the 50–200 band**, while only 6.6% look like true
growth rates.

The practical effect: a Vietnamese source column reading `"so với cùng kỳ năm trước = 105.2"`
(+5.2% YoY) is served through `value_growth_pct` as **105.2**, which any consumer reads as **+105%
growth**. Every YoY series in the warehouse is inflated by roughly 100 percentage points.

**Why QC missed it:** `qc_semantic_validation.invalid_index_growth_conversion` filters on
`metric_type = 'index_yoy_base100'` before testing the conversion, so it validates only the subset
that is already correct and returns 0. The check needs to cover `yoy_growth` — that single predicate
change would have surfaced this.

**Fix:** in `4_Curated.py`, apply the same `value_numeric - 100` derivation to `yoy_growth` rows whose
values are index-like, and extend the QC predicate to all growth metric types. Note the ~6.6% of
`yoy_growth` rows already in true-growth form must not be double-converted — gate on the value band
or on the source column header, not on `metric_type` alone.

---

## P1b — Critical: 53.3% of domain-named sheets were filed under the wrong domain

**Found 2026-07-27, by the data owner, not by this assessment or by any `qc_*` check.**
The reporting symptom was that `trade_prices` appeared to be missing whole years — 2021 and 2022
returned nothing at all, and only 15 of 27 years had any rows. No data was missing: it was filed
under other domains.

Measured over all 944 distinct sheet names, judged against an expectation derived from NSO naming
conventions independently of the extractor's own rules, and weighted by real row counts:

| expected domain | before | after scoring fix | after keyword fix |
|---|---|---|---|
| population_labor_social | 100.0% | 0.0% | 0.0% |
| trade_prices | 91.1% | 1.2% | 1.2% |
| enterprise_business_registration | 81.9% | 0.0% | 0.0% |
| transport_post_telecom | 51.6% | 48.0% | **3.3%** |
| environment_safety_disaster | 43.2% | 0.0% | 0.0% |
| industry | 34.4% | 0.6% | 0.6% |
| investment_construction | 21.6% | 0.8% | 0.8% |
| agriculture_forestry_fishery | 15.7% | 15.4% | **1.3%** |
| retail_services_tourism | 9.8% | 0.1% | 0.1% |
| national_accounts | 0.5% | 0.5% | 0.5% |
| **overall** | **53.3%** (245,173 rows) | **4.3%** (19,968) | **0.7%** (3,324) |

`trade_prices` held 14,353 rows across 15 years; 135,552 rows belonging to it sat in agriculture
(71,374), investment (43,297) and retail (20,881). Post-fix it holds 174,512 rows across all 27 years.

**Final state after both re-extractions** (runs `354651779804609` and `56697615098198`). Row count
unchanged at 699,690 with all IDs unique — this was pure reclassification, nothing created or lost —
and lineage reconciles exactly (extracted = curated = sheet summary = 699,690, 0 difference).
**9 of 11 domains now span all 27 years.**

| domain | rows before → after | years |
|---|---|---|
| industry | 140,566 → 195,272 | 27 |
| trade_prices | 14,353 → 174,512 | 27 |
| investment_construction | 130,907 → 89,151 | 27 |
| retail_services_tourism | 82,380 → 63,437 | 27 |
| enterprise_business_registration | 9,156 → 43,625 | 12 (2010–, genuinely sparse) |
| agriculture_forestry_fishery | 219,671 → 38,885 | 27 |
| transport_post_telecom | 19,224 → 37,279 | 27 |
| national_accounts | 47,381 → 20,409 | 27 |
| population_labor_social | 3,522 → 17,217 | 26 |
| environment_safety_disaster | 13,532 → 13,011 | 22 |
| other_or_unknown | 18,998 → 6,892 | 17 |

Agriculture and investment were the sinks: agriculture shed 180,786 rows and investment 41,756,
which is close to what trade_prices and transport gained. `other_or_unknown` fell 64%.

**The residual 3,324 rows are mostly not errors.** Reviewed individually: ~1,000 are `Gia Van tai`,
a transport *price index* correctly filed under `trade_prices`/`producer_price_index` — the audit
expectation is wrong there, not the data; ~1,500 are `XNK Dich vu` (import/export of services),
genuinely arguable between trade and services. One real miss remained, `XD` (*xây dựng*) at 184 rows,
where only the spelled-out `xay dung` was a keyword; `xd` has been added and applies on the next
extraction. True remaining error is roughly 200 rows out of 459,935 judged.

**Three independent root causes**, all in `3_Extract_Tables.py`:

1. **First-match-wins over concatenated text.** `classify_sheet` built one string from sheet name +
   12 body rows + title and took the first matching `SHEET_RULES` entry. Vietnamese trade tables
   carry a standard `khu vực có vốn đầu tư nước ngoài` (FDI) breakdown row, which matches
   `investment_construction`'s `von dau tu` — listed *before* `trade_prices`. A sheet named
   `xuat khau thang`, titled *"28. Hàng hóa xuất khẩu"*, was filed as investment. Fixed by scoring
   on match location (name 3 / title 2 / body 1), ties falling back to rule order.

2. **Keyword coverage gap.** Transport listed only `van tai hk`/`van tai hh`/`vt hk`/`vt hh` — all
   spaced. The real names are `Vantai`, `VT`, `VTHH`, `06VT`, matching nothing, so scoring could not
   help: with no keyword hit the sheet fell through to body text and landed in agriculture. Industry
   had no plain `cong nghiep`, national accounts no bare `gdp`, labor no glued `Danso`.

3. **`strip_accents` never folded `đ`.** The replacement targeted `"Ä\x90"`/`"Ä‘"` — the UTF-8 bytes
   of `Đ`/`đ` decoded as CP1252 — so it never matched a real one, and `Vốn đầu tư` normalized with
   the stroke intact, missing the `von dau tu` keyword. 20,980 rows carry a `đ` in the sheet name.
   This traced to the whole notebook being CP1252 mojibake (74 runs), present since the initial
   workspace import. It never reached the data: the corrupted canonical unit list is *shadowed* by a
   second, English `UNIT_PHRASE_PATTERNS` definition later in the file — an earlier author worked
   around the corruption instead of fixing it.

**Why no check caught it.** `qc_semantic_validation`'s export/import domain checks only match sheet
names beginning `14 XK` / `15 NK`, a handful of recent sheets, and returned 0. The first version of
`qc_domain_classification` — written *for* this bug — used the transport pattern
`'van tai|vt hh|vt hk'`, matching none of the spellings that were wrong, and so scored transport
clean at 48% error. **That is the third occurrence of one meta-pattern: a QC predicate that excludes
the population it exists to protect.** See also P1 (filtered to `index_yoy_base100`, the subset
already correct).

**Guard added:** `check_sheet_rules()` in `3_Extract_Tables.py` classifies 33 real sheet names using
the sheet name *alone* — no title or body to fall back on, which is the exact condition under which
the transport rules failed — and rejects keywords under two characters. It runs at notebook start,
so a coverage gap fails in seconds rather than after a 50-minute re-extraction. It caught `Danso`
on its first run.

---

## P2 — High severity

### 2.1 One row in five is not uniquely identifiable

> **LARGELY RESOLVED 2026-07-27 (Phase 2).** Ambiguous rows **138,097 → 78,080 (−43%)**; ditto
> marks in labels **65,978 → 21**; composed `parent | child` labels 113,068 → 262,167; null
> `sector_raw` −133,766 and null `product_raw` −94,275. Fixed by capturing Excel `indent_level`
> in Step 2 (the actual structural signal) and resolving ditto marks / propagating parent groups
> in Step 3. Residual concentrates in sheets whose indentation is uniform or absent — see below.

136,535 rows (19.90%), spanning 1,620 of 5,756 sheets (28%), fall into 31,167 groups that share
`(sheet_report_id, indicator_name_raw, metric_name_raw)` **and hold conflicting values**. Adding
`geography_raw`, `sector_raw` and `product_raw` to the key changes nothing — those dimensions are
null on exactly these rows. Only `source_row_index` separates them, so the rows cannot be joined,
pivoted, or aggregated on business meaning.

Root cause is visible in the data — sheet `03SPCN`, one column, 32 distinct values under one key:

```
indicator_name_raw        metric_name_raw                      n   src_col
- Kinh te Nha nuoc | "    Thuc hien / 7 thang Dau / nam 2003   32        3
```

Two label-reconstruction failures compound:
- **Ditto marks are taken literally.** 65,554 rows carry `"` in the label — the Excel "same as above"
  convention — joined in as text instead of resolved against the row above.
- **Hierarchical parents are not propagated.** `- Kinh te Nha nuoc` ("state-owned sector") is an
  indented child repeated under 32 different product headings; the parent product is never carried
  down, so all 32 collapse onto one key.

Concentration is severe in the smaller domains:

| domain | rows affected | % of domain |
|---|---|---|
| transport_post_telecom | 16,472 | **89.3%** |
| enterprise_business_registration | 3,992 | 45.4% |
| population_labor_social | 1,428 | 40.6% |
| national_accounts | 14,750 | 31.5% |
| industry | 41,243 | 29.8% |
| agriculture_forestry_fishery | 48,219 | 22.4% |

`transport_post_telecom` should be treated as unusable for aggregation until labels are repaired.

### 2.2 Data is 84 days stale

> **RESOLVED 2026-07-26.** Freshness **84 → 23 days**; latest period `2026-04-30` → `2026-06-30`.
> Both missing reports ingested; corpus 686,068 → 699,818 rows, 5,756 → 5,881 sheets. The two
> permanently-failed downloads (P4) also recovered — 0 failed attachments remain.

Latest published report `2026-05-03`; latest period covered `2026-04-30`. Against today (2026-07-26)
that is **two missing monthly cycles** — the May and June 2026 reports, normally published in early
June and early July.

**Two independent causes, established from the job run history.** The `NSO Monthly Pipeline
(manual)` job had **0 runs between 2026-05-01 and 2026-07-26** (run history reaches back well past
May — 25 runs of other jobs in May, 25 in June — so this is genuine absence, not retention loss).
The last successful write to `document_processing_log` was **2026-05-17**, from the ad-hoc
`Sprint00x` jobs visible in that history.

1. **The pipeline simply was not run for ~70 days.** That is the staleness cause. The job is
   manual-trigger with no schedule, so nothing was going to run it.
2. **A latent schema-drift bug would have blocked any run that was attempted** — and did block the
   first one, on 2026-07-26:

```
[DELTA_MERGE_UNRESOLVED_EXPRESSION] Cannot resolve content_hash in UPDATE clause
```

`document_processing_log` was created without a `content_hash` column. The column exists in the
notebook DDL, but `CREATE TABLE IF NOT EXISTS` never alters an existing table, so the live table
never gained it — while the MERGE reads `target.content_hash` to decide whether a re-downloaded file
actually changed. The failure lands at the *end* of the crawl, so a run downloads all ~950
attachments and only then dies.

(An earlier revision of this document asserted the crawl "had been run and was failing every time".
That was not supported: the run history shows no attempts at all in that window.)

Two things follow, and both are now fixed in Step 1:

- The schema is reconciled explicitly at startup (`ALTER TABLE … ADD COLUMNS`) with `content_hash`
  backfilled from `nso_report_attachments`, so the first post-fix run does not treat all 953
  attachments as content-changed and reset every parse/extraction status.
- Downloads are incremental. There was previously **no skip logic at all** — `MAX_PAGES = 80` walks
  the whole archive and every discovered attachment was fetched unconditionally. Attachments already
  recorded as successfully downloaded, whose file is still present in the volume, are now reused.
  `FORCE_REDOWNLOAD` (default `False`) forces a full refresh; the incremental path deliberately will
  not notice an upstream revision of an already-downloaded file.

Measured on the first post-fix run: **636 of 960 attachments reused, 324 fetched** — of which only
**7 were genuinely new files** (the May/June attachments). The other 317 are the `inline_html` report
pages, which by design carry `local_path = NULL` and so fall outside the reuse condition. Crawl step
**~40 min → 14.6 min**; making the HTML page fetches incremental too is the obvious next saving.

Two lessons worth keeping:

- **A manual-trigger job with no schedule and no freshness alarm will go stale and nothing will say
  so.** `on_failure` email is configured, but a job that is never run never fails, so it never
  alerts. Staleness needs its own check (`qc_freshness`), independent of run outcome.
- **A latent DDL/table mismatch can sit undetected indefinitely**, because the notebook and the live
  table disagree only at the moment the MERGE runs. Any column added to a `CREATE TABLE IF NOT
  EXISTS` block needs an explicit `ALTER TABLE` reconciliation beside it.

### 2.3 Unit and metric type contradict each other

- **64,812 rows** are typed `yoy_growth`/`mom_growth` but carry a *level* unit (`Ty dong`, `Nghin tan`,
  `VND`, `Nguoi`, …) rather than `%`.
- **2,757 `yoy_growth` rows exceed 1000**, topping out at **1,120,438**. These are header-to-column
  misattribution: `Von dang ky (Ty dong)` under header `"9 thang / nam / cung ky nam / 2023 (%)"` with
  unit `Ty dong` and value 1,120,438 — a level in billion VND filed under a percent header.
- **140,199 rows (20.4%)** carry `unit_raw='%'` taken from the sheet-level title fallback in
  `infer_unit_raw` with nothing in the column header justifying it. This known gap has not moved.
  Percent is a per-column property and the table-wide fallback should be removed rather than tuned.
- Minor: 1,365 `percent` rows fall outside 0–100 (max 3,152.6); 43 `share` rows exceed 100.

---

## P3 — Medium severity

### 3.1 The calendar model silently misleads month-based queries

| symptom | detail |
|---|---|
| `report_month = 3` | **no rows at all**, any year |
| `report_month = 6` / `9` | 1 report each across 27 years |
| `report_month IS NULL` | 220,591 rows (32.2%) — all quarterly + semi-annual reports |
| `report_month = 12` | 26 reports, **all `period_type='annual'`** — zero December-monthly |

Q1/Q2/Q3 reports are typed `quarterly` and correctly carry `report_month = NULL` (this is the
intended post-fix behaviour), but the consequence is that a March filter returns nothing even though
March data exists inside the Q1 report. Worse, `report_month = 12` selects full-year cumulative
figures exclusively — a consumer reading it as "December" gets a 12-month total, a double-count
waiting to happen.

Recommend either deriving `report_month` for quarterly reports from `period_end_date`, or documenting
that `period_end_date` + `period_type` is the only safe temporal filter. `period_months_covered`
already carries the information needed.

*The 2026-07-25 `tháng năm` backfill is confirmed holding:* month 5 has 25 reports, in line with
months 1/2/4/7/8 (26–27 each) — no residual spike — and 0 rows disagree with `nso_reports_url`.

### 3.2 Dimensional enrichment is largely absent

| field | null rows | % |
|---|---|---|
| `geography_raw` | 589,538 | 85.9% |
| `sector_raw` | 439,156 | 64.0% |
| `product_raw` | 387,563 | 56.5% |
| `currency` | 481,722 | 70.2% |

And of the 96,530 rows that *do* carry a geography, **`geography_type` is `'unknown'` for 100% of
them** across all 251 distinct values — the geography classifier is entirely non-functional, not
merely sparse. (251 distinct values against Vietnam's 63 provinces also suggests the extracted
geography strings are unnormalised noise.)

### 3.3 The `_en` columns are not translated

`indicator_name_en` is byte-identical to `indicator_name_raw` on **96.2%** of rows; `unit_en` on
**90.1%**. The curated layer advertises an English surface that is, in practice, Vietnamese.

Related fidelity issue: **0 rows contain Vietnamese Unicode** (U+1EA0–U+1EF9) and only 380 contain
Latin-1 accents. The corpus is ASCII-folded end to end, so `indicator_name_raw` is *not* raw —
`Kinh tế Nhà nước` is stored as `Kinh te Nha nuoc`. This is lossy for display and blocks joins
against any properly-accented Vietnamese reference data. The accented text still exists in
`parsed_workbooks_raw.raw_cells_json`, so this is recoverable.

### 3.4 `needs_review` and `extraction_confidence` carry no independent signal

`extraction_confidence` takes exactly three values, and `needs_review` is precisely the predicate
`indicator_domain = 'other_or_unknown'`:

```
confidence   needs_review      rows   domains
0.9                 false   646,872        10
1.0                 false    20,578         3
0.35                 true    18,618         1
```

Neither field reflects anything about the row itself — not label quality, not unit inference, not
header confidence. `qc_extraction_review_queue` (built on `needs_review`) therefore surfaces
domain-classification failures only, and misses all three P1/P2 defects above, none of which are
flagged on a single row.

---

## P4 — Low / accepted

- **2 permanently failed downloads:** report `27f445bc6722c66b` (2009 annual) and `7c139e6a58786a6b`
  (2015 April monthly). Both are irrecoverable data holes worth one retry attempt.
- **16,734 rows (2.44%)** retain `column_N` placeholder metric names — down from 54% pre-fix, as
  documented. Residual concentration: `population_labor_social` 23.9%, `enterprise_business_registration`
  18.4%, `agriculture_forestry_fishery` 4.9%.
- **21,071 rows across 203 sheets** originate from attachments whose encoding was classified
  `possible_legacy_low_confidence` (17 sheets) — deliberately not converted, and never validated
  either. Worth a spot check.
- **Unit vocabulary is fragmented:** 270 distinct `unit_raw` values with case and language variants
  coexisting (`Ty dong` 25,919 / `Ty Dong` 23,682 / `Billion VND` 15,873; `Trieu USD` / `Million USD`
  / `Nghin USD`). Compound units are unusable as-is — `'Nghin tan, trieu USD'` (thousand tonnes *and*
  million USD) is stamped on 40,778 rows.
- 981 zero values and 4,744 negative values in `value`-typed rows — plausible, not investigated further.

---

## What is verifiably clean

Worth recording, because these were previously-fixed defect classes and they have held:

- All five `qc_*` views green: 0 duplicate observation IDs, 0 rows missing sheet lineage, 0 rows
  missing indicator name or numeric value, 0 replacement characters, 0 wrong-domain export/import
  rows, 0 IIP metric-type errors, 0 mojibake units.
- **Lineage is exact:** `extracted_indicators_long` = `curated_indicators_long` =
  `curated_report_sheet_summary` = 686,068, with 0 null `sheet_report_id` on either side.
- **0 rows** disagree with `nso_reports_url` on year, month, period end date or period type.
- **TCVN3 remediation holding:** 1,553 sheets converted, 4,352 unicode, 0 garble suspects in output.
- **Processing complete:** 314/314 workbook attachments `success` through parse and extract;
  5,756 sheets extracted, 166 empty, 0 failed.

---

## Recommended order of work

1. **Fix `value_growth_pct` for `yoy_growth`** and widen the `qc_semantic_validation` predicate to all
   growth metric types (P1 — largest correctness impact, smallest change).
2. **Run the crawl** to pick up the May and June 2026 reports (P2.2 — pure operations).
3. **Resolve ditto marks and propagate hierarchical parent labels** in `3_Extract_Tables.py`, then
   re-extract; add a QC check for non-unique business keys (P2.1).
4. **Drop the table-level `%` fallback** in `infer_unit_raw` and add a units-vs-metric_type
   contradiction check (P2.3).
5. Populate `geography_type`, or drop the column rather than ship a constant (P3.2).
6. Either make `_en` columns real translations or rename them to reflect what they hold (P3.3).
7. Make `extraction_confidence` reflect per-row evidence — placeholder metric name, fallback unit,
   ambiguous key — so `needs_review` becomes a usable triage queue (P3.4).

Reproducible SQL for every figure above is in [`qc_gap_checks.sql`](qc_gap_checks.sql).
