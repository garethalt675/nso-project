# NSO curated layer — per-domain test results

**747 checks across 11 domains — 729 pass, 8 fail, 10 warn**

| domain | checks | pass | fail | warn |
|---|---|---|---|---|
| agriculture_forestry_fishery | 68 | 67 | 1 | 0 |
| industry | 68 | 66 | 2 | 0 |
| trade_prices | 68 | 66 | 2 | 0 |
| investment_construction | 68 | 68 | 0 | 0 |
| retail_services_tourism | 68 | 66 | 2 | 0 |
| national_accounts | 68 | 68 | 0 | 0 |
| transport_post_telecom | 68 | 67 | 1 | 0 |
| enterprise_business_registration | 68 | 66 | 0 | 2 |
| population_labor_social | 68 | 66 | 0 | 2 |
| environment_safety_disaster | 68 | 65 | 0 | 3 |
| other_or_unknown | 67 | 64 | 0 | 3 |

## Failures (8)

| domain | check | value | expected | note |
|---|---|---|---|---|
| agriculture_forestry_fishery | `growth_band` | 2.0 | == 0 |  |
| industry | `index_value_band` | 714.0 | == 0 |  |
| industry | `growth_band` | 4.0 | == 0 |  |
| trade_prices | `index_value_band` | 628.0 | == 0 |  |
| trade_prices | `growth_band` | 80.0 | == 0 |  |
| retail_services_tourism | `pct_unit_sanity` | 7.0 | == 0 |  |
| retail_services_tourism | `growth_band` | 190.0 | == 0 |  |
| transport_post_telecom | `growth_band` | 3.0 | == 0 |  |

## Warnings (10)

| domain | check | value | expected | note |
|---|---|---|---|---|
| enterprise_business_registration | `year_gaps` | 5.0 | == 0 | Informational: a genuine publication gap looks identical here |
| enterprise_business_registration | `any_dimension_coverage` | 0.0 | > 0 |  |
| population_labor_social | `year_gaps` | 1.0 | == 0 | Informational: a genuine publication gap looks identical here |
| population_labor_social | `any_dimension_coverage` | 0.0 | > 0 |  |
| environment_safety_disaster | `year_gaps` | 5.0 | == 0 | Informational: a genuine publication gap looks identical here |
| environment_safety_disaster | `grain_null_pct` | 84.05964184151871 | <= 60.0 | NULL is correct where the column header names no period at all |
| environment_safety_disaster | `any_dimension_coverage` | 0.0 | > 0 |  |
| other_or_unknown | `year_gaps` | 10.0 | == 0 | Informational: a genuine publication gap looks identical here |
| other_or_unknown | `grain_null_pct` | 73.21109123434705 | <= 60.0 | NULL is correct where the column header names no period at all |
| other_or_unknown | `any_dimension_coverage` | 0.0 | > 0 |  |

---

## What the failures mean

All 8 failures are three related defects, together affecting ~1,630 rows (0.23% of 699,690).
They share one cause: `metric_type` / `unit_raw` assigned upstream in Step 3 do not match the
value, and the derived-value logic in Step 4 trusts them.

**1. `index_value_band` — 1,342 rows (industry 714, trade_prices 628)**
Industry: values up to 1,656,940 carrying `Index (comparison period=100)`. The metric name is
`Cong don / 6 thang dau / nam 2011` — a cumulative *level*, not an index.
`normalize_observation_semantics` stamps `index` on every column of an IIP sheet, including the
level columns beside the index ones. Trade: `index_yoy_base100` values from -18,400 to 0; a
negative base-100 index is not possible.

**2. `growth_band` — 279 rows (retail 190, trade 80, industry 4, transport 3, agriculture 2)**
Level values typed as `yoy_growth`. Example: `value_numeric = 431,500` with `unit_raw = '%'`
becomes `value_growth_pct = 431,400`. The Phase 1 conversion gate accepts `unit_raw = '%'`
*or* a value in 20-400; the `'%'` arm alone lets a level value through. The gate should also
require the magnitude to be plausible for a percentage.

**3. `pct_unit_sanity` — 7 rows (retail)**
`unit_raw = '%'` on values above 100,000. Same root cause as (2).

## What the warnings mean

**`year_gaps` (4 domains)** — genuine publication gaps, not defects. Verified directly:
`environment_safety_disaster` has no rows for 2013-2017 because **no sheet named for that domain
exists in those reports** (60-70 sheets per year, zero environmental); enterprise registration
simply begins in 2016. The paired error-level check `year_gaps_with_source` — a year where the
domain's own sheets *were* extracted yet nothing was filed under it — returns **0 for all ten
domains**, which is the evidence that the misclassification defect is closed.

**`any_dimension_coverage` = 0 (4 domains)** — those domains use `dimension_strategy='indicator'`,
so no geography/sector/product is routed at all. Known gap (P3.2 in the assessment).

**`grain_null_pct` (environment 84%, other_or_unknown 73%)** — those sheets' column headers name
no period, so the period is the report's and NULL is the honest answer.
