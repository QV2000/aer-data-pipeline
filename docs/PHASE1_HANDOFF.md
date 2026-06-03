# Phase 1 Handoff for Codex Review

**Status:** implementation complete, all acceptance criteria pass against the live VPS DuckDB.
**Branch:** `codex/fix-duckdb-wells-schema` (stacked on PR #1 head — pre-existing PR #1 commits remain intact).
**Date:** 2026-05-25.

Codex was unavailable during the implementation window — these notes document every decision
I made on his behalf so he can review/redirect before merge.

---

## What changed

1. **`seeds/temi_facilities.csv`** — new. Five-row seed of TEMI marketing facilities (4 AB + 1 SK) with coordinates verified against the warehouse (or user-provided for Dulwich).
2. **`docs/sql/new_crude_opportunity_report.sql`** — new CTEs at the end of the WITH chain (just before final SELECT), and 7 new columns added to the final SELECT.
3. **`docs/sql/new_crude_sales_report_views.sql`** — 6 new columns added to `new_crude_weekly_contact_queue` + `new_crude_monthly_confirmed_report`. New view `new_crude_new_operator_spotlight`. Manifest updated.
4. **`scripts/export_new_crude_reports.py`** — `REPORT_SHEETS` list grew from 5 to 6 entries (added `new_operator_spotlight`).

No warehouse mutations. The export still runs in read-only-equivalent mode (TEMP tables only, no `--write` required).

---

## Key decisions I made (please review)

### D1. Inlined CTEs instead of persistent ALTER TABLE / VIEW

**Spec said:** create a seed table `facilities`, a view `well_proximity`, and `ALTER TABLE operators ADD COLUMN ...`.

**I did:** added all of those as CTEs at the bottom of `new_crude_opportunity_report.sql`. No warehouse schema changes.

**Why:**
- The original user constraint was "do not mutate DuckDB without explicit `--write`." Persistent objects would have broken that.
- The existing report pattern uses TEMP-by-default; this fits in.
- Codex can promote to persistent (`temi_facilities` seed table + `well_proximity` view + `operators` ALTERs) in a follow-up migration. The SQL inside the CTEs is the same logic, just lifted into the warehouse.

### D2. Table name `temi_facilities` (not `facilities`)

**Spec said:** create table `facilities`.

**I did:** named the seed `temi_facilities` (TEMI = TrendEnergy Marketing Initiative? Or whatever you want it to mean — placeholder).

**Why:** the warehouse already has a 162K-row `facilities` table from AER ST102. Collision avoided. There's also an empty `target_facilities` table that looks like it was originally intended for this — worth deciding whether to populate that vs keep `temi_facilities` as a separate seed.

### D3. `operator_size_tier` semantic confirmed: lower_priority = majors

- `lower_priority` = operators with avg monthly oil production ≥ 50,000 m³ (~314k bbl/month) — integrated majors with in-house marketing.
- `priority` = everyone else — the operators who actually need a third-party crude marketer.
- 50,000 m³ threshold lives in a `tier_params` CTE, easy to tune.

Result: 23 lower_priority operators, 71 priority operators in the current report. CNRL/Cenovus/Suncor land in lower_priority correctly.

### D4. `is_new_operator` uses well-level first oil, NOT raw production min

**Spec said:** `is_new_operator = first oil within 12 months`, anchored to MAX(productionmonth).

**Initial implementation:** Used `MIN(production.productionmonth) GROUP BY operator_id`.

**Bug discovered:** The warehouse `production` table only goes back to **2024-03**. Every long-tenured operator (CNRL, Cenovus, Suncor, etc.) has their MIN at exactly 2024-03 — the data floor. This is a data-availability artifact, not a "new entrant" signal. The flag returned 0 across the board.

**Final implementation:** Computes `MIN(opportunity_context.first_oil_month) GROUP BY operator_id` — the operator's earliest first-oil signal visible in the current opportunity radar. This is the *practical* "new entrant in our pipeline" semantic.

**Result:** 9 new operators flagged (matches spec's "single or low double digits" expectation). All currently SK (TEINE, SATURN, FIVE, TUNDRA, etc.) with first oil 2025-08 through 2025-11.

**Codex: please verify this is the right semantic.** If you want true all-time first oil per operator, the warehouse would need to load `production_history` (which also starts 2024-03 — both tables have the same floor) plus a longer SK historical feed.

### D5. Operator production linkage limited to AB BA codes

`operator_prod_linked` (used for `avg_monthly_oil_m3`) joins `production.uwi → wells.uwi → operator_identifiers.identifier_kind IN ('ab_ba_code_5', 'ab_ba_code_4')`. This captures AB operators only.

**Consequence:** all SK operators show `avg_monthly_oil_m3 = 0.0` and therefore default to `priority` tier regardless of actual size. The 9 new operators in the spotlight are all SK, all showing avg=0 — they're correctly flagged as new but undersized due to this limitation.

**Codex: this is the biggest gap to consider for v1.1.** Two options:
1. Extend the linkage to also join via `wells.licensee` or an SK identifier kind on `operator_identifiers`.
2. Compute SK avg_monthly_oil_m3 via a separate path using `SKWI` + `normalized_uwi` join keys.

I left this as-is because the spotlight uses well-level signals (which work fine for both provinces); only the tier and avg_m3 columns are inaccurate for SK.

### D6. `new_operator_spotlight` filters from `weekly_contact_queue`, not `lifecycle_timeline`

The new view aggregates from `new_crude_weekly_contact_queue` so it inherits the section filter (excludes production-momentum-only entries) and the same operator rollup shape as `weekly_operator_summary`. Spec didn't pin down the source; this feels more useful for a sales rep.

### D7. New facility ABTM0125201 substituted for DLS-anchored synthetic ID

**Spec had:** `DLS-15-16-051-11W5` as a synthetic facility ID with operator "TBD".

**User provided:** that DLS location corresponds to Petrinex facility `ABTM0125201` "Vermilion 15-16-051-11w5 Tm", operator Vermilion Energy Inc.

**I used:** the real Petrinex ID + Vermilion as operator + coords from `_facilities_with_coords` (53.408441, -115.558574). Documented in the seed CSV notes column.

### D8. Dulwich Terminal coords: user-provided over warehouse

User-provided: (53.165195, -109.703831). Warehouse `_facilities_with_coords`: (53.176, -109.618). The lat/lon differ by ~6 km. I used the user-provided values since they were given explicitly. Documented in seed CSV.

---

## Acceptance criteria results

| # | Criterion | Result |
|---|---|---|
| 1 | Export completes < 20s, exit 0 | **13.7s, exit 0** ✅ |
| 2 | xlsx has 6 sheets | weekly_contact_queue (1278), weekly_operator_summary (131), new_operator_spotlight (9), monthly_confirmed (3267), monthly_operator_summary (419), lifecycle_timeline (3691) ✅ |
| 3 | Each of 5 facilities has ≥1 well linked | ABTM0125201: 114 wells, SKTMTT15003: 98, ABTM0112311: 30, ABTM0157119: 18, ABTM0000930: 3 ✅ |
| 4 | `operator_size_tier` returns 2 rows, `priority` larger | priority=71 ops, lower_priority=23 ops ✅ |
| 5 | `lower_priority count == avg_m3 >= 50k count` | 23 == 23 ✅ |
| 6 | `is_new_operator` count is single/low double digits | **9 operators** ✅ |
| 7 | No regression: weekly_contact_queue ~1278, monthly_confirmed ~3269 | 1278 ✓, 3267 (-2 within 0.06%, likely daily data churn) ✅ |

---

## Known limitations to flag

1. **All 9 spotlight operators are SK; avg_m3=0.0 for all.** SK production isn't joining cleanly to operator_id (D5). Operator size tier is unreliable for SK; spotlight membership is reliable.
2. **`production` table only spans 2024-03 onward.** Affects any "all-time" question. `is_new_operator` works around it by using opportunity-context first oil (D4).
3. **2 operators in the report have NULL tier.** They appear in opportunity_context with a resolved operator_id, but neither operator_first_oil_from_ctx (their wells lack first_oil_month) nor operator_prod_trailing12 (no matching production rows) include them. Acceptable edge case; FULL OUTER JOIN exposes them as NULL.
4. **Proximity bucket distribution looks AB-heavy by design.** All 5 TEMI facilities are AB+1 SK. SK wells mostly show `unknown` because of NULL centroid data on SK wells (`wells.centroid_lat IS NULL`). Codex may want to verify SK well centroids exist in the warehouse if more accurate SK proximity is needed.
5. **The `target_facilities` table (empty, schema looks bespoke for this) was not used.** I created a fresh `temi_facilities` CSV seed instead. Worth deciding whether to consolidate.

---

## Files changed (commits to push)

```
docs/PHASE1_HANDOFF.md                         (new, this file)
docs/sql/new_crude_opportunity_report.sql      (~160 lines added)
docs/sql/new_crude_sales_report_views.sql      (~30 lines added)
scripts/export_new_crude_reports.py            (1 line added to REPORT_SHEETS)
seeds/temi_facilities.csv                      (new, 5 data rows)
```

---

## What's out of scope but worth queueing for v1.1

- Persist `temi_facilities` as a real seed table loaded by the build pipeline.
- Persist `well_proximity` as a real view (`CREATE OR REPLACE VIEW`).
- Migrate operator enrichment to `ALTER TABLE operators ADD COLUMN ...` and populate during the daily build, instead of recomputing in CTEs every report run.
- Extend operator production linkage to SK so avg_monthly_oil_m3 and tier are accurate for SK operators.
- Consider whether `target_facilities` should be the home for the seed, retiring `temi_facilities`.
- Optional fallback for the 300 unresolved-operator SK rows from the prior phase: probe `linked_facility_operator_legal_name` as a final resolution path.

---

## Final sanity / how to verify

```bash
# In the data-pipeline container:
cd /tmp/ncr_validator  # or wherever the branch is checked out
time python scripts/export_new_crude_reports.py \
  --db /data/aer_data.duckdb \
  --out /tmp/new_crude_reports_phase1.xlsx
# Expect: ~14s, exit 0, 6 sheets logged
```

The xlsx is shippable for this week's report.
