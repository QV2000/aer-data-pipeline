# New Crude Opportunity Radar

## Recommendation

Build the weekly rep report as a one-row-per-opportunity lifecycle radar, not
as a flat event feed. Most rows still represent one well, but multiwell battery
first-oil events should collapse into one battery-level opportunity.

Default sections:

1. **New Early Watchlist**: new oil licences and spuds.
2. **New Active Crude Status**: status changes into CR-OIL/CR-BIT FLOW/PUMP.
3. **Recently Released Confidentials**: wells becoming public.
4. **First Confirmed Oil**: first reported oil production.
5. **Production Momentum**: restarts and step-change increases.

This keeps speed for prospecting while making confidence explicit. Production is
too lagged to be the only trigger, but it should be the confirmation signal. For
multiwell batteries, production confirmation should be grouped to avoid sending
reps several duplicate rows for one commercial event.

## Report Products

The base `new_crude_opportunity_report` view is the source table. Two rep-facing
products should be built from it:

| Product | Cadence | Purpose | SQL View |
| --- | --- | --- | --- |
| Weekly Contact Queue | Weekly | Early outreach before volume confirms, with confirmed first-oil addendum. | `new_crude_weekly_contact_queue` |
| Weekly Operator Summary | Weekly | Rank operators by current contact urgency and represented wells. | `new_crude_operator_weekly_summary` |
| Weekly Operator Digest | Weekly | Operator-grain HTML digest grouped by new operators, TEMI facility proximity, and production movers. | `weekly_operator_digest` |
| Weekly Operator Timeline | Weekly | Up to 1-3 lifecycle events per digest operator card. | `weekly_operator_timeline` |
| Monthly Confirmed Report | Monthly | Definitive production-backed wells/pads and production momentum. | `new_crude_monthly_confirmed_report` |
| Monthly Operator Summary | Monthly | Rank operators by first oil, battery events, restarts, step changes, and oil size. | `new_crude_operator_monthly_summary` |
| Lifecycle Timeline | Internal / API | Normalized stages, contact scores, and next expected signal. | `new_crude_lifecycle_timeline` |

The derived SQL lives at
`docs/sql/new_crude_sales_report_views.sql`. It depends on
`docs/sql/new_crude_opportunity_report.sql`.

## Report Row Contract

Each row represents one sales opportunity. A row can be a single well or a
multiwell battery event. A well can have several lifecycle dates, but each
opportunity has only one `primary_signal` for sorting and sectioning.

Core fields:

| Column | Purpose |
| --- | --- |
| `opportunity_id` | Stable ID: `WELL:{uwi}` or `BATTERY:{linked_facility_id}:{YYYY-MM}`. |
| `opportunity_type` | `WELL` or `BATTERY`. |
| `primary_signal` | Highest-value recent signal for this report cycle. |
| `confidence` | `CONFIRMED`, `HIGH`, `LIKELY`, or `WATCHLIST`. |
| `recommended_action` | Sales-oriented next action. |
| `lifecycle_badges` | Compact event progression: licensed, spudded, status, release, first oil. |
| `latest_signal_date` | Most recent lifecycle signal date. |
| `licence_date` | Earliest approval signal. |
| `spud_date` | Earliest drilling signal. |
| `status_active_date` | First/recent crude active status signal. |
| `confidential_release_date` | Public visibility signal. |
| `first_oil_month` | First confirmed oil production month. |
| `wells_in_pad` | Comma-separated UWIs for battery-level first-oil opportunities. |
| `well_count` | Number of wells represented by the row. |
| `latest_oil_m3` | Latest known oil volume. |
| `linked_facility_id` | Downstream commercial context. |
| `linked_facility_name` | Battery/facility name reps recognize. |
| `linked_facility_sub_type_desc` | Human-readable SWB/MWB/bitumen/oil-sands facility subtype. |
| `crude_hub_reach` | Whether facility context suggests crude marketing access. |
| `operator_id` | Canonical operator entity id from `operators_resolved`. |
| `operator_short_name` | Short display label from the canonical operator table. |
| `source_display_operator` | Pre-resolution operator string retained for QA. |
| `operator_resolution_kind` | Identifier kind that produced the canonical match. |

Derived fields used by the sales reports:

| Column | Purpose |
| --- | --- |
| `sales_section` | Report section: early signals, confirmed new oil, research, watchlist, production changes. |
| `contact_priority` | Rep-facing action bucket. |
| `contact_priority_score` | Numeric sort order inside the queue. |
| `sales_action` | Plain-language recommended action. |
| `current_stage` | Lifecycle state such as `SPUDDED`, `ACTIVE_PRE_VOLUME`, or `CONFIRMED_PRODUCTION`. |
| `next_expected_signal` | What the system expects to see next. |
| `is_pre_production` | True when no first-oil month has landed yet. |
| `is_production_backed` | True when the signal depends on production volume. |
| `is_production_momentum` | True for restart/step-change rows that are not new-well leads. |

## Activity We Can Monitor

TrendEnergy should treat the report as a sequence of signals, not one magic
definition of "new well."

| Stage | Warehouse Source | What It Means | Rep Value | Caveat |
| --- | --- | --- | --- | --- |
| Licence issued | `well_licences`, `licence_activity` | Operator has regulatory approval to drill or complete. | Earliest prospecting signal for field services and future crude business. | May never spud; may be amended or cancelled. |
| Confidential listing | `confidential_wells` | A well is hidden under confidentiality. | Confirms something valuable may exist, even before details are public. | Limited operational detail until release. |
| Spud / drilling | `spud_activity`, `drilling_activity` | Rig activity has started. | Best early "this is real" signal before production data. | Fluid may be unknown; gas wells can look identical at this stage. |
| Finished drilling | `well_attributes.finished_drill_date` | Well reached finished drill status. | Useful bridge between spud and active/producing. | Not always current enough for weekly action. |
| Active crude status | `status_changes`, `well_attributes.well_status_*` | Well moves into CR-OIL/CR-BIT FLOW/PUMP. | Strong crude signal before monthly production appears. | AB status data is richer than SK. |
| Confidential release | `confidential_wells.release_date`, `wells.release_date` | Previously hidden well becomes visible. | High-value research/prospecting trigger. | Not necessarily a newly drilled well. |
| First oil production | `production_history` | First reported oil volume above threshold. | Confirmed crude marketing opportunity. | Petrinex monthly lag means this is late. |
| Restart / step change | `production_history` | Suspended/low producer resumes or materially increases oil volume. | Upsell or reactivation opportunity. | It is an opportunity signal, not a new-well signal. |
| Facility linkage | `well_attributes.linked_facility_id`, `facilities_enriched`, `facility_crude_reach` | Ties the well to batteries, hubs, and crude reachability. | Helps reps route the opportunity to the right commercial path. | Coverage depends on linked facility completeness. |

## Signal Definitions

### A. Licence Issued

Use `well_licences` and `licence_activity`.

Crude evidence:

- `well_completion_type = 'Oil Well'`
- existing `wells.fluid LIKE 'CR-%'`
- target pool/formation metadata when available
- `linked_facility_sub_type IN ('322', '311', '341', '342', '344', '506')`
  when the well already has a facility link

This is early and useful, but not proof of production. If a linked facility
subtype exists, treat it as more authoritative than the fluid/name heuristics.
If the linked subtype is gas-related, exclude the licence from the crude report.
If there is no facility link yet, fall back to the original early-stage
heuristics.

### B. Spud / Drilling Started

Use `spud_activity` and `drilling_activity`.

Keep unknown-fluid spuds, but label them as `WATCHLIST` unless another table
supports crude likelihood. Dropping unknowns loses early opportunities; treating
them as confirmed crude creates false positives.

For linked wells, use the linked facility subtype before the heuristic:

```sql
linked_facility_sub_type IN ('322', '311', '341', '342', '344', '506')
```

If a spud is linked to a gas facility subtype, exclude it. If it has no linkage,
keep the existing fallback rules because very new spuds may not have a current
Petrinex well-to-facility link yet.

### C. Active Crude Status

Use `status_changes`.

Strong AB signal:

```sql
new_status LIKE 'CR-OIL%' OR new_status LIKE 'CR-BIT%'
```

Exclude:

```sql
new_status LIKE '%ABD%' OR new_status LIKE '%SUSP%'
```

### D. Confidentiality Release

Use `confidential_wells.release_date` and `wells.release_date`.

This is not new drilling, but it is newly visible market information. It should
be a first-class section because reps can finally see and research the well.

### E. First Confirmed Oil

Use `production_history`.

This is the best evidence for crude marketing, but it is delayed by monthly
Petrinex reporting. It should confirm or upgrade existing watchlist wells.

Petrinex production is per UWI in the warehouse, but multiwell batteries can
allocate a battery measurement back to multiple wells. For single-well batteries
and unlinked wells, keep the normal `FIRST_CONFIRMED_OIL` signal. For linked
crude multiwell facilities where two or more wells first report oil in the same
month, collapse those wells into one `NEW_BATTERY_FIRST_OIL` opportunity.

Trigger:

```sql
linked_facility_sub_type IN ('322', '341', '342', '344', '506')
AND COUNT(DISTINCT uwi) >= 2
GROUP BY linked_facility_id, first_oil_month
```

Keep subtype `311` in the crude filter because it identifies crude single-well
batteries, but do not use it for multiwell battery collapse.

The battery row should include `wells_in_pad`, `well_count`, facility name,
facility subtype description, facility operator, and crude reachability.

### F. Production Momentum

Use `production_history`.

Two practical signals:

- restart: current month oil > threshold after several months at zero
- step-change: current month oil >= 2x prior three-month average

This is more "new opportunity" than "new well," so keep it in its own section.

## Weekly Window Logic

Use a short report window for operational activity and a longer production
window for lagged reporting:

| Window | Default | Used For |
| --- | --- | --- |
| `report_days` | 7 | licence, spud, status, release, restart/step-change events |
| `watchlist_days` | 30 | early licence/spud watchlist scope |
| `lifecycle_context_days` | 180 | older licence/spud/status dates shown as context for current signals |
| `production_months` | 6 | first oil, because production is monthly and Petrinex lags current activity |
| `oil_threshold_m3` | 1.0 | suppress tiny/null production noise |

Event-driven signals must use a closed window:

```sql
event_date >= report_cutoff AND event_date <= as_of_date
```

This prevents scheduled future confidential release dates from appearing as
current weekly leads before reps can act on them.

## Ranking

Recommended priority:

1. `FIRST_CONFIRMED_OIL`
2. `NEW_BATTERY_FIRST_OIL`
3. `CONFIDENTIAL_RELEASE`
4. `ACTIVE_CRUDE_STATUS`
5. `PRODUCTION_RESTART`
6. `PRODUCTION_STEP_CHANGE`
7. `SPUD_CRUDE_LIKELY`
8. `LICENCE_OIL`
9. `SPUD_UNKNOWN_FLUID`

Sort by priority, then latest signal date, then latest oil volume.

For the weekly contact queue, use sales sections rather than raw signal ranking:

1. `1_EARLY_SIGNALS`: `CONFIDENTIAL_RELEASE`, `ACTIVE_CRUDE_STATUS`, `SPUD_CRUDE_LIKELY` before first oil.
2. `2_CONFIRMED_NEW_OIL`: `NEW_BATTERY_FIRST_OIL`, `FIRST_CONFIRMED_OIL`, plus recent high-value status/release/spud signals that already have first oil.
3. `3_RESEARCH`: `LICENCE_OIL`.
4. `4_WATCHLIST`: `SPUD_UNKNOWN_FLUID`.
5. `5_PRODUCTION_CHANGES`: restart and step-change rows, kept out of the default new-well queue.

For the monthly confirmed report, include only production-backed signals:

1. `NEW_BATTERY_FIRST_OIL`
2. `FIRST_CONFIRMED_OIL`
3. `PRODUCTION_RESTART`
4. `PRODUCTION_STEP_CHANGE`

## SK and BC Handling

Do not parse coordinates from UWI in the report. Use stored coordinates from the
warehouse:

1. `wells.centroid_lat/lon` for the live warehouse coordinate view
2. `dls_centroids` for AB fallback
3. `sk_section_centroids` for SK fallback

SK should stay in scope. Current SK signals are strongest from SK Petrinex,
daily drilling, well bulletin, production, and facilities. AB status changes
are richer because AER ST2 is AB-specific.

For the live warehouse, SK production uses display UWIs like
`01/01-01-001-12W2/0`, while SK well attributes may only carry Petrinex
`well_id` values like `SKWI101010100112W200`. The report SQL derives the
display UWI from `well_id` before joining, and the silver build should populate
missing SK `uwi` values from `well_id` before deduplication on future rebuilds.

BC should be shown only if rows have a reliable province, coordinates, and
production/licence/spud signals in the warehouse. Otherwise label as unsupported
rather than silently dropping.

## UI Fit

The current toolbox panel, map markers, and XLSX export can stay. Required
changes are data shape and labels:

- one marker per opportunity
- section/filter by `primary_signal`
- badge list for lifecycle progression
- confidence chip
- XLSX export with all lifecycle dates, action fields, facility fields, and
  `wells_in_pad`

For `BATTERY` rows, show the marker at the facility coordinate when available.
Fallback to the average constituent well coordinate. For `WELL` rows, keep the
well coordinate.

Recommended default filters:

- province: AB + SK
- signal sections: all except production momentum if reps want only new wells
- confidence: confirmed/high/likely, with watchlist optional
- window: 7-day report, 30-day watchlist, 180-day lifecycle context

## Facility Linkage

The report should use the new AB + SK `well_attributes` facility linkage
columns:

| Column | Use |
| --- | --- |
| `linked_facility_id` | Join key for battery/pad grouping and crude reachability. |
| `linked_facility_name` | Display/export name for the battery or facility. |
| `linked_facility_sub_type` | Crude/gas pre-filter and MWB/SWB classification. |
| `linked_facility_sub_type_desc` | Human-readable export field. |
| `linked_facility_operator_legal_name` | Preferred display operator. |
| `linked_start_date` | Context only; do not use as first-oil timing. |

Prefer `linked_facility_operator_legal_name` over `wells.licensee` when both are
populated. Keep the well licensee in the export as secondary regulatory context.

Implement crude reachability by joining:

```sql
well_attributes.linked_facility_id = facility_crude_reach.facility_id
```

Surface:

- `is_crude_connected`
- `crude_terminal_status`
- `nearest_hub_id`
- `hops_to_nearest_hub`
- `crude_hub_reach`

## Operator Resolution

The report should resolve operator names through the warehouse
`operators_resolved` view before export. Use a priority-ranked lookup, not a
single broad `OR` join, so each opportunity gets one deterministic canonical
operator.

BA-code identifiers should match exactly. Name-like identifiers should also
match case-insensitively because SK sources commonly store legal names in all
caps, while `operator_identifiers.canonical_name` stores mixed-case names.
Do not put `UPPER(TRIM(...))` directly on both sides of a large join predicate;
materialize normalized candidate and lookup values first, then join on those
raw CTE columns. Otherwise DuckDB can fall back to a very slow join plan during
the export. The current SQL uses de-duplicated exact and normalized lookup maps
plus fixed-priority `LEFT JOIN`s, which avoids building a large candidate-match
intermediate.

Resolution priority:

1. AB 5-character BA code from the well/licensee source.
2. AB 4-character BA code from the well/licensee or linked facility BA source.
3. Linked facility operator legal name.
4. Existing display/operator name from the well context.
5. Raw licensee/operator fallback.

Identifier kinds to use:

- `ab_ba_code_5`
- `ab_ba_code_4`
- `canonical_name`
- `facility_operator_name`
- `well_licensee`
- `sk_legal_name`
- `operator_short_name`

Export both the canonical value and the QA fields:

- `display_operator` from `operators_resolved.canonical_name` when matched
- `operator_id`
- `operator_short_name`
- `source_display_operator`
- `operator_resolution_kind`
- `operator_resolution_value`
- `operator_resolution_confidence`

## Validation Queries

Before wiring this into email or the endpoint, run:

```sql
-- Count rows by primary signal.
SELECT primary_signal, opportunity_type, confidence, COUNT(*)
FROM new_crude_opportunity_report
GROUP BY primary_signal, opportunity_type, confidence
ORDER BY COUNT(*) DESC;

-- Validate opportunity rows and represented wells.
SELECT COUNT(*) AS opportunity_rows,
       COUNT(*) FILTER (WHERE opportunity_type = 'WELL') AS well_rows,
       COUNT(*) FILTER (WHERE opportunity_type = 'BATTERY') AS battery_rows,
       SUM(well_count) AS represented_wells
FROM new_crude_opportunity_report;

-- Check how much FIRST_CONFIRMED_OIL fan-out collapsed into battery rows.
SELECT SUM(well_count) AS wells_represented_by_battery_rows,
       COUNT(*) AS battery_opportunities,
       SUM(well_count) - COUNT(*) AS rows_removed_by_battery_collapse
FROM new_crude_opportunity_report
WHERE primary_signal = 'NEW_BATTERY_FIRST_OIL';

-- Find wells with early signals but no first oil yet.
SELECT primary_signal, COUNT(*)
FROM new_crude_opportunity_report
WHERE first_oil_month IS NULL
GROUP BY primary_signal;

-- Check SK coordinate coverage.
SELECT province, COUNT(*), COUNT(*) FILTER (WHERE centroid_lat IS NOT NULL AND centroid_lon IS NOT NULL)
FROM new_crude_opportunity_report
GROUP BY province;
```

The draft SQL lives at `docs/sql/new_crude_opportunity_report.sql`. It is meant
to be validated against the VPS DuckDB before it becomes application code.

Run the repeatable VPS validation with:

```bash
python scripts/validate_new_crude_report.py \
  --db /data/aer_data.duckdb \
  --sql docs/sql/new_crude_opportunity_report.sql
```

Export the rep workbook with:

```bash
python scripts/export_new_crude_reports.py \
  --db /data/aer_data.duckdb \
  --out reports/new_crude_reports.xlsx
```

The export script materializes the derived lifecycle and sheet objects as
temporary tables in the DuckDB session. This is intentional: the base
opportunity report is expensive, and keeping all five workbook sheets as chained
views would re-run that base report multiple times.

The report applies a shared crude screen before emitting signals. Rows with
explicit non-crude evidence, such as gas-linked facility subtypes or gas/water
well classifications, are excluded. Unknown-fluid spuds/releases can still land
in the watchlist only when there is no explicit gas/non-crude evidence.

The workbook sheets are:

- `weekly_contact_queue`
- `weekly_operator_summary`
- `monthly_confirmed`
- `monthly_operator_summary`
- `lifecycle_timeline`

Production-derived signals should be anchored to the latest parsed
`production_history.productionmonth`, not wall-clock `CURRENT_DATE`, because
Petrinex reporting lags field activity.

## Risks

- `production_history` retention controls whether "first oil" means first ever
  or first seen in retained history.
- A stateless weekly query will repeat production-month signals until the
  production window closes. If reps need "never sent before," add a sent-log or
  compare against the prior report output.
- SK UWI/CWI normalization must stay covered by validation because it controls
  production-to-facility linkage and battery collapse.
- `oil_prod_vol > 0` may be too sensitive; start with `>= 1.0 m3`, then tune.
- Confidential release dates may be scheduled release dates rather than the
  first day reps can practically act.
- Facility/hub enrichment depends on `linked_facility_id` coverage.
- Operator canonicalization should be done through the webapp resolver if the
  warehouse operator tables are not stable.
