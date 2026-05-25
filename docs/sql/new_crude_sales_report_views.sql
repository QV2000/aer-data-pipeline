-- Derived report views for TrendEnergy crude marketing reps.
--
-- Dependency: run docs/sql/new_crude_opportunity_report.sql first so the
-- new_crude_opportunity_report view exists in the current DuckDB session.
--
-- These views split the lifecycle radar into practical sales products:
--   * weekly contact queue: early/pre-volume leads plus confirmed new crude
--   * weekly operator summary: who reps should prioritize this week
--   * monthly confirmed report: production-backed wells/pads and momentum
--   * monthly operator summary: definitive production-backed rollup

CREATE OR REPLACE VIEW new_crude_lifecycle_timeline AS
SELECT
    CASE
        WHEN primary_signal IN ('ACTIVE_CRUDE_STATUS', 'CONFIDENTIAL_RELEASE', 'SPUD_CRUDE_LIKELY')
             AND first_oil_month IS NULL
        THEN '1_CALL_NOW_PRE_VOLUME'
        WHEN primary_signal IN ('NEW_BATTERY_FIRST_OIL', 'FIRST_CONFIRMED_OIL')
        THEN '2_CONFIRMED_NEW_CRUDE'
        WHEN primary_signal IN ('ACTIVE_CRUDE_STATUS', 'CONFIDENTIAL_RELEASE', 'SPUD_CRUDE_LIKELY')
             AND first_oil_month IS NOT NULL
        THEN '2_CONFIRMED_NEW_CRUDE'
        WHEN primary_signal = 'LICENCE_OIL'
        THEN '3_RESEARCH_QUEUE'
        WHEN primary_signal = 'SPUD_UNKNOWN_FLUID'
        THEN '4_WATCHLIST'
        WHEN primary_signal IN ('PRODUCTION_RESTART', 'PRODUCTION_STEP_CHANGE')
        THEN '5_PRODUCTION_MOMENTUM'
        ELSE '9_OTHER'
    END AS sales_section,
    CASE
        WHEN primary_signal = 'NEW_BATTERY_FIRST_OIL' THEN 98
        WHEN primary_signal = 'FIRST_CONFIRMED_OIL' THEN 95
        WHEN primary_signal = 'CONFIDENTIAL_RELEASE' THEN 90
        WHEN primary_signal = 'ACTIVE_CRUDE_STATUS' THEN 85
        WHEN primary_signal = 'SPUD_CRUDE_LIKELY' THEN 75
        WHEN primary_signal = 'LICENCE_OIL' THEN 60
        WHEN primary_signal = 'SPUD_UNKNOWN_FLUID' THEN 35
        WHEN primary_signal = 'PRODUCTION_RESTART' THEN 30
        WHEN primary_signal = 'PRODUCTION_STEP_CHANGE' THEN 20
        ELSE 0
    END AS contact_priority_score,
    CASE
        WHEN primary_signal IN ('CONFIDENTIAL_RELEASE', 'ACTIVE_CRUDE_STATUS', 'SPUD_CRUDE_LIKELY')
             AND first_oil_month IS NULL
        THEN 'CALL_NOW'
        WHEN primary_signal IN ('NEW_BATTERY_FIRST_OIL', 'FIRST_CONFIRMED_OIL')
        THEN 'CONFIRMED_FOLLOW_UP'
        WHEN primary_signal IN ('CONFIDENTIAL_RELEASE', 'ACTIVE_CRUDE_STATUS', 'SPUD_CRUDE_LIKELY')
             AND first_oil_month IS NOT NULL
        THEN 'CONFIRMED_FOLLOW_UP'
        WHEN primary_signal = 'LICENCE_OIL'
        THEN 'RESEARCH_QUEUE'
        WHEN primary_signal = 'SPUD_UNKNOWN_FLUID'
        THEN 'WATCHLIST'
        WHEN primary_signal IN ('PRODUCTION_RESTART', 'PRODUCTION_STEP_CHANGE')
        THEN 'MONITOR_MOMENTUM'
        ELSE 'REVIEW'
    END AS contact_priority,
    CASE
        WHEN primary_signal = 'NEW_BATTERY_FIRST_OIL'
        THEN 'Pad/battery first oil confirmed; contact operator with pad-level crude marketing offer.'
        WHEN primary_signal = 'FIRST_CONFIRMED_OIL'
        THEN 'First oil confirmed; contact operator with production-backed crude marketing offer.'
        WHEN primary_signal = 'CONFIDENTIAL_RELEASE'
        THEN 'Previously confidential well is newly visible; research and contact before monthly production confirms.'
        WHEN primary_signal = 'ACTIVE_CRUDE_STATUS'
             AND first_oil_month IS NOT NULL
        THEN 'Active crude status is already production-backed; contact with latest volume and facility context.'
        WHEN primary_signal = 'ACTIVE_CRUDE_STATUS'
        THEN 'Well reached active crude status; contact before Petrinex monthly volume appears.'
        WHEN primary_signal = 'SPUD_CRUDE_LIKELY'
        THEN 'Drilling started with crude evidence; contact early or assign to rep watchlist.'
        WHEN primary_signal = 'LICENCE_OIL'
        THEN 'Oil licence issued; research operator and area, then queue for spud/status follow-up.'
        WHEN primary_signal = 'SPUD_UNKNOWN_FLUID'
        THEN 'Drilling started but crude is not proven; watch for status, facility, or production confirmation.'
        WHEN primary_signal = 'PRODUCTION_RESTART'
        THEN 'Oil restarted after inactive months; review for changed takeaway needs.'
        WHEN primary_signal = 'PRODUCTION_STEP_CHANGE'
        THEN 'Oil volume stepped up; review as production momentum, not a new-well lead.'
        ELSE recommended_action
    END AS sales_action,
    CASE
        WHEN primary_signal IN ('NEW_BATTERY_FIRST_OIL', 'FIRST_CONFIRMED_OIL')
        THEN 'CONFIRMED_PRODUCTION'
        WHEN first_oil_month IS NOT NULL
        THEN 'PRODUCING'
        WHEN status_active_date IS NOT NULL
        THEN 'ACTIVE_PRE_VOLUME'
        WHEN confidential_release_date IS NOT NULL
        THEN 'RELEASED_CONFIDENTIAL'
        WHEN spud_date IS NOT NULL
        THEN 'SPUDDED'
        WHEN licence_date IS NOT NULL
        THEN 'LICENSED'
        ELSE 'UNCLASSIFIED'
    END AS current_stage,
    CASE
        WHEN first_oil_month IS NULL AND status_active_date IS NOT NULL
        THEN 'monthly production confirmation'
        WHEN first_oil_month IS NULL AND confidential_release_date IS NOT NULL
        THEN 'status change or first production'
        WHEN first_oil_month IS NULL AND spud_date IS NOT NULL
        THEN 'active crude status'
        WHEN first_oil_month IS NULL AND licence_date IS NOT NULL
        THEN 'spud or drilling activity'
        WHEN primary_signal IN ('NEW_BATTERY_FIRST_OIL', 'FIRST_CONFIRMED_OIL')
        THEN 'latest volume and facility path'
        WHEN first_oil_month IS NOT NULL
        THEN 'latest volume and facility path'
        WHEN primary_signal IN ('PRODUCTION_RESTART', 'PRODUCTION_STEP_CHANGE')
        THEN 'sustained production trend'
        ELSE 'manual review'
    END AS next_expected_signal,
    DATE_DIFF('day', latest_signal_date, CURRENT_DATE)::INTEGER AS days_since_latest_signal,
    CASE
        WHEN licence_date IS NOT NULL THEN DATE_DIFF('day', licence_date, CURRENT_DATE)::INTEGER
        ELSE NULL
    END AS days_since_licence,
    CASE
        WHEN spud_date IS NOT NULL THEN DATE_DIFF('day', spud_date, CURRENT_DATE)::INTEGER
        ELSE NULL
    END AS days_since_spud,
    CASE
        WHEN status_active_date IS NOT NULL THEN DATE_DIFF('day', status_active_date, CURRENT_DATE)::INTEGER
        ELSE NULL
    END AS days_since_active_status,
    CASE
        WHEN confidential_release_date IS NOT NULL THEN DATE_DIFF('day', confidential_release_date, CURRENT_DATE)::INTEGER
        ELSE NULL
    END AS days_since_confidential_release,
    CASE
        WHEN first_oil_month IS NULL THEN TRUE
        ELSE FALSE
    END AS is_pre_production,
    CASE
        WHEN primary_signal IN (
            'NEW_BATTERY_FIRST_OIL',
            'FIRST_CONFIRMED_OIL',
            'PRODUCTION_RESTART',
            'PRODUCTION_STEP_CHANGE'
        )
        THEN TRUE
        ELSE FALSE
    END AS is_production_backed,
    CASE
        WHEN primary_signal IN ('PRODUCTION_RESTART', 'PRODUCTION_STEP_CHANGE')
        THEN TRUE
        ELSE FALSE
    END AS is_production_momentum,
    * EXCLUDE (latest_gas_e3m3)
FROM new_crude_opportunity_report;

CREATE OR REPLACE VIEW new_crude_weekly_contact_queue AS
SELECT
    sales_section,
    contact_priority,
    contact_priority_score,
    sales_action,
    current_stage,
    next_expected_signal,
    days_since_latest_signal,
    is_pre_production,
    primary_signal,
    confidence,
    latest_signal_date,
    lifecycle_badges,
    display_operator,
    operator_id,
    operator_short_name,
    source_display_operator,
    operator_resolution_kind,
    operator_resolution_confidence,
    opportunity_type,
    opportunity_id,
    uwi,
    wells_in_pad,
    well_count,
    well_name,
    province,
    field_name,
    pool_deposit,
    formation,
    linked_facility_id,
    linked_facility_name,
    linked_facility_sub_type_desc,
    crude_hub_reach,
    licence_date,
    spud_date,
    status_active_date,
    confidential_release_date,
    first_oil_month,
    latest_prod_month,
    latest_oil_m3,
    latest_water_m3,
    centroid_lat,
    centroid_lon,
    operator_size_tier,
    avg_monthly_oil_m3,
    is_new_operator,
    operator_first_oil_month,
    nearest_facility_id,
    distance_km,
    proximity_bucket
FROM new_crude_lifecycle_timeline
WHERE primary_signal IN (
    'CONFIDENTIAL_RELEASE',
    'ACTIVE_CRUDE_STATUS',
    'SPUD_CRUDE_LIKELY',
    'SPUD_UNKNOWN_FLUID',
    'LICENCE_OIL',
    'NEW_BATTERY_FIRST_OIL',
    'FIRST_CONFIRMED_OIL'
)
  AND latest_signal_date <= CURRENT_DATE
ORDER BY
    sales_section,
    contact_priority_score DESC,
    latest_signal_date DESC,
    COALESCE(latest_oil_m3, 0) DESC,
    display_operator;

CREATE OR REPLACE VIEW new_crude_operator_weekly_summary AS
WITH ranked_queue AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY COALESCE(operator_id::VARCHAR, display_operator, 'UNRESOLVED')
            ORDER BY contact_priority_score DESC, latest_signal_date DESC, COALESCE(latest_oil_m3, 0) DESC
        ) AS operator_example_rank
    FROM new_crude_weekly_contact_queue
),
operator_examples AS (
    SELECT
        operator_id,
        display_operator,
        MAX(operator_short_name) AS operator_short_name,
        STRING_AGG(
            primary_signal || ': ' || COALESCE(well_name, opportunity_id),
            '; '
            ORDER BY contact_priority_score DESC, latest_signal_date DESC
        ) AS top_examples
    FROM ranked_queue
    WHERE operator_example_rank <= 5
    GROUP BY operator_id, display_operator
),
operator_rollup AS (
    SELECT
        operator_id,
        display_operator,
        MAX(operator_short_name) AS operator_short_name,
        COUNT(*) AS opportunity_count,
        SUM(well_count) AS represented_wells,
        COUNT(*) FILTER (WHERE sales_section = '1_CALL_NOW_PRE_VOLUME') AS call_now_pre_volume_count,
        COUNT(*) FILTER (WHERE sales_section = '2_CONFIRMED_NEW_CRUDE') AS confirmed_new_crude_count,
        COUNT(*) FILTER (WHERE sales_section = '3_RESEARCH_QUEUE') AS research_queue_count,
        COUNT(*) FILTER (WHERE sales_section = '4_WATCHLIST') AS watchlist_count,
        COUNT(*) FILTER (WHERE opportunity_type = 'BATTERY') AS battery_opportunity_count,
        COUNT(DISTINCT linked_facility_id) FILTER (WHERE linked_facility_id IS NOT NULL) AS linked_facility_count,
        MAX(contact_priority_score) AS top_contact_priority_score,
        MAX(latest_signal_date) AS latest_signal_date,
        SUM(COALESCE(latest_oil_m3, 0)) AS latest_oil_m3_sum,
        STRING_AGG(DISTINCT province, ', ' ORDER BY province) AS provinces,
        STRING_AGG(DISTINCT field_name, ', ' ORDER BY field_name) FILTER (WHERE field_name IS NOT NULL) AS fields,
        STRING_AGG(DISTINCT primary_signal, ', ' ORDER BY primary_signal) AS signal_mix
    FROM new_crude_weekly_contact_queue
    GROUP BY operator_id, display_operator
)
SELECT
    ROW_NUMBER() OVER (
        ORDER BY
            call_now_pre_volume_count DESC,
            confirmed_new_crude_count DESC,
            top_contact_priority_score DESC,
            represented_wells DESC,
            latest_signal_date DESC
    ) AS operator_rank,
    o.*,
    e.top_examples,
    CASE
        WHEN call_now_pre_volume_count > 0
        THEN 'Contact this week before production volume confirms.'
        WHEN confirmed_new_crude_count > 0
        THEN 'Follow up with production-backed crude opportunity.'
        WHEN research_queue_count > 0
        THEN 'Research licence/spud context and assign to rep queue.'
        WHEN watchlist_count > 0
        THEN 'Monitor until crude evidence improves.'
        ELSE 'Review manually.'
    END AS recommended_operator_action
FROM operator_rollup o
LEFT JOIN operator_examples e
  ON e.operator_id IS NOT DISTINCT FROM o.operator_id
 AND e.display_operator IS NOT DISTINCT FROM o.display_operator
ORDER BY operator_rank;

CREATE OR REPLACE VIEW new_crude_monthly_confirmed_report AS
SELECT
    CASE
        WHEN primary_signal = 'NEW_BATTERY_FIRST_OIL' THEN '1_NEW_PAD_FIRST_OIL'
        WHEN primary_signal = 'FIRST_CONFIRMED_OIL' THEN '2_NEW_WELL_FIRST_OIL'
        WHEN primary_signal = 'PRODUCTION_RESTART' THEN '3_RESTART'
        WHEN primary_signal = 'PRODUCTION_STEP_CHANGE' THEN '4_STEP_CHANGE'
        ELSE '9_OTHER_CONFIRMED'
    END AS monthly_section,
    DATE_TRUNC(
        'month',
        COALESCE(first_oil_month, latest_prod_month, latest_signal_date)
    )::DATE AS production_report_month,
    primary_signal,
    confidence,
    latest_signal_date,
    lifecycle_badges,
    display_operator,
    operator_id,
    operator_short_name,
    source_display_operator,
    operator_resolution_kind,
    operator_resolution_confidence,
    opportunity_type,
    opportunity_id,
    uwi,
    wells_in_pad,
    well_count,
    well_name,
    province,
    field_name,
    pool_deposit,
    formation,
    linked_facility_id,
    linked_facility_name,
    linked_facility_sub_type_desc,
    crude_hub_reach,
    first_oil_month,
    latest_prod_month,
    latest_oil_m3,
    latest_water_m3,
    latest_cond_m3,
    is_crude_connected,
    crude_terminal_status,
    nearest_hub_id,
    hops_to_nearest_hub,
    operator_size_tier,
    avg_monthly_oil_m3,
    is_new_operator,
    operator_first_oil_month,
    nearest_facility_id,
    distance_km,
    proximity_bucket,
    primary_source_detail
FROM new_crude_lifecycle_timeline
WHERE primary_signal IN (
    'NEW_BATTERY_FIRST_OIL',
    'FIRST_CONFIRMED_OIL',
    'PRODUCTION_RESTART',
    'PRODUCTION_STEP_CHANGE'
)
  AND latest_signal_date <= CURRENT_DATE
ORDER BY
    production_report_month DESC,
    monthly_section,
    COALESCE(latest_oil_m3, 0) DESC,
    display_operator;

CREATE OR REPLACE VIEW new_crude_operator_monthly_summary AS
WITH monthly_rollup AS (
    SELECT
        operator_id,
        display_operator,
        MAX(operator_short_name) AS operator_short_name,
        production_report_month,
        COUNT(*) AS opportunity_count,
        SUM(well_count) AS represented_wells,
        COUNT(*) FILTER (WHERE primary_signal = 'NEW_BATTERY_FIRST_OIL') AS new_battery_first_oil_count,
        COUNT(*) FILTER (WHERE primary_signal = 'FIRST_CONFIRMED_OIL') AS first_confirmed_oil_count,
        COUNT(*) FILTER (WHERE primary_signal = 'PRODUCTION_RESTART') AS restart_count,
        COUNT(*) FILTER (WHERE primary_signal = 'PRODUCTION_STEP_CHANGE') AS step_change_count,
        COUNT(DISTINCT linked_facility_id) FILTER (WHERE linked_facility_id IS NOT NULL) AS linked_facility_count,
        SUM(COALESCE(latest_oil_m3, 0)) AS latest_oil_m3_sum,
        MAX(COALESCE(latest_oil_m3, 0)) AS largest_single_oil_m3,
        STRING_AGG(DISTINCT province, ', ' ORDER BY province) AS provinces,
        STRING_AGG(DISTINCT field_name, ', ' ORDER BY field_name) FILTER (WHERE field_name IS NOT NULL) AS fields,
        STRING_AGG(DISTINCT primary_signal, ', ' ORDER BY primary_signal) AS signal_mix
    FROM new_crude_monthly_confirmed_report
    GROUP BY operator_id, display_operator, production_report_month
)
SELECT
    ROW_NUMBER() OVER (
        PARTITION BY production_report_month
        ORDER BY
            new_battery_first_oil_count DESC,
            first_confirmed_oil_count DESC,
            latest_oil_m3_sum DESC,
            represented_wells DESC
    ) AS monthly_operator_rank,
    *,
    CASE
        WHEN new_battery_first_oil_count > 0
        THEN 'Pad-level first oil confirmed; review battery and facility path.'
        WHEN first_confirmed_oil_count > 0
        THEN 'New well-level first oil confirmed; contact with production-backed offer.'
        WHEN restart_count > 0
        THEN 'Restarted crude production; review changed takeaway needs.'
        WHEN step_change_count > 0
        THEN 'Production momentum; monitor for sustained commercial change.'
        ELSE 'Review manually.'
    END AS recommended_operator_action
FROM monthly_rollup
ORDER BY production_report_month DESC, monthly_operator_rank;

CREATE OR REPLACE VIEW new_crude_new_operator_spotlight AS
SELECT
    operator_id,
    display_operator,
    operator_short_name,
    operator_size_tier,
    avg_monthly_oil_m3,
    operator_first_oil_month,
    COUNT(*) AS opportunity_count,
    SUM(well_count) AS represented_wells,
    COUNT(*) FILTER (WHERE sales_section = '1_CALL_NOW_PRE_VOLUME') AS call_now_pre_volume_count,
    COUNT(*) FILTER (WHERE sales_section = '2_CONFIRMED_NEW_CRUDE') AS confirmed_new_crude_count,
    COUNT(*) FILTER (WHERE sales_section = '4_WATCHLIST') AS watchlist_count,
    COUNT(*) FILTER (WHERE opportunity_type = 'BATTERY') AS battery_opportunity_count,
    STRING_AGG(DISTINCT province, ', ' ORDER BY province) AS provinces,
    STRING_AGG(DISTINCT primary_signal, ', ' ORDER BY primary_signal) AS signal_mix,
    MAX(latest_signal_date) AS latest_signal_date,
    SUM(COALESCE(latest_oil_m3, 0)) AS latest_oil_m3_sum
FROM new_crude_weekly_contact_queue
WHERE is_new_operator = TRUE
GROUP BY operator_id, display_operator, operator_short_name,
         operator_size_tier, avg_monthly_oil_m3, operator_first_oil_month
ORDER BY operator_first_oil_month DESC NULLS LAST,
         confirmed_new_crude_count DESC,
         call_now_pre_volume_count DESC,
         opportunity_count DESC;

CREATE OR REPLACE VIEW new_crude_report_sheet_manifest AS
SELECT *
FROM (
    VALUES
        ('weekly_contact_queue', 'new_crude_weekly_contact_queue', 'Rep-facing weekly list of wells/pads to contact or watch.'),
        ('weekly_operator_summary', 'new_crude_operator_weekly_summary', 'Operator rollup for weekly prioritization.'),
        ('monthly_confirmed_report', 'new_crude_monthly_confirmed_report', 'Production-backed monthly report.'),
        ('monthly_operator_summary', 'new_crude_operator_monthly_summary', 'Operator rollup for monthly confirmed production.'),
        ('lifecycle_timeline', 'new_crude_lifecycle_timeline', 'Full derived lifecycle fields for debugging and product integration.'),
        ('new_operator_spotlight', 'new_crude_new_operator_spotlight', 'Operators whose first oil is within the trailing 12 months. New entrants worth proactive outreach.')
) AS t(sheet_name, view_name, description);
