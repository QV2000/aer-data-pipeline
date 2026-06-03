-- Derived report views for TrendEnergy crude marketing reps.
--
-- Dependency: run docs/sql/new_crude_opportunity_report.sql first so the
-- new_crude_opportunity_report view exists in the current DuckDB session.
--
-- These views split the lifecycle radar into practical sales products:
--   * weekly contact queue: early/pre-volume leads plus confirmed new oil
--   * weekly operator summary: who reps should prioritize this week
--   * monthly confirmed report: production-backed wells/pads and momentum
--   * monthly operator summary: definitive production-backed rollup

CREATE OR REPLACE VIEW new_crude_lifecycle_timeline AS
SELECT
    CASE
        WHEN primary_signal IN ('ACTIVE_CRUDE_STATUS', 'CONFIDENTIAL_RELEASE', 'SPUD_CRUDE_LIKELY')
             AND first_oil_month IS NULL
        THEN '1_EARLY_SIGNALS'
        WHEN primary_signal IN ('NEW_BATTERY_FIRST_OIL', 'FIRST_CONFIRMED_OIL')
        THEN '2_CONFIRMED_NEW_OIL'
        WHEN primary_signal IN ('ACTIVE_CRUDE_STATUS', 'CONFIDENTIAL_RELEASE', 'SPUD_CRUDE_LIKELY')
             AND first_oil_month IS NOT NULL
        THEN '2_CONFIRMED_NEW_OIL'
        WHEN primary_signal = 'LICENCE_OIL'
        THEN '3_RESEARCH'
        WHEN primary_signal = 'SPUD_UNKNOWN_FLUID'
        THEN '4_WATCHLIST'
        WHEN primary_signal IN ('PRODUCTION_RESTART', 'PRODUCTION_STEP_CHANGE')
        THEN '5_PRODUCTION_CHANGES'
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
        THEN 'EARLY_REVIEW'
        WHEN primary_signal IN ('NEW_BATTERY_FIRST_OIL', 'FIRST_CONFIRMED_OIL')
        THEN 'CONFIRMED_REVIEW'
        WHEN primary_signal IN ('CONFIDENTIAL_RELEASE', 'ACTIVE_CRUDE_STATUS', 'SPUD_CRUDE_LIKELY')
             AND first_oil_month IS NOT NULL
        THEN 'CONFIRMED_REVIEW'
        WHEN primary_signal = 'LICENCE_OIL'
        THEN 'RESEARCH'
        WHEN primary_signal = 'SPUD_UNKNOWN_FLUID'
        THEN 'WATCHLIST'
        WHEN primary_signal IN ('PRODUCTION_RESTART', 'PRODUCTION_STEP_CHANGE')
        THEN 'MONITOR'
        ELSE 'REVIEW'
    END AS contact_priority,
    CASE
        WHEN primary_signal = 'NEW_BATTERY_FIRST_OIL'
        THEN 'Pad/battery first oil confirmed; review operator, volume, and facility context.'
        WHEN primary_signal = 'FIRST_CONFIRMED_OIL'
        THEN 'First oil confirmed; review production-backed crude opportunity.'
        WHEN primary_signal = 'CONFIDENTIAL_RELEASE'
        THEN 'Previously confidential well is newly visible; prioritize operator research before monthly production confirms.'
        WHEN primary_signal = 'ACTIVE_CRUDE_STATUS'
             AND first_oil_month IS NOT NULL
        THEN 'Well is producing crude — review latest volume and facility context.'
        WHEN primary_signal = 'ACTIVE_CRUDE_STATUS'
        THEN 'Well just started producing crude — call operator before Petrinex monthly volume lands.'
        WHEN primary_signal = 'SPUD_CRUDE_LIKELY'
        THEN 'Drilling started with crude evidence; add to early operator review.'
        WHEN primary_signal = 'LICENCE_OIL'
        THEN 'Oil licence issued; research operator and area, then watch for spud/status follow-up.'
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
        THEN 'well to start producing'
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
    latest_mom_oil_delta_m3,
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
        COUNT(*) FILTER (WHERE sales_section = '1_EARLY_SIGNALS') AS early_signal_count,
        COUNT(*) FILTER (WHERE sales_section = '2_CONFIRMED_NEW_OIL') AS confirmed_new_oil_count,
        COUNT(*) FILTER (WHERE sales_section = '3_RESEARCH') AS research_count,
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
            early_signal_count DESC,
            confirmed_new_oil_count DESC,
            top_contact_priority_score DESC,
            represented_wells DESC,
            latest_signal_date DESC
    ) AS operator_rank,
    o.*,
    e.top_examples,
    CASE
        WHEN early_signal_count > 0
        THEN 'Review this week before production volume confirms.'
        WHEN confirmed_new_oil_count > 0
        THEN 'Review production-backed crude opportunity.'
        WHEN research_count > 0
        THEN 'Research licence/spud context and assign for follow-up.'
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
    latest_mom_oil_delta_m3,
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
        THEN 'New well-level first oil confirmed; review production-backed opportunity.'
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
    COUNT(*) FILTER (WHERE sales_section = '1_EARLY_SIGNALS') AS early_signal_count,
    COUNT(*) FILTER (WHERE sales_section = '2_CONFIRMED_NEW_OIL') AS confirmed_new_oil_count,
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
         confirmed_new_oil_count DESC,
         early_signal_count DESC,
         opportunity_count DESC;

CREATE OR REPLACE VIEW weekly_operator_digest AS
WITH digest_params AS (
    SELECT
        CURRENT_DATE::DATE AS as_of_date,
        (CURRENT_DATE - INTERVAL 7 DAY)::DATE AS week_cutoff
),
temi_facility_names(facility_id, facility_name) AS (
    VALUES
        ('ABTM0000930', 'Cynthia Pembina'),
        ('ABTM0112311', 'Rush Energy Services'),
        ('ABTM0125201', 'Vermilion 15-16-051-11W5'),
        ('ABTM0157119', 'Persist Wayne'),
        ('SKTMTT15003', 'Dulwich')
),
operator_lookup_for_digest AS MATERIALIZED (
    SELECT
        identifier_kind,
        identifier_value,
        operator_id
    FROM (
        SELECT
            identifier_kind,
            UPPER(TRIM(identifier_value)) AS identifier_value,
            operator_id,
            ROW_NUMBER() OVER (
                PARTITION BY identifier_kind, UPPER(TRIM(identifier_value))
                ORDER BY
                    CASE confidence
                        WHEN 'manual' THEN 4
                        WHEN 'high' THEN 3
                        WHEN 'medium' THEN 2
                        WHEN 'low' THEN 1
                        ELSE 0
                    END DESC,
                    operator_id
            ) AS rn
        FROM operators_resolved
        WHERE identifier_kind IN ('ab_ba_code_5', 'ab_ba_code_4')
          AND identifier_value IS NOT NULL
          AND TRIM(identifier_value) != ''
    )
    WHERE rn = 1
),
operator_production_rows AS MATERIALIZED (
    SELECT DISTINCT
        p.uwi,
        CASE
            WHEN REGEXP_MATCHES(TRIM(CAST(p.productionmonth AS VARCHAR)), '^[0-9]{4}-[0-9]{2}-[0-9]{2}$') THEN
                CAST(TRIM(CAST(p.productionmonth AS VARCHAR)) AS DATE)
            WHEN REGEXP_MATCHES(TRIM(CAST(p.productionmonth AS VARCHAR)), '^[0-9]{4}-[0-9]{2}$') THEN
                CAST(TRIM(CAST(p.productionmonth AS VARCHAR)) || '-01' AS DATE)
            WHEN REGEXP_MATCHES(TRIM(CAST(p.productionmonth AS VARCHAR)), '^[0-9]{6}$') THEN
                CAST(STRPTIME(TRIM(CAST(p.productionmonth AS VARCHAR)), '%Y%m') AS DATE)
            WHEN TRY_CAST(TRIM(CAST(p.productionmonth AS VARCHAR)) AS DATE) IS NOT NULL THEN
                DATE_TRUNC('month', TRY_CAST(TRIM(CAST(p.productionmonth AS VARCHAR)) AS DATE))::DATE
            ELSE NULL
        END AS prod_month,
        COALESCE(p.oil_prod_vol, 0)::DOUBLE AS oil_m3,
        ol.operator_id
    FROM production p
    JOIN wells w
      ON w.uwi = p.uwi
    JOIN operator_lookup_for_digest ol
      ON ol.identifier_kind IN ('ab_ba_code_5', 'ab_ba_code_4')
     AND ol.identifier_value = UPPER(TRIM(w.operator_code))
    WHERE p.uwi IS NOT NULL
      AND p.productionmonth IS NOT NULL
      AND p.oil_prod_vol IS NOT NULL
      AND p.oil_prod_vol > 0
      AND w.operator_code IS NOT NULL
),
operator_production_bounds AS (
    SELECT
        MIN(prod_month) AS first_available_prod_month,
        MAX(prod_month) AS latest_prod_month
    FROM operator_production_rows
    WHERE prod_month IS NOT NULL
),
operator_production_monthly AS (
    SELECT
        operator_id,
        prod_month,
        COUNT(DISTINCT uwi) AS active_well_count,
        SUM(oil_m3) AS oil_m3
    FROM operator_production_rows
    WHERE prod_month IS NOT NULL
      AND oil_m3 > 0
    GROUP BY operator_id, prod_month
),
operator_first_production AS (
    SELECT
        operator_id,
        MIN(prod_month) AS raw_first_oil_month
    FROM operator_production_monthly
    GROUP BY operator_id
),
operator_production_lag AS (
    SELECT
        opm.*,
        LAG(opm.oil_m3, 1, 0) OVER (
            PARTITION BY opm.operator_id
            ORDER BY opm.prod_month
        ) AS prior_month_oil_m3
    FROM operator_production_monthly opm
),
operator_production_trailing12 AS (
    SELECT
        opm.operator_id,
        ROUND(SUM(opm.oil_m3) / 12.0, 1) AS avg_monthly_oil_m3
    FROM operator_production_monthly opm
    CROSS JOIN operator_production_bounds opb
    WHERE opm.prod_month >= opb.latest_prod_month - INTERVAL 12 MONTH
    GROUP BY opm.operator_id
),
operator_latest_production AS (
    SELECT *
    FROM (
        SELECT
            opl.*,
            ROW_NUMBER() OVER (
                PARTITION BY opl.operator_id
                ORDER BY opl.prod_month DESC
            ) AS rn
        FROM operator_production_lag opl
    )
    WHERE rn = 1
),
operator_production_stats AS (
    SELECT
        olp.operator_id,
        CASE
            WHEN ofp.raw_first_oil_month > opb.first_available_prod_month
            THEN ofp.raw_first_oil_month
            ELSE NULL
        END AS operator_first_oil_month,
        olp.active_well_count AS well_count_active,
        ROUND(olp.oil_m3, 1) AS last_month_oil_m3,
        ROUND(olp.oil_m3 - COALESCE(olp.prior_month_oil_m3, 0), 1) AS mom_oil_delta_m3,
        COALESCE(t12.avg_monthly_oil_m3, 0) AS avg_monthly_oil_m3
    FROM operator_latest_production olp
    LEFT JOIN operator_first_production ofp
      ON ofp.operator_id = olp.operator_id
    LEFT JOIN operator_production_trailing12 t12
      ON t12.operator_id = olp.operator_id
    CROSS JOIN operator_production_bounds opb
),
latest_production_mover_month AS (
    SELECT MAX(latest_signal_date) AS latest_signal_date
    FROM new_crude_lifecycle_timeline
    WHERE primary_signal IN ('PRODUCTION_RESTART', 'PRODUCTION_STEP_CHANGE')
      AND latest_signal_date <= CURRENT_DATE
),
actionable_events AS (
    SELECT
        COALESCE(
            operator_id::VARCHAR,
            'NAME:' || NULLIF(TRIM(display_operator), '')
        ) AS operator_key,
        'weekly' AS digest_scope,
        new_crude_lifecycle_timeline.*
    FROM new_crude_lifecycle_timeline
    CROSS JOIN digest_params dp
    WHERE new_crude_lifecycle_timeline.latest_signal_date >= dp.week_cutoff
      AND new_crude_lifecycle_timeline.latest_signal_date <= dp.as_of_date
      AND new_crude_lifecycle_timeline.primary_signal != 'SPUD_UNKNOWN_FLUID'
      AND (
          operator_id IS NOT NULL
          OR NULLIF(TRIM(display_operator), '') IS NOT NULL
      )
    UNION ALL
    SELECT
        COALESCE(
            operator_id::VARCHAR,
            'NAME:' || NULLIF(TRIM(display_operator), '')
        ) AS operator_key,
        'latest_production_mover' AS digest_scope,
        new_crude_lifecycle_timeline.*
    FROM new_crude_lifecycle_timeline
    CROSS JOIN latest_production_mover_month lpm
    WHERE new_crude_lifecycle_timeline.primary_signal IN ('PRODUCTION_RESTART', 'PRODUCTION_STEP_CHANGE')
      AND new_crude_lifecycle_timeline.latest_signal_date = lpm.latest_signal_date
      AND (
          operator_id IS NOT NULL
          OR NULLIF(TRIM(display_operator), '') IS NOT NULL
      )
),
digest_source AS (
    SELECT * EXCLUDE (rn)
    FROM (
        SELECT
            ae.*,
            ROW_NUMBER() OVER (
                PARTITION BY operator_key, opportunity_id, primary_signal, latest_signal_date
                ORDER BY CASE WHEN digest_scope = 'weekly' THEN 1 ELSE 2 END
            ) AS rn
        FROM actionable_events ae
        WHERE operator_key IS NOT NULL
    )
    WHERE rn = 1
),
operator_nearest AS (
    SELECT
        operator_key,
        nearest_facility_id,
        distance_km,
        ROW_NUMBER() OVER (
            PARTITION BY operator_key
            ORDER BY distance_km ASC NULLS LAST, nearest_facility_id
        ) AS rn
    FROM digest_source
    WHERE distance_km IS NOT NULL
),
operator_rollup AS (
    SELECT
        operator_key,
        MAX(operator_id) AS operator_id,
        COALESCE(MAX(display_operator), 'Unresolved operator') AS display_operator,
        MAX(operator_short_name) AS operator_short_name,
        STRING_AGG(DISTINCT province, ', ' ORDER BY province)
            FILTER (WHERE province IS NOT NULL) AS province,
        MAX(CASE WHEN is_new_operator THEN 1 ELSE 0 END) = 1 AS is_new_operator,
        MAX(operator_first_oil_month) AS radar_operator_first_oil_month,
        MAX(avg_monthly_oil_m3) AS radar_avg_monthly_oil_m3,
        MIN(distance_km) AS min_distance_km,
        COUNT(*) FILTER (
            WHERE digest_scope = 'weekly'
               OR primary_signal IN ('PRODUCTION_RESTART', 'PRODUCTION_STEP_CHANGE')
        ) AS signal_count_7d,
        SUM(CASE WHEN COALESCE(latest_oil_m3, 0) > 0 THEN well_count ELSE 0 END) AS opportunity_well_count_active,
        SUM(COALESCE(latest_oil_m3, 0)) AS opportunity_last_month_oil_m3,
        MAX(CASE WHEN digest_scope = 'weekly' THEN 1 ELSE 0 END) = 1 AS has_weekly_activity,
        MAX(CASE
            WHEN primary_signal IN ('PRODUCTION_RESTART', 'PRODUCTION_STEP_CHANGE') THEN 1
            ELSE 0
        END) = 1 AS has_production_mover,
        MAX(CASE
            WHEN primary_signal IN ('PRODUCTION_RESTART', 'PRODUCTION_STEP_CHANGE')
            THEN latest_signal_date
            ELSE NULL
        END) AS production_mover_signal_date,
        SUM(CASE
            WHEN primary_signal IN ('PRODUCTION_RESTART', 'PRODUCTION_STEP_CHANGE')
            THEN COALESCE(NULLIF(latest_mom_oil_delta_m3, 0), latest_oil_m3, 0)
            ELSE 0
        END) AS opportunity_mom_oil_delta_m3
    FROM digest_source
    GROUP BY operator_key
),
category_rows AS (
    SELECT *
    FROM (
        SELECT
            *,
            CASE
                WHEN is_new_operator AND has_weekly_activity THEN 'new_operator'
                WHEN min_distance_km < 100
                 AND (has_weekly_activity OR has_production_mover) THEN 'near_facility'
                WHEN has_production_mover THEN 'production_mover'
                WHEN has_weekly_activity THEN 'other'
                ELSE NULL
            END AS category
        FROM operator_rollup
    )
    WHERE category IS NOT NULL
)
SELECT
    r.operator_key,
    r.operator_id,
    r.display_operator,
    r.operator_short_name,
    r.province,
    r.is_new_operator,
    COALESCE(
        ps.operator_first_oil_month,
        CASE WHEN r.is_new_operator THEN r.radar_operator_first_oil_month ELSE NULL END
    ) AS operator_first_oil_month,
    n.nearest_facility_id,
    tfn.facility_name AS nearest_facility_name,
    r.min_distance_km,
    r.signal_count_7d,
    COALESCE(ps.well_count_active, NULLIF(r.opportunity_well_count_active, 0))::INTEGER AS well_count_active,
    ROUND(COALESCE(ps.last_month_oil_m3, NULLIF(r.opportunity_last_month_oil_m3, 0)), 1) AS last_month_oil_m3,
    r.category,
    r.has_production_mover,
    r.production_mover_signal_date,
    ROUND(COALESCE(ps.mom_oil_delta_m3, r.opportunity_mom_oil_delta_m3, 0), 1) AS mom_oil_delta_m3
FROM category_rows r
LEFT JOIN operator_nearest n
  ON n.operator_key = r.operator_key
 AND n.rn = 1
LEFT JOIN temi_facility_names tfn
  ON tfn.facility_id = n.nearest_facility_id
LEFT JOIN operator_production_stats ps
  ON ps.operator_id = r.operator_id
WHERE COALESCE(ps.avg_monthly_oil_m3, r.radar_avg_monthly_oil_m3, 0) < 50000
  AND COALESCE(ps.last_month_oil_m3, r.opportunity_last_month_oil_m3, 0) < 50000
ORDER BY
    CASE
        WHEN category = 'new_operator' THEN 1
        WHEN category = 'near_facility' THEN 2
        WHEN category = 'production_mover' THEN 3
        ELSE 9
    END,
    CASE WHEN category = 'new_operator' THEN operator_first_oil_month END DESC NULLS LAST,
    CASE WHEN category = 'near_facility' THEN min_distance_km END ASC NULLS LAST,
    CASE WHEN category = 'production_mover' THEN mom_oil_delta_m3 END DESC NULLS LAST,
    display_operator;

CREATE OR REPLACE VIEW weekly_operator_timeline AS
WITH digest_params AS (
    SELECT
        CURRENT_DATE::DATE AS as_of_date,
        (CURRENT_DATE - INTERVAL 7 DAY)::DATE AS week_cutoff
),
candidate_opportunities AS (
    SELECT
        d.operator_key,
        d.operator_id,
        d.display_operator,
        d.category,
        d.mom_oil_delta_m3,
        t.opportunity_id,
        t.opportunity_type,
        t.primary_signal,
        t.latest_signal_date,
        t.contact_priority_score,
        t.licence_date,
        t.spud_date,
        t.status_active_date,
        t.status_transition_detail,
        t.confidential_release_date,
        t.first_oil_month,
        t.latest_oil_m3,
        t.latest_mom_oil_delta_m3,
        t.uwi,
        t.wells_in_pad,
        t.well_count,
        t.well_name,
        t.linked_facility_name,
        COALESCE(
            NULLIF(REGEXP_EXTRACT(t.uwi, '^[0-9]{2}/([^/]+)/[0-9]+$', 1), ''),
            t.uwi
        ) AS lsd_label
    FROM weekly_operator_digest d
    JOIN new_crude_lifecycle_timeline t
      ON COALESCE(
            t.operator_id::VARCHAR,
            'NAME:' || NULLIF(TRIM(t.display_operator), '')
         ) = d.operator_key
    CROSS JOIN digest_params dp
    WHERE d.category IN ('new_operator', 'near_facility', 'production_mover')
      AND t.primary_signal != 'SPUD_UNKNOWN_FLUID'
      AND (
          d.category = 'new_operator'
          OR (
              d.category = 'near_facility'
              AND (
                  (
                      t.latest_signal_date >= dp.week_cutoff
                      AND t.latest_signal_date <= dp.as_of_date
                  )
                  OR (
                      t.primary_signal IN ('PRODUCTION_RESTART', 'PRODUCTION_STEP_CHANGE')
                      AND t.latest_signal_date = d.production_mover_signal_date
                  )
              )
          )
          OR (
              d.category = 'production_mover'
              AND t.primary_signal IN ('PRODUCTION_RESTART', 'PRODUCTION_STEP_CHANGE')
              AND t.latest_signal_date = d.production_mover_signal_date
          )
      )
),
expanded_events AS (
    SELECT
        operator_key,
        operator_id,
        display_operator,
        category,
        licence_date AS event_date,
        'LICENCE_OIL' AS event_type,
        'oil licence issued' AS event_detail,
        COALESCE(lsd_label, well_name, opportunity_id) AS well_or_battery_label,
        contact_priority_score,
        opportunity_id
    FROM candidate_opportunities
    WHERE licence_date IS NOT NULL
      AND category != 'production_mover'
    UNION ALL
    SELECT
        operator_key,
        operator_id,
        display_operator,
        category,
        spud_date AS event_date,
        'SPUD_CRUDE_LIKELY' AS event_type,
        'spudded' AS event_detail,
        COALESCE(lsd_label, well_name, opportunity_id) AS well_or_battery_label,
        contact_priority_score,
        opportunity_id
    FROM candidate_opportunities
    WHERE spud_date IS NOT NULL
      AND category != 'production_mover'
    UNION ALL
    SELECT
        operator_key,
        operator_id,
        display_operator,
        category,
        status_active_date AS event_date,
        'ACTIVE_CRUDE_STATUS' AS event_type,
        COALESCE(status_transition_detail, 'well now producing crude') AS event_detail,
        COALESCE(lsd_label, well_name, opportunity_id) AS well_or_battery_label,
        contact_priority_score,
        opportunity_id
    FROM candidate_opportunities
    WHERE status_active_date IS NOT NULL
      AND category != 'production_mover'
    UNION ALL
    SELECT
        operator_key,
        operator_id,
        display_operator,
        category,
        confidential_release_date AS event_date,
        'CONFIDENTIAL_RELEASE' AS event_type,
        'confidentiality released' AS event_detail,
        COALESCE(lsd_label, well_name, opportunity_id) AS well_or_battery_label,
        contact_priority_score,
        opportunity_id
    FROM candidate_opportunities
    WHERE confidential_release_date IS NOT NULL
      AND category != 'production_mover'
    UNION ALL
    SELECT
        operator_key,
        operator_id,
        display_operator,
        category,
        first_oil_month AS event_date,
        CASE
            WHEN opportunity_type = 'BATTERY' THEN 'NEW_BATTERY_FIRST_OIL'
            ELSE 'FIRST_CONFIRMED_OIL'
        END AS event_type,
        CASE
            WHEN opportunity_type = 'BATTERY'
            THEN 'pad first oil produced (' || ROUND(COALESCE(latest_oil_m3, 0), 1)::VARCHAR || ' m3)'
            ELSE 'first oil produced (' || ROUND(COALESCE(latest_oil_m3, 0), 1)::VARCHAR || ' m3)'
        END AS event_detail,
        CASE
            WHEN opportunity_type = 'BATTERY'
            THEN COALESCE(well_count::VARCHAR || ' wells, multi-well battery', linked_facility_name, well_name, opportunity_id)
            ELSE COALESCE(lsd_label, well_name, opportunity_id)
        END AS well_or_battery_label,
        contact_priority_score,
        opportunity_id
    FROM candidate_opportunities
    WHERE first_oil_month IS NOT NULL
      AND category != 'production_mover'
    UNION ALL
    SELECT
        operator_key,
        operator_id,
        display_operator,
        category,
        latest_signal_date AS event_date,
        primary_signal AS event_type,
        CASE
            WHEN primary_signal = 'PRODUCTION_RESTART'
            THEN 'production restarted ('
                || CASE
                    WHEN COALESCE(NULLIF(latest_mom_oil_delta_m3, 0), mom_oil_delta_m3, latest_oil_m3, 0) >= 0
                    THEN '+'
                    ELSE ''
                END
                || ROUND(COALESCE(NULLIF(latest_mom_oil_delta_m3, 0), mom_oil_delta_m3, latest_oil_m3, 0), 1)::VARCHAR
                || ' m3 MoM)'
            ELSE 'production step change ('
                || CASE
                    WHEN COALESCE(NULLIF(latest_mom_oil_delta_m3, 0), mom_oil_delta_m3, latest_oil_m3, 0) >= 0
                    THEN '+'
                    ELSE ''
                END
                || ROUND(COALESCE(NULLIF(latest_mom_oil_delta_m3, 0), mom_oil_delta_m3, latest_oil_m3, 0), 1)::VARCHAR
                || ' m3 MoM)'
        END AS event_detail,
        COALESCE(lsd_label, well_name, opportunity_id) AS well_or_battery_label,
        contact_priority_score,
        opportunity_id
    FROM candidate_opportunities
    WHERE primary_signal IN ('PRODUCTION_RESTART', 'PRODUCTION_STEP_CHANGE')
),
timeline_window AS (
    SELECT
        CURRENT_DATE::DATE AS as_of_date,
        (CURRENT_DATE - INTERVAL 7 DAY)::DATE AS week_cutoff
),
ranked_events AS (
    SELECT
        ee.*,
        tw.week_cutoff,
        tw.as_of_date,
        ROW_NUMBER() OVER (
            PARTITION BY ee.operator_key, ee.category
            ORDER BY ee.event_date DESC, ee.contact_priority_score DESC, ee.opportunity_id, ee.event_type
        ) AS event_rank
    FROM expanded_events ee
    CROSS JOIN timeline_window tw
    WHERE ee.event_date IS NOT NULL
      -- For weekly-activity cards, only surface events that happened in the
      -- 7-day window. Historical lifecycle dates (e.g. an old first oil for
      -- a well that just got a new spud) are excluded to keep the digest
      -- focused on "what happened this week".
      AND (
          ee.category = 'production_mover'
          OR (
              ee.category IN ('new_operator', 'near_facility')
              AND ee.event_date >= tw.week_cutoff
              AND ee.event_date <= tw.as_of_date
          )
      )
)
SELECT
    operator_key,
    operator_id,
    display_operator,
    category,
    event_date,
    event_type,
    event_detail,
    well_or_battery_label
FROM ranked_events
WHERE (
        category = 'production_mover'
        AND event_rank <= 1
      )
   OR (
        category IN ('new_operator', 'near_facility')
        AND event_rank <= 3
      )
ORDER BY
    operator_key,
    event_date DESC;

CREATE OR REPLACE VIEW new_crude_report_sheet_manifest AS
SELECT *
FROM (
    VALUES
        ('weekly_contact_queue', 'new_crude_weekly_contact_queue', 'Rep-facing weekly list of wells/pads to contact or watch.'),
        ('weekly_operator_summary', 'new_crude_operator_weekly_summary', 'Operator rollup for weekly prioritization.'),
        ('monthly_confirmed_report', 'new_crude_monthly_confirmed_report', 'Production-backed monthly report.'),
        ('monthly_operator_summary', 'new_crude_operator_monthly_summary', 'Operator rollup for monthly confirmed production.'),
        ('lifecycle_timeline', 'new_crude_lifecycle_timeline', 'Full derived lifecycle fields for debugging and product integration.'),
        ('new_operator_spotlight', 'new_crude_new_operator_spotlight', 'Operators whose first oil is within the trailing 12 months. New entrants worth proactive outreach.'),
        ('weekly_operator_digest', 'weekly_operator_digest', 'Operator-grain weekly digest for the rep email.'),
        ('weekly_operator_timeline', 'weekly_operator_timeline', 'Lifecycle event snippets for weekly operator digest cards.')
) AS t(sheet_name, view_name, description);
