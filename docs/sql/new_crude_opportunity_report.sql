-- Draft DuckDB SQL for the TrendEnergy weekly New Crude Opportunity report.
--
-- Target: the deployed TrendEnergy warehouse described in
-- docs/new_crude_opportunity_report.md. This file is intentionally read-only:
-- run it against a copy/session first, inspect counts, then decide whether to
-- materialize it as a view or keep it as a report query.
--
-- Main idea:
--   * trigger rows from this week's licence/spud/status/release activity
--   * include delayed production confirmation from the latest few production months
--   * collapse multiwell battery first-oil fan-out into one battery opportunity
--   * expose a primary signal, confidence, recommended action, and lifecycle badges

CREATE OR REPLACE VIEW new_crude_opportunity_report AS
WITH
base_params AS (
    SELECT
        CURRENT_DATE::DATE AS as_of_date,
        (CURRENT_DATE - INTERVAL 7 DAY)::DATE AS report_cutoff,
        (CURRENT_DATE - INTERVAL 30 DAY)::DATE AS watchlist_cutoff,
        (CURRENT_DATE - INTERVAL 180 DAY)::DATE AS lifecycle_cutoff,
        6 AS production_window_months,
        1.0::DOUBLE AS oil_threshold_m3
),

crude_facility_subtypes(sub_type) AS (
    VALUES
        ('322'), -- Crude Oil Multiwell Proration Battery
        ('311'), -- Crude Oil Single-Well Battery
        ('341'), -- Crude Bitumen Multiwell Group
        ('342'), -- Crude Bitumen Multiwell Proration
        ('344'), -- In-Situ Oil Sands legacy code
        ('506')  -- In-Situ Oil Sands
),

multiwell_crude_facility_subtypes(sub_type) AS (
    VALUES
        ('322'), -- Crude Oil Multiwell Proration Battery
        ('341'), -- Crude Bitumen Multiwell Group
        ('342'), -- Crude Bitumen Multiwell Proration
        ('344'), -- In-Situ Oil Sands legacy code
        ('506')  -- In-Situ Oil Sands
),

well_coords AS (
    SELECT
        uwi,
        MAX(centroid_lat) AS centroid_lat,
        MAX(centroid_lon) AS centroid_lon
    FROM wells
    GROUP BY uwi
),

wells_base AS (
    SELECT
        wc.uwi,
        NULLIF(TRIM(wc.well_name), '') AS well_name,
        NULLIF(TRIM(wc.licence_no), '') AS licence,
        NULLIF(TRIM(wc.licence_status), '') AS lic_status,
        COALESCE(NULLIF(TRIM(wc.licensee_name), ''), NULLIF(TRIM(wc.licensee_code), '')) AS raw_operator,
        UPPER(NULLIF(TRIM(wc.fluid), '')) AS fluid,
        UPPER(NULLIF(TRIM(wc.mode), '')) AS mode,
        UPPER(NULLIF(TRIM(wc.type), '')) AS well_type,
        NULLIF(TRIM(wc.structure), '') AS structure,
        wcg.centroid_lat,
        wcg.centroid_lon,
        wc.province
    FROM wells_current wc
    LEFT JOIN well_coords wcg ON wcg.uwi = wc.uwi
),

well_attr_source AS (
    SELECT
        CASE
            WHEN LOWER(TRIM(CAST(uwi AS VARCHAR))) IN ('', 'nan', 'none') THEN NULL
            ELSE TRIM(CAST(uwi AS VARCHAR))
        END AS source_uwi,
        CASE
            WHEN LOWER(TRIM(CAST(well_id AS VARCHAR))) IN ('', 'nan', 'none') THEN NULL
            ELSE TRIM(CAST(well_id AS VARCHAR))
        END AS well_id,
        REGEXP_REPLACE(
            UPPER(
                CASE
                    WHEN LOWER(TRIM(CAST(well_id AS VARCHAR))) IN ('', 'nan', 'none') THEN NULL
                    ELSE TRIM(CAST(well_id AS VARCHAR))
                END
            ),
            '^(ABWI|SKWI)',
            ''
        ) AS compact_well_id,
        field,
        field_name,
        pool_deposit,
        COALESCE(NULLIF(TRIM(pool_deposit_name), ''), NULLIF(TRIM(pool_deposit), '')) AS formation,
        horizontal_drill,
        CAST(finished_drill_date AS DATE) AS finished_drill_date,
        CAST(spud_date AS DATE) AS attribute_spud_date,
        CAST(licence_status_date AS DATE) AS licence_status_date,
        CAST(well_status_date AS DATE) AS well_status_date,
        UPPER(NULLIF(TRIM(well_status_fluid), '')) AS well_status_fluid,
        UPPER(NULLIF(TRIM(well_status_mode), '')) AS well_status_mode,
        NULLIF(TRIM(linked_facility_id), '') AS linked_facility_id,
        NULLIF(TRIM(linked_facility_type), '') AS linked_facility_type,
        NULLIF(TRIM(linked_facility_sub_type), '') AS linked_facility_sub_type,
        NULLIF(TRIM(linked_facility_sub_type_desc), '') AS linked_facility_sub_type_desc,
        NULLIF(TRIM(linked_facility_name), '') AS linked_facility_name,
        NULLIF(TRIM(linked_facility_operator_baid), '') AS linked_facility_operator_baid,
        NULLIF(TRIM(linked_facility_operator_legal_name), '') AS linked_facility_operator_legal_name,
        NULLIF(TRIM(linked_facility_province_state), '') AS linked_facility_province_state,
        NULLIF(TRIM(linked_facility_identifier), '') AS linked_facility_identifier,
        CAST(linked_start_date AS DATE) AS linked_start_date
    FROM well_attributes
),

well_attr_keyed AS (
    SELECT
        COALESCE(
            source_uwi,
            CASE
                WHEN REGEXP_MATCHES(compact_well_id, '^[0-9]{12}W[1-6][0-9]{2}$') THEN
                    SUBSTR(compact_well_id, 2, 2) || '/' ||
                    SUBSTR(compact_well_id, 4, 2) || '-' ||
                    SUBSTR(compact_well_id, 6, 2) || '-' ||
                    SUBSTR(compact_well_id, 8, 3) || '-' ||
                    SUBSTR(compact_well_id, 11, 2) ||
                    SUBSTR(compact_well_id, 13, 2) || '/' ||
                    CAST(CAST(SUBSTR(compact_well_id, 15, 2) AS INTEGER) AS VARCHAR)
                ELSE NULL
            END
        ) AS uwi,
        well_id,
        field,
        field_name,
        pool_deposit,
        formation,
        horizontal_drill,
        finished_drill_date,
        attribute_spud_date,
        licence_status_date,
        well_status_date,
        well_status_fluid,
        well_status_mode,
        linked_facility_id,
        linked_facility_type,
        linked_facility_sub_type,
        linked_facility_sub_type_desc,
        linked_facility_name,
        linked_facility_operator_baid,
        linked_facility_operator_legal_name,
        linked_facility_province_state,
        linked_facility_identifier,
        linked_start_date
    FROM well_attr_source
),

well_attr AS (
    SELECT * EXCLUDE (rn)
    FROM (
        SELECT
            wak.*,
            ROW_NUMBER() OVER (
                PARTITION BY wak.uwi
                ORDER BY
                    (wak.linked_facility_id IS NOT NULL) DESC,
                    wak.linked_start_date DESC NULLS LAST,
                    wak.well_id
            ) AS rn
        FROM well_attr_keyed wak
        WHERE wak.uwi IS NOT NULL
    )
    WHERE rn = 1
),

facility_reach AS (
    SELECT
        facility_id,
        MAX(is_crude_connected) AS is_crude_connected,
        MAX(nearest_hub_id) AS nearest_hub_id,
        MIN(hops_to_nearest_hub) AS hops_to_nearest_hub
    FROM facility_crude_reach
    GROUP BY facility_id
),

facility_context AS (
    SELECT
        facility_id,
        MAX(facility_name) AS facility_name,
        MAX(operator_name) AS facility_operator_name,
        MAX(province) AS facility_province,
        MAX(crude_terminal_status) AS crude_terminal_status,
        MAX(centroid_lat) AS facility_lat,
        MAX(centroid_lon) AS facility_lon
    FROM facilities_enriched
    GROUP BY facility_id
),

production_source AS (
    SELECT
        uwi,
        TRIM(CAST(productionmonth AS VARCHAR)) AS productionmonth_raw,
        oil_prod_vol,
        gas_prod_vol,
        water_prod_vol,
        COND
    FROM production_history
    WHERE uwi IS NOT NULL
      AND productionmonth IS NOT NULL
),

production_clean AS (
    SELECT
        uwi,
        CASE
            WHEN REGEXP_MATCHES(productionmonth_raw, '^[0-9]{4}-[0-9]{2}-[0-9]{2}$') THEN
                CAST(productionmonth_raw AS DATE)
            WHEN REGEXP_MATCHES(productionmonth_raw, '^[0-9]{4}-[0-9]{2}$') THEN
                CAST(productionmonth_raw || '-01' AS DATE)
            WHEN REGEXP_MATCHES(productionmonth_raw, '^[0-9]{6}$') THEN
                CAST(STRPTIME(productionmonth_raw, '%Y%m') AS DATE)
            WHEN TRY_CAST(productionmonth_raw AS DATE) IS NOT NULL THEN
                DATE_TRUNC('month', TRY_CAST(productionmonth_raw AS DATE))::DATE
            ELSE NULL
        END AS prod_month,
        COALESCE(oil_prod_vol, 0)::DOUBLE AS oil_prod_vol,
        COALESCE(gas_prod_vol, 0)::DOUBLE AS gas_prod_vol,
        COALESCE(water_prod_vol, 0)::DOUBLE AS water_prod_vol,
        COALESCE(COND, 0)::DOUBLE AS cond_prod_vol,
        CAST(NULL AS DOUBLE) AS hours_on
    FROM production_source
),

production_bounds AS (
    SELECT MAX(prod_month) AS latest_production_month
    FROM production_clean
),

params AS (
    SELECT
        bp.*,
        pb.latest_production_month,
        (
            COALESCE(pb.latest_production_month, DATE_TRUNC('month', bp.as_of_date)::DATE)
            - INTERVAL 5 MONTH
        )::DATE AS production_cutoff
    FROM base_params bp
    CROSS JOIN production_bounds pb
),

first_oil AS (
    SELECT
        pc.uwi,
        MIN(pc.prod_month) AS first_oil_month
    FROM production_clean pc
    CROSS JOIN params p
    WHERE pc.oil_prod_vol >= p.oil_threshold_m3
      AND pc.prod_month IS NOT NULL
    GROUP BY pc.uwi
),

first_oil_rows AS (
    SELECT
        fo.uwi,
        fo.first_oil_month,
        pc.oil_prod_vol AS first_oil_m3
    FROM first_oil fo
    JOIN production_clean pc
      ON pc.uwi = fo.uwi
     AND pc.prod_month = fo.first_oil_month
),

latest_prod AS (
    SELECT
        uwi,
        prod_month AS latest_prod_month,
        oil_prod_vol AS latest_oil_m3,
        gas_prod_vol AS latest_gas_e3m3,
        water_prod_vol AS latest_water_m3,
        cond_prod_vol AS latest_cond_m3,
        hours_on AS latest_hours_on
    FROM (
        SELECT
            pc.*,
            ROW_NUMBER() OVER (PARTITION BY pc.uwi ORDER BY pc.prod_month DESC) AS rn
        FROM production_clean pc
    )
    WHERE rn = 1
),

production_lag AS (
    SELECT
        pc.*,
        LAG(pc.oil_prod_vol, 1, 0) OVER (
            PARTITION BY pc.uwi ORDER BY pc.prod_month
        ) AS oil_prev_1,
        LAG(pc.oil_prod_vol, 2, 0) OVER (
            PARTITION BY pc.uwi ORDER BY pc.prod_month
        ) AS oil_prev_2,
        LAG(pc.oil_prod_vol, 3, 0) OVER (
            PARTITION BY pc.uwi ORDER BY pc.prod_month
        ) AS oil_prev_3,
        AVG(pc.oil_prod_vol) OVER (
            PARTITION BY pc.uwi
            ORDER BY pc.prod_month
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS oil_prior_3mo_avg
    FROM production_clean pc
),

latest_licence AS (
    SELECT
        uwi,
        well_name,
        licensee,
        CAST(issue_date AS DATE) AS issue_date,
        well_completion_type
    FROM (
        SELECT
            wl.*,
            ROW_NUMBER() OVER (
                PARTITION BY wl.uwi
                ORDER BY CAST(wl.issue_date AS DATE) DESC
            ) AS rn
        FROM well_licences wl
    WHERE wl.uwi IS NOT NULL
      AND wl.issue_date IS NOT NULL
      AND CAST(wl.issue_date AS DATE) <= CURRENT_DATE
    )
    WHERE rn = 1
),

licence_context AS (
    SELECT
        wl.uwi,
        MAX(CAST(wl.issue_date AS DATE)) AS licence_date
    FROM well_licences wl
    CROSS JOIN params p
    WHERE wl.uwi IS NOT NULL
      AND CAST(wl.issue_date AS DATE) >= p.lifecycle_cutoff
      AND CAST(wl.issue_date AS DATE) <= p.as_of_date
    GROUP BY wl.uwi
),

spud_context AS (
    SELECT
        sa.uwi,
        MAX(CAST(sa.spud_date AS DATE)) AS spud_date
    FROM spud_activity sa
    CROSS JOIN params p
    WHERE sa.uwi IS NOT NULL
      AND CAST(sa.spud_date AS DATE) >= p.lifecycle_cutoff
      AND CAST(sa.spud_date AS DATE) <= p.as_of_date
    GROUP BY sa.uwi
),

status_context AS (
    SELECT
        sc.uwi,
        MAX(CAST(sc.event_date AS DATE)) AS status_active_date
    FROM status_changes sc
    CROSS JOIN params p
    WHERE sc.uwi IS NOT NULL
      AND CAST(sc.event_date AS DATE) >= p.lifecycle_cutoff
      AND CAST(sc.event_date AS DATE) <= p.as_of_date
      AND (
          UPPER(sc.new_status) LIKE 'CR-OIL%'
          OR UPPER(sc.new_status) LIKE 'CR-BIT%'
      )
      AND UPPER(sc.new_status) NOT LIKE '%ABD%'
      AND UPPER(sc.new_status) NOT LIKE '%ABAN%'
      AND UPPER(sc.new_status) NOT LIKE '%SUSP%'
    GROUP BY sc.uwi
),

release_events AS (
    SELECT
        cw.uwi,
        CAST(cw.release_date AS DATE) AS release_date
    FROM confidential_wells cw
    WHERE cw.uwi IS NOT NULL
      AND cw.release_date IS NOT NULL
),

release_context AS (
    SELECT
        re.uwi,
        MAX(re.release_date) AS confidential_release_date
    FROM release_events re
    CROSS JOIN params p
    WHERE re.release_date >= p.lifecycle_cutoff
      AND re.release_date <= p.as_of_date
    GROUP BY re.uwi
),

well_keys AS (
    SELECT uwi FROM wells_base WHERE uwi IS NOT NULL
    UNION
    SELECT uwi FROM well_attr WHERE uwi IS NOT NULL
    UNION
    SELECT uwi FROM latest_licence WHERE uwi IS NOT NULL
    UNION
    SELECT uwi FROM licence_context WHERE uwi IS NOT NULL
    UNION
    SELECT uwi FROM spud_context WHERE uwi IS NOT NULL
    UNION
    SELECT uwi FROM status_context WHERE uwi IS NOT NULL
    UNION
    SELECT uwi FROM release_context WHERE uwi IS NOT NULL
    UNION
    SELECT uwi FROM first_oil WHERE uwi IS NOT NULL
    UNION
    SELECT uwi FROM latest_prod WHERE uwi IS NOT NULL
),

well_context AS (
    SELECT
        'WELL:' || wk.uwi AS opportunity_id,
        'WELL' AS opportunity_type,
        wk.uwi,
        wk.uwi AS wells_in_pad,
        1 AS well_count,
        COALESCE(wb.well_name, ll.well_name, wk.uwi) AS well_name,
        wb.raw_operator AS well_licensee,
        COALESCE(wa.linked_facility_operator_legal_name, fc.facility_operator_name, wb.raw_operator, ll.licensee) AS display_operator,
        COALESCE(wb.raw_operator, ll.licensee) AS raw_operator,
        COALESCE(wb.province, wa.linked_facility_province_state, fc.facility_province) AS province,
        wb.licence,
        wb.lic_status,
        wb.fluid,
        wb.mode,
        wb.well_type,
        wb.structure,
        wa.horizontal_drill,
        wa.field,
        wa.field_name,
        wa.pool_deposit,
        wa.formation,
        wa.finished_drill_date,
        lc.licence_date,
        spc.spud_date,
        stc.status_active_date,
        rc.confidential_release_date,
        fo.first_oil_month,
        lp.latest_prod_month,
        lp.latest_oil_m3,
        lp.latest_gas_e3m3,
        lp.latest_water_m3,
        lp.latest_cond_m3,
        lp.latest_hours_on,
        wa.linked_facility_id,
        COALESCE(wa.linked_facility_name, fc.facility_name) AS linked_facility_name,
        wa.linked_facility_type,
        wa.linked_facility_sub_type,
        wa.linked_facility_sub_type_desc,
        wa.linked_facility_operator_baid,
        wa.linked_facility_operator_legal_name,
        wa.linked_facility_province_state,
        wa.linked_facility_identifier,
        wa.linked_start_date,
        fr.is_crude_connected,
        fc.crude_terminal_status,
        fr.nearest_hub_id,
        fr.hops_to_nearest_hub,
        CASE
            WHEN fr.is_crude_connected THEN
                'crude connected'
                || COALESCE(' / terminal: ' || fc.crude_terminal_status, '')
                || COALESCE(' / hub: ' || fr.nearest_hub_id, '')
                || COALESCE(' / hops: ' || fr.hops_to_nearest_hub::VARCHAR, '')
            WHEN fr.facility_id IS NOT NULL THEN 'not crude connected'
            ELSE NULL
        END AS crude_hub_reach,
        COALESCE(wb.centroid_lat, fc.facility_lat) AS centroid_lat,
        COALESCE(wb.centroid_lon, fc.facility_lon) AS centroid_lon
    FROM well_keys wk
    LEFT JOIN wells_base wb ON wb.uwi = wk.uwi
    LEFT JOIN well_attr wa ON wa.uwi = wk.uwi
    LEFT JOIN latest_licence ll ON ll.uwi = wk.uwi
    LEFT JOIN licence_context lc ON lc.uwi = wk.uwi
    LEFT JOIN spud_context spc ON spc.uwi = wk.uwi
    LEFT JOIN status_context stc ON stc.uwi = wk.uwi
    LEFT JOIN release_context rc ON rc.uwi = wk.uwi
    LEFT JOIN first_oil fo ON fo.uwi = wk.uwi
    LEFT JOIN latest_prod lp ON lp.uwi = wk.uwi
    LEFT JOIN facility_reach fr ON fr.facility_id = wa.linked_facility_id
    LEFT JOIN facility_context fc ON fc.facility_id = wa.linked_facility_id
),

battery_first_oil_groups AS (
    SELECT
        wa.linked_facility_id,
        forow.first_oil_month,
        COUNT(DISTINCT forow.uwi) AS well_count,
        STRING_AGG(forow.uwi, ', ' ORDER BY forow.uwi) AS wells_in_pad,
        SUM(forow.first_oil_m3) AS first_oil_m3
    FROM first_oil_rows forow
    JOIN well_attr wa ON wa.uwi = forow.uwi
    JOIN multiwell_crude_facility_subtypes cfs ON cfs.sub_type = wa.linked_facility_sub_type
    CROSS JOIN params p
    WHERE forow.first_oil_month >= p.production_cutoff
      AND wa.linked_facility_id IS NOT NULL
    GROUP BY wa.linked_facility_id, forow.first_oil_month
    HAVING COUNT(DISTINCT forow.uwi) >= 2
),

battery_first_oil_members AS (
    SELECT
        g.linked_facility_id,
        g.first_oil_month,
        forow.uwi
    FROM battery_first_oil_groups g
    JOIN well_attr wa ON wa.linked_facility_id = g.linked_facility_id
    JOIN first_oil_rows forow
      ON forow.uwi = wa.uwi
     AND forow.first_oil_month = g.first_oil_month
),

battery_context AS (
    SELECT
        'BATTERY:' || g.linked_facility_id || ':' || STRFTIME(g.first_oil_month, '%Y-%m') AS opportunity_id,
        'BATTERY' AS opportunity_type,
        CAST(NULL AS VARCHAR) AS uwi,
        g.wells_in_pad,
        g.well_count,
        COALESCE(MAX(wa.linked_facility_name), MAX(fc.facility_name), g.linked_facility_id) AS well_name,
        CAST(NULL AS VARCHAR) AS well_licensee,
        COALESCE(MAX(wa.linked_facility_operator_legal_name), MAX(fc.facility_operator_name)) AS display_operator,
        COALESCE(MAX(wa.linked_facility_operator_legal_name), MAX(fc.facility_operator_name)) AS raw_operator,
        COALESCE(MAX(wa.linked_facility_province_state), MAX(fc.facility_province), MAX(wb.province)) AS province,
        CAST(NULL AS VARCHAR) AS licence,
        CAST(NULL AS VARCHAR) AS lic_status,
        CAST(NULL AS VARCHAR) AS fluid,
        CAST(NULL AS VARCHAR) AS mode,
        CAST(NULL AS VARCHAR) AS well_type,
        CAST(NULL AS VARCHAR) AS structure,
        CAST(NULL AS VARCHAR) AS horizontal_drill,
        MAX(wa.field) AS field,
        MAX(wa.field_name) AS field_name,
        MAX(wa.pool_deposit) AS pool_deposit,
        MAX(wa.formation) AS formation,
        MAX(wa.finished_drill_date) AS finished_drill_date,
        MAX(lc.licence_date) AS licence_date,
        MAX(spc.spud_date) AS spud_date,
        MAX(stc.status_active_date) AS status_active_date,
        MAX(rc.confidential_release_date) AS confidential_release_date,
        g.first_oil_month,
        MAX(lp.latest_prod_month) AS latest_prod_month,
        SUM(COALESCE(lp.latest_oil_m3, 0)) AS latest_oil_m3,
        SUM(COALESCE(lp.latest_gas_e3m3, 0)) AS latest_gas_e3m3,
        SUM(COALESCE(lp.latest_water_m3, 0)) AS latest_water_m3,
        SUM(COALESCE(lp.latest_cond_m3, 0)) AS latest_cond_m3,
        SUM(COALESCE(lp.latest_hours_on, 0)) AS latest_hours_on,
        g.linked_facility_id,
        COALESCE(MAX(wa.linked_facility_name), MAX(fc.facility_name)) AS linked_facility_name,
        MAX(wa.linked_facility_type) AS linked_facility_type,
        MAX(wa.linked_facility_sub_type) AS linked_facility_sub_type,
        MAX(wa.linked_facility_sub_type_desc) AS linked_facility_sub_type_desc,
        MAX(wa.linked_facility_operator_baid) AS linked_facility_operator_baid,
        MAX(wa.linked_facility_operator_legal_name) AS linked_facility_operator_legal_name,
        MAX(wa.linked_facility_province_state) AS linked_facility_province_state,
        MAX(wa.linked_facility_identifier) AS linked_facility_identifier,
        MIN(wa.linked_start_date) AS linked_start_date,
        MAX(fr.is_crude_connected) AS is_crude_connected,
        MAX(fc.crude_terminal_status) AS crude_terminal_status,
        MAX(fr.nearest_hub_id) AS nearest_hub_id,
        MAX(fr.hops_to_nearest_hub) AS hops_to_nearest_hub,
        CASE
            WHEN MAX(fr.is_crude_connected) THEN
                'crude connected'
                || COALESCE(' / terminal: ' || MAX(fc.crude_terminal_status), '')
                || COALESCE(' / hub: ' || MAX(fr.nearest_hub_id), '')
                || COALESCE(' / hops: ' || MAX(fr.hops_to_nearest_hub)::VARCHAR, '')
            WHEN MAX(fr.facility_id) IS NOT NULL THEN 'not crude connected'
            ELSE NULL
        END AS crude_hub_reach,
        COALESCE(MAX(fc.facility_lat), AVG(wb.centroid_lat)) AS centroid_lat,
        COALESCE(MAX(fc.facility_lon), AVG(wb.centroid_lon)) AS centroid_lon
    FROM battery_first_oil_groups g
    JOIN battery_first_oil_members bm
      ON bm.linked_facility_id = g.linked_facility_id
     AND bm.first_oil_month = g.first_oil_month
    JOIN well_attr wa ON wa.uwi = bm.uwi
    LEFT JOIN wells_base wb ON wb.uwi = bm.uwi
    LEFT JOIN latest_prod lp ON lp.uwi = bm.uwi
    LEFT JOIN licence_context lc ON lc.uwi = bm.uwi
    LEFT JOIN spud_context spc ON spc.uwi = bm.uwi
    LEFT JOIN status_context stc ON stc.uwi = bm.uwi
    LEFT JOIN release_context rc ON rc.uwi = bm.uwi
    LEFT JOIN facility_reach fr ON fr.facility_id = g.linked_facility_id
    LEFT JOIN facility_context fc ON fc.facility_id = g.linked_facility_id
    GROUP BY g.linked_facility_id, g.first_oil_month, g.well_count, g.wells_in_pad
),

opportunity_context AS (
    SELECT * FROM well_context
    UNION ALL
    SELECT * FROM battery_context
),

licence_signal AS (
    SELECT
        'WELL:' || wl.uwi AS opportunity_id,
        'WELL' AS opportunity_type,
        wl.uwi,
        wa.linked_facility_id,
        'LICENCE_OIL' AS signal_type,
        CAST(wl.issue_date AS DATE) AS signal_date,
        50 AS signal_priority,
        'LIKELY' AS confidence,
        'Add to prospecting list; verify spud and operator contact.' AS recommended_action,
        COALESCE(wl.well_completion_type, wa.linked_facility_sub_type_desc) AS source_detail
    FROM well_licences wl
    LEFT JOIN wells_base wb ON wb.uwi = wl.uwi
    LEFT JOIN well_attr wa ON wa.uwi = wl.uwi
    CROSS JOIN params p
    WHERE wl.uwi IS NOT NULL
      AND CAST(wl.issue_date AS DATE) >= p.report_cutoff
      AND CAST(wl.issue_date AS DATE) <= p.as_of_date
      AND (
          CASE
              WHEN wa.linked_facility_sub_type IS NOT NULL THEN
                  wa.linked_facility_sub_type IN (SELECT sub_type FROM crude_facility_subtypes)
              ELSE
                  LOWER(COALESCE(wl.well_completion_type, '')) LIKE '%oil%'
                  OR COALESCE(wb.fluid, '') LIKE 'CR-%'
                  OR COALESCE(wb.well_type, '') LIKE '%OIL%'
          END
      )
),

spud_candidates AS (
    SELECT
        sa.uwi,
        wa.linked_facility_id,
        CAST(sa.spud_date AS DATE) AS spud_date,
        wa.formation AS target_formation,
        CASE
            WHEN wa.linked_facility_sub_type IS NOT NULL THEN
                wa.linked_facility_sub_type IN (SELECT sub_type FROM crude_facility_subtypes)
            ELSE
                COALESCE(wb.fluid, '') LIKE 'CR-%'
                OR COALESCE(wb.fluid, '') LIKE '%OIL%'
                OR COALESCE(wb.well_type, '') LIKE '%OIL%'
                OR COALESCE(wa.well_status_fluid, '') LIKE 'CR-%'
                OR LOWER(COALESCE(ll.well_completion_type, '')) LIKE '%oil%'
        END AS has_crude_evidence,
        CASE
            WHEN wa.linked_facility_sub_type IS NOT NULL THEN
                wa.linked_facility_sub_type NOT IN (SELECT sub_type FROM crude_facility_subtypes)
            ELSE
                COALESCE(wb.fluid, '') LIKE 'GAS%'
                OR COALESCE(wb.fluid, '') LIKE '%WAT%'
                OR COALESCE(wb.well_type, '') IN ('GAS', 'WAT', 'WATER', 'INJ')
                OR COALESCE(wa.well_status_fluid, '') LIKE 'GAS%'
        END AS has_non_crude_evidence
    FROM spud_activity sa
    LEFT JOIN wells_base wb ON wb.uwi = sa.uwi
    LEFT JOIN well_attr wa ON wa.uwi = sa.uwi
    LEFT JOIN latest_licence ll ON ll.uwi = sa.uwi
    CROSS JOIN params p
    WHERE sa.uwi IS NOT NULL
      AND CAST(sa.spud_date AS DATE) >= p.report_cutoff
      AND CAST(sa.spud_date AS DATE) <= p.as_of_date
),

spud_signal AS (
    SELECT
        'WELL:' || sc.uwi AS opportunity_id,
        'WELL' AS opportunity_type,
        sc.uwi,
        sc.linked_facility_id,
        CASE
            WHEN sc.has_crude_evidence THEN 'SPUD_CRUDE_LIKELY'
            ELSE 'SPUD_UNKNOWN_FLUID'
        END AS signal_type,
        sc.spud_date AS signal_date,
        CASE
            WHEN sc.has_crude_evidence THEN 60
            ELSE 20
        END AS signal_priority,
        CASE
            WHEN sc.has_crude_evidence THEN 'LIKELY'
            ELSE 'WATCHLIST'
        END AS confidence,
        CASE
            WHEN sc.has_crude_evidence THEN 'Call early; drilling has started and crude evidence exists.'
            ELSE 'Watch only until fluid, status, or licence data confirms crude.'
        END AS recommended_action,
        sc.target_formation AS source_detail
    FROM spud_candidates sc
    WHERE sc.has_crude_evidence
       OR NOT sc.has_non_crude_evidence
),

status_signal AS (
    SELECT
        'WELL:' || sc.uwi AS opportunity_id,
        'WELL' AS opportunity_type,
        sc.uwi,
        wa.linked_facility_id,
        'ACTIVE_CRUDE_STATUS' AS signal_type,
        CAST(sc.event_date AS DATE) AS signal_date,
        80 AS signal_priority,
        'HIGH' AS confidence,
        'Call operator; well reached active crude status before production data lands.' AS recommended_action,
        sc.new_status AS source_detail
    FROM status_changes sc
    LEFT JOIN well_attr wa ON wa.uwi = sc.uwi
    CROSS JOIN params p
    WHERE sc.uwi IS NOT NULL
      AND CAST(sc.event_date AS DATE) >= p.report_cutoff
      AND CAST(sc.event_date AS DATE) <= p.as_of_date
      AND (
          UPPER(sc.new_status) LIKE 'CR-OIL%'
          OR UPPER(sc.new_status) LIKE 'CR-BIT%'
      )
      AND UPPER(sc.new_status) NOT LIKE '%ABD%'
      AND UPPER(sc.new_status) NOT LIKE '%ABAN%'
      AND UPPER(sc.new_status) NOT LIKE '%SUSP%'
),

release_signal AS (
    SELECT
        'WELL:' || re.uwi AS opportunity_id,
        'WELL' AS opportunity_type,
        re.uwi,
        wa.linked_facility_id,
        'CONFIDENTIAL_RELEASE' AS signal_type,
        re.release_date AS signal_date,
        90 AS signal_priority,
        'HIGH' AS confidence,
        'Research immediately; a previously hidden well is now visible.' AS recommended_action,
        'confidential release' AS source_detail
    FROM release_events re
    LEFT JOIN well_attr wa ON wa.uwi = re.uwi
    CROSS JOIN params p
    WHERE re.release_date >= p.report_cutoff
      AND re.release_date <= p.as_of_date
),

new_battery_first_oil_signal AS (
    SELECT
        'BATTERY:' || g.linked_facility_id || ':' || STRFTIME(g.first_oil_month, '%Y-%m') AS opportunity_id,
        'BATTERY' AS opportunity_type,
        CAST(NULL AS VARCHAR) AS uwi,
        g.linked_facility_id,
        'NEW_BATTERY_FIRST_OIL' AS signal_type,
        g.first_oil_month AS signal_date,
        95 AS signal_priority,
        'CONFIRMED' AS confidence,
        'Treat as one new pad/battery opportunity; review constituent wells and facility reach.' AS recommended_action,
        g.well_count::VARCHAR || ' wells first produced oil at this facility in ' || STRFTIME(g.first_oil_month, '%Y-%m') AS source_detail
    FROM battery_first_oil_groups g
),

first_oil_signal AS (
    SELECT
        'WELL:' || fo.uwi AS opportunity_id,
        'WELL' AS opportunity_type,
        fo.uwi,
        wa.linked_facility_id,
        'FIRST_CONFIRMED_OIL' AS signal_type,
        fo.first_oil_month AS signal_date,
        100 AS signal_priority,
        'CONFIRMED' AS confidence,
        'Call with production-backed crude marketing offer.' AS recommended_action,
        'first oil production month' AS source_detail
    FROM first_oil fo
    LEFT JOIN well_attr wa ON wa.uwi = fo.uwi
    LEFT JOIN battery_first_oil_members bm
      ON bm.uwi = fo.uwi
     AND bm.first_oil_month = fo.first_oil_month
    CROSS JOIN params p
    WHERE fo.first_oil_month >= p.production_cutoff
      AND bm.uwi IS NULL
),

production_restart_signal AS (
    SELECT
        'WELL:' || pl.uwi AS opportunity_id,
        'WELL' AS opportunity_type,
        pl.uwi,
        wa.linked_facility_id,
        'PRODUCTION_RESTART' AS signal_type,
        pl.prod_month AS signal_date,
        75 AS signal_priority,
        'CONFIRMED' AS confidence,
        'Review for reactivation or changing crude takeaway needs.' AS recommended_action,
        'oil restarted after three low/zero months' AS source_detail
    FROM production_lag pl
    LEFT JOIN first_oil fo ON fo.uwi = pl.uwi
    LEFT JOIN well_attr wa ON wa.uwi = pl.uwi
    CROSS JOIN params p
    WHERE pl.prod_month >= p.production_cutoff
      AND pl.oil_prod_vol >= p.oil_threshold_m3
      AND pl.oil_prev_1 < p.oil_threshold_m3
      AND pl.oil_prev_2 < p.oil_threshold_m3
      AND pl.oil_prev_3 < p.oil_threshold_m3
      AND fo.first_oil_month < pl.prod_month
),

production_step_change_signal AS (
    SELECT
        'WELL:' || pl.uwi AS opportunity_id,
        'WELL' AS opportunity_type,
        pl.uwi,
        wa.linked_facility_id,
        'PRODUCTION_STEP_CHANGE' AS signal_type,
        pl.prod_month AS signal_date,
        70 AS signal_priority,
        'CONFIRMED' AS confidence,
        'Review volume increase and downstream facility path.' AS recommended_action,
        'oil >= 2x prior three-month average' AS source_detail
    FROM production_lag pl
    LEFT JOIN first_oil fo ON fo.uwi = pl.uwi
    LEFT JOIN well_attr wa ON wa.uwi = pl.uwi
    CROSS JOIN params p
    WHERE pl.prod_month >= p.production_cutoff
      AND pl.oil_prod_vol >= 10
      AND COALESCE(pl.oil_prior_3mo_avg, 0) >= p.oil_threshold_m3
      AND pl.oil_prod_vol >= 2 * pl.oil_prior_3mo_avg
      AND fo.first_oil_month < pl.prod_month
),

all_signals AS (
    SELECT * FROM licence_signal
    UNION ALL SELECT * FROM spud_signal
    UNION ALL SELECT * FROM status_signal
    UNION ALL SELECT * FROM release_signal
    UNION ALL SELECT * FROM new_battery_first_oil_signal
    UNION ALL SELECT * FROM first_oil_signal
    UNION ALL SELECT * FROM production_restart_signal
    UNION ALL SELECT * FROM production_step_change_signal
),

deduped_signals AS (
    SELECT
        opportunity_id,
        opportunity_type,
        uwi,
        linked_facility_id,
        signal_type,
        signal_date,
        signal_priority,
        confidence,
        recommended_action,
        source_detail
    FROM (
        SELECT
            s.*,
            ROW_NUMBER() OVER (
                PARTITION BY s.opportunity_id, s.signal_type
                ORDER BY s.signal_date DESC, s.signal_priority DESC
            ) AS rn
        FROM all_signals s
    )
    WHERE rn = 1
),

ranked_signals AS (
    SELECT
        ds.*,
        ROW_NUMBER() OVER (
            PARTITION BY ds.opportunity_id
            ORDER BY ds.signal_priority DESC, ds.signal_date DESC, ds.signal_type
        ) AS primary_rank
    FROM deduped_signals ds
),

signal_rollup AS (
    SELECT
        opportunity_id,
        MAX(signal_date) AS latest_signal_date,
        STRING_AGG(signal_type, ', ' ORDER BY signal_priority DESC, signal_date DESC) AS recent_signals,
        MAX(CASE WHEN primary_rank = 1 THEN signal_type END) AS primary_signal,
        MAX(CASE WHEN primary_rank = 1 THEN signal_priority END) AS primary_priority,
        MAX(CASE WHEN primary_rank = 1 THEN confidence END) AS confidence,
        MAX(CASE WHEN primary_rank = 1 THEN recommended_action END) AS recommended_action,
        MAX(CASE WHEN primary_rank = 1 THEN source_detail END) AS primary_source_detail
    FROM ranked_signals
    GROUP BY opportunity_id
),

operator_resolution_keys AS MATERIALIZED (
    SELECT
        opportunity_id,
        CASE
            WHEN raw_operator IS NOT NULL
             AND TRIM(raw_operator) != ''
             AND LOWER(TRIM(raw_operator)) NOT IN ('nan', 'none')
            THEN TRIM(raw_operator)
        END AS raw_operator_key,
        CASE
            WHEN raw_operator IS NOT NULL
             AND TRIM(raw_operator) != ''
             AND LOWER(TRIM(raw_operator)) NOT IN ('nan', 'none')
            THEN UPPER(TRIM(raw_operator))
        END AS raw_operator_norm,
        CASE
            WHEN linked_facility_operator_baid IS NOT NULL
             AND TRIM(linked_facility_operator_baid) != ''
             AND LOWER(TRIM(linked_facility_operator_baid)) NOT IN ('nan', 'none')
            THEN TRIM(linked_facility_operator_baid)
        END AS linked_facility_operator_baid_key,
        CASE
            WHEN linked_facility_operator_legal_name IS NOT NULL
             AND TRIM(linked_facility_operator_legal_name) != ''
             AND LOWER(TRIM(linked_facility_operator_legal_name)) NOT IN ('nan', 'none')
            THEN UPPER(TRIM(linked_facility_operator_legal_name))
        END AS linked_facility_operator_legal_name_norm,
        CASE
            WHEN display_operator IS NOT NULL
             AND TRIM(display_operator) != ''
             AND LOWER(TRIM(display_operator)) NOT IN ('nan', 'none')
            THEN UPPER(TRIM(display_operator))
        END AS display_operator_norm
    FROM opportunity_context
),

operator_lookup_source AS MATERIALIZED (
    SELECT
        identifier_kind,
        TRIM(identifier_value) AS identifier_value,
        UPPER(TRIM(identifier_value)) AS identifier_value_norm,
        operator_id,
        canonical_name,
        short_name,
        confidence,
        CASE confidence
            WHEN 'manual' THEN 4
            WHEN 'high' THEN 3
            WHEN 'medium' THEN 2
            WHEN 'low' THEN 1
            ELSE 0
        END AS confidence_rank
    FROM operators_resolved
    WHERE identifier_value IS NOT NULL
      AND TRIM(identifier_value) != ''
),

operator_lookup_exact AS MATERIALIZED (
    SELECT
        identifier_kind,
        identifier_value,
        operator_id,
        canonical_name,
        short_name,
        confidence
    FROM (
        SELECT
            l.*,
            ROW_NUMBER() OVER (
                PARTITION BY l.identifier_kind, l.identifier_value
                ORDER BY l.confidence_rank DESC, l.operator_id
            ) AS rn
        FROM operator_lookup_source l
    )
    WHERE rn = 1
),

operator_lookup_norm AS MATERIALIZED (
    SELECT
        identifier_kind,
        identifier_value_norm,
        identifier_value,
        operator_id,
        canonical_name,
        short_name,
        confidence
    FROM (
        SELECT
            l.*,
            ROW_NUMBER() OVER (
                PARTITION BY l.identifier_kind, l.identifier_value_norm
                ORDER BY l.confidence_rank DESC, l.operator_id
            ) AS rn
        FROM operator_lookup_source l
        WHERE l.identifier_kind IN (
            'canonical_name',
            'facility_operator_name',
            'well_licensee',
            'sk_legal_name',
            'operator_short_name'
        )
    )
    WHERE rn = 1
),

resolved_operator AS (
    SELECT
        k.opportunity_id,
        COALESCE(
            raw_ab5.operator_id,
            raw_ab4.operator_id,
            fac_baid_ab4.operator_id,
            fac_baid_ab5.operator_id,
            fac_legal_canon.operator_id,
            fac_legal_fac.operator_id,
            fac_legal_sk.operator_id,
            display_canon.operator_id,
            display_well.operator_id,
            display_sk.operator_id,
            display_fac.operator_id,
            display_short.operator_id,
            raw_canon.operator_id,
            raw_well.operator_id,
            raw_sk.operator_id
        ) AS operator_id,
        COALESCE(
            raw_ab5.canonical_name,
            raw_ab4.canonical_name,
            fac_baid_ab4.canonical_name,
            fac_baid_ab5.canonical_name,
            fac_legal_canon.canonical_name,
            fac_legal_fac.canonical_name,
            fac_legal_sk.canonical_name,
            display_canon.canonical_name,
            display_well.canonical_name,
            display_sk.canonical_name,
            display_fac.canonical_name,
            display_short.canonical_name,
            raw_canon.canonical_name,
            raw_well.canonical_name,
            raw_sk.canonical_name
        ) AS canonical_name,
        COALESCE(
            raw_ab5.short_name,
            raw_ab4.short_name,
            fac_baid_ab4.short_name,
            fac_baid_ab5.short_name,
            fac_legal_canon.short_name,
            fac_legal_fac.short_name,
            fac_legal_sk.short_name,
            display_canon.short_name,
            display_well.short_name,
            display_sk.short_name,
            display_fac.short_name,
            display_short.short_name,
            raw_canon.short_name,
            raw_well.short_name,
            raw_sk.short_name
        ) AS short_name,
        COALESCE(
            raw_ab5.identifier_kind,
            raw_ab4.identifier_kind,
            fac_baid_ab4.identifier_kind,
            fac_baid_ab5.identifier_kind,
            fac_legal_canon.identifier_kind,
            fac_legal_fac.identifier_kind,
            fac_legal_sk.identifier_kind,
            display_canon.identifier_kind,
            display_well.identifier_kind,
            display_sk.identifier_kind,
            display_fac.identifier_kind,
            display_short.identifier_kind,
            raw_canon.identifier_kind,
            raw_well.identifier_kind,
            raw_sk.identifier_kind
        ) AS operator_resolution_kind,
        COALESCE(
            raw_ab5.identifier_value,
            raw_ab4.identifier_value,
            fac_baid_ab4.identifier_value,
            fac_baid_ab5.identifier_value,
            fac_legal_canon.identifier_value,
            fac_legal_fac.identifier_value,
            fac_legal_sk.identifier_value,
            display_canon.identifier_value,
            display_well.identifier_value,
            display_sk.identifier_value,
            display_fac.identifier_value,
            display_short.identifier_value,
            raw_canon.identifier_value,
            raw_well.identifier_value,
            raw_sk.identifier_value
        ) AS operator_resolution_value,
        COALESCE(
            raw_ab5.confidence,
            raw_ab4.confidence,
            fac_baid_ab4.confidence,
            fac_baid_ab5.confidence,
            fac_legal_canon.confidence,
            fac_legal_fac.confidence,
            fac_legal_sk.confidence,
            display_canon.confidence,
            display_well.confidence,
            display_sk.confidence,
            display_fac.confidence,
            display_short.confidence,
            raw_canon.confidence,
            raw_well.confidence,
            raw_sk.confidence
        ) AS operator_resolution_confidence
    FROM operator_resolution_keys k
    LEFT JOIN operator_lookup_exact raw_ab5
      ON raw_ab5.identifier_kind = 'ab_ba_code_5'
     AND raw_ab5.identifier_value = k.raw_operator_key
    LEFT JOIN operator_lookup_exact raw_ab4
      ON raw_ab4.identifier_kind = 'ab_ba_code_4'
     AND raw_ab4.identifier_value = k.raw_operator_key
    LEFT JOIN operator_lookup_exact fac_baid_ab4
      ON fac_baid_ab4.identifier_kind = 'ab_ba_code_4'
     AND fac_baid_ab4.identifier_value = k.linked_facility_operator_baid_key
    LEFT JOIN operator_lookup_exact fac_baid_ab5
      ON fac_baid_ab5.identifier_kind = 'ab_ba_code_5'
     AND fac_baid_ab5.identifier_value = k.linked_facility_operator_baid_key
    LEFT JOIN operator_lookup_norm fac_legal_canon
      ON fac_legal_canon.identifier_kind = 'canonical_name'
     AND fac_legal_canon.identifier_value_norm = k.linked_facility_operator_legal_name_norm
    LEFT JOIN operator_lookup_norm fac_legal_fac
      ON fac_legal_fac.identifier_kind = 'facility_operator_name'
     AND fac_legal_fac.identifier_value_norm = k.linked_facility_operator_legal_name_norm
    LEFT JOIN operator_lookup_norm fac_legal_sk
      ON fac_legal_sk.identifier_kind = 'sk_legal_name'
     AND fac_legal_sk.identifier_value_norm = k.linked_facility_operator_legal_name_norm
    LEFT JOIN operator_lookup_norm display_canon
      ON display_canon.identifier_kind = 'canonical_name'
     AND display_canon.identifier_value_norm = k.display_operator_norm
    LEFT JOIN operator_lookup_norm display_well
      ON display_well.identifier_kind = 'well_licensee'
     AND display_well.identifier_value_norm = k.display_operator_norm
    LEFT JOIN operator_lookup_norm display_sk
      ON display_sk.identifier_kind = 'sk_legal_name'
     AND display_sk.identifier_value_norm = k.display_operator_norm
    LEFT JOIN operator_lookup_norm display_fac
      ON display_fac.identifier_kind = 'facility_operator_name'
     AND display_fac.identifier_value_norm = k.display_operator_norm
    LEFT JOIN operator_lookup_norm display_short
      ON display_short.identifier_kind = 'operator_short_name'
     AND display_short.identifier_value_norm = k.display_operator_norm
    LEFT JOIN operator_lookup_norm raw_canon
      ON raw_canon.identifier_kind = 'canonical_name'
     AND raw_canon.identifier_value_norm = k.raw_operator_norm
    LEFT JOIN operator_lookup_norm raw_well
      ON raw_well.identifier_kind = 'well_licensee'
     AND raw_well.identifier_value_norm = k.raw_operator_norm
    LEFT JOIN operator_lookup_norm raw_sk
      ON raw_sk.identifier_kind = 'sk_legal_name'
     AND raw_sk.identifier_value_norm = k.raw_operator_norm
),

-- ============================================================================
-- Phase 1 additions: TEMI facilities + well proximity + operator enrichment.
-- Inlined as CTEs (not persisted) so the report stays read-only by default.
-- Promote to a `temi_facilities` seed table + `well_proximity` view +
-- ALTER TABLE operators columns in a follow-up if persistence is needed.
-- ============================================================================

temi_facilities AS (
    -- Source: seeds/temi_facilities.csv. Coords verified against
    -- _facilities_with_coords on 2026-05-25 except SKTMTT15003 which uses
    -- user-provided coordinates (warehouse had ~6km offset).
    SELECT * FROM (VALUES
        ('ABTM0000930', 'Cynthia Pembina Terminal',        'Bench Creek Resources Ltd.',  'AB', 53.310727, -115.619797),
        ('ABTM0112311', 'Rush Energy Services Inc.',       'Rush Energy Services Inc.',   'AB', 53.099561, -114.474046),
        ('ABTM0125201', 'Vermilion 15-16-051-11w5 Tm',     'Vermilion Energy Inc.',       'AB', 53.408441, -115.558574),
        ('ABTM0157119', 'Persist Wayne Oil Terminal',      'Persist Oil And Gas Inc.',    'AB', 51.403331, -112.914700),
        ('SKTMTT15003', 'Dulwich Terminal (COP D11A)',     'Marlin Resources Ltd.',       'SK', 53.165195, -109.703831)
    ) AS t(facility_id, facility_name, operator_company, province, lat, lon)
),

well_proximity_calc AS (
    -- Haversine distance from each well centroid to each TEMI facility.
    -- Earth radius 6371 km. Only wells with non-null centroids are scored.
    SELECT
        wc.uwi,
        tf.facility_id,
        (2.0 * 6371.0 * ASIN(SQRT(
            POW(SIN(RADIANS(tf.lat - wc.centroid_lat) / 2.0), 2)
            + COS(RADIANS(wc.centroid_lat))
              * COS(RADIANS(tf.lat))
              * POW(SIN(RADIANS(tf.lon - wc.centroid_lon) / 2.0), 2)
        ))) AS distance_km
    FROM well_coords wc
    CROSS JOIN temi_facilities tf
    WHERE wc.centroid_lat IS NOT NULL
      AND wc.centroid_lon IS NOT NULL
),

well_proximity AS MATERIALIZED (
    SELECT
        uwi,
        nearest_facility_id,
        distance_km,
        CASE
            WHEN distance_km < 25  THEN 'within_25'
            WHEN distance_km < 50  THEN 'within_50'
            WHEN distance_km < 100 THEN 'within_100'
            ELSE                        'beyond'
        END AS proximity_bucket
    FROM (
        SELECT
            uwi,
            facility_id AS nearest_facility_id,
            distance_km,
            ROW_NUMBER() OVER (PARTITION BY uwi ORDER BY distance_km ASC) AS rn
        FROM well_proximity_calc
    )
    WHERE rn = 1
),

-- Tunable thresholds for operator size tier.
tier_params AS (
    SELECT
        50000.0::DOUBLE AS lower_priority_threshold_m3
),

-- Production rows linked to canonical operator_id via wells.operator_code.
-- AB joins on wells.uwi; SK production uses normalized_uwi (uwi is null in
-- some SK rows). We union both paths and dedupe by (uwi, productionmonth).
operator_prod_linked AS MATERIALIZED (
    SELECT DISTINCT
        p.uwi,
        p.productionmonth,
        p.oil_prod_vol,
        ol.operator_id
    FROM production p
    JOIN wells w
      ON w.uwi = p.uwi
    JOIN operator_lookup_exact ol
      ON ol.identifier_kind IN ('ab_ba_code_5', 'ab_ba_code_4')
     AND ol.identifier_value = UPPER(TRIM(w.operator_code))
    WHERE p.oil_prod_vol IS NOT NULL
      AND p.oil_prod_vol > 0
      AND w.operator_code IS NOT NULL
),

operator_prod_bounds AS (
    SELECT
        MAX(CAST(productionmonth || '-01' AS DATE)) AS max_pmd
    FROM operator_prod_linked
),

operator_prod_trailing12 AS (
    SELECT
        opl.operator_id,
        ROUND(SUM(opl.oil_prod_vol) / 12.0, 2) AS avg_monthly_oil_m3
    FROM operator_prod_linked opl
    CROSS JOIN operator_prod_bounds b
    WHERE CAST(opl.productionmonth || '-01' AS DATE) >= b.max_pmd - INTERVAL 12 MONTH
    GROUP BY opl.operator_id
),

operator_first_oil_from_ctx AS (
    -- Per-operator first oil month, computed from wells currently visible in
    -- the opportunity radar. NOTE: we do NOT use MIN(production.productionmonth)
    -- because the production table only spans 2024-03 onward in the live
    -- warehouse — that floor would mis-flag every long-tenured operator as
    -- "first oil 2024-03". Using opportunity_context.first_oil_month captures
    -- the per-well first-oil signal which is the practical "new entrant"
    -- semantic the spotlight needs.
    SELECT
        ro.operator_id,
        MIN(oc.first_oil_month) AS operator_first_oil_month
    FROM resolved_operator ro
    JOIN opportunity_context oc ON oc.opportunity_id = ro.opportunity_id
    WHERE ro.operator_id IS NOT NULL
      AND oc.first_oil_month IS NOT NULL
    GROUP BY ro.operator_id
),

operator_enrichment AS (
    -- One row per operator_id with size-tier + new-operator flag.
    -- Anchored to MAX(productionmonth) (not CURRENT_DATE) because
    -- Petrinex production lags ~2 months.
    SELECT
        COALESCE(t.operator_id, fa.operator_id) AS operator_id,
        COALESCE(t.avg_monthly_oil_m3, 0.0)     AS avg_monthly_oil_m3,
        CASE
            WHEN COALESCE(t.avg_monthly_oil_m3, 0.0) >= tp.lower_priority_threshold_m3
                THEN 'lower_priority'
            ELSE 'priority'
        END AS operator_size_tier,
        fa.operator_first_oil_month,
        CASE
            WHEN fa.operator_first_oil_month IS NOT NULL
             AND fa.operator_first_oil_month >= b.max_pmd - INTERVAL 12 MONTH
            THEN TRUE
            ELSE FALSE
        END AS is_new_operator
    FROM operator_first_oil_from_ctx fa
    FULL OUTER JOIN operator_prod_trailing12 t
      ON t.operator_id = fa.operator_id
    CROSS JOIN tier_params tp
    CROSS JOIN operator_prod_bounds b
)

SELECT
    sr.primary_signal,
    oc.opportunity_type,
    oc.opportunity_id,
    sr.confidence,
    sr.recommended_action,
    sr.latest_signal_date,
    sr.recent_signals,
    CONCAT_WS(
        ' | ',
        CASE
            WHEN oc.licence_date IS NOT NULL
            THEN 'LICENSED ' || DATE_DIFF('day', oc.licence_date, p.as_of_date)::VARCHAR || 'd'
        END,
        CASE
            WHEN oc.spud_date IS NOT NULL
            THEN 'SPUDDED ' || DATE_DIFF('day', oc.spud_date, p.as_of_date)::VARCHAR || 'd'
        END,
        CASE
            WHEN oc.status_active_date IS NOT NULL
            THEN 'ACTIVE ' || DATE_DIFF('day', oc.status_active_date, p.as_of_date)::VARCHAR || 'd'
        END,
        CASE
            WHEN oc.confidential_release_date IS NOT NULL
            THEN 'RELEASED ' || DATE_DIFF('day', oc.confidential_release_date, p.as_of_date)::VARCHAR || 'd'
        END,
        CASE
            WHEN oc.first_oil_month IS NOT NULL
            THEN 'FIRST OIL ' || STRFTIME(oc.first_oil_month, '%Y-%m')
        END
    ) AS lifecycle_badges,

    oc.uwi,
    oc.wells_in_pad,
    oc.well_count,
    oc.well_name,
    COALESCE(ro.canonical_name, oc.display_operator) AS display_operator,
    ro.operator_id,
    ro.short_name AS operator_short_name,
    oc.display_operator AS source_display_operator,
    ro.operator_resolution_kind,
    ro.operator_resolution_value,
    ro.operator_resolution_confidence,
    oc.raw_operator,
    oc.well_licensee,
    oc.province,
    oc.licence,
    oc.lic_status,
    oc.fluid,
    oc.mode,
    oc.well_type,
    oc.structure,
    oc.horizontal_drill,
    oc.field,
    oc.field_name,
    oc.pool_deposit,
    oc.formation,
    oc.finished_drill_date,

    oc.licence_date,
    oc.spud_date,
    oc.status_active_date,
    oc.confidential_release_date,
    oc.first_oil_month,
    oc.latest_prod_month,
    oc.latest_oil_m3,
    oc.latest_gas_e3m3,
    oc.latest_water_m3,
    oc.latest_cond_m3,
    oc.latest_hours_on,

    oc.linked_facility_id,
    oc.linked_facility_name,
    oc.linked_facility_type,
    oc.linked_facility_sub_type,
    oc.linked_facility_sub_type_desc,
    oc.linked_facility_operator_baid,
    oc.linked_facility_operator_legal_name,
    oc.linked_facility_province_state,
    oc.linked_facility_identifier,
    oc.linked_start_date,
    oc.is_crude_connected,
    oc.crude_terminal_status,
    oc.nearest_hub_id,
    oc.hops_to_nearest_hub,
    oc.crude_hub_reach,
    oc.centroid_lat,
    oc.centroid_lon,

    -- Phase 1 additions: operator size tier + well proximity to TEMI facilities.
    oe.operator_size_tier,
    oe.avg_monthly_oil_m3,
    oe.is_new_operator,
    oe.operator_first_oil_month,
    wp.nearest_facility_id,
    wp.distance_km,
    COALESCE(wp.proximity_bucket, 'unknown') AS proximity_bucket,

    sr.primary_source_detail
FROM signal_rollup sr
CROSS JOIN params p
JOIN opportunity_context oc ON oc.opportunity_id = sr.opportunity_id
LEFT JOIN resolved_operator ro ON ro.opportunity_id = oc.opportunity_id
LEFT JOIN operator_enrichment oe ON oe.operator_id = ro.operator_id
LEFT JOIN well_proximity wp ON wp.uwi = oc.uwi
ORDER BY
    sr.primary_priority DESC,
    sr.latest_signal_date DESC,
    COALESCE(oc.latest_oil_m3, 0) DESC;

-- Validation checks to run after creating the view:
--
-- SELECT primary_signal, opportunity_type, confidence, COUNT(*)
-- FROM new_crude_opportunity_report
-- GROUP BY primary_signal, opportunity_type, confidence
-- ORDER BY COUNT(*) DESC;
--
-- SELECT COUNT(*) AS opportunity_rows,
--        COUNT(*) FILTER (WHERE opportunity_type = 'WELL') AS well_rows,
--        COUNT(*) FILTER (WHERE opportunity_type = 'BATTERY') AS battery_rows,
--        SUM(well_count) AS represented_wells
-- FROM new_crude_opportunity_report;
--
-- SELECT SUM(well_count) AS wells_represented_by_battery_rows,
--        COUNT(*) AS battery_opportunities,
--        SUM(well_count) - COUNT(*) AS rows_removed_by_battery_collapse
-- FROM new_crude_opportunity_report
-- WHERE primary_signal = 'NEW_BATTERY_FIRST_OIL';
--
-- SELECT province, COUNT(*) AS opportunities, COUNT(*) FILTER (
--     WHERE centroid_lat IS NOT NULL AND centroid_lon IS NOT NULL
-- ) AS opportunities_with_coordinates
-- FROM new_crude_opportunity_report
-- GROUP BY province
-- ORDER BY province;
