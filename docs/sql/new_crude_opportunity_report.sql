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

operator_identifier_candidates AS (
    SELECT opportunity_id, 10 AS priority, 'ab_ba_code_5' AS identifier_kind, raw_operator AS identifier_value
    FROM opportunity_context
    UNION ALL
    SELECT opportunity_id, 20, 'ab_ba_code_4', raw_operator
    FROM opportunity_context
    UNION ALL
    SELECT opportunity_id, 30, 'ab_ba_code_4', linked_facility_operator_baid
    FROM opportunity_context
    UNION ALL
    SELECT opportunity_id, 40, 'ab_ba_code_5', linked_facility_operator_baid
    FROM opportunity_context
    UNION ALL
    SELECT opportunity_id, 50, 'canonical_name', linked_facility_operator_legal_name
    FROM opportunity_context
    UNION ALL
    SELECT opportunity_id, 55, 'facility_operator_name', linked_facility_operator_legal_name
    FROM opportunity_context
    UNION ALL
    SELECT opportunity_id, 60, 'sk_legal_name', linked_facility_operator_legal_name
    FROM opportunity_context
    UNION ALL
    SELECT opportunity_id, 70, 'canonical_name', display_operator
    FROM opportunity_context
    UNION ALL
    SELECT opportunity_id, 80, 'well_licensee', display_operator
    FROM opportunity_context
    UNION ALL
    SELECT opportunity_id, 90, 'sk_legal_name', display_operator
    FROM opportunity_context
    UNION ALL
    SELECT opportunity_id, 100, 'facility_operator_name', display_operator
    FROM opportunity_context
    UNION ALL
    SELECT opportunity_id, 110, 'operator_short_name', display_operator
    FROM opportunity_context
    UNION ALL
    SELECT opportunity_id, 120, 'canonical_name', raw_operator
    FROM opportunity_context
    UNION ALL
    SELECT opportunity_id, 130, 'well_licensee', raw_operator
    FROM opportunity_context
    UNION ALL
    SELECT opportunity_id, 140, 'sk_legal_name', raw_operator
    FROM opportunity_context
),

resolved_operator AS (
    SELECT * EXCLUDE (rn)
    FROM (
        SELECT
            c.opportunity_id,
            r.operator_id,
            r.canonical_name,
            r.short_name,
            r.identifier_kind AS operator_resolution_kind,
            r.identifier_value AS operator_resolution_value,
            r.confidence AS operator_resolution_confidence,
            ROW_NUMBER() OVER (
                PARTITION BY c.opportunity_id
                ORDER BY c.priority, r.confidence DESC, r.operator_id
            ) AS rn
        FROM operator_identifier_candidates c
        JOIN operators_resolved r
          ON r.identifier_kind = c.identifier_kind
         AND r.identifier_value = c.identifier_value
        WHERE c.identifier_value IS NOT NULL
          AND TRIM(c.identifier_value) != ''
          AND LOWER(TRIM(c.identifier_value)) NOT IN ('nan', 'none')
    )
    WHERE rn = 1
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
    sr.primary_source_detail
FROM signal_rollup sr
CROSS JOIN params p
JOIN opportunity_context oc ON oc.opportunity_id = sr.opportunity_id
LEFT JOIN resolved_operator ro ON ro.opportunity_id = oc.opportunity_id
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
