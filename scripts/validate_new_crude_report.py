#!/usr/bin/env python3
"""Validate the New Crude Opportunity report against a TrendEnergy DuckDB."""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any

import duckdb

REQUIRED_TABLES: dict[str, set[str]] = {
    "wells_current": {
        "uwi",
        "name",
        "licence",
        "lic_status",
        "licensee",
        "fluid",
        "mode",
        "type",
        "structure",
        "centroid_lat",
        "centroid_lon",
        "province",
        "release_date",
    },
    "well_attributes": {
        "uwi",
        "field",
        "field_name",
        "pool_deposit",
        "formation",
        "horizontal_drill",
        "finished_drill_date",
        "spud_date",
        "licence_status_date",
        "well_status_date",
        "well_status_fluid",
        "well_status_mode",
        "linked_facility_id",
        "linked_facility_type",
        "linked_facility_sub_type",
        "linked_facility_sub_type_desc",
        "linked_facility_name",
        "linked_facility_operator_baid",
        "linked_facility_operator_legal_name",
        "linked_facility_province_state",
        "linked_facility_identifier",
        "linked_start_date",
    },
    "production_history": {
        "uwi",
        "prod_month",
        "oil_prod_vol",
        "gas_prod_vol",
        "water_prod_vol",
        "cond_prod_vol",
        "hours_on",
    },
    "well_licences": {"uwi", "well_name", "licensee", "issue_date", "well_completion_type"},
    "spud_activity": {"uwi", "spud_date", "target_formation"},
    "status_changes": {"uwi", "event_date", "new_status"},
    "confidential_wells": {"uwi", "release_date"},
    "facility_crude_reach": {
        "facility_id",
        "is_crude_connected",
        "crude_terminal_status",
        "nearest_hub_id",
        "hops_to_nearest_hub",
    },
    "facilities_enriched": {
        "facility_id",
        "facility_name",
        "operator_name",
        "province",
        "centroid_lat",
        "centroid_lon",
    },
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run and summarize docs/sql/new_crude_opportunity_report.sql."
    )
    parser.add_argument(
        "--db",
        default="/data/aer_data.duckdb",
        help="DuckDB path. Default: /data/aer_data.duckdb",
    )
    parser.add_argument(
        "--sql",
        default="docs/sql/new_crude_opportunity_report.sql",
        help="Report SQL path. Default: docs/sql/new_crude_opportunity_report.sql",
    )
    parser.add_argument(
        "--sample-limit",
        type=int,
        default=20,
        help="Sample rows to print from the report. Default: 20",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit JSON instead of human-readable text.",
    )
    parser.add_argument(
        "--write",
        action="store_true",
        help="Create/replace a persistent view instead of a temporary view.",
    )
    return parser.parse_args()


def rows_as_dicts(cursor: duckdb.DuckDBPyConnection) -> list[dict[str, Any]]:
    columns = [desc[0] for desc in cursor.description]
    return [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]


def get_columns(conn: duckdb.DuckDBPyConnection, table_name: str) -> set[str] | None:
    try:
        return {row[1] for row in conn.execute(f"PRAGMA table_info('{table_name}')").fetchall()}
    except duckdb.CatalogException:
        return None


def preflight(conn: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    missing_tables: list[str] = []
    missing_columns: dict[str, list[str]] = {}

    for table_name, required_columns in REQUIRED_TABLES.items():
        existing_columns = get_columns(conn, table_name)
        if existing_columns is None:
            missing_tables.append(table_name)
            continue

        missing = sorted(required_columns - existing_columns)
        if missing:
            missing_columns[table_name] = missing

    return {
        "ok": not missing_tables and not missing_columns,
        "missing_tables": missing_tables,
        "missing_columns": missing_columns,
    }


def load_report_sql(sql_path: Path, *, persistent: bool) -> str:
    sql = sql_path.read_text(encoding="utf-8")
    sql = sql.split("-- Validation checks to run after creating the view:")[0].strip()
    if persistent:
        return sql

    return re.sub(
        r"CREATE\s+OR\s+REPLACE\s+VIEW\s+new_crude_opportunity_report\s+AS",
        "CREATE OR REPLACE TEMP VIEW new_crude_opportunity_report AS",
        sql,
        count=1,
        flags=re.IGNORECASE,
    )


def run_report(conn: duckdb.DuckDBPyConnection, sql: str, sample_limit: int) -> dict[str, Any]:
    conn.execute(sql)

    counts_by_signal = rows_as_dicts(
        conn.execute(
            """
            SELECT primary_signal, opportunity_type, confidence,
                   COUNT(*) AS opportunity_rows,
                   SUM(well_count) AS represented_wells
            FROM new_crude_opportunity_report
            GROUP BY primary_signal, opportunity_type, confidence
            ORDER BY opportunity_rows DESC
            """
        )
    )

    opportunity_totals = rows_as_dicts(
        conn.execute(
            """
            SELECT COUNT(*) AS opportunity_rows,
                   COUNT(*) FILTER (WHERE opportunity_type = 'WELL') AS well_rows,
                   COUNT(*) FILTER (WHERE opportunity_type = 'BATTERY') AS battery_rows,
                   SUM(well_count) AS represented_wells
            FROM new_crude_opportunity_report
            """
        )
    )[0]

    battery_collapse = rows_as_dicts(
        conn.execute(
            """
            SELECT COALESCE(SUM(well_count), 0) AS wells_represented_by_battery_rows,
                   COUNT(*) AS battery_opportunities,
                   COALESCE(SUM(well_count), 0) - COUNT(*) AS rows_removed_by_battery_collapse
            FROM new_crude_opportunity_report
            WHERE primary_signal = 'NEW_BATTERY_FIRST_OIL'
            """
        )
    )[0]

    first_oil_linkage = rows_as_dicts(
        conn.execute(
            """
            WITH first_oil AS (
                SELECT uwi, MIN(prod_month) AS first_oil_month
                FROM production_history
                WHERE oil_prod_vol >= 1.0
                GROUP BY uwi
            )
            SELECT COUNT(*) AS first_oil_wells,
                   COUNT(*) FILTER (WHERE wa.linked_facility_id IS NOT NULL) AS with_facility,
                   COUNT(*) FILTER (
                       WHERE wa.linked_facility_sub_type IN ('322','311','341','342','344','506')
                   ) AS crude_facility_linked,
                   COUNT(*) FILTER (
                       WHERE wa.linked_facility_sub_type IN ('322','341','342','344','506')
                   ) AS multiwell_crude_facility_linked
            FROM first_oil fo
            LEFT JOIN well_attributes wa USING (uwi)
            WHERE fo.first_oil_month >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL 3 MONTH
            """
        )
    )[0]

    battery_subtypes = rows_as_dicts(
        conn.execute(
            """
            SELECT linked_facility_sub_type,
                   linked_facility_sub_type_desc,
                   COUNT(*) AS battery_opportunities,
                   SUM(well_count) AS represented_wells
            FROM new_crude_opportunity_report
            WHERE primary_signal = 'NEW_BATTERY_FIRST_OIL'
            GROUP BY linked_facility_sub_type, linked_facility_sub_type_desc
            ORDER BY represented_wells DESC
            """
        )
    )

    sample_rows = rows_as_dicts(
        conn.execute(
            """
            SELECT primary_signal, opportunity_type, well_count, well_name,
                   display_operator, province, linked_facility_id,
                   linked_facility_name, linked_facility_sub_type_desc,
                   first_oil_month, latest_oil_m3, crude_hub_reach,
                   wells_in_pad
            FROM new_crude_opportunity_report
            ORDER BY primary_signal = 'NEW_BATTERY_FIRST_OIL' DESC,
                     latest_signal_date DESC,
                     latest_oil_m3 DESC NULLS LAST
            LIMIT ?
            """,
            [sample_limit],
        )
    )

    return {
        "counts_by_signal": counts_by_signal,
        "opportunity_totals": opportunity_totals,
        "battery_collapse": battery_collapse,
        "first_oil_linkage": first_oil_linkage,
        "battery_subtypes": battery_subtypes,
        "sample_rows": sample_rows,
    }


def print_text(result: dict[str, Any]) -> None:
    print("Preflight: OK")

    print("\nCounts by signal")
    for row in result["counts_by_signal"]:
        print(
            f"- {row['primary_signal']} / {row['opportunity_type']} / "
            f"{row['confidence']}: {row['opportunity_rows']} rows, "
            f"{row['represented_wells']} wells"
        )

    print("\nOpportunity totals")
    print(result["opportunity_totals"])

    print("\nBattery collapse")
    print(result["battery_collapse"])

    print("\nFirst-oil linkage diagnostic")
    print(result["first_oil_linkage"])

    print("\nBattery subtype breakdown")
    if result["battery_subtypes"]:
        for row in result["battery_subtypes"]:
            print(f"- {row}")
    else:
        print("- No NEW_BATTERY_FIRST_OIL rows")

    print("\nSample rows")
    for row in result["sample_rows"]:
        print(f"- {row}")


def main() -> int:
    args = parse_args()
    db_path = Path(args.db)
    sql_path = Path(args.sql)

    if not db_path.exists():
        print(f"DuckDB not found: {db_path}", file=sys.stderr)
        return 2
    if not sql_path.exists():
        print(f"Report SQL not found: {sql_path}", file=sys.stderr)
        return 2

    conn = duckdb.connect(str(db_path), read_only=not args.write)
    try:
        preflight_result = preflight(conn)
        if not preflight_result["ok"]:
            payload = {"preflight": preflight_result}
            if args.json:
                print(json.dumps(payload, indent=2, default=str))
            else:
                print("Preflight failed.")
                print(json.dumps(preflight_result, indent=2, default=str))
            return 2

        sql = load_report_sql(sql_path, persistent=args.write)
        report_result = run_report(conn, sql, args.sample_limit)
        payload = {"preflight": preflight_result, "report": report_result}
        if args.json:
            print(json.dumps(payload, indent=2, default=str))
        else:
            print_text(report_result)
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
