#!/usr/bin/env python3
"""Export TrendEnergy New Crude weekly/monthly report workbooks."""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

import duckdb
import pandas as pd

REPORT_SHEETS: list[tuple[str, str]] = [
    ("weekly_contact_queue", "new_crude_weekly_contact_queue"),
    ("weekly_operator_summary", "new_crude_operator_weekly_summary"),
    ("monthly_confirmed", "new_crude_monthly_confirmed_report"),
    ("monthly_operator_summary", "new_crude_operator_monthly_summary"),
    ("lifecycle_timeline", "new_crude_lifecycle_timeline"),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build New Crude report views and export a multi-sheet XLSX workbook."
    )
    parser.add_argument(
        "--db",
        default="/data/aer_data.duckdb",
        help="DuckDB path. Default: /data/aer_data.duckdb",
    )
    parser.add_argument(
        "--opportunity-sql",
        default="docs/sql/new_crude_opportunity_report.sql",
        help="Base opportunity radar SQL path.",
    )
    parser.add_argument(
        "--derived-sql",
        default="docs/sql/new_crude_sales_report_views.sql",
        help="Derived weekly/monthly report SQL path.",
    )
    parser.add_argument(
        "--out",
        default="reports/new_crude_reports.xlsx",
        help="Output XLSX path. Default: reports/new_crude_reports.xlsx",
    )
    parser.add_argument(
        "--write",
        action="store_true",
        help="Create persistent DuckDB views instead of temporary session views.",
    )
    return parser.parse_args()


def load_sql(path: Path, *, persistent: bool) -> str:
    sql = path.read_text(encoding="utf-8")
    sql = sql.split("-- Validation checks to run after creating the view:")[0].strip()
    if persistent:
        return sql

    return re.sub(
        r"CREATE\s+OR\s+REPLACE\s+VIEW\s+",
        "CREATE OR REPLACE TEMP VIEW ",
        sql,
        flags=re.IGNORECASE,
    )


def assert_path_exists(path: Path, label: str) -> None:
    if not path.exists():
        raise FileNotFoundError(f"{label} not found: {path}")


def fetch_sheet(conn: duckdb.DuckDBPyConnection, view_name: str) -> pd.DataFrame:
    return conn.execute(f"SELECT * FROM {view_name}").df()


def write_workbook(
    conn: duckdb.DuckDBPyConnection,
    output_path: Path,
) -> dict[str, int]:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    row_counts: dict[str, int] = {}

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        for sheet_name, view_name in REPORT_SHEETS:
            df = fetch_sheet(conn, view_name)
            row_counts[sheet_name] = len(df)
            df.to_excel(writer, sheet_name=sheet_name[:31], index=False)

            worksheet = writer.sheets[sheet_name[:31]]
            worksheet.freeze_panes = "A2"
            worksheet.auto_filter.ref = worksheet.dimensions

            for column_cells in worksheet.columns:
                max_length = max(
                    len(str(cell.value)) if cell.value is not None else 0
                    for cell in column_cells
                )
                adjusted_width = min(max(max_length + 2, 10), 60)
                worksheet.column_dimensions[column_cells[0].column_letter].width = adjusted_width

    return row_counts


def main() -> int:
    args = parse_args()
    db_path = Path(args.db)
    opportunity_sql_path = Path(args.opportunity_sql)
    derived_sql_path = Path(args.derived_sql)
    output_path = Path(args.out)

    try:
        assert_path_exists(db_path, "DuckDB")
        assert_path_exists(opportunity_sql_path, "Opportunity SQL")
        assert_path_exists(derived_sql_path, "Derived report SQL")
    except FileNotFoundError as exc:
        print(exc, file=sys.stderr)
        return 2

    conn = duckdb.connect(str(db_path), read_only=not args.write)
    try:
        conn.execute(load_sql(opportunity_sql_path, persistent=args.write))
        conn.execute(load_sql(derived_sql_path, persistent=args.write))
        row_counts = write_workbook(conn, output_path)
    finally:
        conn.close()

    print(f"Wrote {output_path}")
    for sheet_name, row_count in row_counts.items():
        print(f"- {sheet_name}: {row_count} rows")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
