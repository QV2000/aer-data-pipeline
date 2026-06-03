#!/usr/bin/env python3
"""Render the TrendEnergy monthly New Crude operator digest."""

from __future__ import annotations

import argparse
import os
import re
import sys
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd
from jinja2 import Environment, FileSystemLoader, select_autoescape

DEFAULT_PREVIEW_PATH = Path("/tmp/monthly_digest_preview.html")
MAX_SAMPLES_PER_OPERATOR = 3
SIGNAL_LABELS = {
    "NEW_BATTERY_FIRST_OIL": "New pad first oil",
    "FIRST_CONFIRMED_OIL": "New well first oil",
    "PRODUCTION_RESTART": "Production restart",
    "PRODUCTION_STEP_CHANGE": "Production step change",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Render the TrendEnergy monthly New Crude operator digest."
    )
    parser.add_argument("--db", default="/data/aer_data.duckdb")
    parser.add_argument(
        "--opportunity-sql",
        default="docs/sql/new_crude_opportunity_report.sql",
    )
    parser.add_argument(
        "--derived-sql",
        default="docs/sql/new_crude_sales_report_views.sql",
    )
    parser.add_argument(
        "--template",
        default="templates/monthly_digest.html.j2",
    )
    parser.add_argument(
        "--preview-out",
        default=str(DEFAULT_PREVIEW_PATH),
        help="HTML output path. Default: /tmp/monthly_digest_preview.html",
    )
    parser.add_argument(
        "--month",
        help="Target production month as YYYY-MM-DD (first of month). Defaults to the latest production month in the warehouse.",
    )
    parser.add_argument(
        "--as-of",
        help="Generation date as YYYY-MM-DD. Used only for the footer label.",
    )
    parser.add_argument(
        "--xlsx-url",
        default=None,
        help="Link to the companion xlsx for the footer Download button.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Alias for default behavior — writes preview only.")
    parser.add_argument("--write", action="store_true", help="Use persistent DuckDB report objects instead of TEMP.")
    return parser.parse_args()


def parse_as_of(value: str | None) -> date:
    if value is None:
        return date.today()
    return datetime.strptime(value, "%Y-%m-%d").date()


def parse_month(value: str | None) -> date | None:
    if value is None:
        return None
    return datetime.strptime(value, "%Y-%m-%d").date()


def load_sql(path: Path, *, persistent: bool, materialize: bool = False) -> str:
    sql = path.read_text(encoding="utf-8")
    sql = sql.split("-- Validation checks to run after creating the view:")[0].strip()
    if persistent:
        replacement = "CREATE OR REPLACE TABLE " if materialize else "CREATE OR REPLACE VIEW "
    else:
        replacement = "CREATE OR REPLACE TEMP TABLE " if materialize else "CREATE OR REPLACE TEMP VIEW "
    return re.sub(
        r"CREATE\s+OR\s+REPLACE\s+VIEW\s+",
        replacement,
        sql,
        flags=re.IGNORECASE,
    )


def clean_value(value: Any) -> Any:
    if value is None:
        return None
    if pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime().date()
    return value


def fmt_m3(value: Any) -> str:
    value = clean_value(value)
    if value is None or value == 0:
        return ""
    return f"{float(value):,.0f} m³"


def signal_label(signal: str) -> str:
    return SIGNAL_LABELS.get(signal, signal.replace("_", " ").title())


def lsd_label_from_uwi(uwi: Any) -> str:
    uwi_str = clean_value(uwi)
    if uwi_str is None:
        return ""
    match = re.match(r"^\d{2}/([^/]+)/\d+$", str(uwi_str))
    return match.group(1) if match else str(uwi_str)


def fetch_target_month(conn: duckdb.DuckDBPyConnection, requested: date | None) -> date | None:
    if requested is not None:
        return requested
    row = conn.execute(
        "SELECT MAX(production_report_month) FROM new_crude_monthly_confirmed_report"
    ).fetchone()
    return row[0] if row and row[0] else None


def fetch_operator_rows(conn: duckdb.DuckDBPyConnection, month: date) -> list[dict[str, Any]]:
    df = conn.execute(
        """
        SELECT
            operator_id,
            display_operator,
            operator_short_name,
            production_report_month,
            opportunity_count,
            represented_wells,
            new_battery_first_oil_count,
            first_confirmed_oil_count,
            restart_count,
            step_change_count,
            latest_oil_m3_sum,
            provinces,
            signal_mix,
            recommended_operator_action,
            monthly_operator_rank
        FROM new_crude_operator_monthly_summary
        WHERE production_report_month = ?
        ORDER BY monthly_operator_rank
        """,
        [month],
    ).df()
    return [
        {key: clean_value(value) for key, value in row.items()}
        for row in df.to_dict("records")
    ]


def fetch_samples(conn: duckdb.DuckDBPyConnection, month: date) -> dict[Any, list[dict[str, Any]]]:
    df = conn.execute(
        """
        WITH ranked AS (
            SELECT
                operator_id,
                display_operator,
                primary_signal,
                uwi,
                wells_in_pad,
                well_name,
                linked_facility_name,
                opportunity_type,
                latest_oil_m3,
                ROW_NUMBER() OVER (
                    PARTITION BY operator_id, display_operator
                    ORDER BY
                        CASE primary_signal
                            WHEN 'NEW_BATTERY_FIRST_OIL' THEN 1
                            WHEN 'FIRST_CONFIRMED_OIL'  THEN 2
                            WHEN 'PRODUCTION_RESTART'   THEN 3
                            WHEN 'PRODUCTION_STEP_CHANGE' THEN 4
                            ELSE 9
                        END,
                        COALESCE(latest_oil_m3, 0) DESC,
                        uwi
                ) AS rn
            FROM new_crude_monthly_confirmed_report
            WHERE production_report_month = ?
        )
        SELECT *
        FROM ranked
        WHERE rn <= ?
        ORDER BY operator_id, display_operator, rn
        """,
        [month, MAX_SAMPLES_PER_OPERATOR],
    ).df()
    samples: dict[Any, list[dict[str, Any]]] = defaultdict(list)
    for row in df.to_dict("records"):
        record = {key: clean_value(value) for key, value in row.items()}
        oil = clean_value(record.get("latest_oil_m3"))
        label_source = (
            record.get("linked_facility_name")
            if record.get("opportunity_type") == "BATTERY"
            else None
        )
        label = (
            label_source
            or lsd_label_from_uwi(record.get("uwi"))
            or record.get("well_name")
            or "unknown"
        )
        detail_bits: list[str] = []
        if oil and float(oil) > 0:
            detail_bits.append(f"{float(oil):,.1f} m³")
        if record.get("opportunity_type") == "BATTERY" and record.get("wells_in_pad"):
            pad_count = len(str(record["wells_in_pad"]).split(","))
            detail_bits.append(f"{pad_count} wells in pad")
        samples[(record["operator_id"], record["display_operator"])].append(
            {
                "signal_label": signal_label(str(record.get("primary_signal", ""))),
                "label": label,
                "detail": " · ".join(detail_bits),
            }
        )
    return samples


def build_context(
    conn: duckdb.DuckDBPyConnection,
    *,
    month: date,
    as_of: date,
    xlsx_url: str | None,
) -> dict[str, Any]:
    operator_rows = fetch_operator_rows(conn, month)
    samples = fetch_samples(conn, month)

    operators: list[dict[str, Any]] = []
    totals = {
        "operator_count": 0,
        "well_first_oil": 0,
        "pad_first_oil": 0,
        "restarts": 0,
        "step_changes": 0,
    }
    for op in operator_rows:
        op_samples = samples.get((op.get("operator_id"), op.get("display_operator")), [])
        op["samples"] = op_samples
        op["latest_oil_m3_sum_fmt"] = fmt_m3(op.get("latest_oil_m3_sum"))
        operators.append(op)
        totals["operator_count"] += 1
        totals["well_first_oil"] += int(op.get("first_confirmed_oil_count") or 0)
        totals["pad_first_oil"] += int(op.get("new_battery_first_oil_count") or 0)
        totals["restarts"] += int(op.get("restart_count") or 0)
        totals["step_changes"] += int(op.get("step_change_count") or 0)

    return {
        "month": month,
        "month_label": month.strftime("%B %Y"),
        "as_of": as_of,
        "as_of_label": as_of.strftime("%b %d, %Y"),
        "operators": operators,
        "totals": totals,
        "xlsx_url": xlsx_url or os.getenv("DIGEST_XLSX_URL") or "#",
    }


def render(template_path: Path, context: dict[str, Any]) -> str:
    env = Environment(
        loader=FileSystemLoader(template_path.parent),
        autoescape=select_autoescape(["html", "xml", "j2"]),
        trim_blocks=False,
        lstrip_blocks=False,
    )
    template = env.get_template(template_path.name)
    return template.render(**context)


def main() -> int:
    args = parse_args()
    db_path = Path(args.db)
    opp_sql = Path(args.opportunity_sql)
    derived_sql = Path(args.derived_sql)
    template_path = Path(args.template)
    preview_path = Path(args.preview_out)
    as_of = parse_as_of(args.as_of)
    requested_month = parse_month(args.month)

    for path, label in [
        (db_path, "DuckDB"),
        (opp_sql, "Opportunity SQL"),
        (derived_sql, "Derived report SQL"),
        (template_path, "Template"),
    ]:
        if not path.exists():
            print(f"{label} not found: {path}", file=sys.stderr)
            return 2

    conn = duckdb.connect(str(db_path), read_only=not args.write)
    try:
        conn.execute(load_sql(opp_sql, persistent=args.write))
        conn.execute(load_sql(derived_sql, persistent=args.write, materialize=True))
        month = fetch_target_month(conn, requested_month)
        if month is None:
            print("No production data available — cannot build monthly digest.", file=sys.stderr)
            return 1
        context = build_context(
            conn,
            month=month,
            as_of=as_of,
            xlsx_url=args.xlsx_url,
        )
    finally:
        conn.close()

    html = render(template_path, context)
    preview_path.parent.mkdir(parents=True, exist_ok=True)
    preview_path.write_text(html, encoding="utf-8")
    print(f"Wrote monthly digest preview: {preview_path}")
    print(f"Production month: {context['month_label']}")
    print(f"- {context['totals']['operator_count']} operators")
    print(f"- {context['totals']['well_first_oil']} new well first oils")
    print(f"- {context['totals']['pad_first_oil']} new pad first oils")
    print(f"- {context['totals']['restarts']} restarts")
    print(f"- {context['totals']['step_changes']} step changes")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
