#!/usr/bin/env python3
"""Render and optionally send the TrendEnergy weekly operator digest."""

from __future__ import annotations

import argparse
import os
import re
import shutil
import smtplib
import subprocess
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta
from email.message import EmailMessage
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd
from jinja2 import Environment, FileSystemLoader, select_autoescape


CATEGORY_ORDER = ("new_operator", "near_facility", "production_mover")
DEFAULT_PREVIEW_PATH = Path("/tmp/digest_preview.html")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Render the TrendEnergy weekly New Crude operator digest."
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
        help="Derived report SQL path containing weekly_operator_digest.",
    )
    parser.add_argument(
        "--template",
        default="templates/weekly_digest.html.j2",
        help="Jinja template path. Default: templates/weekly_digest.html.j2",
    )
    parser.add_argument(
        "--export-dir",
        default="reports",
        help="Directory containing exported New Crude XLSX files.",
    )
    parser.add_argument(
        "--xlsx",
        default=None,
        help="Explicit XLSX attachment path. Defaults to newest *.xlsx in --export-dir.",
    )
    parser.add_argument(
        "--preview-out",
        default=str(DEFAULT_PREVIEW_PATH),
        help="HTML output path for --dry-run. Default: /tmp/digest_preview.html",
    )
    parser.add_argument(
        "--recipient",
        action="append",
        help="Override DIGEST_RECIPIENTS. Can be passed more than once.",
    )
    parser.add_argument(
        "--as-of",
        help="Override report date as YYYY-MM-DD for backfills and previews.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Write rendered HTML to --preview-out without sending.",
    )
    parser.add_argument(
        "--write",
        action="store_true",
        help="Create persistent DuckDB report objects instead of temporary session objects.",
    )
    return parser.parse_args()


def parse_as_of(value: str | None) -> date:
    if value is None:
        return date.today()
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError("--as-of must be YYYY-MM-DD") from exc


def load_sql(
    path: Path,
    *,
    persistent: bool,
    materialize: bool = False,
    as_of: date | None = None,
) -> str:
    sql = path.read_text(encoding="utf-8")
    sql = sql.split("-- Validation checks to run after creating the view:")[0].strip()

    if as_of is not None:
        sql = re.sub(r"\bCURRENT_DATE\b", f"DATE '{as_of.isoformat()}'", sql)

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


def assert_path_exists(path: Path, label: str) -> None:
    if not path.exists():
        raise FileNotFoundError(f"{label} not found: {path}")


def latest_xlsx(export_dir: Path) -> Path | None:
    candidates = sorted(
        export_dir.glob("*.xlsx"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    return candidates[0] if candidates else None


def clean_value(value: Any) -> Any:
    if value is None:
        return None
    if pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime().date()
    return value


def frame_records(df: pd.DataFrame) -> list[dict[str, Any]]:
    return [
        {key: clean_value(value) for key, value in record.items()}
        for record in df.to_dict("records")
    ]


def build_report_objects(
    conn: duckdb.DuckDBPyConnection,
    *,
    opportunity_sql_path: Path,
    derived_sql_path: Path,
    as_of: date,
    persistent: bool,
) -> None:
    conn.execute(load_sql(opportunity_sql_path, persistent=persistent, as_of=as_of))
    conn.execute(
        load_sql(
            derived_sql_path,
            persistent=persistent,
            materialize=True,
            as_of=as_of,
        )
    )


def fetch_digest_context(
    conn: duckdb.DuckDBPyConnection,
    *,
    as_of: date,
    xlsx_path: Path | None,
) -> dict[str, Any]:
    digest_df = conn.execute(
        """
        SELECT *
        FROM weekly_operator_digest
        WHERE category IN ('new_operator', 'near_facility', 'production_mover')
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
            display_operator
        """
    ).df()
    timeline_df = conn.execute(
        """
        SELECT *
        FROM weekly_operator_timeline
        ORDER BY operator_key, event_date DESC
        """
    ).df()

    timeline_by_operator: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for event in frame_records(timeline_df):
        timeline_by_operator[(str(event["operator_key"]), str(event["category"]))].append(event)

    sections: dict[str, list[dict[str, Any]]] = {category: [] for category in CATEGORY_ORDER}
    for operator in frame_records(digest_df):
        category = operator["category"]
        if category not in sections:
            continue
        operator["timeline"] = timeline_by_operator.get(
            (str(operator["operator_key"]), str(category)),
            [],
        )
        sections[category].append(operator)

    week_start = as_of - timedelta(days=as_of.weekday())
    counts = {category: len(sections[category]) for category in CATEGORY_ORDER}
    subject_day_format = "%#d" if os.name == "nt" else "%-d"
    subject = (
        f"TrendEnergy Radar — week of {week_start.strftime('%b ' + subject_day_format)}: "
        f"{counts['new_operator']} new operators, {counts['near_facility']} near facilities"
    )

    xlsx_url = os.getenv("DIGEST_XLSX_URL")
    if not xlsx_url and xlsx_path is not None:
        xlsx_url = xlsx_path.name

    return {
        "as_of": as_of,
        "week_label": week_start.strftime("%b %d, %Y"),
        "subject": subject,
        "sections": sections,
        "counts": counts,
        "has_activity": any(counts.values()),
        "full_list_url": os.getenv("DIGEST_FULL_LIST_URL", "#"),
        "xlsx_url": xlsx_url or "#",
        "xlsx_path": xlsx_path,
    }


def format_date_label(value: Any) -> str:
    value = clean_value(value)
    if value is None:
        return "n/a"
    if isinstance(value, datetime):
        value = value.date()
    if isinstance(value, date):
        return value.strftime("%b %d")
    return str(value)


def format_month_label(value: Any) -> str:
    value = clean_value(value)
    if value is None:
        return "n/a"
    if isinstance(value, datetime):
        value = value.date()
    if isinstance(value, date):
        return value.strftime("%Y-%m")
    return str(value)[:7]


def format_m3(value: Any) -> str:
    value = clean_value(value)
    if value is None:
        return "0 m³"
    return f"{float(value):,.0f} m³"


def format_signed_m3(value: Any) -> str:
    value = clean_value(value)
    if value is None:
        value = 0
    amount = float(value)
    sign = "+" if amount >= 0 else "-"
    return f"{sign}{abs(amount):,.0f} m³"


def format_distance(value: Any) -> str:
    value = clean_value(value)
    if value is None:
        return "n/a"
    return f"{float(value):.1f} km"


def render_html(template_path: Path, context: dict[str, Any]) -> str:
    env = Environment(
        loader=FileSystemLoader(str(template_path.parent)),
        autoescape=select_autoescape(["html", "xml", "j2"]),
    )
    env.filters["date_label"] = format_date_label
    env.filters["month_label"] = format_month_label
    env.filters["m3"] = format_m3
    env.filters["signed_m3"] = format_signed_m3
    env.filters["distance"] = format_distance
    template = env.get_template(template_path.name)
    return template.render(**context)


def recipients_from_env(override: list[str] | None) -> list[str]:
    if override:
        return [email.strip() for email in override if email.strip()]
    raw = os.getenv("DIGEST_RECIPIENTS", "")
    return [email.strip() for email in raw.split(",") if email.strip()]


def build_email_message(
    *,
    subject: str,
    html: str,
    recipients: list[str],
    xlsx_path: Path | None,
) -> EmailMessage:
    sender = os.getenv("DIGEST_FROM") or os.getenv("SMTP_FROM") or "radar@trendenergy.ca"
    message = EmailMessage()
    message["Subject"] = subject
    message["From"] = sender
    message["To"] = ", ".join(recipients)
    message.set_content("HTML email required. See attached New Crude workbook.")
    message.add_alternative(html, subtype="html")

    if xlsx_path is not None and xlsx_path.exists():
        message.add_attachment(
            xlsx_path.read_bytes(),
            maintype="application",
            subtype="vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            filename=xlsx_path.name,
        )

    return message


def send_message(message: EmailMessage) -> None:
    smtp_host = os.getenv("SMTP_HOST")
    if smtp_host:
        smtp_port = int(os.getenv("SMTP_PORT", "587"))
        smtp_username = os.getenv("SMTP_USERNAME")
        smtp_password = os.getenv("SMTP_PASSWORD")
        use_starttls = os.getenv("SMTP_STARTTLS", "true").lower() not in {"0", "false", "no"}
        with smtplib.SMTP(smtp_host, smtp_port, timeout=30) as smtp:
            if use_starttls:
                smtp.starttls()
            if smtp_username:
                smtp.login(smtp_username, smtp_password or "")
            smtp.send_message(message)
        return

    sendmail_path = os.getenv("SENDMAIL_PATH") or shutil.which("sendmail")
    if sendmail_path:
        subprocess.run([sendmail_path, "-t", "-oi"], input=message.as_bytes(), check=True)
        return

    raise RuntimeError("No email delivery configured. Set SMTP_HOST or provide sendmail.")


def main() -> int:
    args = parse_args()
    try:
        as_of = parse_as_of(args.as_of)
    except ValueError as exc:
        print(exc, file=sys.stderr)
        return 2

    db_path = Path(args.db)
    opportunity_sql_path = Path(args.opportunity_sql)
    derived_sql_path = Path(args.derived_sql)
    template_path = Path(args.template)
    preview_path = Path(args.preview_out)
    xlsx_path = Path(args.xlsx) if args.xlsx else latest_xlsx(Path(args.export_dir))

    try:
        assert_path_exists(db_path, "DuckDB")
        assert_path_exists(opportunity_sql_path, "Opportunity SQL")
        assert_path_exists(derived_sql_path, "Derived report SQL")
        assert_path_exists(template_path, "Digest template")
    except FileNotFoundError as exc:
        print(exc, file=sys.stderr)
        return 2

    conn = duckdb.connect(str(db_path), read_only=not args.write)
    try:
        build_report_objects(
            conn,
            opportunity_sql_path=opportunity_sql_path,
            derived_sql_path=derived_sql_path,
            as_of=as_of,
            persistent=args.write,
        )
        context = fetch_digest_context(conn, as_of=as_of, xlsx_path=xlsx_path)
    finally:
        conn.close()

    html = render_html(template_path, context)

    if args.dry_run:
        preview_path.parent.mkdir(parents=True, exist_ok=True)
        preview_path.write_text(html, encoding="utf-8")
        print(f"Wrote digest preview: {preview_path}")
        print(f"Subject: {context['subject']}")
        for category in CATEGORY_ORDER:
            print(f"- {category}: {context['counts'][category]} operators")
        return 0

    if not context["has_activity"]:
        print("No digest activity for this period; nothing sent.")
        return 0

    recipients = recipients_from_env(args.recipient)
    if not recipients:
        print("No recipients configured. Set DIGEST_RECIPIENTS or pass --recipient.", file=sys.stderr)
        return 2

    message = build_email_message(
        subject=context["subject"],
        html=html,
        recipients=recipients,
        xlsx_path=xlsx_path,
    )
    try:
        send_message(message)
    except Exception as exc:
        fallback = DEFAULT_PREVIEW_PATH
        fallback.parent.mkdir(parents=True, exist_ok=True)
        fallback.write_text(html, encoding="utf-8")
        print(f"Failed to send digest: {exc}", file=sys.stderr)
        print(f"Rendered HTML saved to {fallback}", file=sys.stderr)
        return 2

    print(f"Sent digest to {', '.join(recipients)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
