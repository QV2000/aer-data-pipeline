#!/usr/bin/env python3
"""Render a status-change-only digest for the TrendEnergy New Crude report.

Surfaces every well whose status transitioned through one of the four
sales-relevant patterns in the window, regardless of whether the well also
has production data (the main opportunity report's signal dedupe collapses
these into higher-priority signals like FIRST_CONFIRMED_OIL).

Patterns:
  DRL&C       -> CR-OIL PUMP   (new crude producer, pumping)
  DRL&C       -> CR-BIT PUMP   (new bitumen producer, pumping)
  DRL&C       -> CR-OIL FLOW   (new crude producer, flowing)
  CR-OIL SUSP -> CR-OIL PUMP   (reactivation from suspension)

Output is HTML; one card per operator with all their transition wells listed.
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb
from jinja2 import Environment, FileSystemLoader, select_autoescape


PATTERN_LABELS = {
    ("DRL&C", "CR-OIL PUMP"): "New crude oil well — pumping",
    ("DRL&C", "CR-BIT PUMP"): "New bitumen well — pumping",
    ("DRL&C", "CR-OIL FLOW"): "New crude oil well — flowing",
    ("CR-OIL SUSP", "CR-OIL PUMP"): "Reactivated crude oil well — pumping",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Render the well-status-change digest."
    )
    parser.add_argument("--db", default="/data/aer_data.duckdb")
    parser.add_argument(
        "--template",
        default="templates/status_digest.html.j2",
    )
    parser.add_argument(
        "--preview-out",
        default="/tmp/status_digest.html",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=28,
        help="Window size in days (anchored to MAX(event_date) in status_changes). Default 28.",
    )
    return parser.parse_args()


def lsd_label_from_uwi(uwi: str | None) -> str:
    if not uwi:
        return ""
    match = re.match(r"^\d{2}/([^/]+)/\d+$", uwi)
    return match.group(1) if match else uwi


def fetch_transitions(conn: duckdb.DuckDBPyConnection, days: int) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        WITH bounds AS (
            SELECT MAX(CAST(event_date AS DATE)) AS anchor_date FROM status_changes
        ),
        matches AS (
            SELECT
                sc.uwi,
                CAST(sc.event_date AS DATE) AS event_date,
                UPPER(TRIM(sc.old_status)) AS old_status,
                UPPER(TRIM(sc.new_status)) AS new_status,
                sc.licensee_code,
                sc.licence,
                sc.field_name,
                sc.province
            FROM status_changes sc
            CROSS JOIN bounds b
            WHERE CAST(sc.event_date AS DATE) >= b.anchor_date - INTERVAL ($days) DAY
              AND CAST(sc.event_date AS DATE) <= b.anchor_date
              AND (
                  (UPPER(TRIM(sc.old_status)) = 'DRL&C' AND UPPER(TRIM(sc.new_status)) IN ('CR-OIL PUMP','CR-BIT PUMP','CR-OIL FLOW'))
                  OR (UPPER(TRIM(sc.old_status)) = 'CR-OIL SUSP' AND UPPER(TRIM(sc.new_status)) = 'CR-OIL PUMP')
              )
        ),
        with_master AS (
            SELECT
                m.*,
                wc.licensee_name AS master_licensee,
                wc.licensee_code AS master_licensee_code,
                wc.fluid         AS master_fluid,
                wc.well_name     AS master_well_name,
                wc.licence_no    AS master_licence_no
            FROM matches m
            LEFT JOIN wells_current wc ON wc.uwi = m.uwi
        ),
        with_op AS (
            SELECT
                w.*,
                COALESCE(w.master_licensee_code, w.licensee_code) AS resolved_licensee_code,
                COALESCE(
                    w.master_licensee,
                    op4.canonical_name,
                    op5.canonical_name
                ) AS resolved_operator_name,
                COALESCE(op4.operator_id, op5.operator_id) AS resolved_operator_id
            FROM with_master w
            LEFT JOIN operators op4
              ON op4.operator_id = (
                  SELECT operator_id FROM operator_identifiers oi
                  WHERE oi.identifier_kind = 'ab_ba_code_4'
                    AND oi.identifier_value = UPPER(TRIM(COALESCE(w.master_licensee_code, w.licensee_code)))
                  LIMIT 1
              )
            LEFT JOIN operators op5
              ON op5.operator_id = (
                  SELECT operator_id FROM operator_identifiers oi
                  WHERE oi.identifier_kind = 'ab_ba_code_5'
                    AND oi.identifier_value = UPPER(TRIM(COALESCE(w.master_licensee_code, w.licensee_code)))
                  LIMIT 1
              )
        )
        SELECT
            uwi,
            event_date,
            old_status,
            new_status,
            resolved_licensee_code,
            resolved_operator_id,
            resolved_operator_name,
            master_well_name,
            master_fluid,
            province,
            field_name,
            (SELECT anchor_date FROM bounds) AS anchor_date
        FROM with_op
        ORDER BY resolved_operator_name NULLS LAST, event_date DESC, uwi
        """.replace("($days)", str(days))
    ).df()
    return [
        {key: (None if value is None or (hasattr(value, "isoformat") is False and str(value) == "NaT") else value)
         for key, value in row.items()}
        for row in rows.to_dict("records")
    ]


def group_by_operator(transitions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    for tx in transitions:
        key = tx.get("resolved_operator_name") or "Unresolved operator"
        op = grouped.setdefault(
            key,
            {
                "operator_name": key,
                "operator_id": tx.get("resolved_operator_id"),
                "licensee_code": tx.get("resolved_licensee_code"),
                "provinces": set(),
                "wells": [],
            },
        )
        province = tx.get("province")
        if province:
            op["provinces"].add(province)
        pattern_key = (str(tx.get("old_status") or ""), str(tx.get("new_status") or ""))
        pattern_label = PATTERN_LABELS.get(pattern_key, f"{pattern_key[0]} → {pattern_key[1]}")
        well = {
            "uwi": tx.get("uwi"),
            "lsd": lsd_label_from_uwi(tx.get("uwi")),
            "event_date": tx.get("event_date"),
            "old_status": tx.get("old_status"),
            "new_status": tx.get("new_status"),
            "pattern_label": pattern_label,
            "well_name": tx.get("master_well_name"),
        }
        op["wells"].append(well)
    # Sort: operators with most wells first; within operator, newest event first
    ops = []
    for op in grouped.values():
        op["provinces"] = ", ".join(sorted(op["provinces"])) if op["provinces"] else None
        op["well_count"] = len(op["wells"])
        op["wells"].sort(key=lambda w: (w["event_date"] is None, w["event_date"] or ""), reverse=True)
        ops.append(op)
    ops.sort(key=lambda o: (-o["well_count"], o["operator_name"]))
    return ops


def render_html(template_path: Path, context: dict[str, Any]) -> str:
    env = Environment(
        loader=FileSystemLoader(str(template_path.parent)),
        autoescape=select_autoescape(["html", "xml", "j2"]),
    )

    def date_label(value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, datetime):
            value = value.date()
        if isinstance(value, date):
            return value.strftime("%b %d")
        return str(value)

    env.filters["date_label"] = date_label
    template = env.get_template(template_path.name)
    return template.render(**context)


def main() -> int:
    args = parse_args()
    db_path = Path(args.db)
    template_path = Path(args.template)
    preview_path = Path(args.preview_out)

    if not db_path.exists():
        print(f"DuckDB not found: {db_path}", file=sys.stderr)
        return 2
    if not template_path.exists():
        print(f"Template not found: {template_path}", file=sys.stderr)
        return 2

    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        transitions = fetch_transitions(conn, args.days)
    finally:
        conn.close()

    operators = group_by_operator(transitions)
    anchor_date = transitions[0]["anchor_date"] if transitions else None
    if isinstance(anchor_date, datetime):
        anchor_date = anchor_date.date()

    pattern_counts: dict[str, int] = defaultdict(int)
    for tx in transitions:
        pattern_key = (str(tx.get("old_status") or ""), str(tx.get("new_status") or ""))
        label = PATTERN_LABELS.get(pattern_key, f"{pattern_key[0]} → {pattern_key[1]}")
        pattern_counts[label] += 1

    context = {
        "operators": operators,
        "total_wells": sum(op["well_count"] for op in operators),
        "operator_count": len(operators),
        "anchor_date": anchor_date,
        "window_start": (anchor_date - timedelta(days=args.days)) if anchor_date else None,
        "days": args.days,
        "pattern_counts": dict(pattern_counts),
    }
    html = render_html(template_path, context)
    preview_path.parent.mkdir(parents=True, exist_ok=True)
    preview_path.write_text(html, encoding="utf-8")
    print(f"Wrote status digest: {preview_path}")
    print(f"Window: {context['window_start']} → {anchor_date} ({args.days} days)")
    print(f"- {context['operator_count']} operators, {context['total_wells']} wells")
    for label, n in pattern_counts.items():
        print(f"  - {label}: {n}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
