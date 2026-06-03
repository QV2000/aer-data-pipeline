#!/usr/bin/env python3
"""Render a status-change-only digest for the TrendEnergy New Crude report.

Surfaces every well whose status transitioned through one of the
sales-relevant patterns in the window, regardless of whether the well also
has production data (the main opportunity report's signal dedupe collapses
these into higher-priority signals like FIRST_CONFIRMED_OIL).

Patterns (AB, from ST2 transition log):
  DRL&C       -> CR-OIL PUMP   (new crude producer, pumping)
  DRL&C       -> CR-BIT PUMP   (new bitumen producer, pumping)
  DRL&C       -> CR-OIL FLOW   (new crude producer, flowing)
  CR-OIL SUSP -> CR-OIL PUMP   (reactivation from suspension)

Patterns (SK, synthesized from sk_well_bulletin 'New' rows):
  SK-NEW LIC  -> SK-OIL LIC    (new crude oil licence issued)
  SK-NEW LIC  -> SK-BIT LIC    (new bitumen / heavy-oil licence issued)

The SK signal fires earlier in the lifecycle than the AB one (licensed,
not yet pumping) because SK has no direct equivalent of ST2's transition
log; this is intentional and the labels are kept separate so reps can
read the semantic difference.

Output is HTML; one card per operator with all their transition wells listed.
"""

from __future__ import annotations

import argparse
import os
import re
import smtplib
import ssl
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta
from email.message import EmailMessage
from email.utils import formataddr
from pathlib import Path
from typing import Any

import duckdb
from jinja2 import Environment, FileSystemLoader, select_autoescape

PATTERN_LABELS = {
    # AB ST2 transitions — "Pumping" is the default mode for these patterns,
    # so we leave it implicit. Only the flowing exception is called out.
    ("DRL&C", "CR-OIL PUMP"): "New crude oil",
    ("DRL&C", "CR-BIT PUMP"): "New bitumen",
    ("DRL&C", "CR-OIL FLOW"): "New crude oil — flowing",
    ("CR-OIL SUSP", "CR-OIL PUMP"): "Reactivated crude oil",
    # SK well-bulletin synthesized rows — licence-issued events. Kept as
    # separate labels because the SK signal is "licensed", not "pumping".
    ("SK-NEW LIC", "SK-OIL LIC"): "New SK crude licence",
    ("SK-NEW LIC", "SK-BIT LIC"): "New SK bitumen licence",
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
    parser.add_argument(
        "--max-km",
        type=float,
        default=100.0,
        help="Only include wells within this distance (km) of any TEMI facility. Default 100.",
    )
    parser.add_argument(
        "--send",
        action="store_true",
        help="Send the digest via SMTP instead of just writing the preview HTML.",
    )
    parser.add_argument(
        "--recipient",
        action="append",
        default=None,
        help="Email recipient. Can be passed multiple times. Defaults to DIGEST_RECIPIENTS env var.",
    )
    parser.add_argument(
        "--env-file",
        default="/data/.env.digest",
        help="Path to env file with SMTP creds. Default: /data/.env.digest",
    )
    parser.add_argument(
        "--subject",
        default=None,
        help="Override the email subject line.",
    )
    return parser.parse_args()


def load_env_file(path: Path) -> None:
    """Load KEY=VALUE lines from an env file into os.environ (no-op if missing)."""
    if not path.exists():
        return
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())


def send_email(
    *,
    html_body: str,
    subject: str,
    recipients: list[str],
) -> None:
    """Send the digest via SMTP. Reads SMTP_* from env."""
    smtp_host = os.environ.get("SMTP_HOST", "smtp.gmail.com")
    smtp_port = int(os.environ.get("SMTP_PORT", "587"))
    smtp_user = os.environ.get("SMTP_USER")
    smtp_password = os.environ.get("SMTP_PASSWORD")
    from_name = os.environ.get("SMTP_FROM_NAME", "TrendEnergy Radar")
    if not smtp_user or not smtp_password:
        raise RuntimeError(
            "SMTP_USER and SMTP_PASSWORD must be set (via env or --env-file)."
        )

    msg = EmailMessage()
    msg["From"] = formataddr((from_name, smtp_user))
    msg["To"] = ", ".join(recipients)
    msg["Subject"] = subject
    # Plain-text fallback (very minimal — most clients show HTML)
    msg.set_content(
        "This email contains an HTML-rendered TrendEnergy crude-radar digest. "
        "If you can't see it, view in an HTML-capable client."
    )
    msg.add_alternative(html_body, subtype="html")

    context = ssl.create_default_context()
    with smtplib.SMTP(smtp_host, smtp_port, timeout=30) as server:
        server.ehlo()
        server.starttls(context=context)
        server.ehlo()
        server.login(smtp_user, smtp_password)
        server.send_message(msg)


def lsd_label_from_uwi(uwi: str | None) -> str:
    if not uwi:
        return ""
    match = re.match(r"^\d{2}/([^/]+)/\d+$", uwi)
    return match.group(1) if match else uwi


def fetch_transitions(conn: duckdb.DuckDBPyConnection, days: int, max_km: float) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        WITH bounds AS (
            SELECT MAX(CAST(event_date AS DATE)) AS anchor_date FROM status_changes
        ),
        temi_facilities(facility_id, facility_name, lat, lon) AS (
            VALUES
                ('ABTM0000930', 'Cynthia Pembina',                53.310727, -115.619797),
                ('ABTM0112311', 'Rush Energy Services',           53.099561, -114.474046),
                ('ABTM0125201', 'Vermilion 15-16-051-11W5',       53.408441, -115.558574),
                ('ABTM0157119', 'Persist Wayne',                  51.403331, -112.914700),
                ('SKTMTT15003', 'Dulwich',                        53.165195, -109.703831)
        ),
        pattern_filter AS (
            SELECT
                sc.uwi,
                CAST(sc.event_date AS DATE) AS event_date,
                UPPER(TRIM(sc.old_status)) AS old_status,
                UPPER(TRIM(sc.new_status)) AS new_status,
                sc.licensee_code,
                sc.licence,
                sc.field_name,
                sc.province,
                -- Populated for SK synthesized rows (sk_well_bulletin
                -- licensee_name); NULL for AB ST2 rows.
                sc.licensee_name AS sc_licensee_name,
                -- Pre-computed centroids for SK rows whose UWIs aren't
                -- yet in the wells silver table. NULL for AB.
                sc.centroid_lat AS sc_centroid_lat,
                sc.centroid_lon AS sc_centroid_lon
            FROM status_changes sc
            WHERE (
                  (UPPER(TRIM(sc.old_status)) = 'DRL&C' AND UPPER(TRIM(sc.new_status)) IN ('CR-OIL PUMP','CR-BIT PUMP','CR-OIL FLOW'))
                  OR (UPPER(TRIM(sc.old_status)) = 'CR-OIL SUSP' AND UPPER(TRIM(sc.new_status)) = 'CR-OIL PUMP')
                  OR (UPPER(TRIM(sc.old_status)) = 'SK-NEW LIC' AND UPPER(TRIM(sc.new_status)) IN ('SK-OIL LIC','SK-BIT LIC'))
              )
        ),
        first_seen AS (
            -- For each (well, transition) combination, find the earliest
            -- date it was ever reported in status_changes. ST2 publishes
            -- the same transition in every weekly snapshot until the well
            -- changes status again — so the "true" event was the first
            -- time we saw it. Anything whose first_seen falls BEFORE the
            -- current digest window has been reported in a prior week
            -- and should not repeat in this one.
            SELECT
                uwi, old_status, new_status,
                MIN(event_date) AS first_seen_date
            FROM pattern_filter
            GROUP BY 1, 2, 3
        ),
        matches AS (
            SELECT pf.*
            FROM pattern_filter pf
            JOIN first_seen fs USING (uwi, old_status, new_status)
            CROSS JOIN bounds b
            WHERE pf.event_date = fs.first_seen_date
              AND fs.first_seen_date >= b.anchor_date - INTERVAL ($days) DAY
              AND fs.first_seen_date <= b.anchor_date
        ),
        with_master AS (
            SELECT
                m.*,
                wc.licensee_name AS master_licensee,
                wc.licensee_code AS master_licensee_code,
                wc.fluid         AS master_fluid,
                wc.well_name     AS master_well_name,
                wc.licence_no    AS master_licence_no,
                COALESCE(wcg.centroid_lat, m.sc_centroid_lat) AS centroid_lat,
                COALESCE(wcg.centroid_lon, m.sc_centroid_lon) AS centroid_lon
            FROM matches m
            LEFT JOIN wells_current wc ON wc.uwi = m.uwi
            LEFT JOIN (
                SELECT uwi, MAX(centroid_lat) AS centroid_lat, MAX(centroid_lon) AS centroid_lon
                FROM wells GROUP BY uwi
            ) wcg ON wcg.uwi = m.uwi
        ),
        with_nearest AS (
            SELECT
                w.*,
                tf.facility_id   AS nearest_facility_id,
                tf.facility_name AS nearest_facility_name,
                ROUND(tf.distance_km, 1) AS distance_km
            FROM with_master w
            LEFT JOIN LATERAL (
                SELECT
                    f.facility_id,
                    f.facility_name,
                    2.0 * 6371.0 * ASIN(SQRT(
                        POW(SIN(RADIANS(f.lat - w.centroid_lat) / 2.0), 2)
                        + COS(RADIANS(w.centroid_lat)) * COS(RADIANS(f.lat))
                          * POW(SIN(RADIANS(f.lon - w.centroid_lon) / 2.0), 2)
                    )) AS distance_km
                FROM temi_facilities f
                WHERE w.centroid_lat IS NOT NULL AND w.centroid_lon IS NOT NULL
                ORDER BY distance_km ASC
                LIMIT 1
            ) tf ON TRUE
        ),
        with_op AS (
            SELECT
                w.*,
                COALESCE(w.master_licensee_code, w.licensee_code) AS resolved_licensee_code,
                COALESCE(
                    w.master_licensee,
                    op4.canonical_name,
                    op5.canonical_name,
                    op_sk.canonical_name,
                    w.sc_licensee_name
                ) AS resolved_operator_name,
                COALESCE(op4.operator_id, op5.operator_id, op_sk.operator_id) AS resolved_operator_id
            FROM with_nearest w
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
            LEFT JOIN operators op_sk
              ON op_sk.operator_id = (
                  SELECT operator_id FROM operator_identifiers oi
                  WHERE oi.identifier_kind = 'sk_legal_name'
                    AND oi.identifier_value = UPPER(TRIM(w.sc_licensee_name))
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
            nearest_facility_id,
            nearest_facility_name,
            distance_km,
            (SELECT anchor_date FROM bounds) AS anchor_date
        FROM with_op
        WHERE distance_km IS NOT NULL AND distance_km < ($max_km)
        ORDER BY resolved_operator_name NULLS LAST, event_date DESC, uwi
        """.replace("($days)", str(days)).replace("($max_km)", str(max_km))
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
            "nearest_facility_name": tx.get("nearest_facility_name"),
            "distance_km": tx.get("distance_km"),
        }
        op["wells"].append(well)
    # Sort: operators with most wells first; within operator, newest event first.
    # Also group wells by pattern_label so the template can render each
    # category once with its wells underneath (less repetitive than per-row labels).
    ops = []
    for op in grouped.values():
        op["provinces"] = ", ".join(sorted(op["provinces"])) if op["provinces"] else None
        op["well_count"] = len(op["wells"])
        op["wells"].sort(key=lambda w: (w["event_date"] is None, w["event_date"] or ""), reverse=True)

        groups_by_label: dict[str, list[dict[str, Any]]] = {}
        for w in op["wells"]:
            groups_by_label.setdefault(w["pattern_label"], []).append(w)
        op["well_groups"] = [
            {"label": label, "count": len(wells), "wells": wells}
            for label, wells in groups_by_label.items()
        ]
        # Group order: most wells first, then label alpha
        op["well_groups"].sort(key=lambda g: (-g["count"], g["label"]))
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
        transitions = fetch_transitions(conn, args.days, args.max_km)
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
        "max_km": args.max_km,
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

    if args.send:
        load_env_file(Path(args.env_file))
        recipients = args.recipient or [
            r.strip() for r in os.environ.get("DIGEST_RECIPIENTS", "").split(",") if r.strip()
        ]
        if not recipients:
            print(
                "ERROR: --send requested but no recipients. Pass --recipient or set DIGEST_RECIPIENTS.",
                file=sys.stderr,
            )
            return 3
        if args.subject:
            subject = args.subject
        else:
            window_label = anchor_date.strftime("%b %d") if anchor_date else "latest"
            subject = (
                f"Well Status Changes — {context['total_wells']} wells, "
                f"week of {window_label}"
            )
        try:
            send_email(html_body=html, subject=subject, recipients=recipients)
            print(f"Sent to {len(recipients)} recipient(s): {', '.join(recipients)}")
        except Exception as e:
            print(f"ERROR: SMTP send failed: {e}", file=sys.stderr)
            return 4

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
