# Weekly Operator Digest

Phase 2 adds the rep-facing HTML digest on top of the New Crude Opportunity
report. The digest is operator-grain: one card per operator, with the XLSX still
available as the audit/export artifact.

## SQL Views

Run `docs/sql/new_crude_opportunity_report.sql` first, then
`docs/sql/new_crude_sales_report_views.sql`.

| View | Grain | Purpose |
| --- | --- | --- |
| `weekly_operator_digest` | One row per operator/category | Section assignment, counts, distance, volume, and MoM movement. |
| `weekly_operator_timeline` | Up to 1-3 rows per operator/category | Lifecycle snippets rendered inside each email card. |

Digest categories are not mutually exclusive. If an operator has new activity
near a TEMI facility and also has a production movement, the operator can appear
in both sections. This keeps the production-mover section meaningful instead of
hiding those operators behind facility proximity.

Category rules:

1. `new_operator`: `is_new_operator = TRUE` and the operator has a weekly signal.
2. `near_facility`: existing operator with activity inside 100 km of a TEMI facility.
3. `production_mover`: production restart or step-change in the latest production movement month.
4. `other`: retained in the view for actionable non-section operators, excluded from the email.

Rows without an operator id or displayable operator name are filtered out before
aggregation. They are not actionable in a rep email and should be handled as a
data-quality follow-up, not as "Unresolved operator" cards.

The HTML email renders only `new_operator`, `near_facility`, and
`production_mover`. The `other` category remains queryable in
`weekly_operator_digest` for diagnostics and later product decisions, but it is
not included in the Monday rep email.

The new-operator section intentionally remains weekly. The broader trailing
12-month `new_operator_spotlight` workbook sheet is still available, but it is
not repeated in every Monday digest unless one of those operators also has a
fresh weekly signal.

## Dry Run

```bash
python scripts/send_weekly_digest.py \
  --db /data/aer_data.duckdb \
  --dry-run
```

This writes `/tmp/digest_preview.html` and does not send email.

For backfill testing:

```bash
python scripts/send_weekly_digest.py \
  --db /data/aer_data.duckdb \
  --as-of 2026-05-25 \
  --dry-run
```

The script also accepts `--xlsx path/to/new_crude_reports.xlsx`; otherwise it
uses the newest `.xlsx` in `reports/`.

## Delivery Placeholder

Actual delivery is intentionally not required for this phase. The script can
send later through either SMTP (`SMTP_HOST`, `SMTP_PORT`, `SMTP_USERNAME`,
`SMTP_PASSWORD`, `SMTP_FROM`) or a local `sendmail` binary. Recipients come from
`DIGEST_RECIPIENTS` or repeated `--recipient` flags.

## Email Copy

The template avoids directive language such as "call now." Sections use neutral
operator-review language:

- New operators on the radar
- Existing operators, new activity within 100km
- Production movers
