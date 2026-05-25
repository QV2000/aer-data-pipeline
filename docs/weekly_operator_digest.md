# Weekly Operator Digest

Phase 2 adds the rep-facing HTML digest on top of the New Crude Opportunity
report. The digest is operator-grain: one card per operator, with the XLSX still
available as the audit/export artifact.

## SQL Views

Run `docs/sql/new_crude_opportunity_report.sql` first, then
`docs/sql/new_crude_sales_report_views.sql`.

| View | Grain | Purpose |
| --- | --- | --- |
| `weekly_operator_digest` | One row per operator | Section assignment, counts, distance, volume, and MoM movement. |
| `weekly_operator_timeline` | Up to 1-3 rows per operator | Lifecycle snippets rendered inside each email card. |

Digest categories are derived in this order:

1. `new_operator`: `is_new_operator = TRUE`.
2. `near_facility`: existing operator with activity inside 100 km of a TEMI facility.
3. `production_mover`: production restart or step-change event.
4. `other`: retained in the view, excluded from the email.

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
