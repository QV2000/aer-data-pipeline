"""
Schema definitions for key datasets.

Used for post-parse validation to detect upstream AER schema changes early.
Logs warnings for missing or unexpected columns but does not block ingestion.
"""

import logging

logger = logging.getLogger(__name__)

# Expected columns and their Python/Pandas dtypes for key datasets.
# This is not exhaustive. It covers the most important columns that
# downstream code relies on.
EXPECTED_SCHEMAS = {
    "st37_wells_txt": {
        "columns": {
            "uwi": "str",
            "uwi_id": "str",
            "well_name": "str",
            "field_code": "str",
            "licence_no": "str",
            "licence_status": "str",
            "licensee_code": "str",
            "well_stat_code": "str",
            "well_stat_date": "str",
            "mode": "str",
            "fluid": "str",
            "type": "str",
        },
    },
    "petrinex_production": {
        "columns": {
            "productionmonth": "str",
            "activityid": "str",
            "productid": "str",
            "volume": "str",
        },
    },
    "st102_active_facilities": {
        "columns": {
            "facility_id": "str",
            "facility_name": "str",
            "operator_name": "str",
        },
    },
    "st49_daily_spud": {
        "columns": {
            "uwi": "str",
            "spud_date": "object",
            "licence_number": "str",
        },
    },
    "st1_well_licences": {
        "columns": {
            "licence_number": "str",
            "uwi": "str",
            "well_name": "str",
        },
    },
    "st2_weekly_status_changes": {
        "columns": {
            "uwi": "str",
            "new_status": "str",
            "event_date": "object",
        },
    },
}


def validate_schema(df, dataset_id: str) -> bool:
    """Validate a parsed DataFrame against the expected schema.

    Logs warnings for missing or extra columns but never raises.
    Returns True if schema matches, False if there are discrepancies.
    """
    expected = EXPECTED_SCHEMAS.get(dataset_id)
    if expected is None:
        return True  # No schema defined for this dataset

    expected_cols = set(expected["columns"].keys())
    actual_cols = set(df.columns)

    missing = expected_cols - actual_cols
    extra = actual_cols - expected_cols - {"_snapshot_date", "_source_id", "province"}

    if missing:
        logger.warning(
            f"Schema drift in {dataset_id}: missing expected columns {sorted(missing)}. "
            f"Upstream may have changed format."
        )

    if extra and len(extra) < 20:  # Only log if not too many (first parse might have many)
        logger.info(f"Schema note for {dataset_id}: {len(extra)} extra columns found")

    return len(missing) == 0
