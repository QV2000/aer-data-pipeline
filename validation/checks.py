"""
Validation checks for data quality.

Runs checks against:
- Required columns present
- Primary key uniqueness
- Row count thresholds
- Date range monotonicity
- Type coercion
"""

import json
import logging
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd

from registry.loader import DatasetConfig

logger = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    """Result of a single validation check."""
    check_name: str
    passed: bool
    message: str
    details: Optional[dict] = None


@dataclass
class ValidationReport:
    """Complete validation report for a dataset."""
    dataset_id: str
    timestamp: str
    passed: bool
    row_count: int
    results: list[ValidationResult]

    def to_dict(self) -> dict:
        return {
            'dataset_id': self.dataset_id,
            'timestamp': self.timestamp,
            'passed': self.passed,
            'row_count': self.row_count,
            'results': [asdict(r) for r in self.results]
        }


def validate_dataframe(
    df: pd.DataFrame,
    config: DatasetConfig,
    data_dir: Optional[Path] = None
) -> ValidationReport:
    """
    Run all validation checks on a DataFrame.

    Args:
        df: DataFrame to validate
        config: Dataset configuration
        data_dir: Optional path to data directory (needed for drift checks)

    Returns:
        ValidationReport with all check results
    """
    results = []

    # Check required columns
    results.append(check_required_columns(df, config))

    # Check primary key uniqueness
    results.append(check_primary_key_uniqueness(df, config))

    # Check minimum row count
    results.append(check_min_rows(df, config))

    # Check for monotonic dates if configured
    if config.validation.check_monotonic_dates:
        results.append(check_monotonic_dates(df, config))

    # Check null rates on required columns
    results.append(check_null_rates(df, config))

    # Check row count drift (needs data_dir for previous snapshot comparison)
    if data_dir is not None:
        results.append(check_row_count_drift(df, config, data_dir))

    # Check UWI format if applicable
    results.append(check_uwi_format(df, config))

    # Check date value sanity
    results.append(check_date_sanity(df, config))

    # Overall pass/fail
    all_passed = all(r.passed for r in results)

    return ValidationReport(
        dataset_id=config.id,
        timestamp=datetime.now().isoformat(),
        passed=all_passed,
        row_count=len(df),
        results=results
    )


def check_required_columns(df: pd.DataFrame, config: DatasetConfig) -> ValidationResult:
    """Check that all required columns are present."""
    required = config.schema.required_columns

    if not required:
        return ValidationResult(
            check_name="required_columns",
            passed=True,
            message="No required columns specified"
        )

    missing = [c for c in required if c not in df.columns]

    if missing:
        return ValidationResult(
            check_name="required_columns",
            passed=False,
            message=f"Missing required columns: {missing}",
            details={'missing': missing, 'present': list(df.columns)}
        )

    return ValidationResult(
        check_name="required_columns",
        passed=True,
        message=f"All {len(required)} required columns present"
    )


def check_primary_key_uniqueness(df: pd.DataFrame, config: DatasetConfig) -> ValidationResult:
    """Check that primary key columns are unique."""
    pk_cols = config.schema.primary_keys

    if not pk_cols:
        return ValidationResult(
            check_name="primary_key_uniqueness",
            passed=True,
            message="No primary keys specified"
        )

    # Check if all PK columns exist
    missing_pk = [c for c in pk_cols if c not in df.columns]
    if missing_pk:
        return ValidationResult(
            check_name="primary_key_uniqueness",
            passed=False,
            message=f"Primary key columns not found: {missing_pk}"
        )

    # Check uniqueness
    duplicates = df.duplicated(subset=pk_cols, keep=False)
    dup_count = duplicates.sum()

    if dup_count > 0:
        return ValidationResult(
            check_name="primary_key_uniqueness",
            passed=False,
            message=f"{dup_count} duplicate primary key rows",
            details={'duplicate_count': int(dup_count), 'pk_columns': pk_cols}
        )

    return ValidationResult(
        check_name="primary_key_uniqueness",
        passed=True,
        message=f"Primary key ({pk_cols}) is unique"
    )


def check_min_rows(df: pd.DataFrame, config: DatasetConfig) -> ValidationResult:
    """Check that row count meets minimum threshold."""
    min_rows = config.validation.min_rows
    actual_rows = len(df)

    if actual_rows < min_rows:
        return ValidationResult(
            check_name="min_rows",
            passed=False,
            message=f"Row count {actual_rows} below minimum {min_rows}",
            details={'actual': actual_rows, 'minimum': min_rows}
        )

    return ValidationResult(
        check_name="min_rows",
        passed=True,
        message=f"Row count {actual_rows} meets minimum {min_rows}"
    )


def check_monotonic_dates(df: pd.DataFrame, config: DatasetConfig) -> ValidationResult:
    """Check that date columns grow monotonically (for time series)."""
    date_cols = [c for c in df.columns if 'date' in c.lower() or 'month' in c.lower()]

    if not date_cols:
        return ValidationResult(
            check_name="monotonic_dates",
            passed=True,
            message="No date columns found to check"
        )

    # Check the first date column
    date_col = date_cols[0]

    try:
        dates = pd.to_datetime(df[date_col], errors='coerce')
        dates_clean = dates.dropna().sort_values()

        if len(dates_clean) == 0:
            return ValidationResult(
                check_name="monotonic_dates",
                passed=True,
                message=f"No valid dates in {date_col}"
            )

        # Check for gaps or reversals
        min_date = dates_clean.min()
        max_date = dates_clean.max()

        return ValidationResult(
            check_name="monotonic_dates",
            passed=True,
            message=f"Date range: {min_date} to {max_date}",
            details={'min_date': str(min_date), 'max_date': str(max_date)}
        )

    except Exception as e:
        return ValidationResult(
            check_name="monotonic_dates",
            passed=False,
            message=f"Error checking dates: {e}"
        )


def check_row_count_drift(df: pd.DataFrame, config: DatasetConfig, data_dir: Path) -> ValidationResult:
    """Check if row count changed more than max_row_delta_pct from previous snapshot."""
    max_delta = config.validation.max_row_delta_pct
    bronze_path = data_dir / "bronze" / config.id
    prev_files = sorted(bronze_path.glob("*.parquet")) if bronze_path.exists() else []

    if len(prev_files) < 2:
        return ValidationResult("row_count_drift", True, "No previous snapshot to compare")

    import pyarrow.parquet as pq
    prev_count = pq.read_metadata(str(prev_files[-2])).num_rows
    curr_count = len(df)

    if prev_count == 0:
        return ValidationResult("row_count_drift", True, "Previous snapshot was empty")

    delta_pct = abs(curr_count - prev_count) / prev_count * 100
    if delta_pct > max_delta:
        return ValidationResult(
            "row_count_drift", False,
            f"Row count changed {delta_pct:.1f}% ({prev_count} -> {curr_count}, threshold={max_delta}%)"
        )
    return ValidationResult("row_count_drift", True, f"Row count delta: {delta_pct:.1f}%")


def check_null_rates(df: pd.DataFrame, config: DatasetConfig) -> ValidationResult:
    """Flag required columns with more than 50% null values."""
    required = config.schema.required_columns
    if not required:
        return ValidationResult("null_rates", True, "No required columns specified")

    high_null = {}
    for col in required:
        if col in df.columns:
            null_pct = df[col].isna().mean() * 100
            if null_pct > 50:
                high_null[col] = f"{null_pct:.1f}%"

    if high_null:
        return ValidationResult(
            "null_rates", False,
            f"Required columns with >50% nulls: {high_null}"
        )
    return ValidationResult("null_rates", True, "Null rates acceptable")


def check_uwi_format(df: pd.DataFrame, config: DatasetConfig) -> ValidationResult:
    """Validate UWI format on a sample of rows (XX/XX-XX-XXX-XXWX/X pattern)."""
    if 'uwi' not in df.columns:
        return ValidationResult("uwi_format", True, "No uwi column present")

    import re
    uwi_pattern = re.compile(r'^\d{2}/\d{2}-\d{2}-\d{3}-\d{2}W\d/\d+$')

    sample = df['uwi'].dropna().head(1000)
    if len(sample) == 0:
        return ValidationResult("uwi_format", True, "No non-null UWIs to check")

    valid = sample.apply(lambda x: bool(uwi_pattern.match(str(x))))
    valid_pct = valid.mean() * 100

    if valid_pct < 50:
        return ValidationResult(
            "uwi_format", False,
            f"Only {valid_pct:.0f}% of sampled UWIs match expected format (XX/XX-XX-XXX-XXWX/X)"
        )
    return ValidationResult("uwi_format", True, f"{valid_pct:.0f}% of UWIs match expected format")


def check_date_sanity(df: pd.DataFrame, config: DatasetConfig) -> ValidationResult:
    """Check that date columns contain reasonable values (no future, no pre-1900)."""
    date_cols = [c for c in df.columns if 'date' in c.lower() or 'month' in c.lower()]
    if not date_cols:
        return ValidationResult("date_sanity", True, "No date columns found")

    issues = []
    for col in date_cols[:3]:  # Check up to 3 date columns
        try:
            dates = pd.to_datetime(df[col], errors='coerce').dropna()
            if len(dates) == 0:
                continue

            future = (dates > pd.Timestamp.now() + pd.Timedelta(days=60)).sum()
            ancient = (dates < pd.Timestamp('1900-01-01')).sum()

            if future > 0:
                issues.append(f"{col}: {future} future dates")
            if ancient > 0:
                issues.append(f"{col}: {ancient} pre-1900 dates")
        except Exception:
            continue

    if issues:
        return ValidationResult("date_sanity", False, f"Date issues: {'; '.join(issues)}")
    return ValidationResult("date_sanity", True, "Date values within expected range")


def write_validation_report(
    report: ValidationReport,
    data_dir: Path
) -> Path:
    """Write validation report to JSON file."""
    reports_dir = data_dir / "validation_reports"
    reports_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_path = reports_dir / f"{report.dataset_id}_{timestamp}.json"

    with open(output_path, 'w') as f:
        json.dump(report.to_dict(), f, indent=2)

    logger.info(f"Wrote validation report: {output_path}")
    return output_path
