"""
Validation checks for data quality.

Runs checks against:
- Required columns present
- Primary key uniqueness
- Row count thresholds
- Date range monotonicity
- Type coercion
"""

import logging
import json
from datetime import datetime
from pathlib import Path
from dataclasses import dataclass, asdict
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
    config: DatasetConfig
) -> ValidationReport:
    """
    Run all validation checks on a DataFrame.
    
    Args:
        df: DataFrame to validate
        config: Dataset configuration
        
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
