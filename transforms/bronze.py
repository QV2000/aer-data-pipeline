"""
Bronze layer transforms: Raw -> Bronze.

Bronze layer contains:
- Normalized column names (snake_case)
- Consistent date/time parsing (America/Edmonton)
- Metadata columns (_snapshot_date, _source_id)
- Original data structure preserved
"""

import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from registry.loader import DatasetConfig

logger = logging.getLogger(__name__)


def transform_to_bronze(
    df: pd.DataFrame,
    config: DatasetConfig,
    snapshot_date: Optional[str] = None
) -> pd.DataFrame:
    """
    Transform a parsed DataFrame to bronze layer format.

    Args:
        df: Parsed DataFrame from a parser
        config: Dataset configuration
        snapshot_date: Override snapshot date (default: now)

    Returns:
        Bronze-layer DataFrame with metadata columns
    """
    if snapshot_date is None:
        snapshot_date = datetime.now().isoformat()

    # Add metadata columns
    df = df.copy()
    df['_snapshot_date'] = snapshot_date
    df['_source_id'] = config.id
    df['province'] = getattr(config, 'province', 'AB')  # Default AB for backwards compat

    # Parse date columns if identifiable
    date_columns = [c for c in df.columns if any(
        kw in c.lower() for kw in ['date', 'time', 'spud', 'created', 'updated']
    )]

    for col in date_columns:
        if col.startswith('_'):
            continue
        try:
            df[col] = pd.to_datetime(df[col], errors='coerce')
        except Exception:
            pass  # Keep original if parsing fails

    # Optimize memory for low-cardinality columns
    for col in df.columns:
        if col in ('province', 'fluid', 'mode', 'type', 'structure', 'status',
                   'facility_type', 'well_stat_code', 'licence_status', 'activityid',
                   'productid', 'event_type'):
            if col in df.columns:
                try:
                    df[col] = df[col].astype('category')
                except Exception:
                    pass

    logger.info(f"Transformed to bronze: {len(df)} rows, {len(df.columns)} columns")
    return df


def write_bronze(
    df: pd.DataFrame,
    config: DatasetConfig,
    data_dir: Path,
    snapshot_date: Optional[str] = None,
    suffix: str = ""
) -> Path:
    """
    Write a DataFrame to the bronze layer as Parquet.

    Args:
        df: Bronze-layer DataFrame
        config: Dataset configuration
        data_dir: Base data directory
        snapshot_date: Override snapshot date
        suffix: Optional suffix for filename (e.g. for multi-file datasets)

    Returns:
        Path to written Parquet file
    """
    if snapshot_date is None:
        snapshot_date = datetime.now().strftime('%Y-%m-%d')

    bronze_dir = data_dir / "bronze" / config.id
    bronze_dir.mkdir(parents=True, exist_ok=True)

    file_name = f"{snapshot_date}{suffix}.parquet"
    output_path = bronze_dir / file_name


    # Convert object columns to strings to avoid mixed type errors in PyArrow
    # (e.g., location_exception column with both int and str values)
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].astype(str)

    table = pa.Table.from_pandas(df)

    # Atomic write: write to temp file then rename to prevent corrupt parquet on crash
    import tempfile
    fd, tmp_path = tempfile.mkstemp(dir=str(bronze_dir), suffix='.parquet.tmp')
    try:
        os.close(fd)
        pq.write_table(table, tmp_path, compression='snappy')
        os.replace(tmp_path, str(output_path))
    except Exception:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)
        raise

    logger.info(f"Wrote bronze: {output_path} ({len(df)} rows)")
    return output_path


def get_latest_bronze(config: DatasetConfig, data_dir: Path) -> Optional[Path]:
    """Get the path to the latest bronze snapshot for a dataset."""
    bronze_dir = data_dir / "bronze" / config.id

    if not bronze_dir.exists():
        return None

    parquet_files = sorted(bronze_dir.glob("*.parquet"))
    if not parquet_files:
        return None

    return parquet_files[-1]


def read_bronze(config: DatasetConfig, data_dir: Path) -> Optional[pd.DataFrame]:
    """Read the latest bronze snapshot for a dataset."""
    latest = get_latest_bronze(config, data_dir)

    if latest is None:
        return None

    return pd.read_parquet(latest)
