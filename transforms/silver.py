"""
Silver layer transforms: Bronze -> Silver.

Silver layer contains curated, analysis-ready tables:
- wells: Master well dimension table
- facilities: Facility dimension table
- pipelines_lines: Pipeline line geometry
- monthly_production: Time series with amendment handling
"""

import logging
from pathlib import Path
from typing import Optional, Union

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from registry.loader import DatasetConfig, SourceRegistry

logger = logging.getLogger(__name__)


def _trim_join_keys(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """
    Trim whitespace from join key columns.

    This is critical for reliable joins across AER datasets - many have leading/trailing
    whitespace that causes join failures (e.g., licence numbers, UWIs).
    """
    for col in columns:
        if col in df.columns:
            # Only trim string columns
            if df[col].dtype == 'object' or pd.api.types.is_string_dtype(df[col]):
                df[col] = df[col].astype(str).str.strip()
                # Convert 'nan' strings back to actual NaN
                df.loc[df[col] == 'nan', col] = None
    return df


def _facility_source_series(df: pd.DataFrame) -> pd.Series:
    if 'sub_type' in df.columns:
        return df['sub_type'].fillna('')
    if 'facility_type' in df.columns:
        return df['facility_type'].fillna('')
    if 'facility_name' in df.columns:
        return df['facility_name'].fillna('')
    return pd.Series([''] * len(df), index=df.index)


def _classify_facility_class(text: str) -> str:
    t = str(text or '').lower()
    if not t:
        return 'other'
    if 'battery' in t:
        if any(k in t for k in ['bitumen', 'sagd', 'thermal', 'oil sands']):
            return 'bitumen_battery'
        if any(k in t for k in ['gas', 'ngl']):
            return 'gas_battery'
        if any(k in t for k in ['oil', 'crude']):
            return 'oil_battery'
        return 'oil_battery'
    if any(k in t for k in ['plant', 'fractionation', 'refinery', 'sulphur']):
        if 'gas' in t:
            return 'gas_plant'
        return 'plant'
    if 'compressor' in t:
        return 'compressor'
    if any(k in t for k in ['gathering', 'meter', 'regulator']):
        return 'gathering' if 'gathering' in t else 'meter'
    if any(k in t for k in ['terminal', 'tank', 'storage']):
        return 'terminal' if 'terminal' in t or 'tank' in t else 'storage'
    if any(k in t for k in ['disposal', 'inject', 'injection', 'waste']):
        return 'disposal'
    if 'treat' in t:
        return 'treating'
    return 'other'


def _classify_facility_group(facility_class: str) -> str:
    class_map = {
        'oil_battery': 'Battery',
        'gas_battery': 'Battery',
        'bitumen_battery': 'Battery',
        'gas_plant': 'Plant',
        'plant': 'Plant',
        'compressor': 'Plant',
        'treating': 'Plant',
        'gathering': 'Gathering/Meter',
        'meter': 'Gathering/Meter',
        'terminal': 'Storage/Terminal',
        'storage': 'Storage/Terminal',
        'disposal': 'Injection/Disposal',
    }
    return class_map.get(facility_class, 'Other')


def _classify_facility_commodity(text: str) -> str:
    t = str(text or '').lower()
    if any(k in t for k in ['crude', 'oil', 'bitumen', 'sagd', 'thermal', 'oil sands']):
        return 'Crude'
    if 'gas' in t or 'ngl' in t:
        return 'Gas'
    return 'Other'


def build_facilities_table(
    data_dir: Path,
    registry: Optional[SourceRegistry] = None
) -> pd.DataFrame:
    """
    Build the silver facilities table from ST102 bronze data (AB) and
    SK Petrinex facility data.

    Combines active and inactive facilities and joins with geocoded shapefile data.
    """
    if registry is None:
        registry = SourceRegistry()

    dfs = []

    # Load AB active facilities
    try:
        active_config = registry.get('st102_active_facilities')
        active_path = data_dir / "bronze" / active_config.id
        active_files = sorted(active_path.glob("*.parquet")) if active_path.exists() else []

        if active_files:
            df_active = pd.read_parquet(active_files[-1])
            df_active['is_active'] = True
            if 'province' not in df_active.columns:
                df_active['province'] = 'AB'
            dfs.append(df_active)
            logger.info(f"Loaded {len(df_active)} AB active facilities")
    except KeyError:
        pass

    # Load AB inactive facilities
    try:
        inactive_config = registry.get('st102_inactive_facilities')
        inactive_path = data_dir / "bronze" / inactive_config.id
        inactive_files = sorted(inactive_path.glob("*.parquet")) if inactive_path.exists() else []

        if inactive_files:
            df_inactive = pd.read_parquet(inactive_files[-1])
            df_inactive['is_active'] = False
            if 'province' not in df_inactive.columns:
                df_inactive['province'] = 'AB'
            dfs.append(df_inactive)
            logger.info(f"Loaded {len(df_inactive)} AB inactive facilities")
    except KeyError:
        pass

    # Load SK Petrinex facility infrastructure
    try:
        sk_fac_config = registry.get('sk_petrinex_facility_infrastructure')
        sk_fac_path = data_dir / "bronze" / sk_fac_config.id
        sk_fac_files = sorted(sk_fac_path.glob("*.parquet")) if sk_fac_path.exists() else []

        if sk_fac_files:
            df_sk = pd.read_parquet(sk_fac_files[-1])
            df_sk['is_active'] = True  # Petrinex infrastructure is active
            if 'province' not in df_sk.columns:
                df_sk['province'] = 'SK'
            dfs.append(df_sk)
            logger.info(f"Loaded {len(df_sk)} SK facilities from Petrinex")
    except KeyError:
        pass

    # Load SK new/active facilities report
    try:
        sk_active_config = registry.get('sk_new_active_facilities')
        sk_active_path = data_dir / "bronze" / sk_active_config.id
        sk_active_files = sorted(sk_active_path.glob("*.parquet")) if sk_active_path.exists() else []

        if sk_active_files:
            df_sk_active = pd.read_parquet(sk_active_files[-1])
            df_sk_active['is_active'] = True
            if 'province' not in df_sk_active.columns:
                df_sk_active['province'] = 'SK'
            dfs.append(df_sk_active)
            logger.info(f"Loaded {len(df_sk_active)} SK active facilities")
    except KeyError:
        pass

    # Load SK suspended facilities
    try:
        sk_susp_config = registry.get('sk_suspended_facilities')
        sk_susp_path = data_dir / "bronze" / sk_susp_config.id
        sk_susp_files = sorted(sk_susp_path.glob("*.parquet")) if sk_susp_path.exists() else []

        if sk_susp_files:
            df_sk_susp = pd.read_parquet(sk_susp_files[-1])
            df_sk_susp['is_active'] = False
            if 'province' not in df_sk_susp.columns:
                df_sk_susp['province'] = 'SK'
            dfs.append(df_sk_susp)
            logger.info(f"Loaded {len(df_sk_susp)} SK suspended facilities")
    except KeyError:
        pass

    if not dfs:
        logger.warning("No facility data found in bronze layer (AB or SK)")
        return pd.DataFrame()
    
    # Combine metadata
    df_meta = pd.concat(dfs, ignore_index=True)

    # Ensure province column exists
    if 'province' not in df_meta.columns:
        df_meta['province'] = 'AB'

    # Load geocoded data (shapefile) - AB only
    df_geo = pd.DataFrame()
    try:
        geo_config = registry.get('st102_facilities_shapefile')
        geo_path = data_dir / "bronze" / geo_config.id
        geo_files = sorted(geo_path.glob("*.parquet")) if geo_path.exists() else []

        if geo_files:
            df_geo = pd.read_parquet(geo_files[-1])
            # Standardize for join
            df_geo = df_geo[['fac_id', 'centroid_lon', 'centroid_lat']].rename(columns={
                'fac_id': 'facility_id'
            })
            logger.info(f"Loaded {len(df_geo)} geocoded AB facilities")
    except KeyError:
        pass

    # Join coordinates (only applies to AB facilities with matching IDs)
    if not df_geo.empty:
        df = df_meta.merge(df_geo, on='facility_id', how='left')
        logger.info(f"Geocoded facility coverage: {df['centroid_lat'].notna().sum()}/{len(df)}")
    else:
        df = df_meta
        df['centroid_lon'] = None
        df['centroid_lat'] = None

    # Add facility classification columns
    source_series = _facility_source_series(df)
    df['facility_class'] = source_series.apply(_classify_facility_class)
    df['facility_group'] = df['facility_class'].apply(_classify_facility_group)
    df['facility_commodity'] = source_series.apply(_classify_facility_commodity)

    # TRIM all join keys to ensure reliable joins
    df = _trim_join_keys(df, ['facility_id', 'operator_ba_id', 'licensee_code'])

    # Clean up metadata columns for silver
    df = df.drop(columns=['_snapshot_date', '_source_id'], errors='ignore')

    # Add silver metadata
    df['_silver_version'] = pd.Timestamp.now().isoformat()

    logger.info(f"Built facilities silver table: {len(df)} rows (AB + SK)")
    return df


def _load_facility_snapshots(
    data_dir: Path,
    registry: SourceRegistry
) -> pd.DataFrame:
    dfs = []

    configs = [
        ('st102_active_facilities', True),
        ('st102_inactive_facilities', False)
    ]

    for config_id, is_active in configs:
        try:
            config = registry.get(config_id)
        except KeyError:
            continue

        bronze_path = data_dir / "bronze" / config.id
        files = sorted(bronze_path.glob("*.parquet")) if bronze_path.exists() else []

        for f in files:
            try:
                df = pd.read_parquet(f)
                if '_snapshot_date' not in df.columns:
                    df['_snapshot_date'] = f.stem
                df['is_active'] = is_active
                dfs.append(df)
            except Exception as e:
                logger.warning(f"Failed to read {f}: {e}")

    if not dfs:
        return pd.DataFrame()

    df_all = pd.concat(dfs, ignore_index=True)
    df_all['_snapshot_date'] = pd.to_datetime(df_all['_snapshot_date'], errors='coerce')
    return df_all


def _enrich_facility_geo(
    df: pd.DataFrame,
    data_dir: Path,
    registry: SourceRegistry
) -> pd.DataFrame:
    try:
        geo_config = registry.get('st102_facilities_shapefile')
        geo_path = data_dir / "bronze" / geo_config.id
        geo_files = sorted(geo_path.glob("*.parquet")) if geo_path.exists() else []
        if not geo_files:
            return df
        df_geo = pd.read_parquet(geo_files[-1])
        df_geo = df_geo[['fac_id', 'centroid_lon', 'centroid_lat']].rename(columns={
            'fac_id': 'facility_id'
        })
        return df.merge(df_geo, on='facility_id', how='left')
    except Exception as e:
        logger.warning(f"Could not enrich facility history with coordinates: {e}")
        return df


def build_facility_history_table(
    data_dir: Path,
    registry: Optional[SourceRegistry] = None
) -> pd.DataFrame:
    """
    Build facility history table from ST102 snapshots.
    Tracks first/last seen dates and active status over time.
    """
    if registry is None:
        registry = SourceRegistry()

    df_all = _load_facility_snapshots(data_dir, registry)
    if df_all.empty:
        logger.warning("No facility snapshots found for history table")
        return pd.DataFrame()

    df_all = df_all.sort_values(['facility_id', '_snapshot_date'])

    # Latest row per facility
    latest = df_all.groupby('facility_id').last().reset_index()

    # First/last seen
    seen = df_all.groupby('facility_id')['_snapshot_date'].agg(['min', 'max']).reset_index()
    seen = seen.rename(columns={'min': 'first_seen', 'max': 'last_seen'})

    # Active date range
    active_only = df_all[df_all['is_active'] == True]
    active_dates = active_only.groupby('facility_id')['_snapshot_date'].agg(['min', 'max']).reset_index()
    active_dates = active_dates.rename(columns={'min': 'first_active_date', 'max': 'last_active_date'})

    result = latest.merge(seen, on='facility_id', how='left')
    result = result.merge(active_dates, on='facility_id', how='left')
    result['was_ever_active'] = result['first_active_date'].notna()
    result['is_currently_active'] = result['is_active'].fillna(False)

    result = _enrich_facility_geo(result, data_dir, registry)

    # Add classification if missing
    source_series = _facility_source_series(result)
    if 'facility_class' not in result.columns:
        result['facility_class'] = source_series.apply(_classify_facility_class)
    if 'facility_group' not in result.columns:
        result['facility_group'] = result['facility_class'].apply(_classify_facility_group)
    if 'facility_commodity' not in result.columns:
        result['facility_commodity'] = source_series.apply(_classify_facility_commodity)

    result = result.drop(columns=['_snapshot_date', '_source_id'], errors='ignore')
    result['_silver_version'] = pd.Timestamp.now().isoformat()
    logger.info(f"Built facility_history silver table: {len(result)} rows")
    return result


def build_facility_status_changes_table(
    data_dir: Path,
    registry: Optional[SourceRegistry] = None
) -> pd.DataFrame:
    """
    Build facility status change events by diffing ST102 snapshots.
    """
    if registry is None:
        registry = SourceRegistry()

    df_all = _load_facility_snapshots(data_dir, registry)
    if df_all.empty:
        logger.warning("No facility snapshots found for status change table")
        return pd.DataFrame()

    df_all = df_all.sort_values(['facility_id', '_snapshot_date'])
    df_all['prev_active'] = df_all.groupby('facility_id')['is_active'].shift(1)

    # New facilities (first seen)
    first_rows = df_all.groupby('facility_id').first().reset_index()
    first_rows = first_rows.assign(
        old_status=None,
        new_status=first_rows['is_active'].map(lambda v: 'Active' if v else 'Inactive'),
        event_date=first_rows['_snapshot_date']
    )

    # Status transitions
    changes = df_all[df_all['prev_active'].notna() & (df_all['is_active'] != df_all['prev_active'])].copy()
    changes['old_status'] = changes['prev_active'].map(lambda v: 'Active' if v else 'Inactive')
    changes['new_status'] = changes['is_active'].map(lambda v: 'Active' if v else 'Inactive')
    changes['event_date'] = changes['_snapshot_date']

    events = pd.concat([first_rows, changes], ignore_index=True)

    events = _enrich_facility_geo(events, data_dir, registry)

    source_series = _facility_source_series(events)
    if 'facility_class' not in events.columns:
        events['facility_class'] = source_series.apply(_classify_facility_class)
    if 'facility_group' not in events.columns:
        events['facility_group'] = events['facility_class'].apply(_classify_facility_group)
    if 'facility_commodity' not in events.columns:
        events['facility_commodity'] = source_series.apply(_classify_facility_commodity)

    keep_cols = [
        'facility_id', 'facility_name', 'operator_name', 'facility_type', 'sub_type',
        'event_date', 'old_status', 'new_status', 'is_active',
        'facility_class', 'facility_group', 'facility_commodity',
        'centroid_lat', 'centroid_lon'
    ]
    keep_cols = [c for c in keep_cols if c in events.columns]
    events = events[keep_cols]

    events = events.drop(columns=['_source_id'], errors='ignore')
    events['_silver_version'] = pd.Timestamp.now().isoformat()
    logger.info(f"Built facility_status_changes silver table: {len(events)} rows")
    return events



def format_petrinex_uwi(raw_uwi: str) -> str:
    """Format Petrinex identifier to match ST37 wells table UWI format.

    Petrinex uses: ABWI100010100308W402 or SKWI100010100308W202 (20 chars with prefix)
    ST37 wells use: 00/01-01-003-08W4/2 (19 chars with separators)

    The mapping:
    - Strip 'ABWI' or 'SKWI' prefix
    - Position breakdown of remaining 16 chars:
      [0:3] = location exception (but first char is type indicator, drop it)
      [3:5] = LSD
      [5:7] = Section
      [7:10] = Township
      [10:12] = Range
      [12:14] = Meridian (e.g., W4 for AB, W2/W3 for SK)
      [14:16] = Event sequence (drop leading zero)
    """
    # Strip ABWI or SKWI prefix if present
    upper_uwi = raw_uwi.upper()
    if upper_uwi.startswith('ABWI') or upper_uwi.startswith('SKWI'):
        raw_uwi = raw_uwi[4:]
    
    if len(raw_uwi) != 16:
        return raw_uwi
    
    # Parse the 16-char identifier
    loc_exc = raw_uwi[0:3]   # e.g., "100" -> we want "00"
    lsd = raw_uwi[3:5]       # e.g., "01"
    sec = raw_uwi[5:7]       # e.g., "01"
    twp = raw_uwi[7:10]      # e.g., "003"
    rg = raw_uwi[10:12]      # e.g., "08"
    mer = raw_uwi[12:14]     # e.g., "W4"
    evt = raw_uwi[14:16]     # e.g., "02" -> we want "2"
    
    # Match ST37 wells format:
    # - Location exception: take last 2 chars (drop leading type indicator)
    # - Event: convert to int to drop leading zeros, then back to string
    loc_fmt = loc_exc[-2:]  # "100" -> "00", "102" -> "02"
    evt_fmt = str(int(evt)) # "02" -> "2", "00" -> "0"
    
    return f"{loc_fmt}/{lsd}-{sec}-{twp}-{rg}{mer}/{evt_fmt}"

def _build_production_month_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert a single-month Petrinex bronze dataframe into the production silver schema.
    Designed to be run per-month to keep memory bounded.
    """
    # Normalize common header variants (depends on parser + upstream files)
    rename_cols = {}
    if 'production_month' in df.columns and 'productionmonth' not in df.columns:
        rename_cols['production_month'] = 'productionmonth'
    if 'activity_id' in df.columns and 'activityid' not in df.columns:
        rename_cols['activity_id'] = 'activityid'
    if 'product_id' in df.columns and 'productid' not in df.columns:
        rename_cols['product_id'] = 'productid'
    if rename_cols:
        df = df.rename(columns=rename_cols)

    # Deduplicate within the month (Petrinex can contain amendments in re-pulls)
    candidate_key_sets: list[list[str]] = [
        ['fromtoididentifier', 'productionmonth', 'activityid', 'productid'],
        ['from_to_id_identifier', 'production_month', 'activity_id', 'product_id'],
        ['uwi', 'productionmonth'],
    ]

    dedupe_keys: list[str] = []
    for ks in candidate_key_sets:
        if all(k in df.columns for k in ks):
            dedupe_keys = ks
            break

    if dedupe_keys:
        if '_snapshot_date' in df.columns:
            df = df.sort_values('_snapshot_date')
        df = df.drop_duplicates(subset=dedupe_keys, keep='last')

    # Pivot logic for Petrinex (where Activity/Product are rows)
    if 'activityid' in df.columns and 'productid' in df.columns:
        df_prod = df[df['activityid'] == 'PROD'].copy()

        if df_prod.empty:
            return pd.DataFrame()

        # Ensure we have `uwi` (Petrinex can use different identifier column names)
        if 'uwi' not in df_prod.columns:
            id_candidates = [
                'fromtoididentifier',
                'from_to_id_identifier',
                'fromtoidentifier',
                'from_to_identifier',
                'fromtoid',
                'from_to_id',
            ]
            id_col = next((c for c in id_candidates if c in df_prod.columns), None)
            if id_col:
                df_prod['uwi'] = df_prod[id_col].astype(str).apply(format_petrinex_uwi)
            else:
                logger.warning("Petrinex production missing identifier column for UWI; skipping month")
                return pd.DataFrame()

        # Ensure numeric volume (some parsers historically mapped 'Volume' -> oil_prod_vol)
        value_col = 'volume' if 'volume' in df_prod.columns else 'oil_prod_vol'
        if value_col not in df_prod.columns:
            logger.warning("Petrinex production missing volume column; skipping month")
            return pd.DataFrame()

        df_prod[value_col] = pd.to_numeric(df_prod[value_col], errors='coerce').fillna(0)

        # Include province in pivot index to preserve it through aggregation
        pivot_index = ['productionmonth', 'uwi']
        if 'province' in df_prod.columns:
            pivot_index.append('province')

        pivot_df = df_prod.pivot_table(
            index=pivot_index,
            columns='productid',
            values=value_col,
            aggfunc='sum'
        ).reset_index()

        rename_map = {'OIL': 'oil_prod_vol', 'GAS': 'gas_prod_vol', 'WATER': 'water_prod_vol'}
        pivot_df = pivot_df.rename(columns=rename_map)

        # Ensure province column exists (default to AB for backwards compat)
        if 'province' not in pivot_df.columns:
            pivot_df['province'] = 'AB'

        for col in rename_map.values():
            if col not in pivot_df.columns:
                pivot_df[col] = 0
            else:
                pivot_df[col] = pivot_df[col].fillna(0)

        df = pivot_df
    else:
        # Not a Petrinex-shaped DF; nothing to do
        return pd.DataFrame()

    # TRIM UWI join key to ensure reliable joins
    df = _trim_join_keys(df, ['uwi'])

    # Final cleanup
    df = df.drop(columns=['_silver_version'], errors='ignore')
    df['_silver_version'] = pd.Timestamp.now().isoformat()
    return df


def build_production_table(
    data_dir: Path,
    registry: Optional[SourceRegistry] = None
) -> Union[pd.DataFrame, Path]:
    """
    Build the silver production table from Petrinex AB and SK production data.
    Handles pivoting and UWI formatting.

    IMPORTANT: To avoid OOM on memory-constrained instances, this function writes
    per-month partition parquets to `silver/production/parts/*.parquet`
    and returns the parts directory path.
    """
    if registry is None:
        registry = SourceRegistry()

    # Collect bronze files from both AB and SK Petrinex production
    # Each entry is (province_prefix, file_path) to avoid filename collisions
    tagged_files = []

    # AB Petrinex production
    try:
        config_ab = registry.get('petrinex_production')
        bronze_path_ab = data_dir / "bronze" / config_ab.id
        if bronze_path_ab.exists():
            for f in sorted(bronze_path_ab.glob("*.parquet")):
                tagged_files.append(('AB', f))
    except KeyError:
        pass

    # SK Petrinex production
    try:
        config_sk = registry.get('sk_petrinex_production')
        bronze_path_sk = data_dir / "bronze" / config_sk.id
        if bronze_path_sk.exists():
            for f in sorted(bronze_path_sk.glob("*.parquet")):
                tagged_files.append(('SK', f))
    except KeyError:
        pass

    if not tagged_files:
        logger.warning("No production data found in bronze layer (AB or SK)")
        return pd.DataFrame()

    parts_dir = data_dir / "silver" / "production" / "parts"
    parts_dir.mkdir(parents=True, exist_ok=True)

    # Remove old un-prefixed part files (legacy format that caused AB/SK overwrites)
    for old_file in parts_dir.glob("[0-9][0-9][0-9][0-9]-[0-9][0-9].parquet"):
        old_file.unlink()
        logger.info(f"Removed legacy production part: {old_file.name}")

    # Process each month independently to keep memory bounded.
    # Prefix output with province so AB and SK don't overwrite each other.
    wrote_any = False
    for province, f in tagged_files:
        month = f.stem  # expected YYYY-MM
        out_path = parts_dir / f"{province}_{month}.parquet"

        try:
            df_month = pd.read_parquet(f)
            df_out = _build_production_month_df(df_month)
            del df_month

            if df_out is None or df_out.empty:
                continue

            table = pa.Table.from_pandas(df_out)
            pq.write_table(table, out_path, compression='snappy')
            wrote_any = True
            logger.info(f"Wrote production month: {out_path} ({len(df_out)} rows)")
            del df_out
        except Exception as e:
            logger.error(f"Failed to build production month {month} ({province}) from {f.name}: {e}")

    if not wrote_any:
        logger.warning("No production partitions were written")
        return pd.DataFrame()

    # Also write a small 'latest.parquet' for convenience (latest month only),
    # but DuckDB should read from parts/ to avoid duplication.
    try:
        latest_file = max(files, key=lambda p: p.stem)
        latest_month = latest_file.stem
        df_latest = pd.read_parquet(latest_file)
        df_latest_out = _build_production_month_df(df_latest)
        del df_latest
        if df_latest_out is not None and not df_latest_out.empty:
            silver_dir = data_dir / "silver" / "production"
            silver_dir.mkdir(parents=True, exist_ok=True)
            latest_path = silver_dir / "latest.parquet"
            pq.write_table(pa.Table.from_pandas(df_latest_out), latest_path, compression='snappy')
            logger.info(f"Wrote production latest: {latest_path} ({len(df_latest_out)} rows)")
    except Exception as e:
        logger.warning(f"Failed to write production latest.parquet: {e}")

    logger.info(f"Built production silver partitions: {parts_dir}")
    return parts_dir




def build_pipelines_table(
    data_dir: Path,
    registry: Optional[SourceRegistry] = None
) -> pd.DataFrame:
    """
    Build the silver pipelines table (lines) from Enhanced Pipeline shapefile.
    """
    if registry is None:
        registry = SourceRegistry()
    
    config = registry.get('enhanced_pipeline_shapefile')
    bronze_path = data_dir / "bronze" / config.id
    # Look specifically for the lines file
    files = sorted(bronze_path.glob("*Pipelines_SHP.parquet")) if bronze_path.exists() else []
    
    if not files:
        logger.warning("No pipeline lines data found in bronze layer")
        return pd.DataFrame()
    
    df = pd.read_parquet(files[-1])
    
    # Clean up for silver
    df = df.drop(columns=['_snapshot_date', '_source_id'], errors='ignore')
    df['_silver_version'] = pd.Timestamp.now().isoformat()
    
    logger.info(f"Built pipelines silver table: {len(df)} rows")
    return df

def build_pipeline_installations_table(
    data_dir: Path,
    registry: Optional[SourceRegistry] = None
) -> pd.DataFrame:
    """
    Build the silver pipeline installations table (points) from Enhanced Pipeline shapefile.
    """
    if registry is None:
        registry = SourceRegistry()
    
    config = registry.get('enhanced_pipeline_shapefile')
    bronze_path = data_dir / "bronze" / config.id
    # Look specifically for the installations file
    files = sorted(bronze_path.glob("*Pipeline_Installations_SHP.parquet")) if bronze_path.exists() else []
    
    if not files:
        logger.warning("No pipeline installations data found in bronze layer")
        return pd.DataFrame()
    
    df = pd.read_parquet(files[-1])
    
    # Clean up for silver
    df = df.drop(columns=['_snapshot_date', '_source_id'], errors='ignore')
    df['_silver_version'] = pd.Timestamp.now().isoformat()
    
    logger.info(f"Built pipeline installations silver table: {len(df)} rows")
    return df


def build_sk_pipelines_table(
    data_dir: Path,
    registry: Optional[SourceRegistry] = None
) -> pd.DataFrame:
    """
    Build the silver SK pipelines table from Saskatchewan GeoHub ArcGIS data.
    """
    if registry is None:
        registry = SourceRegistry()

    try:
        config = registry.get('sk_pipelines')
    except KeyError:
        logger.warning("sk_pipelines dataset not found in registry")
        return pd.DataFrame()

    bronze_path = data_dir / "bronze" / config.id
    files = sorted(bronze_path.glob("*.parquet")) if bronze_path.exists() else []

    if not files:
        logger.warning("No SK pipeline data found in bronze layer")
        return pd.DataFrame()

    df = pd.read_parquet(files[-1])

    # Clean up for silver - standardize column names to match AB pipelines
    df = df.drop(columns=['_snapshot_date', '_source_id'], errors='ignore')

    # Rename columns to match AB pipeline schema
    column_mapping = {
        'licence_number': 'licence_no',
        'licensee': 'licensee',
        'substance': 'substance',
        'status': 'lic_status',
    }
    df = df.rename(columns={k: v for k, v in column_mapping.items() if k in df.columns})

    # Ensure province column
    df['province'] = 'SK'
    df['_silver_version'] = pd.Timestamp.now().isoformat()

    logger.info(f"Built SK pipelines silver table: {len(df)} rows")
    return df


def build_confidential_wells_table(
    data_dir: Path,
    registry: Optional[SourceRegistry] = None
) -> pd.DataFrame:
    """
    Build the silver confidential wells table.
    Cumulative: keeps all wells ever reported as confidential.
    """
    if registry is None:
        registry = SourceRegistry()
    
    config = registry.get('confidential_well_list')
    bronze_path = data_dir / "bronze" / config.id
    files = sorted(bronze_path.glob("*.parquet")) if bronze_path.exists() else []
    
    if not files:
        logger.warning("No confidential well data found in bronze layer")
        return pd.DataFrame()
        
    # Load all snapshots
    all_dfs = []
    for f in files:
        sdf = pd.read_parquet(f)
        sdf['_snapshot_date'] = f.stem  # Use filename (YYYY-MM-DD)
        all_dfs.append(sdf)
        
    df = pd.concat(all_dfs, ignore_index=True)
    
    # Sort by snapshot date to track latest status
    df = df.sort_values('_snapshot_date')
    
    latest_snapshot = files[-1].stem
    
    # Aggregate by UWI
    # We want: uwi, op_name, release_date, first_seen, last_seen, is_currently_confidential
    agg_df = df.groupby('uwi').agg({
        'op_name': 'last',
        'release_date': 'last',
        '_snapshot_date': ['min', 'max']
    }).reset_index()
    
    # Flatten multi-index columns
    agg_df.columns = ['uwi', 'operator_name', 'release_date', 'first_seen', 'last_seen']
    
    # is_currently_confidential if it appeared in the latest snapshot
    agg_df['is_currently_confidential'] = agg_df['last_seen'] == latest_snapshot
    agg_df['was_ever_confidential'] = True
    
    agg_df['_silver_version'] = pd.Timestamp.now().isoformat()
    logger.info(f"Built cumulative confidential wells table: {len(agg_df)} rows")
    return agg_df

def build_spud_activity_table(
    data_dir: Path,
    registry: Optional[SourceRegistry] = None
) -> pd.DataFrame:
    """
    Build the silver spud activity table from ST49 daily snapshots AND archives,
    plus SK daily drilling data.
    Unions all snapshots and deduplicates.
    """
    if registry is None:
        registry = SourceRegistry()

    all_dfs = []

    # Load from daily snapshots (st49_daily_spud) - AB
    try:
        config = registry.get('st49_daily_spud')
        bronze_path = data_dir / "bronze" / config.id
        files = sorted(bronze_path.glob("*.parquet")) if bronze_path.exists() else []
        for f in files:
            try:
                df = pd.read_parquet(f)
                if 'province' not in df.columns:
                    df['province'] = 'AB'
                all_dfs.append(df)
            except Exception:
                pass
    except KeyError:
        pass

    # Also load from archive (st49_spud_archive) for historical data - AB
    try:
        config = registry.get('st49_spud_archive')
        bronze_path = data_dir / "bronze" / config.id
        files = sorted(bronze_path.glob("*.parquet")) if bronze_path.exists() else []
        logger.info(f"Found {len(files)} AB archive files in {bronze_path}")
        for f in files:
            try:
                df = pd.read_parquet(f)
                if 'province' not in df.columns:
                    df['province'] = 'AB'
                all_dfs.append(df)
            except Exception:
                pass
    except KeyError:
        pass

    # Load SK daily drilling data
    try:
        config = registry.get('sk_daily_drilling')
        bronze_path = data_dir / "bronze" / config.id
        files = sorted(bronze_path.glob("*.parquet")) if bronze_path.exists() else []
        logger.info(f"Found {len(files)} SK daily drilling files in {bronze_path}")
        for f in files:
            try:
                df = pd.read_parquet(f)
                if 'province' not in df.columns:
                    df['province'] = 'SK'
                # Normalize SK columns to match AB schema
                if 'rig_release_date' in df.columns and 'spud_date' not in df.columns:
                    # SK daily drilling may have rig_release_date
                    pass  # spud_date should already be present from parser
                all_dfs.append(df)
            except Exception as e:
                logger.warning(f"Failed to read SK drilling file {f}: {e}")
    except KeyError:
        pass

    if not all_dfs:
        return pd.DataFrame()

    df = pd.concat(all_dfs, ignore_index=True)

    # Ensure province column exists
    if 'province' not in df.columns:
        df['province'] = 'AB'

    # Deduplicate: Keep the latest reported activity for each UWI + Spud Date + Province
    if '_snapshot_date' in df.columns:
        df = df.sort_values('_snapshot_date', ascending=False)
    df = df.drop_duplicates(subset=['uwi', 'spud_date', 'province'], keep='first')

    # TRIM all join keys to ensure reliable joins
    df = _trim_join_keys(df, ['uwi', 'licence_number', 'licensee_code', 'cwi'])

    df['_silver_version'] = pd.Timestamp.now().isoformat()
    logger.info(f"Built spud_activity silver table: {len(df)} rows (AB + SK)")
    return df


def build_licence_activity_table(
    data_dir: Path,
    registry: Optional[SourceRegistry] = None
) -> pd.DataFrame:
    """
    Build the silver well licences table from ST1 daily snapshots AND archives,
    plus SK well bulletin data.
    """
    if registry is None:
        registry = SourceRegistry()

    all_dfs = []

    # Load from daily snapshots (st1_well_licences) - AB
    try:
        config = registry.get('st1_well_licences')
        bronze_path = data_dir / "bronze" / config.id
        files = sorted(bronze_path.glob("*.parquet")) if bronze_path.exists() else []
        logger.info(f"[DEBUG] ST1 bronze path: {bronze_path}, exists: {bronze_path.exists()}, files: {len(files)}")
        for f in files[-5:]:  # Log last 5 files
            logger.info(f"[DEBUG] ST1 bronze file: {f.name}")
        for f in files:
            try:
                df = pd.read_parquet(f)
                if 'province' not in df.columns:
                    df['province'] = 'AB'
                all_dfs.append(df)
            except Exception as e:
                logger.warning(f"[DEBUG] Failed to read {f.name}: {e}")
    except KeyError as e:
        logger.warning(f"[DEBUG] st1_well_licences not in registry: {e}")
    
    # Also load from archive (st1_well_licences_archive) for historical data - AB
    try:
        config = registry.get('st1_well_licences_archive')
        bronze_path = data_dir / "bronze" / config.id
        files = sorted(bronze_path.glob("*.parquet")) if bronze_path.exists() else []
        logger.info(f"Found {len(files)} AB licence archive files in {bronze_path}")
        for f in files:
            try:
                df = pd.read_parquet(f)
                if 'province' not in df.columns:
                    df['province'] = 'AB'
                all_dfs.append(df)
            except Exception:
                pass
    except KeyError:
        pass

    # Load SK well bulletin (contains licences) - SK
    try:
        config = registry.get('sk_well_bulletin')
        bronze_path = data_dir / "bronze" / config.id
        files = sorted(bronze_path.glob("*.parquet")) if bronze_path.exists() else []
        logger.info(f"Found {len(files)} SK well bulletin files in {bronze_path}")
        for f in files:
            try:
                df = pd.read_parquet(f)
                if 'province' not in df.columns:
                    df['province'] = 'SK'
                all_dfs.append(df)
            except Exception as e:
                logger.warning(f"Failed to read SK well bulletin {f}: {e}")
    except KeyError:
        pass

    if not all_dfs:
        logger.warning("[DEBUG] No licence dataframes loaded - returning empty")
        return pd.DataFrame()

    logger.info(f"[DEBUG] Loaded {len(all_dfs)} dataframes for licences (AB + SK)")
    df = pd.concat(all_dfs, ignore_index=True)
    logger.info(f"[DEBUG] Concatenated: {len(df)} total rows before dedup")

    # Ensure province column exists
    if 'province' not in df.columns:
        df['province'] = 'AB'

    # Debug: show issue_date range before dedup
    if 'issue_date' in df.columns:
        logger.info(f"[DEBUG] issue_date range BEFORE dedup: {df['issue_date'].min()} to {df['issue_date'].max()}")
    if '_snapshot_date' in df.columns:
        logger.info(f"[DEBUG] _snapshot_date range: {df['_snapshot_date'].min()} to {df['_snapshot_date'].max()}")

    # Deduplicate by licence number + province (licences can have same number across provinces)
    if '_snapshot_date' in df.columns:
        df = df.sort_values('_snapshot_date', ascending=False)
    df = df.drop_duplicates(subset=['licence_number', 'province'], keep='first')

    # Debug: show issue_date range after dedup
    if 'issue_date' in df.columns:
        logger.info(f"[DEBUG] issue_date range AFTER dedup: {df['issue_date'].min()} to {df['issue_date'].max()}")

    # TRIM all join keys to ensure reliable joins
    df = _trim_join_keys(df, ['licence_number', 'uwi', 'licensee_code', 'licensee', 'cwi'])

    df['_silver_version'] = pd.Timestamp.now().isoformat()
    logger.info(f"Built licence_activity silver table: {len(df)} rows (AB + SK)")
    return df


def build_well_attributes_table(
    data_dir: Path,
    registry: Optional[SourceRegistry] = None
) -> pd.DataFrame:
    """
    Build the silver well attributes table from ST37 TXT snapshots (AB)
    and SK Petrinex well infrastructure.

    This table contains detailed well attributes:
    - uwi, uwi_id (LOC-DESC key for joins)
    - well_name, field_code, pool_code
    - licence_no, licence_status, licence_issue_date
    - licensee_code (BA code), agent_code, operator_code
    - fin_drl_date, well_total_depth
    - well_stat_code, well_stat_date
    - fluid, mode, type, structure
    - province (AB or SK)
    - cwi (SK only - Canadian Well Identifier)
    """
    if registry is None:
        registry = SourceRegistry()

    dfs = []

    # Load AB ST37 wells
    try:
        config = registry.get('st37_wells_txt')
        bronze_path = data_dir / "bronze" / config.id
        files = sorted(bronze_path.glob("*.parquet")) if bronze_path.exists() else []

        if files:
            df_ab = pd.read_parquet(files[-1])
            if 'province' not in df_ab.columns:
                df_ab['province'] = 'AB'
            dfs.append(df_ab)
            logger.info(f"Loaded {len(df_ab)} AB well attributes from ST37")
    except KeyError:
        pass

    # Load SK Petrinex well infrastructure
    try:
        config = registry.get('sk_petrinex_well_infrastructure')
        bronze_path = data_dir / "bronze" / config.id
        files = sorted(bronze_path.glob("*.parquet")) if bronze_path.exists() else []

        if files:
            df_sk = pd.read_parquet(files[-1])
            if 'province' not in df_sk.columns:
                df_sk['province'] = 'SK'
            dfs.append(df_sk)
            logger.info(f"Loaded {len(df_sk)} SK well attributes from Petrinex")
    except KeyError:
        pass

    if not dfs:
        return pd.DataFrame()

    # Combine AB and SK data
    df = pd.concat(dfs, ignore_index=True)

    # Ensure province column exists
    if 'province' not in df.columns:
        df['province'] = 'AB'

    # Parse date columns (format: YYYYMMDD for ST37)
    date_cols = ['well_stat_date', 'fin_drl_date', 'licence_issue_date']
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], format='%Y%m%d', errors='coerce')

    # TRIM all join keys to ensure reliable joins
    df = _trim_join_keys(df, ['uwi', 'uwi_id', 'licence_no', 'licensee_code', 'field_code', 'pool_code', 'cwi'])

    df = df.drop(columns=['_snapshot_date', '_source_id'], errors='ignore')
    df['_silver_version'] = pd.Timestamp.now().isoformat()
    logger.info(f"Built well_attributes silver table: {len(df)} rows (AB + SK)")
    return df


def build_wells_table(
    data_dir: Path,
    registry: Optional[SourceRegistry] = None
) -> pd.DataFrame:
    """
    Build the silver wells table from ST37 wells TXT data (AB) and
    SK Petrinex well data.

    Enriches with:
    - Confidential status
    - Coordinates from abandoned_wells_shapefile (for spatial queries)
    - Latest status change date from ST2 (for "what changed" queries)
    - Province column (AB or SK)
    - CWI (SK only)
    """
    if registry is None:
        registry = SourceRegistry()

    dfs = []

    # Load AB ST37 wells TXT (the main AB wells list)
    try:
        config = registry.get('st37_wells_txt')
        bronze_path = data_dir / "bronze" / config.id
        files = sorted(bronze_path.glob("*.parquet")) if bronze_path.exists() else []
        if files:
            df_ab = pd.read_parquet(files[-1])
            if 'province' not in df_ab.columns:
                df_ab['province'] = 'AB'
            dfs.append(df_ab)
            logger.info(f"Loaded {len(df_ab)} AB wells from ST37")
    except KeyError:
        logger.warning("st37_wells_txt not found in registry")

    # Load SK Petrinex well licence (contains well data)
    try:
        config = registry.get('sk_petrinex_well_licence')
        bronze_path = data_dir / "bronze" / config.id
        files = sorted(bronze_path.glob("*.parquet")) if bronze_path.exists() else []
        if files:
            df_sk = pd.read_parquet(files[-1])
            if 'province' not in df_sk.columns:
                df_sk['province'] = 'SK'
            dfs.append(df_sk)
            logger.info(f"Loaded {len(df_sk)} SK wells from Petrinex licence")
    except KeyError:
        pass

    # Also load SK well bulletin (contains additional SK well info)
    try:
        config = registry.get('sk_well_bulletin')
        bronze_path = data_dir / "bronze" / config.id
        files = sorted(bronze_path.glob("*.parquet")) if bronze_path.exists() else []
        if files:
            df_sk_bull = pd.read_parquet(files[-1])
            if 'province' not in df_sk_bull.columns:
                df_sk_bull['province'] = 'SK'
            dfs.append(df_sk_bull)
            logger.info(f"Loaded {len(df_sk_bull)} SK wells from well bulletin")
    except KeyError:
        pass

    if not dfs:
        logger.warning("No well data found in bronze layer (AB or SK)")
        return pd.DataFrame()

    df = pd.concat(dfs, ignore_index=True)

    # Ensure province column exists
    if 'province' not in df.columns:
        df['province'] = 'AB'

    # Deduplicate by uwi + province (keep latest snapshot)
    if '_snapshot_date' in df.columns:
        df = df.sort_values('_snapshot_date', ascending=False)
    df = df.drop_duplicates(subset=['uwi', 'province'], keep='first')

    # ST37 parser now produces correct column names:
    # uwi, uwi_id, update_flag, well_name, field_code, pool_code, os_area_code, os_dep_code,
    # licence_no, licence_status, licence_issue_date, licensee_code, agent_code, operator_code,
    # fin_drl_date, well_total_depth, well_stat_code, well_stat_date, fluid, mode, type, structure

    # Rename to standard column names expected by change_engine and webapp
    rename_map = {
        'well_name': 'name',              # change_engine uses 'name'
        'licensee_code': 'licensee',      # BA code for licensee
        'licence_status': 'lic_status',   # change_engine uses 'lic_status'
        'licence_no': 'licence',          # standardize licence column name
        'well_stat_date': 'stat_date',    # status change date
        'well_stat_code': 'stat_code',    # status code
        'fin_drl_date': 'drill_date',     # finished drilling date
        'well_total_depth': 'total_depth', # total depth in meters
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    # Parse date columns (format: YYYYMMDD)
    date_cols = ['stat_date', 'drill_date', 'licence_issue_date']
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], format='%Y%m%d', errors='coerce')
        
    # Enrich with confidential status
    conf_df = build_confidential_wells_table(data_dir, registry)
    if not conf_df.empty:
        df = df.merge(
            conf_df[['uwi', 'was_ever_confidential', 'is_currently_confidential', 'release_date']], 
            on='uwi', 
            how='left'
        )
        df['was_ever_confidential'] = df['was_ever_confidential'].fillna(False)
        df['is_currently_confidential'] = df['is_currently_confidential'].fillna(False)
    else:
        df['was_ever_confidential'] = False
        df['is_currently_confidential'] = False
        df['release_date'] = None

    # Calculate coordinates from UWI using DLS (Dominion Land Survey) conversion
    # NOTE: Must always calculate - needed for well_attributes view and map display
    if True:
        # This is the proper way - UWI contains the location info (LSD-Section-Township-Range-Meridian)
        try:
            # Try webapp import first, then fallback to local implementation
            try:
                import sys
                from pathlib import Path
                # Add webapp to path if not already there
                webapp_path = Path(__file__).parent.parent.parent / "webapp" / "backend"
                if str(webapp_path) not in sys.path:
                    sys.path.insert(0, str(webapp_path))
                from domain.spatial import uwi_to_centroid
            except ImportError:
                # Fallback: inline UWI to centroid conversion with latitude-adjusted longitude
                import re
                import math

                DLS_BASE = {
                    # Saskatchewan meridians
                    2: {"lat": 49.0, "lon": -102.0},  # W2 - Saskatchewan
                    3: {"lat": 49.0, "lon": -106.0},  # W3 - Saskatchewan
                    # Alberta meridians
                    4: {"lat": 49.0, "lon": -110.0},  # W4 - Alberta
                    5: {"lat": 49.0, "lon": -114.0},  # W5 - Alberta
                    6: {"lat": 49.0, "lon": -118.0},  # W6 - Alberta (BC border)
                }
                DEG_PER_TOWNSHIP = 0.087
                DEG_PER_SECTION_LAT = DEG_PER_TOWNSHIP / 6
                DEG_PER_LSD_LAT = DEG_PER_SECTION_LAT / 4
                RANGE_WIDTH_KM = 9.656  # 6 miles in km

                def deg_per_range_at_lat(lat):
                    """Degrees longitude per range varies with latitude."""
                    km_per_deg_lon = 111.32 * math.cos(math.radians(lat))
                    return RANGE_WIDTH_KM / km_per_deg_lon

                def uwi_to_centroid(uwi: str):
                    if not uwi:
                        return None
                    uwi = str(uwi).strip().upper()
                    pattern = r'^(\d{2})[/\\]?(\d{2})-?(\d{2})-?(\d{3})-?(\d{2})([WE])(\d)[/\\]?(\d)$'
                    match = re.match(pattern, uwi)
                    if not match:
                        return None
                    lsd = int(match.group(2))
                    section = int(match.group(3))
                    township = int(match.group(4))
                    range_num = int(match.group(5))
                    meridian = int(match.group(7))
                    base = DLS_BASE.get(meridian)
                    if not base:
                        return None
                    # Calculate latitude first
                    lat = base["lat"] + (township - 1) * DEG_PER_TOWNSHIP
                    section_row = (section - 1) // 6
                    section_col = (section - 1) % 6
                    if section_row % 2 == 1:
                        section_col = 5 - section_col
                    lat += section_row * DEG_PER_SECTION_LAT + DEG_PER_SECTION_LAT / 2
                    lsd_row = (lsd - 1) // 4
                    lat += lsd_row * DEG_PER_LSD_LAT + DEG_PER_LSD_LAT / 2
                    # Calculate longitude using latitude-adjusted values
                    deg_per_range = deg_per_range_at_lat(lat)
                    deg_per_section_lon = deg_per_range / 6
                    deg_per_lsd_lon = deg_per_range / 24
                    lon = base["lon"] - (range_num - 1) * deg_per_range
                    lon -= section_col * deg_per_section_lon + deg_per_section_lon / 2
                    lsd_col = (lsd - 1) % 4
                    lon -= lsd_col * deg_per_lsd_lon + deg_per_lsd_lon / 2
                    return (round(lat, 6), round(lon, 6))

            def calc_coords(uwi):
                """Calculate lat/lon from UWI."""
                if not uwi or pd.isna(uwi):
                    return (None, None)
                result = uwi_to_centroid(str(uwi))
                return result if result else (None, None)

            # Vectorized coordinate calculation
            coords = df['uwi'].apply(calc_coords)
            df['centroid_lat'] = coords.apply(lambda x: x[0] if x else None)
            df['centroid_lon'] = coords.apply(lambda x: x[1] if x else None)

            coord_count = df['centroid_lat'].notna().sum()
            logger.info(f"Calculated coordinates for {coord_count}/{len(df)} wells from UWI")
        except Exception as e:
            logger.warning(f"Could not calculate coordinates from UWI: {e}")
            df['centroid_lat'] = None
            df['centroid_lon'] = None

    # Enrich with latest status change date from ST2
    # This gives us a 'stat_date' for "what changed" queries
    try:
        status_changes = _load_st2_status_changes(data_dir, registry)
        if not status_changes.empty and 'uwi' in status_changes.columns:
            # Get the latest status change date per UWI
            latest_changes = status_changes.sort_values('event_date').groupby('uwi').last().reset_index()
            latest_changes = latest_changes[['uwi', 'event_date']].rename(columns={'event_date': 'stat_date'})
            
            df = df.merge(latest_changes, on='uwi', how='left')
            dated_count = df['stat_date'].notna().sum()
            logger.info(f"Added stat_date to {dated_count}/{len(df)} wells from ST2 status changes")
    except Exception as e:
        logger.warning(f"Could not enrich wells with stat_date: {e}")
    
    # Ensure stat_date column exists even if no data
    if 'stat_date' not in df.columns:
        df['stat_date'] = None
    
    # TRIM all join keys to ensure reliable joins across datasets
    df = _trim_join_keys(df, ['uwi', 'licence', 'licensee', 'field_code', 'cwi'])

    # Clean up for silver
    df = df.drop(columns=['_snapshot_date', '_source_id'], errors='ignore')
    df['_silver_version'] = pd.Timestamp.now().isoformat()

    logger.info(f"Built wells silver table: {len(df)} rows (AB + SK)")
    return df


def _load_st2_status_changes(
    data_dir: Path,
    registry: Optional[SourceRegistry] = None
) -> pd.DataFrame:
    """Load all ST2 status change records from bronze."""
    if registry is None:
        registry = SourceRegistry()
    
    try:
        config = registry.get('st2_weekly_status_changes')
        bronze_path = data_dir / "bronze" / config.id
        files = sorted(bronze_path.glob("*.parquet")) if bronze_path.exists() else []
    except KeyError:
        return pd.DataFrame()
    
    if not files:
        return pd.DataFrame()
    
    # Load all status change snapshots
    all_dfs = []
    for f in files:
        try:
            sdf = pd.read_parquet(f)
            all_dfs.append(sdf)
        except Exception as e:
            logger.warning(f"Error reading {f}: {e}")
    
    if not all_dfs:
        return pd.DataFrame()
    
    df = pd.concat(all_dfs, ignore_index=True)
    
    # Ensure event_date is datetime
    if 'event_date' in df.columns:
        df['event_date'] = pd.to_datetime(df['event_date'], errors='coerce')
    
    # Deduplicate by uwi + event_date
    if 'uwi' in df.columns and 'event_date' in df.columns:
        df = df.drop_duplicates(subset=['uwi', 'event_date'], keep='last')
    
    return df


def build_status_changes_table(
    data_dir: Path,
    registry: Optional[SourceRegistry] = None
) -> pd.DataFrame:
    """
    Build the silver status_changes table from ST2 weekly status changes.

    This is the PRIMARY source for "What Changed" status change detection.
    Contains actual dated status transitions (old_status -> new_status).
    """
    if registry is None:
        registry = SourceRegistry()

    df = _load_st2_status_changes(data_dir, registry)

    if df.empty:
        logger.warning("No ST2 status change data found in bronze layer")
        return pd.DataFrame()

    # Handle raw Tableau column names (00.UnFormatted UWI, etc.) if parser didn't clean them
    raw_column_mapping = {
        '00.unformatted_uwi': 'uwi_unformatted',
        '01.formatted_uwi': 'uwi',
        '02.well_name': 'name',
        '03.lic_no': 'licence',
        '04.licensee': 'licensee_code',
        '05.fld': 'field_code',
        '06.field_name': 'field_name',
        '07.statcurr': 'new_status_code',
        '08.statcurrdesc': 'new_status',
        '09.statprev': 'old_status_code',
        '10.statprevdesc': 'old_status',
    }
    # Apply mapping for any columns that match
    for raw_col, clean_col in raw_column_mapping.items():
        if raw_col in df.columns and clean_col not in df.columns:
            df[clean_col] = df[raw_col]
            logger.info(f"Mapped raw column {raw_col} -> {clean_col}")

    # Filter out corrupt rows (HTML error pages have <!doctype in columns)
    html_cols = [c for c in df.columns if '<!doctype' in c.lower() or '<html' in c.lower()]
    if html_cols:
        logger.warning(f"Found {len(html_cols)} corrupt HTML columns, dropping them")
        df = df.drop(columns=html_cols, errors='ignore')

    # Ensure required columns exist
    required_cols = ['uwi', 'event_date', 'old_status', 'new_status']
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        logger.warning(f"ST2 status changes missing columns: {missing}")
        # Try to recover with alternative column names
        if 'status_change_date' in df.columns and 'event_date' not in df.columns:
            df['event_date'] = df['status_change_date']
    
    # Skip coordinate enrichment in low-memory mode - causes OOM by reloading 657K wells
    # Enrichment can be done at query time via JOIN instead
    import os
    if not os.path.exists('/data'):
        # Local only - add coordinates from wells
        wells_df = build_wells_table(data_dir, registry)
        if not wells_df.empty and 'centroid_lat' in wells_df.columns:
            coord_cols = ['uwi', 'centroid_lat', 'centroid_lon']
            if 'licensee' not in df.columns and 'licensee' in wells_df.columns:
                coord_cols.append('licensee')
            coord_cols = [c for c in coord_cols if c in wells_df.columns]
            df = df.merge(wells_df[coord_cols], on='uwi', how='left')
            logger.info(f"Enriched {df['centroid_lat'].notna().sum()}/{len(df)} status changes with coordinates")

    # TRIM all join keys to ensure reliable joins
    df = _trim_join_keys(df, ['uwi', 'licence', 'licensee', 'licensee_code', 'field_code'])

    df = df.drop(columns=['_snapshot_date', '_source_id'], errors='ignore')
    df['_silver_version'] = pd.Timestamp.now().isoformat()

    logger.info(f"Built status_changes silver table: {len(df)} rows")
    return df


def build_drilling_activity_table(
    data_dir: Path,
    registry: Optional[SourceRegistry] = None
) -> pd.DataFrame:
    """
    Build the silver drilling_activity table from ST2 weekly drilling + ST49 spuds.
    
    This is the PRIMARY source for "What Changed" new drilling activity detection.
    """
    if registry is None:
        registry = SourceRegistry()
    
    all_dfs = []
    
    # Load ST2 weekly drilling
    try:
        config = registry.get('st2_weekly_drilling')
        bronze_path = data_dir / "bronze" / config.id
        files = sorted(bronze_path.glob("*.parquet")) if bronze_path.exists() else []
        
        for f in files:
            try:
                sdf = pd.read_parquet(f)
                sdf['source'] = 'st2_drilling'
                all_dfs.append(sdf)
            except Exception:
                pass
    except KeyError:
        pass
    
    # Load ST49 daily spuds
    try:
        config = registry.get('st49_daily_spud')
        bronze_path = data_dir / "bronze" / config.id
        files = sorted(bronze_path.glob("*.parquet")) if bronze_path.exists() else []
        
        for f in files:
            try:
                sdf = pd.read_parquet(f)
                # Normalize spud_date to event_date
                if 'spud_date' in sdf.columns and 'event_date' not in sdf.columns:
                    sdf['event_date'] = pd.to_datetime(sdf['spud_date'], errors='coerce')
                sdf['source'] = 'st49_spud'
                sdf['event_type'] = 'spud'
                all_dfs.append(sdf)
            except Exception:
                pass
    except KeyError:
        pass
    
    # Also load ST49 archive for historical spud data
    try:
        config = registry.get('st49_spud_archive')
        bronze_path = data_dir / "bronze" / config.id
        files = sorted(bronze_path.glob("*.parquet")) if bronze_path.exists() else []
        logger.info(f"Found {len(files)} spud archive files in {bronze_path}")
        
        for f in files:
            try:
                sdf = pd.read_parquet(f)
                # Normalize spud_date to event_date
                if 'spud_date' in sdf.columns and 'event_date' not in sdf.columns:
                    sdf['event_date'] = pd.to_datetime(sdf['spud_date'], errors='coerce')
                sdf['source'] = 'st49_spud_archive'
                sdf['event_type'] = 'spud'
                all_dfs.append(sdf)
            except Exception:
                pass
    except KeyError:
        pass
    
    if not all_dfs:
        logger.warning("No drilling activity data found in bronze layer")
        return pd.DataFrame()
    
    df = pd.concat(all_dfs, ignore_index=True)
    
    # Ensure event_date is datetime
    if 'event_date' in df.columns:
        df['event_date'] = pd.to_datetime(df['event_date'], errors='coerce')
    
    # Deduplicate
    if 'uwi' in df.columns and 'event_date' in df.columns:
        df = df.sort_values(['uwi', 'event_date', 'source'])
        df = df.drop_duplicates(subset=['uwi', 'event_date'], keep='last')
    
    # Skip coordinate enrichment in low-memory mode - causes OOM
    import os
    if not os.path.exists('/data'):
        wells_df = build_wells_table(data_dir, registry)
        if not wells_df.empty and 'centroid_lat' in wells_df.columns:
            coord_cols = ['uwi', 'centroid_lat', 'centroid_lon', 'licensee', 'name']
            coord_cols = [c for c in coord_cols if c in wells_df.columns]
            df = df.merge(wells_df[coord_cols], on='uwi', how='left')

    # TRIM all join keys to ensure reliable joins
    df = _trim_join_keys(df, ['uwi', 'licence', 'licensee', 'licensee_code', 'field_code'])

    df = df.drop(columns=['_snapshot_date', '_source_id'], errors='ignore')
    df['_silver_version'] = pd.Timestamp.now().isoformat()

    logger.info(f"Built drilling_activity silver table: {len(df)} rows")
    return df





def write_silver(
    df: pd.DataFrame,
    table_name: str,
    data_dir: Path
) -> Path:
    """Write a silver table to Parquet."""
    silver_dir = data_dir / "silver" / table_name
    silver_dir.mkdir(parents=True, exist_ok=True)
    
    output_path = silver_dir / "latest.parquet"
    
    table = pa.Table.from_pandas(df)
    pq.write_table(table, output_path, compression='snappy')
    
    logger.info(f"Wrote silver: {output_path} ({len(df)} rows)")
    return output_path


def build_all_silver_tables(data_dir: Path) -> dict[str, Path]:
    """Build all silver tables from bronze data."""
    import gc
    
    results = {}
    
    def safe_build(name: str, builder_func, *args, **kwargs):
        """Safely build a table, catching any errors."""
        try:
            result = builder_func(*args, **kwargs)

            # Some builders return a Path (e.g. production partitions) to avoid OOM.
            if isinstance(result, Path):
                results[name] = result
                gc.collect()
                return

            df = result
            if df is not None and not df.empty:
                path = write_silver(df, name, data_dir)
                results[name] = path
                del df
                gc.collect()
        except Exception as e:
            logger.error(f"Failed to build {name}: {e}")
    
    # Check if memory-constrained (set LOW_MEMORY=1 to skip heavy tables)
    import os
    low_memory = os.environ.get('LOW_MEMORY', '') == '1'

    # Facilities
    safe_build('facilities', build_facilities_table, data_dir)

    if not low_memory:
        safe_build('facility_history', build_facility_history_table, data_dir)
        safe_build('facility_status_changes', build_facility_status_changes_table, data_dir)
    else:
        logger.info("Skipping facility_history/status_changes (LOW_MEMORY=1)")

    # Pipelines
    if not low_memory:
        safe_build('pipelines', build_pipelines_table, data_dir)
        safe_build('pipeline_installations', build_pipeline_installations_table, data_dir)
    else:
        logger.info("Skipping AB pipelines tables (LOW_MEMORY=1)")

    # SK Pipelines (lighter weight)
    safe_build('sk_pipelines', build_sk_pipelines_table, data_dir)

    # Wells
    safe_build('confidential_wells', build_confidential_wells_table, data_dir)
    gc.collect()
    safe_build('wells', build_wells_table, data_dir)
    gc.collect()
    safe_build('well_attributes', build_well_attributes_table, data_dir)
    gc.collect()

    if not low_memory:
        safe_build('ownership_changes', build_ownership_changes_table, data_dir)
        
    # Activity - ST2 status changes and drilling (PRIMARY source for What Changed)
    safe_build('status_changes', build_status_changes_table, data_dir)
    safe_build('drilling_activity', build_drilling_activity_table, data_dir)
    
    # Legacy activity tables (from ST49/ST1 snapshots)
    safe_build('spud_activity', build_spud_activity_table, data_dir)
    safe_build('well_licences', build_licence_activity_table, data_dir)
    
    # Production
    safe_build('production', build_production_table, data_dir)
    
    return results


def build_ownership_changes_table(
    data_dir: Path,
    registry: Optional[SourceRegistry] = None
) -> pd.DataFrame:
    """
    Detect ownership changes by comparing consecutive ST37 well snapshots.
    
    When a well's `well_name` changes between snapshots (e.g., "CNRL WELL X" -> "TENAZ WELL X"),
    this indicates an ownership transfer. This function tracks all such changes.
    
    Returns:
        DataFrame with columns: uwi, old_well_name, new_well_name, change_date, old_operator, new_operator
    """
    if registry is None:
        registry = SourceRegistry()
    
    try:
        config = registry.get('st37_wells_txt')
        bronze_path = data_dir / "bronze" / config.id
        files = sorted(bronze_path.glob("*.parquet"))
    except KeyError:
        logger.warning("st37_wells_txt not found in registry")
        return pd.DataFrame()
    
    if len(files) < 2:
        logger.info(f"Need at least 2 ST37 snapshots for ownership change detection, found {len(files)}")
        return pd.DataFrame()
    
    logger.info(f"Comparing {len(files)} ST37 snapshots for ownership changes")
    
    all_changes = []
    
    for i in range(1, len(files)):
        prev_file = files[i-1]
        curr_file = files[i]
        
        try:
            # Read only the columns we need to minimize memory
            prev_df = pd.read_parquet(prev_file, columns=['uwi', 'well_name'])
            curr_df = pd.read_parquet(curr_file, columns=['uwi', 'well_name'])
            
            # Merge on UWI to compare
            merged = prev_df.merge(curr_df, on='uwi', suffixes=('_old', '_new'))
            
            # Find rows where well_name changed
            changed = merged[merged['well_name_old'] != merged['well_name_new']].copy()
            
            if not changed.empty:
                changed['change_date'] = curr_file.stem  # e.g., "2026-01-15"
                all_changes.append(changed)
                logger.info(f"Found {len(changed)} ownership changes between {prev_file.stem} and {curr_file.stem}")
            
            del prev_df, curr_df, merged
            
        except Exception as e:
            logger.warning(f"Error comparing {prev_file.stem} to {curr_file.stem}: {e}")
    
    if not all_changes:
        logger.info("No ownership changes detected across snapshots")
        return pd.DataFrame()
    
    result = pd.concat(all_changes, ignore_index=True)
    
    # Extract operator name (first word of well_name, usually the company abbreviation)
    def extract_operator(well_name):
        if pd.isna(well_name) or not well_name:
            return None
        parts = str(well_name).split()
        return parts[0] if parts else None
    
    result['old_operator'] = result['well_name_old'].apply(extract_operator)
    result['new_operator'] = result['well_name_new'].apply(extract_operator)

    # Rename columns for clarity
    result = result.rename(columns={
        'well_name_old': 'old_well_name',
        'well_name_new': 'new_well_name'
    })

    # TRIM all join keys to ensure reliable joins
    result = _trim_join_keys(result, ['uwi'])

    result['_silver_version'] = pd.Timestamp.now().isoformat()

    logger.info(f"Built ownership_changes silver table: {len(result)} rows")
    return result
