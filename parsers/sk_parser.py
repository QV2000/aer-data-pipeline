"""
Saskatchewan data parsers.

Handles:
- Petrinex SK datasets (same API as AB, swap AB→SK in URL)
- Saskatchewan government direct downloads (drilling, well bulletin)
- Petrinex SK reference tables

Key differences from AB:
- SK uses CWI (Canadian Well Identifier, format SK1234567) as primary key
- SK uses UWI as secondary identifier (DLS-based, same format as AB)
- SK DLS locations use meridians W2 and W3 (AB uses W4, W5, W6)
- Petrinex SK volumetrics: 'Heat' populated, 'Energy' blank (opposite of AB)
"""

import io
import logging
import zipfile
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)


def _extract_csv_from_nested_zip(file_path: Path) -> bytes:
    """
    Extract CSV from potentially nested ZIP file.

    Petrinex API returns: outer ZIP → inner ZIP → CSV
    """
    with open(file_path, 'rb') as f:
        file_bytes = f.read()

    # Check if it's a ZIP file
    if file_bytes[:2] != b'PK':
        # Not a ZIP, return as-is (might be raw CSV)
        return file_bytes

    with zipfile.ZipFile(io.BytesIO(file_bytes)) as outer_zip:
        for name in outer_zip.namelist():
            inner_bytes = outer_zip.read(name)

            # Check if this is another ZIP (nested)
            if inner_bytes[:2] == b'PK' or name.lower().endswith('.zip'):
                with zipfile.ZipFile(io.BytesIO(inner_bytes)) as inner_zip:
                    for inner_name in inner_zip.namelist():
                        if inner_name.upper().endswith('.CSV'):
                            return inner_zip.read(inner_name)

            # Direct CSV in outer ZIP
            if name.upper().endswith('.CSV'):
                return inner_bytes

    raise ValueError(f"No CSV found in {file_path}")


def _read_csv_with_encodings(content: bytes) -> pd.DataFrame:
    """Try multiple encodings to read CSV content."""
    for encoding in ['utf-8-sig', 'utf-8', 'cp1252', 'latin-1']:
        try:
            df = pd.read_csv(io.BytesIO(content), encoding=encoding, dtype=str, on_bad_lines='skip')
            logger.info(f"Successfully read CSV with encoding: {encoding}")
            return df
        except UnicodeDecodeError:
            continue
        except Exception as e:
            logger.warning(f"Failed to read with encoding {encoding}: {e}")
            continue

    raise ValueError("Could not parse CSV with any known encoding")


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize column names to snake_case."""
    import re

    def normalize(name: str) -> str:
        name = str(name).strip()
        name = re.sub(r'[\s\-\/\.\(\)]+', '_', name)
        name = re.sub(r'([a-z])([A-Z])', r'\1_\2', name)
        name = name.lower()
        name = re.sub(r'_+', '_', name)
        return name.strip('_')

    df.columns = [normalize(c) for c in df.columns]
    return df


# =============================================================================
# PETRINEX SK PARSERS
# =============================================================================

def parse_petrinex_sk_production(file_path: Path) -> pd.DataFrame:
    """
    Parse Petrinex SK Conventional Volumetric Data CSV.

    Same structure as AB Petrinex, but:
    - 'Heat' (MJ/m3) is populated, 'Energy' (GJ) is blank
    - Facility IDs start with 'SK' instead of 'AB'
    - Meridians are W2/W3 instead of W4/W5/W6
    """
    logger.info(f"Parsing Petrinex SK production from {file_path}")

    # Handle nested ZIP
    try:
        content = _extract_csv_from_nested_zip(file_path)
    except Exception as e:
        logger.warning(f"ZIP extraction failed: {e}, trying as raw file")
        with open(file_path, 'rb') as f:
            content = f.read()

    df = _read_csv_with_encodings(content)
    df = _normalize_columns(df)

    # Standard Petrinex column mappings (same as AB)
    column_mapping = {
        'production_month': 'productionmonth',
        'operator_ba_id': 'operator_ba_id',
        'operator_name': 'operator_name',
        'reporting_facility_id': 'reporting_facility_id',
        'activity_id': 'activityid',
        'product_id': 'productid',
        'volume': 'volume',
        'energy': 'energy',
        'hours': 'hours_on',
        'heat': 'heat',  # SK-specific: populated here
    }

    for old, new in column_mapping.items():
        if old in df.columns and new not in df.columns:
            df = df.rename(columns={old: new})

    # Add province marker
    df['province'] = 'SK'

    logger.info(f"Parsed {len(df)} SK production records")
    return df


def parse_petrinex_sk_well_infrastructure(file_path: Path) -> pd.DataFrame:
    """Parse Petrinex SK Well Infrastructure CSV."""
    logger.info(f"Parsing Petrinex SK well infrastructure from {file_path}")

    content = _extract_csv_from_nested_zip(file_path)
    df = _read_csv_with_encodings(content)
    df = _normalize_columns(df)
    df['province'] = 'SK'

    logger.info(f"Parsed {len(df)} SK well infrastructure records")
    return df


def parse_petrinex_sk_well_licence(file_path: Path) -> pd.DataFrame:
    """Parse Petrinex SK Well Licence CSV."""
    logger.info(f"Parsing Petrinex SK well licence from {file_path}")

    content = _extract_csv_from_nested_zip(file_path)
    df = _read_csv_with_encodings(content)
    df = _normalize_columns(df)
    df['province'] = 'SK'

    logger.info(f"Parsed {len(df)} SK well licence records")
    return df


def parse_petrinex_sk_facility_licence(file_path: Path) -> pd.DataFrame:
    """Parse Petrinex SK Facility Licence CSV."""
    logger.info(f"Parsing Petrinex SK facility licence from {file_path}")

    content = _extract_csv_from_nested_zip(file_path)
    df = _read_csv_with_encodings(content)
    df = _normalize_columns(df)
    df['province'] = 'SK'

    logger.info(f"Parsed {len(df)} SK facility licence records")
    return df


def parse_petrinex_sk_facility_infrastructure(file_path: Path) -> pd.DataFrame:
    """Parse Petrinex SK Facility Infrastructure CSV."""
    logger.info(f"Parsing Petrinex SK facility infrastructure from {file_path}")

    content = _extract_csv_from_nested_zip(file_path)
    df = _read_csv_with_encodings(content)
    df = _normalize_columns(df)
    df['province'] = 'SK'

    logger.info(f"Parsed {len(df)} SK facility infrastructure records")
    return df


def parse_petrinex_sk_well_facility_link(file_path: Path) -> pd.DataFrame:
    """
    Parse Petrinex SK Well to Facility Link CSV.

    Maps each well to its linked facility (battery).
    Same structure as AB version.
    """
    logger.info(f"Parsing Petrinex SK well-facility link from {file_path}")

    content = _extract_csv_from_nested_zip(file_path)
    df = _read_csv_with_encodings(content)
    df = _normalize_columns(df)

    # Column mapping (same as AB)
    column_mapping = {
        'wellid': 'well_id',
        'wellprovincestate': 'province',
        'wellidentifier': 'well_identifier',
        'welllegals subdivision': 'well_lsd',
        'wellsection': 'well_sec',
        'welltownship': 'well_twp',
        'wellrange': 'well_rng',
        'wellmeridian': 'well_mer',
        'wellstatusfluid': 'fluid',
        'wellstatusmode': 'mode',
        'linkedfacilityid': 'linked_facility_id',
        'linkedfacilityprovincestate': 'facility_province',
        'linkedfacilitysubtype': 'linked_facility_sub_type',
        'linkedfacilityname': 'facility_name',
    }

    for old, new in column_mapping.items():
        if old in df.columns and new not in df.columns:
            df = df.rename(columns={old: new})

    # Ensure province is set
    if 'province' not in df.columns:
        df['province'] = 'SK'
    else:
        df['province'] = df['province'].fillna('SK')

    logger.info(f"Parsed {len(df)} SK well-facility link records")
    return df


def parse_petrinex_sk_business_associate(file_path: Path) -> pd.DataFrame:
    """Parse Petrinex SK Business Associate (operator lookup) CSV."""
    logger.info(f"Parsing Petrinex SK business associate from {file_path}")

    content = _extract_csv_from_nested_zip(file_path)
    df = _read_csv_with_encodings(content)
    df = _normalize_columns(df)
    df['province'] = 'SK'

    logger.info(f"Parsed {len(df)} SK business associate records")
    return df


# =============================================================================
# SASKATCHEWAN GOVERNMENT PARSERS
# =============================================================================

def parse_sk_daily_drilling(file_path: Path) -> pd.DataFrame:
    """
    Parse Saskatchewan Daily Drilling Activity Report CSV.

    Simple CSV with columns like:
    - CWI, Spud Date, Rig Release Date, Drilling Contractor
    """
    logger.info(f"Parsing SK daily drilling from {file_path}")

    df = pd.read_csv(file_path, dtype=str, encoding='utf-8-sig')
    df = _normalize_columns(df)

    # Map SK column names to standard schema
    column_mapping = {
        'canadian_well_identifier': 'cwi',
        'drilling_start_date': 'spud_date',
        'rig_release_date': 'rig_release_date',
        'contractor_name': 'drilling_contractor',
        'licence_number': 'licence_number',
        'licensee_name': 'licensee',
        'licensee_baid': 'licensee_code',
    }

    for old, new in column_mapping.items():
        if old in df.columns:
            df = df.rename(columns={old: new})

    # Generate UWI from surface location if not present
    # SK uses W2/W3 meridians, format: LL/SS-TT-RRR-MMWX/E
    if 'uwi' not in df.columns:
        def build_uwi(row):
            try:
                lsd = str(row.get('surface_legal_subdivision', '')).zfill(2)
                sec = str(row.get('surface_section', '')).zfill(2)
                twp = str(row.get('surface_township', '')).zfill(3)
                rge = str(row.get('surface_range', '')).zfill(2)
                mer = str(row.get('surface_meridian', ''))
                if not all([lsd, sec, twp, rge, mer]):
                    return None
                # Format: 00/LL-SS-TTT-RRW#/0
                return f"00/{lsd}-{sec}-{twp}-{rge}{mer}/0"
            except Exception:
                return None
        df['uwi'] = df.apply(build_uwi, axis=1)

    # Parse dates
    for col in ['spud_date', 'rig_release_date', 'drilling_end_date']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')

    df['province'] = 'SK'

    logger.info(f"Parsed {len(df)} SK daily drilling records")
    return df


def parse_sk_well_bulletin(file_path: Path) -> pd.DataFrame:
    """
    Parse Saskatchewan Daily Well Bulletin CSV.

    Rich dataset with 30+ columns covering licences AND status changes.
    Contains both CWI (SK primary key) and UWI (DLS-based).
    """
    logger.info(f"Parsing SK well bulletin from {file_path}")

    df = pd.read_csv(file_path, dtype=str, encoding='utf-8-sig')
    df = _normalize_columns(df)

    # Key columns to preserve
    column_mapping = {
        'licence_number': 'licence_number',
        'licence_status': 'licence_status',
        'status_date': 'status_date',
        'issued_date': 'issued_date',
        'cancelled_date': 'cancelled_date',
        'licensee_address': 'licensee',
        'trajectory': 'trajectory',
        'cwi': 'cwi',
        'uwi': 'uwi',
        'well_completion_type': 'well_completion_type',
        'target_pool_name': 'pool_name',
        'target_pool_code': 'pool_code',
        'measured_depth': 'measured_depth',
        'true_vertical_depth': 'tvd',
        'ground_elevation': 'ground_elev',
        'surface_land_description': 'surface_location',
        'bottom_hole_land_description': 'bottom_location',
    }

    for old, new in column_mapping.items():
        if old in df.columns and new not in df.columns:
            df = df.rename(columns={old: new})

    # Parse dates
    for col in ['status_date', 'issued_date', 'cancelled_date']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')

    df['province'] = 'SK'

    logger.info(f"Parsed {len(df)} SK well bulletin records")
    return df


# =============================================================================
# PETRINEX SK REFERENCE TABLE PARSERS
# =============================================================================

def parse_petrinex_sk_ref_csv(file_path: Path) -> pd.DataFrame:
    """
    Generic parser for Petrinex SK reference/lookup CSVs.

    Works for:
    - PRABAIdentifiers.csv
    - PRAFacilityIds.csv
    - PRAActivityCodes.csv
    - PRAFacilityCodes.csv
    - PRAFieldandPoolCodes.csv
    - PRAProductCodes.csv
    """
    logger.info(f"Parsing Petrinex SK reference table from {file_path}")

    df = pd.read_csv(file_path, dtype=str, encoding='utf-8-sig')
    df = _normalize_columns(df)
    df['province'] = 'SK'

    logger.info(f"Parsed {len(df)} SK reference records from {file_path.name}")
    return df


# =============================================================================
# SASKATCHEWAN PIPELINE PARSER
# =============================================================================

def parse_sk_pipelines(file_path: Path) -> pd.DataFrame:
    """
    Parse Saskatchewan Pipelines GeoJSON from ArcGIS REST service.

    The GeoJSON contains LineString geometries with pipeline segments.
    We extract the start/end coordinates for each segment.

    Fields from ArcGIS:
    - LICENCENUMBER: Pipeline licence (e.g., "PL-00000001")
    - OWNERNAME: Operator/owner name
    - SUBSTANCE: What flows through (Crude Oil, Gas, etc.)
    - SEGMENTIDENTIFIER: Unique segment ID
    - SEGMENTSTATUS: Status (Active, Removed, etc.)
    - geometry: LineString coordinates
    """
    import json

    logger.info(f"Parsing SK pipelines from {file_path}")

    with open(file_path, 'r', encoding='utf-8') as f:
        geojson = json.load(f)

    features = geojson.get('features', [])
    if not features:
        logger.warning("No features found in SK pipelines GeoJSON")
        return pd.DataFrame()

    records = []
    for feature in features:
        props = feature.get('properties', {})
        geom = feature.get('geometry', {})
        coords = geom.get('coordinates', [])

        # Extract first and last coordinate for from/to points
        if coords and len(coords) >= 2:
            from_lon, from_lat = coords[0][0], coords[0][1]
            to_lon, to_lat = coords[-1][0], coords[-1][1]
        elif coords and len(coords) == 1:
            from_lon, from_lat = coords[0][0], coords[0][1]
            to_lon, to_lat = from_lon, from_lat
        else:
            continue  # Skip features without valid coordinates

        records.append({
            'licence_number': props.get('LICENCENUMBER', ''),
            'licensee': props.get('OWNERNAME', ''),
            'substance': props.get('SUBSTANCE', ''),
            'segment_id': props.get('SEGMENTIDENTIFIER', ''),
            'segment_number': props.get('SEGMENTNUMBER'),
            'status': props.get('SEGMENTSTATUS', ''),
            'licence_type': props.get('LICENCETYPE', ''),
            'pipeline_name': props.get('PIPELINENAME', ''),
            'from_lat': from_lat,
            'from_lon': from_lon,
            'to_lat': to_lat,
            'to_lon': to_lon,
            'length_m': props.get('SHAPE.LEN'),
            'province': 'SK'
        })

    df = pd.DataFrame(records)

    # Convert numeric columns
    for col in ['from_lat', 'from_lon', 'to_lat', 'to_lon', 'length_m']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    logger.info(f"Parsed {len(df)} SK pipeline segments")
    return df
