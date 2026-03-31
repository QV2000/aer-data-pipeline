"""
CSV parsers for AER datasets.

Handles:
- ST2 Weekly Status Changes
- ST2 Weekly Drilling
- Well-Facility Links
- Active Disposal Wells
"""

import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)


def parse_st2_status_changes(file_path: Path) -> pd.DataFrame:
    """
    Parse ST2 Weekly Status Changes CSV from Tableau export.

    Tableau columns are numbered like:
    - 00.UnFormatted UWI, 01.Formatted UWI, 02.Well Name, 03.Lic No,
    - 04.Licensee, 05.Fld, 06.Field Name, 07.StatCurr, 08.StatCurrDesc,
    - 09.StatPrev, 10.StatPrevDesc
    """
    logger.info(f"Parsing ST2 status changes from {file_path}")

    df = pd.read_csv(file_path, encoding='utf-8-sig')

    # Normalize column names - remove numbered prefixes and clean
    def clean_column_name(col):
        # Remove leading numbers like "00." or "10."
        import re
        col = re.sub(r'^\d{2}\.', '', col)
        # Standard normalization
        return col.lower().strip().replace(' ', '_')

    df.columns = [clean_column_name(c) for c in df.columns]

    # Rename to standard schema for ST2
    column_mapping = {
        # UWI columns
        'formatted_uwi': 'uwi',
        'unformatted_uwi': 'uwi_unformatted',
        # Well info
        'well_name': 'name',
        'lic_no': 'licence',
        'licensee': 'licensee_code',
        # Field
        'fld': 'field_code',
        'field_name': 'field_name',
        # Status
        'statcurr': 'new_status_code',
        'statcurrdesc': 'new_status',
        'statprev': 'old_status_code',
        'statprevdesc': 'old_status',
    }

    df = df.rename(columns={k: v for k, v in column_mapping.items() if k in df.columns})

    # Add event type
    df['event_type'] = 'status_change'

    # Extract date from filename (e.g., 2026-02-21_WeeklyStatusChangesReport.csv)
    import re
    date_match = re.search(r'(\d{4}-\d{2}-\d{2})', str(file_path))
    if date_match:
        df['event_date'] = pd.to_datetime(date_match.group(1))
    else:
        df['event_date'] = pd.Timestamp.now()

    logger.info(f"Parsed {len(df)} status change records")
    return df


def parse_st2_drilling(file_path: Path) -> pd.DataFrame:
    """
    Parse ST2 Weekly Drilling Report CSV.

    Tableau columns are numbered like:
    - 00.UnFormatted UWI, 01.Formatted UWI, 02.Well Name, 03.AER Classification,
    - 04.StatDate, 05.StatusCDesc, 06.Bottom Location, 07.Surface Location,
    - 08.Lic#, 09.LicenseeBAId, 10.Gr Elev, 11.KB Elev, 12.Field Id,
    - 13.FDD, 14.Final TD, 15.SpudDate, 16.Max TVD
    """
    logger.info(f"Parsing ST2 drilling report from {file_path}")

    df = pd.read_csv(file_path, encoding='utf-8-sig')

    # Normalize column names - remove numbered prefixes and clean
    import re
    def clean_column_name(col):
        # Remove leading numbers like "00." or "16."
        col = re.sub(r'^\d{2}\.', '', col)
        # Standard normalization
        return col.lower().strip().replace(' ', '_').replace('#', '_no')

    df.columns = [clean_column_name(c) for c in df.columns]

    # Rename to standard schema for ST2 drilling
    column_mapping = {
        # UWI columns
        'formatted_uwi': 'uwi',
        'unformatted_uwi': 'uwi_unformatted',
        # Well info
        'well_name': 'name',
        'lic_no': 'licence',
        'licenseebaid': 'licensee_code',
        'aer_classification': 'well_class',
        # Status
        'statdate': 'stat_date',
        'statuscdesc': 'status_desc',
        # Location
        'bottom_location': 'bottom_location',
        'surface_location': 'surface_location',
        # Field
        'field_id': 'field_code',
        # Drilling
        'fdd': 'drill_date',
        'final_td': 'total_depth',
        'spuddate': 'spud_date',
        'max_tvd': 'max_tvd',
        # Elevation
        'gr_elev': 'ground_elev',
        'kb_elev': 'kb_elev',
    }

    df = df.rename(columns={k: v for k, v in column_mapping.items() if k in df.columns})

    # Add event type
    df['event_type'] = 'drilling'

    # Parse dates
    for date_col in ['stat_date', 'drill_date', 'spud_date']:
        if date_col in df.columns:
            df[date_col] = pd.to_datetime(df[date_col], errors='coerce')

    # Extract date from filename for event_date
    date_match = re.search(r'(\d{4}-\d{2}-\d{2})', str(file_path))
    if date_match:
        df['event_date'] = pd.to_datetime(date_match.group(1))
    else:
        df['event_date'] = pd.Timestamp.now()

    logger.info(f"Parsed {len(df)} drilling records")
    return df


def parse_well_facility_links(file_path: Path) -> pd.DataFrame:
    """
    Parse Well-to-Facility Links CSV from AER Catalogue #219.

    Maps wells (UWI) to their linked facilities (batteries, plants, etc.)
    """
    logger.info(f"Parsing well-facility links from {file_path}")

    df = pd.read_csv(file_path, encoding='utf-8-sig')

    # Normalize column names
    df.columns = df.columns.str.lower().str.strip().str.replace(' ', '_')

    column_mapping = {
        'well_id': 'uwi',
        'well_identifier': 'uwi',
        'uwi': 'uwi',
        'well_name': 'well_name',
        'well_status': 'well_status',
        'linked_facility_id': 'facility_id',
        'facility_id': 'facility_id',
        'linked_facility_name': 'facility_name',
        'facility_name': 'facility_name',
        'linked_facility_sub_type': 'facility_subtype',
        'facility_sub_type': 'facility_subtype',
        'linked_facility_operator_ba_id': 'operator_ba_id',
        'operator_ba_id': 'operator_ba_id',
        'effective_date': 'effective_date',
    }

    df = df.rename(columns={k: v for k, v in column_mapping.items() if k in df.columns})

    # Parse date if present
    if 'effective_date' in df.columns:
        df['effective_date'] = pd.to_datetime(df['effective_date'], errors='coerce')

    logger.info(f"Parsed {len(df)} well-facility link records")
    return df


def parse_petrinex_well_facility_link(file_path: Path) -> pd.DataFrame:
    """
    Parse Petrinex Well to Facility Link CSV.

    This is the critical dataset for pipeline connectivity classification.
    Maps each well (UWI) to its linked facility (battery).

    Key columns:
    - WellIdentifier: Well UWI (e.g., 100010100816W400)
    - LinkedFacilityID: Battery ID (e.g., ABBT8960001)
    - LinkedFacilitySubType: 311=single-well, 321=multiwell group, 322=multiwell proration
    - WellStatusFluid: CR-OIL, GAS, WATER, etc.
    - WellLegalSubdivision/Section/Township/Range/Meridian: DLS location
    """
    import io
    import zipfile

    logger.info(f"Parsing Petrinex well-facility link from {file_path}")

    # The Petrinex API returns a nested ZIP: outer ZIP contains inner ZIP contains CSV
    # Check if the file is a ZIP and extract the CSV
    csv_content = None
    try:
        with open(file_path, 'rb') as f:
            file_bytes = f.read()

        # Check if it's a ZIP file (starts with PK)
        if file_bytes[:2] == b'PK':
            logger.info("File is a ZIP archive, extracting...")
            with zipfile.ZipFile(io.BytesIO(file_bytes)) as outer_zip:
                for name in outer_zip.namelist():
                    logger.info(f"  Found in outer ZIP: {name}")
                    inner_bytes = outer_zip.read(name)

                    # Check if this is another ZIP (nested)
                    if inner_bytes[:2] == b'PK' or name.endswith('.zip'):
                        logger.info(f"  Extracting nested ZIP: {name}")
                        with zipfile.ZipFile(io.BytesIO(inner_bytes)) as inner_zip:
                            for inner_name in inner_zip.namelist():
                                if inner_name.upper().endswith('.CSV'):
                                    logger.info(f"    Found CSV: {inner_name}")
                                    csv_content = inner_zip.read(inner_name)
                                    break
                    elif name.upper().endswith('.CSV'):
                        csv_content = inner_bytes
                        break

                    if csv_content:
                        break
    except Exception as e:
        logger.warning(f"ZIP extraction failed: {e}, trying as raw CSV")

    # Parse the CSV content
    df = None
    for encoding in ['utf-8-sig', 'utf-8', 'cp1252', 'latin-1']:
        try:
            if csv_content:
                df = pd.read_csv(io.BytesIO(csv_content), encoding=encoding, on_bad_lines='skip')
            else:
                df = pd.read_csv(file_path, encoding=encoding, on_bad_lines='skip')
            logger.info(f"Successfully read with encoding: {encoding}")
            break
        except UnicodeDecodeError:
            logger.warning(f"Failed to read with encoding: {encoding}")
            continue
        except Exception as e:
            logger.warning(f"Failed to read with encoding {encoding}: {e}")
            continue

    if df is None:
        raise ValueError(f"Could not parse {file_path} with any known encoding")

    logger.info(f"Raw columns: {list(df.columns)}")

    # Map Petrinex columns to our standard schema
    column_mapping = {
        'WellID': 'well_id',
        'WellProvinceState': 'province',
        'WellType': 'well_type',
        'WellIdentifier': 'well_identifier',
        'WellLocationException': 'location_exception',
        'WellLegalSubdivision': 'well_lsd',
        'WellSection': 'well_sec',
        'WellTownship': 'well_twp',
        'WellRange': 'well_rng',
        'WellMeridian': 'well_mer',
        'WellEventSequence': 'event_sequence',
        'WellName': 'well_name',
        'WellStatusFluid': 'fluid',
        'WellStatusMode': 'mode',
        'WellStatusType': 'type',
        'WellStatusStructure': 'structure',
        'WellStatusFluidCode': 'fluid_code',
        'WellStatusModeCode': 'mode_code',
        'WellStatusTypeCode': 'type_code',
        'WellStatusStructureCode': 'structure_code',
        'WellStatusStartDate': 'status_date',
        'LinkedFacilityID': 'linked_facility_id',
        'LinkedFacilityProvinceState': 'facility_province',
        'LinkedFacilityType': 'linked_facility_type',
        'LinkedFacilityIdentifier': 'facility_identifier',
        'LinkedFacilityName': 'facility_name',
        'LinkedFacilitySubType': 'linked_facility_sub_type',
        'LinkedFacilitySubTypeDesc': 'facility_sub_type_desc',
        'LinkedStartDate': 'link_start_date',
        'LinkedFacilityOperatorBAID': 'facility_operator_ba_id',
        'LinkedFacilityOperatorName': 'facility_operator_name',
    }

    df = df.rename(columns=column_mapping)

    # Parse dates
    for date_col in ['status_date', 'link_start_date']:
        if date_col in df.columns:
            df[date_col] = pd.to_datetime(df[date_col], errors='coerce')

    # Build well DLS string for matching (format: LSD-SEC-TWP-RGE-MER like "01-01-008-16W4")
    def format_well_dls(row):
        try:
            lsd = str(row.get('well_lsd', '') or '').strip().zfill(2)
            sec = str(row.get('well_sec', '') or '').strip().zfill(2)
            twp = str(row.get('well_twp', '') or '').strip().zfill(3)
            rng = str(row.get('well_rng', '') or '').strip().zfill(2)
            mer = str(row.get('well_mer', '') or '').strip()
            if lsd and sec and twp and rng and mer:
                return f"{lsd}-{sec}-{twp}-{rng}W{mer}"
        except Exception:
            pass
        return None

    df['well_dls'] = df.apply(format_well_dls, axis=1)

    logger.info(f"Parsed {len(df)} well-facility link records")
    logger.info(f"  Crude oil wells (CR-OIL): {len(df[df['fluid'] == 'CR-OIL'])}")
    logger.info(f"  Gas wells: {len(df[df['fluid'] == 'GAS'])}")
    logger.info(f"  Unique batteries: {df['linked_facility_id'].nunique()}")

    return df


def parse_disposal_wells(file_path: Path) -> pd.DataFrame:
    """
    Parse Active Disposal Well List CSV from AER Catalogue #308.

    Lists wells approved for injection/disposal with scheme details.
    """
    logger.info(f"Parsing disposal wells from {file_path}")

    df = pd.read_csv(file_path, encoding='utf-8-sig')

    # Normalize column names
    df.columns = df.columns.str.lower().str.strip().str.replace(' ', '_')

    column_mapping = {
        'well_id': 'uwi',
        'well_identifier': 'uwi',
        'uwi': 'uwi',
        'approval_number': 'approval_number',
        'approval_no': 'approval_number',
        'approval_holder': 'approval_holder',
        'operator': 'approval_holder',
        'scheme_type': 'scheme_type',
        'scheme_sub_type': 'scheme_subtype',
        'scheme_status': 'scheme_status',
        'status': 'scheme_status',
    }

    df = df.rename(columns={k: v for k, v in column_mapping.items() if k in df.columns})

    logger.info(f"Parsed {len(df)} disposal well records")
    return df
