"""
TXT file parsers for AER data files.

Handles various TXT formats:
- Pipe-delimited (most ST reports)
- Tab-delimited
- Fixed-width (older formats)
"""

import logging
from pathlib import Path
from typing import Optional, List, Tuple
from io import StringIO, BytesIO
import zipfile

import pandas as pd

logger = logging.getLogger(__name__)


def extract_lines_from_zip(file_path: Path) -> Tuple[List[str], int]:
    """
    Extract text lines from a ZIP file, handling nested ZIPs.
    
    Some AER yearly archives contain nested ZIPs (e.g., dwll2024.zip contains
    dwll2024-01.zip, dwll2024-02.zip, etc.). This function handles both cases.
    
    Returns:
        Tuple of (all_lines, file_count) where all_lines is the combined content
        from all TXT files found, and file_count is how many TXT files were processed.
    """
    all_lines = []
    file_count = 0
    
    try:
        with zipfile.ZipFile(file_path, 'r') as zf:
            all_files = zf.namelist()
            txt_files = [n for n in all_files if n.lower().endswith('.txt')]
            nested_zips = [n for n in all_files if n.lower().endswith('.zip')]
            
            # If we have TXT files directly, use them
            if txt_files:
                target = txt_files[0]
                logger.info(f"Extracting {target} from {file_path}")
                with zf.open(target) as f:
                    content = f.read().decode('latin-1')
                    all_lines = content.split('\n')
                    file_count = 1
            
            # If we have nested ZIPs, extract from each one
            elif nested_zips:
                logger.info(f"Found {len(nested_zips)} nested ZIPs in {file_path}")
                for inner_zip_name in nested_zips:
                    try:
                        with zf.open(inner_zip_name) as inner_zip_file:
                            inner_data = BytesIO(inner_zip_file.read())
                            with zipfile.ZipFile(inner_data, 'r') as inner_zf:
                                inner_files = inner_zf.namelist()
                                inner_txt = [n for n in inner_files if n.lower().endswith('.txt')]
                                if inner_txt:
                                    with inner_zf.open(inner_txt[0]) as txt_file:
                                        content = txt_file.read().decode('latin-1')
                                        all_lines.extend(content.split('\n'))
                                        file_count += 1
                    except Exception as e:
                        logger.warning(f"Failed to extract {inner_zip_name}: {e}")
                        continue
                        
                if file_count > 0:
                    logger.info(f"Extracted content from {file_count} nested ZIPs")
            else:
                logger.warning(f"No TXT files or nested ZIPs found in {file_path}. Contents: {all_files[:10]}")
                
    except zipfile.BadZipFile:
        # Check if file is actually HTML (error page)
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                first_100 = f.read(100)
                if '<html' in first_100.lower() or '<!doctype' in first_100.lower():
                    logger.error(f"File is HTML error page, not ZIP: {file_path}")
                else:
                    logger.error(f"Invalid ZIP file: {file_path}")
        except:
            logger.error(f"Invalid ZIP file: {file_path}")
            
    return all_lines, file_count


def parse_st102_facility(file_path: Path, encoding: str = 'utf-8') -> pd.DataFrame:
    """
    Parse ST102 ActiveFacility.txt or InactiveFacility.txt
    
    The ST102 format is often a report style with a 4-5 line header.
    It is typically tab-separated or whitespace-separated.
    """
    logger.info(f"Parsing ST102 facility file: {file_path}")
    
    # Try different encodings
    for enc in [encoding, 'latin-1', 'cp1252']:
        try:
            # Detect header skip and delimiter
            with open(file_path, 'r', encoding=enc) as f:
                lines = [f.readline() for _ in range(10)]
            
            # Find the header row (typically starts with "Facility ID")
            header_idx = -1
            for i, line in enumerate(lines):
                if "Facility ID" in line:
                    header_idx = i
                    break
            
            if header_idx == -1:
                # Fallback to standard pipe-delimited if no report header found
                skiprows = 0
                sep = '|' if '|' in lines[0] else ','
            else:
                skiprows = header_idx
                # Check for tabs vs multiple spaces
                test_line = lines[header_idx]
                sep = '\t' if '\t' in test_line else r'\s{2,}'
            
            # Read the file
                df = pd.read_csv(
                    file_path,
                    sep=sep,
                    skiprows=skiprows,
                    encoding=enc,
                    dtype=str,
                    engine='python',  # Required for \s{2,} regex
                    on_bad_lines='skip',
                    quoting=3  # QUOTE_NONE
                )
            
            # Normalize column names
            df.columns = [normalize_column_name(c) for c in df.columns]
            
            # Filter out any lingering footer lines (e.g. "Total Rows: ...")
            if 'facility_id' in df.columns:
                df = df[df['facility_id'].str.contains(r'[A-Z0-9]{5,}', na=False)]
            
            logger.info(f"Parsed {len(df)} rows with columns: {list(df.columns)}")
            return df
            
        except UnicodeDecodeError:
            continue
    
    raise ValueError(f"Could not parse {file_path}")


def parse_st49_spud(file_path: Path, encoding: str = 'utf-8') -> pd.DataFrame:
    """
    Parse ST49 daily SPUD files (Daily Drilling Activity).
    ASCII format with multi-line layout.
    Supports both direct TXT files and ZIP archives (including nested ZIPs).
    """
    logger.info(f"Parsing ST49 spud file: {file_path}")
    
    import re
    
    uwi_pattern = re.compile(r'(\d{2}/\d{2}-\d{2}-\d{3}-\d{2}W\d/\d)')
    licence_pattern = re.compile(r'(\d{7})')
    
    # Handle ZIP archives (including nested ZIPs)
    if Path(file_path).suffix.lower() == '.zip':
        lines, file_count = extract_lines_from_zip(file_path)
        if not lines:
            return pd.DataFrame()
    else:
        with open(file_path, 'r', encoding='latin-1') as f:
            lines = f.readlines()
    
    data = []
    for i, line in enumerate(lines):
        # Look for UWI
        match = uwi_pattern.search(line)
        if match:
            uwi = match.group(1)
            # Licence is often on the same line after the well name
            lic_match = licence_pattern.search(line[40:])
            licence = lic_match.group(1) if lic_match else None
            
            # Well name is between UWI and Licence
            well_name = line[match.end():].split(licence or '      ')[0].strip() if licence else line[match.end():].strip()
            
            # Look at next lines for more info if needed
            contractor = ""
            if i + 1 < len(lines) and "CONTRACTOR" not in lines[i+1]:
                 contractor = lines[i+1].strip() if isinstance(lines[i+1], str) else lines[i+1]
                 
            data.append({
                'uwi': uwi,
                'well_name': well_name,
                'licence_number': licence,
                'drilling_contractor': contractor,
                '_raw_line': line.strip() if isinstance(line, str) else line
            })
            
    df = pd.DataFrame(data)
    if not df.empty:
        df['spud_date'] = pd.Timestamp.now().strftime('%Y-%m-%d') # Fallback to file date ideally
        
    logger.info(f"Parsed {len(df)} spud records from report")
    return df


def parse_st1_licences(file_path: Path, encoding: str = 'utf-8') -> pd.DataFrame:
    """
    Parse ST1 daily Well Licences Issued.

    ST1 has two formats:
    1. NEW LICENCES (5-line blocks):
       Line 1: Well Name + Licence Number + MINERAL RIGHTS + Ground Elevation
       Line 2: UWI + Surface Coords + AER Field Centre + Projected Depth
       Line 3: AER Classification + Field + Terminating Zone
       Line 4: Drilling Operation + Well Purpose + Well Type + Substance
       Line 5: LICENSEE NAME + Surface Location

    2. AMENDMENTS (variable format with labels like "UWI:", "WELL NAME:", etc.)
       These use a different structure and are skipped.

    The key identifier for a NEW LICENCE line is:
    - Has a 7-digit licence number
    - Has "ALBERTA CROWN" or other mineral rights indicator
    - Followed by a UWI on line 2

    Supports both direct TXT files and ZIP archives (including nested ZIPs).
    """
    logger.info(f"Parsing ST1 licences file: {file_path}")

    import re

    # Handle ZIP archives (including nested ZIPs)
    if Path(file_path).suffix.lower() == '.zip':
        lines, file_count = extract_lines_from_zip(file_path)
        if not lines:
            return pd.DataFrame()
    else:
        with open(file_path, 'r', encoding='latin-1') as f:
            lines = f.readlines()

    # Patterns for NEW LICENCE format
    # Line 1: Has licence number (7 digits) AND mineral rights indicator
    new_licence_pattern = re.compile(r'\s(\d{7})\s+(ALBERTA CROWN|FREEHOLD|CROWN)')
    # UWI: format like 109/01-06-086-06W4/05 or 100/04-14-090-14W4/02
    uwi_pattern = re.compile(r'(\d{3}/\d{2}-\d{2}-\d{3}-\d{2}W\d/\d{2})')
    # Licensee line: starts with company name, ends with DLS location pattern
    # Company names end with LTD., INC., CORP., ULC, etc.
    dls_pattern = re.compile(r'\d{2}-\d{2}-\d{3}-\d{2}W\d')

    data = []
    i = 0
    while i < len(lines):
        line = lines[i]

        # Look for NEW LICENCE format (has licence number AND mineral rights)
        lic_match = new_licence_pattern.search(line)
        if lic_match:
            licence = lic_match.group(1)
            well_name = line[:lic_match.start()].strip()

            # Extract UWI from line 2 (next line)
            uwi = None
            if i + 1 < len(lines):
                uwi_match = uwi_pattern.search(lines[i + 1])
                if uwi_match:
                    uwi = uwi_match.group(1)

            # Extract licensee from line 5 (4 lines down)
            # Only if it looks like a company name (not a label like "WELL NAME:")
            licensee = None
            if i + 4 < len(lines):
                licensee_line = lines[i + 4].strip()
                # Skip if it's a header, label, or amendment format
                if licensee_line and not licensee_line.startswith('-'):
                    if not re.search(r'^\s*(UWI|WELL NAME|BOTTOMHOLE|LICENCE):', licensee_line):
                        # Company name is before the DLS location
                        dls_match = dls_pattern.search(licensee_line)
                        if dls_match:
                            licensee = licensee_line[:dls_match.start()].strip()
                        else:
                            # No DLS, just take the whole line as company name
                            licensee = licensee_line.strip()

                        # Validate it looks like a company name
                        # (contains common suffixes or is all caps with spaces)
                        if licensee and len(licensee) > 3:
                            # Good enough to be a company name
                            pass
                        else:
                            licensee = None

            data.append({
                'licence_number': licence,
                'uwi': uwi,
                'well_name': well_name,
                'licensee': licensee,
                'issue_date': pd.Timestamp.now().strftime('%Y-%m-%d')
            })

            # Skip to after this record block (5 lines + blank line typically)
            i += 5
        else:
            i += 1

    df = pd.DataFrame(data)
    # Deduplicate in case a licence spans multiple descriptive lines
    if not df.empty:
        df = df.drop_duplicates(subset=['licence_number'])

    logger.info(f"Parsed {len(df)} licence records with licensee info")
    return df


def parse_st37_wells(file_path: Path, encoding: str = 'utf-8') -> pd.DataFrame:
    """
    Parse ST37 List of Wells TXT file.
    This is typically in a ZIP archive and is whitespace-delimited (2+ spaces).

    ST37 Column Layout (from AER documentation):
    - Col 0: UWI_DISPLAY_FORMAT (e.g., 00/06-06-001-01W4/0)
    - Col 1: UWI_ID (LOC-DESC key, e.g., 14010606000)
    - Col 2: UPDATE_FLAG
    - Col 3: WELL_NAME
    - Col 4: FIELD_CODE
    - Col 5: POOL_CODE
    - Col 6: OS_AREA_CODE
    - Col 7: OS_DEP_CODE
    - Col 8: LICENCE_NO
    - Col 9: LICENCE_STATUS
    - Col 10: LICENCE_ISSUE_DATE
    - Col 11: LICENSEE_CODE (BA code)
    - Col 12: AGENT_CODE
    - Col 13: OPERATOR_CODE
    - Col 14: FIN_DRL_DATE (finished drilling date)
    - Col 15: WELL_TOTAL_DEPTH (meters)
    - Col 16: WELL_STAT_CODE
    - Col 17: WELL_STAT_DATE
    - Col 18: FLUID
    - Col 19: MODE (ABD=abandoned, etc.)
    - Col 20: TYPE
    - Col 21: STRUCTURE

    Uses streaming approach to minimize memory usage.
    """
    logger.info(f"Parsing ST37 wells file: {file_path}")

    import zipfile
    import io

    if file_path.suffix.lower() == '.zip':
        with zipfile.ZipFile(file_path, 'r') as zf:
            txt_files = [n for n in zf.namelist() if n.lower().endswith('.txt')]
            if not txt_files:
                raise ValueError(f"No TXT file found in {file_path}")
            # Usually WellList.txt
            target = [f for f in txt_files if 'welllist' in f.lower()]
            target_file = target[0] if target else txt_files[0]

            # Stream directly from ZIP using TextIOWrapper (memory efficient)
            with zf.open(target_file) as f:
                text_stream = io.TextIOWrapper(f, encoding='latin-1')
                df = pd.read_csv(
                    text_stream,
                    sep='\t',  # ST37 is tab-delimited
                    header=None,
                    on_bad_lines='skip',
                    quoting=3,  # QUOTE_NONE
                    dtype=str
                )
    else:
        # Non-ZIP file - read directly
        df = pd.read_csv(
            file_path,
            sep='\t',  # ST37 is tab-delimited
            header=None,
            encoding='latin-1',
            on_bad_lines='skip',
            quoting=3,  # QUOTE_NONE
            dtype=str
        )

    # ST37 column names based on actual AER file structure
    st37_columns = [
        'uwi',              # 0: UWI_DISPLAY_FORMAT (e.g., 00/06-06-001-01W4/0)
        'uwi_id',           # 1: UWI_ID / LOC-DESC key (e.g., 14010606000)
        'update_flag',      # 2: UPDATE_FLAG
        'well_name',        # 3: WELL_NAME
        'field_code',       # 4: FIELD_CODE (links to field reference)
        'pool_code',        # 5: POOL_CODE
        'os_area_code',     # 6: OS_AREA_CODE
        'os_dep_code',      # 7: OS_DEP_CODE
        'licence_no',       # 8: LICENCE_NO
        'licence_status',   # 9: LICENCE_STATUS (e.g., RecCertified)
        'licence_issue_date', # 10: LICENCE_ISSUE_DATE (YYYYMMDD)
        'licensee_code',    # 11: LICENSEE_CODE (BA code, e.g., A5D40)
        'agent_code',       # 12: AGENT_CODE
        'operator_code',    # 13: OPERATOR_CODE
        'fin_drl_date',     # 14: FIN_DRL_DATE (finished drilling, YYYYMMDD)
        'well_total_depth', # 15: WELL_TOTAL_DEPTH (meters)
        'well_stat_code',   # 16: WELL_STAT_CODE
        'well_stat_date',   # 17: WELL_STAT_DATE (YYYYMMDD)
        'fluid',            # 18: FLUID
        'mode',             # 19: MODE (ABD=abandoned, etc.)
        'type',             # 20: TYPE
        'structure',        # 21: STRUCTURE
    ]

    # Assign column names based on how many columns we actually have
    if len(df.columns) >= len(st37_columns):
        df.columns = st37_columns + [f'col_{i}' for i in range(len(st37_columns), len(df.columns))]
    elif len(df.columns) > 0:
        # Assign as many column names as we have columns
        df.columns = st37_columns[:len(df.columns)]

    # Log column info for debugging
    logger.info(f"ST37 parsed with {len(df.columns)} columns: {list(df.columns[:10])}...")

    logger.info(f"Parsed {len(df)} wells from ST37 TXT")
    return df


def parse_petrinex_production(file_path: Path) -> pd.DataFrame:
    """
    Parse Petrinex Conventional Volumetric Data CSV.
    Note: The file can be a double-zipped CSV.
    """
    import zipfile
    import io
    
    logger.info(f"Parsing Petrinex production file: {file_path}")
    
    def extract_from_zip(path):
        with zipfile.ZipFile(path) as z:
            # Check for inner zips (Petrinex style)
            inner_files = z.namelist()
            if any(f.lower().endswith('.zip') for f in inner_files):
                inner_zip = [f for f in inner_files if f.lower().endswith('.zip')][0]
                with z.open(inner_zip) as f_inner:
                    # Nested zip
                    with zipfile.ZipFile(io.BytesIO(f_inner.read())) as z2:
                        csv_file = [f for f in z2.namelist() if f.lower().endswith('.csv')][0]
                        return pd.read_csv(z2.open(csv_file), dtype=str)
            else:
                csv_file = [f for f in inner_files if f.lower().endswith('.csv')][0]
                return pd.read_csv(z.open(csv_file), dtype=str)

    if str(file_path).lower().endswith('.zip'):
        try:
            df = extract_from_zip(file_path)
        except zipfile.BadZipFile:
            # Corrupted download - delete and re-raise so it re-downloads next run
            logger.error(f"Corrupted zip file detected, deleting: {file_path}")
            Path(file_path).unlink(missing_ok=True)
            raise
    else:
        df = pd.read_csv(file_path, dtype=str)
        
    # Standardize column names
    # Petrinex CSV commonly includes:
    # - ProductionMonth, ActivityID, ProductID, Volume, Energy, Hours, FromToIDIdentifier, ...
    #
    # We normalize headers and keep a simple, explicit canonical set used by silver:
    # - productionmonth, activityid, productid, volume, energy, hours_on, fromtoididentifier
    df.columns = [normalize_column_name(c) for c in df.columns]

    # Canonicalize key/value columns (be tolerant to minor header variants)
    rename_cols = {}
    if 'production_month' in df.columns and 'productionmonth' not in df.columns:
        rename_cols['production_month'] = 'productionmonth'
    if 'activity_id' in df.columns and 'activityid' not in df.columns:
        rename_cols['activity_id'] = 'activityid'
    if 'product_id' in df.columns and 'productid' not in df.columns:
        rename_cols['product_id'] = 'productid'
    if 'hours' in df.columns and 'hours_on' not in df.columns:
        rename_cols['hours'] = 'hours_on'
    df = df.rename(columns=rename_cols)
    
    # Attempt to find UWI (often in 'well' or 'facility_id' if type is WI/WP)
    if 'well' in df.columns:
        df = df.rename(columns={'well': 'uwi'})
    elif 'facility_id' in df.columns and 'uwi' not in df.columns:
        # Check if any rows look like UWIs (e.g. 16 chars with / and -)
        import re
        uwi_pattern = re.compile(r'\d{2}/\d{2}-\d{2}-\d{3}-\d{2}W\d/\d')
        if df['facility_id'].astype(str).str.contains(uwi_pattern, na=False).any():
            df = df.rename(columns={'facility_id': 'uwi'})
            
    logger.info(f"Parsed {len(df)} Petrinex production rows")
    return df


def parse_general_well_data(file_path: Path, encoding: str = 'utf-8') -> pd.DataFrame:
    """
    Parse General Well Data File - All Alberta.
    
    This is a large bulk file with well reference data.
    Uses chunked reading for memory efficiency.
    """
    logger.info(f"Parsing General Well Data file: {file_path}")
    
    # Check if ZIP
    if file_path.suffix.lower() == '.zip':
        import zipfile
        with zipfile.ZipFile(file_path, 'r') as zf:
            txt_files = [n for n in zf.namelist() if n.lower().endswith('.txt')]
            if not txt_files:
                raise ValueError(f"No TXT file found in {file_path}")
            
            with zf.open(txt_files[0]) as f:
                # Read in chunks for large files
                chunks = []
                for chunk in pd.read_csv(
                    f,
                    delimiter='|',
                    encoding=encoding,
                    dtype=str,
                    chunksize=100000,
                    on_bad_lines='skip'
                ):
                    chunks.append(chunk)
                df = pd.concat(chunks, ignore_index=True)
    else:
        df = pd.read_csv(
            file_path,
            delimiter='|',
            encoding=encoding,
            dtype=str,
            on_bad_lines='skip'
        )
    
    df.columns = [normalize_column_name(c) for c in df.columns]
    
    logger.info(f"Parsed {len(df)} well records")
    return df


def parse_well_production(file_path: Path, encoding: str = 'utf-8') -> pd.DataFrame:
    """
    Parse Well Production Data File - All Alberta.
    
    This is a very large time-series file with monthly production data.
    Uses streaming/chunked processing.
    """
    logger.info(f"Parsing Well Production file: {file_path}")
    
    if file_path.suffix.lower() == '.zip':
        import zipfile
        with zipfile.ZipFile(file_path, 'r') as zf:
            txt_files = [n for n in zf.namelist() if n.lower().endswith('.txt')]
            if not txt_files:
                raise ValueError(f"No TXT file found in {file_path}")
            
            with zf.open(txt_files[0]) as f:
                chunks = []
                for chunk in pd.read_csv(
                    f,
                    delimiter='|',
                    encoding=encoding,
                    dtype=str,
                    chunksize=500000,  # Larger chunks for production data
                    on_bad_lines='skip'
                ):
                    chunks.append(chunk)
                df = pd.concat(chunks, ignore_index=True)
    else:
        df = pd.read_csv(
            file_path,
            delimiter='|',
            encoding=encoding,
            dtype=str,
            on_bad_lines='skip'
        )
    
    df.columns = [normalize_column_name(c) for c in df.columns]
    
    logger.info(f"Parsed {len(df)} production records")
    return df


def normalize_column_name(name: str) -> str:
    """
    Normalize column names to snake_case.
    
    Examples:
        "Facility ID" -> "facility_id"
        "Operator Name" -> "operator_name"
        "UWI" -> "uwi"
    """
    import re
    
    # Strip whitespace
    name = str(name).strip()
    
    # Replace spaces and special chars with underscore
    name = re.sub(r'[\s\-\/\.\(\)]+', '_', name)
    
    # Convert camelCase to snake_case
    name = re.sub(r'([a-z])([A-Z])', r'\1_\2', name)
    
    # Lowercase
    name = name.lower()
    
    # Remove duplicate underscores
    name = re.sub(r'_+', '_', name)
    
    # Remove leading/trailing underscores
    name = name.strip('_')
    
    return name


def parse_confidential_well_list(file_path: Path) -> pd.DataFrame:
    """
    Parse the AER Confidential Well List (ConWell.txt).
    Fixed-width format with specific columns.
    """
    logger.info(f"Parsing confidential well list: {file_path}")
    
    # Define columns based on inspection
    # Positions are approximate/generous to capture full values
    colspecs = [
        (0, 21),   # uwi
        (21, 31),  # licence
        (31, 38),  # op_code
        (38, 74),  # op_name
        (74, 110), # conf_type
        (110, 130) # release_date
    ]
    names = ['uwi', 'licence', 'op_code', 'op_name', 'conf_type', 'release_date']
    
    # Read as FWF
    try:
        df = pd.read_fwf(file_path, colspecs=colspecs, names=names, dtype=str)
    except Exception as e:
        logger.error(f"Failed to read FWF confidential list: {e}")
        return pd.DataFrame()
        
    if df.empty:
        return df
        
    # Clean up
    df = df[~df['uwi'].isna()].copy()
    
    # Strip whitespace from all columns and uppercase UWI
    for col in df.columns:
        df[col] = df[col].astype(str).str.strip()
    
    if 'uwi' in df.columns:
        df['uwi'] = df['uwi'].str.upper()

        
    # Filter out header/decorative lines
    # Usually headers like "DIE - INT01", "Confidential Well List", "===" or "UWI"
    df = df[~df['uwi'].str.startswith(('---', '===', 'UWI', 'DIE', 'Page', 'Confidential'))].copy()
    
    # Valid UWIs should be roughly 19 characters for this format (00/01-01-001-01W1/0)
    df = df[df['uwi'].str.len() >= 10].copy()
    
    # Handle 'no date avail' and formatting
    if 'release_date' in df.columns:
        df['release_date'] = df['release_date'].replace('no date avail', None)
        # Attempt date parsing (format usually MM/DD/YYYY or similar in these reports)
        df['release_date_parsed'] = pd.to_datetime(df['release_date'], errors='coerce')
        
    logger.info(f"Parsed {len(df)} confidential wells")
    return df

