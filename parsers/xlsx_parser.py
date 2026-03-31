"""
Excel (XLSX) parser for Alberta Open Data files.
"""

import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)


def parse_xlsx(file_path: Path, sheet_name: int = 0) -> pd.DataFrame:
    """
    Parse an Excel file.
    
    Args:
        file_path: Path to .xlsx file
        sheet_name: Sheet index or name to read
        
    Returns:
        DataFrame with normalized column names
    """
    logger.info(f"Parsing Excel file: {file_path}")
    
    df = pd.read_excel(
        file_path,
        sheet_name=sheet_name,
        dtype=str
    )
    
    # Normalize column names
    from parsers.txt_parser import normalize_column_name
    df.columns = [normalize_column_name(c) for c in df.columns]
    
    logger.info(f"Parsed {len(df)} rows from Excel")
    return df


def parse_facility_codes(file_path: Path) -> pd.DataFrame:
    """
    Parse the AER Facility and Facility Cost Centre listing from Alberta Open Data.
    
    This file contains reference codes for facility normalization.
    """
    logger.info(f"Parsing facility codes from: {file_path}")
    
    df = parse_xlsx(file_path)
    
    # Any specific transformations for this file
    # (column mappings, type conversions, etc.)
    
    return df


def parse_st3_provincial_stats(file_path: Path) -> pd.DataFrame:
    """
    Parse ST3 Provincial Statistics Excel files.
    
    ST3 files contain monthly production data by product type (Oil, Gas, NGL, 
    Butane, Ethane, Propane) at the provincial level.
    
    The files typically have:
    - Monthly rows
    - Columns for production, disposition, prices
    """
    logger.info(f"Parsing ST3 provincial stats from: {file_path}")
    
    # Extract product type from filename
    filename = Path(file_path).stem.lower()
    product_type = "unknown"
    for product in ["oil", "gas", "ngl", "butane", "ethane", "propane"]:
        if product in filename:
            product_type = product
            break
    
    # Handle prices files separately
    if "prices" in filename:
        product_type = f"{product_type}_prices"
    
    try:
        # Read Excel - skip header rows if needed
        df = pd.read_excel(file_path, sheet_name=0, header=None)
        
        # Find the header row (usually contains 'Month' or 'Year')
        header_row = 0
        for i, row in df.iterrows():
            row_str = ' '.join(str(v).lower() for v in row.values if pd.notna(v))
            if 'month' in row_str or 'year' in row_str or 'january' in row_str:
                header_row = i
                break
        
        # Re-read with proper header
        df = pd.read_excel(file_path, sheet_name=0, header=header_row)
        
        # Normalize column names
        from parsers.txt_parser import normalize_column_name
        df.columns = [normalize_column_name(str(c)) for c in df.columns]
        
        # Add product type column
        df['product_type'] = product_type
        
        # Remove completely empty rows
        df = df.dropna(how='all')
        
        # Extract year from filename for context
        import re
        year_match = re.search(r'(\d{4})', str(file_path))
        if year_match:
            df['source_year'] = year_match.group(1)
        
        logger.info(f"Parsed {len(df)} rows for {product_type} from ST3")
        return df
        
    except Exception as e:
        logger.error(f"Error parsing ST3 file {file_path}: {e}")
        # Return empty DataFrame with minimal columns
        return pd.DataFrame(columns=['product_type', 'error'])

