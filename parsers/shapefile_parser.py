"""
Shapefile parser for AER spatial data.

Converts ZIP shapefiles to Parquet with:
- WKB geometry column for analytics
- Centroid lon/lat for web visualization
- CRS normalization to WGS84 (EPSG:4326)
"""

import logging
import tempfile
import zipfile
from pathlib import Path
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)


def parse_shapefile(
    file_path: Path,
    output_crs: str = "EPSG:4326",
    include_centroids: bool = True
) -> pd.DataFrame:
    """
    Parse a shapefile (or ZIP containing shapefile) to DataFrame.
    
    Args:
        file_path: Path to .shp or .zip file
        output_crs: Target CRS for output (default WGS84)
        include_centroids: If True, add centroid_lon, centroid_lat columns
        
    Returns:
        DataFrame with geometry as WKB and optional centroids
    """
    try:
        import geopandas as gpd
        from shapely import wkb
    except ImportError:
        raise ImportError("geopandas and shapely required for shapefile parsing")
    
    logger.info(f"Parsing shapefile: {file_path}")
    
    # Handle ZIP files
    if file_path.suffix.lower() == '.zip':
        gdf = _read_shapefile_from_zip(file_path)
    else:
        gdf = gpd.read_file(file_path)
    
    logger.info(f"Loaded {len(gdf)} features with CRS: {gdf.crs}")
    
    # Reproject to target CRS
    if gdf.crs and str(gdf.crs) != output_crs:
        logger.info(f"Reprojecting from {gdf.crs} to {output_crs}")
        gdf = gdf.to_crs(output_crs)
    
    # Convert to DataFrame
    df = pd.DataFrame(gdf.drop(columns='geometry'))
    
    # Add WKB geometry
    df['geometry_wkb'] = gdf.geometry.apply(
        lambda g: g.wkb if g is not None else None
    )
    
    # Add centroids if requested
    if include_centroids:
        centroids = gdf.geometry.centroid
        df['centroid_lon'] = centroids.x
        df['centroid_lat'] = centroids.y
    
    # Normalize column names
    from parsers.txt_parser import normalize_column_name
    df.columns = [normalize_column_name(c) for c in df.columns]
    
    logger.info(f"Parsed shapefile with {len(df)} rows, columns: {list(df.columns)}")
    return df


def _read_shapefile_from_zip(zip_path: Path):
    """Extract and read shapefile from ZIP archive."""
    import geopandas as gpd
    
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        
        # Extract ZIP
        with zipfile.ZipFile(zip_path, 'r') as zf:
            zf.extractall(tmpdir)
        
        # Find .shp file(s)
        shp_files = list(tmpdir.rglob('*.shp'))
        
        if not shp_files:
            raise ValueError(f"No .shp file found in {zip_path}")
        
        if len(shp_files) > 1:
            logger.warning(f"Multiple .shp files found, using first: {shp_files[0].name}")
        
        return gpd.read_file(shp_files[0])


def shapefile_to_parquet(
    input_path: Path,
    output_path: Path,
    output_crs: str = "EPSG:4326",
    include_centroids: bool = True
) -> Path:
    """
    Convert shapefile to Parquet format.
    
    Args:
        input_path: Path to input .shp or .zip file
        output_path: Path for output .parquet file
        output_crs: Target CRS
        include_centroids: Include centroid columns
        
    Returns:
        Path to created Parquet file
    """
    import pyarrow as pa
    import pyarrow.parquet as pq
    
    df = parse_shapefile(input_path, output_crs, include_centroids)
    
    # Ensure output directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Write to Parquet
    table = pa.Table.from_pandas(df)
    pq.write_table(table, output_path, compression='snappy')
    
    logger.info(f"Wrote Parquet: {output_path}")
    return output_path
