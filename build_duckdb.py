"""
DuckDB builder for the AER data pipeline.

Creates/updates a DuckDB database with views onto the silver Parquet files.
"""

import logging
import os
from pathlib import Path

import duckdb

logger = logging.getLogger(__name__)


def build_duckdb(data_dir: Path, db_path: Path = None) -> Path:
    """
    Build/refresh the DuckDB database.
    
    Creates views for each silver table pointing to the Parquet files.
    
    Args:
        data_dir: Base data directory
        db_path: Path to DuckDB file (default: data_dir/../aer_data.duckdb)
        
    Returns:
        Path to created/updated DuckDB file
    """
    import gc
    
    if db_path is None:
        db_path = data_dir.parent / "aer_data.duckdb"
    
    silver_dir = data_dir / "silver"
    
    if not silver_dir.exists():
        raise ValueError(f"Silver directory not found: {silver_dir}")
    
    logger.info(f"Building DuckDB at: {db_path}")

    conn = duckdb.connect(str(db_path))

    # Memory optimization - configurable via environment
    memory_limit = os.environ.get('DUCKDB_MEMORY_LIMIT', '2GB')
    conn.execute(f"SET memory_limit='{memory_limit}'")
    
    try:
        for table_dir in silver_dir.iterdir():
            if not table_dir.is_dir():
                continue

            table_name = table_dir.name
            # Prefer partitioned data if present (avoids huge single-parquet rebuilds)
            parts_dir = table_dir / "parts"
            parquet_glob = None
            if parts_dir.exists() and any(parts_dir.glob("*.parquet")):
                parquet_glob = (parts_dir / "*.parquet").as_posix()
            else:
                parquet_path = table_dir / "latest.parquet"
                if parquet_path.exists():
                    parquet_glob = parquet_path.as_posix()

            if not parquet_glob:
                logger.warning(f"No parquet data found for {table_name}")
                continue
            
            # Create or replace view
            # Using read_parquet for flexibility with schema changes
            if table_name == 'production':
                # Special handling for production:
                # 1. Use union_by_name=true to handle schema differences across months
                # 2. Add normalized_uwi column for SK well joins
                query = f"""
                    CREATE OR REPLACE VIEW {table_name} AS
                    SELECT
                        *,
                        CASE
                            WHEN province = 'SK' AND uwi LIKE '%/%' THEN
                                '1' ||
                                LPAD(SPLIT_PART(uwi, '/', 1), 2, '0') ||
                                LPAD(SPLIT_PART(SPLIT_PART(uwi, '/', 2), '-', 1), 2, '0') ||
                                LPAD(SPLIT_PART(SPLIT_PART(uwi, '/', 2), '-', 2), 2, '0') ||
                                LPAD(SPLIT_PART(SPLIT_PART(uwi, '/', 2), '-', 3), 3, '0') ||
                                SPLIT_PART(SPLIT_PART(uwi, '/', 2), '-', 4) ||
                                SPLIT_PART(uwi, '/', 3) ||
                                '0'
                            ELSE uwi
                        END AS normalized_uwi
                    FROM read_parquet('{parquet_glob}', union_by_name=true)
                """
            else:
                query = f"""
                    CREATE OR REPLACE VIEW {table_name} AS
                    SELECT * FROM read_parquet('{parquet_glob}')
                """
            conn.execute(query)
            logger.info(f"Created view '{table_name}'")
        
        # Create some useful aggregate views
        _create_summary_views(conn)

        # Import ST103 lookup tables (field and pool code mappings)
        _import_st103_lookups(conn, data_dir.parent)

        # Create heavy oil quality classification views
        _create_heavy_oil_views(conn)

        conn.commit()
        
    finally:
        conn.close()
    
    # Force garbage collection after heavy DB work
    gc.collect()
    
    logger.info(f"DuckDB build complete: {db_path}")
    return db_path


def _import_st103_lookups(conn: duckdb.DuckDBPyConnection, base_dir: Path):
    """Import AER ST103 Field and Pool lookup tables from Excel files."""
    import pandas as pd
    import gc
    
    field_list_path = base_dir / "FieldList.xlsx"
    field_pool_path = base_dir / "FieldPoolList.xlsx"
    
    # Import field lookup
    if field_list_path.exists():
        try:
            df = pd.read_excel(field_list_path)
            df.columns = [c.strip().lower().replace(' ', '_') for c in df.columns]
            conn.execute("DROP TABLE IF EXISTS field_lookup")
            conn.execute("CREATE TABLE field_lookup AS SELECT * FROM df")
            logger.info(f"Created table 'field_lookup'")
            del df  # Free memory immediately
            gc.collect()
        except Exception as e:
            logger.warning(f"Failed to import FieldList.xlsx: {e}")
    else:
        logger.warning(f"FieldList.xlsx not found at {field_list_path}")
    
    # Import field/pool lookup
    if field_pool_path.exists():
        try:
            df = pd.read_excel(field_pool_path)
            df.columns = [c.strip().lower().replace(' ', '_') for c in df.columns]
            conn.execute("DROP TABLE IF EXISTS field_pool_lookup")
            conn.execute("CREATE TABLE field_pool_lookup AS SELECT * FROM df")
            logger.info(f"Created table 'field_pool_lookup'")
            del df  # Free memory immediately
            gc.collect()
        except Exception as e:
            logger.warning(f"Failed to import FieldPoolList.xlsx: {e}")
    else:
        logger.warning(f"FieldPoolList.xlsx not found at {field_pool_path}")


def _create_summary_views(conn: duckdb.DuckDBPyConnection):
    """Create useful summary/aggregate views."""

    # Create wells alias view if wells doesn't exist but well_attributes does
    # This handles disk space errors where wells parquet fails to write
    try:
        conn.execute("SELECT 1 FROM wells LIMIT 1")
    except Exception:
        try:
            conn.execute("SELECT 1 FROM well_attributes LIMIT 1")
            conn.execute("CREATE OR REPLACE VIEW wells AS SELECT * FROM well_attributes")
            logger.info("Created alias view 'wells' -> 'well_attributes'")
        except Exception:
            logger.debug("Neither wells nor well_attributes available")

    # Create licence_activity alias -> well_licences (code uses both names)
    try:
        conn.execute("SELECT 1 FROM well_licences LIMIT 1")
        conn.execute("CREATE OR REPLACE VIEW licence_activity AS SELECT * FROM well_licences")
        logger.info("Created alias view 'licence_activity' -> 'well_licences'")
    except Exception:
        logger.debug("well_licences not available for alias")

    # Create operator normalization and grouping views
    _create_operator_views(conn)

    # Check if facilities table exists
    try:
        conn.execute("SELECT 1 FROM facilities LIMIT 1")
        
        # Facilities by operator
        conn.execute("""
            CREATE OR REPLACE VIEW facilities_by_operator AS
            SELECT 
                operator_name,
                COUNT(*) as facility_count,
                SUM(CASE WHEN is_active THEN 1 ELSE 0 END) as active_count,
                SUM(CASE WHEN NOT is_active THEN 1 ELSE 0 END) as inactive_count
            FROM facilities
            WHERE operator_name IS NOT NULL
            GROUP BY operator_name
            ORDER BY facility_count DESC
        """)
        logger.info("Created view 'facilities_by_operator'")
        
    except Exception as e:
        logger.debug(f"Skipping facilities summary: {e}")


def _create_operator_views(conn: duckdb.DuckDBPyConnection):
    """
    Create operator normalization and grouping views.

    This solves the problem of the same company appearing multiple times
    in the operator dropdown due to:
    - Different BA codes from acquisitions
    - Slight name variations (Corp vs Corp. vs Corporation)
    - Typos (Tenaz vs Tenax)
    """
    try:
        conn.execute("SELECT 1 FROM wells LIMIT 1")
    except Exception:
        logger.debug("Skipping operator views - wells table not available")
        return

    # 1. Create operator_aliases table for manual overrides
    # This allows mapping known variations to canonical names
    conn.execute("""
        CREATE OR REPLACE TABLE operator_aliases (
            ba_code VARCHAR PRIMARY KEY,
            raw_name VARCHAR,
            canonical_name VARCHAR,
            notes VARCHAR
        )
    """)

    # Insert known aliases (typos, acquisitions, etc.)
    # These are manual overrides for cases automation can't catch
    # NOTE: These only apply if the BA code exists in wells table
    known_aliases = [
        # Tenax → Tenaz (typo or predecessor) - A04N0 has "TENAX" wells
        # But A04N0 is TVE (Tamarack Valley Energy) which is much larger
        # So we don't alias A04N0 to TENAZ - the 1 TENAX well is historical
        # Add more as needed when you find real cases of name variations
    ]

    for ba_code, raw_name, canonical_name, notes in known_aliases:
        conn.execute("""
            INSERT OR REPLACE INTO operator_aliases (ba_code, raw_name, canonical_name, notes)
            VALUES (?, ?, ?, ?)
        """, [ba_code, raw_name, canonical_name, notes])

    logger.info("Created table 'operator_aliases' with manual overrides")

    # 2. Create a macro for name normalization
    # Standardizes Corp/Corp./Corporation, Inc/Inc., Ltd/Ltd., etc.
    conn.execute("""
        CREATE OR REPLACE MACRO normalize_operator_name(name) AS
            UPPER(TRIM(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(
                            REGEXP_REPLACE(
                                REGEXP_REPLACE(
                                    COALESCE(name, ''),
                                    '\\s+', ' ', 'g'  -- Collapse multiple spaces
                                ),
                                '\\bCORPORATION\\b', 'CORP', 'gi'
                            ),
                            '\\bCORP\\.?\\b', 'CORP', 'gi'
                        ),
                        '\\bINCORPORATED\\b', 'INC', 'gi'
                    ),
                    '\\bINC\\.?\\b', 'INC', 'gi'
                )
            ))
    """)
    logger.info("Created macro 'normalize_operator_name'")

    # 3. Create operator_groups view that groups by normalized name
    # Uses just the FIRST WORD of well name as the operator name
    # This is the main view used by the API
    conn.execute("""
        CREATE OR REPLACE VIEW operator_groups AS
        WITH ba_code_stats AS (
            -- Get the most common first word for each BA code
            SELECT
                TRIM(licensee) as ba_code,
                UPPER(TRIM(SPLIT_PART(name, ' ', 1))) as operator_name,
                COUNT(*) as well_count
            FROM wells
            WHERE licensee IS NOT NULL AND TRIM(licensee) != ''
                AND name IS NOT NULL AND TRIM(name) != ''
            GROUP BY TRIM(licensee), UPPER(TRIM(SPLIT_PART(name, ' ', 1)))
        ),
        primary_names AS (
            -- Pick the most common name for each BA code
            SELECT
                ba_code,
                FIRST(operator_name ORDER BY well_count DESC) as primary_name,
                SUM(well_count) as total_wells
            FROM ba_code_stats
            WHERE operator_name != '' AND LENGTH(operator_name) > 1
            GROUP BY ba_code
        ),
        with_canonical AS (
            -- Apply manual aliases first, then use primary name
            SELECT
                pn.ba_code,
                pn.primary_name,
                pn.total_wells,
                COALESCE(
                    oa.canonical_name,  -- Use manual override if exists
                    normalize_operator_name(pn.primary_name)  -- Otherwise normalize
                ) as normalized_name
            FROM primary_names pn
            LEFT JOIN operator_aliases oa ON pn.ba_code = oa.ba_code
        ),
        grouped AS (
            -- Group by normalized name, aggregate BA codes and well counts
            SELECT
                normalized_name,
                SUM(total_wells) as total_wells,
                STRING_AGG(DISTINCT ba_code, ',' ORDER BY ba_code) as ba_codes,
                COUNT(DISTINCT ba_code) as ba_code_count,
                FIRST(primary_name ORDER BY total_wells DESC) as display_name
            FROM with_canonical
            WHERE normalized_name IS NOT NULL AND normalized_name != ''
            GROUP BY normalized_name
        )
        SELECT
            normalized_name,
            display_name,
            total_wells,
            ba_codes,
            ba_code_count
        FROM grouped
        ORDER BY total_wells DESC
    """)
    logger.info("Created view 'operator_groups'")


def _create_heavy_oil_views(conn: duckdb.DuckDBPyConnection):
    """Create heavy oil quality classification views (optional)."""
    # This is a stub - extend with your own formation-density lookup
    # to classify wells as heavy/light/medium crude.
    pass


def query_duckdb(db_path: Path, query: str):
    """Execute a query against the DuckDB database."""
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        result = conn.execute(query).fetchdf()
        return result
    finally:
        conn.close()


def get_table_stats(db_path: Path) -> dict:
    """Get row counts for all tables."""
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        tables = conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
        ).fetchall()
        
        stats = {}
        for (table_name,) in tables:
            count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            stats[table_name] = count
        
        return stats
    finally:
        conn.close()
