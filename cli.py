#!/usr/bin/env python3
"""
AER Data Pipeline CLI

Commands:
    ingest --all              Ingest all datasets
    ingest --dataset DATASET  Ingest a specific dataset
    validate --latest         Validate latest snapshots
    build-duckdb              Build/refresh DuckDB
    status                    Show dataset status
"""

import sys
import logging
import gc
from pathlib import Path
from datetime import datetime
import pandas as pd
from dateutil.relativedelta import relativedelta
import requests
import io
import zipfile

import click

# Add project to path
sys.path.insert(0, str(Path(__file__).parent))

from registry.loader import load_registry
from downloader.base import PipelineDownloader, DatasetDownloader
from parsers.txt_parser import parse_st102_facility
from parsers.shapefile_parser import parse_shapefile
from transforms.bronze import transform_to_bronze, write_bronze
from transforms.silver import build_all_silver_tables
from validation.checks import validate_dataframe, write_validation_report

# Lazy import for duckdb (may not be installed)
def _import_duckdb():
    from build_duckdb import build_duckdb, get_table_stats
    return build_duckdb, get_table_stats

# Configure logging
import os

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

logger = logging.getLogger(__name__)


def get_data_dir() -> Path:
    """Get the data directory. Override with DATA_DIR environment variable."""
    data_dir = Path(os.environ.get('DATA_DIR', str(Path(__file__).parent / "data")))
    data_dir.mkdir(parents=True, exist_ok=True)
    logger.info(f"Using data directory: {data_dir}")
    return data_dir


@click.group()
def cli():
    """AER Data Pipeline - Alberta Oil & Gas Public Data Ingestion"""
    pass


@cli.command()
@click.option('--all', 'ingest_all', is_flag=True, help='Ingest all datasets')
@click.option('--dataset', '-d', help='Specific dataset ID to ingest')
@click.option('--cadence', '-c', help='Filter by update cadence (daily, weekly, monthly)')
@click.option('--force', '-f', is_flag=True, help='Force download even if unchanged')
@click.option(
    '--petrinex-months',
    type=int,
    default=3,
    show_default=True,
    help="For petrinex_production only: number of months to ingest (going backwards from last month). Use 60 for ~5 years backfill.",
)
def ingest(ingest_all: bool, dataset: str, cadence: str, force: bool, petrinex_months: int):
    """Download and process datasets."""
    data_dir = get_data_dir()
    registry = load_registry()
    
    targets = []
    if dataset:
        targets = [dataset]
    elif cadence:
        click.echo(f"Ingesting {cadence} datasets...")
        targets = registry.list_by_cadence(cadence)
    elif ingest_all:
        click.echo("Ingesting all datasets...")
        targets = registry.list_all()
    else:
        click.echo("Specify --all, --dataset ID, or --cadence")
        click.echo("\nAvailable datasets:")
        for ds_id in registry.list_all():
            config = registry.get(ds_id)
            click.echo(f"  {ds_id} ({config.expected_cadence}): {config.name}")
        return

    for ds_id in targets:
        try:
            click.echo(f"\n[INFO] Processing {ds_id}...")
            result = ingest_single_dataset(
                ds_id,
                data_dir,
                registry,
                force,
                petrinex_months=petrinex_months,
            )
            if result:
                click.echo(f"  [OK] {ds_id}")
            else:
                click.echo(f"  [SKIP] {ds_id} (no updates)")
        except Exception as e:
            click.echo(f"  [ERROR] {ds_id}: {e}")
        finally:
            # Force garbage collection between datasets to free memory
            gc.collect()


@cli.command('ingest-petrinex')
@click.option('--months', default=3, help='Number of months to ingest (going backwards)')
@click.option('--force', is_flag=True, help='Force download even if already exists')
def ingest_petrinex(months, force):
    """Ingest multiple months of Petrinex production data."""
    data_dir = get_data_dir()
    registry = load_registry()
    process_petrinex_ingestion(data_dir, registry, months, force)


def process_petrinex_ingestion(data_dir: Path, registry, months: int = 3, force: bool = False, dataset_id: str = 'petrinex_production') -> bool:
    """Core logic for Petrinex ingestion (AB or SK)."""
    config = registry.get(dataset_id)
    province = getattr(config, 'province', 'AB')
    
    # We will use our own download logic for Petrinex because it is date-based
    # Try fetching starting from 1 month ago (e.g. in Jan, try Dec and Nov)
    current_date = datetime.now().replace(day=1) - relativedelta(months=1)
    
    downloaded_files = []
    
    for i in range(months):
        target_date = current_date - relativedelta(months=i)
        date_str = target_date.strftime('%Y-%m')
        url = config.url.format(date=date_str)
        
        # Log to click if running via CLI, else logger
        click.echo(f"Ingesting Petrinex {province}: {date_str}...")
        
        try:
            # If bronze already exists for this month and we're not forcing, skip work.
            # This makes long backfills resumable and prevents reprocessing 5y repeatedly.
            bronze_dir = data_dir / "bronze" / config.id
            bronze_dir.mkdir(parents=True, exist_ok=True)
            expected_bronze_path = bronze_dir / f"{date_str}.parquet"
            if expected_bronze_path.exists() and not force:
                click.echo(f"  [SKIP] Bronze already exists: {expected_bronze_path.name}")
                continue

            # Check if already exists in raw
            raw_dir = data_dir / "raw" / config.id
            raw_dir.mkdir(parents=True, exist_ok=True)
            snapshot_path = raw_dir / f"{date_str}_petrinex_vol.zip"
            
            if not snapshot_path.exists() or force:
                r = requests.get(url)
                if r.status_code == 200:
                    with open(snapshot_path, 'wb') as f:
                        f.write(r.content)
                else:
                    click.echo(f"  [FAILED] Failed to download {date_str} (Status: {r.status_code})")
                    continue

            
            # Load parser
            if config.parser:
                import importlib
                module_path, func_name = config.parser.rsplit('.', 1)
                module = importlib.import_module(module_path)
                parser_func = getattr(module, func_name)
                df = parser_func(snapshot_path)
            else:
                from parsers.txt_parser import parse_txt_default
                df = parse_txt_default(snapshot_path)
            
            # Bronze transform
            from transforms.bronze import transform_to_bronze, write_bronze
            bronze_df = transform_to_bronze(df, config, snapshot_date=date_str)
            bronze_path = write_bronze(bronze_df, config, data_dir, snapshot_date=date_str)
            
            click.echo(f"  [OK] Saved to bronze: {bronze_path.name}")
            downloaded_files.append(bronze_path)
            
        except Exception as e:
            click.echo(f"  [FAILED] Error ingesting {date_str}: {e}")
            logger.error(f"Petrinex error for {date_str}", exc_info=True)

    if downloaded_files:
        click.echo(f"\nSuccessfully ingested {len(downloaded_files)} months of Petrinex data.")
        return True
    else:
        click.echo("\nNo data ingested.")
        return False


def ingest_single_dataset(
    dataset_id: str,
    data_dir: Path,
    registry,
    force: bool,
    petrinex_months: int = 3,
) -> bool:
    """Ingest a single dataset end-to-end (can handle multiple files)."""
    # Special handling for Petrinex (date-based dynamic URL)
    if dataset_id == 'petrinex_production':
        months = petrinex_months if petrinex_months and petrinex_months > 0 else 3
        return process_petrinex_ingestion(data_dir, registry, months=months, force=force)

    # SK Petrinex production uses the same pattern (swap AB→SK in URL)
    if dataset_id == 'sk_petrinex_production':
        months = petrinex_months if petrinex_months and petrinex_months > 0 else 3
        return process_petrinex_ingestion(data_dir, registry, months=months, force=force, dataset_id='sk_petrinex_production')

    config = registry.get(dataset_id)
    
    # Download
    with DatasetDownloader(data_dir, config) as downloader:
        raw_paths = downloader.download(force=force)
    
    if not raw_paths:
        return False  # No updates
    
    for raw_path in raw_paths:
        click.echo(f"  Parsing: {raw_path.name}")
        
        # Parse using configured parser or format-based defaults
        if config.parser:
            import importlib
            try:
                module_path, func_name = config.parser.rsplit('.', 1)
                module = importlib.import_module(module_path)
                parser_func = getattr(module, func_name)
                df = parser_func(raw_path)
            except Exception as e:
                logger.error(f"Failed to load/run parser {config.parser} for {raw_path.name}: {e}")
                continue
        elif config.download_format in ('txt', 'zip_txt'):
            if 'st102' in dataset_id:
                df = parse_st102_facility(raw_path)
            else:
                from parsers.txt_parser import parse_txt_default
                df = parse_txt_default(raw_path)
        elif config.download_format == 'zip_shp':
            df = parse_shapefile(raw_path)
        else:
            logger.warning(f"No parser or format-based fallback for {dataset_id}")
            continue
            
        # Validate
        report = validate_dataframe(df, config)
        if not report.passed:
            logger.warning(f"Validation failed for {raw_path.name}")
            # write_validation_report(report, data_dir)
        
        # Transform to bronze
        bronze_df = transform_to_bronze(df, config)
        
        # Free parsed dataframe memory
        del df
        
        # If multi_file, use filename as suffix (without dates)
        suffix = ""
        if config.multi_file:
            # Strip date if present from downloader: "2025-12-23_filename.zip"
            base_name = raw_path.name
            if "_" in base_name:
                base_name = base_name.split("_", 1)[1]
            suffix = f"_{base_name.split('.')[0]}"
            
        write_bronze(bronze_df, config, data_dir, suffix=suffix)
        
        # Free bronze dataframe and force garbage collection
        del bronze_df
        gc.collect()
    
    return True


@cli.command('validate')
@click.option('--latest', is_flag=True, help='Validate latest snapshots')
@click.option('--dataset', '-d', help='Specific dataset to validate')
def validate_cmd(latest: bool, dataset: str):
    """Validate data quality."""
    data_dir = get_data_dir()
    registry = load_registry()
    
    if dataset:
        config = registry.get(dataset)
        bronze_path = data_dir / "bronze" / dataset
        files = sorted(bronze_path.glob("*.parquet")) if bronze_path.exists() else []
        
        if not files:
            click.echo(f"No bronze data for {dataset}")
            return
        
        import pandas as pd
        df = pd.read_parquet(files[-1])
        report = validate_dataframe(df, config)
        
        click.echo(f"\nValidation: {dataset}")
        click.echo(f"  Rows: {report.row_count:,}")
        click.echo(f"  Status: {'PASSED' if report.passed else 'FAILED'}")
        for r in report.results:
            status = '[OK]' if r.passed else '[ERROR]'
            click.echo(f"    {status} {r.check_name}: {r.message}")

            
    elif latest:
        click.echo("Validating all latest snapshots...")
        for ds_id in registry.list_all():
            config = registry.get(ds_id)
            bronze_path = data_dir / "bronze" / ds_id
            files = sorted(bronze_path.glob("*.parquet")) if bronze_path.exists() else []
            
            if not files:
                click.echo(f"  [SKIP] {ds_id}: no data")
                continue
            
            import pandas as pd
            df = pd.read_parquet(files[-1])
            report = validate_dataframe(df, config)
            
            status = '[OK]' if report.passed else '[ERROR]'
            click.echo(f"  {status} {ds_id}: {report.row_count:,} rows")



@cli.command('build-duckdb')
def build_duckdb_cmd():
    """Build or refresh the DuckDB database."""
    build_duckdb, get_table_stats = _import_duckdb()
    data_dir = get_data_dir()

    click.echo("Building silver tables...")
    silver_results = build_all_silver_tables(data_dir)

    for table_name, path in silver_results.items():
        click.echo(f"  [OK] {table_name}: {path}")


    click.echo("\nBuilding DuckDB...")
    db_path = build_duckdb(data_dir)

    click.echo(f"\nDuckDB: {db_path}")

    # Build pipeline connectivity scores for crude wells
    click.echo("\nBuilding pipeline connectivity scores...")
    try:
        from pipeline_scoring import build_pipeline_scores
        build_pipeline_scores(db_path)
        click.echo("  [OK] Pipeline scores built")
    except Exception as e:
        click.echo(f"  [WARN] Pipeline scoring failed: {e}")

    # Show stats
    stats = get_table_stats(db_path)
    click.echo("\nTable statistics:")
    for table, count in stats.items():
        click.echo(f"  {table}: {count:,} rows")


@cli.command()
def status():
    """Show pipeline status."""
    data_dir = get_data_dir()
    registry = load_registry()
    
    click.echo("AER Data Pipeline Status\n")
    click.echo(f"Data directory: {data_dir}")
    click.echo(f"Datasets configured: {len(registry)}")
    
    click.echo("\nDatasets:")
    for config in registry:
        # Check for data
        bronze_path = data_dir / "bronze" / config.id
        has_data = bronze_path.exists() and list(bronze_path.glob("*.parquet"))
        
        status_icon = '[OK]' if has_data else '[SKIP]'
        click.echo(f"  {status_icon} {config.id}")

        click.echo(f"      Name: {config.name}")
        click.echo(f"      Cadence: {config.expected_cadence}")
        click.echo(f"      Format: {config.download_format}")
        
        if has_data:
            files = sorted(bronze_path.glob("*.parquet"))
            latest = files[-1].name
            click.echo(f"      Latest: {latest}")
    
    # DuckDB status
    db_path = data_dir.parent / "aer_data.duckdb"
    if db_path.exists():
        try:
            _, get_table_stats = _import_duckdb()
            stats = get_table_stats(db_path)
            click.echo(f"\nDuckDB: {db_path}")
            for table, count in stats.items():
                click.echo(f"  {table}: {count:,} rows")
        except ImportError:
            click.echo(f"\nDuckDB: {db_path} (install duckdb to see stats)")
    else:
        click.echo(f"\nDuckDB: not built yet")


@cli.command()
@click.option('--dataset', '-d', type=click.Choice(['st1', 'st49', 'st3', 'all']), default='all', 
              help='Which archive to backfill')
def backfill_archives(dataset: str):
    """Backfill historical archives (10+ years).
    
    Downloads and processes historical archives:
    - ST1: Well licences (2015-2025)
    - ST49: Well spuds (2015-2025)  
    - ST3: Provincial production stats by product (2010-2024)
    """
    data_dir = get_data_dir()
    registry = load_registry()
    
    archives_to_process = []
    
    if dataset in ['st1', 'all']:
        try:
            config = registry.get('st1_well_licences_archive')
            archives_to_process.append(('st1_well_licences_archive', config))
            logger.info(f"Found ST1 archive config with {len(config.archive_urls)} URLs")
        except KeyError:
            logger.warning("st1_well_licences_archive not found in registry")
    
    if dataset in ['st49', 'all']:
        try:
            config = registry.get('st49_spud_archive')
            archives_to_process.append(('st49_spud_archive', config))
            logger.info(f"Found ST49 archive config with {len(config.archive_urls)} URLs")
        except KeyError:
            logger.warning("st49_spud_archive not found in registry")
    
    if dataset in ['st3', 'all']:
        try:
            config = registry.get('st3_provincial_stats_archive')
            archives_to_process.append(('st3_provincial_stats_archive', config))
            logger.info(f"Found ST3 archive config with {len(config.archive_urls)} URLs")
        except KeyError:
            logger.warning("st3_provincial_stats_archive not found in registry")
    
    if not archives_to_process:
        click.echo("No archives to process")
        return
    
    click.echo(f"Backfilling {len(archives_to_process)} archive dataset(s)...")
    click.echo("This may take several minutes - downloading 10 years of data.\n")
    
    for dataset_id, config in archives_to_process:
        click.echo(f"\nProcessing {dataset_id}...")
        click.echo(f"  {len(config.archive_urls)} archive URLs to download")
        
        # Process each archive URL one at a time to avoid memory issues
        total_rows = 0
        for i, url in enumerate(config.archive_urls):
            try:
                click.echo(f"  [{i+1}/{len(config.archive_urls)}] Downloading {url.split('/')[-1]}...")
                
                # Download the file with browser-like headers to avoid 403
                import tempfile
                raw_dir = data_dir / "raw" / dataset_id
                raw_dir.mkdir(parents=True, exist_ok=True)
                
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.5',
                    'Referer': 'https://www.aer.ca/',
                }
                
                resp = requests.get(url, headers=headers, timeout=120)
                if resp.status_code != 200:
                    click.echo(f"    Skipped (HTTP {resp.status_code})")
                    continue
                
                # Save to temp file
                filename = url.split('/')[-1]
                temp_path = raw_dir / filename
                temp_path.write_bytes(resp.content)
                
                # Parse
                if config.parser:
                    parser_module, parser_func = config.parser.rsplit('.', 1)
                    import importlib
                    module = importlib.import_module(parser_module)
                    parser = getattr(module, parser_func)
                    
                    try:
                        df = parser(temp_path)
                        
                        if df is not None and not df.empty:
                            # Extract date from filename for snapshot_date
                            import re
                            date_match = re.search(r'(\d{4}(?:-\d{2})?)', filename)
                            snapshot_date = date_match.group(1) if date_match else filename.replace('.zip', '')
                            
                            bronze_df = transform_to_bronze(df, config, snapshot_date)
                            output_path = write_bronze(bronze_df, config, data_dir, snapshot_date)
                            total_rows += len(df)
                            click.echo(f"    Saved {len(df):,} rows")
                            
                            del df, bronze_df
                            gc.collect()
                    except Exception as e:
                        click.echo(f"    Parse error: {e}")
                
                # Clean up temp file
                try:
                    temp_path.unlink()
                except:
                    pass
                    
            except Exception as e:
                click.echo(f"    Error: {e}")
        
        click.echo(f"  Total: {total_rows:,} rows for {dataset_id}")
    
    click.echo("\nBackfill complete! Run 'build-duckdb' to update the database.")


if __name__ == '__main__':
    cli()

