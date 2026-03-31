# AER Data Pipeline

Open-source pipeline for ingesting Alberta and Saskatchewan oil & gas public data into a local DuckDB database.

**No API keys required.** All data comes from publicly available government sources (AER, Petrinex, Saskatchewan GeoHub).

## What You Get

Run the pipeline and get a single DuckDB database with:

### Alberta

| Table | Source | Description | Cadence |
|-------|--------|-------------|---------|
| `wells` / `well_attributes` | AER ST37 | ~820K wells with UWI, licensee, status, fluid, depth, coordinates | Monthly |
| `production` | Petrinex AB | ~3M+ rows of oil/gas/water volumes per well (last 5 years) | Monthly |
| `facilities` | AER ST102 | ~120K batteries, gas plants, meter stations (active + inactive) | Monthly |
| `facilities` (shapefile) | AER ST102 | Geocoded facility locations | Monthly |
| `well_licences` | AER ST1 | Daily well licences issued (rolling 7-day + 10yr archive) | Daily |
| `spud_activity` | AER ST49 | Daily spud reports (rolling 7-day + 10yr archive) | Daily |
| `status_changes` | AER ST2 | Weekly well status changes (drilling, completed, suspended, abandoned) | Weekly |
| `drilling_activity` | AER ST2 | Weekly drilling activity report | Weekly |
| `pipelines` | AER Spatial Data | ~500K pipeline segments with substance, status, operator | Monthly |
| `confidential_wells` | AER | Daily list of wells with confidential status | Daily |
| `abandoned_wells` | AER Spatial Data | Geocoded abandoned/decommissioned wells | Monthly |
| `scheme_approvals` | AER | Injection and disposal scheme approval locations | Monthly |
| `well_facility_link` | Petrinex AB | Maps each well to its linked facility (battery) | Monthly |
| `st3_provincial_stats` | AER ST3 | Provincial production stats by product (oil/gas/NGL/butane/ethane/propane, 2010-present) | Monthly |

### Saskatchewan

| Table | Source | Description | Cadence |
|-------|--------|-------------|---------|
| `production` | Petrinex SK | SK well production volumes (oil/gas/water) | Monthly |
| `sk_pipelines` | SK GeoHub ArcGIS | ~5K pipeline segments with licence, owner, substance | Monthly |
| `sk_well_infrastructure` | Petrinex SK | Well infrastructure data | Daily |
| `sk_well_licence` | Petrinex SK | Well licence data | Daily |
| `sk_facility_licence` | Petrinex SK | Facility licence data | Daily |
| `sk_facility_infrastructure` | Petrinex SK | Facility infrastructure data | Daily |
| `sk_well_facility_link` | Petrinex SK | Well-to-facility linkage | Daily |
| `sk_business_associate` | Petrinex SK | Operator/company lookup | Daily |
| `sk_daily_drilling` | SK Gov | Daily drilling activity (spuds/rig releases) | Daily |
| `sk_well_bulletin` | SK Gov | Daily well bulletin (licences + status changes) | Daily |
| `sk_ref_*` | Petrinex SK | Reference tables: BA identifiers, facility IDs, activity codes, pool codes, product codes | Daily |

All data is public and sourced directly from government websites. No scraping of private or paywalled sources.

## Quick Start

```bash
# Clone
git clone https://github.com/QV2000/aer-data-pipeline.git
cd aer-data-pipeline

# Install
pip install -r requirements.txt

# Ingest everything (takes ~10 min first run)
python cli.py ingest --all

# Build DuckDB
python cli.py build-duckdb

# Query it
python -c "
import duckdb
conn = duckdb.connect('aer_data.duckdb', read_only=True)
print(conn.execute('SELECT COUNT(*) FROM wells').fetchone())
print(conn.execute('SELECT COUNT(*) FROM production').fetchone())
conn.close()
"
```

## Usage

### Ingest specific datasets

```bash
# Daily datasets (licences, spuds)
python cli.py ingest --cadence daily

# Weekly datasets (status changes, drilling)
python cli.py ingest --cadence weekly

# Monthly datasets (wells, facilities, production, pipelines)
python cli.py ingest --cadence monthly

# Single dataset
python cli.py ingest --dataset st37_wells_txt
python cli.py ingest --dataset petrinex_production
```

### Petrinex production (date-range based)

```bash
# Last 3 months (default)
python cli.py ingest-petrinex

# Last 5 years backfill
python cli.py ingest-petrinex --months 60

# Force re-download
python cli.py ingest-petrinex --months 12 --force
```

### Saskatchewan data

```bash
# SK production
python cli.py ingest --dataset sk_petrinex_production

# SK wells, facilities, pipelines
python cli.py ingest --dataset sk_petrinex_well_infrastructure
python cli.py ingest --dataset sk_petrinex_facility_infrastructure
python cli.py ingest --dataset sk_pipelines

# SK daily drilling & well bulletin
python cli.py ingest --dataset sk_daily_drilling
python cli.py ingest --dataset sk_well_bulletin
```

### Historical archives (10+ years)

```bash
# Backfill ST1 licences + ST49 spuds (2015-present)
python cli.py backfill-archives --dataset all
```

### Check status

```bash
python cli.py status
```

## Architecture

```
Raw (downloads)  -->  Bronze (parsed parquet)  -->  Silver (cleaned)  -->  DuckDB
```

- **Raw**: Original files from government websites, date-stamped snapshots
- **Bronze**: Parsed into Parquet with source metadata
- **Silver**: Cleaned, deduplicated, standardized column names
- **DuckDB**: Views pointing to silver Parquet files (no data duplication)

The pipeline uses **manifest-based change detection** - it tracks ETags and Last-Modified headers so it only downloads when data has actually changed.

## Data Sources

All URLs are defined in [`config/sources.yaml`](config/sources.yaml). Every source is a public government endpoint:

| Source | URL | Auth |
|--------|-----|------|
| AER ST37 (Wells) | aer.ca/statistical-reports/st37 | None |
| AER ST102 (Facilities) | aer.ca/statistical-reports/st102 | None |
| AER ST1 (Licences) | aer.ca/statistical-reports/st1 | None |
| AER ST49 (Spuds) | aer.ca/statistical-reports/st49 | None |
| AER ST2 (Status Changes) | aer.ca/statistical-reports/st2 | None |
| AER Pipelines | aer.ca/spatial-data | None |
| Petrinex AB Production | petrinex.gov.ab.ca/publicdata | None |
| Petrinex SK Production | petrinex.gov.ab.ca/publicdata | None |
| SK Pipelines | gis.saskatchewan.ca/arcgis | None |
| SK Drilling/Bulletin | drillingactivity.saskatchewan.ca | None |

## Adding New Datasets

1. Add an entry to `config/sources.yaml` with the URL pattern and parser
2. Write a parser in `parsers/` if the format isn't already supported
3. Add a silver transform in `transforms/silver.py` if custom cleaning is needed
4. Run `python cli.py ingest --dataset your_new_dataset`

Supported source types: `html_index`, `direct_file`, `static_url`, `tableau_csv`, `archive_list`, `arcgis_rest`

## Query Examples

```sql
-- Top 10 Alberta crude oil producers (latest month)
SELECT
    SUBSTRING(TRIM(w.licensee), 1, 4) as operator,
    SUM(p.oil_prod_vol) as oil_m3
FROM production p
JOIN wells w ON p.uwi = w.uwi
WHERE p.productionmonth = (SELECT MAX(productionmonth) FROM production WHERE province = 'AB')
  AND w.fluid LIKE 'CR%'
GROUP BY operator
ORDER BY oil_m3 DESC
LIMIT 10;

-- Wells licensed in the last 30 days
SELECT * FROM well_licences
WHERE licence_date >= CURRENT_DATE - INTERVAL '30 days'
ORDER BY licence_date DESC;

-- Production decline by operator
WITH monthly AS (
    SELECT
        SUBSTRING(TRIM(w.licensee), 1, 4) as op,
        p.productionmonth,
        SUM(p.oil_prod_vol) as oil
    FROM production p
    JOIN wells w ON p.uwi = w.uwi
    WHERE p.province = 'AB'
    GROUP BY op, p.productionmonth
)
SELECT op,
    MAX(CASE WHEN productionmonth = '2026-01' THEN oil END) as jan,
    MAX(CASE WHEN productionmonth = '2025-01' THEN oil END) as jan_prev,
    ROUND((MAX(CASE WHEN productionmonth = '2026-01' THEN oil END) -
           MAX(CASE WHEN productionmonth = '2025-01' THEN oil END)) /
           NULLIF(MAX(CASE WHEN productionmonth = '2025-01' THEN oil END), 0) * 100, 1) as yoy_pct
FROM monthly
GROUP BY op
HAVING jan > 10000
ORDER BY yoy_pct DESC;
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DATA_DIR` | `./data` | Where raw/bronze/silver files are stored |
| `DUCKDB_MEMORY_LIMIT` | `2GB` | DuckDB memory limit |

## License

Apache 2.0 - see [LICENSE](LICENSE).

## Contributing

PRs welcome. If you add a new public data source, please include the government URL and update the sources table above.
