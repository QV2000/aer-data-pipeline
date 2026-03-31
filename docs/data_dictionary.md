# Data Dictionary

All volumes follow Petrinex conventions and are reported in metric units.

## Unit Reference

| Measurement | Unit | Abbreviation | To convert to Imperial |
|------------|------|--------------|----------------------|
| Oil volume | cubic metres | m3 | multiply by 6.29287 for barrels |
| Gas volume | thousand cubic metres | e3m3 | multiply by 35.3147 for MCF |
| Water volume | cubic metres | m3 | multiply by 6.29287 for barrels |
| Well depth | metres | m | multiply by 3.28084 for feet |
| Coordinates | NAD83 / WGS84 | EPSG:4326 | latitude/longitude in degrees |

## Tables

### wells / well_attributes

Master well dimension table from AER ST37 (monthly).

| Column | Type | Description |
|--------|------|-------------|
| uwi | VARCHAR | Unique Well Identifier (format: `00/01-01-003-08W4/2`) |
| uwi_id | VARCHAR | Numeric UWI key for joins (LOC-DESC format) |
| well_name | VARCHAR | Well name (typically starts with operator name) |
| field_code | VARCHAR | AER field code (joins to field_lookup table) |
| pool_code | VARCHAR | AER pool code (joins to field_pool_lookup table) |
| licence_no | VARCHAR | Well licence number |
| licence_status | VARCHAR | Licence status (e.g., RecCertified, Issued) |
| licensee_code | VARCHAR | Business Associate code for the licensee (e.g., A5D40) |
| well_stat_code | VARCHAR | Current well status code |
| well_stat_date | VARCHAR | Date of current status (YYYYMMDD) |
| fluid | VARCHAR | Primary fluid: CR-OIL (crude), GAS, CBM, WATER, etc. |
| mode | VARCHAR | Well mode: FLW (flowing), PMP (pumping), ABD (abandoned), etc. |
| type | VARCHAR | Well type (e.g., OIL, GAS, INJ) |
| structure | VARCHAR | Well structure (e.g., VERTICAL, HORIZONTAL, DEVIATED) |
| province | VARCHAR | Province: AB or SK |
| fin_drl_date | VARCHAR | Finished drilling date (YYYYMMDD) |
| well_total_depth | VARCHAR | Total depth in metres |

### production

Monthly well production volumes from Petrinex (AB and SK combined).

| Column | Type | Description |
|--------|------|-------------|
| productionmonth | VARCHAR | Production period (YYYY-MM) |
| uwi | VARCHAR | Well UWI (formatted to match wells table) |
| oil_prod_vol | DOUBLE | Oil production volume in cubic metres (m3) |
| gas_prod_vol | DOUBLE | Gas production volume in thousand cubic metres (e3m3) |
| water_prod_vol | DOUBLE | Water production volume in cubic metres (m3) |
| province | VARCHAR | Province: AB or SK |

### facilities

Facility dimension table from AER ST102 (active + inactive combined).

| Column | Type | Description |
|--------|------|-------------|
| facility_id | VARCHAR | Unique facility identifier (e.g., ABBT0001234) |
| facility_name | VARCHAR | Facility name |
| operator_name | VARCHAR | Current operator |
| facility_type | VARCHAR | Facility type description |
| is_active | BOOLEAN | Whether the facility is currently active |
| centroid_lat | DOUBLE | Latitude (WGS84) from shapefile join |
| centroid_lon | DOUBLE | Longitude (WGS84) from shapefile join |
| facility_class | VARCHAR | Classified type: oil_battery, gas_plant, compressor, etc. |
| facility_group | VARCHAR | Group: Battery, Plant, Gathering/Meter, etc. |
| facility_commodity | VARCHAR | Primary commodity: Crude, Gas, or Other |
| province | VARCHAR | Province: AB or SK |

### well_licences

Daily well licences issued from AER ST1.

| Column | Type | Description |
|--------|------|-------------|
| licence_number | VARCHAR | 7-digit licence number |
| uwi | VARCHAR | Well UWI |
| well_name | VARCHAR | Well name |
| licensee | VARCHAR | Licensee company name |
| issue_date | DATE | Date the licence was issued |
| province | VARCHAR | Province: AB |

### spud_activity

Daily spud reports from AER ST49.

| Column | Type | Description |
|--------|------|-------------|
| uwi | VARCHAR | Well UWI |
| spud_date | DATE | Date the well was spudded |
| licence_number | VARCHAR | Licence number |
| well_name | VARCHAR | Well name |
| drilling_contractor | VARCHAR | Drilling contractor name |
| province | VARCHAR | Province: AB |

### status_changes

Weekly well status changes from AER ST2.

| Column | Type | Description |
|--------|------|-------------|
| uwi | VARCHAR | Well UWI |
| new_status | VARCHAR | New status description |
| new_status_code | VARCHAR | New status code |
| old_status | VARCHAR | Previous status description |
| event_date | DATE | Date of the status change |
| event_type | VARCHAR | Always 'status_change' |
| province | VARCHAR | Province: AB |

### sk_pipelines

Saskatchewan pipeline segments from GeoHub ArcGIS service.

| Column | Type | Description |
|--------|------|-------------|
| licence_no | VARCHAR | Pipeline licence number (e.g., PL-00000001) |
| licensee | VARCHAR | Pipeline owner/operator name |
| substance | VARCHAR | Substance transported (Crude Oil, Gas, etc.) |
| status | VARCHAR | Segment status (Active, Removed, etc.) |
| from_lat | DOUBLE | Start point latitude |
| from_lon | DOUBLE | Start point longitude |
| to_lat | DOUBLE | End point latitude |
| to_lon | DOUBLE | End point longitude |
| province | VARCHAR | Always SK |

## Pre-built Views

| View | Description |
|------|-------------|
| v_latest_production | Latest month's production per well |
| v_well_summary | One row per well: status, cumulative production, first/last dates |
| v_operator_scorecard | Per-operator summary: well counts by type and status |
| v_monthly_provincial | Monthly provincial rollup of production volumes |
| v_new_activity | Combined recent licences, spuds, and status changes |
| operator_groups | Normalized operator names with BA code grouping |
| facilities_by_operator | Facility counts per operator (active/inactive) |

## Known Quirks

- **UWI formats differ** between AER (display format: `00/01-01-003-08W4/2`) and Petrinex (compact: `ABWI100010100308W402`). The pipeline normalizes Petrinex UWIs to AER format during silver transforms.
- **Petrinex data is typically 2 months behind.** January data usually becomes available in late March.
- **Confidential wells** are excluded from production data until their confidentiality period expires (typically 1 year from rig release).
- **Some operators report under multiple licensee codes** due to acquisitions. The `operator_groups` view attempts to normalize these.
- **Saskatchewan uses CWI** (Canadian Well Identifier, format SK1234567) as its primary key, while Alberta uses UWI (DLS-based). Both are preserved in the data.
