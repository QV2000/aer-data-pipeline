"""
Registry loader and validator for AER data sources.

Loads sources.yaml and provides typed access to dataset configurations.
"""

import re
from pathlib import Path
from dataclasses import dataclass, field
from typing import Optional

import yaml


@dataclass
class GeoConfig:
    """Configuration for spatial data processing."""
    output_crs: str = "EPSG:4326"
    include_centroids: bool = True
    timeout_seconds: int = 120


@dataclass
class SchemaConfig:
    """Schema validation configuration."""
    required_columns: list[str] = field(default_factory=list)
    primary_keys: list[str] = field(default_factory=list)
    geo_fields: list[str] = field(default_factory=list)


@dataclass
class ValidationConfig:
    """Validation rules for a dataset."""
    min_rows: int = 0
    max_row_delta_pct: float = 50.0
    check_monotonic_dates: bool = False


@dataclass 
class AmendmentConfig:
    """Configuration for handling amendments/restatements."""
    enabled: bool = False
    upsert_keys: list[str] = field(default_factory=list)


@dataclass
class DatasetConfig:
    """Configuration for a single dataset."""
    id: str
    name: str
    description: str = ""

    # Province/jurisdiction (AB or SK)
    province: str = "AB"

    # Source configuration
    source_type: str = "html_index"  # html_index | direct_file | catalog_page | open_data_api | tableau_csv | arcgis_rest
    index_url: Optional[str] = None
    catalog_url: Optional[str] = None
    dataset_url: Optional[str] = None
    url: Optional[str] = None
    base_url: Optional[str] = None  # For arcgis_rest source type
    link_pattern: Optional[str] = None
    download_urls: list[str] = field(default_factory=list)  # For explicit URL lists (e.g., Tableau CSVs)
    archive_urls: list[str] = field(default_factory=list)  # For archive_list source type (zip archives)
    query_params: dict = field(default_factory=dict)  # For arcgis_rest source type
    pagination: dict = field(default_factory=dict)  # For arcgis_rest pagination config

    
    # Timing
    expected_cadence: str = "monthly"  # daily | weekly | monthly | as_needed
    timezone: str = "America/Edmonton"
    
    # Download
    download_format: str = "txt"  # txt | csv | xlsx | zip_txt | zip_shp
    multi_file: bool = False
    
    # Parser
    parser: str = ""
    
    # Flags
    public_source: bool = True
    redistributable: bool = False
    
    # Nested configs
    schema: SchemaConfig = field(default_factory=SchemaConfig)
    validation: ValidationConfig = field(default_factory=ValidationConfig)
    geo_config: Optional[GeoConfig] = None
    amendment_handling: Optional[AmendmentConfig] = None
    
    @property
    def canonical_url(self) -> Optional[str]:
        """Get the canonical URL for this dataset."""
        return self.index_url or self.catalog_url or self.dataset_url
    
    @property
    def link_regex(self) -> Optional[re.Pattern]:
        """Compiled regex for link pattern matching."""
        if self.link_pattern:
            return re.compile(self.link_pattern)
        return None


class SourceRegistry:
    """Registry of all configured data sources."""
    
    def __init__(self, config_path: Optional[Path] = None):
        if config_path is None:
            config_path = Path(__file__).parent.parent / "config" / "sources.yaml"
        
        self.config_path = config_path
        self._datasets: dict[str, DatasetConfig] = {}
        self._defaults: dict = {}
        self._load()
    
    def _load(self) -> None:
        """Load and parse the sources.yaml file."""
        with open(self.config_path, 'r', encoding='utf-8') as f:
            raw = yaml.safe_load(f)
        
        self._defaults = raw.get('defaults', {})
        
        for dataset_id, dataset_raw in raw.get('datasets', {}).items():
            self._datasets[dataset_id] = self._parse_dataset(dataset_id, dataset_raw)
    
    def _parse_dataset(self, dataset_id: str, raw: dict) -> DatasetConfig:
        """Parse a raw dataset dictionary into a DatasetConfig."""
        # Apply defaults
        for key, value in self._defaults.items():
            if key not in raw:
                raw[key] = value
        
        # Parse nested configs
        schema_raw = raw.pop('schema', {})
        schema = SchemaConfig(
            required_columns=schema_raw.get('required_columns', []),
            primary_keys=schema_raw.get('primary_keys', []),
            geo_fields=schema_raw.get('geo_fields', []),
        )
        
        validation_raw = raw.pop('validation', {})
        validation = ValidationConfig(
            min_rows=validation_raw.get('min_rows', 0),
            max_row_delta_pct=validation_raw.get('max_row_delta_pct', 50.0),
            check_monotonic_dates=validation_raw.get('check_monotonic_dates', False),
        )
        
        geo_raw = raw.pop('geo_config', None)
        geo_config = None
        if geo_raw:
            geo_config = GeoConfig(
                output_crs=geo_raw.get('output_crs', 'EPSG:4326'),
                include_centroids=geo_raw.get('include_centroids', True),
                timeout_seconds=geo_raw.get('timeout_seconds', 120),
            )
        
        amendment_raw = raw.pop('amendment_handling', None)
        amendment_config = None
        if amendment_raw:
            amendment_config = AmendmentConfig(
                enabled=amendment_raw.get('enabled', False),
                upsert_keys=amendment_raw.get('upsert_keys', []),
            )
        
        return DatasetConfig(
            id=dataset_id,
            name=raw.get('name', dataset_id),
            description=raw.get('description', ''),
            province=raw.get('province', 'AB'),
            source_type=raw.get('source_type', 'html_index'),
            index_url=raw.get('index_url'),
            catalog_url=raw.get('catalog_url'),
            dataset_url=raw.get('dataset_url'),
            url=raw.get('url'),
            base_url=raw.get('base_url'),
            link_pattern=raw.get('link_pattern'),
            download_urls=raw.get('download_urls', []),
            archive_urls=raw.get('archive_urls', []),
            query_params=raw.get('query_params', {}),
            pagination=raw.get('pagination', {}),
            expected_cadence=raw.get('expected_cadence', 'monthly'),
            timezone=raw.get('timezone', 'America/Edmonton'),
            download_format=raw.get('download_format', 'txt'),
            multi_file=raw.get('multi_file', False),
            parser=raw.get('parser', ''),
            public_source=raw.get('public_source', True),
            redistributable=raw.get('redistributable', False),
            schema=schema,
            validation=validation,
            geo_config=geo_config,
            amendment_handling=amendment_config,
        )
    
    def get(self, dataset_id: str) -> DatasetConfig:
        """Get a dataset configuration by ID."""
        if dataset_id not in self._datasets:
            raise KeyError(f"Unknown dataset: {dataset_id}")
        return self._datasets[dataset_id]
    
    def list_all(self) -> list[str]:
        """List all dataset IDs."""
        return list(self._datasets.keys())
    
    def list_by_cadence(self, cadence: str) -> list[str]:
        """List datasets by cadence (daily, weekly, monthly)."""
        return [
            ds_id for ds_id, ds in self._datasets.items()
            if ds.expected_cadence == cadence
        ]
    
    def list_spatial(self) -> list[str]:
        """List datasets that have spatial/geo data."""
        return [
            ds_id for ds_id, ds in self._datasets.items()
            if ds.geo_config is not None or 'shp' in ds.download_format
        ]
    
    def __iter__(self):
        return iter(self._datasets.values())
    
    def __len__(self):
        return len(self._datasets)


# Convenience function
def load_registry(config_path: Optional[Path] = None) -> SourceRegistry:
    """Load the source registry."""
    return SourceRegistry(config_path)
