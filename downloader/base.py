"""
Base downloader that orchestrates the full download pipeline.

Coordinates:
- Registry lookup
- Link discovery via HTML index
- Update detection via manifest
- Raw snapshot storage
"""

import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

from downloader.html_index import HtmlIndexDownloader
from downloader.manifest import ManifestManager
from registry.loader import DatasetConfig, SourceRegistry

logger = logging.getLogger(__name__)


class DatasetDownloader:
    """Orchestrates downloading for a single dataset."""

    def __init__(self,
                 data_dir: Path,
                 config: DatasetConfig):
        self.data_dir = data_dir
        self.config = config
        self.manifest = ManifestManager(data_dir, config.id)
        self.html_downloader = HtmlIndexDownloader()

    def get_raw_dir(self) -> Path:
        """Get the raw storage directory for this dataset."""
        raw_dir = self.data_dir / "raw" / self.config.id
        raw_dir.mkdir(parents=True, exist_ok=True)
        return raw_dir

    def get_snapshot_path(self, url: str, snapshot_date: str) -> Path:
        """Get the path for a snapshot file."""
        # Extract filename from URL
        filename = url.split('/')[-1]
        # Add date prefix for versioning
        date_str = snapshot_date.split('T')[0]
        versioned_name = f"{date_str}_{filename}"
        return self.get_raw_dir() / versioned_name

    def check_for_updates(self) -> tuple[bool, list[str]]:
        """
        Check if dataset has updates without downloading.

        Returns:
            Tuple of (has_updates, discovered_urls)
        """
        if self.config.source_type == 'direct_file':
            if not self.config.url:
                return True, []
            etag, last_modified, _ = self.html_downloader.get_resource_metadata(self.config.url)
            has_changed = self.manifest.has_changed(etag=etag, last_modified=last_modified)
            return has_changed, [self.config.url]

        if self.config.source_type == 'tableau_csv':
            # Tableau CSVs use explicit URL list - check first URL for changes
            if not self.config.download_urls:
                return True, []
            url = self.config.download_urls[0]
            etag, last_modified, _ = self.html_downloader.get_resource_metadata(url)
            has_changed = self.manifest.has_changed(etag=etag, last_modified=last_modified)
            return has_changed, self.config.download_urls

        if self.config.source_type == 'catalog_page':
            # Catalog pages - always download since they're updated frequently
            catalog_url = self.config.catalog_url
            if not catalog_url:
                return True, []
            links = self.html_downloader.discover_links(catalog_url, self.config.link_regex)
            if links:
                etag, last_modified, _ = self.html_downloader.get_resource_metadata(links[0])
                has_changed = self.manifest.has_changed(etag=etag, last_modified=last_modified)
                return has_changed, links
            return True, []

        if self.config.source_type == 'archive_list':
            # Archive lists - always force download since these are backfill operations
            return True, self.config.archive_urls or []

        if self.config.source_type != 'html_index':
            logger.warning(f"Update check not implemented for {self.config.source_type}")
            return True, []

        if not self.config.index_url:
            return True, []

        # Discover current links
        discovered = self.html_downloader.discover_links(
            self.config.index_url,
            self.config.link_regex
        )

        if not discovered:
            logger.warning(f"No links found for {self.config.id}")
            return False, []

        # Get metadata for primary link
        url = discovered[0]
        etag, last_modified, _ = self.html_downloader.get_resource_metadata(url)

        # Check against manifest
        has_changed = self.manifest.has_changed(etag=etag, last_modified=last_modified)

        return has_changed, discovered


    def download(self, force: bool = False) -> list[Path]:
        """
        Download the dataset if updated.

        Args:
            force: If True, download even if unchanged

        Returns:
            List of paths to downloaded file(s), or empty list if unchanged
        """
        logger.info(f"Starting download for {self.config.id}")

        # Check for updates first
        if not force:
            has_updates, discovered = self.check_for_updates()
            if not has_updates:
                logger.info(f"No updates for {self.config.id}, skipping")
                return []
        else:
            discovered = []

        try:
            # Download resources based on source type
            if self.config.source_type == 'direct_file':
                if not self.config.url:
                    raise ValueError(f"Dataset {self.config.id} has no url for direct_file")
                content, etag, last_modified = self.html_downloader.download(self.config.url)
                results = [(self.config.url, content, etag or '', last_modified or '')]

            elif self.config.source_type == 'tableau_csv':
                # Tableau CSV exports use explicit download_urls list
                if not self.config.download_urls:
                    raise ValueError(f"Dataset {self.config.id} has no download_urls for tableau_csv")
                results = []
                for url in self.config.download_urls:
                    content, etag, last_modified = self.html_downloader.download(url)
                    results.append((url, content, etag or '', last_modified or ''))

            elif self.config.source_type == 'catalog_page':
                # Catalog pages need to be scraped for the actual download link
                # For now, try to find links on the catalog page
                catalog_url = self.config.catalog_url
                if not catalog_url:
                    raise ValueError(f"Dataset {self.config.id} has no catalog_url for catalog_page")

                links = self.html_downloader.discover_links(catalog_url, self.config.link_regex)
                if not links:
                    # Try direct download if pattern matches common formats
                    logger.warning(f"No links found on catalog page {catalog_url}, treating as direct download")
                    content, etag, last_modified = self.html_downloader.download(catalog_url)
                    results = [(catalog_url, content, etag or '', last_modified or '')]
                else:
                    results = []
                    for url in (links if self.config.multi_file else [links[0]]):
                        content, etag, last_modified = self.html_downloader.download(url)
                        results.append((url, content, etag or '', last_modified or ''))

            elif self.config.source_type == 'archive_list':
                # Download and extract zip archives from explicit URL list
                import io
                import zipfile

                archive_urls = self.config.archive_urls or []
                if not archive_urls:
                    raise ValueError(f"Dataset {self.config.id} has no archive_urls for archive_list")

                results = []
                for archive_url in archive_urls:
                    try:
                        logger.info(f"Downloading archive: {archive_url}")
                        content, etag, last_modified = self.html_downloader.download(archive_url)

                        # Check if it's a zip file
                        if archive_url.endswith('.zip'):
                            # Extract all txt/csv files from the zip
                            with zipfile.ZipFile(io.BytesIO(content)) as zf:
                                for name in zf.namelist():
                                    if name.endswith(('.txt', '.csv', '.TXT', '.CSV')):
                                        extracted_content = zf.read(name)
                                        # Use the inner filename for tracking
                                        inner_url = f"{archive_url}#{name}"
                                        results.append((inner_url, extracted_content, etag or '', last_modified or ''))
                                        logger.info(f"Extracted: {name} ({len(extracted_content)} bytes)")
                        else:
                            results.append((archive_url, content, etag or '', last_modified or ''))

                    except Exception as e:
                        logger.warning(f"Failed to download archive {archive_url}: {e}")
                        continue

            elif self.config.source_type == 'arcgis_rest':
                # ArcGIS REST API with pagination support
                import json

                import requests

                base_url = self.config.base_url
                if not base_url:
                    raise ValueError(f"Dataset {self.config.id} has no base_url for arcgis_rest")

                query_params = self.config.query_params or {}
                pagination = self.config.pagination or {}
                page_size = pagination.get('page_size', 1000)
                offset_param = pagination.get('offset_param', 'resultOffset')
                count_param = pagination.get('count_param', 'resultRecordCount')

                all_features = []
                offset = 0

                while True:
                    params = {**query_params, offset_param: offset, count_param: page_size}
                    logger.info(f"Fetching {self.config.id} offset={offset}...")

                    resp = requests.get(base_url, params=params, timeout=120, headers={
                        'User-Agent': 'aer-data-pipeline/1.0 (+https://github.com/QV2000/aer-data-pipeline)'
                    })
                    resp.raise_for_status()

                    data = resp.json()
                    features = data.get('features', [])

                    if not features:
                        break

                    all_features.extend(features)
                    logger.info(f"  Got {len(features)} features (total: {len(all_features)})")

                    # Check if we got fewer than page_size (last page)
                    if len(features) < page_size:
                        break

                    offset += page_size

                # Build complete GeoJSON
                geojson = {
                    "type": "FeatureCollection",
                    "features": all_features
                }
                content = json.dumps(geojson).encode('utf-8')
                logger.info(f"Downloaded {len(all_features)} total features for {self.config.id}")
                results = [(base_url, content, '', '')]

            else:
                # Default: html_index type
                results = self.html_downloader.download_for_dataset(self.config)

            # Skip processing if content is unchanged (SHA256 match)
            if results and not force:
                from downloader.manifest import compute_sha256
                primary_hash = compute_sha256(results[0][1])
                _, _, prev_hash = self.manifest.get_latest_signature()
                if prev_hash and primary_hash == prev_hash:
                    logger.info(f"Content unchanged (SHA256 match) for {self.config.id}, skipping")
                    return []

            snapshot_date = datetime.now().isoformat()
            downloaded_paths = []


            for url, content, etag, last_modified in results:
                # Save to raw directory
                snapshot_path = self.get_snapshot_path(url, snapshot_date)
                with open(snapshot_path, 'wb') as f:
                    f.write(content)
                downloaded_paths.append(snapshot_path)
                logger.info(f"Saved: {snapshot_path} ({len(content)} bytes)")

            # Create manifest entry (track first file as primary indicator)
            if results:
                url, content, etag, last_modified = results[0]
                self.manifest.create_entry(
                    source_url=url,
                    discovered_urls=[r[0] for r in results],
                    content=content,
                    etag=etag if etag else None,
                    last_modified=last_modified if last_modified else None,
                )

            return downloaded_paths

        except Exception as e:
            logger.error(f"Download failed for {self.config.id}: {e}")
            self.manifest.create_failed_entry(
                source_url=self.config.canonical_url or "",
                discovered_urls=discovered,
                error_message=str(e)
            )
            raise

    def close(self):
        self.html_downloader.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class PipelineDownloader:
    """Orchestrates downloading for all datasets."""

    def __init__(self, data_dir: Path, registry: Optional[SourceRegistry] = None):
        self.data_dir = data_dir
        self.registry = registry or SourceRegistry()

    def download_dataset(self, dataset_id: str, force: bool = False) -> list[Path]:
        """Download a single dataset."""
        config = self.registry.get(dataset_id)
        with DatasetDownloader(self.data_dir, config) as downloader:
            return downloader.download(force=force)

    def download_all(self, force: bool = False) -> dict[str, list[Path]]:
        """Download all datasets."""
        results = {}
        for config in self.registry:
            try:
                paths = self.download_dataset(config.id, force=force)
                results[config.id] = paths
            except Exception as e:
                logger.error(f"Failed to download {config.id}: {e}")
                results[config.id] = []
        return results

    def download_by_cadence(self, cadence: str, force: bool = False) -> dict[str, list[Path]]:
        """Download datasets matching a cadence (daily, weekly, monthly)."""
        dataset_ids = self.registry.list_by_cadence(cadence)
        results = {}
        for ds_id in dataset_ids:
            try:
                results[ds_id] = self.download_dataset(ds_id, force=force)
            except Exception as e:
                logger.error(f"Failed to download {ds_id}: {e}")
                results[ds_id] = []
        return results

