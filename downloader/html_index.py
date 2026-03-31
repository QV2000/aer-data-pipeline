"""
HTML Index downloader for AER pages.

Scrapes HTML pages for download links matching a pattern, 
rather than relying on hardcoded URLs.
"""

import re
import logging
from typing import Optional
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup

from registry.loader import DatasetConfig

logger = logging.getLogger(__name__)


class HtmlIndexDownloader:
    """Downloads resources by scraping HTML index pages for links."""
    
    def __init__(self, timeout: int = 60):
        self.timeout = timeout
        self._client: Optional[httpx.Client] = None
    
    @property
    def client(self) -> httpx.Client:
        if self._client is None:
            self._client = httpx.Client(
                timeout=self.timeout,
                follow_redirects=True,
                headers={
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
                }
            )
        return self._client
    
    def close(self) -> None:
        if self._client:
            self._client.close()
            self._client = None
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
    
    def discover_links(self, 
                       index_url: str, 
                       link_pattern: Optional[re.Pattern] = None) -> list[str]:
        """
        Fetch an HTML page and discover download links.
        
        Args:
            index_url: URL of the HTML page to scrape
            link_pattern: Compiled regex to match link hrefs
            
        Returns:
            List of absolute URLs matching the pattern
        """
        logger.info(f"Fetching index page: {index_url}")
        response = self.client.get(index_url)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'lxml')
        discovered = []
        
        for link in soup.find_all('a', href=True):
            href = link['href']
            
            # Skip empty/anchor-only links
            if not href or href.startswith('#'):
                continue
            
            # Make absolute URL
            absolute_url = urljoin(index_url, href)
            
            # Apply pattern filter if provided
            if link_pattern:
                if link_pattern.search(absolute_url):
                    discovered.append(absolute_url)
            else:
                # If no pattern, include all links to files (not pages)
                if any(absolute_url.endswith(ext) for ext in 
                       ['.txt', '.csv', '.zip', '.xlsx', '.pdf', '.shp']):
                    discovered.append(absolute_url)
        
        logger.info(f"Discovered {len(discovered)} links matching pattern")
        return discovered
    
    def get_resource_metadata(self, url: str) -> tuple[Optional[str], Optional[str], Optional[int]]:
        """
        Get resource metadata via HEAD request.
        
        Returns:
            Tuple of (etag, last_modified, content_length)
        """
        try:
            response = self.client.head(url)
            response.raise_for_status()
            return (
                response.headers.get('ETag'),
                response.headers.get('Last-Modified'),
                int(response.headers.get('Content-Length', 0)) or None
            )
        except Exception as e:
            logger.warning(f"HEAD request failed for {url}: {e}")
            return None, None, None
    
    def download(self, url: str) -> tuple[bytes, Optional[str], Optional[str]]:
        """
        Download a resource.
        
        Returns:
            Tuple of (content_bytes, etag, last_modified)
        """
        logger.info(f"Downloading: {url}")
        response = self.client.get(url)
        response.raise_for_status()
        
        return (
            response.content,
            response.headers.get('ETag'),
            response.headers.get('Last-Modified'),
        )
    
    def download_for_dataset(self, config: DatasetConfig) -> list[tuple[str, bytes, str, str]]:
        """
        Download resources for a dataset configuration.
        
        Args:
            config: Dataset configuration from registry
            
        Returns:
            List of (url, content, etag, last_modified) tuples
        """
        if not config.index_url:
            raise ValueError(f"Dataset {config.id} has no index_url")
        
        # Discover links
        links = self.discover_links(config.index_url, config.link_regex)
        
        if not links:
            raise ValueError(f"No links found matching pattern '{config.link_pattern}' on {config.index_url}")
        
        # For single-file datasets, take the first (or most recent)
        # For multi-file datasets (like ST49 daily), take all
        if not config.multi_file:
            links = [links[0]]
        
        results = []
        for url in links:
            content, etag, last_modified = self.download(url)
            results.append((url, content, etag or '', last_modified or ''))
        
        return results
