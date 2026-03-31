"""
Manifest handling for snapshot tracking and update detection.

Each download creates a manifest entry with:
- snapshot_date: When the download occurred
- source_url: The URL(s) that were downloaded
- discovered_urls: All URLs found on the index page
- etag: HTTP ETag header if available
- last_modified: HTTP Last-Modified header if available
- sha256: Hash of the downloaded content
- byte_size: Size in bytes
- row_count: Number of rows after parsing (optional)
- status: success | failed | quarantined
"""

import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional


@dataclass
class ManifestEntry:
    """A single snapshot manifest entry."""
    snapshot_date: str
    source_url: str
    discovered_urls: list[str]
    etag: Optional[str] = None
    last_modified: Optional[str] = None
    sha256: str = ""
    byte_size: int = 0
    row_count: Optional[int] = None
    status: str = "success"  # success | failed | quarantined
    error_message: Optional[str] = None

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> 'ManifestEntry':
        return cls(**data)


class ManifestManager:
    """Manages manifest files for a dataset."""

    def __init__(self, data_dir: Path, dataset_id: str):
        self.data_dir = data_dir
        self.dataset_id = dataset_id
        self.manifest_path = data_dir / "raw" / dataset_id / "manifest.json"
        self._entries: list[ManifestEntry] = []
        self._load()

    def _load(self) -> None:
        """Load existing manifest if present."""
        if self.manifest_path.exists():
            with open(self.manifest_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                self._entries = [ManifestEntry.from_dict(e) for e in data.get('entries', [])]

    def _save(self) -> None:
        """Save manifest to disk atomically."""
        import os
        import tempfile
        self.manifest_path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_path = tempfile.mkstemp(
            dir=str(self.manifest_path.parent), suffix='.tmp'
        )
        try:
            with os.fdopen(fd, 'w', encoding='utf-8') as f:
                json.dump({
                    'dataset_id': self.dataset_id,
                    'last_updated': datetime.now().isoformat(),
                    'entries': [e.to_dict() for e in self._entries]
                }, f, indent=2)
            os.replace(tmp_path, str(self.manifest_path))
        except Exception:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
            raise

    def get_latest(self) -> Optional[ManifestEntry]:
        """Get the most recent successful manifest entry."""
        successful = [e for e in self._entries if e.status == 'success']
        if successful:
            return successful[-1]
        return None

    def get_latest_signature(self) -> tuple[Optional[str], Optional[str], Optional[str]]:
        """Get (etag, last_modified, sha256) from latest successful entry."""
        latest = self.get_latest()
        if latest:
            return latest.etag, latest.last_modified, latest.sha256
        return None, None, None

    def has_changed(self,
                    etag: Optional[str] = None,
                    last_modified: Optional[str] = None,
                    sha256: Optional[str] = None) -> bool:
        """Check if the resource has changed since last download."""
        prev_etag, prev_lm, prev_sha = self.get_latest_signature()

        # If we have no previous entry, it's "changed"
        if prev_etag is None and prev_lm is None and prev_sha is None:
            return True

        # ETag comparison (strongest signal)
        if etag and prev_etag:
            return etag != prev_etag

        # Last-Modified comparison
        if last_modified and prev_lm:
            return last_modified != prev_lm

        # SHA256 comparison (definitive but requires download)
        if sha256 and prev_sha:
            return sha256 != prev_sha

        # If we can't compare, assume changed
        return True

    def add_entry(self, entry: ManifestEntry) -> None:
        """Add a new manifest entry."""
        self._entries.append(entry)
        self._save()

    def create_entry(self,
                     source_url: str,
                     discovered_urls: list[str],
                     content: bytes,
                     etag: Optional[str] = None,
                     last_modified: Optional[str] = None,
                     row_count: Optional[int] = None) -> ManifestEntry:
        """Create a new successful manifest entry."""
        entry = ManifestEntry(
            snapshot_date=datetime.now().isoformat(),
            source_url=source_url,
            discovered_urls=discovered_urls,
            etag=etag,
            last_modified=last_modified,
            sha256=hashlib.sha256(content).hexdigest(),
            byte_size=len(content),
            row_count=row_count,
            status="success",
        )
        self.add_entry(entry)
        return entry

    def create_failed_entry(self,
                            source_url: str,
                            discovered_urls: list[str],
                            error_message: str) -> ManifestEntry:
        """Create a failed manifest entry."""
        entry = ManifestEntry(
            snapshot_date=datetime.now().isoformat(),
            source_url=source_url,
            discovered_urls=discovered_urls,
            status="failed",
            error_message=error_message,
        )
        self.add_entry(entry)
        return entry

    def quarantine_latest(self, reason: str) -> None:
        """Mark the latest entry as quarantined."""
        if self._entries:
            self._entries[-1].status = "quarantined"
            self._entries[-1].error_message = reason
            self._save()

    def list_entries(self, limit: int = 10) -> list[ManifestEntry]:
        """List recent manifest entries."""
        return self._entries[-limit:]


def compute_sha256(content: bytes) -> str:
    """Compute SHA256 hash of content."""
    return hashlib.sha256(content).hexdigest()


def compute_file_sha256(file_path: Path) -> str:
    """Compute SHA256 hash of a file."""
    sha256 = hashlib.sha256()
    with open(file_path, 'rb') as f:
        for chunk in iter(lambda: f.read(8192), b''):
            sha256.update(chunk)
    return sha256.hexdigest()
