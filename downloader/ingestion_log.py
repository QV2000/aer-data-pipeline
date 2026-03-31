"""
Ingestion log for crash recovery and audit trail.

Records every successful ingestion with a content hash so the pipeline
can detect already-processed files and skip redundant work.
"""

import json
from datetime import datetime
from pathlib import Path


class IngestionLog:
    """Append-only JSONL log of ingestion events."""

    def __init__(self, data_dir: Path):
        self.log_path = data_dir / "ingestion_log.jsonl"

    def record(self, dataset_id: str, source_url: str, file_hash: str,
               row_count: int, status: str, output_path: str = None):
        """Append an ingestion event to the log."""
        entry = {
            "timestamp": datetime.now().isoformat(),
            "dataset_id": dataset_id,
            "source_url": source_url,
            "file_hash": file_hash,
            "row_count": row_count,
            "status": status,
            "output_path": str(output_path) if output_path else None,
        }
        with open(self.log_path, "a") as f:
            f.write(json.dumps(entry) + "\n")

    def was_processed(self, file_hash: str) -> bool:
        """Check if a file with this hash was already successfully processed."""
        if not self.log_path.exists():
            return False
        with open(self.log_path) as f:
            for line in f:
                try:
                    entry = json.loads(line)
                    if entry.get("file_hash") == file_hash and entry.get("status") == "success":
                        return True
                except json.JSONDecodeError:
                    continue
        return False
