from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


@dataclass(slots=True)
class Manifest:
    """Metadata summary stored alongside each partition table."""

    path: Path
    row_count: int
    schema_version: int
    sha256: str
    updated_at: str

    @classmethod
    def from_file(cls, parquet_path: Path, schema_version: int) -> "Manifest":
        """Create a manifest from a parquet file, computing hash and row count."""
        sha256 = _sha256_file(parquet_path)
        # late import to avoid heavy deps at module import time
        try:
            import polars as pl
        except Exception:  # pragma: no cover - fallback
            row_count = 0
        else:
            row_count = (
                pl.scan_parquet(parquet_path, hive_partitioning=False)
                .select(pl.len())
                .collect()
                .item(0, 0)
            )
        return cls(
            path=parquet_path.parent / "manifest.json",
            row_count=row_count,
            schema_version=schema_version,
            sha256=sha256,
            updated_at=datetime.now(timezone.utc).isoformat(),
        )

    @classmethod
    def load(cls, path: Path) -> "Manifest":
        """Load a manifest from disk."""
        data = json.loads(path.read_text())
        return cls(
            path=path,
            row_count=data["row_count"],
            schema_version=data["schema_version"],
            sha256=data["sha256"],
            updated_at=data["updated_at"],
        )

    def save(self) -> None:
        """Persist manifest JSON to disk."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(
            json.dumps(
                {
                    "row_count": self.row_count,
                    "schema_version": self.schema_version,
                    "sha256": self.sha256,
                    "updated_at": self.updated_at,
                },
                indent=2,
            )
        )


def _sha256_file(path: Path) -> str:
    """Compute sha256 for a file in streaming chunks."""
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()
