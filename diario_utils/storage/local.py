from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import polars as pl

from diario_utils.storage.base import Storage


class LocalStorage(Storage):
    """Filesystem rooted at ``base_path``."""

    def __init__(self, base_path: Path | str = "data") -> None:
        """Initialize the backend and ensure base directory exists."""
        self.base_path = Path(base_path).expanduser()
        self.base_path.mkdir(parents=True, exist_ok=True)

    # ----------------------------- bytes ---------------------------------
    def write_bytes(
        self, path: str, data: bytes, metadata: dict[str, Any] | None = None
    ) -> str:
        """Write bytes to disk and optional sidecar metadata JSON."""
        full_path = self.base_path / path
        full_path.parent.mkdir(parents=True, exist_ok=True)
        full_path.write_bytes(data)
        if metadata:
            meta_path = full_path.with_suffix(full_path.suffix + ".meta.json")
            meta_path.write_text(json.dumps(metadata, indent=2))
        return str(full_path.relative_to(self.base_path))

    def read_bytes(self, path: str) -> bytes:
        """Read bytes from a relative path inside the base directory."""
        return (self.base_path / path).read_bytes()

    def exists(self, path: str) -> bool:
        """Check whether a given relative path exists."""
        return (self.base_path / path).exists()

    # ----------------------------- parquet -------------------------------
    def write_parquet(self, path: str, df: pl.DataFrame, **kwargs: Any) -> str:
        """Persist DataFrame to Parquet with compression defaults."""
        full_path = self.base_path / path
        full_path.parent.mkdir(parents=True, exist_ok=True)
        df.write_parquet(
            full_path,
            compression=kwargs.get("compression", "zstd"),
            compression_level=kwargs.get("compression_level", 3),
            statistics=kwargs.get("statistics", True),
        )
        return str(full_path.relative_to(self.base_path))

    def read_parquet(self, path: str, columns: list[str] | None = None) -> pl.DataFrame:
        """Read Parquet file from disk, optionally selecting columns."""
        full_path = self.base_path / path
        return pl.read_parquet(full_path, columns=columns, hive_partitioning=False)

    def list_files(self, prefix: str, suffix: str | None = None) -> list[str]:
        """Recursively list files under a prefix, filtered by optional suffix."""
        prefix_path = self.base_path / prefix
        if not prefix_path.exists():
            return []
        files: list[str] = []
        for p in prefix_path.rglob("*"):
            if p.is_file():
                if suffix is None or p.name.endswith(suffix):
                    files.append(str(p.relative_to(self.base_path)))
        return sorted(files)

    def get_uri(self, path: str) -> str:
        """Return a file:// URI for a stored path."""
        return f"file://{self.base_path / path}"
