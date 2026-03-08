from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(slots=True)
class StorageConfig:
    """Configuration for storage operations."""

    base_path: Path | str = Path("data")
    duckdb_path: str = ":memory:"
    compression: str = "zstd"
    compression_level: int = 3
    threads: int = 4

    def resolve_base(self) -> Path:
        """Expand and resolve ``base_path`` to an absolute Path."""
        return Path(self.base_path).expanduser().resolve()
