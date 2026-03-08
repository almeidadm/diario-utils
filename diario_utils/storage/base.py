from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

import polars as pl


class Storage(ABC):
    """Abstract interface for storage backends."""

    @abstractmethod
    def write_bytes(
        self, path: str, data: bytes, metadata: dict[str, Any] | None = None
    ) -> str:
        """Persist arbitrary bytes and optional metadata; returns relative path."""
        ...

    @abstractmethod
    def read_bytes(self, path: str) -> bytes:
        """Read raw bytes from a stored path."""
        ...

    @abstractmethod
    def exists(self, path: str) -> bool:
        """Return True when the given path exists in the backend."""
        ...

    @abstractmethod
    def write_parquet(self, path: str, df: pl.DataFrame, **kwargs: Any) -> str:
        """Write a DataFrame to Parquet and return the relative path."""
        ...

    @abstractmethod
    def read_parquet(
        self, path: str, columns: list[str] | None = None
    ) -> pl.DataFrame:
        """Read a Parquet file and optionally project selected columns."""
        ...

    @abstractmethod
    def list_files(self, prefix: str, suffix: str | None = None) -> list[str]:
        """List files under a prefix, optionally filtered by suffix."""
        ...

    @abstractmethod
    def get_uri(self, path: str) -> str:
        """Return a URI suitable for external references (e.g., file:// or s3://)."""
        ...
