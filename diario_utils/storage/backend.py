import json
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from diario_utils.logging.logging import get_logger
from diario_utils.storage.base import (
    PARQUET_COMPRESSION,
    PARQUET_COMPRESSION_LEVEL,
    StorageBackend,
)

logger = get_logger(__name__)


class LocalBackend(StorageBackend):
    """Backend para armazenamento em filesystem local."""

    def __init__(self, base_path: Path | str = "data/raw"):
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)
        logger.info(f"LocalBackend inicializado em {self.base_path}")

    def write_bytes(
        self, path: str, data: bytes, metadata: dict[str, Any] | None = None
    ) -> str:
        full_path = self.base_path / path
        full_path.parent.mkdir(parents=True, exist_ok=True)
        full_path.write_bytes(data)

        # Opcionalmente salva metadados em sidecar
        if metadata:
            meta_path = full_path.with_suffix(full_path.suffix + ".meta.json")
            meta_path.write_text(json.dumps(metadata, indent=2))

        return str(full_path.relative_to(self.base_path))

    def read_bytes(self, path: str) -> bytes:
        full_path = self.base_path / path
        return full_path.read_bytes()

    def exists(self, path: str) -> bool:
        return (self.base_path / path).exists()

    def write_parquet(self, path: str, table: pa.Table, **kwargs: Any) -> str:
        full_path = self.base_path / path
        full_path.parent.mkdir(parents=True, exist_ok=True)

        pq.write_table(
            table,
            str(full_path),
            compression=kwargs.get("compression", PARQUET_COMPRESSION),
            compression_level=kwargs.get(
                "compression_level", PARQUET_COMPRESSION_LEVEL
            ),
            use_dictionary=kwargs.get("use_dictionary", True),
            write_statistics=kwargs.get("write_statistics", True),
        )
        return str(full_path.relative_to(self.base_path))

    def read_parquet(self, path: str, columns: list[str] | None = None) -> pa.Table:
        full_path = self.base_path / path
        return pq.read_table(str(full_path), columns=columns)

    def list_files(self, prefix: str, suffix: str | None = None) -> list[str]:
        prefix_path = self.base_path / prefix
        if not prefix_path.exists():
            return []

        files = []
        for p in prefix_path.rglob("*"):
            if p.is_file():
                if suffix is None or p.suffix == suffix:
                    files.append(str(p.relative_to(self.base_path)))
        return sorted(files)

    def get_uri(self, path: str) -> str:
        return f"file://{self.base_path / path}"
