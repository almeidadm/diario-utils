# diario_utils/storage/local.py
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from diario_utils.storage.backend import StorageBackend

PARQUET_COMPRESSION = "zstd"
PARQUET_COMPRESSION_LEVEL = 9


class LocalBackend(StorageBackend):
    def __init__(self, base_path: str | Path):
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)

    def write_bytes(
        self, path: str, data: bytes, *, metadata: dict | None = None
    ) -> str:
        full = self.base_path / path
        full.parent.mkdir(parents=True, exist_ok=True)
        full.write_bytes(data)
        if metadata:
            full.with_suffix(full.suffix + ".meta.json").write_text(
                json.dumps(metadata, indent=2)
            )
        return path

    def read_bytes(self, path: str) -> bytes:
        return (self.base_path / path).read_bytes()

    def write_parquet(self, path: str, table: pa.Table, **kwargs) -> str:
        full = self.base_path / path
        full.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(
            table,
            full,
            compression=kwargs.get("compression", PARQUET_COMPRESSION),
            compression_level=kwargs.get(
                "compression_level", PARQUET_COMPRESSION_LEVEL
            ),
        )
        return path

    def read_parquet(self, path: str, columns: list[str] | None = None) -> pa.Table:
        return pq.read_table(self.base_path / path, columns=columns)

    def list_files(self, prefix: str, suffix: str | None = None) -> list[str]:
        root = self.base_path / prefix
        if not root.exists():
            return []
        out = []
        for p in root.rglob("*"):
            if p.is_file() and (suffix is None or p.suffix == suffix):
                out.append(str(p.relative_to(self.base_path)))
        return sorted(out)

    def exists(self, path: str) -> bool:
        return (self.base_path / path).exists()

    def get_uri(self, path: str) -> str:
        return f"file://{self.base_path / path}"
