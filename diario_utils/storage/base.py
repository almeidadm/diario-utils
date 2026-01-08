from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

import pyarrow as pa

from diario_utils.logging.logging import get_logger

logger = get_logger(__name__)


# ============================================================================
# Configurações e Constantes
# ============================================================================


PARQUET_COMPRESSION = "zstd"
PARQUET_COMPRESSION_LEVEL = 9


# ============================================================================
# Interfaces e Classes Base
# ============================================================================


class StorageBackend(ABC):
    """Interface abstrata para backends de armazenamento."""

    @abstractmethod
    def write_bytes(
        self, path: str, data: bytes, metadata: dict[str, Any] | None = None
    ) -> str:
        """Escreve bytes e retorna o path/URI final."""
        pass

    @abstractmethod
    def read_bytes(self, path: str) -> bytes:
        """Lê bytes de um path/URI."""
        pass

    @abstractmethod
    def exists(self, path: str) -> bool:
        """Verifica se path existe."""
        pass

    @abstractmethod
    def write_parquet(self, path: str, table: pa.Table, **kwargs: Any) -> str:
        """Escreve tabela Parquet."""
        pass

    @abstractmethod
    def read_parquet(self, path: str, columns: list[str] | None = None) -> pa.Table:
        """Lê tabela Parquet."""
        pass

    @abstractmethod
    def list_files(self, prefix: str, suffix: str | None = None) -> list[str]:
        """Lista arquivos com prefixo opcional."""
        pass

    @abstractmethod
    def get_uri(self, path: str) -> str:
        """Retorna URI completo para o path."""
        pass
