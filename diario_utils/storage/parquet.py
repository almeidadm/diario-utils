import hashlib
import json
from datetime import datetime
from typing import Any

import duckdb
import pyarrow as pa
from diario_contract.article.content import ContentType
from diario_contract.article.parquet import ARTICLES_SCHEMA
from diario_contract.gazette.edition import GazetteEdition
from diario_contract.gazette.parquet import EDITIONS_SCHEMA

from diario_utils.logging.logging import get_logger
from diario_utils.storage.base import StorageBackend
from diario_utils.storage.local import LocalBackend

logger = get_logger(__name__)


CONTENT_INLINE_THRESHOLD = 2000
CONTENT_DIRNAME = "content"


class MockStorage:
    def __init__(self, **kwargs) -> None:
        pass

    def save_editions(self, editions: Any, **kwargs):
        pass


class ParquetStorage:
    """
    Storage otimizado que delega persistência para backends plugáveis.

    Uso:
        # Local
        storage = ParquetStorage(LocalBackend("data/raw"))

        # MinIO
        storage = ParquetStorage(MinIOBackend(
            endpoint="minio:9000",
            bucket="gazettes",
            access_key="minioadmin",
            secret_key="minioadmin",
            secure=False,
        ))
    """

    def __init__(
        self,
        backend: StorageBackend,
        partition_by: str = "day",  # "day", "month", "year"
        enable_duckdb: bool = True,
        duckdb_path: str | None = None,
    ):
        self.backend = backend
        self.partition_by = partition_by
        self.enable_duckdb = enable_duckdb

        # DuckDB para consultas (opcional)
        self._duck_conn = None
        if enable_duckdb:
            self.duckdb_path = duckdb_path or ":memory:"
            self._duck_conn = duckdb.connect(self.duckdb_path)
            logger.debug(f"DuckDB habilitado: {self.duckdb_path}")

    @property
    def duckdb(self) -> duckdb.DuckDBPyConnection | None:
        """Acesso ao connection DuckDB."""
        return self._duck_conn

    # -----------------------
    # Helpers
    # -----------------------

    def _generate_edition_hash(self, edition: GazetteEdition) -> str:
        content = f"{edition.metadata.edition_id}_{edition.metadata.publication_date}_{len(edition.articles)}"
        return hashlib.md5(content.encode("utf-8")).hexdigest()

    def _write_content_blob(self, raw: str | bytes) -> dict[str, Any]:
        """Persiste conteúdo pesado e retorna metadados."""
        if isinstance(raw, str):
            raw_bytes = raw.encode("utf-8")
        else:
            raw_bytes = raw

        size = len(raw_bytes)
        content_hash = hashlib.sha256(raw_bytes).hexdigest()
        filename = f"{content_hash}.bin"
        path = f"{CONTENT_DIRNAME}/{content_hash[:2]}/{content_hash[2:4]}/{filename}"

        # Escreve se não existir (deduplicação por hash)
        if not self.backend.exists(path):
            self.backend.write_bytes(
                path,
                raw_bytes,
                metadata={
                    "content_hash": content_hash,
                    "content_size": size,
                    "created_at": datetime.now().isoformat(),
                },
            )

        return {
            "content_hash": content_hash,
            "content_path": path,
            "content_size": size,
        }

    def _publication_date_parts(self, publication_date: str) -> dict[str, int]:
        """Extrai year/month/day da data."""
        try:
            d = datetime.strptime(publication_date, "%Y-%m-%d").date()
            return {"year": d.year, "month": d.month, "day": d.day}
        except Exception:
            today = datetime.now().date()
            return {"year": today.year, "month": today.month, "day": today.day}

    # -----------------------
    # API Pública
    # -----------------------

    def save_editions(
        self, editions: list[GazetteEdition], **kwargs: Any
    ) -> dict[str, Any]:
        """
        Persiste edições e artigos no backend.

        Returns:
            Dict com estatísticas da operação.
        """
        if not editions:
            logger.warning("Nenhuma edição para salvar")
            return {"editions": 0, "articles": 0, "relationships": 0}

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        batch_id = kwargs.get("batch_id", f"batch_{timestamp}")

        editions_rows: list[dict[str, Any]] = []
        articles_rows: list[dict[str, Any]] = []
        relationships_rows: list[dict[str, Any]] = []

        # Processa edições e artigos
        for edition in editions:
            meta = edition.metadata
            edition_hash = self._generate_edition_hash(edition)
            pub_parts = self._publication_date_parts(meta.publication_date)

            # Parse publication_date para date32
            pub_date_obj = datetime.strptime(meta.publication_date, "%Y-%m-%d").date()

            editions_rows.append(
                {
                    "municipality": kwargs.get("municipality", ""),
                    "edition_id": str(meta.edition_id),
                    "publication_date": pub_date_obj,
                    "edition_number": int(meta.edition_number),
                    "supplement": bool(meta.supplement),
                    "edition_type_id": int(meta.edition_type_id),
                    "edition_type_name": str(meta.edition_type_name),
                    "pdf_url": str(meta.pdf_url),
                    "total_articles": len(edition.articles),
                    "processed_at": datetime.now(),
                    "edition_hash": edition_hash,
                    "batch_id": batch_id,
                    "year": pub_parts["year"],
                    "month": pub_parts["month"],
                    "day": pub_parts["day"],
                }
            )

            for article in edition.articles:
                raw_content = article.content.raw_content

                # Normaliza conteúdo
                if isinstance(raw_content, bytes):
                    try:
                        raw_text = raw_content.decode("utf-8")
                        raw_bytes = raw_content
                    except Exception:
                        raw_text = None
                        raw_bytes = raw_content
                else:
                    raw_text = str(raw_content) if raw_content is not None else ""
                    raw_bytes = raw_text.encode("utf-8")

                # Decide inline vs blob
                if raw_text is None or len(raw_text) > CONTENT_INLINE_THRESHOLD:
                    content_meta = self._write_content_blob(raw_bytes)
                    inline_text = None
                else:
                    content_meta = {
                        "content_hash": None,
                        "content_path": None,
                        "content_size": len(raw_bytes),
                    }
                    inline_text = raw_text

                hierarchy = getattr(article.metadata, "hierarchy_path", []) or []

                articles_rows.append(
                    {
                        "municipality": kwargs.get("municipality", ""),
                        "article_id": str(article.metadata.article_id),
                        "edition_id": str(article.metadata.edition_id),
                        "edition_hash": edition_hash,
                        "publication_date": pub_date_obj,
                        "title": getattr(article.metadata, "title", "") or "",
                        "hierarchy_path": json.dumps(hierarchy),
                        "identifier": str(
                            getattr(article.metadata, "identifier", None) or ""
                        ),
                        "protocol": str(
                            getattr(article.metadata, "protocol", None) or ""
                        ),
                        "depth": len(hierarchy),
                        "content_type": (
                            article.content.content_type.value
                            if isinstance(article.content.content_type, ContentType)
                            else str(article.content.content_type)
                        ),
                        "content_size": content_meta.get("content_size", 0),
                        "content_hash": content_meta.get("content_hash"),
                        "content_path": content_meta.get("content_path"),
                        "inline_text": inline_text,
                        "processed_at": datetime.now(),
                        "batch_id": batch_id,
                        "year": pub_parts["year"],
                        "month": pub_parts["month"],
                        "day": pub_parts["day"],
                    }
                )

                relationships_rows.append(
                    {
                        "municipality": kwargs.get("municipality", ""),
                        "edition_id": str(meta.edition_id),
                        "article_id": str(article.metadata.article_id),
                        "edition_hash": edition_hash,
                        "publication_date": meta.publication_date,
                        "batch_id": batch_id,
                        "processed_at": datetime.now().isoformat(),
                    }
                )

        # Persiste usando backend
        stats = {
            "municipality": kwargs.get("municipality", ""),
            "editions": 0,
            "articles": 0,
            "relationships": 0,
            "timestamp": timestamp,
            "batch_id": batch_id,
        }

        try:
            # Editions (particionado por year/month)
            if editions_rows:
                table = pa.Table.from_pylist(editions_rows, schema=EDITIONS_SCHEMA)
                path = f"gazettes/batch_{timestamp}.parquet"
                self.backend.write_parquet(path, table)
                stats["editions"] = len(editions_rows)
                logger.info(f"✓ {len(editions_rows)} edições salvas")

            # Articles (particionado por year/month/day)
            if articles_rows:
                table = pa.Table.from_pylist(articles_rows, schema=ARTICLES_SCHEMA)
                path = f"articles/batch_{timestamp}.parquet"
                self.backend.write_parquet(path, table)
                stats["articles"] = len(articles_rows)
                logger.info(f"✓ {len(articles_rows)} artigos salvos")

            # Relationships (sem schema fixo, mais flexível)
            if relationships_rows:
                table = pa.Table.from_pylist(relationships_rows)
                path = f"relationships/batch_{timestamp}.parquet"
                self.backend.write_parquet(path, table)
                stats["relationships"] = len(relationships_rows)
                logger.info(f"✓ {len(relationships_rows)} relações salvas")

            return stats

        except Exception as exc:
            logger.error(f"Erro ao salvar: {exc}", exc_info=True)
            raise

    def query_articles(
        self,
        municipality: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        content_types: list[str] | None = None,
        limit: int | None = None,
    ) -> pa.Table:
        """
        Consulta artigos com filtros (requer DuckDB habilitado).

        Example:
            results = storage.query_articles(
                start_date="2024-01-01",
                end_date="2024-12-31",
                content_types=["text"],
                limit=100
            )
        """
        if not self.enable_duckdb:
            raise RuntimeError("DuckDB não habilitado")

        # Lista arquivos parquet de artigos
        files = self.backend.list_files("articles", suffix=".parquet")

        if not files:
            logger.warning("Nenhum arquivo de artigos encontrado")
            return pa.table({})

        # Monta query SQL
        # Nota: DuckDB pode ler parquet direto de S3 com configuração apropriada
        where_clauses = []
        if municipality:
            where_clauses.append(f"municipality = '{municipality}'")
        if start_date:
            where_clauses.append(f"publication_date >= '{start_date}'")
        if end_date:
            where_clauses.append(f"publication_date <= '{end_date}'")
        if content_types:
            types_str = "', '".join(content_types)
            where_clauses.append(f"content_type IN ('{types_str}')")

        where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"
        limit_sql = f"LIMIT {limit}" if limit else ""

        # Para backends locais, usa paths diretos
        # Para S3/MinIO, usa extensão httpfs do DuckDB
        if isinstance(self.backend, LocalBackend):
            paths = [self.backend.get_uri(f) for f in files]
        else:
            # TODO: Configurar credenciais S3 no DuckDB
            paths = [self.backend.get_uri(f) for f in files]

        paths_str = "', '".join(paths)
        query = f"""
            SELECT * FROM read_parquet(['{paths_str}'])
            WHERE {where_sql}
            {limit_sql}
        """

        result = self._duck_conn.execute(query).fetch_arrow_table()
        return result

    def get_content(self, content_path: str) -> bytes:
        """Recupera conteúdo de blob externo."""
        return self.backend.read_bytes(content_path)

    def get_stats(self) -> dict[str, Any]:
        """Retorna estatísticas do storage."""
        stats = {
            "backend": type(self.backend).__name__,
            "editions_files": len(self.backend.list_files("gazettes", ".parquet")),
            "articles_files": len(self.backend.list_files("articles", ".parquet")),
            "content_files": len(self.backend.list_files(CONTENT_DIRNAME)),
        }
        return stats
