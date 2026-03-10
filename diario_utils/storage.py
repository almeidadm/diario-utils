from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Sequence

import polars as pl

from diario_contract.article.article import Article
from diario_contract.gazette.edition import GazetteEdition
from diario_contract.enums.content_type import ContentType

from diario_utils.logging.structlog_config import get_logger


logger = get_logger(component="storage")


@dataclass(slots=True)
class Manifest:
    """Lightweight manifest persisted next to Parquet partitions."""

    row_count: int
    schema_version: int = 1
    data_sha256: str | None = None

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(asdict(self), ensure_ascii=False, indent=2))

    @classmethod
    def load(cls, path: Path) -> "Manifest":
        payload = json.loads(path.read_text())
        return cls(**payload)


@dataclass(slots=True)
class StorageConfig:
    """Runtime configuration for the storage client."""
    base_path: Path
    duckdb_path: str | Path | None = None  # preserved for API compatibility, unused
    schema_version: int = 1
    compression: str = "zstd"

    def __init__(
        self,
        base_path: str | Path,
        duckdb_path: str | Path | None = None,
        schema_version: int = 1,
        compression: str = "zstd",
    ) -> None:
        self.base_path = Path(base_path)
        self.duckdb_path = duckdb_path
        self.schema_version = schema_version
        self.compression = compression


@dataclass(slots=True)
class GazetteWriteResult:
    """Paths written for a single publication month of gazettes/articles."""
    city_id: str
    publication_month: str
    gazette_path: Path
    articles_path: Path | None


class StorageClient:
    """Filesystem-backed medallion storage using Parquet + Polars."""
    def __init__(self, config: StorageConfig) -> None:
        self.config = config
        self.base = config.base_path

    # ---------------- Bronze -----------------
    def append_gazettes(
        self,
        editions: Iterable[GazetteEdition],
        city_id: str,
        crawler_tag: str | None = None,
        ingestion_run_id: str | None = None,
    ) -> list[GazetteWriteResult]:
        """Append GazetteEdition objects into Bronze partitions.

        Writes `gazettes.parquet` (metadata) and `articles.parquet` (raw content split
        into text/bytes) under `bronze/{city}/{YYYYMM}` with dedup by edition/article.
        """
        results: list[GazetteWriteResult] = []
        by_month: dict[str, list[GazetteEdition]] = {}
        for ed in editions:
            month = f"{ed.publication_date.year:04d}{ed.publication_date.month:02d}"
            by_month.setdefault(month, []).append(ed)

        for month, month_editions in by_month.items():
            part_dir = self.base / "bronze" / city_id / month
            part_dir.mkdir(parents=True, exist_ok=True)
            gaz_path = part_dir / "gazettes.parquet"
            art_path = part_dir / "articles.parquet"

            new_gaz = self._build_gazette_df(month_editions, city_id, month, crawler_tag, ingestion_run_id)
            existing_gaz = self._load_if_exists(gaz_path)
            gaz_df = self._dedup(existing_gaz, new_gaz, subset=["edition_id"])
            self._write_table(gaz_df, gaz_path)
            Manifest(row_count=gaz_df.height, schema_version=self.config.schema_version).save(part_dir / "manifest.json")

            new_art = self._build_article_df(month_editions, city_id, month, crawler_tag, ingestion_run_id)
            existing_art = self._load_if_exists(art_path)
            art_df = self._dedup(existing_art, new_art, subset=["article_id"])
            self._write_table(art_df, art_path)
            Manifest(row_count=art_df.height, schema_version=self.config.schema_version).save(part_dir / "manifest.json")

            results.append(GazetteWriteResult(city_id=city_id, publication_month=month, gazette_path=gaz_path, articles_path=art_path))

        return results

    def load_gazettes(self, city_id: str | None = None, publication_month: str | None = None) -> pl.DataFrame:
        """Load gazettes metadata filtered by city/month (if provided)."""
        paths = self._collect_paths(layer="bronze", city_id=city_id, publication_month=publication_month, filename="gazettes.parquet")
        return self._concat(paths)

    def load_articles(self, city_id: str | None = None, publication_month: str | None = None) -> pl.DataFrame:
        """Load articles with raw content columns from Bronze."""
        paths = self._collect_paths(layer="bronze", city_id=city_id, publication_month=publication_month, filename="articles.parquet")
        return self._concat(paths)

    # ---------------- Silver / Gold -----------------
    def append_chunks(
        self,
        df: pl.DataFrame,
        base_keys: dict,
        layer: str = "silver",
        parser_tag: str | None = None,
    ) -> list[Path]:
        """Append chunk/act rows into Silver or Gold with dedup by `chunk_id`."""
        enriched = df.clone()
        for key, value in base_keys.items():
            if key not in enriched.columns:
                enriched = enriched.with_columns(pl.lit(value).alias(key))
        if "publication_date" in enriched.columns:
            enriched = enriched.with_columns(pl.col("publication_date").cast(pl.Date, strict=False))
        if "publication_month" not in enriched.columns and "publication_date" in enriched.columns:
            enriched = enriched.with_columns(pl.col("publication_date").dt.strftime("%Y%m").alias("publication_month"))
        if parser_tag and "parser_tag" not in enriched.columns:
            enriched = enriched.with_columns(pl.lit(parser_tag).alias("parser_tag"))

        written: list[Path] = []
        for (city, month), group in enriched.group_by(["city_id", "publication_month"], maintain_order=True):
            part_dir = self.base / layer / str(city) / str(month)
            part_dir.mkdir(parents=True, exist_ok=True)
            path = part_dir / "chunks.parquet"
            existing = self._load_if_exists(path)
            combined = self._dedup(existing, group, subset=["chunk_id"])
            self._write_table(combined, path)
            Manifest(row_count=combined.height, schema_version=self.config.schema_version).save(part_dir / "manifest.json")
            written.append(path)
        return written

    def load_chunks(self, layer: str = "silver", city_id: str | None = None, publication_month: str | None = None) -> pl.DataFrame:
        """Load chunks from given layer with optional city/month filters."""
        paths = self._collect_paths(layer=layer, city_id=city_id, publication_month=publication_month, filename="chunks.parquet")
        return self._concat(paths)

    def list_needing_review(self) -> pl.DataFrame:
        """Return Silver chunks flagged with `needs_review`."""
        df = self.load_chunks(layer="silver")
        if df.is_empty():
            return df
        return df.filter(pl.col("needs_review") == True)  # noqa: E712

    def apply_review(self, chunk_id: str, reviewer_id: str, status: str) -> pl.DataFrame:
        """Update review status for a single chunk in Silver and rewrite partitions."""
        df = self.load_chunks(layer="silver")
        if df.is_empty():
            return df
        # ensure columns exist for update
        if "review_status" not in df.columns:
            df = df.with_columns(pl.lit(None).cast(pl.Utf8).alias("review_status"))
        if "reviewer_id" not in df.columns:
            df = df.with_columns(pl.lit(None).cast(pl.Utf8).alias("reviewer_id"))
        if "reviewed_at" not in df.columns:
            df = df.with_columns(pl.lit(None).cast(pl.Datetime(time_unit="us", time_zone=None)).alias("reviewed_at"))
        now = datetime.now()
        updated = df.with_columns(
            pl.when(pl.col("chunk_id") == chunk_id)
            .then(pl.lit(status))
            .otherwise(pl.col("review_status")).alias("review_status"),
            pl.when(pl.col("chunk_id") == chunk_id)
            .then(pl.lit(False))
            .otherwise(pl.col("needs_review")).alias("needs_review"),
            pl.when(pl.col("chunk_id") == chunk_id)
            .then(pl.lit(reviewer_id))
            .otherwise(pl.col("reviewer_id")).alias("reviewer_id"),
            pl.when(pl.col("chunk_id") == chunk_id)
            .then(pl.lit(now))
            .otherwise(pl.col("reviewed_at")).alias("reviewed_at"),
        )

        # rewrite partitions
        for (city, month), group in updated.group_by(["city_id", "publication_month"], maintain_order=True):
            part_dir = self.base / "silver" / str(city) / str(month)
            path = part_dir / "chunks.parquet"
            self._write_table(group, path)
            Manifest(row_count=group.height, schema_version=self.config.schema_version).save(part_dir / "manifest.json")
        return updated.filter(pl.col("chunk_id") == chunk_id)

    def promote_to_gold(
        self,
        chunk_ids: Sequence[str],
        embedding_model_tag: str,
        retrieval_profile: str,
        chunk_schema_version: int | None = None,
    ) -> Path | None:
        """Copy selected Silver chunks to Gold, tagging embedding/retrieval metadata."""
        silver = self.load_chunks(layer="silver")
        if silver.is_empty():
            return None
        subset = silver.filter(pl.col("chunk_id").is_in(chunk_ids))
        if subset.is_empty():
            return None
        subset = subset.with_columns(
            pl.lit(embedding_model_tag).alias("embedding_model_tag"),
            pl.lit(retrieval_profile).alias("retrieval_profile"),
            pl.lit(chunk_schema_version).alias("chunk_schema_version"),
        )
        last_path: Path | None = None
        for (city, month), group in subset.group_by(["city_id", "publication_month"], maintain_order=True):
            part_dir = self.base / "gold" / str(city) / str(month)
            part_dir.mkdir(parents=True, exist_ok=True)
            path = part_dir / "chunks.parquet"
            existing = self._load_if_exists(path)
            combined = self._dedup(existing, group, subset=["chunk_id"])
            self._write_table(combined, path)
            Manifest(row_count=combined.height, schema_version=self.config.schema_version).save(part_dir / "manifest.json")
            last_path = path
        return last_path

    # ---------------- Logging -----------------
    def register_run(self, run_id: str, layer: str, tag: str, row_count: int, status: str) -> None:
        """Emit a `storage_run` event to structured logs with optional file sink."""
        log_file = self.base / "logs" / "ingestion.log"
        log_file.parent.mkdir(parents=True, exist_ok=True)
        # Ensure file handler attached
        from diario_utils.logging.structlog_config import configure_structlog

        configure_structlog(log_file=log_file)
        logger.info(
            "storage_run",
            run_id=run_id,
            layer=layer,
            tag=tag,
            row_count=row_count,
            status=status,
        )

    # ---------------- Helpers -----------------
    def _build_gazette_df(
        self,
        editions: list[GazetteEdition],
        city_id: str,
        publication_month: str,
        crawler_tag: str | None,
        ingestion_run_id: str | None,
    ) -> pl.DataFrame:
        rows = []
        for ed in editions:
            rows.append(
                {
                    "city_id": city_id,
                    "publication_month": publication_month,
                    "edition_id": ed.edition_id,
                    "publication_date": ed.publication_date,
                    "edition_number": ed.metadata.edition_number,
                    "supplement": ed.metadata.supplement,
                    "edition_type_id": ed.metadata.edition_type_id,
                    "edition_type_name": ed.metadata.edition_type_name,
                    "pdf_url": ed.metadata.pdf_url,
                    "total_articles": ed.total_articles,
                    "crawler_tag": crawler_tag,
                    "ingestion_run_id": ingestion_run_id,
                }
            )
        return pl.DataFrame(rows)

    def _build_article_df(
        self,
        editions: list[GazetteEdition],
        city_id: str,
        publication_month: str,
        crawler_tag: str | None,
        ingestion_run_id: str | None,
    ) -> pl.DataFrame:
        rows = []
        for ed in editions:
            for article in ed.articles:
                raw = article.content.raw_content
                is_bytes = isinstance(raw, (bytes, bytearray))
                rows.append(
                    {
                        "city_id": city_id,
                        "publication_month": publication_month,
                        "article_id": article.article_id,
                        "edition_id": ed.edition_id,
                        "publication_date": ed.publication_date,
                        "title": article.title,
                        "hierarchy_path": article.hierarchy_path,
                        "identifier": article.metadata.identifier,
                        "protocol": article.metadata.protocol,
                        "depth": len(article.hierarchy_path),
                        "content_type": article.content.content_type.value,
                        "content_path": None,
                        "raw_content_text": None if is_bytes else raw,
                        "raw_content_bytes": raw if is_bytes else None,
                        "crawler_tag": crawler_tag,
                        "ingestion_run_id": ingestion_run_id,
                    }
                )
        schema = {
            "raw_content_text": pl.Utf8,
            "raw_content_bytes": pl.Binary,
            "content_type": pl.Utf8,
            "content_path": pl.Utf8,
        }
        df = pl.DataFrame(rows)
        for col, dtype in schema.items():
            if col in df.columns:
                df = df.with_columns(pl.col(col).cast(dtype))
            else:
                df = df.with_columns(pl.lit(None).cast(dtype).alias(col))
        return df

    def _collect_paths(self, layer: str, city_id: str | None, publication_month: str | None, filename: str) -> list[Path]:
        base = self.base / layer
        if city_id and publication_month:
            path = base / str(city_id) / str(publication_month) / filename
            return [path] if path.exists() else []
        if city_id:
            return sorted((base / str(city_id)).glob(f"*/{filename}"))
        return sorted(base.glob(f"*/*/{filename}"))

    def _concat(self, paths: list[Path]) -> pl.DataFrame:
        if not paths:
            return pl.DataFrame()
        frames = [pl.read_parquet(p) for p in paths if p.exists()]
        if not frames:
            return pl.DataFrame()
        return pl.concat(frames, how="vertical_relaxed")

    def _load_if_exists(self, path: Path) -> pl.DataFrame | None:
        if path.exists():
            return pl.read_parquet(path)
        return None

    def _dedup(self, existing: pl.DataFrame | None, incoming: pl.DataFrame, subset: list[str]) -> pl.DataFrame:
        if existing is None or existing.is_empty():
            return incoming.unique(subset=subset, keep="last")

        # Align schemas
        for col in incoming.columns:
            if col not in existing.columns:
                existing = existing.with_columns(pl.lit(None).cast(incoming.schema[col]).alias(col))
        for col in existing.columns:
            if col not in incoming.columns:
                incoming = incoming.with_columns(pl.lit(None).cast(existing.schema[col]).alias(col))
        # Cast differing dtypes conservatively to Utf8 when both string-like, else keep existing
        for col in incoming.columns:
            if col in existing.columns and incoming.schema[col] != existing.schema[col]:
                incoming = incoming.with_columns(pl.col(col).cast(existing.schema[col]))

        combined = pl.concat([existing, incoming], how="vertical_relaxed")
        return combined.unique(subset=subset, keep="last")

    def _write_table(self, df: pl.DataFrame, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        df.write_parquet(path, compression=self.config.compression)
