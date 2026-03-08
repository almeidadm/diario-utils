from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Literal

import duckdb
import polars as pl

from diario_utils.storage.base import Storage
from diario_utils.storage.config import StorageConfig
from diario_utils.storage.local import LocalStorage
from diario_utils.storage.manifest import Manifest
from diario_utils.storage.paths import (
    build_partition_path,
    manifest_path,
    month_from_date,
    table_filename,
)

logger = logging.getLogger(__name__)

Layer = Literal["bronze", "silver", "gold"]


class StorageClient:
    """High-level API to append, read, query, and manage medallion storage."""

    def __init__(
        self,
        config: StorageConfig | None = None,
        backend: Storage | None = None,
    ) -> None:
        """Create a client with optional config and backend (defaults to LocalStorage)."""
        self.config = config or StorageConfig()
        base_path = self.config.resolve_base()
        self.backend = backend or LocalStorage(base_path)
        self.base_path = base_path

        self._duck_conn: duckdb.DuckDBPyConnection | None = None
        if self.config.duckdb_path:
            self._duck_conn = duckdb.connect(self.config.duckdb_path)
            self._duck_conn.execute(f"SET threads TO {self.config.threads}")

    # ----------------------------- helpers -----------------------------
    def _ensure_columns(
        self, df: pl.DataFrame, required: dict[str, Any]
    ) -> pl.DataFrame:
        """Add missing columns with default values to keep writes schema-consistent."""
        missing = {k: v for k, v in required.items() if k not in df.columns}
        if not missing:
            return df
        return df.with_columns([pl.lit(v).alias(k) for k, v in missing.items()])

    def _ensure_month(self, partition_keys: dict[str, Any]) -> str:
        """Resolve publication month from partition keys."""
        if "month" in partition_keys:
            return str(partition_keys["month"])
        if "publication_date" in partition_keys:
            return month_from_date(partition_keys["publication_date"])
        raise ValueError("partition_keys must include 'month' or 'publication_date'")

    def _write_table(
        self,
        layer: Layer,
        table: Literal["gazette", "chunks", "vectors"],
        df: pl.DataFrame,
        key_columns: list[str],
        partition_keys: dict[str, Any],
        parser_tag: str | None = None,
        embedding_model_tag: str | None = None,
        schema_version: int = 1,
    ) -> Path:
        """Write a logical table with deduplication and manifest refresh."""
        city_id = str(partition_keys.get("city_id"))
        if not city_id:
            raise ValueError("partition_keys must include city_id")
        month = self._ensure_month(partition_keys)

        defaults: dict[str, Any] = {
            "city_id": city_id,
            "publication_month": month,
        }
        if parser_tag:
            defaults["parser_tag"] = parser_tag
        if embedding_model_tag:
            defaults["embedding_model_tag"] = embedding_model_tag
        df = self._ensure_columns(df, defaults)
        if (
            "publication_date" in partition_keys
            and "publication_date" not in df.columns
        ):
            df = df.with_columns(
                pl.lit(partition_keys["publication_date"]).alias("publication_date")
            )

        partition = build_partition_path(
            self.base_path,
            layer,
            city_id=city_id,
            month=month,
            parser_tag=parser_tag,
            embedding_model_tag=embedding_model_tag,
        )
        table_path = partition / table_filename(table)

        # Merge with existing data (dedup keeping last)
        if table_path.exists():
            existing = self.backend.read_parquet(
                str(table_path.relative_to(self.base_path))
            )
            combined = pl.concat([existing, df], how="diagonal")
        else:
            combined = df

        combined = combined.unique(subset=key_columns, keep="last")

        self.backend.write_parquet(
            str(table_path.relative_to(self.base_path)),
            combined,
            compression=self.config.compression,
            compression_level=self.config.compression_level,
            statistics=True,
        )

        manifest = Manifest.from_file(table_path, schema_version=schema_version)
        manifest.save()
        return table_path

    def _scan_partition(
        self,
        table: Literal["gazette", "chunks", "vectors"],
        layer: Layer,
        city_id: str | None,
        month: str | None,
        parser_tag: str | None,
        embedding_model_tag: str | None,
    ) -> list[str]:
        """Return matching parquet paths for a partition selection."""
        base = self.base_path
        city_glob = f"city_id={city_id}" if city_id else "city_id=*"
        month_glob = f"yyyymm={month}" if month else "yyyymm=*"
        suffix_tag = ""
        if layer == "silver" and parser_tag:
            suffix_tag = f"/parser_tag={parser_tag}"
        elif layer == "silver":
            suffix_tag = "/parser_tag=*"
        if layer == "gold" and embedding_model_tag:
            suffix_tag = f"/embedding_model_tag={embedding_model_tag}"
        elif layer == "gold":
            suffix_tag = "/embedding_model_tag=*"

        pattern = (
            base
            / layer
            / city_glob
            / month_glob
            / suffix_tag.strip("/")
            / table_filename(table)
        )
        # Remove possible double slashes
        pattern = Path(str(pattern).replace("**", "*").replace("//", "/"))
        matches = [str(p) for p in base.glob(str(pattern.relative_to(base)))]
        return matches

    def get_manifest(
        self,
        layer: Layer,
        city_id: str,
        month: str,
        parser_tag: str | None = None,
        embedding_model_tag: str | None = None,
    ) -> Manifest | None:
        """Load manifest for a partition if it exists."""
        partition = build_partition_path(
            self.base_path,
            layer=layer,
            city_id=city_id,
            month=month,
            parser_tag=parser_tag,
            embedding_model_tag=embedding_model_tag,
        )
        mpath = manifest_path(partition)
        if not mpath.exists():
            return None
        return Manifest.load(mpath)

    # ----------------------------- writes ------------------------------
    def append_gazettes(
        self, df: pl.DataFrame, partition_keys: dict[str, Any], schema_version: int = 1
    ) -> Path:
        """Append gazette rows into bronze layer, deduplicating by edition_id."""
        return self._write_table(
            layer="bronze",
            table="gazette",
            df=df,
            key_columns=["edition_id"],
            partition_keys=partition_keys,
            schema_version=schema_version,
        )

    def append_chunks(
        self,
        df: pl.DataFrame,
        partition_keys: dict[str, Any],
        layer: Literal["silver", "gold"] = "silver",
        parser_tag: str | None = None,
        embedding_model_tag: str | None = None,
        schema_version: int = 1,
    ) -> Path:
        """Append chunks into silver/gold with required tags and dedup by chunk_id."""
        if layer == "silver":
            parser_tag = parser_tag or partition_keys.get("parser_tag")
        if layer == "gold":
            embedding_model_tag = embedding_model_tag or partition_keys.get(
                "embedding_model_tag"
            )
        return self._write_table(
            layer=layer,
            table="chunks",
            df=df,
            key_columns=["chunk_id"],
            partition_keys=partition_keys,
            parser_tag=parser_tag,
            embedding_model_tag=embedding_model_tag,
            schema_version=schema_version,
        )

    def append_vectors(
        self,
        df: pl.DataFrame,
        partition_keys: dict[str, Any],
        embedding_model_tag: str | None = None,
        schema_version: int = 1,
    ) -> Path:
        """Append vectors into gold, requiring an embedding_model_tag."""
        embedding_model_tag = embedding_model_tag or partition_keys.get(
            "embedding_model_tag"
        )
        if embedding_model_tag is None:
            raise ValueError("embedding_model_tag is required for vectors")
        return self._write_table(
            layer="gold",
            table="vectors",
            df=df,
            key_columns=["chunk_id", "embedding_model_tag"],
            partition_keys=partition_keys,
            embedding_model_tag=embedding_model_tag,
            schema_version=schema_version,
        )

    # ----------------------------- reads -------------------------------
    def load_chunks(
        self,
        layer: Literal["silver", "gold"] = "silver",
        city_id: str | None = None,
        month: str | None = None,
        parser_tag: str | None = None,
        embedding_model_tag: str | None = None,
        columns: list[str] | None = None,
    ) -> pl.DataFrame:
        """Load chunks from silver/gold with optional filters and projections."""
        paths = self._scan_partition(
            table="chunks",
            layer=layer,
            city_id=city_id,
            month=month,
            parser_tag=parser_tag,
            embedding_model_tag=embedding_model_tag,
        )
        if not paths:
            return pl.DataFrame()
        lazy = pl.scan_parquet(paths, hive_partitioning=False)
        schema = lazy.schema
        if columns:
            lazy = lazy.select([pl.col(c) for c in columns if c in schema])
        if city_id and "city_id" in schema:
            lazy = lazy.filter(pl.col("city_id") == city_id)
        if month and "publication_month" in schema:
            lazy = lazy.filter(pl.col("publication_month") == month)
        if parser_tag and "parser_tag" in schema:
            lazy = lazy.filter(pl.col("parser_tag") == parser_tag)
        if embedding_model_tag and "embedding_model_tag" in schema:
            lazy = lazy.filter(pl.col("embedding_model_tag") == embedding_model_tag)
        return lazy.collect()

    def load_vectors(
        self,
        embedding_model_tag: str | None = None,
        retrieval_profile: str | None = None,
        columns: list[str] | None = None,
    ) -> pl.DataFrame:
        """Load vectors from gold filtered by embedding model and retrieval profile."""
        paths = self._scan_partition(
            table="vectors",
            layer="gold",
            city_id=None,
            month=None,
            parser_tag=None,
            embedding_model_tag=embedding_model_tag,
        )
        if not paths:
            return pl.DataFrame()
        lazy = pl.scan_parquet(paths, hive_partitioning=False)
        schema = lazy.schema
        if columns:
            lazy = lazy.select([pl.col(c) for c in columns if c in schema])
        if embedding_model_tag and "embedding_model_tag" in schema:
            lazy = lazy.filter(pl.col("embedding_model_tag") == embedding_model_tag)
        if retrieval_profile and "retrieval_profile" in schema:
            lazy = lazy.filter(pl.col("retrieval_profile") == retrieval_profile)
        return lazy.collect()

    # ----------------------------- queries -----------------------------
    def query(self, sql: str, params: dict[str, Any] | None = None) -> pl.DataFrame:
        """Run a SQL query via DuckDB and return a Polars DataFrame."""
        if self._duck_conn is None:
            raise RuntimeError("DuckDB connection not enabled")
        rel = self._duck_conn.execute(sql, params or {})
        try:
            return rel.pl()
        except AttributeError:  # duckdb<1.0 fallback
            return pl.from_arrow(rel.arrow())

    # ----------------------------- review flow -------------------------
    def list_needing_review(
        self, limit: int = 100, city_id: str | None = None
    ) -> pl.DataFrame:
        """Return silver chunks flagged for review, optionally filtered by city."""
        df = self.load_chunks(layer="silver", city_id=city_id)
        if df.is_empty():
            return df
        if "needs_review" not in df.columns:
            return pl.DataFrame()
        reviewed = df.filter(pl.col("needs_review") == True)  # noqa: E712
        if limit:
            reviewed = reviewed.head(limit)
        return reviewed

    def apply_review(
        self,
        chunk_id: str,
        reviewer_id: str,
        new_text: str | None = None,
        status: Literal["approved", "rejected", "pending"] = "approved",
        change_log: str | None = None,
    ) -> pl.DataFrame:
        """Update review fields for a chunk and persist updates partition-wise."""
        df = self.load_chunks(layer="silver")
        if df.is_empty():
            raise ValueError("No chunks available for review")
        mask_expr = pl.col("chunk_id") == chunk_id
        if df.filter(mask_expr).is_empty():
            raise ValueError(f"chunk_id {chunk_id} not found")
        df = self._ensure_columns(
            df,
            {
                "review_status": "pending",
                "reviewer_id": "",
                "reviewed_at": None,
                "change_log": "",
                "needs_review": False,
            },
        )
        text_expr = pl.col("text")
        if new_text is not None:
            text_expr = (
                pl.when(mask_expr).then(pl.lit(new_text)).otherwise(pl.col("text"))
            )
        updated = df.with_columns(
            [
                pl.when(mask_expr)
                .then(pl.lit(status))
                .otherwise(pl.col("review_status"))
                .alias("review_status"),
                pl.when(mask_expr)
                .then(pl.lit(reviewer_id))
                .otherwise(pl.col("reviewer_id"))
                .alias("reviewer_id"),
                pl.when(mask_expr)
                .then(pl.lit(datetime.now(timezone.utc)))
                .otherwise(pl.col("reviewed_at"))
                .alias("reviewed_at"),
                pl.when(mask_expr)
                .then(pl.lit(change_log or ""))
                .otherwise(pl.col("change_log"))
                .alias("change_log"),
                text_expr.alias("text"),
                pl.when(mask_expr)
                .then(pl.lit(False))
                .otherwise(pl.col("needs_review"))
                .alias("needs_review"),
            ]
        )
        # Write back grouped by partition
        for city in updated["city_id"].unique().to_list():
            subset = updated.filter(pl.col("city_id") == city)
            months = subset["publication_month"].unique().to_list()
            for month in months:
                part_df = subset.filter(pl.col("publication_month") == month)
                self.append_chunks(
                    part_df,
                    partition_keys={"city_id": city, "month": month},
                    layer="silver",
                    parser_tag=part_df.get_column("parser_tag").item(0)
                    if "parser_tag" in part_df.columns
                    else None,
                )
        return updated.filter(mask_expr)

    def promote_to_gold(
        self,
        chunk_ids: Iterable[str],
        embedding_model_tag: str,
        retrieval_profile: str,
    ) -> Path | None:
        """Promote approved silver chunks to gold, tagging embedding metadata."""
        ids = set(chunk_ids)
        silver = self.load_chunks(layer="silver")
        if silver.is_empty():
            raise ValueError("No silver chunks available")
        to_promote = silver.filter(
            (pl.col("chunk_id").is_in(ids)) & (pl.col("review_status") == "approved")
        )
        if to_promote.is_empty():
            return None
        promoted = to_promote.with_columns(
            [
                pl.lit(embedding_model_tag).alias("embedding_model_tag"),
                pl.lit(retrieval_profile).alias("retrieval_profile"),
                pl.col("publication_date")
                .str.slice(0, 7)
                .str.replace("-", "")
                .alias("publication_month"),
            ]
        )
        paths: list[Path] = []
        for city in promoted["city_id"].unique().to_list():
            city_df = promoted.filter(pl.col("city_id") == city)
            for month in city_df["publication_month"].unique().to_list():
                month_df = city_df.filter(pl.col("publication_month") == month)
                path = self.append_chunks(
                    month_df,
                    partition_keys={
                        "city_id": city,
                        "month": month,
                        "embedding_model_tag": embedding_model_tag,
                    },
                    layer="gold",
                    embedding_model_tag=embedding_model_tag,
                )
                paths.append(path)
        return paths[-1] if paths else None

    # ----------------------------- logging -----------------------------
    def register_run(
        self,
        run_id: str,
        layer: Layer,
        tag: str,
        row_count: int,
        status: str,
        log_path: str | Path | None = None,
    ) -> None:
        """Append a structured log line for an ingestion/review run."""
        log_file = (
            Path(log_path) if log_path else self.base_path / "logs" / "ingestion.log"
        )
        log_file.parent.mkdir(parents=True, exist_ok=True)
        line = {
            "run_id": run_id,
            "layer": layer,
            "tag": tag,
            "row_count": row_count,
            "status": status,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        with log_file.open("a", encoding="utf-8") as f:
            f.write(f"{line}\n")
