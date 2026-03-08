from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from typing import Literal


def month_from_date(value: date | datetime | str) -> str:
    """Return yyyymm string from date/datetime/ISO string."""
    if isinstance(value, str):
        try:
            dt = datetime.fromisoformat(value)
        except ValueError:
            raise ValueError(f"Invalid date string: {value}")
    else:
        dt = value
    return f"{dt.year}{dt.month:02d}"


def build_partition_path(
    base_path: Path,
    layer: Literal["bronze", "silver", "gold"],
    city_id: str,
    month: str,
    parser_tag: str | None = None,
    embedding_model_tag: str | None = None,
) -> Path:
    """Build a partition path according to layer and optional tags."""
    path = base_path / layer / f"city_id={city_id}" / f"yyyymm={month}"
    if layer == "silver" and parser_tag:
        path = path / f"parser_tag={parser_tag}"
    if layer == "gold" and embedding_model_tag:
        path = path / f"embedding_model_tag={embedding_model_tag}"
    return path


def manifest_path(partition_path: Path) -> Path:
    """Return manifest.json path under a partition."""
    return partition_path / "manifest.json"


def table_filename(
    table: Literal["gazette", "articles", "chunks", "vectors"]
) -> str:
    """Map logical table name to Parquet filename."""
    return f"{table}.parquet"
