# diario-utils

Shared infrastructure utilities for the Diario RAG ETL, focused on local medallion storage (Bronze/Silver/Gold) using Parquet + DuckDB + Polars.

## Documentation
- See `docs/index.md` for the Markdown docs entry point.

## Highlights
- StorageClient API for append/read/query across Bronze/Silver/Gold.
- Local filesystem backend with manifests (hash + row_count + schema_version).
- Review and promotion helpers to move Silver chunks to Gold.
- Ready for future cloud backends via pluggable StorageBackend.

## Quick start
```python
from diario_utils.storage import StorageClient, StorageConfig
import polars as pl

client = StorageClient(StorageConfig(base_path="data"))
chunks = pl.DataFrame([
    {
        "chunk_id": "c1",
        "city_id": "123",
        "publication_date": "2026-03-01",
        "publication_month": "202603",
        "text": "example",
        "needs_review": True,
        "parser_tag": "v1",
    }
])
client.append_chunks(chunks, {"city_id": "123", "publication_date": "2026-03-01", "parser_tag": "v1"})
```
