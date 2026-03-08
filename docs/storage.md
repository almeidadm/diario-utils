# Storage medallion

Este documento descreve o módulo de persistência local seguindo as camadas Bronze/Silver/Gold.

## Layout de diretórios

```text
  data/
    bronze/city_id=123/yyyymm=202603/
      gazette.parquet
      manifest.json
    silver/city_id=123/yyyymm=202603/parser_tag=v1/
      chunks.parquet
      manifest.json
    gold/city_id=123/yyyymm=202603/embedding_model_tag=e5-base/
      chunks.parquet
      vectors.parquet
      manifest.json
    logs/ingestion.log
```

## Fluxo principal

```mermaid
flowchart TD
  A[append_gazettes] -->|bronze| B[gazette.parquet]
  B -->|filters| Q[query]
  C[append_chunks silver] -->|silver| D[chunks.parquet]
  D --> E[list_needing_review]
  E --> F[apply_review]
  F --> G[promote_to_gold]
  G --> H[chunks (gold)]
  I[append_vectors] --> H
  H --> R[load_chunks (gold)]
  R --> Q
```

## Uso rápido

```python
import polars as pl
from diario_utils.storage import StorageClient, StorageConfig

client = StorageClient(StorageConfig(base_path="data", duckdb_path=":memory:"))

gazettes = pl.DataFrame([
    {"edition_id": "ed1", "city_id": "123", "publication_date": "2026-03-01"},
])
client.append_gazettes(gazettes, {"city_id": "123", "publication_date": "2026-03-01"})

chunks = pl.DataFrame([
    {
        "chunk_id": "c1",
        "city_id": "123",
        "publication_date": "2026-03-01",
        "publication_month": "202603",
        "text": "lorem",
        "needs_review": True,
        "parser_tag": "v1",
    }
])
client.append_chunks(chunks, {"city_id": "123", "publication_date": "2026-03-01", "parser_tag": "v1"})

needing = client.list_needing_review()
client.apply_review(chunk_id="c1", reviewer_id="alice", status="approved")
client.promote_to_gold(["c1"], embedding_model_tag="e5-base", retrieval_profile="default")
```
