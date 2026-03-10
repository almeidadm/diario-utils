# diario-utils

Shared infrastructure utilities for the Diario RAG ETL, focused on local medallion storage (Bronze/Silver/Gold) using Parquet + Polars.

## Documentation
- See `docs/index.md` for the Markdown docs entry point.
- Content column rules: `docs/storage.md` (seção "Colunas de conteúdo").

## Highlights
- StorageClient API for append/read/query across Bronze/Silver/Gold.
- Local filesystem backend with manifests (hash + row_count + schema_version).
- Review and promotion helpers to move Silver chunks to Gold.
- Structured JSON logging with `structlog`, including ingestion run records in `logs/ingestion.log`.
- Ready for future cloud backends via pluggable StorageBackend.

## Quick start
```python
import polars as pl
from diario_contract.article.article import Article
from diario_contract.article.content import ArticleContent
from diario_contract.article.metadata import ArticleMetadata
from diario_contract.enums.content_type import ContentType
from diario_contract.gazette.edition import GazetteEdition
from diario_contract.gazette.metadata import GazetteMetadata
from diario_utils.storage import StorageClient, StorageConfig

client = StorageClient(StorageConfig(base_path="data"))

edition = GazetteEdition(
    metadata=GazetteMetadata(
        edition_id="ed1",
        publication_date="2026-03-01",
        edition_number=1,
        supplement=False,
        edition_type_id=1,
        edition_type_name="regular",
        pdf_url="http://example.com",
    ),
    articles=[
        Article(
            metadata=ArticleMetadata(
                article_id="a1",
                edition_id="ed1",
                hierarchy_path=["root"],
                title="title",
                identifier="id-1",
                protocol=None,
            ),
            content=ArticleContent(
                raw_content="content", content_type=ContentType.TEXT
            ),
        )
    ],
)
client.append_gazettes([edition], city_id="123")

chunks = pl.DataFrame(
    [
        {
            "chunk_id": "c1",
            "city_id": "123",
            "publication_date": "2026-03-01",
            "publication_month": "202603",
            "text": "example",
            "needs_review": True,
            "parser_tag": "v1",
        }
    ]
)
client.append_chunks(
    chunks,
    {"city_id": "123", "publication_date": "2026-03-01", "parser_tag": "v1"},
)
```
