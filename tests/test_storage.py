from pathlib import Path

import polars as pl
from diario_contract.article.article import Article
from diario_contract.article.content import ArticleContent
from diario_contract.article.metadata import ArticleMetadata
from diario_contract.enums.content_type import ContentType
from diario_contract.gazette.edition import GazetteEdition
from diario_contract.gazette.metadata import GazetteMetadata

from diario_utils.storage import Manifest, StorageClient, StorageConfig


def _build_edition(
    edition_id: str,
    publication_date: str,
    article_id: str,
    content: str | bytes = "content",
) -> GazetteEdition:
    metadata = GazetteMetadata(
        edition_id=edition_id,
        publication_date=publication_date,  # type: ignore[arg-type]
        edition_number=1,
        supplement=False,
        edition_type_id=1,
        edition_type_name="regular",
        pdf_url="http://example.com",
    )
    article = Article(
        metadata=ArticleMetadata(
            article_id=article_id,
            edition_id=edition_id,
            hierarchy_path=["root"],
            title="title",
            identifier="id-1",
            protocol=None,
        ),
        content=ArticleContent(
            raw_content=content,
            content_type=ContentType.PDF if isinstance(content, bytes) else ContentType.TEXT,
        ),
    )
    return GazetteEdition(metadata=metadata, articles=[article])


def test_append_gazettes_writes_both_tables(tmp_path: Path):
    client = StorageClient(StorageConfig(base_path=tmp_path, duckdb_path=":memory:"))
    edition = _build_edition("ed1", "2026-03-01", "a1")

    results = client.append_gazettes(
        [edition], city_id="123", crawler_tag="ctag", ingestion_run_id="run1"
    )

    assert len(results) == 1
    result = results[0]
    assert result.gazette_path.exists()
    assert result.articles_path and result.articles_path.exists()

    gaz_manifest = Manifest.load(result.gazette_path.parent / "manifest.json")
    art_manifest = Manifest.load(result.articles_path.parent / "manifest.json")
    assert gaz_manifest.row_count == 1
    assert art_manifest.row_count == 1

    gaz_df = client.load_gazettes(city_id="123")
    art_df = client.load_articles(city_id="123")
    assert gaz_df.get_column("total_articles")[0] == 1
    assert art_df.get_column("content_type")[0] == ContentType.TEXT.value
    assert art_df.get_column("crawler_tag")[0] == "ctag"
    assert art_df.get_column("ingestion_run_id")[0] == "run1"


def test_dedup_chunks_keeps_last(tmp_path: Path):
    client = StorageClient(StorageConfig(base_path=tmp_path, duckdb_path=":memory:"))
    base_keys = {"city_id": "123", "publication_date": "2026-03-02", "parser_tag": "v1"}

    first = pl.DataFrame(
        [
            {
                "chunk_id": "c1",
                "city_id": "123",
                "publication_date": "2026-03-02",
                "text": "old",
                "needs_review": True,
                "parser_tag": "v1",
            }
        ]
    )
    second = first.with_columns(pl.lit("new").alias("text"))

    client.append_chunks(first, base_keys, layer="silver", parser_tag="v1")
    client.append_chunks(second, base_keys, layer="silver", parser_tag="v1")

    loaded = client.load_chunks(layer="silver", city_id="123")
    assert loaded.height == 1
    assert loaded.get_column("text")[0] == "new"


def test_append_gazettes_dedup_by_edition_and_article(tmp_path: Path):
    client = StorageClient(StorageConfig(base_path=tmp_path, duckdb_path=":memory:"))
    first = _build_edition("ed1", "2026-03-01", "a1", content="old")
    second = _build_edition("ed1", "2026-03-01", "a1", content="new")

    client.append_gazettes([first], city_id="123")
    client.append_gazettes([second], city_id="123")

    gazettes = client.load_gazettes(city_id="123")
    articles = client.load_articles(city_id="123")
    assert gazettes.height == 1
    assert articles.height == 1
    assert articles.get_column("raw_content_text")[0] == "new"


def test_review_flow_and_promotion(tmp_path: Path):
    client = StorageClient(StorageConfig(base_path=tmp_path, duckdb_path=":memory:"))
    chunk_df = pl.DataFrame(
        [
            {
                "chunk_id": "c1",
                "city_id": "123",
                "publication_date": "2026-03-03",
                "text": "needs review",
                "needs_review": True,
                "parser_tag": "v1",
                "review_status": "pending",
            }
        ]
    )
    client.append_chunks(
        chunk_df,
        {"city_id": "123", "publication_date": "2026-03-03", "parser_tag": "v1"},
    )

    needing = client.list_needing_review()
    assert needing.height == 1

    reviewed = client.apply_review(chunk_id="c1", reviewer_id="bob", status="approved")
    assert reviewed.get_column("review_status")[0] == "approved"
    assert reviewed.get_column("needs_review")[0] is False

    promoted_path = client.promote_to_gold(
        ["c1"], embedding_model_tag="e5-base", retrieval_profile="default"
    )
    assert promoted_path is not None
    gold = client.load_chunks(layer="gold")
    assert gold.height == 1
    assert gold.get_column("embedding_model_tag")[0] == "e5-base"
    assert gold.get_column("retrieval_profile")[0] == "default"


def test_append_gazettes_groups_by_month(tmp_path: Path):
    client = StorageClient(StorageConfig(base_path=tmp_path, duckdb_path=":memory:"))
    mar = _build_edition("ed-mar", "2026-03-15", "a-mar")
    apr = _build_edition("ed-apr", "2026-04-02", "a-apr")

    results = client.append_gazettes([mar, apr], city_id="123")

    months = {res.publication_month for res in results}
    assert months == {"202603", "202604"}
    assert all(res.gazette_path.exists() for res in results)
