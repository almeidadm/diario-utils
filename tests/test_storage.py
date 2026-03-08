from pathlib import Path

import polars as pl

from diario_utils.storage import Manifest, StorageClient, StorageConfig


def test_append_gazettes_creates_manifest(tmp_path: Path):
    client = StorageClient(StorageConfig(base_path=tmp_path, duckdb_path=":memory:"))
    df = pl.DataFrame(
        [
            {
                "edition_id": "ed1",
                "city_id": "123",
                "publication_date": "2026-03-01",
                "edition_number": 1,
                "supplement": False,
                "edition_type_id": 1,
                "edition_type_name": "regular",
                "pdf_url": "http://example.com",
                "content_type": "pdf",
            }
        ]
    )

    path = client.append_gazettes(
        df, {"city_id": "123", "publication_date": "2026-03-01"}
    )

    manifest_file = path.parent / "manifest.json"
    assert manifest_file.exists()
    manifest = Manifest.load(manifest_file)
    assert manifest.row_count == 1
    assert manifest.schema_version == 1
    assert len(manifest.sha256) == 64


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
