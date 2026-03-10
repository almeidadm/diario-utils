**Implement Parquet Storage (no DuckDB)**  
Rebuild `diario_utils.storage` around Parquet + Polars, wire review/promotion, update docs and deps.

**Summary**  
- Add a Parquet-only medallion storage layer (bronze/silver/gold) with typed contracts.  
- Replace DuckDB references; keep `diario-contract` at v2.0.0.  
- Document usage and bump project version.

**Key Changes**  
- Create `diario_utils/storage.py` with:
  - `Manifest` (row_count, schema_version, data_sha256; save/load JSON).
  - `StorageConfig` (base_path: Path, duckdb_path accepted but ignored, schema_version default=1, compression="zstd").
  - `GazetteWriteResult` (city_id, publication_month, gazette_path, articles_path).
  - `StorageClient` methods:
    - `_align_schema` to merge Null/Binary/String/Struct differences; ensures stable schema across appends.
    - `_write_table`/`_load_table` helpers for Parquet + manifest.
    - `append_gazettes(editions, city_id, crawler_tag=None, ingestion_run_id=None)`:
      writes per `publication_month` under `bronze/{city_id}/{YYYYMM}/(gazettes|articles).parquet`; dedup keys: gazettes by `edition_id`, articles by `article_id`; splits `raw_content` into `raw_content_text`/`raw_content_bytes`; stores `content_type`, `crawler_tag`, `ingestion_run_id`, totals.
    - `load_gazettes(city_id, publication_month=None)` / `load_articles(...)`.
    - `append_chunks(df, base_keys, layer="silver", parser_tag=None)`:
      injects `publication_month` from `publication_date` if missing; dedup by `chunk_id`; path `layer/{city_id}/{YYYYMM}/chunks.parquet`.
    - `load_chunks(layer="silver", city_id=None, publication_month=None)`.
    - `list_needing_review()` (silver, `needs_review=True`).
    - `apply_review(chunk_id, reviewer_id, status)` updates silver row (`review_status`, `needs_review=False`, `reviewed_at`).
    - `promote_to_gold(chunk_ids, embedding_model_tag, retrieval_profile, chunk_schema_version=None)` copies selected silver rows to gold, setting tags; dedup by `chunk_id`; returns written path.
    - `register_run(run_id, layer, tag, row_count, status)` logs `storage_run` to `base_path/logs/ingestion.log` via existing structlog config.
- Export public API from `diario_utils/__init__.py`.
- Dependency cleanup: remove DuckDB from `pyproject.toml` deps and any docs; regenerate `requirements.lock`/`uv.lock`.
- Version bump to `1.5.0` (SemVer MINOR for new functionality and dependency change).
- Docs:
  - Add `docs/storage.md` (referenced by `docs/index.md`): overview, path layout, quick-start code, dedup/review/promote notes, mermaid flow.
  - Clarify content columns (`raw_content_text`, `raw_content_bytes`, `content_path`, `content_type`, `text`) across Bronze articles/gazettes and Silver/Gold chunks; include contract→column table and schema/nullability guidance.
  - Update `README.md` to highlight Parquet-only backend and revised quick start (no DuckDB).
- Changelog: add `1.5.0 - 2026-03-09` entry summarizing new Parquet storage, review/promotion flow, DuckDB removal.

**Test Plan**  
- Run `pytest tests/test_storage.py -q` (or full `pytest -q`) after implementation.  
- (Optional) `ruff check diario_utils` if configured post-change.

**Assumptions**  
- Keep `diario-contract` pinned to current v2.0.0 as per user choice.  
- Base paths are local filesystem; no S3 backend needed.  
- Compression `zstd` acceptable; Parquet write/read via Polars available in env.</proposed_plan>
