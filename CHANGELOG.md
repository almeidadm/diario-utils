# Changelog

All notable changes to this project will be documented in this file.

## 1.4.0 - 2026-03-09
- Adiciona logging estruturado com `structlog` ao `StorageClient` (eventos de append/review/promote) e ao `register_run`, emitindo JSON para stdout e `logs/ingestion.log`.
- Inclui helper `configure_structlog`/`get_logger`, com testes cobrindo idempotência e registro em arquivo.
- Atualiza versão do pacote para `1.4.0`, acrescenta dependência `structlog` e regenera lockfiles.

## 1.5.0 - 2026-03-09
- Reimplementa storage somente em Parquet/Polars, removendo dependência de DuckDB.
- Documenta colunas de conteúdo (`raw_content_text`, `raw_content_bytes`, `content_path`, `content_type`, `text`) em `docs/storage.md` e adiciona tabela contrato→colunas.
- Mantém API de revisão/promoção e deduplicação, alinhando schemas entre partições.

## 1.3.1 - 2026-03-08
- Corrige concatenação de Parquet quando colunas `Null` recebem bytes, alinhando schemas antes de `append_gazettes` (evita erro “type Binary is incompatible with expected type Null”).
- Aplica alinhamento de schema a todas as tabelas escritas por `_write_table`.
- Adiciona teste cobrindo mistura de `raw_content_text` e `raw_content_bytes` no mesmo mês.

## 1.3.0 - 2026-03-08
- Breaking: `append_gazettes` agora aceita apenas `GazetteEdition` e grava duas tabelas (`gazette.parquet` + `articles.parquet`) por partição Bronze.
- Adiciona `load_gazettes` e `load_articles` para leitura filtrada por cidade/mês.
- Inclui manifesto e deduplicação por `edition_id` e `article_id` ao escrever edições e artigos.
- Atualiza documentação (AGENTS, README, docs/storage.md) com novos schemas e exemplos.

## 1.1.5 - 2026-03-08
- Migra a documentação para Markdown puro, removendo configuração e artefatos Sphinx.
- Adiciona docstrings às classes, métodos e funções públicas de logging e storage.
- Ajusta ambiente de testes para resolver importação de `diario_utils`.

## 1.1.4 - 2026-03-07
- Reescreve módulo `diario_utils.storage` com API em Polars e suporte a camadas Bronze/Silver/Gold.
- Adiciona manifests de auditoria (hash, row_count, schema_version) por partição.
- Implementa fluxo de revisão e promoção para Gold, incluindo `list_needing_review`, `apply_review` e `promote_to_gold`.
- Inclui documentação com diagrama mermaid do fluxo de storage.
- Atualiza dependências, adicionando `polars` e nova versão `diario-contract` v1.2.0.

## 0.1.1 - 2026-03-05
- Update `diario-contract` source to https and rev `v1.1.2`.
- Bump project version to `0.1.1`.
