# Changelog

All notable changes to this project will be documented in this file.

## 1.1.5 - 2026-03-08
- Migra a documentação para Markdown puro, removendo configuração e artefatos Sphinx.
- Adiciona docstrings às classes, métodos e funções públicas de logging e storage.
- Ajusta ambiente de testes para resolver importação de `diario_utils`.

## 1.2.0 - 2026-03-07
- Reescreve módulo `diario_utils.storage` com API em Polars e suporte a camadas Bronze/Silver/Gold.
- Adiciona manifests de auditoria (hash, row_count, schema_version) por partição.
- Implementa fluxo de revisão e promoção para Gold, incluindo `list_needing_review`, `apply_review` e `promote_to_gold`.
- Inclui documentação com diagrama mermaid do fluxo de storage.
- Atualiza dependências, adicionando `polars` e nova versão `diario-contract` v1.2.0.

## 0.1.1 - 2026-03-05
- Update `diario-contract` source to https and rev `v1.1.2`.
- Bump project version to `0.1.1`.
