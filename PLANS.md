
    # Corrigir concatenação Parquet (Null vs Binary) e liberar 1.3.1 — repo: PLANS.md

    This ExecPlan is a living document. The sections `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` must be kept up to date as work proceeds.

    ## Purpose / Big Picture

    Garantir que o diario-crawler consiga gravar artigos com conteúdo texto ou bytes na camada Bronze sem quebrar com erro de schema (Null vs Binary). Usuário vê o crawler terminar normalmente e encontra Parquets consistentes.

    ## Progress

    - [x] (2026-03-08 23:49Z) Analisado stacktrace e identificado causa: concatenação Polars falha ao combinar coluna Null com Binary.
    - [x] (2026-03-08 23:55Z) Implementado helper de alinhamento de schema em `diario_utils/storage/client.py` e aplicado em `_write_table`.
    - [x] (2026-03-08 23:57Z) Adicionado teste cobrindo mistura texto+bytes e garantindo dtype Binary.
    - [x] (2026-03-08 23:58Z) Atualizados `pyproject.toml` (v1.3.1) e `CHANGELOG.md`.
    - [x] (2026-03-08 23:50Z) Rodado `pytest tests/test_storage.py -k gazettes` — todos passaram.

    ## Surprises & Discoveries

    - Observation: Polars 0.20.x não expõe `find_common_supertype` e não converte automaticamente Null -> Binary em concat diagonal.
      Evidence: tentativa de `pl.concat` com DataFrame Null + Binary reproduz SchemaError.

    ## Decision Log

    - Decision: Adotar helper `_align_frames` que adiciona colunas faltantes, converte Null para dtype da outra tabela e, em conflito concreto, faz fallback para Utf8 antes do concat.
      Rationale: remove classe de erros Null vs Binary e previne divergências futuras entre partições.
      Date/Author: 2026-03-08 / Codex
    - Decision: Lançar versão 1.3.1 como patch de compatibilidade.
      Rationale: correção sem mudança de contrato público, desbloqueia crawler.
      Date/Author: 2026-03-08 / Codex

    ## Outcomes & Retrospective

    - Patch 1.3.1 validado localmente; testes específicos de gazettes passaram.
    - Próximo passo sugerido: reexecutar diario-crawler em ambiente alvo para confirmar ausência do SchemaError.

    ## Context and Orientation

    - Código impactado: `diario_utils/storage/client.py` (escritas e concatenação), `tests/test_storage.py` (cobertura de append_gazettes), versão em `pyproject.toml`, changelog em `CHANGELOG.md`.
    - Ambiente: Polars 0.20.x; storage local via Parquet; erro reproduzido ao gravar artigos com `raw_content_bytes`.

    ## Plan of Work

    1) Introduzir helper de alinhamento de schema e usá-lo antes de concatenar parquet existente com novo lote em `_write_table`.
    2) Cobrir cenário com artigos texto e bytes no mesmo mês em teste dedicado.
    3) Atualizar versão para 1.3.1 e registrar no changelog.
    4) Executar testes direcionados e registrar resultado.

    ## Concrete Steps

    - Code: editar `diario_utils/storage/client.py` para adicionar `_align_frames` e chamar na leitura de partição existente.
    - Tests: criar `test_append_gazettes_handles_text_and_bytes` em `tests/test_storage.py`.
    - Versioning: atualizar `pyproject.toml` e `CHANGELOG.md`.
    - Commands (cwd repo):
      - `pytest tests/test_storage.py -k gazettes`

    ## Validation and Acceptance

    - Executar `pytest tests/test_storage.py -k gazettes`; espera passar inclusive o novo teste de mistura texto+bytes.
    - Ao reprocessar crawler, não deve surgir erro “type Binary is incompatible with expected type Null”; artigos escritos com dtype Binary em `raw_content_bytes`.

    ## Idempotence and Recovery

    - Reexecutar `append_gazettes` é idempotente por dedup (keep last) e alinhamento de schema; testes podem ser rodados repetidamente.
    - Em caso de falha de teste, reverter apenas arquivos tocados nesta tarefa.

    ## Artifacts and Notes

    - Stacktrace original: Polars SchemaError em `pl.concat` (Null vs Binary) no `_write_table` ao escrever bronze/articles.

    ## Interfaces and Dependencies

    - Função adicionada: `StorageClient._align_frames(existing: pl.DataFrame, incoming: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]`, usada internamente por `_write_table`.
    - Dependências: Polars 0.20.x; nenhum ajuste de contrato público na API do StorageClient.
