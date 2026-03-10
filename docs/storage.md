# Storage medallion

Guia rápido do layout Parquet usado pelo `diario-utils` para Bronze/Silver/Gold.
Foco especial nas colunas de conteúdo (`raw_content_*`, `text`, `content_path`, `content_type`) e como elas devem ser preenchidas para evitar deriva de schema.

## Colunas de conteúdo

### Bronze — `articles.parquet`
- `raw_content_text` (Utf8, nullable): versão texto quando o conteúdo original já está em texto/HTML extraído.
- `raw_content_bytes` (Binary, nullable): bytes originais (ex.: PDF). Nunca preencha `raw_content_text` e `raw_content_bytes` ao mesmo tempo.
- `content_type` (Utf8, obrigatório): valor do enum `ContentType` do `diario-contract` (`text`, `html`, `pdf`).
- `content_path` (Utf8, nullable): caminho/URI para o blob original, quando existir. Útil para reprocessar ou extrair texto posteriormente.

### Bronze — `gazettes.parquet`
- Não armazena conteúdo bruto. Apenas metadados da edição (datas, ids, totais). `content_type` não é persistido aqui.

### Silver/Gold — `chunks.parquet` (Acts/TextChunk)
- `text` (Utf8, obrigatório): texto normalizado do chunk/ato.
- `content_path` (Utf8, nullable): caminho para o artefato bruto (PDF/HTML) do qual o texto foi extraído.
- `content_type` (Utf8, obrigatório): enum `ContentType` do ato. Mantém o tipo original mesmo após extração de texto.
- Não armazenamos bytes em Silver/Gold; apenas texto e referência ao blob.

### Tabela de referência (contrato → colunas persistidas)

| Contrato (`diario-contract`) | Camada/arquivo              | Colunas de conteúdo | Observações |
| --- | --- | --- | --- |
| `GazetteEdition`             | `bronze/{city}/{YYYYMM}/gazettes.parquet` | (nenhuma) | Só metadados de edição; conteúdo não guardado. |
| `Article`                    | `bronze/{city}/{YYYYMM}/articles.parquet` | `raw_content_text`, `raw_content_bytes`, `content_type`, `content_path` | Preencher apenas um dos `raw_content_*`; `content_path` opcional. |
| `ParsedChunk`                | `silver/{city}/{YYYYMM}/chunks.parquet`   | `text`, `content_path`, `content_type` | Texto normalizado; sem bytes. |
| `Act` / `TextChunk`          | `silver` ou `gold` `chunks.parquet`       | `text`, `content_path`, `content_type` | Representação canônica armazenada; dedup por `chunk_id`. |

## Estratégia de escrita e alinhamento
- **Bronze:** separar `raw_content` em `raw_content_text` (Utf8) ou `raw_content_bytes` (Binary); manter ambas as colunas no schema e preencher a alternativa com `null` para evitar que Polars crie tipo `Null` diferente entre partições.
- **Silver/Gold:** persistir apenas `text` + `content_path`; nunca escrever bytes. Copiar `content_type` do ato original.
- **Conversão Article → Act (pipeline futura):** se o artigo veio em bytes, mantenha `content_path` apontando para o blob bruto e coloque o texto extraído em `text`; preserve o `content_type` original.
- **Alinhamento de schema:** ao anexar (append), force dtypes esperadas (`Utf8` para texto e paths, `Binary` para bytes, `Utf8` para `content_type`) mesmo quando os valores são nulos; isso evita deriva de schema entre partições mensais.
- **Deduplicação:** regras permanecem inalteradas (articles por `article_id`, acts/chunks por `chunk_id`); esta seção apenas esclarece semântica das colunas.

