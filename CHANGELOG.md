# Changelog

## [0.4.0] - 2026-03-26

Refatoracao arquitetural com mudancas de API, migracao de HTTP client, novos metodos de consulta e otimizacoes de performance.

### BREAKING CHANGES

**Assinatura de `read()` alterada em todos os providers:**
- `start` agora e o primeiro argumento posicional (antes vinha depois de `instituicao`)
- `instituicao` agora e keyword-only e opcional (antes era posicional e obrigatorio em IFDATA/COSIF)
- Permite bulk reads sem filtro de instituicao (`instituicao=None` retorna todas)

```python
# Antes (v0.3.0)
df = bcb.ifdata.read('60872504', '2024-12')
df = bcb.cosif.read('60872504', '2024-12')

# Agora (v0.4.0)
df = bcb.ifdata.read('2024-12', instituicao='60872504')
df = bcb.cosif.read('2024-12', instituicao='60872504')
df = bcb.ifdata.read('2024-12')  # bulk: todas as instituicoes
```

**`cadastro.read()`: `start` agora e obrigatorio** (antes era opcional com fallback para ultimo periodo).

**Metodos renomeados (ingles -> portugues):**

| v0.3.0 | v0.4.0 |
|--------|--------|
| `list_periods()` | `list_periodos()` |
| `list_accounts()` | `list_contas()` |
| `list_mapeamento()` | `mapeamento()` |

**Metodos removidos:**

| Removido | Substituto |
|----------|------------|
| `bcb.search()` | `bcb.cadastro.search()` |
| `cadastro.info()` | `cadastro.read()` |
| `cadastro.list_segmentos()` | `cadastro.list(["SEGMENTO"])` |
| `cadastro.list_ufs()` | `cadastro.list(["UF"])` |
| `cadastro.get_conglomerate_members()` | `ifdata.mapeamento()` |
| `ifdata.list_institutions()` | `cadastro.search(fonte='ifdata')` |
| `ifdata.list_reporters()` | removido sem substituto |
| `ifdata.list_reports()` | `ifdata.list(["RELATORIO"])` |

**Excecoes removidas:** `EntityNotFoundError`, `AmbiguousIdentifierError`

**Dependencia HTTP:** `requests` + `urllib3` substituidos por `httpx>=0.28.0`

**Removidos:** dataclass `ScopeResolution`, modulo `domain/models.py`, funcao `yyyymm_to_datetime()`

### Added

- `cadastro.search(termo, *, fonte, escopo, start, end, limit)`: busca centralizada de instituicoes com fuzzy matching, filtros por fonte de dados (ifdata/cosif), escopo, e filtragem por disponibilidade de dados no periodo
- `list()` generico em todos os providers (IFDATA, COSIF, Cadastro): retorna valores distintos para colunas solicitadas via `SELECT DISTINCT` no DuckDB, com filtros categoricos e truncation warning
- `ifdata.mapeamento(start, end)`: acesso direto a tabela de mapeamento COD_INST <-> CNPJ_8 por escopo e periodo
- Bulk read (`instituicao=None`) em `cosif.read()` e `ifdata.read()`: retorna dados de todas as instituicoes sem resolver entidade
- Parametro `grupo` em `ifdata.read()` para filtrar por grupo de conta
- 6 novos filtros em `cadastro.read()`: `atividade`, `tcb`, `td`, `tc`, `sr`, `municipio`
- `check_ifdata_era()` em `core/eras.py`: verificacao de era especifica para IFDATA com logica por tipo de relatorio -- detecta relatorios descontinuados (`DroppedReportWarning`) e migracoes de escopo (`ScopeMigrationWarning`)
- 8 novas classes de warning estruturadas com atributos semanticos: `PartialDataWarning`, `ScopeUnavailableWarning`, `NullValuesWarning`, `ScopeMigrationWarning`, `DroppedReportWarning`, `EmptyFilterWarning`, `TruncatedResultWarning`
- `InvalidColumnError` para colunas invalidas em `list()`
- `DateScalar` type alias: `read()`, `collect()` e demais metodos aceitam `date`, `datetime` e `pd.Timestamp` nativos alem de int/str
- `stem_ptbr()` em `utils/text.py`: stemming PT-BR para busca singular/plural -- `list_contas()` usa para matching ("operacao" encontra "Operacoes")
- `infra/sql.py` com 7 funcoes de construcao SQL: `build_string_condition`, `build_int_condition`, `build_account_condition`, `build_like_condition`, `join_conditions`, `escape_sql_string`, `build_in_clause`
- `utils/nulls.py` com `is_valid()`: check escalar de nulidade sem pandas, compativel com None, NaN, `pd.NA` e `pd.NaT`
- `format_entity_labels()` em `utils/text.py` para formatacao de CNPJs com nomes em mensagens de warning
- `NOME_CONGL_PRUD` como coluna derivada no enrichment cadastral: nome da instituicao lider do conglomerado prudencial, resolvida via lookup SQL
- `describe()` agora inclui key `"columns"` com nomes aceitos pelo `list()`
- `providers/enrichment.py` com `enrich_with_cadastro()` e `validate_cadastro_columns()`: enriquecimento cadastral extraido como modulo independente
- `TemporalResolver` e `TemporalGroup` em `valores/temporal.py`: resolucao de CNPJs para codigos IFDATA por periodo com suporte a backfill/forward-fill
- `CadastroSearch` em `cadastro/search.py`: logica de busca extraida do explorer
- `EntitySearch` em `core/entity/search.py`: busca fuzzy isolada do lookup de metadados
- Novos metodos no BaseExplorer: `_validate_escopo()`, `_validate_columns()`, `_filter_columns()`, `_storage_columns_for_query()`, `_apply_canonical_names()`, `_check_null_value_instituicoes()`, `_diagnose_empty_result()`, `_ensure_data_exists()`

### Changed

**Arquitetura:**
- Provider IFDATA decomposto em sub-packages: `ifdata/cadastro/` (collector, explorer, search) e `ifdata/valores/` (collector, explorer, temporal)
- `EntityLookup` decomposto em pacote `core/entity/`: `lookup.py` (resolucao de metadados) e `search.py` (busca fuzzy via `EntitySearch`) -- responsabilidades separadas com dependencia unidirecional
- `CadastroExplorer.search()` extraido para `CadastroSearch` em `cadastro/search.py` -- explorer reduzido de ~570 para ~290 linhas
- `BaseExplorer` movido de `core/` para `providers/` (pertence a hierarquia de providers)
- `CollectStatus` movido de `collector_models.py` para `base_collector.py`
- HTTP client migrado de `requests` para `httpx` com connection pooling via `httpx.Client`, eliminando overhead de conexao TCP por request
- `TRANSIENT_EXCEPTIONS` simplificada: `requests.*` + `urllib3.*` consolidados em `httpx.HTTPError`
- `IFDATA_API_BASE` URL centralizada em `core/constants.py` (antes duplicada nos collectors)

**Performance:**
- Pipeline de finalizacao (`_finalize_read`) simplificado: dedup, conversao datetime e exclusao de colunas movidos para o DuckDB; pipeline pos-query reduzido a rename + sort + reorder
- Queries DuckDB consolidadas por batch de periodos em reads prudencial/financeiro -- antes cada conglomerado gerava um `read_glob` separado, agora grupos com mesmos periodos sao lidos em uma unica query (~2x mais rapido)
- Cache de nomes canonicos em `EntityLookup`: queries subsequentes com mesmos CNPJs retornam do cache sem hit no DuckDB
- `_search_without_termo()` usa arrays `.values` em vez de `.iterrows()` (~18x mais rapido)
- Null checks escalares (`pd.notna`/`pd.isna`) substituidos por `is_valid()` em pure Python (~2x mais rapido para valores DuckDB)
- Enrichment cadastral migrado de `pd.merge_asof` para ASOF LEFT JOIN via DuckDB SQL
- `TemporalResolver.resolve_mapeamento()` consolidado em uma unica query SQL (antes 3 metodos + manipulacao pandas)
- `_check_null_value_instituicoes()` usa operacoes vetorizadas com sets em vez de `groupby().apply()`
- Conversao redundante `pd.to_datetime()` em `_base_list()` removida (DuckDB ja retorna `datetime64[us]`)

**Logging:**
- Filosofia redefinida: log interno reduzido ~78%, removidos logs de parsing de datas e SQL de rotina
- Reads promovidos de DEBUG para INFO com output estruturado (ex: `COSIF read: escopo=prudencial -> 301 rows`)
- `emit_user_warning()` log level rebaixado de WARNING para DEBUG (reduz ruido para warnings que ja sao emitidos via `warnings.warn()`)
- Fix: log de enrichment usava printf-style (`%d/%d`) com loguru -- corrigido para f-string

**Outros:**
- CNPJ regex corrigido de `^\d+$` para `^\d{8}$` -- codigos de conglomerado numericos curtos nao sao mais tratados como CNPJ
- `FIRST_AVAILABLE_PERIOD` ampliado: `cosif_individual` recuado para 198807, `ifdata_valores` para 200003
- `EntityLookup.real_entity_condition()` tornado `@staticmethod`
- `_apply_canonical_names()` so atua quando INSTITUICAO nao existe no DataFrame
- `list_contas()` COSIF aplica dedup via `ROW_NUMBER() OVER (PARTITION BY CONTA ORDER BY DATA_BASE DESC)` para eliminar variantes de nome entre eras
- `InstitutionList.normalize_and_validate()` reutiliza `ValidatedCnpj8` em vez de duplicar regex
- Metodos publicos padronizados para portugues: `list_periods` -> `list_periodos`, `_normalize_dates` -> `_normalize_datas`, etc.
- Documentacao atualizada em 15 arquivos refletindo nova arquitetura

### Removed

**Arquivos deletados:**
- `core/api.py` (funcionalidade absorvida por `cadastro.search()`)
- `core/base_explorer.py` (movido para `providers/base_explorer.py`)
- `core/entity_lookup.py` (decomposto em `core/entity/`)
- `domain/models.py` (`ScopeResolution` removida)
- `providers/collector_models.py` (`CollectStatus` movido para `base_collector.py`)
- `providers/ifdata/collector.py` (dividido em `cadastro/collector.py` e `valores/collector.py`)
- `providers/ifdata/explorer.py` (movido para `valores/explorer.py`)
- `providers/ifdata/cadastro_explorer.py` (movido para `cadastro/explorer.py`)

**API removida:**
- `bcb.search()` do namespace top-level
- `cadastro.info()`, `cadastro.get_conglomerate_members()`, `cadastro.list_segmentos()`, `cadastro.list_ufs()`
- `ifdata.list_institutions()`, `ifdata.list_reporters()`, `ifdata.list_reports()`, `ifdata.list_mapeamento()`
- Excecoes `EntityNotFoundError` e `AmbiguousIdentifierError`
- `ScopeResolution` dataclass e `resolve_ifdata_escopo()`
- `yyyymm_to_datetime()` em `utils/date.py`
- Dependencias `requests` e `urllib3`

---

## [0.3.0] - 2026-03-15

Release inicial com suporte a COSIF multi-era, validacao Pydantic, enrichment cadastral e suite de testes.

### Added
- Modulo `core/eras.py` para deteccao e tratamento de multiplas eras de formato do BCB (Era 1: 1995-2010/09, Era 2: 2010/10-2024/12, Era 3: 2025/01+)
- `IncompatibleEraWarning` para alertar sobre combinacao de periodos com planos COSIF diferentes
- COSIF collector com suporte a todas as eras (antes crashava em CSVs Era 1)
- `union_by_name=true` no `QueryEngine.read_glob` para leitura defensiva de parquets heterogeneos
- Validacao de cutoff dates por fonte: periodos anteriores ao primeiro disponivel no BCB sao filtrados automaticamente
- `FIRST_AVAILABLE_PERIOD` com datas empiricas por fonte
- Colunas `COD_CONTA` e `DOCUMENTO` expostas no COSIF; `COD_CONTA` no IFDATA
- Filtro por codigo de conta numerico em `cosif.read(conta=)` e `ifdata.read(conta=)`
- Parametros `documento` (COSIF), `situacao` (Cadastro), `relatorio` (IFDATA `list_accounts()`)
- Parametro `cadastro` em `cosif.read()` e `ifdata.read()` para enriquecimento inline com atributos cadastrais
- `domain/validation.py` com validators Pydantic: `NormalizedDates`, `ValidatedCnpj8`, `InstitutionList`, `AccountList`
- `infra/config.py` com `Settings` via `pydantic-settings`
- `IFDATAExplorer`: metodos de introspeccao (`list_accounts`, `list_institutions`, `list_reporters`, `list_reports`)
- `EntityLookup`: resolucao canonica de entidades com suporte a CodInst
- Suite de testes QA (64 cenarios), unitarios (151), integracao (111) -- 326 total
- Testes de contrato BCB com health check dos endpoints reais

### Changed
- NOME_CONTA normalizado para UPPER em todas as eras
- `cadastro.read()`: `start` agora opcional com fallback para ultimo periodo
- Codebase migrada de `Optional[X]`/`Union[X, Y]` para sintaxe Python 3.12+
- Suite de testes reorganizada em `tests/unit/`, `tests/integration/`, `tests/qa/`
- `BaseExplorer`: validacao delegada para Pydantic
- `BaseCollector`: downloads isolados em `temp_dir`

### Fixed
- Filtros accent-insensitive: `'Lucro Liquido'` funciona igual a versao acentuada
- DataFrames vazios retornam colunas de apresentacao
- `cosif.read()` nao retorna mais duplicatas por DOCUMENTO
- `columns=` aceita nomes de apresentacao alem de storage
- `NormalizedDates` valida range de mes (1-12)
- String de ano puro (`"2024"`) levanta erro em vez de ser interpretada como YYYYMM
- Validacao de `cadastro` movida para inicio de `read()`
- CNPJ usa `[0-9]{8}` rejeitando digitos unicode fullwidth
- `import ifdata_bcb` reduzido de ~0.65s para ~0.017s via lazy loading
