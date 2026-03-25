# Project Changelog

## [2026-03-24 20:55]

### Changed
- `EntityLookup` decomposto em pacote `core/entity/`: `lookup.py` (resolucao de metadados, ~300 linhas) e `search.py` (busca fuzzy via `EntitySearch`, ~200 linhas) -- responsabilidades separadas com dependencia unidirecional
- `CadastroExplorer.search()` extraido para `cadastro/search.py` (`CadastroSearch`, ~280 linhas) -- explorer reduzido de 567 para ~290 linhas, search com filtros fonte/escopo isolado em classe propria
- Bulk prudencial CNPJ resolution: regex `^\d+$` corrigido para `^\d{8}$` -- codigos de conglomerado numericos curtos (ex: "40") agora vao para o lookup de conglomerado em vez de serem tratados como CNPJ direto

### Removed
- `core/entity_lookup.py` (523 linhas) substituido pelo pacote `core/entity/` com mesma API publica via re-exports

## [2026-03-23 02:12]

### Changed
- HTTP client migrado de `requests` para `httpx` -- connection pooling via `httpx.Client` no `BaseCollector`, eliminando overhead de conexao TCP por request em coletas com dezenas de periodos
- `TRANSIENT_EXCEPTIONS` simplificada: `requests.RequestException` + `requests.ConnectionError` + `requests.Timeout` + `urllib3.exceptions.HTTPError` consolidados em `httpx.HTTPError` (que cobre toda a hierarquia de excecoes httpx)
- Testes de contrato: chamadas que verificavam apenas status code agora usam `httpx.head()` em vez de GET com `stream=True`

### Removed
- Dependencias `requests` e `urllib3` (transitiva) substituidas por `httpx>=0.28.0`

## [2026-03-22 23:19]

### Added
- `list()` generico em todos os providers (IFDATA, COSIF, Cadastro): retorna valores distintos para colunas solicitadas via `SELECT DISTINCT` no DuckDB, com filtros categoricos, truncation warning contextual e conversao automatica de DATA para datetime64
- `cadastro.search()`: busca centralizada de instituicoes com fuzzy matching (via EntityLookup), filtros `fonte=` (ifdata/cosif), `escopo=` com validacao cruzada, e listagem completa sem termo
- `ifdata.mapeamento()`: rename de `list_mapeamento()` -- acesso direto a tabela de mapeamento COD_INST <-> CNPJ_8 por escopo
- `stem_ptbr()` em `utils/text.py`: stemming PT-BR para busca singular/plural com pares atomicos e raiz minima de 4 chars
- `InvalidColumnError` e `TruncatedResultWarning` em `domain/exceptions.py`: excecoes estruturadas para o `list()` generico
- Infraestrutura de list na `BaseExplorer`: `_base_list()`, `_validate_list_columns()`, hooks `_LIST_COLUMNS`, `_BLOCKED_COLUMNS`, `_get_list_source()`, `_build_list_conditions()` para extensibilidade por provider
- `describe()` agora inclui key `"columns"` com nomes canonicos aceitos pelo `list()`
- Testes: search (25 cenarios), list (contratos por provider), stem_ptbr (16 cenarios)

### Changed
- `list_contas()` IFDATA e COSIF agora usa `stem_ptbr()` para stemming do termo de busca -- "operacao" encontra "Operacoes", "captacao" encontra "Captacoes"
- `list_contas()` COSIF aplica dedup via `ROW_NUMBER() OVER (PARTITION BY CONTA ORDER BY DATA_BASE DESC)` -- elimina variantes de nome entre eras (ex: UPPERCASE vs Titlecase)
- COSIF `list()` monta UNION ALL de escopos com coluna ESCOPO literal, verificando `has_glob` antes de incluir cada escopo
- IFDATA `list()` usa coluna ESCOPO derivada via `CASE TipoInstituicao WHEN 1 THEN 'prudencial' ...`
- Documento filter no COSIF extraido para `_build_documento_condition()` (DRY: antes duplicado em `_read_single_escopo` e `_build_list_conditions`)

### Removed
- `bcb.search()` do namespace top-level (centralizado em `cadastro.search()`)
- `core/api.py` (arquivo deletado, funcionalidade absorvida por `cadastro.search()`)
- `cadastro.info()`, `cadastro.get_conglomerate_members()` (sugar removido; usar `read()` ou `mapeamento()`)
- `ifdata.list_instituicoes()`, `cosif.list_instituicoes()` (absorvidos por `cadastro.search(fonte=...)`)
- `ifdata.list_relatorios()` (absorvido por `ifdata.list(["RELATORIO"])`)
- `cadastro.list_segmentos()`, `cadastro.list_ufs()` (absorvidos por `cadastro.list(["SEGMENTO"])`, `cadastro.list(["UF"])`)
- `ifdata.list_mapeamento()` (renomeado para `ifdata.mapeamento()`)
- Constantes orfas: `_EMPTY_INSTITUTION_COLUMNS`, `_EMPTY_INSTITUTION_COLUMNS_ALL`, `_list_instituicoes_single`

## [2026-03-22 21:02]

### Added
- `check_ifdata_era()` em `core/eras.py`: verificacao de era especifica para IFDATA Valores com logica por tipo de relatorio
  - Detecta relatorios descontinuados (ex: "por nivel de risco da operacao" apos 202412) via `DroppedReportWarning`
  - Detecta migracao de escopo em relatorios de credito (financeiro -> prudencial a partir de 202503) via `ScopeMigrationWarning`
  - Emite `IncompatibleEraWarning` apenas para relatorios com contas renumeradas (skip para credit reports e "Informacoes de Capital" que sao estaveis entre eras)
- `ScopeMigrationWarning` e `DroppedReportWarning` em `domain/exceptions.py`: warnings estruturados para os novos cenarios de era IFDATA
- `DateScalar` type alias e `DateInput` expandido para aceitar `date`, `datetime` e `pd.Timestamp` alem de int/str -- permite passar datas nativas do Python e pandas diretamente para `read()`, `collect()`, etc.
- `format_entity_labels()` extraido para `utils/text.py` para formatacao de CNPJs com nomes em mensagens de warning
- Testes para `check_ifdata_era` (25 cenarios), cache de nomes canonicos (5), `format_entity_labels` (8), novos tipos de data (12), validacao de documento COSIF (3), warnings estruturados (2)

### Changed
- `IFDATAExplorer.read()` agora usa `check_ifdata_era()` com contexto de relatorio/escopo em vez do generico `check_era_boundary()` -- warnings mais precisos e acionaveis
- `FIRST_AVAILABLE_PERIOD` ajustado: cosif_individual recuado para 198807 (era 198501), ifdata_valores recuado para 200003 (era 200303) -- amplia cobertura historica disponivel
- `emit_user_warning()` log level rebaixado de `warning` para `debug` -- reduz ruido no log para warnings que ja sao emitidos via `warnings.warn()`
- **Otimizacao de leitura prudencial/financeiro**: queries DuckDB consolidadas por batch de periodos em `_collect_resolved_groups()` -- antes cada conglomerado gerava um `read_glob` separado (~100ms cada), agora grupos com mesmos periodos sao lidos em uma unica query com `CodInst IN (...)`. Leitura prudencial ~2x mais rapida (p50: 706ms -> 466ms single, 919ms -> 450ms cross-era)
- **Cache de nomes canonicos**: `EntityLookup.get_canonical_names_for_cnpjs()` agora cacheia resultados em `_name_cache` por sessao -- queries subsequentes com mesmos CNPJs (ou subsets) retornam do cache sem hit no DuckDB (~50-100ms economizados por chamada repetida). `clear_cache()` limpa tambem este cache
- `_normalize_text_fields` usa `.map(na_action="ignore")` em vez de `.apply()` -- semanticamente correto e evita warning de depreciacao futuro
- Enrichment cadastral: CAST para VARCHAR em SQL de `NOME_CONGL_PRUD` e uso de `pd.Series(..., dtype="string")` para colunas ausentes -- evita colunas com dtype object/mixed

### Fixed
- `_parse_date_input()` agora rejeita `pd.NaT` e `None` com `InvalidDateFormatError` -- antes `pd.NaT` propagava silenciosamente (`isinstance(pd.NaT, datetime)` e `True`) e produzia `nan` downstream

## [2026-03-19 18:37]

### Changed
- Provider IFDATA decomposto em sub-packages: `ifdata/cadastro/` e `ifdata/valores/` com modulos `collector.py`, `explorer.py` e `temporal.py` dedicados (antes eram arquivos monoliticos em `ifdata/`)
- `CollectStatus` enum movido de `collector_models.py` para `base_collector.py` (colocation com `BaseCollector`)
- `ScopeResolution` dataclass movida de `domain/models.py` para `providers/ifdata/valores/scope.py` (colocation com logica de resolucao de escopo)
- `IFDATA_API_BASE` URL centralizada em `core/constants.py` (antes duplicada nos collectors)
- `EntityLookup` refatorado: metodo monolitico `_get_data_sources_for_cnpjs` decomposto em 3 metodos privados (`_check_cosif_sources`, `_check_ifdata_individual_sources`, `_check_ifdata_conglomerate_sources`); metodo `search` decomposto em `_build_search_corpus`, `_search_exact_cnpj`, `_assemble_search_results`
- `EntityLookup._latest_cadastro_sql()`: template SQL reutilizavel para queries de "linha mais recente por CNPJ" (ROW_NUMBER), eliminando duplicacao em `_get_latest_situacao`, `search` e `get_canonical_names_for_cnpjs`
- `EntityLookup._source_path()` unifica `_get_source_path()` (antes recebia subdir+pattern separados, agora recebe source_key)
- `InstitutionList.normalize_and_validate()` reutiliza `ValidatedCnpj8` em vez de duplicar regex
- `FuzzyMatcher.search()` simplificado: remove copia desnecessaria da lista de matches
- `format_entity_labels()` extraido de `BaseExplorer` e `IFDATAExplorer` para `utils/text.py` (reuso entre warning formatters)
- `Settings.cache_path` nao cria mais diretorios como side-effect; `_resolve_base_path()` extraido em `storage.py`
- `infra/__init__.py` exporta `emit_user_warning`, `DEFAULT_REQUEST_TIMEOUT`, `staggered_delay` (antes importados diretamente dos sub-modulos)
- Imports atualizados em todos os testes de integracao/unit/qa para refletir nova estrutura de packages

### Removed
- `domain/models.py` (conteudo movido para `providers/ifdata/valores/scope.py`)
- `providers/collector_models.py` (conteudo movido para `base_collector.py`)
- `providers/ifdata/collector.py` (dividido em `cadastro/collector.py` e `valores/collector.py`)
- `providers/ifdata/valores_explorer.py` (movido para `valores/explorer.py`)
- `providers/ifdata/cadastro_explorer.py` (movido para `cadastro/explorer.py`)
- `providers/ifdata/scope.py` (movido para `valores/scope.py`)
- `providers/ifdata/temporal.py` (movido para `valores/temporal.py`)
- Excecoes `EntityNotFoundError` e `AmbiguousIdentifierError` removidas da hierarquia (nao tinham call sites restantes)
- Testes de `resolve_ifdata_escopo` em `test_entity_lookup.py` (movidos junto com o modulo para sub-package valores)
- Testes de `AmbiguousIdentifierError` e `EntityNotFoundError` em `test_exceptions.py`
- Parametro `**_kwargs` de `EntityLookup.real_entity_condition()`

## [2026-03-19 14:03]

### Added
- Bulk read (sem `instituicao`) em `cosif.read()` e `ifdata.read()`: retorna dados de todas as instituicoes de uma vez, sem necessidade de resolver entidade
  - `IFDATAExplorer._read_bulk()`: leitura direta do parquet por escopo, sem resolucao temporal
  - Diagnostico diferenciado para bulk vs filtrado (`had_institution_filter` em `_diagnose_empty_result`)
- `QueryEngine.read_glob()` com novos parametros: `distinct`, `date_column`/`date_alias` (conversao YYYYMM->datetime via DuckDB), `exclude_columns` (EXCLUDE no SQL)
  - Dedup e conversao de datas agora feitos no DuckDB em vez de pandas pos-query
- `QueryEngine.sql_with_df()`: executa SQL com DataFrames registrados como tabelas virtuais (habilita JOINs, ASOF JOINs entre DataFrames em memoria)
- `BaseExplorer._read_glob()`: wrapper que injeta `distinct=True`, `date_column`, e `exclude_columns` automaticamente
- `_PASSTHROUGH_COLUMNS` em BaseExplorer: colunas nativas do parquet aceitas em `columns=` sem precisar estar em `_COLUMN_MAP`
- `_DATE_COLUMN` em BaseExplorer: declara coluna YYYYMM int para conversao automatica em datetime no DuckDB
- Coluna derivada `NOME_CONGL_PRUD` no enrichment cadastral: nome da instituicao lider do conglomerado prudencial, resolvida via lookup SQL
- Parametro `grupo` em `ifdata.read()` para filtrar por grupo de conta
- 6 novos parametros de filtro em `cadastro.read()`: `atividade`, `tcb`, `td`, `tc`, `sr`, `municipio`
- `_COLUMN_ORDER` definido em CadastroExplorer para ordenacao consistente do output
- Validacao `limit > 0` em `list_contas()` do COSIF e IFDATA
- Validacao de `documento` numerico no COSIF (levanta `InvalidScopeError` se nao-numerico)
- 3 novos modulos de teste: `test_bulk_read.py`, `test_cadastro_filters.py` (integration), `test_warnings_structured.py` (unit)
- Scripts de benchmarking e smoke test: `bench_pandas_migration.py`, `smoke_nome_congl.py`, `smoke_nome_congl_e2e.py`

### Changed
- API breaking: `instituicao` agora e keyword-only e opcional em `cosif.read()`, `ifdata.read()` e `cadastro.read()`; `start` e o primeiro argumento posicional
- Warnings estruturados: todas as classes de warning (`IncompatibleEraWarning`, `PartialDataWarning`, `ScopeUnavailableWarning`, `NullValuesWarning`, `EmptyFilterWarning`) agora carregam atributos semanticos (ex: `reason`, `entities`, `boundary`, `parameter`) alem da mensagem textual
- `emit_user_warning()` aceita instancias de `Warning` diretamente, alem de string + category
- `EntityLookup.real_entity_condition()` tornado `@staticmethod` (nao depende mais de estado de instancia); fallback legacy para caches sem CodInst removido
- `_finalize_read()` simplificado: dedup, datetime conversion e drop de colunas movidos para o DuckDB; pipeline pos-query reduzido a rename + sort + reorder
- `_apply_canonical_names()` so atua quando INSTITUICAO nao existe no DataFrame (skip lookup quando parquet ja tem os nomes)
- `_check_null_value_instituicoes()` usa operacao vetorizada com sets em vez de `groupby().apply()`; threshold de entities exibidas aumentado de 3 para 5
- Enrichment cadastral migrado de `pd.merge_asof` para ASOF LEFT JOIN via DuckDB SQL; caso data-unica usa LEFT JOIN com ROW_NUMBER
- `TemporalResolver.resolve_mapeamento()` consolidado em uma unica query SQL (antes eram 3 metodos + manipulacao pandas)
- `TemporalResolver`: iteracao com `iterrows()` substituida por arrays numpy + `zip()`
- Filtros do `cadastro.read()` consolidados em loop data-driven (dict de parametros) em vez de blocos repetitivos
- `_validate_required_params()` nao valida mais `instituicao` (agora opcional)

### Removed
- `yyyymm_to_datetime()` em `utils/date.py` (conversao de datas agora feita no DuckDB)
- `EntityLookup._cadastro_has_codinst()` e `_legacy_alias_condition()` (fallback para caches sem CodInst)
- `TemporalResolver.load_mapeamento_rows()` e `load_cadastro_entities()` (consolidados em `resolve_mapeamento()`)
- `_TIPO_INST_REVERSE` dict (lookup reverso nao mais necessario)
- `CadastroExplorer._resolve_start()` movido para `_resolve_start_fallback()` (so usado por `info()` e `get_conglomerate_members()`, nao mais por `read()`)
- Testes de fallback legacy (`TestRealEntityConditionFallback`, `test_legacy_fallback_without_codinst`)
- Testes de `yyyymm_to_datetime` (funcao removida)

## [2026-03-18 01:35]

### Added
- Modulo `infra/sql.py` com 8 funcoes utilitarias de construcao SQL extraidas do BaseExplorer: `build_string_condition`, `build_int_condition`, `build_account_condition`, `build_like_condition`, `join_conditions`, `escape_sql_string`, `build_in_clause`
  - Consolida logica SQL dispersa em metodos de instancia, habilitando reuso entre explorers
  - `build_account_condition()` suporta match dual por nome (accent/case insensitive) OU codigo numerico
  - `build_like_condition()` escapa metacaracteres SQL (`%`, `_`, `$`) automaticamente
- `TemporalResolver` em `providers/ifdata/temporal.py`: resolve CNPJs para codigos IFDATA por periodo, rastreando mudancas de conglomerado ao longo do tempo
  - `TemporalGroup` dataclass encapsula cod_inst, tipo_inst, periodos e mapeamento de CNPJs
  - Suporte a backfill/forward-fill para periodos sem correspondencia direta
- `resolve_ifdata_escopo()` extraido para `providers/ifdata/scope.py`: valida CNPJ contra escopo (individual, prudencial, financeiro) de forma isolada
- `enrich_with_cadastro()` extraido para `providers/enrichment.py`: enriquecimento cadastral com merge temporal via `merge_asof` backward-looking para series temporais
- `ValoresExplorer` (renomeado de `IFDATAExplorer`) com novos metodos de introspeccao: `list_contas()`, `list_instituicoes()`, `list_mapeamento()`, `list_relatorios()`
- 4 novos subtipos de warning: `PartialDataWarning`, `ScopeUnavailableWarning`, `NullValuesWarning`, `EmptyFilterWarning`
- Novos metodos no BaseExplorer: `_validate_escopo()`, `_validate_columns()`, `_filter_columns()`, `_storage_columns_for_query()`, `_apply_canonical_names()`, `_check_null_value_instituicoes()`, `_diagnose_empty_result()`, `_ensure_data_exists()`
- 5 novos modulos de teste: `test_sql.py`, `test_temporal.py`, `test_enrichment.py` (unit), `test_temporal_resolution.py`, `test_heterogeneous_schemas.py` (integration)

### Changed
- BaseExplorer movido de `core/base_explorer.py` para `providers/base_explorer.py` -- pertence a hierarquia de providers, nao ao core
- `IFDATAExplorer` decomposto: `explorer.py` renomeado para `valores_explorer.py`, logica de escopo extraida para `scope.py`, resolucao temporal para `temporal.py`, enriquecimento para `enrichment.py`
- Metodos publicos padronizados para portugues: `list_periods()` -> `list_periodos()`, `_normalize_dates()` -> `_normalize_datas()`, `_normalize_accounts()` -> `_normalize_contas()`, `_normalize_institutions()` -> `_normalize_instituicoes()`, `_resolve_entity()` -> `_resolve_entidade()`
- Metodos SQL migrados de instancia para funcoes de modulo em `infra/sql.py`
- `EntityLookup`: `_real_entity_condition()` e `_resolved_entity_cnpj_expr()` tornados publicos (sem underscore) e parametrizaveis por nome de coluna
- Pipeline de finalizacao `_finalize_read()` expandido para 7 etapas: drop colunas internas, mapeamento, dedup, conversao DATA->datetime, sort, reordenacao, reset index
- Documentacao atualizada em 11 arquivos refletindo nova arquitetura modular

### Removed
- `src/ifdata_bcb/core/base_explorer.py` (movido para providers)
- `src/ifdata_bcb/providers/ifdata/explorer.py` (substituido por `valores_explorer.py`)

## [2026-03-15 17:58]

### Added
- Modulo `core/eras.py` para deteccao e tratamento de multiplas eras de formato do BCB (Era 1: 1995-2010/09, Era 2: 2010/10-2024/12, Era 3: 2025/01+)
  - `detect_cosif_csv_era()`: identifica era do CSV pelo header (8 colunas vs 11 colunas)
  - `build_cosif_select()`: gera SQL normalizado por era, produzindo schema uniforme independente do formato de origem
  - `check_era_boundary()`: emite warning quando query abrange periodos com codigos de conta incompativeis
- `IncompatibleEraWarning` em `domain/exceptions.py` para alertar usuarios sobre combinacao de periodos com planos COSIF diferentes (pre/pos COSIF 1.5)
- COSIF collector agora suporta todas as eras (antes crashava em CSVs Era 1 com `BinderException: "#DATA_BASE" not found`)
- `union_by_name=true` no `QueryEngine.read_glob` para leitura defensiva de parquets com schemas heterogeneos
- `_download_single` movido para `BaseCollector`, eliminando duplicacao nos collectors IFDATA
- 46 novos testes: `test_eras.py` (27), `test_cosif_collector_eras.py` (13), `test_query_engine_union.py` (6)

### Changed
- NOME_CONTA normalizado para UPPER em todas as eras (Era 3 vinha em Title Case)
- Contrato de `_process_to_parquet` atualizado: parametro renomeado de `csv_path` para `data_path` (IFDATA Valores passa diretorio, nao arquivo)
- `base_explorer.py`: conversao de DATA usa `pd.to_datetime` + `MonthEnd` em vez de funcao custom `yyyymm_to_datetime`
- `entity_lookup.py`: iteracao por DataFrame substituida por operacoes vetorizadas com `zip()` sobre arrays numpy

### Removed
- Parametro `threshold_auto` de `FuzzyMatcher` (nao era utilizado)
- Parametro `fuzzy_threshold_auto` de `EntityLookup.__init__`
- Metodos `_download_single` duplicados em `IFDATAValoresCollector` e `IFDATACadastroCollector`

## [2026-03-15 16:51]

### Added
- Validacao de cutoff dates por fonte: periodos anteriores ao primeiro disponivel no BCB sao filtrados automaticamente, evitando centenas de requests 404 com retry desnecessarios
  - Registry `FIRST_AVAILABLE_PERIOD` com datas empiricas: COSIF Individual >=199501, COSIF Prudencial >=201407, IFDATA Valores >=200303, Cadastro >=200503
  - `_filter_by_availability()` no BaseCollector integrado ao `_generate_periods()`
- Colunas `COD_CONTA` e `DOCUMENTO` agora expostas no COSIF (antes eram dropadas internamente)
- Coluna `COD_CONTA` exposta no IFDATA Valores
- Filtro por codigo de conta (numerico) alem de nome em `cosif.read(conta=)` e `ifdata.read(conta=)`
- Parametro `documento` em `cosif.read()` para filtrar por tipo de documento (balancete/semestral)
- Parametro `situacao` em `cadastro.read()` para filtrar por situacao da instituicao (A/I)
- Parametro `relatorio` em `ifdata.list_accounts()` para filtrar contas por relatorio
- Colunas `RELATORIO` e `GRUPO` retornadas por `ifdata.list_accounts()`
- Suite de testes para providers: `test_resilience.py` (retry/backoff), `test_cosif_collector.py` (parse CSV COSIF), `test_ifdata_collector.py` (parse CSV IFDATA)
- Testes de contrato BCB (`tests/contract/`) com 8 testes de health check dos endpoints reais, marcados `@pytest.mark.contract` e excluidos do CI

### Changed
- `AccountList` validator agora aceita inputs nao-iteraveis (converte para string)
- Fuzzy matching: threshold de sugestao ajustado de 70 para 78 para reduzir falsos positivos
- `ifdata.list_accounts()` retorna resultados ordenados por RELATORIO, GRUPO, CONTA

### Fixed
- Coleta de periodos anteriores a disponibilidade da fonte no BCB gerava centenas de requests 404 com retries exponenciais, tornando coletas com range amplo extremamente lentas

## [2026-03-15 13:51]

### Added
- Suite de testes QA com 64 cenarios adversariais simulando usuarios reais: inputs invalidos (CNPJ, datas, escopos), edge cases de dados (NaN, Inf, strings 10k chars, parquet corrompido), concorrencia (20 reads simultaneos), e experiencia de primeiro uso (lazy loading, cache vazio, excecoes)
- 16 testes unitarios para validators Pydantic (`NormalizedDates`, `ValidatedCnpj8`, `InstitutionList`, `AccountList`)
- `ruff format --check` no CI, garantindo formatacao consistente em PRs

### Fixed
- Validacao de CNPJ agora usa `[0-9]{8}` em vez de `\d{8}`, rejeitando digitos unicode fullwidth (U+FF10-U+FF19) que passavam silenciosamente pela regex anterior
- `import ifdata_bcb` era lento (~0.65s) por carregar pandas/duckdb eagerly via `domain/__init__.py` e import direto de `search`; agora `search` e lazy via `__getattr__` e `domain/__init__.py` foi esvaziado (import cai para ~0.017s)
- `dir(ifdata_bcb)` agora expoe atributos lazy (`cosif`, `ifdata`, `cadastro`, `search`) via `__dir__()`
- Mensagens de erro do `EntityLookup` agora sugerem `cadastro.collect()` quando dados cadastrais estao ausentes

### Changed
- Toda a codebase migrada de `Optional[X]`/`Union[X, Y]` para sintaxe Python 3.12+ (`X | None`, `X | Y`) -- ~74 ocorrencias em codigo + documentacao
- Type hints refinados: `TypedDict` para config COSIF, `Sequence` para covariancia em `_join_conditions`, `cast()` para iteracao de `Literal`, `assert` para narrowing de `None`
- Suite de testes reorganizada de estrutura flat para `tests/unit/`, `tests/integration/`, `tests/qa/` (151 + 111 + 64 = 326 testes)
- `test_integration.py` dividido em 4 arquivos focados: `test_cosif.py`, `test_ifdata.py`, `test_cadastro.py`, `test_query_engine.py`
- Documentacao atualizada em 11 arquivos para refletir nova sintaxe de tipos, lazy loading de `search`, e regex CNPJ corrigida

## [2026-03-15 03:00]

### Fixed
- `NormalizedDates` agora valida range de mes (1-12), rejeitando inputs como `202413` ou `202400` que antes eram aceitos silenciosamente e resultavam em queries vazias sem erro informativo
  - Eliminada duplicacao de parsing: validator agora delega para `normalize_date_to_int()` (fonte unica de verdade para conversao de datas)
- String de ano puro (`"2024"`) agora levanta `InvalidDateFormatError` em vez de ser interpretada como YYYYMM=2024 (ano 20, mes 24)
- Parametro `cadastro` com colunas invalidas agora levanta `InvalidScopeError` mesmo quando o resultado da query e vazio
  - Validacao movida para o inicio de `read()` (antes do query), garantindo feedback imediato independente dos dados

## [2026-03-15 02:12]

### Added
- Parametro `cadastro` em `cosif.read()` e `ifdata.read()` para enriquecer dados financeiros com atributos cadastrais inline (ex: `cadastro=["TCB", "SEGMENTO", "UF"]`)
  - Elimina o pattern manual de ler cadastro separado e fazer merge por CNPJ
  - Alinhamento temporal automatico: dados mensais COSIF recebem cadastro do trimestre mais recente via `merge_asof` backward-looking
  - Dados trimestrais IFDATA fazem merge exato por DATA + CNPJ_8
  - Colunas cadastrais invalidas levantam `InvalidScopeError` (consistente com hierarquia de excecoes da lib)
- `openpyxl` como dev dependency para exportacao Excel em scripts

## [2026-03-13 19:48]

Refatoracao significativa focada em validacao com Pydantic, configuracao centralizada, e melhoria da resolucao de entidades no IFDATA.

Correcao de 6 bugs de usabilidade nas APIs publicas e flexibilizacao do Cadastro.

### Fixed
- Filtros de `conta`, `segmento`, `relatorio` agora sao accent-insensitive (`'Lucro Liquido'` funciona igual a `'Lucro Liquido'` com acento)
  - `_build_string_condition` aceita `accent_insensitive=True` usando `strip_accents()` do DuckDB
  - `list_accounts()` do COSIF e IFDATA tambem usam accent-insensitive no LIKE
- DataFrames vazios agora retornam colunas de apresentacao (DATA, INSTITUICAO) em vez de nomes de storage (DATA_BASE, NomeInstituicao)
- `cosif.read()` nao retorna mais linhas duplicadas causadas pelo campo DOCUMENTO (balancete vs semestral)
  - `_finalize_read` base agora chama `drop_duplicates()` apos mapeamento de colunas
- Parametro `columns` aceita nomes de apresentacao (DATA, VALOR) alem de nomes de storage (DATA_BASE, SALDO)
  - Novo metodo `_translate_columns()` no BaseExplorer traduz antes de passar ao DuckDB
- `search('')` retorna DataFrame vazio sem warning do thefuzz
- `search(limit=0)` e `search(limit=-1)` agora levantam `ValueError`

### Added
- `domain/validation.py`: Validadores Pydantic para datas, CNPJs, instituicoes e contas (`NormalizedDates`, `ValidatedCnpj8`, `InstitutionList`, `AccountList`)
- `infra/paths.py`: Utilitarios `ensure_dir` e `temp_dir` para gerenciamento seguro de diretorios temporarios
- `domain/exceptions.py`: Nova excecao `DataProcessingError` para falhas de processamento de fontes
- `infra/config.py`: Classe `Settings` baseada em `pydantic-settings` substituindo funcoes avulsas de configuracao
- `IFDATAExplorer`: Novos metodos de introspeccao (`list_accounts`, `list_institutions`, `list_reporters`, `list_reports`) e validacao de escopo
- `EntityLookup`: Resolucao canonica de entidades com suporte a CodInst e heuristica para caches legados
- Suite de testes formal em `tests/` com 11 modulos cobrindo core, domain, infra e providers
- `pyproject.toml`: Dependencias `pydantic` e `pydantic-settings`; grupo dev com `pytest`; configuracao pytest

### Changed
- `cadastro.read()`, `cadastro.info()`, `cadastro.get_conglomerate_members()`: `start` agora e opcional (default: ultimo periodo disponivel)
  - Novo metodo `_resolve_start()` no CadastroExplorer
  - Novo metodo `_get_latest_period()` no BaseExplorer
- `BaseExplorer`: Validacao de inputs delegada para modelos Pydantic em vez de regex manual; novo metodo `_align_to_quarter_end`
- `BaseCollector`: Downloads isolados em `temp_dir` context manager; `_normalize_text_fields` agora opera em todas as colunas object
- `BaseCollector._download_period`: Assinatura atualizada para receber `work_dir` explicitamente
- `infra/log.py`: Logging de arquivo usa `get_settings().logs_path` com fallback silencioso para ambientes restritos
- `infra/__init__.py`: Exports atualizados (`Settings`, `get_settings`, `ensure_dir`, `temp_dir` substituem `get_cache_path`)
- `IFDATAExplorer._add_institution_names`: Usa `get_canonical_names_for_cnpjs` para nomes canonicos
- Documentacao atualizada em todos os docs para refletir nova arquitetura (README, getting-started, internals, providers)
- `notebooks/quickstart.ipynb`: Exemplos atualizados para nova API

### Removed
- `CHANGELOG.md` anterior (substituido por esta versao atualizada)
- `EntityLookup._build_entity_union_sql`: Substituido por resolucao canonica via cadastro com CodInst
- `get_cache_path` e `get_logs_path`: Substituidos pela classe `Settings`
- Lista fixa de colunas de texto em `_normalize_text_fields`
