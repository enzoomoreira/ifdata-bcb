# Project Changelog

## [2026-02-04 15:36]

Centralizacao de constantes e melhorias na funcao `search()`.

### Added
- `core/constants.py`: Novo modulo centralizando constantes de fontes de dados
  - `DATA_SOURCES`: Dict com subdir, pattern e prefix de cada fonte (cadastro, ifdata_valores, cosif_individual, cosif_prudencial)
  - `TIPO_INST_MAP`: Mapeamento escopo -> codigo (individual=3, prudencial=1, financeiro=2)
  - `get_subdir()`, `get_pattern()`: Helpers para acessar configs
- `EntityLookup._get_data_sources_for_cnpjs()`: Verifica quais fontes (cosif, ifdata) tem dados para cada CNPJ
- `EntityLookup._get_latest_situacao()`: Retorna situacao mais recente de cada CNPJ (A=Ativa, I=Inativa)
- Coluna `SITUACAO` no resultado de `search()`: Indica se instituicao esta ativa ou inativa

### Changed
- `search()` agora ordena resultados por: ativas primeiro (A < I), depois por score
- `search()` agora busca apenas no cadastro (antes: union de 3 fontes)
- `search()` coluna `FONTES` agora indica onde ha dados disponiveis ('cosif', 'ifdata')
- Explorers (COSIF, IFDATA, Cadastro) agora usam constantes de `core/constants.py`
- Queries SQL usam `strip_accents()` do DuckDB para normalizar nomes

### Removed
- `EntityLookup.find_cnpj()`: Metodo removido (busca automatica de CNPJ por nome)
- `EntityNotFoundError`, `AmbiguousIdentifierError`: Exceptions removidas da API publica
- Colunas internas removidas dos resultados:
  - COSIF: `COD_CONTA` (codigo da conta)
  - IFDATA: `TIPO_INST`, `COD_CONTA`
- Barra de progresso: Tempo restante estimado removido (mantido apenas tempo decorrido)

---

## [2026-02-04 03:18]

Correcoes pos-review e suporte a multi-source no BaseExplorer.

### Added
- `BaseExplorer._get_sources()`: Metodo para explorers com multiplas fontes de dados (mesmo schema)
  - Default retorna fonte unica derivada de `_get_subdir()`/`_get_file_prefix()`
  - Override para multi-source (ex: COSIF com escopos individual/prudencial)
- `BaseExplorer._list_periods_for_source()`: Metodo auxiliar para listar periodos de uma fonte especifica
- `BaseExplorer.list_periods(source)`: Parametro opcional `source` para filtrar por fonte
- `BaseExplorer.has_data(source)`: Parametro opcional `source`
- `BaseExplorer.describe(source)`: Parametro opcional `source`, retorna info agregada + detalhes por fonte
- `COSIFExplorer._get_escopo_config()`: Metodo auxiliar para obter config de um escopo

### Changed
- `COSIFExplorer` agora implementa `_get_sources()` retornando os escopos como fontes
- `COSIFExplorer._get_subdir()` e `_get_file_prefix()` simplificados (sem parametro escopo)
- `IFDATAExplorer._read_single_scope()`: Captura `BacenAnalysisError` em vez de `Exception` generico
  - Erros de dominio (DataUnavailableError, InvalidIdentifierError) retornam None
  - Bugs reais (TypeError, KeyError) propagam normalmente

### Fixed
- `bcb.cosif.list_periods()` agora retorna periodos de AMBOS escopos (antes: apenas individual)
- `bcb.cosif.has_data()` agora verifica ambos escopos
- `bcb.cosif.describe()` agora mostra info de todas as fontes

### Architecture
- Criterio formalizado: mesmo schema + multiplas fontes = multi-source explorer
- Schemas diferentes = explorers separados (ex: IFDATA valores vs cadastro)

---

## [2026-02-04 02:32]

Refatoracao massiva do codebase (~2270 linhas novas, ~5060 linhas removidas = -2790 linhas liquidas).

### Added
- `core/` - Novo modulo central compartilhado:
  - `BaseExplorer`: Classe abstrata com metodos utilitarios para normalizacao de datas/contas/instituicoes, mapeamento de colunas (storage -> apresentacao), conversao automatica de DATA para datetime, e WHERE builders centralizados (`_build_string_condition`, `_build_int_condition`, `_build_date_condition`, `_build_cnpj_condition`, `_join_conditions`)
  - `EntityLookup`: Unifica busca e resolucao de entidades via SQL puro (exata, contains, fuzzy). Inclui `find_cnpj()`, `search()`, `resolve_ifdata_scope()`, `get_entity_identifiers()`
  - `api.py`: Funcao `search()` de alto nivel com lazy loading
- `domain/types.py` - Type aliases para parametros: `DateInput`, `AccountInput`, `InstitutionInput`
- `domain/models.py` - Dataclass `ScopeResolution` para resolucao de escopo IFDATA
- `providers/base_collector.py` - Classe base para collectors com:
  - Coleta paralela via `ThreadPoolExecutor` (4 workers)
  - Staggered delay para evitar rate limiting
  - Conexao DuckDB thread-safe via `_get_cursor()` (cursors thread-local)
  - Metodos de display (`_start`, `_end`, `_fetch_start`, `_fetch_result`, `_info`, `_warning`)
  - Normalizacao automatica de campos de texto dos CSVs
  - Retorno de status por periodo (`CollectStatus`: SUCCESS, UNAVAILABLE, FAILED)
- `infra/storage.py` - Novo metodo `save_from_query()` para salvar query DuckDB direto em Parquet (sem conversao para Pandas)
- `providers/collector_models.py` - Enum `CollectStatus`
- `providers/ifdata/cadastro_explorer.py` - Explorer dedicado para dados cadastrais com metodos `info()`, `list_segmentos()`, `list_ufs()`, `get_conglomerate_members()`
- `utils/date.py` - Funcoes de data: `normalize_date_to_int()`, `generate_month_range()`, `generate_quarter_range()`, `yyyymm_to_datetime()`
- `utils/fuzzy.py` - Classe `FuzzyMatcher` encapsulando thefuzz com thresholds configuraveis
- `utils/text.py` - `normalize_text()` (whitespace), `normalize_accents()` (unicode NFKD)
- `utils/period.py` - `parse_period_from_filename()`, `extract_periods_from_files()`, `get_latest_period()`
- `domain/exceptions.py` - Novas excecoes: `InvalidDateFormatError`, `PeriodUnavailableError`

### Changed
- `BaseExplorer`: Removidos metodos abstratos `read()` e `collect()` - cada explorer define sua propria assinatura (fix pyright override errors)
- `COSIFExplorer` e `IFDATAExplorer` agora herdam de `core/BaseExplorer` (antes: `domain/explorers.BaseExplorer`)
- Explorers usam `EntityLookup` em vez de `EntityResolver` + `EntitySearcher` separados
- `QueryEngine` simplificado: removido metodo `read()` (mantido apenas `read_glob()` e `sql()`)
- Exceptions com mensagens diretas sem docstrings verbosas
- `DataManager.save()` agora usa `mode="delta"` em `write_deltalake` por padrao
- Coleta paralela ativada em todos os collectors (antes: sequencial)
- Mapeamento de colunas movido para cada Explorer (`_COLUMN_MAP`) em vez de hardcoded
- `ifdata.collect()` e `cadastro.collect()` agora aceitam `verbose` (padronizado com COSIF)
- `cadastro.read()` agora exige `instituicao` e `start` (antes: tudo opcional)
- `cadastro.info()` agora exige `start` (antes: opcional)
- `list_accounts()` padronizado em IFDATA e COSIF: novos parametros `termo` (busca textual) e `escopo` (filtro por escopo)

### Removed
- `read_by_account_code()` removido de `IFDATAExplorer` e `COSIFExplorer` (usar `read()` com filtro `conta`)
- `services/` - Modulo inteiro removido:
  - `base_collector.py` (substituido por `providers/base_collector.py`)
  - `entity_resolver.py` (incorporado em `core/entity_lookup.py`)
  - `entity_searcher.py` (incorporado em `core/entity_lookup.py`)
- `providers/cadastro/` - Modulo removido (movido para `providers/ifdata/cadastro_explorer.py`)
- `domain/explorers.py` - Removido (substituido por `core/base_explorer.py`)
- `utils/date_utils.py`, `utils/fuzzy_matcher.py`, `utils/text_utils.py` - Removidos (substituidos por modulos mais enxutos)
- Funcao `bcb.sql()` removida da API publica
- `QueryEngine.read()` - Metodo removido (usar `read_glob()`)
- Constantes de resilience removidas dos exports (`DEFAULT_RETRY_*`, `TRANSIENT_EXCEPTIONS`)
- `CacheStats` removido dos exports de `infra/`
- Docstrings verbosas removidas de todas as classes e funcoes
- Re-exports de compatibilidade removidos de todos os `__init__.py`
- Exceptions removidas dos exports publicos: `InvalidDateRangeError`, `InvalidIdentifierError`, `InvalidScopeError`, `MissingRequiredParameterError` (ainda existem internamente)
