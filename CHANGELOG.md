# Project Changelog

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
