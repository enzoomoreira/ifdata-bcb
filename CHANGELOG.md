# Project Changelog

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
