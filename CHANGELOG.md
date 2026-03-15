# Project Changelog

## [2026-02-03 21:21]
### Changed
- **BREAKING**: Separacao de responsabilidades entre Collectors e Explorers
  - Collectors agora salvam dados com nomes de storage (schema raw)
  - Explorers aplicam mapeamento de colunas para apresentacao
- **Schema Parquet alterado** - Cache existente incompativel, necessario re-coletar

#### Nomes de Storage (Parquet) -> Apresentacao (API):
- **COSIF**: `DATA_BASE->DATA`, `NOME_INSTITUICAO->INSTITUICAO`, `CONTA->COD_CONTA`, `NOME_CONTA->CONTA`, `SALDO->VALOR`
- **IFDATA Valores**: `AnoMes->DATA`, `CodInst->COD_INST`, `TipoInstituicao->TIPO_INST`, `Conta->COD_CONTA`, `NomeColuna->CONTA`, `Saldo->VALOR`, `NomeRelatorio->RELATORIO`, `Grupo->GRUPO`
- **Cadastro**: `Data->DATA`, `NomeInstituicao->INSTITUICAO`, `SegmentoTb->SEGMENTO`, etc.

### Added
- `_COLUMN_MAP` no BaseExplorer para definir mapeamentos storage->apresentacao
- `_storage_col()` para traduzir nomes de apresentacao para storage em queries
- `_apply_column_mapping()` para aplicar renomeacoes automaticamente
- `_reverse_column_map` property para traducao reversa

### Technical
- Collectors fazem menos transformacoes (mais rapido)
- Cache mais estavel (schema nao muda com API)
- Flexibilidade para mudar nomes de apresentacao sem re-coletar dados

### Migration Guide
**Obrigatorio limpar cache e re-coletar** (schema Parquet mudou):
```powershell
Remove-Item -Recurse -Force "$env:LOCALAPPDATA\py-bacen\Cache\*"
```

## [2026-02-03 21:30]
### Added
- Funcao `normalize_accents()` em `text_utils.py` para busca insensivel a acentos
- Metodo `_reorder_cosif_columns()` no COSIFExplorer para ordenacao padronizada de colunas

### Changed
- **BREAKING**: Colunas renomeadas para nomes mais curtos:
  - `NOME_INSTITUICAO` -> `INSTITUICAO`
  - `NOME_CONTA` -> `CONTA`
  - `NOME_RELATORIO` -> `RELATORIO`
- **BREAKING**: Parametros renomeados para singular (aceita str ou lista):
  - `instituicoes` -> `instituicao`
  - `contas` -> `conta`
- **BREAKING**: `get_institution_info()` renomeado para `info()` no CadastroExplorer
- Busca `bcb.search()` agora e insensivel a acentos (`itau` encontra "ITAU UNIBANCO")
- Ordem de colunas padronizada: `DATA, CNPJ_8, INSTITUICAO, ESCOPO, ...` (ESCOPO logo apos INSTITUICAO)
- Parametro `start` agora e opcional em `bcb.cadastro.info()` (retorna periodo mais recente se omitido)
- Valores string "null" nos dados agora sao convertidos para `None`/`NaN`

### Fixed
- Strings "null" em colunas como GRUPO e ATIVIDADE agora sao `None` em vez de string literal

### Migration Guide
Apos atualizar para esta versao:
1. Renomear parametros: `instituicoes` -> `instituicao`, `contas` -> `conta`
2. Renomear colunas no codigo: `NOME_INSTITUICAO` -> `INSTITUICAO`, `NOME_CONTA` -> `CONTA`, `NOME_RELATORIO` -> `RELATORIO`
3. Renomear metodo: `get_institution_info()` -> `info()`
4. **Re-coletar dados**: Os arquivos Parquet existentes terao schema antigo. Limpe o cache e re-colete:
   ```powershell
   Remove-Item -Recurse -Force "$env:LOCALAPPDATA\py-bacen\Cache\*"
   ```

## [2026-02-03 19:33]
### Added
- Novas excecoes `MissingRequiredParameterError` e `InvalidDateRangeError` para validacao de parametros
- Coluna `ESCOPO` nos resultados de `cosif.read()` e `ifdata.read()`
- Coluna `NOME_INSTITUICAO` nos resultados de `ifdata.read()` e `ifdata.list_institutions()`
- Metodo `get_names_for_cnpjs()` no EntityResolver para lookup eficiente de nomes
- Metodo helper `_build_string_condition()` no BaseExplorer para filtros SQL case-insensitive

### Changed
- **BREAKING**: `instituicoes` e `start` agora sao OBRIGATORIOS em `read()`, `read_by_account_code()`, `get_institution_info()`, `get_conglomerate_members()`
- **BREAKING**: `escopo=None` agora busca em TODOS os escopos disponiveis (antes era obrigatorio especificar)
- **BREAKING**: Coluna `NOME` renomeada para `NOME_INSTITUICAO` nos resultados de `search()` e `list_all()`
- **BREAKING**: Coluna `CNPJ_ORIGINAL` renomeada para `CNPJ_8` nos resultados de `ifdata.read()`
- **BREAKING**: `list_institutions()` e `list_reports()` agora usam `start`/`end` ao inves de `datas`
- Filtros de contas agora sao case-insensitive (nao precisa mais digitar exatamente igual)
- Filtros de segmento e UF no Cadastro agora usam helper centralizado
- Ordem dos parametros em `read()` padronizada: `instituicoes`, `start`, `end`, `contas`, ...
- Validacao de range de datas: `start > end` levanta `InvalidDateRangeError`
- Colunas do IFDATA reordenadas: DATA, CNPJ_8, NOME_INSTITUICAO, ESCOPO primeiro

### Removed
- Parametro `tipo_inst` removido de `ifdata.read()` (use `escopo` para definir tipo)
- Parametro `contas` removido de `cadastro.read()` (nao fazia sentido para cadastro)
- Logica de "mais recente" removida de `get_conglomerate_members()` (agora `start` e obrigatorio)

## [2026-02-01 18:13]
### Changed
- **BREAKING**: API de datas simplificada - `start` sozinho filtra data unica, `start`+`end` gera range
- Documentacao atualizada (README, docstrings, notebook) para nova API de datas
- `CacheStats` agora e thread-safe com `threading.Lock` em todas as operacoes

### Removed
- Parametro `datas` removido de todos os metodos `read()` e `read_by_account_code()`

## [2026-02-01 18:00]
### Added
- Novo sistema de cache centralizado com metricas (`infra/cache.py`)
- Utilitario para normalizacao de texto (`utils/text_utils.py`)
- Suporte a multiplas instituicoes nas queries (`instituicoes` aceita str ou lista)
- Parametros `start` e `end` para gerar range de datas automaticamente
- Metodo `list_reports()` no IFDATAExplorer para listar relatorios disponiveis
- Parametro `relatorio` no `ifdata.read()` para filtrar por relatorio especifico
- Parametro `escopo` no IFDATAExplorer ('individual', 'prudencial', 'financeiro')
- Coluna `CNPJ_ORIGINAL` no resultado quando `escopo` usado com multiplas instituicoes
- `ScopeResolution` dataclass para resolucao de escopo IFDATA
- `PeriodUnavailableError` para indicar periodos nao publicados no BCB
- `CollectStatus` enum para status de coleta (SUCCESS, UNAVAILABLE, FAILED)
- Metodo `resolve_ifdata_scope()` no EntityResolver
- Normalizacao automatica de campos de texto nos CSVs do BCB (remove newlines/espacos)
- Suporte a `COD_CONGL_FIN` (conglomerado financeiro) no EntityResolver

### Changed
- API unificada: parametro `identificador` renomeado para `instituicoes` em todos os explorers
- **BREAKING**: Parametro `datas` removido do `read()`. Use `start` para data unica ou `start`+`end` para range
- COSIF gera range mensal, IFDATA/Cadastro geram range trimestral automaticamente
- `CacheStats` agora e thread-safe (adiciona `threading.Lock`)
- Timeout de requisicoes aumentado de 120s para 240s
- Logs de retry/falhas reduzidos para DEBUG (evita poluir terminal do usuario)
- Banner de conclusao agora mostra periodos indisponiveis separadamente
- Cor do banner de conclusao: verde (OK), amarelo (parcial), vermelho (falha total)
- Coleta retorna tupla com 4 valores: `(registros, periodos_ok, falhas, indisponiveis)`
- `get_institution_info()` usa parametro `instituicao` ao inves de `identificador`
- Cache do EntityResolver usa decorator `@cached` centralizado ao inves de `@lru_cache`

### Fixed
- Tratamento especifico para 404 (periodo indisponivel) vs erros de rede
- Diferenciacao entre "periodo sem dados" e "erro de download" nos coletores
