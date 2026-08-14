# Modulo Core

O modulo `core` concentra a logica central compartilhada entre todos os providers.

## Localizacao

```
src/ifdata_bcb/core/
|-- __init__.py           # Exports publicos (EntityLookup, EntitySearch)
|-- entity/              # Resolucao e busca de entidades
|   |-- __init__.py      # Re-exports: EntityLookup, EntitySearch
|   |-- lookup.py        # EntityLookup (metadados, source checking, canonical names)
|   +-- search.py        # EntitySearch (fuzzy matching, corpus building)
|-- constants.py         # Configuracoes centralizadas
+-- eras.py              # Deteccao e tratamento de eras de formato BCB
```

> **Nota:** `BaseExplorer` foi movido para `providers/base_explorer.py`.

## constants.py

### IFDATA_API_BASE

URL base da API IFDATA (OData), centralizada para uso pelos collectors:

```python
IFDATA_API_BASE = "https://olinda.bcb.gov.br/olinda/servico/IFDATA/versao/v1/odata"
```

### TIPO_INST_MAP

Mapeamento entre escopo e codigo TipoInstituicao do IFDATA:

```python
TIPO_INST_MAP: dict[str, int] = {
    "individual": 3,  # Instituicao individual
    "prudencial": 1,  # Conglomerado prudencial
    "financeiro": 2,  # Conglomerado financeiro
}
```

### DATA_SOURCES

Configuracao das fontes de dados:

```python
DATA_SOURCES: dict[str, dict[str, str]] = {
    "cadastro": {
        "subdir": "ifdata/cadastro",
        "prefix": "ifdata_cad",
    },
    "ifdata_valores": {
        "subdir": "ifdata/valores",
        "prefix": "ifdata_val",
    },
    "cosif_individual": {
        "subdir": "cosif/individual",
        "prefix": "cosif_ind",
    },
    "cosif_prudencial": {
        "subdir": "cosif/prudencial",
        "prefix": "cosif_prud",
    },
}
```

### FIRST_AVAILABLE_PERIOD

Primeiro periodo disponivel por fonte (YYYYMM). Periodos anteriores retornam 404 no BCB:

```python
FIRST_AVAILABLE_PERIOD: dict[str, int] = {
    "cosif_individual": 198807,
    "cosif_prudencial": 201407,
    "ifdata_valores": 200003,
    "cadastro": 200503,
}
```

### Funcoes Auxiliares

```python
def get_pattern(source: str) -> str:
    """Retorna glob pattern para arquivos da fonte."""
    # get_pattern("cosif_individual") -> "cosif_ind_*.parquet"


def get_subdir(source: str) -> str:
    """Retorna subdiretorio da fonte."""
    # get_subdir("cosif_individual") -> "cosif/individual"


def get_source_key(prefix: str) -> str | None:
    """Reverse lookup: prefix -> source key."""
    # get_source_key("cosif_ind") -> "cosif_individual"


def get_first_available(prefix: str) -> int | None:
    """Retorna primeiro periodo disponivel para um prefix."""
    # get_first_available("cosif_prud") -> 201407
```

---

## BaseExplorer (providers/base_explorer.py)

### Responsabilidades

O `BaseExplorer` e a classe base abstrata para todos os explorers:

1. **Normalizacao**: Padroniza formatos de entrada (datas, CNPJs, contas)
2. **Validacao**: Verifica parametros obrigatorios e formatos
3. **SQL Building**: Constroi clausulas WHERE dinamicamente (funcoes em `infra.sql`)
4. **Mapeamento**: Traduz nomes de colunas (storage -> apresentacao lowercase)
5. **Finalizacao**: Rename/sort/reorder e, no ultimo passo de read(), move a
   coluna `data` para um `DatetimeIndex` chamado `date` (`_to_datetime_index`)

### Propriedades de Classe

Subclasses definem estas propriedades:

```python
class COSIFExplorer(BaseExplorer):
    # Mapeamento: nome_storage -> nome_apresentacao (lowercase)
    # Toda coluna do parquet que aparece no output esta aqui, inclusive as
    # que so mudam de caixa (CNPJ_8 -> cnpj_8, DOCUMENTO -> documento).
    _COLUMN_MAP = {
        "DATA_BASE": "data",
        "CNPJ_8": "cnpj_8",
        "NOME_INSTITUICAO": "instituicao",
        "NOME_CONTA": "conta",
        "CONTA": "cod_conta",
        "DOCUMENTO": "documento",
        "SALDO": "valor",
    }

    # Colunas adicionadas pos-query por Python (nao existem no Parquet)
    _DERIVED_COLUMNS: set[str] = {"escopo"}

    # Coluna YYYYMM int para conversao automatica em datetime no DuckDB
    _DATE_COLUMN = "DATA_BASE"

    # Colunas a remover do resultado
    _DROP_COLUMNS: list[str] = []

    # Ordem das colunas no resultado
    _COLUMN_ORDER = [
        "data",
        "cnpj_8",
        "instituicao",
        "escopo",
        "cod_conta",
        "conta",
        ...,
    ]

    # Escopos validos para _validate_escopo()
    _VALID_ESCOPOS = ["individual", "prudencial"]

    # list_values(): chave lowercase -> expressao SQL de storage
    _LIST_COLUMNS = {
        "data": "DATA_BASE",
        "escopo": "ESCOPO",
        "documento": "DOCUMENTO",
    }

    # list_values(): colunas recusadas, com mensagem apontando a alternativa
    _BLOCKED_COLUMNS = {
        "conta": "Use list_contas(termo='...') para buscar contas.",
        ...: ...,
    }
```

### Construtor

```python
def __init__(
    self,
    query_engine: QueryEngine | None = None,
    entity_lookup: EntityLookup | None = None,
):
    self._qe = query_engine or QueryEngine()
    self._resolver = entity_lookup or EntityLookup(query_engine=self._qe)
```

- Permite injecao de dependencias para testes
- Compartilha QueryEngine com EntityLookup

### Metodos Abstratos

Subclasses **devem** implementar:

```python
@abstractmethod
def _get_subdir(self) -> str:
    """Retorna subdiretorio dos dados (ex: 'cosif/individual')."""


@abstractmethod
def _get_file_prefix(self) -> str:
    """Retorna prefixo dos arquivos (ex: 'cosif_ind')."""
```

### Metodo para Multi-Source

Para explorers com multiplas fontes (ex: COSIF com individual + prudencial):

```python
def _get_sources(self) -> dict[str, dict[str, str]]:
    """
    Default: retorna fonte unica baseada em _get_subdir/_get_file_prefix.
    Override para multi-source.
    """
    return {
        "default": {
            "subdir": self._get_subdir(),
            "prefix": self._get_file_prefix(),
        }
    }
```

Exemplo de override em COSIFExplorer:

```python
_ESCOPOS = {
    "individual": {"subdir": "cosif/individual", "prefix": "cosif_ind"},
    "prudencial": {"subdir": "cosif/prudencial", "prefix": "cosif_prud"},
}


def _get_sources(self):
    return self._ESCOPOS
```

Explorers com escopos implementam tambem o hook `_periodos_por_escopo()`,
que responde `list_periodos(escopo=...)`, `has_data(escopo=...)` e
`describe(escopo=...)`:

```python
def _periodos_por_escopo(self) -> dict[str, list[int]]:
    """Mapa escopo -> periodos disponiveis. Override onde ha escopos."""
```

- **COSIF**: escopos coincidem com as fontes de armazenamento; resolve por
  `_list_periodos_for_source()` de cada uma.
- **IFDATA**: escopo e coluna dos dados (`TipoInstituicao`), nao fonte;
  resolve via query `DISTINCT AnoMes, escopo` sobre o parquet.
- Explorer sem escopos (`_VALID_ESCOPOS` vazio): `escopo=` e rejeitado com
  `InvalidScopeError` (via `_require_escopo()`).

### _read_glob() (wrapper)

Wrapper que injeta `distinct=True`, `date_column` e `exclude_columns` automaticamente ao delegar para `QueryEngine.read_glob()`:

```python
def _read_glob(
    self,
    pattern: str,
    subdir: str,
    columns: list[str] | None = None,
    where: str | None = None,
) -> pd.DataFrame:
    """
    Le parquets via DuckDB com dedup, datetime e exclude automaticos.

    - distinct=True: dedup no DuckDB (em vez de drop_duplicates pos-query)
    - date_column: usa _DATE_COLUMN da subclasse para conversao YYYYMM->datetime
    - exclude_columns: usa _DROP_COLUMNS para excluir colunas internas
    """
```

### Metodos de Normalizacao

#### _normalize_datas()

```python
def _normalize_datas(self, datas: DateInput) -> list[int]:
    """
    Normaliza datas para lista de inteiros YYYYMM.

    Aceita:
    - int: 202412
    - str: '202412', '2024-12'
    - list[int | str]: [202401, '2024-02']

    Retorna: [202412], [202401, 202402]
    """
```

#### _normalize_contas()

```python
def _normalize_contas(self, contas: AccountInput | None) -> list[str] | None:
    """
    Normaliza contas para lista de strings.

    Aceita:
    - str: 'TOTAL DO ATIVO'
    - list[str]: ['TOTAL DO ATIVO', 'PATRIMONIO LIQUIDO']
    - None

    Retorna: ['TOTAL DO ATIVO'], [...], None
    """
```

#### _normalize_instituicoes()

```python
def _normalize_instituicoes(
    self, instituicoes: InstitutionInput | None
) -> list[str] | None:
    """
    Normaliza instituicoes para lista de CNPJs validados.
    Delega validacao para normalize_institutions().
    """
```

### Metodos de Validacao

Normalizacao e validacao de inputs sao delegadas para as funcoes de `domain/validation.py`:
- `normalize_dates()`: Normaliza DateInput -> list[int]
- `validate_cnpj8()`: Valida CNPJ (base-8 ou 14 digitos com DV) -> base de 8
- `normalize_institutions()`: Normaliza e valida lista de CNPJs
- `normalize_accounts()`: Normaliza lista de contas

#### _resolve_entidade()

```python
def _resolve_entidade(self, identificador: str) -> str:
    """
    Valida CNPJ e normaliza para a base de 8 digitos.
    Delega para validate_cnpj8().

    Raises:
        InvalidIdentifierError: Se nao for CNPJ valido
    """
```

#### _validate_required_params()

```python
def _validate_required_params(
    self,
    start: str | None,
) -> None:
    """
    Valida parametros obrigatorios.

    Raises:
        MissingRequiredParameterError: Se faltar start
    """
```

**Nota**: `instituicao` nao e mais validado aqui, pois agora e opcional em todos os explorers.

#### _validate_escopo()

```python
def _validate_escopo(self, escopo: str) -> str:
    """
    Valida e normaliza nome de escopo.

    Raises:
        InvalidScopeError: Se escopo nao estiver em _VALID_ESCOPOS
    """
```

#### _validate_columns()

```python
def _validate_columns(self, columns: list[str] | None) -> list[str] | None:
    """
    Valida nomes de colunas contra o conjunto conhecido.
    Emite EmptyFilterWarning se columns=[].

    Raises:
        InvalidColumnError: Se coluna desconhecida
    """
```

#### _filter_columns()

```python
def _filter_columns(self, df: pd.DataFrame, columns: list[str] | None) -> pd.DataFrame:
    """Filtra DataFrame para conter apenas as colunas solicitadas."""
```

#### _storage_columns_for_query()

```python
def _storage_columns_for_query(
    self,
    columns: list[str] | None,
    required: list[str] | None = None,
) -> list[str] | None:
    """
    Traduz colunas para storage, filtrando derivadas e garantindo required.
    Usado antes de read_glob() para montar lista de colunas eficiente.
    """
```

#### _apply_canonical_names()

```python
def _apply_canonical_names(self, df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica nomes canonicos do cadastro a coluna instituicao.
    Se instituicao ja existe no DataFrame, retorna sem alteracao
    (nomes do parquet sao mantidos). Caso contrario, resolve
    nomes a partir de cnpj_8 via EntityLookup.
    """
```

#### _diagnose_empty_result()

```python
def _diagnose_empty_result(
    self,
    source_name: str,
    has_files: bool,
    had_conta_filter: bool,
    had_institution_filter: bool = True,
) -> None:
    """Cascata de diagnostico quando read() retorna vazio. Emite PartialDataWarning.
    had_institution_filter diferencia diagnostico entre bulk e filtrado."""
```

#### _ensure_data_exists()

```python
def _ensure_data_exists(
    self,
    pattern: str | None = None,
    subdir: str | None = None,
) -> bool:
    """Retorna True se existem arquivos parquet para o pattern."""
```

#### _resolve_date_range()

```python
def _resolve_date_range(
    self,
    start: str | None,
    end: str | None,
    trimestral: bool = False,
) -> list[int] | None:
    """
    Resolve range de datas.

    - None, None -> None (todos os periodos)
    - start, None -> [start_int] (data unica)
    - start, end -> generate_month_range ou generate_quarter_range

    Raises:
        InvalidDateRangeError: Se start > end
    """
```

### Funcoes de Construcao SQL (infra.sql)

As funcoes de construcao SQL foram extraidas para `infra.sql` como funcoes de modulo:

```python
from ifdata_bcb.infra.sql import (
    build_string_condition,
    build_int_condition,
    join_conditions,
)
```

#### build_string_condition()

```python
def build_string_condition(
    column: str,
    values: list[str],
    case_insensitive: bool = False,
    accent_insensitive: bool = False,
) -> str:
    """
    Constroi clausula WHERE para strings.

    Exemplos:
    - ["valor"] -> "COLUNA = 'valor'"
    - ["a", "b"] -> "COLUNA IN ('a', 'b')"
    - case_insensitive=True -> "UPPER(COLUNA) IN ('A', 'B')"
    - accent_insensitive=True -> "strip_accents(COLUNA) = 'valor'"
    - ambos -> "UPPER(strip_accents(COLUNA)) IN ('A', 'B')"
    """
```

#### _translate_columns()

```python
def _translate_columns(self, columns: list[str] | None) -> list[str] | None:
    """Traduz nomes de apresentacao para storage. Aceita ambos."""
```

#### build_int_condition()

```python
def build_int_condition(column: str, values: list[int]) -> str:
    """
    Constroi clausula WHERE para inteiros.

    Exemplos:
    - [202412] -> "DATA = 202412"
    - [202412, 202501] -> "DATA IN (202412, 202501)"
    """
```

#### _build_date_condition()

```python
def _build_date_condition(
    self,
    start: str | None,
    end: str | None,
    trimestral: bool = False,
) -> str | None:
    """
    Constroi clausula WHERE para range de datas.
    Usa _storage_col("data") para obter nome correto.
    """
```

#### _build_cnpj_condition()

```python
def _build_cnpj_condition(
    self,
    instituicoes: InstitutionInput | None,
    column: str = "CNPJ_8",
) -> str | None:
    """
    Constroi clausula WHERE para CNPJs.
    Normaliza e valida instituicoes internamente.
    """
```

#### join_conditions()

```python
def join_conditions(conditions: list[str | None]) -> str | None:
    """
    Junta condicoes com AND, ignorando None.

    Exemplo:
    ["DATA = 202412", None, "CNPJ_8 = '12345678'"]
    -> "DATA = 202412 AND CNPJ_8 = '12345678'"
    """
```

### Metodos de Mapeamento

#### _storage_col()

```python
def _storage_col(self, presentation_col: str) -> str:
    """
    Traduz nome de apresentacao para storage.

    Exemplo (COSIF):
    - "data" -> "DATA_BASE"
    - "instituicao" -> "NOME_INSTITUICAO"

    Se nao mapeado, retorna original.
    """
```

#### _apply_column_mapping()

```python
def _apply_column_mapping(self, df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica renomear: storage -> apresentacao.
    Usa _COLUMN_MAP da subclass.
    """
```

### Metodos de Finalizacao

#### _finalize_read()

```python
def _finalize_read(self, df: pd.DataFrame) -> pd.DataFrame:
    """
    Pipeline final (simplificado -- dedup, datetime e drop movidos para DuckDB):
    1. Aplica mapeamento de colunas (_COLUMN_MAP, lowercase)
    2. Ordena por data ascending
    3. Reordena colunas (_COLUMN_ORDER, se definido)
    4. Reset index

    Dedup (DISTINCT), conversao datetime (LAST_DAY/MAKE_DATE) e drop de
    colunas internas sao feitos no DuckDB via _read_glob(), antes do
    pipeline Python.
    """
```

#### _to_datetime_index()

```python
def _to_datetime_index(self, df: pd.DataFrame) -> pd.DataFrame:
    """
    Move a coluna data para um DatetimeIndex chamado 'date'.

    Ultimo passo de read(): enrichment e a analise de era rodam antes,
    com a data ainda como coluna. Vale tambem para o DataFrame vazio,
    para o formato nao depender de haver resultado.
    """
```

### Metodos de Introspeccao

#### list_periodos()

```python
def list_periodos(self, escopo: str | None = None) -> list[int]:
    """
    Lista periodos disponiveis (ordenados).

    Args:
        escopo: Filtra pelos periodos com dados desse escopo (via
                _periodos_por_escopo()). Se None, retorna uniao de
                todas as fontes.

    Raises:
        InvalidScopeError: Se escopo invalido, ou se o explorer nao tem escopos.

    Retorna: [202401, 202402, ..., 202412]
    """
```

#### has_data()

```python
def has_data(self, escopo: str | None = None) -> bool:
    """Verifica se ha dados disponiveis."""
```

#### describe()

```python
def describe(self, escopo: str | None = None) -> ExplorerInfo:
    """
    Retorna o que o explorer aceita e o que ha coletado.

    Alem dos periodos em disco, descreve a superficie de chamada:
    escopos validos, colunas listaveis por list_values(), colunas
    devolvidas por read(), filtros aceitos e colunas validas em
    cadastro=. Os filtros sao lidos da assinatura de read() via
    _read_signature_info() -- nao ha lista paralela para envelhecer.

    Sem escopo=:
    {
        "escopos": ["individual", "prudencial"],
        "columns": ["data", "documento", "escopo"],
        "read_columns": ["cnpj_8", "instituicao", ...],  # sem data
        "read_index": "date",  # data vira o DatetimeIndex do read()
        "filtros": ["conta", "documento", "escopo", "instituicao"],
        "cadastro_columns": ["atividade", ...],
        "periods": [202401, ...],  # Uniao
        "period_count": 12,
        "has_data": True,
        "first_period": 202401,
        "last_period": 202412,
        "by_escopo": {  # apenas em explorers com escopos
            "individual": {"period_count": 12, "has_data": True},
            "prudencial": {"period_count": 12, "has_data": True},
        },
    }

    Com escopo=: a chave "escopo" substitui "by_escopo" e os periodos
    ficam restritos ao escopo pedido.
    """
```

O retorno e o `TypedDict` `ExplorerInfo` (com `EscopoInfo` para as entradas
de `by_escopo`), definido em `domain/types.py`.

### Infraestrutura de list_values()

A base fornece a implementacao generica de `list_values()`, chamada pelas
subclasses via `_base_list()`:

```python
def _base_list(
    self,
    columns: list[str],
    *,
    start: DateScalar | None = None,
    end: DateScalar | None = None,
    limit: int = 100,
    **filters: object,
) -> pd.DataFrame:
    """SELECT DISTINCT das expressoes de _LIST_COLUMNS, com ORDER BY e LIMIT.
    Emite TruncatedResultWarning quando o resultado bate no limit."""


def _validate_list_columns(self, columns: list[str]) -> None:
    """Valida colunas contra _LIST_COLUMNS/_BLOCKED_COLUMNS.
    Raises InvalidColumnError para coluna desconhecida."""
```

- `_LIST_COLUMNS`: dict com chaves lowercase -> expressao SQL de storage
  (ex: `{"data": "DATA_BASE", "escopo": "ESCOPO"}`)
- `_BLOCKED_COLUMNS`: dict com chaves lowercase -> mensagem apontando a
  alternativa (`list_contas()`, `cadastro.search()`); coluna bloqueada gera
  warning e DataFrame vazio, nao erro
- Input case-insensitive: as colunas pedidas sao comparadas via `col.lower()`
- Hooks para subclasses: `_get_list_source()` (FROM SQL; COSIF monta UNION ALL
  dos escopos com coluna ESCOPO literal) e `_build_list_conditions()` (WHERE)

---

## entity/ (pacote)

O pacote `entity/` contem duas classes com responsabilidades distintas:

- **`EntityLookup`** (`lookup.py`): resolucao de metadados, source checking, canonical names
- **`EntitySearch`** (`search.py`): busca fuzzy de entidades por nome

Dependencia unidirecional: `EntitySearch` depende de `EntityLookup`, mas nao o inverso.

### EntityLookup (lookup.py)

Resolucao de entidades e metadados via queries DuckDB:

1. **Verificacao de disponibilidade**: Quais fontes (cosif/ifdata) tem dados para cada CNPJ
2. **Resolucao de identificadores**: CNPJ, conglomerado, lider
3. **Nomes canonicos**: Cache de nomes a partir do cadastro
4. **Cache**: LRU cache para `get_entity_identifiers`, session cache para nomes

```python
def __init__(
    self,
    query_engine: QueryEngine | None = None,
):
    self._qe = query_engine or QueryEngine()
```

### EntitySearch (search.py)

Busca fuzzy de entidades por nome. Recebe `EntityLookup` no construtor.

```python
class EntitySearch:
    def __init__(
        self,
        lookup: EntityLookup,
        fuzzy_threshold_suggest: int | None = None,
    ):
        self._lookup = lookup
        self._fuzzy = FuzzyMatcher(threshold_suggest=fuzzy_threshold_suggest)
```

Com `fuzzy_threshold_suggest=None`, o `FuzzyMatcher` usa o valor de
`get_settings().fuzzy_threshold` (padrao 78).

### EntitySearch.search()

```python
def search(
    self,
    termo: str,
    limit: int = 10,
    date_range: tuple[int, int] | None = None,
) -> pd.DataFrame:
    """
    Busca instituicoes por nome com fuzzy matching.

    Args:
        termo: Nome ou parte dele
        limit: Maximo de resultados
        date_range: Tupla (min_yyyymm, max_yyyymm) para restringir a
            verificacao de disponibilidade de dados

    Retorna DataFrame com colunas:
    - cnpj_8: CNPJ de 8 digitos
    - instituicao: Nome oficial
    - situacao: "A" (ativa) ou "I" (inativa)
    - fontes: "cosif,ifdata" (onde ha dados)
    - score: Score fuzzy (0-100)

    Ordenacao:
    1. Ativas primeiro (A < I)
    2. Score descrescente
    3. Nome alfabetico
    """
```

Fluxo interno:

1. Normaliza termo (remove acentos, upper)
2. Carrega entidades reais do cadastro (filtra aliases com `real_entity_condition()`)
3. Carrega aliases pesquisaveis (incluindo nomes prudenciais/financeiros), resolvidos para CNPJ real
4. Fuzzy match com token_set_ratio sobre todos os aliases
5. Verifica fontes de dados para CNPJs encontrados (via `EntityLookup`)
6. Busca situacao mais recente (via `EntityLookup`)
7. Se houver matches com fontes disponiveis, filtra resultados sem `fontes`
8. Ordena (ativas primeiro, score desc, nome asc) e aplica limit

### EntityLookup.get_entity_identifiers() [CACHED]

```python
@cached(maxsize=256)
def get_entity_identifiers(self, cnpj_8: str) -> dict[str, str | None]:
    """
    Retorna identificadores de uma entidade.

    Retorna:
    {
        "cnpj_interesse": cnpj_8,
        "cnpj_reporte_cosif": str,  # CNPJ do lider se conglomerado
        "cod_congl_prud": str,      # Codigo conglomerado prudencial
        "cod_congl_fin": str,       # Codigo conglomerado financeiro
        "nome_entidade": str,
    }

    Cache LRU evita re-queries.
    Retorna dados padrao vazio se CNPJ nao encontrado.
    """
```

### EntityLookup.get_canonical_names_for_cnpjs() [CACHED]

```python
def get_canonical_names_for_cnpjs(self, cnpjs: list[str]) -> dict[str, str]:
    """
    Retorna nomes canonicos a partir do cadastro mais recente.

    O cadastro e a fonte mestra para nomes de entidades nas leituras
    analiticas. Filtra apenas entidades reais (exclui aliases).
    Se um CNPJ nao existir no cadastro, retorna string vazia.
    Resultados sao cacheados por sessao para evitar queries repetidas.
    """
```

Cache incremental por sessao (`_name_cache`): apenas CNPJs nao vistos geram query SQL. Chamadas subsequentes com mesmos CNPJs (ou subsets) retornam do cache sem hit no DuckDB.

### EntityLookup.clear_cache()

```python
def clear_cache(self) -> None:
    """Limpa caches LRU de get_entity_identifiers() e cache de nomes."""
```

Limpa tanto o LRU cache de `get_entity_identifiers()` quanto o `_name_cache` de nomes canonicos.

### Filtragem de Entidades Reais

O EntityLookup distingue entidades reais de aliases prudenciais/financeiros no cadastro:

```python
@staticmethod
def real_entity_condition(
    cnpj_col: str = "CNPJ_8",
    cod_inst_col: str = "CodInst",
) -> str:
    """
    Filtra linhas que representam entidades reais.

    Regra canonica: toda linha com CNPJ_8 e CodInst numerico
    representa uma entidade. Aliases prudenciais/financeiros
    sao identificados pelo CodInst nao-numerico.

    E um @staticmethod -- nao depende de estado de instancia.
    """


def resolved_entity_cnpj_expr(
    self,
    cnpj_col: str = "CNPJ_8",
    cnpj_lider_col: str = "CNPJ_LIDER_8",
    cod_inst_col: str = "CodInst",
) -> str:
    """Resolve aliases prudenciais para o CNPJ da entidade lider."""
```

### SQL Interno do EntitySearch

O `search()` usa duas queries separadas:

1. **Entidades reais**: nomes canonicos do cadastro, filtrados por `real_entity_condition()`,
   com dedup por CNPJ (nome mais recente)
2. **Aliases pesquisaveis**: todos os nomes do cadastro (incluindo prudenciais/financeiros),
   resolvidos para o CNPJ real via `resolved_entity_cnpj_expr()`
3. **Pos-processamento**: quando existem matches com `fontes`, resultados sem dados sao descartados

A funcao `strip_accents()` e UDF registrada no DuckDB para comparacao insensivel a acentos.

---

## Integracao entre Componentes

### BaseExplorer usa EntityLookup

```python
class BaseExplorer:
    def __init__(self, query_engine=None, entity_lookup=None):
        self._qe = query_engine or QueryEngine()
        self._resolver = entity_lookup or EntityLookup(query_engine=self._qe)

    @property
    def resolver(self) -> EntityLookup:
        return self._resolver
```

### COSIFExplorer usa BaseExplorer

```python
class COSIFExplorer(BaseExplorer):
    def read(self, start, end=None, *, instituicao=None, escopo=None, conta=None):
        # Validacao (herdada -- apenas start e obrigatorio)
        self._validate_required_params(start)

        # Normalizacao (herdada)
        cnpjs = self._normalize_instituicoes(instituicao) if instituicao else None

        # SQL Building (herdado + funcoes de infra.sql)
        conditions = [
            self._build_cnpj_condition(cnpjs),
            self._build_date_condition(start, end),
        ]
        where = join_conditions(conditions)

        # Query (usa _read_glob com distinct, date_column, exclude)
        df = self._read_glob(
            pattern=f"{prefix}_*.parquet",
            subdir=subdir,
            where=where,
        )

        # Finalizacao (herdada): rename/sort/reorder e, por ultimo,
        # a coluna data vira o DatetimeIndex 'date'
        return self._to_datetime_index(self._finalize_read(df))
```

### EntityLookup usa Constants

```python
from ifdata_bcb.core.constants import get_pattern, get_subdir


class EntityLookup:  # em core/entity/lookup.py
    def _source_path(self, source_key: str) -> str:
        """Retorna path completo para glob de arquivos de uma fonte."""
        return (
            f"{self._qe.cache_path}/{get_subdir(source_key)}/{get_pattern(source_key)}"
        )
```

---

## eras.py

### Responsabilidades

O modulo `eras` centraliza toda logica de deteccao e tratamento das diferentes eras de formato do BCB:

1. **Deteccao de formato**: Identifica a era de um CSV COSIF pelo header
2. **SQL Building**: Gera queries que normalizam qualquer era para um schema uniforme
3. **Diagnostico de continuidade**: Mede, no dado retornado, se a serie sobrevive a transicao de era -- e emite os warnings correspondentes

O diagnostico e **derivado do dado**, nao de tabelas de metadados: `diagnose_eras()` compara os conjuntos de codigos de conta dos dois lados do boundary. As tabelas do modulo servem apenas para explicar *por que* uma lacuna existe quando a causa e conhecida.

### Constantes

```python
COSIF_ERA_BOUNDARY: int = 202501  # Primeiro periodo COSIF com novo plano contabil
IFDATA_ERA_BOUNDARY: int = 202503  # Primeiro trimestre IFDATA com codigos novos
```

### Eras de Formato COSIF

| Era | Periodo | Colunas CSV | CONTA | NOME_CONTA |
|-----|---------|-------------|-------|------------|
| 1 | 199501-201009 | 8 (`DATA;CNPJ;...`) | 10 digitos com leading zeros | UPPER |
| 2 | 201010-202412 | 11 (`#DATA_BASE;...`) | 8 digitos | UPPER |
| 3 | 202501+ | 11 (`#DATA_BASE;...`) | 10 digitos (COSIF 1.5) | Title Case |

Eras 1-2 tem codigos de conta compativeis (strip leading zeros). Era 3 tem codigos incompativeis (novo plano contabil, Resolucao CMN 4.966).

### detect_cosif_csv_era()

```python
def detect_cosif_csv_era(csv_path: Path, encoding: str) -> int:
    """Retorna 1 (pre-201010) ou 2 (201010+, inclui Era 3)."""
```

Pula 3 linhas de metadata e verifica se o header contem `#DATA_BASE`. Usa `errors="replace"` para robustez com encoding incorreto.

### build_cosif_select()

```python
def build_cosif_select(era: int, csv_path: Path, encoding: str) -> str:
    """Retorna query SQL que produz schema normalizado independente da era."""
```

Output uniforme: `DATA_BASE, CNPJ, NOME_INSTITUICAO, DOCUMENTO, CONTA, NOME_CONTA, SALDO`.

- **Era 1**: Mapeia colunas antigas (`"DATA"` -> `DATA_BASE`, `"NOME INSTITUICAO"` -> `NOME_INSTITUICAO`), `CAST(CONTA AS BIGINT)` para strip leading zeros, `UPPER("NOME CONTA")`
- **Era 2/3**: Query padrao com `UPPER(NOME_CONTA)` para normalizar Title Case da Era 3

### diagnose_eras()

```python
def diagnose_eras(
    df: pd.DataFrame,
    *,
    boundary: int,
    source: str,
    periodos_solicitados: list[int] | None,
    group_col: str | None = None,   # "relatorio" (IFDATA) | "documento" (COSIF)
    date_col: str = "data",
    account_col: str = "cod_conta",
    escopo: str | None = None,
) -> EraDiagnostic
```

Chamado por `BaseExplorer._check_eras()` apos `_finalize_read()` e antes de `_filter_columns()`. So analisa quando o range **solicitado** cobre os dois lados do boundary -- uma query inteira dentro de uma era nao gera diagnostico nem warning.

Para cada grupo, compara os conjuntos de codigos de conta pre e pos boundary e classifica em `status`:

| status | condicao | warning |
|--------|----------|---------|
| `estavel` | overlap >= `_STABLE_OVERLAP_THRESHOLD` (0.9) | nenhum |
| `renumerado` | overlap abaixo do threshold | `IncompatibleEraWarning` |
| `so_pre` | grupo sem dados apos o boundary | conforme `motivo` |
| `so_post` | grupo sem dados antes do boundary | conforme `motivo` |

Reducao via `drop_duplicates()` antes de agrupar: no bulk IFDATA de 202412+202503, 1.592.353 linhas viram 755 antes do agrupamento. Overhead medido de um `read()` cruzando o boundary: 7% (IFDATA) a 12% (COSIF).

Degrada quando o chamador restringiu `columns`: sem `group_col` a analise e global; sem `account_col` fica so a cobertura de periodos. Para evitar isso no caso que mais importa, `BaseExplorer._era_required_columns()` forca as colunas de dimensao na query -- **apenas** quando o range cruza o boundary.

### EraDiagnostic

`TypedDict` serializavel (`json.dumps` direto), exposto em `df.attrs["era"]` e como retorno de `explorer.check_era()`:

```python
{
    "source": "IFDATA",
    "boundary": 202503,
    "cruza_boundary": True,
    "periodos_solicitados": [202412, 202503],
    "periodos_presentes": [202412, 202503],
    "periodos_ausentes": [],
    "grupos": {
        "Resumo": {
            "status": "renumerado",
            "n_pre": 9,
            "n_post": 10,
            "n_comum": 3,
            "pct_overlap": 30.0,
            "motivo": None,
        },
    },
}
```

### emit_era_warnings()

```python
def emit_era_warnings(diag: EraDiagnostic, *, stacklevel: int = 3) -> None
```

Traduz o diagnostico em warnings, **agregando por causa** -- um bulk cruzando o boundary tem dezenas de grupos e um warning por grupo seria ruido:

1. **`IncompatibleEraWarning`**: um unico warning listando os grupos renumerados com o overlap medido de cada um.
2. **`DroppedReportWarning`**: por relatorio descontinuado (`motivo == "descontinuado"`).
3. **`ScopeMigrationWarning`**: um warning para os relatorios de credito que migraram de `financeiro` para `prudencial` (`motivo == "migracao_escopo"`).
4. **`PartialDataWarning`** com `reason="era_coverage_gap"`: um warning agregando as lacunas sem causa conhecida -- inclui relatorios introduzidos pelo BCB e cache incompleto.

O `motivo` vem das tabelas `_DROPPED_REPORTS_NORMALIZED` e do prefixo de credito, com normalizacao Unicode (remove acentos, lowercase). Uma lacuna sem entrada na tabela ainda e detectada e avisada -- so perde a explicacao.

---

## Exports Publicos

```python
# core/__init__.py
from ifdata_bcb.core.entity import EntityLookup, EntitySearch

__all__ = [
    "EntityLookup",
    "EntitySearch",
]
```

> **Nota:** `BaseExplorer` e exportado de `providers/`, nao de `core/`.
