# Modulo Core

O modulo `core` concentra a logica central compartilhada entre todos os providers.

## Localizacao

```
src/ifdata_bcb/core/
|-- __init__.py           # Exports publicos
|-- api.py               # Funcao search() de alto nivel
|-- entity_lookup.py     # Resolucao e busca de entidades
|-- constants.py         # Configuracoes centralizadas
+-- eras.py              # Deteccao e tratamento de eras de formato BCB
```

> **Nota:** `BaseExplorer` foi movido para `providers/base_explorer.py`.

## constants.py

### TIPO_INST_MAP

Mapeamento entre escopo e codigo TipoInstituicao do IFDATA:

```python
TIPO_INST_MAP: dict[str, int] = {
    "individual": 3,      # Instituicao individual
    "prudencial": 1,      # Conglomerado prudencial
    "financeiro": 2,      # Conglomerado financeiro
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
    "cosif_individual": 199501,
    "cosif_prudencial": 201407,
    "ifdata_valores": 200303,
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
4. **Mapeamento**: Traduz nomes de colunas (storage -> apresentacao)
5. **Finalizacao**: Transforma dados de saida (DATA int -> datetime)

### Propriedades de Classe

Subclasses definem estas propriedades:

```python
class COSIFExplorer(BaseExplorer):
    # Mapeamento: nome_storage -> nome_apresentacao
    _COLUMN_MAP = {
        "DATA_BASE": "DATA",
        "NOME_INSTITUICAO": "INSTITUICAO",
        "NOME_CONTA": "CONTA",
        "CONTA": "COD_CONTA",
        "SALDO": "VALOR",
    }

    # Colunas adicionadas pos-query por Python (nao existem no Parquet)
    _DERIVED_COLUMNS: set[str] = {"ESCOPO"}

    # Colunas a remover do resultado
    _DROP_COLUMNS: list[str] = []

    # Ordem das colunas no resultado
    _COLUMN_ORDER = ["DATA", "CNPJ_8", "INSTITUICAO", "ESCOPO", "COD_CONTA", "CONTA", ...]

    # Escopos validos para _validate_escopo()
    _VALID_ESCOPOS = ["individual", "prudencial"]
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
def _normalize_contas(
    self, contas: AccountInput | None
) -> list[str] | None:
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
    Delega validacao para InstitutionList (Pydantic).
    """
```

### Metodos de Validacao

Normalizacao e validacao de inputs sao delegadas para modelos Pydantic em `domain/validation.py`:
- `NormalizedDates`: Normaliza DateInput -> list[int]
- `ValidatedCnpj8`: Valida CNPJ de 8 digitos
- `InstitutionList`: Normaliza e valida lista de CNPJs
- `AccountList`: Normaliza lista de contas

#### _resolve_entidade()

```python
def _resolve_entidade(self, identificador: str) -> str:
    """
    Valida CNPJ de exatamente 8 digitos.
    Delega para ValidatedCnpj8 (Pydantic).

    Raises:
        InvalidIdentifierError: Se nao for [0-9]{8}
    """
```

#### _validate_required_params()

```python
def _validate_required_params(
    self,
    instituicao: InstitutionInput | None,
    start: str | None,
) -> None:
    """
    Valida parametros obrigatorios.

    Raises:
        MissingRequiredParameterError: Se faltar instituicao ou start
    """
```

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
        InvalidScopeError: Se coluna desconhecida
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
    Aplica nomes canonicos do cadastro a coluna INSTITUICAO.
    Se a coluna ja existe, substitui apenas onde o canonico nao eh vazio.
    """
```

#### _diagnose_empty_result()

```python
def _diagnose_empty_result(
    self,
    source_name: str,
    has_files: bool,
    had_conta_filter: bool,
) -> None:
    """Cascata de diagnostico quando read() retorna vazio. Emite PartialDataWarning."""
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
from ifdata_bcb.infra.sql import build_string_condition, build_int_condition, join_conditions
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
    Usa _storage_col("DATA") para obter nome correto.
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
    - "DATA" -> "DATA_BASE"
    - "INSTITUICAO" -> "NOME_INSTITUICAO"

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
    Pipeline final:
    1. Drop colunas internas (_DROP_COLUMNS)
    2. Aplica mapeamento de colunas (_COLUMN_MAP)
    3. Deduplicacao (drop_duplicates)
    4. Converte DATA (int YYYYMM) -> datetime via pd.to_datetime + MonthEnd
    5. Ordena por DATA ascending
    6. Reordena colunas (_COLUMN_ORDER, se definido)
    7. Reset index

    Cria copia para evitar SettingWithCopyWarning.
    """
```

### Metodos de Introspeccao

#### list_periodos()

```python
def list_periodos(self, source: str | None = None) -> list[int]:
    """
    Lista periodos disponiveis (ordenados).

    Args:
        source: Nome da fonte especifica (para multi-source)
                Se None, retorna uniao de todas as fontes

    Retorna: [202401, 202402, ..., 202412]
    """
```

#### has_data()

```python
def has_data(self, source: str | None = None) -> bool:
    """Verifica se ha dados disponiveis."""
```

#### describe()

```python
def describe(self, source: str | None = None) -> dict:
    """
    Retorna metadados do explorer.

    Single-source:
    {
        "source": "default",
        "subdir": "cosif/individual",
        "prefix": "cosif_ind",
        "periods": [202401, ...],
        "period_count": 12,
        "has_data": True,
        "first_period": 202401,
        "last_period": 202412,
    }

    Multi-source:
    {
        "sources": ["individual", "prudencial"],
        "periods": [202401, ...],  # Uniao
        "period_count": 12,
        "by_source": {
            "individual": {...},
            "prudencial": {...},
        }
    }
    """
```

---

## entity_lookup.py

### Responsabilidades

O `EntityLookup` centraliza toda logica de busca e resolucao de entidades:

1. **Busca fuzzy**: Encontra instituicoes por nome parcial
2. **Resolucao de escopos**: Resolve CNPJ para codigo IFDATA
3. **Cache**: LRU cache para evitar re-queries
4. **Uniao de fontes**: Agrega dados de cadastro, COSIF individual e prudencial

### Construtor

```python
def __init__(
    self,
    query_engine: QueryEngine | None = None,
    fuzzy_threshold_suggest: int = 78,
):
    self._qe = query_engine or QueryEngine()
    self._fuzzy = FuzzyMatcher(threshold_suggest=fuzzy_threshold_suggest)
```

- `threshold_suggest`: Score >= 78 aparece em sugestoes

### query_engine (property)

```python
@property
def query_engine(self) -> QueryEngine:
    """QueryEngine usada para consultas."""
    return self._qe
```

### search()

```python
def search(self, termo: str, limit: int = 10) -> pd.DataFrame:
    """
    Busca instituicoes por nome com fuzzy matching.

    Args:
        termo: Nome ou parte dele
        limit: Maximo de resultados

    Retorna DataFrame com colunas:
    - CNPJ_8: CNPJ de 8 digitos
    - INSTITUICAO: Nome oficial
    - SITUACAO: "A" (ativa) ou "I" (inativa)
    - FONTES: "cosif,ifdata" (onde ha dados)
    - SCORE: Score fuzzy (0-100)

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
5. Verifica fontes de dados para CNPJs encontrados
6. Busca situacao mais recente
7. Se houver matches com fontes disponiveis, filtra resultados sem `FONTES`
8. Ordena (ativas primeiro, score desc, nome asc) e aplica limit

### get_entity_identifiers() [CACHED]

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

### resolve_ifdata_escopo() (providers.ifdata.scope)

A resolucao de escopo IFDATA foi extraida para funcao de modulo em `providers.ifdata.scope`:

```python
from ifdata_bcb.providers.ifdata.scope import resolve_ifdata_escopo

resolve_ifdata_escopo(entity_lookup, cnpj_8, escopo) -> ScopeResolution
```

```python
def resolve_ifdata_escopo(
    entity_lookup: EntityLookup,
    cnpj_8: str,
    escopo: str,
) -> ScopeResolution:
    """
    Resolve CNPJ para codigo IFDATA baseado no escopo.

    Args:
        entity_lookup: Instancia de EntityLookup
        cnpj_8: CNPJ de 8 digitos
        escopo: "individual", "prudencial", ou "financeiro"

    Retorna ScopeResolution:
    {
        "cod_inst": str,      # Codigo para filtrar IFDATA
        "tipo_inst": int,     # 1, 2, ou 3
        "cnpj_original": str,
        "escopo": str,
    }

    Logica:
    - individual: CNPJ direto, tipo=3
    - prudencial: CodConglomeradoPrudencial, tipo=1
    - financeiro: Verifica CodConglomeradoFinanceiro e CNPJ direto
                  como candidatos nos dados IFDATA, tipo=2

    Raises:
        InvalidScopeError: Escopo invalido
        DataUnavailableError: CNPJ nao tem dados para escopo
    """
```

### get_canonical_names_for_cnpjs()

```python
def get_canonical_names_for_cnpjs(self, cnpjs: list[str]) -> dict[str, str]:
    """
    Retorna nomes canônicos a partir do cadastro mais recente.

    O cadastro e a fonte mestra para nomes de entidades nas leituras
    analiticas. Filtra apenas entidades reais (exclui aliases).
    Se um CNPJ nao existir no cadastro, retorna string vazia.
    """
```

### clear_cache()

```python
def clear_cache(self) -> None:
    """Limpa caches LRU de get_entity_identifiers() e _cadastro_has_codinst()."""
```

### Filtragem de Entidades Reais

O EntityLookup distingue entidades reais de aliases prudenciais/financeiros no cadastro:

```python
def real_entity_condition(
    self,
    cnpj_col: str = "CNPJ_8",
    cnpj_lider_col: str = "CNPJ_LIDER_8",
    name_col: str = "NomeInstituicao",
    cod_inst_col: str = "CodInst",
) -> str:
    """
    Filtra linhas que representam entidades reais.

    Se o cache possui CodInst (caches novos):
      CNPJ_8 IS NOT NULL AND CodInst e numerico

    Se nao (caches legados):
      Heuristica por nome (exclui aliases como 'PRUDENCIAL', 'MASTER')
    """

def resolved_entity_cnpj_expr(
    self,
    cnpj_col: str = "CNPJ_8",
    cnpj_lider_col: str = "CNPJ_LIDER_8",
    name_col: str = "NomeInstituicao",
    cod_inst_col: str = "CodInst",
) -> str:
    """Resolve aliases prudenciais para o CNPJ da entidade lider."""
```

### SQL Interno

O `search()` usa duas queries separadas:

1. **Entidades reais**: nomes canonicos do cadastro, filtrados por `real_entity_condition()`,
   com dedup por CNPJ (nome mais recente)
2. **Aliases pesquisaveis**: todos os nomes do cadastro (incluindo prudenciais/financeiros),
   resolvidos para o CNPJ real via `resolved_entity_cnpj_expr()`
3. **Pos-processamento**: quando existem matches com `FONTES`, resultados sem dados sao descartados

A funcao `strip_accents()` e UDF registrada no DuckDB para comparacao insensivel a acentos.

---

## api.py

### search()

Funcao de alto nivel para busca de instituicoes:

```python
_lookup: EntityLookup | None = None

def search(termo: str, limit: int = 10) -> pd.DataFrame:
    """
    Busca instituicoes por nome em todas as fontes.

    Use esta funcao para encontrar o CNPJ antes de fazer consultas.

    Args:
        termo: Nome ou parte dele
        limit: Maximo de resultados (default 10)

    Exemplo:
        >>> from ifdata_bcb import search
        >>> search("itau")
           CNPJ_8          INSTITUICAO SITUACAO       FONTES  SCORE
        0  60872504  ITAU UNIBANCO S.A.        A  cosif,ifdata    100
    """
    global _lookup
    if _lookup is None:
        _lookup = EntityLookup()
    return _lookup.search(termo, limit=limit)
```

- Lazy loading do EntityLookup
- Singleton reutilizado em chamadas subsequentes

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
    def read(self, instituicao, start, end=None, conta=None, escopo=None):
        # Validacao (herdada)
        self._validate_required_params(instituicao, start)

        # Normalizacao (herdada)
        cnpjs = self._normalize_instituicoes(instituicao)

        # SQL Building (herdado + funcoes de infra.sql)
        conditions = [
            self._build_cnpj_condition(cnpjs),
            self._build_date_condition(start, end),
        ]
        where = join_conditions(conditions)

        # Query (usa QueryEngine herdado)
        df = self._qe.read_glob(
            pattern=f"{prefix}_*.parquet",
            subdir=subdir,
            where=where,
        )

        # Finalizacao (herdada)
        return self._finalize_read(df)
```

### EntityLookup usa Constants

```python
from ifdata_bcb.core.constants import DATA_SOURCES, get_pattern, get_subdir

class EntityLookup:
    def _get_source_path(self, source: str) -> str:
        subdir = get_subdir(source)
        pattern = get_pattern(source)
        return f"{self._cache_path}/{subdir}/{pattern}"
```

---

## eras.py

### Responsabilidades

O modulo `eras` centraliza toda logica de deteccao e tratamento das diferentes eras de formato do BCB:

1. **Deteccao**: Identifica a era de um CSV COSIF pelo header
2. **SQL Building**: Gera queries que normalizam qualquer era para um schema uniforme
3. **Warnings**: Alerta quando uma query abrange periodos com codigos de conta incompativeis

### Constantes

```python
COSIF_ERA_BOUNDARY: int = 202501   # Primeiro periodo COSIF com novo plano contabil
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

### check_era_boundary()

```python
def check_era_boundary(
    dates: list[int] | None,
    boundary: int,
    source_name: str,
) -> None:
    """Emite IncompatibleEraWarning se dates cruzam o boundary."""
```

Condicao: `min(dates) < boundary <= max(dates)`. Nao bloqueia a query -- apenas emite `warnings.warn()`.

---

## Exports Publicos

```python
# core/__init__.py
from ifdata_bcb.core.api import search
from ifdata_bcb.core.entity_lookup import EntityLookup

__all__ = [
    "search",
    "EntityLookup",
]
```

> **Nota:** `BaseExplorer` e exportado de `providers/`, nao de `core/`.
