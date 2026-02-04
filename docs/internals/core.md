# Modulo Core

O modulo `core` concentra a logica central compartilhada entre todos os providers.

## Localizacao

```
src/ifdata_bcb/core/
|-- __init__.py           # Exports publicos
|-- api.py               # Funcao search() de alto nivel
|-- base_explorer.py     # Classe base abstrata para explorers
|-- entity_lookup.py     # Resolucao e busca de entidades
+-- constants.py         # Configuracoes centralizadas
```

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

### Funcoes Auxiliares

```python
def get_pattern(source: str) -> str:
    """Retorna glob pattern para arquivos da fonte."""
    # get_pattern("cosif_individual") -> "cosif_ind_*.parquet"

def get_subdir(source: str) -> str:
    """Retorna subdiretorio da fonte."""
    # get_subdir("cosif_individual") -> "cosif/individual"
```

---

## base_explorer.py

### Responsabilidades

O `BaseExplorer` e a classe base abstrata para todos os explorers:

1. **Normalizacao**: Padroniza formatos de entrada (datas, CNPJs, contas)
2. **Validacao**: Verifica parametros obrigatorios e formatos
3. **SQL Building**: Constroi clausulas WHERE dinamicamente
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
        "SALDO": "VALOR",
    }

    # Colunas a remover do resultado
    _DROP_COLUMNS = ["CONTA", "DOCUMENTO"]

    # Ordem das colunas no resultado
    _COLUMN_ORDER = ["DATA", "CNPJ_8", "INSTITUICAO", "ESCOPO", ...]
```

### Construtor

```python
def __init__(
    self,
    query_engine: Optional[QueryEngine] = None,
    entity_lookup: Optional[EntityLookup] = None,
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

#### _normalize_dates()

```python
def _normalize_dates(self, datas: DateInput) -> list[int]:
    """
    Normaliza datas para lista de inteiros YYYYMM.

    Aceita:
    - int: 202412
    - str: '202412', '2024-12'
    - list[int | str]: [202401, '2024-02']

    Retorna: [202412], [202401, 202402]
    """
```

#### _normalize_accounts()

```python
def _normalize_accounts(
    self, contas: Optional[AccountInput]
) -> Optional[list[str]]:
    """
    Normaliza contas para lista de strings.

    Aceita:
    - str: 'TOTAL DO ATIVO'
    - list[str]: ['TOTAL DO ATIVO', 'PATRIMONIO LIQUIDO']
    - None

    Retorna: ['TOTAL DO ATIVO'], [...], None
    """
```

#### _normalize_institutions()

```python
def _normalize_institutions(
    self, instituicoes: Optional[InstitutionInput]
) -> Optional[list[str]]:
    """
    Normaliza instituicoes para lista de CNPJs validados.
    Chama _resolve_entity() para cada CNPJ.
    """
```

### Metodos de Validacao

#### _resolve_entity()

```python
def _resolve_entity(self, identificador: str) -> str:
    """
    Valida CNPJ de exatamente 8 digitos.

    Raises:
        InvalidIdentifierError: Se nao for \d{8}
    """
    if not re.fullmatch(r"\d{8}", identificador):
        raise InvalidIdentifierError(identificador)
    return identificador
```

#### _validate_required_params()

```python
def _validate_required_params(
    self,
    instituicao: Optional[InstitutionInput],
    start: Optional[str],
) -> None:
    """
    Valida parametros obrigatorios.

    Raises:
        MissingRequiredParameterError: Se faltar instituicao ou start
    """
```

#### _resolve_date_range()

```python
def _resolve_date_range(
    self,
    start: Optional[str],
    end: Optional[str],
    trimestral: bool = False,
) -> Optional[list[int]]:
    """
    Resolve range de datas.

    - None, None -> None (todos os periodos)
    - start, None -> [start_int] (data unica)
    - start, end -> generate_month_range ou generate_quarter_range

    Raises:
        InvalidDateRangeError: Se start > end
    """
```

### Metodos para Construir SQL

#### _build_string_condition()

```python
def _build_string_condition(
    self,
    column: str,
    values: list[str],
    case_insensitive: bool = False,
) -> str:
    """
    Constroi clausula WHERE para strings.

    Exemplos:
    - ["valor"] -> "COLUNA = 'valor'"
    - ["a", "b"] -> "COLUNA IN ('a', 'b')"
    - case_insensitive=True -> "UPPER(COLUNA) IN ('A', 'B')"
    """
```

#### _build_int_condition()

```python
def _build_int_condition(self, column: str, values: list[int]) -> str:
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
    start: Optional[str],
    end: Optional[str],
    trimestral: bool = False,
) -> Optional[str]:
    """
    Constroi clausula WHERE para range de datas.
    Usa _storage_col("DATA") para obter nome correto.
    """
```

#### _build_cnpj_condition()

```python
def _build_cnpj_condition(
    self,
    instituicoes: Optional[InstitutionInput],
    column: str = "CNPJ_8",
) -> Optional[str]:
    """
    Constroi clausula WHERE para CNPJs.
    Normaliza e valida instituicoes internamente.
    """
```

#### _join_conditions()

```python
def _join_conditions(self, conditions: list[Optional[str]]) -> Optional[str]:
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
    1. Aplica mapeamento de colunas
    2. Converte DATA (int YYYYMM) -> datetime
    3. Ordena por DATA ascending
    4. Reset index

    Cria copia para evitar SettingWithCopyWarning.
    """
```

### Metodos de Introspeccao

#### list_periods()

```python
def list_periods(self, source: Optional[str] = None) -> list[int]:
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
def has_data(self, source: Optional[str] = None) -> bool:
    """Verifica se ha dados disponiveis."""
```

#### describe()

```python
def describe(self, source: Optional[str] = None) -> dict:
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
    query_engine: Optional[QueryEngine] = None,
    fuzzy_threshold_auto: int = 85,
    fuzzy_threshold_suggest: int = 70,
):
    self._qe = query_engine or QueryEngine()
    self._fuzzy = FuzzyMatcher(
        threshold_auto=fuzzy_threshold_auto,
        threshold_suggest=fuzzy_threshold_suggest,
    )
```

- `threshold_auto`: Score >= 85 aceita automaticamente
- `threshold_suggest`: Score >= 70 aparece em sugestoes

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
2. Executa SQL UNION de 3 fontes
3. Fuzzy match com token_set_ratio
4. Verifica fontes de dados para CNPJs
5. Busca situacao mais recente
6. Monta e ordena DataFrame

### get_entity_identifiers() [CACHED]

```python
@cached(maxsize=256)
def get_entity_identifiers(self, cnpj_8: str) -> dict[str, Optional[str]]:
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

### resolve_ifdata_scope()

```python
def resolve_ifdata_scope(self, cnpj_8: str, escopo: str) -> ScopeResolution:
    """
    Resolve CNPJ para codigo IFDATA baseado no escopo.

    Args:
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
    - financeiro: CodConglomeradoFinanceiro, tipo=2

    Raises:
        InvalidScopeError: Escopo invalido
        DataUnavailableError: CNPJ nao tem dados para escopo
    """
```

### get_names_for_cnpjs()

```python
def get_names_for_cnpjs(self, cnpjs: list[str]) -> dict[str, str]:
    """
    Retorna mapeamento {cnpj: nome} para lista de CNPJs.
    Para CNPJs nao encontrados: string vazia.
    """
```

### clear_cache()

```python
def clear_cache(self) -> None:
    """Limpa cache LRU de get_entity_identifiers()."""
```

### SQL Interno

O EntityLookup usa SQL para unir fontes:

```python
def _build_entity_union_sql(self):
    """
    Gera SQL que une 3 fontes:

    SELECT CNPJ_8, NomeInstituicao AS NOME,
           strip_accents(UPPER(NomeInstituicao)) AS NOME_NORM,
           'cadastro' AS FONTE
    FROM '{cache}/ifdata/cadastro/*.parquet'

    UNION ALL

    SELECT CNPJ_8, NOME_INSTITUICAO AS NOME,
           strip_accents(UPPER(NOME_INSTITUICAO)) AS NOME_NORM,
           'cosif_ind' AS FONTE
    FROM '{cache}/cosif/individual/*.parquet'

    UNION ALL

    SELECT CNPJ_8, NOME_INSTITUICAO AS NOME,
           strip_accents(UPPER(NOME_INSTITUICAO)) AS NOME_NORM,
           'cosif_prud' AS FONTE
    FROM '{cache}/cosif/prudencial/*.parquet'
    """
```

A funcao `strip_accents()` e UDF registrada no DuckDB para comparacao insensivel a acentos.

---

## api.py

### search()

Funcao de alto nivel para busca de instituicoes:

```python
_lookup: Optional[EntityLookup] = None

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
    def read(self, instituicao, start, end=None, conta=None, escopo="individual"):
        # Validacao (herdada)
        self._validate_required_params(instituicao, start)

        # Normalizacao (herdada)
        cnpjs = self._normalize_institutions(instituicao)

        # SQL Building (herdado)
        conditions = [
            self._build_cnpj_condition(cnpjs),
            self._build_date_condition(start, end),
        ]
        where = self._join_conditions(conditions)

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

## Exports Publicos

```python
# core/__init__.py
from ifdata_bcb.core.api import search
from ifdata_bcb.core.base_explorer import BaseExplorer
from ifdata_bcb.core.entity_lookup import EntityLookup
from ifdata_bcb.core.constants import DATA_SOURCES, TIPO_INST_MAP

__all__ = [
    "search",
    "BaseExplorer",
    "EntityLookup",
    "DATA_SOURCES",
    "TIPO_INST_MAP",
]
```
