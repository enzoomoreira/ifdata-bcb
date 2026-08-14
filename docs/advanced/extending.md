# Estendendo a Biblioteca

Guia para criar novos providers e customizar comportamentos.

## Arquitetura de Providers

### Componentes

Cada provider e composto por dois componentes principais:

| Componente | Responsabilidade | Classe Base |
|------------|------------------|-------------|
| **Collector** | Coleta de dados (download, processamento) | `BaseCollector` |
| **Explorer** | Interface de consulta (read, fetch, list_values, list_contas) | `BaseExplorer` |

Os explorers embutidos (COSIF, IFDATA, Cadastro) oferecem tambem `fetch()`:
coleta para um diretorio temporario (via `infra.paths.temp_dir`), injetando um
`DataManager(base_path=tmp)` no collector, e delega ao `read()` de um explorer
construido com `QueryEngine(base_path=tmp)` -- baixa do BCB e devolve o
DataFrame sem tocar o cache local.

### Estrutura de Diretorio

```
src/ifdata_bcb/
  core/
    constants.py       # Registro de fontes de dados
  providers/
    base_collector.py  # Classe base dos collectors
    base_explorer.py   # Classe base dos explorers
    novo_provider/
      __init__.py
      collector.py     # NovoCollector (herda BaseCollector)
      explorer.py      # NovoExplorer (herda BaseExplorer)
```

## Criando um Novo Provider

### Passo 1: Registrar a Fonte em constants.py

```python
# src/ifdata_bcb/core/constants.py

DATA_SOURCES: dict[str, dict[str, str]] = {
    # ... fontes existentes ...
    "novo_dados": {
        "subdir": "novo/dados",
        "prefix": "novo_dados",
    },
}
```

### Passo 2: Criar o Collector

O Collector e responsavel por baixar e processar dados.

```python
# src/ifdata_bcb/providers/novo/collector.py

from pathlib import Path
import tempfile
import pandas as pd
from ifdata_bcb.providers.base_collector import BaseCollector
from ifdata_bcb.domain.exceptions import PeriodUnavailableError
from ifdata_bcb.core.constants import DATA_SOURCES, get_subdir
from ifdata_bcb.infra.storage import DataManager


class NovoCollector(BaseCollector):
    """
    Collector para dados do Novo Provider.

    Baixa dados de [fonte] e processa para formato Parquet.
    """

    # Periodicidade: 'monthly' ou 'quarterly'
    _PERIOD_TYPE = "monthly"

    # Numero de workers paralelos (ajustar conforme API)
    _MAX_WORKERS = 4

    def __init__(self, data_manager: DataManager | None = None):
        super().__init__(data_manager)

    def _get_file_prefix(self) -> str:
        """Prefixo dos arquivos (ex: novo_dados_202412.parquet)."""
        return DATA_SOURCES["novo_dados"]["prefix"]

    def _get_subdir(self) -> str:
        """Subdiretorio dentro de cache/."""
        return get_subdir("novo_dados")

    # _download_single e herdado do BaseCollector:
    # @retry(delay=2.0)
    # def _download_single(self, url: str, output_path: Path) -> bool:
    #     """Baixa um arquivo da URL e salva em output_path."""

    def _download_period(self, period: int, work_dir: Path) -> Path | None:
        """
        Baixa dados de um periodo para work_dir.

        Args:
            period: Periodo no formato YYYYMM.
            work_dir: Diretorio temporario para downloads.

        Returns:
            Path do arquivo CSV baixado ou None se falhar.
        """
        url = f"https://api.exemplo.com/dados/{period}.csv"
        output_path = work_dir / f"novo_{period}.csv"

        try:
            self._download_single(url, output_path)
            return output_path
        except PeriodUnavailableError:
            raise  # Re-raise para marcar como indisponivel
        except Exception as e:
            self.logger.error(f"Download failed for {period}: {e}")
            return None

    def _process_to_parquet(self, csv_path: Path, period: int) -> pd.DataFrame | None:
        """
        Processa CSV para DataFrame normalizado.

        Args:
            csv_path: Caminho do arquivo CSV.
            period: Periodo no formato YYYYMM.

        Returns:
            DataFrame processado ou None se falhar.
        """
        try:
            # Usar DuckDB para processamento eficiente
            query = f"""
                SELECT
                    {period} as DATA,
                    TRIM(coluna1) as COLUNA_NORMALIZADA,
                    TRY_CAST(REPLACE(valor, ',', '.') AS DOUBLE) as VALOR
                FROM read_csv(
                    '{csv_path}',
                    delim=',',
                    header=true,
                    ignore_errors=true
                )
            """

            cursor = self._get_cursor()
            df = cursor.sql(query).df()

            if df.empty:
                return None

            # Reordenar colunas
            cols = ["DATA", "COLUNA_NORMALIZADA", "VALOR"]
            return df[[c for c in cols if c in df.columns]]

        except Exception as e:
            self.logger.error(f"Erro processando {csv_path}: {e}")
            return None
```

### Passo 3: Criar o Explorer

O Explorer fornece a interface de consulta.

```python
# src/ifdata_bcb/providers/novo/explorer.py

import pandas as pd

from ifdata_bcb.providers.base_explorer import BaseExplorer
from ifdata_bcb.core.constants import DATA_SOURCES, get_subdir
from ifdata_bcb.domain.types import AccountInput, InstitutionInput
from ifdata_bcb.infra.query import QueryEngine
from ifdata_bcb.infra.sql import (
    build_like_condition,
    build_string_condition,
    join_conditions,
    merge_params,
)
from ifdata_bcb.core.entity import EntityLookup
from ifdata_bcb.providers.novo.collector import NovoCollector


class NovoExplorer(BaseExplorer):
    """
    Explorer para dados do Novo Provider.

    Exemplo:
        explorer = NovoExplorer()
        explorer.collect('2024-01', '2024-12')
        df = explorer.read('2024-12', instituicao='60872504')
    """

    # Mapeamento de colunas storage -> apresentacao (lowercase)
    _COLUMN_MAP = {
        "DATA": "data",
        "CNPJ_8": "cnpj_8",
        "COLUNA_NORMALIZADA": "conta",
        "VALOR": "valor",
    }

    # Ordem das colunas no output (tambem usado como fallback para DataFrame vazio)
    _COLUMN_ORDER = ["data", "cnpj_8", "conta", "valor"]

    def __init__(
        self,
        query_engine: QueryEngine | None = None,
        entity_lookup: EntityLookup | None = None,
    ):
        super().__init__(query_engine, entity_lookup)
        self._collector: NovoCollector | None = None

    def _get_subdir(self) -> str:
        return get_subdir("novo_dados")

    def _get_file_prefix(self) -> str:
        return DATA_SOURCES["novo_dados"]["prefix"]

    def _get_pattern(self) -> str:
        return f"{self._get_file_prefix()}_*.parquet"

    def _get_collector(self) -> NovoCollector:
        """Lazy initialization do collector."""
        if self._collector is None:
            self._collector = NovoCollector()
        return self._collector

    def collect(
        self, start: str, end: str, force: bool = False, verbose: bool = True
    ) -> None:
        """
        Coleta dados do Novo Provider.

        Args:
            start: Data inicial (YYYY-MM).
            end: Data final (YYYY-MM).
            force: Se True, recoleta dados existentes.
        """
        self._get_collector().collect(start, end, force=force, verbose=verbose)

    def read(
        self,
        start: str,
        end: str | None = None,
        *,
        instituicao: InstitutionInput | None = None,
        conta: AccountInput | None = None,
        columns: list[str] | None = None,
    ) -> pd.DataFrame:
        """
        Le dados com filtros opcionais.

        Args:
            start: Data inicial ou unica (YYYY-MM). OBRIGATORIO.
            end: Data final para range (YYYY-MM).
            instituicao: CNPJ de 8 digitos. Se None, retorna todas (bulk).
            conta: Nome(s) da(s) conta(s).
            columns: Colunas especificas.

        Returns:
            DataFrame com DatetimeIndex 'date' e colunas de apresentacao
            em lowercase.
        """
        self._validate_required_params(start)

        conditions = [
            self._build_cnpj_condition(instituicao),
            self._build_date_condition(start, end, trimestral=False),
        ]

        if conta:
            contas = self._normalize_contas(conta)
            if contas:
                conditions.append(
                    build_string_condition(
                        self._storage_col("conta"),
                        contas,
                        case_insensitive=True,
                        accent_insensitive=True,
                    )
                )

        df = self._qe.read_glob(
            pattern=self._get_pattern(),
            subdir=self._get_subdir(),
            columns=columns,
            where=join_conditions(conditions),
        )

        if df.empty:
            return self._to_datetime_index(pd.DataFrame(columns=self._COLUMN_ORDER))

        # _finalize_read renomeia/ordena; _to_datetime_index move a coluna
        # data para um DatetimeIndex chamado 'date' (ultimo passo de read()).
        return self._to_datetime_index(self._finalize_read(df))

    def list_contas(
        self, termo: str | None = None, *, limit: int = 100
    ) -> pd.DataFrame:
        """Lista contas disponiveis. Filtros sao keyword-only apos termo."""
        path = self._qe.cache_path / self._get_subdir() / self._get_pattern()

        cond = None
        if termo:
            cond = build_like_condition("COLUNA_NORMALIZADA", termo)
        where = f"WHERE {cond}" if cond else ""

        query = f"""
            SELECT DISTINCT COLUNA_NORMALIZADA as "conta"
            FROM '{path}'
            {where}
            ORDER BY "conta"
            LIMIT {limit}
        """
        # Interpolar o fragmento por f-string descarta os valores: passe-os
        # explicitamente. Nunca escape aspas a mao.
        return self._qe.sql(query, params=merge_params(cond))
```

> **Nunca monte um literal SQL a mao.** As funcoes de `infra.sql` devolvem
> `SqlCondition`, que carrega o texto do fragmento (com placeholders `$p0`,
> `$p1`, ...) e, em `.params`, os valores correspondentes. Quem executa a query
> precisa repassar os params. `read_glob(where=...)` faz isso sozinho; queries
> montadas a mao e enviadas por `sql()` precisam de `params=merge_params(...)`.
>
> Esquecer nao devolve resultado errado em silencio: o DuckDB recusa a query
> com placeholder sem valor.

### Passo 4: Criar __init__.py

```python
# src/ifdata_bcb/providers/novo/__init__.py

from ifdata_bcb.providers.novo.collector import NovoCollector
from ifdata_bcb.providers.novo.explorer import NovoExplorer

__all__ = ["NovoCollector", "NovoExplorer"]
```

### Passo 5: Registrar no Modulo Principal (opcional)

Para acesso via `bcb.novo`:

```python
# src/ifdata_bcb/__init__.py

_novo = None


def __getattr__(name: str):
    global _novo
    # ... outros providers ...

    if name == "novo":
        if _novo is None:
            from ifdata_bcb.providers.novo.explorer import NovoExplorer

            _novo = NovoExplorer()
        return _novo

    raise AttributeError(f"module 'ifdata_bcb' has no attribute '{name}'")


__all__ = [
    # ... outros ...
    "novo",
]
```

## Classes Base

### BaseCollector

**Localizacao:** `src/ifdata_bcb/providers/base_collector.py`

Fornece infraestrutura completa para coleta paralela, logging e tratamento de erros.

#### Metodos Abstratos (OBRIGATORIOS)

```python
def _get_file_prefix(self) -> str:
    """Prefixo unico para os arquivos (ex: 'cosif_ind', 'ifdata_val')."""


def _get_subdir(self) -> str:
    """Subdiretorio de armazenamento (ex: 'cosif/individual')."""


def _download_period(self, period: int, work_dir: Path) -> Path | None:
    """
    Baixa dados de um periodo para work_dir.

    Args:
        period: Numero do periodo em formato YYYYMM
        work_dir: Diretorio temporario para downloads

    Returns:
        Path ao arquivo CSV temporario, ou None se falhar

    Raises:
        PeriodUnavailableError: Se o periodo nao esta disponivel (404)
    """


def _process_to_parquet(self, data_path: Path, period: int) -> pd.DataFrame | None:
    """
    Processa dados em DataFrame normalizado.

    Args:
        data_path: Caminho do arquivo ou diretorio de dados
        period: Periodo dos dados

    Returns:
        DataFrame normalizado, ou None se vazio/erro
    """
```

#### Atributos de Classe

```python
_PERIOD_TYPE: str = "monthly"  # ou "quarterly"
_MAX_WORKERS: int = 4  # Threads paralelas
```

#### Metodos Auxiliares Fornecidos

```python
# Coleta principal (nao precisa reimplementar)
def collect(self, start: str, end: str, force: bool = False) -> tuple[int, int, int, int]

# Utilitarios
def _get_cursor(self) -> duckdb.DuckDBPyConnection  # Cursor thread-local
def _generate_periods(self, start: str, end: str) -> list[int]
def _get_missing_periods(self, start: str, end: str) -> list[int]
def _normalize_text_fields(self, df: pd.DataFrame) -> pd.DataFrame

# Display
def _start(self, title: str, num_items: int) -> None
def _end(self, verbose: bool = True) -> None
def _info(self, message: str) -> None
def _warning(self, message: str) -> None
```

### BaseExplorer

**Localizacao:** `src/ifdata_bcb/providers/base_explorer.py`

Fornece infraestrutura para leitura e consulta de dados.

#### Metodos Abstratos (OBRIGATORIOS)

```python
def _get_subdir(self) -> str:
    """Subdiretorio dos dados."""


def _get_file_prefix(self) -> str:
    """Prefixo dos arquivos Parquet."""
```

#### Atributos de Classe

```python
_COLUMN_MAP: dict[str, str] = {}  # Mapeamento storage -> apresentacao (lowercase)
_DERIVED_COLUMNS: set[str] = set()  # Colunas adicionadas pos-query por Python
_DROP_COLUMNS: list[str] = []  # Colunas a remover antes do mapeamento
_COLUMN_ORDER: list[str] = []  # Ordem desejada das colunas no output
_VALID_ESCOPOS: list[str] = []  # Escopos validos para _validate_escopo
_DATE_COLUMN: str | None = (
    None  # Coluna YYYYMM int para conversao automatica em datetime
)
_LIST_COLUMNS: dict[str, str] = {}  # list_values(): chave lowercase -> expressao SQL
_BLOCKED_COLUMNS: dict[str, str] = {}  # list_values(): coluna recusada -> mensagem
```

Todas as colunas do parquet que aparecem no output precisam estar em
`_COLUMN_MAP` -- inclusive as que so mudam de caixa (ex: `CNPJ_8` -> `cnpj_8`).
Em `list_values()` o input de colunas e case-insensitive (via `col.lower()`).

#### Metodos Auxiliares Fornecidos

```python
# Normalizacao de entrada
def _normalize_datas(self, datas: DateInput) -> list[int]
def _normalize_contas(self, contas: AccountInput | None) -> list[str] | None
def _normalize_instituicoes(self, instituicoes: InstitutionInput | None) -> list[str] | None
def _resolve_entidade(self, identificador: str) -> str  # Valida CNPJ

# Resolucao de ranges
def _resolve_date_range(self, start, end, trimestral=False) -> list[int] | None

# Construcao de queries SQL (funcoes em infra.sql)
# SqlCondition e subclasse de str: o texto tem placeholders, .params tem os valores
# from ifdata_bcb.infra.sql import build_string_condition, join_conditions, merge_params
build_string_condition(column, values, case_insensitive=False, accent_insensitive=False) -> SqlCondition
build_int_condition(column, values) -> SqlCondition
build_between_condition(column, low, high) -> SqlCondition
build_like_condition(column, term, case_insensitive=True, accent_insensitive=True) -> SqlCondition
build_account_condition(name_col, code_col, values) -> SqlCondition
build_in_clause(values) -> SqlCondition  # so a lista: "$p0, $p1"
join_conditions(conditions: list) -> SqlCondition | None  # junta com AND e mescla params
merge_params(*fragments) -> dict  # coleta params para passar ao sql()

# Metodos na classe base
def _build_date_condition(self, start, end, trimestral=False) -> SqlCondition | None
def _build_cnpj_condition(self, instituicoes, column="CNPJ_8") -> SqlCondition | None

# Mapeamento de colunas
def _storage_col(self, presentation_col: str) -> str  # Traduz nome
def _apply_column_mapping(self, df: pd.DataFrame) -> pd.DataFrame
def _finalize_read(self, df: pd.DataFrame) -> pd.DataFrame  # Rename + sort + reorder
def _to_datetime_index(self, df: pd.DataFrame) -> pd.DataFrame  # data -> DatetimeIndex 'date'

# Descoberta (escopo unificado -- nao ha mais source=)
def list_periodos(self, escopo: str | None = None) -> list[int]
def has_data(self, escopo: str | None = None) -> bool
def describe(self, escopo: str | None = None) -> ExplorerInfo
def _periodos_por_escopo(self) -> dict[str, list[int]]  # Hook: override onde ha escopos

# list_values() (infra generica)
def _base_list(self, columns, *, start=None, end=None, limit=100, **filters) -> pd.DataFrame
def _validate_list_columns(self, columns: list[str]) -> None

# Validacao
def _validate_required_params(self, start) -> None
```

#### Multi-Source Pattern

Para providers com multiplas fontes (mesmo schema):

```python
class COSIFExplorer(BaseExplorer):
    _ESCOPOS = {
        "individual": {"subdir": "cosif/individual", "prefix": "cosif_ind"},
        "prudencial": {"subdir": "cosif/prudencial", "prefix": "cosif_prud"},
    }

    def _get_sources(self) -> dict[str, dict[str, str]]:
        return self._ESCOPOS

    def _periodos_por_escopo(self) -> dict[str, list[int]]:
        # No COSIF os escopos coincidem com as fontes de armazenamento
        return {
            esc: self._list_periodos_for_source(cfg["subdir"], cfg["prefix"])
            for esc, cfg in self._ESCOPOS.items()
        }
```

`_periodos_por_escopo()` e o hook que responde `list_periodos(escopo=...)`,
`has_data(escopo=...)` e `describe(escopo=...)`. No IFDATA, onde escopo e
coluna dos dados e nao fonte, o hook resolve via query `DISTINCT` sobre
`TipoInstituicao`. Um explorer sem escopos (`_VALID_ESCOPOS` vazio) rejeita
`escopo=` com `InvalidScopeError`.

## Customizando Comportamentos

### QueryEngine Customizado

```python
from ifdata_bcb.infra import QueryEngine
from ifdata_bcb.providers.cosif.explorer import COSIFExplorer

# QueryEngine com path customizado
qe = QueryEngine(base_path="/dados/bcb")

# Injetar no explorer
explorer = COSIFExplorer(query_engine=qe)
```

### EntityLookup e EntitySearch Customizados

```python
from ifdata_bcb.core.entity import EntityLookup, EntitySearch

# Lookup padrao
lookup = EntityLookup()

# Search com threshold ajustado
search = EntitySearch(lookup, fuzzy_threshold_suggest=80)
df = search.search("Itau")
```

### DataManager Customizado

```python
from ifdata_bcb.infra import DataManager
from ifdata_bcb.providers.cosif.collector import COSIFCollector

dm = DataManager(base_path="/dados/bcb")
collector = COSIFCollector("individual", data_manager=dm)
```

## Excecoes

### Hierarquia

```
BacenAnalysisError (base)
  InvalidScopeError              # Valor invalido em escopo/fonte/documento
  InvalidIdentifierError         # CNPJ invalido (base de 8 ou completo de 14 com DV)
  MissingRequiredParameterError  # Param obrigatorio faltando
  InvalidDateRangeError          # start > end
  InvalidDateFormatError         # Formato de data invalido
  PeriodUnavailableError         # Periodo nao disponivel na fonte (404)
  DataProcessingError            # Falha no processamento de dados
  InvalidColumnError             # Coluna invalida em read(), list_values() ou cadastro=

BacenWarning (base, herda de UserWarning)
  IncompatibleEraWarning         # Query cruza fronteira de era
  PartialDataWarning             # Resultado incompleto
  ScopeUnavailableWarning        # Escopo indisponivel para entidade
  NullValuesWarning              # Valores financeiros NULL
  ScopeMigrationWarning          # Relatorio migrou de escopo entre eras
  DroppedReportWarning           # Relatorio descontinuado
  EmptyFilterWarning             # Filtro vazio (ex: columns=[])
  TruncatedResultWarning         # Resultado truncado pelo limit
```

Um provider novo deve derivar seus warnings de `BacenWarning`: e o que faz
`warnings.simplefilter("ignore", BacenWarning)` cobrir a biblioteca inteira.

### Uso

```python
from ifdata_bcb import (
    BacenAnalysisError,
    InvalidIdentifierError,
    PeriodUnavailableError,
)

# Capturar todas
try:
    df = explorer.read("invalido", "2024-01")
except BacenAnalysisError as e:
    print(f"Erro: {e}")

# Capturar especificas
from pathlib import Path

try:
    collector._download_period(202499, Path("/tmp/work"))
except PeriodUnavailableError:
    print("Periodo nao disponivel")
```

## Tipos de Entrada

**Localizacao:** `src/ifdata_bcb/domain/types.py`

```python
DateScalar = int | str | date | datetime | pd.Timestamp
DateInput = DateScalar | list[DateScalar]
# Aceita: 202412, '202412', '2024-12', date(2024,12,1), pd.Timestamp(...), ou lista de qualquer um

AccountInput = str | list[str]
# Aceita: 'TOTAL ATIVO', ['ATIVO', 'PASSIVO']

InstitutionInput = str | list[str]
# Aceita: '60872504', ['60872504', '60746948']
```

## Checklist para Novo Provider

- [ ] Registrar fonte em `core/constants.py` (DATA_SOURCES)
- [ ] Criar Collector que herda de `BaseCollector`
  - [ ] Implementar `_get_file_prefix()`
  - [ ] Implementar `_get_subdir()`
  - [ ] Implementar `_download_period()` (com `@retry`)
  - [ ] Implementar `_process_to_parquet()` (usar DuckDB)
  - [ ] Definir `_PERIOD_TYPE` ("monthly" ou "quarterly")
- [ ] Criar Explorer que herda de `BaseExplorer`
  - [ ] Implementar `_get_subdir()`
  - [ ] Implementar `_get_file_prefix()`
  - [ ] Definir `_COLUMN_MAP` se precisar mapear nomes
  - [ ] Implementar metodo `read()` com filtros
  - [ ] Implementar metodos `list_*()` para listar recursos
- [ ] Criar `__init__.py` com exports
- [ ] (Opcional) Adicionar ao `__all__` em `__init__.py` raiz
- [ ] Testar coleta: `collector.collect(start, end)`
- [ ] Testar leitura: `explorer.read(start, instituicao=...)`
- [ ] Testar listagem: `explorer.list_periodos()`, `explorer.has_data()`

## Padroes Utilizados

### Template Method

```python
class BaseCollector:
    def collect(self, start, end, ...):
        # Framework fornece o fluxo principal
        for period in periods:
            csv_path = self._download_period(period)       # Subclass impl
            df = self._process_to_parquet(csv_path, period) # Subclass impl
            self.dm.save(df, ...)
```

### Dependency Injection

```python
class BaseExplorer:
    def __init__(self, query_engine=None, entity_lookup=None):
        self._qe = query_engine or QueryEngine()
        self._resolver = entity_lookup or EntityLookup(query_engine=self._qe)
```

### Decorator

```python
from ifdata_bcb.infra import retry, cached


@retry(delay=2.0)
def _download_single(self, url: str) -> bool:
    # Retry automatico em falhas
    pass


@cached(maxsize=256)
def get_entity_identifiers(self, cnpj_8: str) -> dict:
    # Cache automatico de resultados
    pass
```

### Lazy Loading

```python
_cosif = None


def __getattr__(name: str):
    global _cosif
    if name == "cosif":
        if _cosif is None:
            from ifdata_bcb.providers.cosif.explorer import COSIFExplorer

            _cosif = COSIFExplorer()
        return _cosif
```

## Referencias

- [Arquitetura](../internals/architecture.md)
- [Infraestrutura](../internals/infra.md)
- [Providers](../providers/)
