# Estendendo a Biblioteca

Guia para criar novos providers e customizar comportamentos.

## Arquitetura de Providers

### Componentes

Cada provider e composto por dois componentes principais:

| Componente | Responsabilidade | Classe Base |
|------------|------------------|-------------|
| **Collector** | Coleta de dados (download, processamento) | `BaseCollector` |
| **Explorer** | Interface de consulta (read, list_*) | `BaseExplorer` |

### Estrutura de Diretorio

```
src/ifdata_bcb/
  core/
    constants.py       # Registro de fontes de dados
    base_explorer.py   # Classe base dos explorers
  providers/
    base_collector.py  # Classe base dos collectors
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
from typing import Optional
import tempfile
import pandas as pd
import requests

from ifdata_bcb.providers.base_collector import BaseCollector
from ifdata_bcb.domain.exceptions import PeriodUnavailableError
from ifdata_bcb.core.constants import DATA_SOURCES, get_subdir
from ifdata_bcb.infra.resilience import retry, DEFAULT_REQUEST_TIMEOUT
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

    def __init__(self, data_manager: Optional[DataManager] = None):
        super().__init__(data_manager)

    def _get_file_prefix(self) -> str:
        """Prefixo dos arquivos (ex: novo_dados_202412.parquet)."""
        return DATA_SOURCES["novo_dados"]["prefix"]

    def _get_subdir(self) -> str:
        """Subdiretorio dentro de cache/."""
        return get_subdir("novo_dados")

    @retry(delay=2.0)
    def _download_single(self, url: str, output_path: Path, period: int) -> bool:
        """Download com retry automatico."""
        response = requests.get(url, timeout=DEFAULT_REQUEST_TIMEOUT)
        if response.status_code == 404:
            raise PeriodUnavailableError(period)
        response.raise_for_status()
        output_path.write_bytes(response.content)
        return True

    def _download_period(self, period: int) -> Optional[Path]:
        """
        Baixa dados de um periodo especifico.

        Args:
            period: Periodo no formato YYYYMM.

        Returns:
            Path do arquivo CSV baixado ou None se falhar.
        """
        url = f"https://api.exemplo.com/dados/{period}.csv"
        temp_dir = Path(tempfile.mkdtemp(prefix=f"novo_{period}_"))
        output_path = temp_dir / f"novo_{period}.csv"

        try:
            self._download_single(url, output_path, period)
            return output_path
        except PeriodUnavailableError:
            raise  # Re-raise para marcar como indisponivel
        except Exception as e:
            self.logger.error(f"Download failed for {period}: {e}")
            return None

    def _process_to_parquet(
        self, csv_path: Path, period: int
    ) -> Optional[pd.DataFrame]:
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

from typing import Optional
import pandas as pd

from ifdata_bcb.core.base_explorer import BaseExplorer
from ifdata_bcb.core.constants import DATA_SOURCES, get_subdir
from ifdata_bcb.domain.types import AccountInput, InstitutionInput
from ifdata_bcb.infra.query import QueryEngine
from ifdata_bcb.core.entity_lookup import EntityLookup
from ifdata_bcb.providers.novo.collector import NovoCollector


_EMPTY_COLUMNS = ["DATA", "CNPJ_8", "INSTITUICAO", "VALOR"]


class NovoExplorer(BaseExplorer):
    """
    Explorer para dados do Novo Provider.

    Exemplo:
        explorer = NovoExplorer()
        explorer.collect('2024-01', '2024-12')
        df = explorer.read('60872504', start='2024-12')
    """

    # Mapeamento de colunas storage -> apresentacao
    _COLUMN_MAP = {
        "DATA": "DATA",
        "COLUNA_NORMALIZADA": "CONTA",
    }

    def __init__(
        self,
        query_engine: Optional[QueryEngine] = None,
        entity_lookup: Optional[EntityLookup] = None,
    ):
        super().__init__(query_engine, entity_lookup)
        self._collector: Optional[NovoCollector] = None

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
        instituicao: InstitutionInput,
        start: str,
        end: Optional[str] = None,
        conta: Optional[AccountInput] = None,
        columns: Optional[list[str]] = None,
    ) -> pd.DataFrame:
        """
        Le dados com filtros opcionais.

        Args:
            instituicao: CNPJ de 8 digitos. OBRIGATORIO.
            start: Data inicial ou unica (YYYY-MM). OBRIGATORIO.
            end: Data final para range (YYYY-MM).
            conta: Nome(s) da(s) conta(s).
            columns: Colunas especificas.

        Returns:
            DataFrame com os dados filtrados.
        """
        self._validate_required_params(instituicao, start)

        conditions = [
            self._build_cnpj_condition(instituicao),
            self._build_date_condition(start, end, trimestral=False),
        ]

        if conta:
            contas = self._normalize_accounts(conta)
            if contas:
                conditions.append(
                    self._build_string_condition(
                        self._storage_col("CONTA"), contas, case_insensitive=True
                    )
                )

        df = self._qe.read_glob(
            pattern=self._get_pattern(),
            subdir=self._get_subdir(),
            columns=columns,
            where=self._join_conditions(conditions),
        )

        if df.empty:
            return pd.DataFrame(columns=_EMPTY_COLUMNS)

        return self._finalize_read(df)

    def list_contas(
        self, termo: Optional[str] = None, limit: int = 100
    ) -> pd.DataFrame:
        """Lista contas disponiveis."""
        path = self._qe.cache_path / self._get_subdir() / self._get_pattern()

        where = ""
        if termo:
            termo_clean = termo.strip().replace("'", "''").upper()
            where = f"WHERE UPPER(COLUNA_NORMALIZADA) LIKE '%{termo_clean}%'"

        query = f"""
            SELECT DISTINCT COLUNA_NORMALIZADA as CONTA
            FROM '{path}'
            {where}
            ORDER BY CONTA
            LIMIT {limit}
        """
        return self._qe.sql(query)
```

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

def _download_period(self, period: int) -> Optional[Path]:
    """
    Baixa dados de um periodo especifico.

    Args:
        period: Numero do periodo em formato YYYYMM

    Returns:
        Path ao arquivo CSV temporario, ou None se falhar

    Raises:
        PeriodUnavailableError: Se o periodo nao esta disponivel (404)
    """

def _process_to_parquet(self, csv_path: Path, period: int) -> Optional[pd.DataFrame]:
    """
    Processa CSV bruto em DataFrame normalizado.

    Args:
        csv_path: Caminho ao arquivo CSV
        period: Periodo dos dados

    Returns:
        DataFrame normalizado, ou None se vazio/erro
    """
```

#### Atributos de Classe

```python
_PERIOD_TYPE: str = "monthly"  # ou "quarterly"
_MAX_WORKERS: int = 4          # Threads paralelas
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

**Localizacao:** `src/ifdata_bcb/core/base_explorer.py`

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
_COLUMN_MAP: dict[str, str] = {}  # Mapeamento storage -> apresentacao
```

#### Metodos Auxiliares Fornecidos

```python
# Normalizacao de entrada
def _normalize_dates(self, datas: DateInput) -> list[int]
def _normalize_accounts(self, contas: Optional[AccountInput]) -> Optional[list[str]]
def _normalize_institutions(self, instituicoes: Optional[InstitutionInput]) -> Optional[list[str]]
def _resolve_entity(self, identificador: str) -> str  # Valida CNPJ

# Resolucao de ranges
def _resolve_date_range(self, start, end, trimestral=False) -> Optional[list[int]]

# Construcao de queries SQL
def _build_string_condition(self, column, values, case_insensitive=False) -> str
def _build_int_condition(self, column, values) -> str
def _build_date_condition(self, start, end, trimestral=False) -> Optional[str]
def _build_cnpj_condition(self, instituicoes, column="CNPJ_8") -> Optional[str]
def _join_conditions(self, conditions: list) -> Optional[str]

# Mapeamento de colunas
def _storage_col(self, presentation_col: str) -> str  # Traduz nome
def _apply_column_mapping(self, df: pd.DataFrame) -> pd.DataFrame
def _finalize_read(self, df: pd.DataFrame) -> pd.DataFrame  # Aplica mapeamento + converte DATA

# Descoberta
def list_periods(self, source: Optional[str] = None) -> list[int]
def has_data(self, source: Optional[str] = None) -> bool
def describe(self, source: Optional[str] = None) -> dict

# Validacao
def _validate_required_params(self, instituicao, start) -> None
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
```

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

### EntityLookup Customizado

```python
from ifdata_bcb.core.entity_lookup import EntityLookup
from ifdata_bcb.providers.ifdata.explorer import IFDATAExplorer

# Lookup com thresholds ajustados
lookup = EntityLookup(
    fuzzy_threshold_auto=90,    # Mais restritivo
    fuzzy_threshold_suggest=80
)

explorer = IFDATAExplorer(entity_lookup=lookup)
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
  InvalidScopeError        # Escopo invalido
  DataUnavailableError     # Dados nao disponiveis
  EntityNotFoundError      # CNPJ nao encontrado
  AmbiguousIdentifierError # Multiplos matches
  InvalidIdentifierError   # CNPJ invalido (nao tem 8 digitos)
  MissingRequiredParameterError  # Param obrigatorio faltando
  InvalidDateRangeError    # start > end
  InvalidDateFormatError   # Formato de data invalido
  PeriodUnavailableError   # Periodo nao disponivel na fonte (404)
```

### Uso

```python
from ifdata_bcb.domain.exceptions import (
    BacenAnalysisError,
    InvalidIdentifierError,
    PeriodUnavailableError,
)

# Capturar todas
try:
    df = explorer.read('invalido', '2024-01')
except BacenAnalysisError as e:
    print(f"Erro: {e}")

# Capturar especificas
try:
    collector._download_period(202499)
except PeriodUnavailableError:
    print("Periodo nao disponivel")
```

## Tipos de Entrada

**Localizacao:** `src/ifdata_bcb/domain/types.py`

```python
DateInput = Union[int, str, list[int], list[str]]
# Aceita: 202412, '202412', '2024-12', [202412, 202501], etc.

AccountInput = Union[str, list[str]]
# Aceita: 'TOTAL ATIVO', ['ATIVO', 'PASSIVO']

InstitutionInput = Union[str, list[str]]
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
- [ ] Testar leitura: `explorer.read(instituicao, start)`
- [ ] Testar listagem: `explorer.list_periods()`, `explorer.has_data()`

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
        self._resolver = entity_lookup or EntityLookup()
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
