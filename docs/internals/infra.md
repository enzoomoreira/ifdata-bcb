# Infraestrutura

A camada de infraestrutura fornece servicos tecnicos para a biblioteca.

## Localizacao

```
src/ifdata_bcb/infra/
|-- __init__.py           # Exports publicos
|-- config.py            # Paths e configuracoes
|-- query.py             # QueryEngine (DuckDB)
|-- storage.py           # DataManager (Parquet)
|-- log.py               # Logging (Loguru)
|-- cache.py             # Cache LRU com registro global
+-- resilience.py        # Retry e backoff
```

---

## config.py

### Constantes

```python
APP_NAME = "py-bacen"
```

### get_cache_path()

Retorna o caminho para cache de dados:

```python
def get_cache_path() -> Path:
    """
    Prioridade:
    1. Variavel de ambiente BACEN_DATA_DIR
    2. Diretorio de cache do sistema (platformdirs)
    """
```

**Paths por sistema**:

| Sistema | Caminho Padrao |
|---------|----------------|
| Windows | `%LOCALAPPDATA%\py-bacen\Cache\` |
| Linux   | `~/.cache/py-bacen/` |
| macOS   | `~/Library/Caches/py-bacen/` |

**Customizacao**:

```powershell
# Windows PowerShell
$env:BACEN_DATA_DIR = "D:\dados\bcb"

# Linux/macOS
export BACEN_DATA_DIR="/dados/bcb"
```

### get_logs_path()

Retorna o caminho para diretorio de logs:

```python
def get_logs_path() -> Path:
    """Logs sao armazenados separadamente do cache."""
```

**Estrutura de diretorios**:

```
py-bacen/
|-- Cache/
|   |-- cosif/
|   |   |-- individual/
|   |   +-- prudencial/
|   +-- ifdata/
|       |-- valores/
|       +-- cadastro/
+-- Logs/
    +-- ifdata_2024-01-15.log
```

### get_subdir() / ensure_subdir()

```python
def get_subdir(dataset: str) -> str:
    """Retorna subdiretorio para um dataset."""

def ensure_subdir(dataset: str) -> Path:
    """Garante que subdiretorio existe e retorna Path."""
```

---

## query.py (QueryEngine)

### Responsabilidades

Motor de consultas DuckDB sobre arquivos Parquet.

### Construtor

```python
class QueryEngine:
    def __init__(
        self,
        base_path: Optional[Path] = None,  # Padrao: get_cache_path()
        progress_bar: bool = False         # Barra de progresso DuckDB
    ):
```

### read()

Le um arquivo Parquet especifico:

```python
def read(
    self,
    filename: str,       # Nome sem extensao (ex: "cosif_prud_202412")
    subdir: str,         # Subdiretorio (ex: "cosif/prudencial")
    columns: list = None,  # Colunas a carregar (None = todas)
    where: str = None      # Filtro SQL
) -> pd.DataFrame:
```

**Exemplo**:

```python
qe = QueryEngine()
df = qe.read(
    "cosif_prud_202412",
    "cosif/prudencial",
    columns=["CNPJ_8", "VALOR"],
    where="CNPJ_8 = '60872504'"
)
```

### read_glob()

Le multiplos arquivos via glob pattern:

```python
def read_glob(
    self,
    pattern: str,        # Glob (ex: "cosif_prud_*.parquet")
    subdir: str,
    columns: list = None,
    where: str = None
) -> pd.DataFrame:
```

**Exemplo**:

```python
df = qe.read_glob(
    "cosif_prud_2024*.parquet",
    "cosif/prudencial",
    where="CONTA = 'TOTAL GERAL DO ATIVO'"
)
```

**Predicate Pushdown**:

O DuckDB aplica filtros durante leitura do Parquet, evitando carregar dados desnecessarios:

```sql
-- Query interna
SELECT * FROM '{cache}/cosif/prudencial/cosif_prud_*.parquet'
WHERE CNPJ_8 = '60872504' AND DATA_BASE = 202412
```

### sql()

Executa SQL arbitrario:

```python
def sql(self, query: str) -> pd.DataFrame:
    """
    Variaveis disponiveis:
        {cache} - Caminho para diretorio de cache
        {raw}   - Alias para {cache}
    """
```

**Exemplo**:

```python
df = qe.sql("""
    SELECT CNPJ_8, SUM(SALDO) as total
    FROM '{cache}/cosif/prudencial/*.parquet'
    WHERE NOME_CONTA LIKE '%ATIVO%'
    GROUP BY CNPJ_8
    ORDER BY total DESC
    LIMIT 10
""")
```

### describe()

Retorna schema do arquivo:

```python
def describe(self, filename: str, subdir: str) -> pd.DataFrame:
    """Retorna: column_name, column_type, null, key, default, extra"""
```

### get_metadata()

Retorna metadados basicos:

```python
def get_metadata(self, filename: str, subdir: str) -> Optional[dict]:
    """
    Retorna:
    {
        'arquivo': str,
        'subdir': str,
        'registros': int,
        'colunas': int,
        'status': str
    }
    """
```

### list_files()

Lista arquivos em um subdiretorio:

```python
def list_files(self, subdir: str, pattern: str = "*.parquet") -> list[str]:
    """Retorna lista de nomes (sem extensao)."""
```

### file_exists()

```python
def file_exists(self, filename: str, subdir: str) -> bool:
```

---

## storage.py (DataManager)

### Responsabilidades

Gerenciador de persistencia em formato Parquet.

### Construtor

```python
class DataManager:
    def __init__(self, base_path: Optional[Path] = None):
```

### save()

Salva DataFrame em arquivo Parquet:

```python
def save(
    self,
    df: pd.DataFrame,
    filename: str,           # Nome sem extensao
    subdir: str,
    compression: str = "snappy"  # snappy, gzip, zstd
) -> Path:
```

**Exemplo**:

```python
dm = DataManager()
path = dm.save(df, "cosif_prud_202412", "cosif/prudencial")
```

### read()

Le arquivo via QueryEngine:

```python
def read(
    self,
    filename: str,
    subdir: str,
    columns: list = None,
    where: str = None
) -> pd.DataFrame:
```

### get_last_period()

Retorna ultimo periodo disponivel:

```python
def get_last_period(
    self,
    prefix: str,    # Ex: "cosif_prud"
    subdir: str
) -> Optional[tuple[int, int]]:
    """Retorna (ano, mes) ou None."""
```

### get_available_periods()

Retorna todos os periodos disponiveis:

```python
def get_available_periods(
    self,
    prefix: str,
    subdir: str
) -> list[tuple[int, int]]:
    """Retorna lista de (ano, mes) ordenada."""
```

### delete()

Remove um arquivo:

```python
def delete(self, filename: str, subdir: str) -> bool:
    """Retorna True se removeu, False se nao existia."""
```

---

## log.py (Logging)

### Responsabilidades

Sistema de logging com dual output:
- **Console (stderr)**: WARNING+ para usuario
- **Arquivo**: DEBUG+ para debugging tecnico

### configure_logging()

```python
def configure_logging(
    level: str = "WARNING",      # Nivel minimo console
    enable_file: bool = True,
    file_level: str = "DEBUG"
):
```

**Estrutura do log**:

```
# Console (stderr)
WARNING  | Mensagem de aviso

# Arquivo (Logs/ifdata_YYYY-MM-DD.log)
[2024-01-15 10:30:45] DEBUG    [ifdata_bcb.infra.query] Query: ...
[2024-01-15 10:30:46] INFO     [ifdata_bcb.providers.base_collector] Saved: ...
```

**Configuracao de arquivo**:
- Rotacao: 10 MB por arquivo
- Retencao: 30 dias
- Encoding: UTF-8

### get_logger()

```python
def get_logger(name: str = "ifdata_bcb"):
    """
    Lazy initialization do logging.
    Retorna logger loguru com binding de nome.
    """
```

**Exemplo**:

```python
from ifdata_bcb.infra.log import get_logger

logger = get_logger(__name__)
logger.debug("Mensagem de debug")
logger.info("Mensagem informativa")
logger.warning("Aviso")
logger.error("Erro")
```

### set_log_level()

```python
def set_log_level(level: str):
    """Altera nivel (DEBUG, INFO, WARNING, ERROR, CRITICAL)."""
```

### get_log_path()

```python
def get_log_path() -> Path:
```

---

## cache.py

### Responsabilidades

Sistema de cache centralizado com registro global. Thread-safe.

### @cached Decorator

```python
from ifdata_bcb.infra.cache import cached

@cached(maxsize=256)
def funcao_custosa(param: str) -> dict:
    # ...
```

**Parametros**:
- `maxsize`: Tamanho maximo do cache (padrao: 128)

**Diferencial**: Registra funcao em lista global para limpeza centralizada.

### clear_all_caches()

Limpa todos os caches registrados:

```python
from ifdata_bcb.infra.cache import clear_all_caches

count = clear_all_caches()
print(f"Limpos {count} caches")
```

### get_cache_info()

Retorna informacoes de todos os caches:

```python
from ifdata_bcb.infra.cache import get_cache_info

info = get_cache_info()
# {'EntityLookup.get_entity_identifiers': {'hits': 10, 'misses': 5, 'maxsize': 256, 'currsize': 5}}
```

### Implementacao

```python
_registered_caches: list[Callable] = []
_lock = threading.Lock()

def cached(maxsize: int = 128):
    def decorator(func):
        cached_func = lru_cache(maxsize=maxsize)(func)
        with _lock:
            _registered_caches.append(cached_func)
        return cached_func
    return decorator

def clear_all_caches() -> int:
    with _lock:
        for cache in _registered_caches:
            cache.cache_clear()
        return len(_registered_caches)
```

---

## resilience.py

### Responsabilidades

Utilitarios para lidar com falhas transientes em APIs externas.

### Constantes

```python
DEFAULT_RETRY_ATTEMPTS = 3
DEFAULT_RETRY_DELAY = 1.0
DEFAULT_BACKOFF_FACTOR = 2.0
DEFAULT_REQUEST_TIMEOUT = 240
DEFAULT_PARALLEL_STAGGER = 0.5

TRANSIENT_EXCEPTIONS = (
    requests.RequestException,
    requests.ConnectionError,
    requests.Timeout,
    urllib3.exceptions.HTTPError,
    ConnectionError,
    TimeoutError,
    OSError,
    json.JSONDecodeError,
    ValueError,
)
```

### @retry Decorator

Adiciona retry com exponential backoff:

```python
def retry(
    max_attempts: int = 3,
    delay: float = 1.0,
    backoff_factor: float = 2.0,
    exceptions: tuple = TRANSIENT_EXCEPTIONS,
    jitter: bool = True
):
```

**Exemplo**:

```python
from ifdata_bcb.infra.resilience import retry

@retry(max_attempts=3, delay=2.0)
def download_data(url):
    response = requests.get(url, timeout=240)
    response.raise_for_status()
    return response.content
```

**Comportamento**:
1. Tentativa 1: Executa imediatamente
2. Tentativa 2: Espera ~1s (delay + jitter)
3. Tentativa 3: Espera ~2s (delay * backoff + jitter)
4. Falha: Levanta excecao original

### staggered_delay()

Adiciona delay escalonado para workers paralelos:

```python
def staggered_delay(index: int, base_delay: float = 0.5):
    """
    Evita que workers iniciem simultaneamente.
    Worker 0: 0s
    Worker 1: ~0.5s
    Worker 2: ~1.0s
    Worker 3: ~1.5s
    """
```

**Uso em BaseCollector**:

```python
with ThreadPoolExecutor(max_workers=4) as executor:
    futures = {
        executor.submit(self._process_single_period, period, index): period
        for index, period in enumerate(periods)
    }

def _process_single_period(self, period, index):
    staggered_delay(index)  # Delay baseado no indice
    csv = self._download_period(period)
    # ...
```

---

## Uso Direto

### QueryEngine

```python
from ifdata_bcb.infra import QueryEngine

qe = QueryEngine()

# Listar arquivos
arquivos = qe.list_files('cosif/prudencial')

# Ler com filtros
df = qe.read_glob(
    pattern='cosif_prud_*.parquet',
    subdir='cosif/prudencial',
    columns=['CNPJ_8', 'VALOR'],
    where="DATA_BASE = 202412"
)

# SQL direto
df = qe.sql("SELECT COUNT(*) FROM '{cache}/cosif/prudencial/*.parquet'")
```

### DataManager

```python
from ifdata_bcb.infra import DataManager

dm = DataManager()

# Verificar periodos
periodos = dm.get_available_periods('cosif_prud', 'cosif/prudencial')
ultimo = dm.get_last_period('cosif_prud', 'cosif/prudencial')

# Salvar DataFrame
dm.save(df, 'meu_arquivo', 'meu_subdir')

# Deletar
dm.delete('meu_arquivo', 'meu_subdir')
```

### Logging

```python
from ifdata_bcb.infra.log import get_logger, set_log_level, get_log_path

# Aumentar verbosidade
set_log_level("DEBUG")

# Verificar onde logs sao salvos
print(f"Logs em: {get_log_path()}")

# Usar logger
logger = get_logger("meu_modulo")
logger.info("Mensagem")
```

---

## Exports Publicos

```python
# infra/__init__.py
from ifdata_bcb.infra.config import get_cache_path, get_logs_path
from ifdata_bcb.infra.query import QueryEngine
from ifdata_bcb.infra.storage import DataManager
from ifdata_bcb.infra.log import get_logger, set_log_level, configure_logging
from ifdata_bcb.infra.cache import cached, clear_all_caches, get_cache_info
from ifdata_bcb.infra.resilience import retry, staggered_delay

__all__ = [
    "get_cache_path",
    "get_logs_path",
    "QueryEngine",
    "DataManager",
    "get_logger",
    "set_log_level",
    "configure_logging",
    "cached",
    "clear_all_caches",
    "get_cache_info",
    "retry",
    "staggered_delay",
]
```
