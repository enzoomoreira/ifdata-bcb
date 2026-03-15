# Infraestrutura

A camada de infraestrutura fornece servicos tecnicos para a biblioteca.

## Localizacao

```
src/ifdata_bcb/infra/
|-- __init__.py           # Exports publicos
|-- config.py            # Settings (pydantic-settings)
|-- paths.py             # ensure_dir, temp_dir
|-- query.py             # QueryEngine (DuckDB)
|-- storage.py           # DataManager (Parquet)
|-- log.py               # Logging (Loguru)
|-- cache.py             # Cache LRU com registro global
+-- resilience.py        # Retry e backoff
```

---

## config.py (Settings)

Configuracao centralizada via pydantic-settings.

```python
class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="BACEN_")

    data_dir: Path = Path(user_cache_dir(APP_NAME, appauthor=False))

    @property
    def cache_path(self) -> Path: ...   # data_dir com mkdir

    @property
    def logs_path(self) -> Path: ...    # data_dir.parent / "Logs" com mkdir
```

```python
def get_settings() -> Settings:
    """Singleton. Retorna instancia unica de Settings."""
```

**Paths por sistema** (padrao, sem env var):

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

**Estrutura de diretorios**:

```
py-bacen/
|-- Cache/                   (ou valor de BACEN_DATA_DIR)
|   |-- cosif/
|   |   |-- individual/
|   |   +-- prudencial/
|   +-- ifdata/
|       |-- valores/
|       +-- cadastro/
+-- Logs/
    +-- ifdata_2024-01-15.log
```

---

## paths.py

Utilitarios de filesystem centralizados.

### ensure_dir()

```python
def ensure_dir(path: Path) -> Path:
    """Cria diretorio (e pais) se nao existir. Thread-safe."""
```

Usado por `DataManager.save()` para garantir que subdiretorios existam antes de gravar parquet.

### temp_dir()

```python
@contextmanager
def temp_dir(prefix: str) -> Generator[Path, None, None]:
    """Context manager para diretorio temporario com cleanup automatico."""
```

Cria diretorio em `%TEMP%` via `tempfile.mkdtemp`, remove com `shutil.rmtree` no `finally`.
Thread-safe (cada chamada cria diretorio independente).

**Uso em BaseCollector**:

```python
with temp_dir(prefix=f"{self._get_file_prefix()}_{period}") as work_dir:
    csv_path = self._download_period(period, work_dir)
    df = self._process_to_parquet(csv_path, period)
    # ... work_dir limpo automaticamente ao sair
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
        base_path: Path | None = None,  # Padrao: get_settings().cache_path
        progress_bar: bool = False          # Barra de progresso DuckDB
    ):
```

### has_glob()

Verifica se existe ao menos um arquivo para o glob informado:

```python
def has_glob(self, pattern: str, subdir: str) -> bool:
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
qe = QueryEngine()
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

---

## storage.py (DataManager)

### Responsabilidades

Gerenciador de persistencia em formato Parquet.

### Construtor

```python
class DataManager:
    def __init__(self, base_path: Path | None = None):
        # Padrao: get_settings().cache_path
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

### save_from_query()

Salva resultado de query DuckDB direto para Parquet (sem materializar em Pandas):

```python
def save_from_query(
    self,
    query: str,
    filename: str,
    subdir: str,
    compression: str = "snappy"
) -> Path:
```

### list_files()

```python
def list_files(self, subdir: str, pattern: str = "*.parquet") -> list[str]:
    """Retorna lista de nomes (sem extensao)."""
```

### get_metadata()

```python
def get_metadata(self, filename: str, subdir: str) -> dict | None:
    """Retorna {arquivo, subdir, registros, colunas, status} ou None."""
```

### get_available_periods()

```python
def get_available_periods(
    self,
    prefix: str,
    subdir: str
) -> list[tuple[int, int]]:
    """Retorna lista de (ano, mes) ordenada."""
```

### Funcoes de modulo

Funcoes standalone para uso sem instanciar DataManager:

```python
list_parquet_files(subdir, pattern, base_path=None) -> list[str]
parquet_exists(filename, subdir, base_path=None) -> bool
get_parquet_path(filename, subdir, base_path=None) -> Path
get_parquet_metadata(filename, subdir, base_path=None) -> dict | None
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

# Verificar existencia
qe.has_glob('cosif_prud_*.parquet', 'cosif/prudencial')

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

# Salvar DataFrame
dm.save(df, 'meu_arquivo', 'meu_subdir')
```

### Settings

```python
from ifdata_bcb.infra import get_settings

settings = get_settings()
print(f"Cache: {settings.cache_path}")
print(f"Logs: {settings.logs_path}")
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
from ifdata_bcb.infra.cache import cached, clear_all_caches, get_cache_info
from ifdata_bcb.infra.config import Settings, get_settings
from ifdata_bcb.infra.log import configure_logging, get_log_path, get_logger, set_log_level
from ifdata_bcb.infra.paths import ensure_dir, temp_dir
from ifdata_bcb.infra.query import QueryEngine
from ifdata_bcb.infra.resilience import retry
from ifdata_bcb.infra.storage import (
    DataManager,
    get_parquet_metadata,
    get_parquet_path,
    list_parquet_files,
    parquet_exists,
)
```
