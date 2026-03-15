# Infraestrutura

A camada de infraestrutura fornece servicos tecnicos para a biblioteca.

## config.py

### Responsabilidades

Centraliza configuracao de paths e diretorios.

### Localizacao

```
src/ifdata_bcb/infra/config.py
```

### Constantes

```python
APP_NAME = "py-bacen"

SUBDIRS = {
    "cosif_individual": "cosif/individual",
    "cosif_prudencial": "cosif/prudencial",
    "ifdata_valores": "ifdata/valores",
    "ifdata_cadastro": "ifdata/cadastro",
}
```

### get_cache_path()

Retorna o caminho para cache de dados.

```python
def get_cache_path() -> Path:
    """
    Prioridade:
    1. Variavel de ambiente BACEN_DATA_DIR
    2. Diretorio de cache do sistema (XDG/AppData)
    """
```

**Paths por sistema**:

| Sistema | Caminho |
|---------|---------|
| Windows | `%LOCALAPPDATA%\py-bacen\Cache\` |
| Linux | `~/.cache/py-bacen/` |
| macOS | `~/Library/Caches/py-bacen/` |

**Customizacao via variavel de ambiente**:

```bash
# Windows PowerShell
$env:BACEN_DATA_DIR = "C:\dados\bcb"

# Linux/macOS
export BACEN_DATA_DIR="/dados/bcb"
```

### get_logs_path()

Retorna o caminho para diretorio de logs.

```python
def get_logs_path() -> Path:
    """Logs sao armazenados separadamente do cache."""
```

**Estrutura**:

```
py-bacen/
  Cache/
    cosif/
    ifdata/
  Logs/
    ifdata_2024-01-15.log
```

### get_subdir() / ensure_subdir()

```python
def get_subdir(dataset: str) -> str:
    """Retorna subdiretorio para um dataset."""

def ensure_subdir(dataset: str) -> Path:
    """Garante que subdiretorio existe e retorna Path."""
```

## QueryEngine (query.py)

### Responsabilidades

Motor de consultas DuckDB sobre arquivos Parquet.

### Localizacao

```
src/ifdata_bcb/infra/query.py
```

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

Le um arquivo Parquet especifico.

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

Le multiplos arquivos via glob pattern.

```python
def read_glob(
    self,
    pattern: str,        # Glob pattern (ex: "cosif_prud_*.parquet")
    subdir: str,         # Subdiretorio
    columns: list = None,  # Colunas
    where: str = None      # Filtro SQL
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

### sql()

Executa SQL arbitrario com substituicao de variaveis.

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
    SELECT CNPJ_8, SUM(VALOR) as total
    FROM '{cache}/cosif/prudencial/*.parquet'
    GROUP BY CNPJ_8
    ORDER BY total DESC
    LIMIT 10
""")
```

### describe()

Retorna schema do arquivo.

```python
def describe(self, filename: str, subdir: str) -> pd.DataFrame:
    """Retorna colunas: column_name, column_type, null, key, default, extra"""
```

### get_metadata()

Retorna metadados basicos.

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

Lista arquivos em um subdiretorio.

```python
def list_files(self, subdir: str, pattern: str = "*.parquet") -> list[str]:
    """Retorna lista de nomes (sem extensao)."""
```

### file_exists()

Verifica se arquivo existe.

```python
def file_exists(self, filename: str, subdir: str) -> bool:
```

## DataManager (storage.py)

### Responsabilidades

Gerenciador de persistencia em formato Parquet.

### Localizacao

```
src/ifdata_bcb/infra/storage.py
```

### Construtor

```python
class DataManager:
    def __init__(self, base_path: Optional[Path] = None):
```

### save()

Salva DataFrame em arquivo Parquet.

```python
def save(
    self,
    df: pd.DataFrame,
    filename: str,           # Nome sem extensao
    subdir: str,             # Subdiretorio
    compression: str = "snappy"  # snappy, gzip, zstd
) -> Path:
```

**Exemplo**:

```python
dm = DataManager()
path = dm.save(df, "cosif_prud_202412", "cosif/prudencial")
```

### read()

Le arquivo via QueryEngine.

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

Retorna ultimo periodo disponivel.

```python
def get_last_period(
    self,
    prefix: str,    # Ex: "cosif_prud"
    subdir: str     # Ex: "cosif/prudencial"
) -> Optional[tuple[int, int]]:
    """Retorna (ano, mes) ou None."""
```

**Exemplo**:

```python
dm = DataManager()
last = dm.get_last_period("cosif_prud", "cosif/prudencial")
# (2024, 12)
```

### get_available_periods()

Retorna todos os periodos disponiveis.

```python
def get_available_periods(
    self,
    prefix: str,
    subdir: str
) -> list[tuple[int, int]]:
    """Retorna lista de (ano, mes) ordenada."""
```

### delete()

Remove um arquivo.

```python
def delete(self, filename: str, subdir: str) -> bool:
    """Retorna True se removeu, False se nao existia."""
```

## Logging (log.py)

### Responsabilidades

Sistema de logging com dual output:
- Console (stderr): WARNING+ para usuario
- Arquivo: DEBUG+ para debugging tecnico

### Localizacao

```
src/ifdata_bcb/infra/log.py
```

### configure_logging()

Configura loguru com dual output.

```python
def configure_logging(
    level: str = "WARNING",      # Nivel minimo console
    enable_file: bool = True,    # Habilitar arquivo
    file_level: str = "DEBUG"    # Nivel minimo arquivo
):
```

**Estrutura do log**:

```
# Console (stderr)
WARNING  | Mensagem de aviso

# Arquivo (AppData/Local/py-bacen/Logs/ifdata_YYYY-MM-DD.log)
[2024-01-15 10:30:45] DEBUG    [ifdata_bcb.infra.query] Query: cosif/prudencial/...
[2024-01-15 10:30:46] INFO     [ifdata_bcb.services.base_collector] Saved: ...
```

### get_logger()

Retorna logger configurado.

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

Altera nivel de logging do console.

```python
def set_log_level(level: str):
    """Altera nivel (DEBUG, INFO, WARNING, ERROR, CRITICAL)."""
```

### get_log_path()

Retorna caminho do diretorio de logs.

```python
def get_log_path() -> Path:
```

## Cache (cache.py)

### Responsabilidades

Sistema de cache centralizado com metricas e gerenciamento global. Thread-safe.

### Localizacao

```
src/ifdata_bcb/infra/cache.py
```

### @cached Decorator

Decorator que wrapa `lru_cache` com funcionalidades adicionais.

```python
from ifdata_bcb.infra.cache import cached

@cached(maxsize=256, name="find_cnpj", track_stats=False)
def find_cnpj(identificador: str) -> str:
    # ...
```

**Parametros**:
- `maxsize`: Tamanho maximo do cache (padrao: 128)
- `name`: Nome para identificacao nas metricas
- `track_stats`: Se True, registra hits/misses

### CacheStats

Classe para estatisticas de cache. Thread-safe com `threading.Lock`.

```python
from ifdata_bcb.infra.cache import CacheStats

# Obter estatisticas de todos os caches
stats = CacheStats.get_stats()
# {'find_cnpj': {'hits': 10, 'misses': 5, 'total': 15, 'hit_rate': '66.7%'}}

# Limpar estatisticas
CacheStats.clear()
```

### clear_all_caches()

Limpa todos os caches registrados.

```python
from ifdata_bcb.infra.cache import clear_all_caches

count = clear_all_caches()
print(f"Limpos {count} caches")
```

### get_cache_info()

Retorna informacoes de todos os caches.

```python
from ifdata_bcb.infra.cache import get_cache_info

info = get_cache_info()
# {'EntityResolver.find_cnpj': {'hits': 10, 'misses': 5, 'maxsize': 256, 'currsize': 5}}
```

## Resilience (resilience.py)

### Responsabilidades

Utilitarios para lidar com falhas transientes em APIs externas.

### Localizacao

```
src/ifdata_bcb/infra/resilience.py
```

### Constantes

```python
DEFAULT_RETRY_ATTEMPTS = 3
DEFAULT_RETRY_DELAY = 1.0
DEFAULT_BACKOFF_FACTOR = 2.0
DEFAULT_REQUEST_TIMEOUT = 240
DEFAULT_PARALLEL_STAGGER = 0.5
```

### @retry Decorator

Adiciona retry com exponential backoff.

```python
def retry(
    max_attempts: int = 3,
    delay: float = 1.0,
    backoff_factor: float = 2.0,
    exceptions: tuple = TRANSIENT_EXCEPTIONS,
    jitter: bool = True
):
```

**Excecoes capturadas por padrao**:

```python
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

Adiciona delay escalonado para workers paralelos.

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

**Exemplo**:

```python
from ifdata_bcb.infra.resilience import staggered_delay

with ThreadPoolExecutor(max_workers=4) as executor:
    futures = {
        executor.submit(process_with_stagger, i, item): item
        for i, item in enumerate(items)
    }

def process_with_stagger(index, item):
    staggered_delay(index)  # Delay baseado no indice
    return process(item)
```

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
    where="DATA = 202412"
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
