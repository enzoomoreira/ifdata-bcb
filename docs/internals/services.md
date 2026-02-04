# Servicos

A camada de servicos fornece funcionalidades transversais usadas por multiplos providers.

## BaseCollector

### Responsabilidades

Classe base que implementa logica comum de coleta:
- Download com retry e backoff
- Processamento paralelo de periodos
- Persistencia em Parquet
- Dual output (Display + Logger)

### Localizacao

```
src/ifdata_bcb/services/base_collector.py
```

### Template Method Pattern

O `BaseCollector` define o esqueleto do algoritmo de coleta:

```python
class BaseCollector(ABC):
    def collect(self, start, end, force=False):
        periods = self._generate_periods(start, end)   # Template
        for period in periods:
            csv = self._download_period(period)         # Abstract
            df = self._process_to_parquet(csv, period)  # Abstract
            self.dm.save(df, filename, subdir)          # Template
```

### Metodos Abstratos

Subclasses **devem** implementar:

```python
@abstractmethod
def _get_file_prefix(self) -> str:
    """Prefixo do arquivo (ex: 'cosif_ind')."""

@abstractmethod
def _get_subdir(self) -> str:
    """Subdiretorio (ex: 'cosif/individual')."""

@abstractmethod
def _download_period(self, period: int) -> Optional[Path]:
    """Baixa dados de um periodo. Retorna Path do CSV ou None."""

@abstractmethod
def _process_to_parquet(self, csv_path: Path, period: int) -> Optional[pd.DataFrame]:
    """Processa CSV e retorna DataFrame normalizado."""
```

### Atributos de Classe

```python
_PERIOD_TYPE: str = "monthly"  # 'monthly' ou 'quarterly'
_MAX_WORKERS: int = 4          # Workers para coleta paralela
```

### Coleta Paralela

O `collect()` usa `ThreadPoolExecutor` para paralelismo:

```python
def collect(self, start, end, force=False, verbose=True):
    periods = self._get_missing_periods(start, end)

    with ThreadPoolExecutor(max_workers=self._MAX_WORKERS) as executor:
        futures = {
            executor.submit(self._process_single_period, p, i): p
            for i, p in enumerate(periods)
        }
        for future in as_completed(futures):
            registros, sucesso, erro = future.result()
```

### Dual Output

Integra Display (visual) + Logger (arquivo):

```python
def _start(self, title, num_items, verbose=True):
    """Banner de inicio (console + arquivo)."""
    self.display.banner(title, indicator_count=num_items)
    self.logger.info(f"Coleta iniciada: {num_items} periodos")

def _end(self, verbose=True, periodos=None, falhas=None):
    """Banner de conclusao com estatisticas."""
    self.display.end_banner(total=total, periodos=periodos, falhas=falhas)
    self.logger.info(f"Coleta concluida: {total:,} registros")
```

### Exemplo de Implementacao

```python
class COSIFCollector(BaseCollector):
    _PERIOD_TYPE = "monthly"

    def _get_file_prefix(self):
        return "cosif_ind"

    def _get_subdir(self):
        return "cosif/individual"

    @retry(delay=2.0)
    def _download_period(self, period):
        url = f"https://bcb.gov.br/cosif/{period}BANCOS.csv.zip"
        response = requests.get(url, timeout=120)
        # ... processar ZIP e retornar Path do CSV

    def _process_to_parquet(self, csv_path, period):
        conn = duckdb.connect()
        df = conn.sql(f"SELECT * FROM read_csv('{csv_path}', ...)").df()
        # ... normalizar colunas
        return df
```

## EntityResolver

### Responsabilidades

Resolve identificadores (nomes/CNPJs) em CNPJs canonicos e metadados.

### Localizacao

```
src/ifdata_bcb/services/entity_resolver.py
```

### Construtor

```python
class EntityResolver:
    def __init__(
        self,
        query_engine: QueryEngine = None,
        enable_fuzzy: bool = True,
        fuzzy_threshold_auto: int = 85,    # Auto-aceita se score >= 85
        fuzzy_threshold_suggest: int = 70   # Sugere se score >= 70
    ):
```

### find_cnpj()

Encontra CNPJ_8 a partir de nome ou CNPJ.

```python
@lru_cache(maxsize=256)
def find_cnpj(self, identificador: str) -> str:
    """
    Args:
        identificador: Nome ou CNPJ de 8 digitos

    Returns:
        CNPJ de 8 digitos

    Raises:
        EntityNotFoundError: Se nao encontrar
        AmbiguousIdentifierError: Se multiplas correspondencias
    """
```

**Fluxo de resolucao**:
1. Se ja e CNPJ de 8 digitos, retorna direto
2. Busca exata no mapeamento
3. Busca parcial (contains)
4. Fuzzy matching (se habilitado)
5. Levanta `EntityNotFoundError`

### find_cnpj_fuzzy()

Busca fuzzy explicita retornando multiplas correspondencias.

```python
def find_cnpj_fuzzy(
    self,
    identificador: str,
    limit: int = 5,
    score_cutoff: int = 70
) -> list[tuple[str, str, int]]:
    """
    Retorna: [(nome, cnpj, score), ...]
    """
```

### get_entity_identifiers()

Obtem metadados completos da entidade.

```python
@lru_cache(maxsize=256)
def get_entity_identifiers(self, cnpj_8: str) -> dict:
    """
    Retorna:
        {
            'cnpj_interesse': str,
            'cnpj_reporte_cosif': str,  # Pode ser do lider
            'cod_congl_prud': str,
            'nome_entidade': str
        }
    """
```

### resolve_full()

Resolve completamente em uma operacao.

```python
@lru_cache(maxsize=256)
def resolve_full(self, identificador: str) -> ResolvedEntity:
    """
    Retorna ResolvedEntity (dataclass):
        - cnpj_interesse
        - cnpj_reporte_cosif
        - cod_congl_prud
        - nome_entidade
        - identificador_original
    """
```

### ResolvedEntity

Dataclass imutavel com identificadores resolvidos:

```python
@dataclass(frozen=True)
class ResolvedEntity:
    cnpj_interesse: Optional[str]
    cnpj_reporte_cosif: Optional[str]
    cod_congl_prud: Optional[str]
    nome_entidade: Optional[str]
    identificador_original: str
```

### Exemplo de Uso

```python
from ifdata_bcb.services import EntityResolver

resolver = EntityResolver()

# Resolver nome
cnpj = resolver.find_cnpj('Itau Unibanco')  # '60872504'

# Resolver completamente
entity = resolver.resolve_full('60872504')
print(entity.nome_entidade)      # 'ITAU UNIBANCO HOLDING S.A.'
print(entity.cod_congl_prud)     # 'C0080099'
print(entity.cnpj_reporte_cosif) # '60872504'
```

## EntitySearcher

### Responsabilidades

Busca entidades por nome em todas as fontes de dados, implementando o padrao "search + select".

### Localizacao

```
src/ifdata_bcb/services/entity_searcher.py
```

### Construtor

```python
class EntitySearcher:
    def __init__(
        self,
        query_engine: QueryEngine = None,
        fuzzy_threshold: int = 50
    ):
```

### search()

Busca entidades por nome.

```python
def search(self, termo: str, limit: int = 10) -> pd.DataFrame:
    """
    Args:
        termo: Nome parcial ou completo
        limit: Maximo de resultados

    Returns:
        DataFrame com colunas:
        - CNPJ_8: CNPJ de 8 digitos
        - NOME: Nome da instituicao
        - FONTES: Fontes onde aparece (cadastro,cosif_ind,cosif_prud)
        - SCORE: Similaridade (0-100)
    """
```

### list_all()

Lista todas as entidades.

```python
def list_all(self, limit: int = 100) -> pd.DataFrame:
    """Retorna DataFrame com CNPJ_8, NOME, FONTES (sem SCORE)."""
```

### Fontes Agregadas

O EntitySearcher agrega dados de:
- IFDATA Cadastro (fonte primaria)
- COSIF Individual
- COSIF Prudencial

```python
_CADASTRO_SUBDIR = "ifdata/cadastro"
_COSIF_IND_SUBDIR = "cosif/individual"
_COSIF_PRUD_SUBDIR = "cosif/prudencial"
```

### Exemplo de Uso

```python
from ifdata_bcb.services import EntitySearcher

searcher = EntitySearcher()

# Buscar por nome
df = searcher.search('Bradesco')
#    CNPJ_8                  NOME                          FONTES  SCORE
# 0  60746948  BANCO BRADESCO S.A.  cadastro,cosif_ind,cosif_prud    100

# Listar todas
df = searcher.list_all(limit=50)
```

## FuzzyMatcher

### Responsabilidades

Wrapper para logica de fuzzy matching usando `thefuzz`.

### Localizacao

```
src/ifdata_bcb/utils/fuzzy_matcher.py
```

### Construtor

```python
class FuzzyMatcher:
    def __init__(
        self,
        threshold_auto: int = 85,    # Auto-aceita
        threshold_suggest: int = 70   # Sugere
    ):
```

### search()

Busca fuzzy retornando matches ordenados.

```python
def search(
    self,
    query: str,
    choices: dict[str, str],  # {nome: cnpj}
    limit: int = 5,
    score_cutoff: int = 0
) -> list[tuple[str, int]]:
    """Retorna: [(chave, score), ...]"""
```

**Algoritmo**: Usa `fuzz.token_set_ratio` do `thefuzz` para tolerancia a ordem de palavras.

### get_best_match()

Retorna melhor match com indicador de confianca.

```python
def get_best_match(
    self,
    query: str,
    choices: dict[str, str]
) -> tuple[str, int, bool]:
    """Retorna: (melhor_chave, score, is_auto_accepted)"""
```

### get_suggestions()

Retorna sugestoes acima do threshold.

```python
def get_suggestions(
    self,
    query: str,
    choices: dict[str, str],
    limit: int = 3
) -> list[tuple[str, int]]:
```

### Exemplo de Uso

```python
from ifdata_bcb.utils.fuzzy import FuzzyMatcher

matcher = FuzzyMatcher(threshold_auto=85, threshold_suggest=70)

choices = {
    "BANCO DO BRASIL S.A.": "00000000",
    "BANCO ITAU UNIBANCO S.A.": "60872504",
    "BANCO BRADESCO S.A.": "60746948"
}

# Buscar
matches = matcher.search("BANCO BRASIL", choices, limit=3)
# [("BANCO DO BRASIL S.A.", 95), ...]

# Melhor match
best, score, auto = matcher.get_best_match("ITAU", choices)
# ("BANCO ITAU UNIBANCO S.A.", 90, True)
```

## Display

### Responsabilidades

Sistema de display para output visual usando Rich.

### Localizacao

```
src/ifdata_bcb/ui/display.py
```

### Singleton

```python
def get_display(verbose: bool = True) -> Display:
    """
    Retorna instancia unica (Singleton thread-safe).
    Double-checked locking para performance.
    """
```

### Construtor

```python
class Display:
    def __init__(
        self,
        verbose: bool = True,
        stream: TextIO = sys.stdout,
        colors: bool = True
    ):
```

### banner()

Exibe banner de inicio.

```python
def banner(
    self,
    title: str,
    subtitle: str = None,
    first_run: bool = None,
    indicator_count: int = None
):
```

### end_banner()

Exibe banner de conclusao com estatisticas.

```python
def end_banner(
    self,
    total: int = None,      # Registros coletados
    periodos: int = None,   # Periodos OK
    falhas: int = None      # Periodos com falha
):
```

### progress()

Retorna iterador com barra de progresso.

```python
def progress(
    self,
    iterable: Iterable,
    total: int = None,
    desc: str = None,
    leave: bool = False
) -> Iterator:
```

**Exemplo**:

```python
display = get_display()

for item in display.progress(items, total=100, desc="Baixando"):
    download(item)
```

### Mensagens

```python
def print_warning(self, message: str):
    """[!] Mensagem em amarelo"""

def print_error(self, message: str):
    """[X] Mensagem em vermelho (sempre exibe)"""

def print_success(self, message: str):
    """[OK] Mensagem em verde"""

def print_info(self, message: str):
    """[i] Mensagem informativa"""
```

### Deteccao de Jupyter

O Display detecta automaticamente ambiente Jupyter e ajusta comportamento:
- Desabilita `transient` para evitar duplicacao
- Reduz refresh rate

```python
detect_console = Console()
self._is_jupyter = detect_console.is_jupyter
```

### Exemplo de Uso

```python
from ifdata_bcb.ui.display import get_display

display = get_display(verbose=True)

display.banner("Coletando COSIF", indicator_count=12)

for item in display.progress(items, total=12, desc="Periodos"):
    process(item)

display.end_banner(total=50000, periodos=12)
```

## Exports Publicos

### services/__init__.py

```python
from ifdata_bcb.services.entity_resolver import EntityResolver, ResolvedEntity
from ifdata_bcb.services.entity_searcher import EntitySearcher
from ifdata_bcb.services.base_collector import BaseCollector

__all__ = [
    "EntityResolver",
    "ResolvedEntity",
    "EntitySearcher",
    "BaseCollector",
]
```

### Uso

```python
from ifdata_bcb.services import EntityResolver, EntitySearcher
```
