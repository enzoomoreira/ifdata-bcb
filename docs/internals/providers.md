# Providers

A camada de providers implementa coleta e leitura para cada fonte de dados do BCB.

## Localizacao

```
src/ifdata_bcb/providers/
|-- __init__.py              # Exports publicos
|-- base_explorer.py        # Classe base abstrata para explorers
|-- base_collector.py       # Template para coleta
|-- collector_models.py     # CollectStatus enum
|-- enrichment.py           # Enriquecimento cadastral inline
|-- cosif/                   # COSIF (mensal)
|   |-- __init__.py
|   |-- collector.py        # COSIFCollector
|   +-- explorer.py         # COSIFExplorer
+-- ifdata/                  # IFDATA (trimestral)
    |-- __init__.py
    |-- collector.py        # IFDATAValoresCollector, IFDATACadastroCollector
    |-- valores_explorer.py # IFDATAExplorer
    |-- cadastro_explorer.py # CadastroExplorer
    |-- scope.py            # resolve_ifdata_escopo()
    +-- temporal.py         # TemporalResolver (resolucao temporal por periodo)
```

---

## base_collector.py

### Responsabilidades

Classe base que implementa logica comum de coleta:
- Download com retry e backoff
- Processamento paralelo de periodos
- Persistencia em Parquet
- Dual output (Display + Logger)

### Atributos de Classe

```python
_PERIOD_TYPE: str = "monthly"  # 'monthly' ou 'quarterly'
_MAX_WORKERS: int = 4          # Workers para coleta paralela
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
def _download_period(self, period: int, work_dir: Path) -> Path | None:
    """Baixa dados de um periodo para work_dir. Retorna Path do CSV ou None."""

@abstractmethod
def _process_to_parquet(self, data_path: Path, period: int) -> pd.DataFrame | None:
    """Processa dados e retorna DataFrame normalizado.
    data_path: CSV (COSIF) ou diretorio com CSVs (IFDATA Valores)."""
```

### _download_single()

Metodo compartilhado para download de arquivos com retry:

```python
@retry(delay=2.0)
def _download_single(self, url: str, output_path: Path) -> bool:
    """Baixa um arquivo da URL e salva em output_path."""
```

Herdado por todos os collectors (COSIF, IFDATA Valores, IFDATA Cadastro).

### collect()

Metodo principal de coleta:

```python
def collect(
    self,
    start: str,
    end: str,
    force: bool = False,
    verbose: bool = True,
    progress_desc: str | None = None,
) -> tuple[int, int, int, int]:
    """
    Coleta dados do BCB.

    Args:
        start: Periodo inicial (YYYY-MM)
        end: Periodo final (YYYY-MM)
        force: Se True, redownload todos os periodos
        verbose: Se True, mostra progresso
        progress_desc: Descricao na barra de progresso

    Retorna:
        (total_registros, periodos_ok, falhas, indisponiveis)
    """
```

### Fluxo Interno

```
collect(start, end)
    |
    +-- _generate_periods(start, end)
    |   +-- generate_month_range() ou generate_quarter_range()
    |   +-- _filter_by_availability()  # Remove periodos antes do cutoff
    |   --> [202401, 202402, ..., 202412]
    |
    +-- _get_missing_periods() (se force=False)
    |   --> Filtra periodos ja coletados
    |
    +-- Display.banner()
    |
    +-- ThreadPoolExecutor(max_workers=4)
        |
        +-- Worker 0:
        |   +-- staggered_delay(0) --> 0s
        |   +-- _process_single_period(202401, 0)
        |       +-- temp_dir(prefix="cosif_ind_202401") as work_dir
        |       |   +-- _download_period(202401, work_dir)
        |       |   |   +-- @retry(max_attempts=3)
        |       |   |   +-- requests.get(url)
        |       |   |   --> Path do CSV ou None
        |       |   +-- _process_to_parquet(csv_path, 202401)
        |       |   |   --> pd.DataFrame normalizado
        |       |   +-- dm.save(df, 'cosif_ind_202401', 'cosif/individual')
        |       |   --> (registros, CollectStatus.SUCCESS, None)
        |       +-- work_dir limpo automaticamente
        |
        +-- Worker 1, 2, 3... (paralelo com delays escalonados)
    |
    +-- Display.end_banner()
    |
    --> (total, ok, falhas, indisponiveis)
```

### Dual Output

O collector integra Display (visual) + Logger (arquivo):

```python
def _start(self, title, num_items, verbose=True):
    """Banner de inicio."""
    self.display.banner(title, indicator_count=num_items)
    self.logger.info(f"Coleta iniciada: {num_items} periodos")

def _end(self, verbose=True, periodos=None, falhas=None):
    """Banner de conclusao."""
    self.display.end_banner(total=total, periodos=periodos, falhas=falhas)
    self.logger.info(f"Coleta concluida: {total:,} registros")
```

### CollectStatus

Enum para status de coleta:

```python
# collector_models.py
class CollectStatus(Enum):
    SUCCESS = auto()      # Arquivo salvo
    UNAVAILABLE = auto()  # Periodo nao disponivel no BCB
    FAILED = auto()       # Erro no download/processamento
```

---

## cosif/collector.py (COSIFCollector)

### Especificidades

- **Periodicidade**: Mensal (`_PERIOD_TYPE = "monthly"`)
- **Fonte**: CSV compactado em ZIP do BCB
- **Escopos**: individual e prudencial (collectors separados)

### Implementacao

```python
class COSIFCollector(BaseCollector):
    _PERIOD_TYPE = "monthly"

    def __init__(self, escopo: Literal["individual", "prudencial"] = "individual"):
        self.escopo = escopo
        # ...

    def _get_file_prefix(self):
        return "cosif_ind" if self.escopo == "individual" else "cosif_prud"

    def _get_subdir(self):
        return f"cosif/{self.escopo}"

    @retry(max_attempts=3, delay=2.0)
    def _download_period(self, period: int, work_dir: Path) -> Path | None:
        """
        URL: https://www4.bcb.gov.br/fis/cosif/balancetes/{YYYYMM}BANCOS.CSV.zip
        Ou para prudencial: {YYYYMM}CONGL.CSV.zip
        """
        # Download para work_dir, extrai ZIP, retorna Path do CSV

    def _process_to_parquet(self, csv_path: Path, period: int) -> pd.DataFrame | None:
        """
        - Detecta era via eras.detect_cosif_csv_era()
        - Gera SQL normalizado via eras.build_cosif_select()
        - Suporta todas as eras (Era 1/2/3)
        - Normaliza NOME_CONTA para UPPER
        - Adiciona coluna ESCOPO
        """
```

---

## cosif/explorer.py (COSIFExplorer)

### Especificidades

- **Multi-source**: individual + prudencial
- **Nomes canônicos**: Substitui nomes do COSIF por nomes do cadastro via `get_canonical_names_for_cnpjs()`
- **Mapeamento de colunas**:
  - `DATA_BASE` -> `DATA`
  - `NOME_INSTITUICAO` -> `INSTITUICAO`
  - `NOME_CONTA` -> `CONTA`
  - `SALDO` -> `VALOR`

### Implementacao

```python
class COSIFExplorer(BaseExplorer):
    _COLUMN_MAP = {
        "DATA_BASE": "DATA",
        "NOME_INSTITUICAO": "INSTITUICAO",
        "NOME_CONTA": "CONTA",
        "CONTA": "COD_CONTA",
        "SALDO": "VALOR",
    }

    _DROP_COLUMNS: list[str] = []
    _PASSTHROUGH_COLUMNS: set[str] = {"CNPJ_8", "DOCUMENTO"}
    _DATE_COLUMN = "DATA_BASE"

    _ESCOPOS = {
        "individual": {"subdir": "cosif/individual", "prefix": "cosif_ind"},
        "prudencial": {"subdir": "cosif/prudencial", "prefix": "cosif_prud"},
    }

    def _get_sources(self):
        return self._ESCOPOS

    def _apply_canonical_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """Substitui aliases do COSIF por nomes canônicos do cadastro."""

    def read(
        self,
        start: str,
        end: str | None = None,
        *,
        instituicao: InstitutionInput | None = None,
        escopo: Literal["individual", "prudencial"] | None = None,
        conta: AccountInput | None = None,
        documento: str | list[str] | None = None,
        columns: list[str] | None = None,
        cadastro: list[str] | None = None,
    ) -> pd.DataFrame:
        """
        Le dados COSIF.
        instituicao e keyword-only e opcional (bulk read quando None).
        Apos finalizacao, aplica nomes canônicos do cadastro.
        """

    def collect(
        self,
        start: str,
        end: str,
        escopo: Literal["individual", "prudencial"] | None = None,
        force: bool = False,
    ):
        """
        Coleta dados COSIF.

        Args:
            escopo: Se None, coleta ambos escopos em paralelo
        """
```

### Colunas Disponiveis

| Coluna | Descricao |
|--------|-----------|
| DATA | Data do balancete (datetime) |
| CNPJ_8 | CNPJ de 8 digitos |
| INSTITUICAO | Nome da instituicao |
| ESCOPO | "individual" ou "prudencial" |
| COD_CONTA | Codigo da conta COSIF |
| CONTA | Nome da conta |
| DOCUMENTO | Tipo de documento |
| VALOR | Saldo em reais |

---

## ifdata/collector.py

### IFDATAValoresCollector

- **Periodicidade**: Trimestral (`_PERIOD_TYPE = "quarterly"`)
- **Fonte**: API OData do BCB
- **Prefix**: `ifdata_val`

```python
class IFDATAValoresCollector(BaseCollector):
    _PERIOD_TYPE = "quarterly"

    def _get_file_prefix(self):
        return "ifdata_val"

    def _get_subdir(self):
        return "ifdata/valores"

    def _download_period(self, period: int, work_dir: Path) -> Path | None:
        """
        URL OData: https://olinda.bcb.gov.br/olinda/servico/IFData/...
        Parametro: AnoMes={YYYYMM}
        Formato: CSV
        """
```

### IFDATACadastroCollector

- **Periodicidade**: Trimestral
- **Fonte**: API OData do BCB
- **Prefix**: `ifdata_cad`

```python
class IFDATACadastroCollector(BaseCollector):
    _PERIOD_TYPE = "quarterly"

    def _get_file_prefix(self):
        return "ifdata_cad"

    def _get_subdir(self):
        return "ifdata/cadastro"
```

---

## ifdata/valores_explorer.py (IFDATAExplorer)

### Especificidades

- **Resolucao de escopo**: Usa EntityLookup para resolver CNPJ -> codigo IFDATA
- **Nomes canônicos**: Usa `get_canonical_names_for_cnpjs()` do cadastro
- **Mapeamento de reporters**: Resolve chaves de reporte para entidades analiticas
- **Mapeamento de colunas**:
  - `AnoMes` -> `DATA`
  - `CodInst` -> `COD_INST`
  - `NomeColuna` -> `CONTA`
  - `Saldo` -> `VALOR`

### Implementacao

```python
class IFDATAExplorer(BaseExplorer):
    _COLUMN_MAP = {
        "AnoMes": "DATA",
        "CodInst": "COD_INST",
        "NomeColuna": "CONTA",
        "Conta": "COD_CONTA",
        "Saldo": "VALOR",
        "NomeRelatorio": "RELATORIO",
        "Grupo": "GRUPO",
    }

    def read(
        self,
        start: str,
        end: str | None = None,
        *,
        instituicao: InstitutionInput | None = None,
        escopo: Literal["individual", "prudencial", "financeiro"] | None = None,
        conta: AccountInput | None = None,
        relatorio: str | None = None,
        grupo: str | None = None,
        columns: list[str] | None = None,
        cadastro: list[str] | None = None,
    ) -> pd.DataFrame:
        """
        Args:
            instituicao: Se None, bulk read (todas as instituicoes).
            escopo: "individual", "prudencial", ou "financeiro"
            relatorio: Filtrar por relatorio (Ativo, Passivo, DRE, Resumo)
            grupo: Filtrar por grupo de conta
        """
```

### Resolucao de Escopo

```python
from ifdata_bcb.providers.ifdata.scope import resolve_ifdata_escopo

def _resolve_scope(self, cnpj_8: str, escopo: str) -> ScopeResolution:
    """
    Usa resolve_ifdata_escopo() para resolver CNPJ -> codigo IFDATA.

    individual: CNPJ direto, TipoInstituicao=3
    prudencial: CodConglomeradoPrudencial, TipoInstituicao=1
    financeiro: CodConglomeradoFinanceiro ou CNPJ direto, TipoInstituicao=2
    """
    return resolve_ifdata_escopo(self._resolver, cnpj_8, escopo)
```

### Mapeamento de Reporters

O metodo `_resolve_mapeamento()` cruza dados do IFDATA com o cadastro para mapear
chaves de reporte (COD_INST) para entidades analiticas (CNPJ_8):

- **Individual**: COD_INST = CNPJ_8 direto
- **Prudencial**: COD_INST pode ser CodConglomeradoPrudencial ou CNPJ direto
- **Financeiro**: COD_INST pode ser CodConglomeradoFinanceiro ou CNPJ direto

### list_instituicoes()

Retorna visao centrada em entidades com disponibilidade por escopo.

**IFDATA** (`bcb.ifdata.list_instituicoes()`):

| Coluna | Descricao |
|--------|-----------|
| CNPJ_8 | CNPJ de 8 digitos |
| INSTITUICAO | Nome canonico do cadastro |
| TEM_INDIVIDUAL | bool |
| TEM_PRUDENCIAL | bool |
| TEM_FINANCEIRO | bool |
| COD_INST_INDIVIDUAL | Codigo(s) de reporte |
| COD_INST_PRUDENCIAL | Codigo(s) de reporte |
| COD_INST_FINANCEIRO | Codigo(s) de reporte |

**COSIF** (`bcb.cosif.list_instituicoes()`):

| Coluna | Descricao |
|--------|-----------|
| CNPJ_8 | CNPJ de 8 digitos |
| INSTITUICAO | Nome canonico do cadastro |
| TEM_INDIVIDUAL | bool |
| TEM_PRUDENCIAL | bool |

### list_mapeamento()

Lista chaves operacionais de reporte por entidade e escopo:

| Coluna | Descricao |
|--------|-----------|
| COD_INST | Codigo de reporte no IFDATA |
| TIPO_INST | 1, 2 ou 3 |
| ESCOPO | individual, prudencial, financeiro |
| REPORT_KEY_TYPE | "cnpj" ou nome do escopo |
| CNPJ_8 | CNPJ da entidade associada |
| INSTITUICAO | Nome canônico |

### Colunas Disponiveis (read)

| Coluna | Descricao |
|--------|-----------|
| DATA | Data do trimestre (datetime) |
| CNPJ_8 | CNPJ de 8 digitos |
| INSTITUICAO | Nome da instituicao (canônico do cadastro) |
| ESCOPO | "individual", "prudencial", "financeiro" |
| COD_INST | Codigo no IFDATA |
| COD_CONTA | Codigo numerico da conta |
| CONTA | Nome da conta |
| VALOR | Saldo em reais |
| RELATORIO | Ativo, Passivo, DRE, Resumo |
| GRUPO | Grupo da conta |

---

## ifdata/cadastro_explorer.py (CadastroExplorer)

### Especificidades

- **Fonte unica**: ifdata/cadastro
- **Filtragem de entidades reais**: Todas as queries filtram aliases via `real_entity_condition()`
- **Drop de colunas internas**: `CodInst` removido do output
- **Mapeamento extenso de colunas**

### Implementacao

```python
class CadastroExplorer(BaseExplorer):
    _DROP_COLUMNS = ["CodInst"]

    _COLUMN_MAP = {
        "Data": "DATA",
        "NomeInstituicao": "INSTITUICAO",
        "SegmentoTb": "SEGMENTO",
        "CodConglomeradoPrudencial": "COD_CONGL_PRUD",
        "CodConglomeradoFinanceiro": "COD_CONGL_FIN",
        "Situacao": "SITUACAO",
        "Atividade": "ATIVIDADE",
        # ...
    }

    def read(
        self,
        start: str,
        end: str | None = None,
        *,
        instituicao: InstitutionInput | None = None,
        segmento: str | None = None,
        uf: str | None = None,
        situacao: str | None = None,
        atividade: str | None = None,
        tcb: str | None = None,
        td: str | None = None,
        tc: str | int | None = None,
        sr: str | None = None,
        municipio: str | None = None,
        columns: list[str] | None = None,
    ) -> pd.DataFrame:
        """
        Args:
            start: Periodo obrigatorio.
            instituicao: CNPJ de 8 digitos (opcional para listar todas)
            segmento: Filtrar por segmento
            uf: Filtrar por UF
            atividade, tcb, td, tc, sr, municipio: Novos filtros (case/accent insensitive)
        """

    def info(self, instituicao: str, start: str | None = None) -> dict | None:
        """Retorna dict com info da instituicao. None se nao encontrar."""

    def list_segmentos(self) -> list[str]:
        """Lista segmentos disponiveis."""

    def list_ufs(self) -> list[str]:
        """Lista UFs disponiveis."""

    def get_conglomerate_members(self, cod_congl: str, start: str | None = None) -> pd.DataFrame:
        """Retorna membros de um conglomerado prudencial."""
```

### Colunas Disponiveis

| Coluna | Descricao |
|--------|-----------|
| DATA | Data do trimestre |
| CNPJ_8 | CNPJ de 8 digitos |
| INSTITUICAO | Nome |
| SEGMENTO | Segmento regulatorio |
| COD_CONGL_PRUD | Codigo conglomerado prudencial |
| COD_CONGL_FIN | Codigo conglomerado financeiro |
| CNPJ_LIDER_8 | CNPJ do lider do conglomerado |
| SITUACAO | A (Ativa) ou I (Inativa) |
| ATIVIDADE | Tipo de atividade |
| UF | Estado |
| MUNICIPIO | Municipio |

---

## providers/enrichment.py

### Responsabilidades

Modulo de enriquecimento cadastral inline. Permite adicionar colunas cadastrais ao resultado de `cosif.read()` e `ifdata.read()` via parametro `cadastro=`.

### validate_cadastro_columns()

```python
VALID_CADASTRO_COLUMNS = {
    "SEGMENTO", "COD_CONGL_PRUD", "COD_CONGL_FIN", "SITUACAO",
    "ATIVIDADE", "TCB", "TD", "TC", "UF", "MUNICIPIO", "SR",
    "DATA_INICIO_ATIVIDADE", "NOME_CONGL_PRUD",
}

def validate_cadastro_columns(columns: list[str] | None) -> None:
    """Valida nomes de colunas cadastrais. Raises InvalidScopeError."""
```

### enrich_with_cadastro()

```python
def enrich_with_cadastro(
    df: pd.DataFrame,
    cadastro_columns: list[str],
    query_engine: QueryEngine,
    entity_lookup: EntityLookup,
) -> pd.DataFrame:
    """
    Enriquece DataFrame financeiro com colunas cadastrais.

    Usa ASOF LEFT JOIN no DuckDB para alinhamento temporal backward-looking:
    cada linha financeira recebe os atributos cadastrais do trimestre
    mais recente <= sua data.

    Para data unica: LEFT JOIN com ROW_NUMBER para pegar registro mais recente.
    Para time-series: ASOF LEFT JOIN via DuckDB SQL.

    Suporta coluna derivada NOME_CONGL_PRUD: nome da instituicao lider
    do conglomerado prudencial, resolvida via lookup SQL interno.
    """
```

---

## ifdata/scope.py

### Responsabilidades

Resolucao de escopo IFDATA a partir de CNPJ. Extraido do `EntityLookup` para modulo independente.

### resolve_ifdata_escopo()

```python
def resolve_ifdata_escopo(
    entity_lookup: EntityLookup,
    cnpj_8: str,
    escopo: str,
) -> ScopeResolution:
    """
    Resolve CNPJ para codigo IFDATA baseado no escopo.

    individual: CNPJ direto, TipoInstituicao=3
    prudencial: CodConglomeradoPrudencial, TipoInstituicao=1
    financeiro: Verifica CodConglomeradoFinanceiro e CNPJ direto, TipoInstituicao=2

    Raises:
        DataUnavailableError: Entidade nao tem dados para o escopo.
        InvalidScopeError: Escopo invalido.
    """
```

---

## ifdata/temporal.py

### Responsabilidades

Resolucao temporal de CNPJs para codigos IFDATA por periodo. Trata o fato de que codigos de conglomerado podem mudar ao longo do tempo.

### TemporalResolver

Classe que agrupa periodos com mesmo `cod_inst` para um escopo, permitindo queries eficientes quando o codigo de reporte muda ao longo do range temporal.

---

## Exemplo de Implementacao de Novo Provider

### 1. Criar Collector

```python
# providers/novo/collector.py
from ifdata_bcb.providers.base_collector import BaseCollector
from ifdata_bcb.infra.resilience import retry

class NovoCollector(BaseCollector):
    _PERIOD_TYPE = "monthly"  # ou "quarterly"

    def _get_file_prefix(self):
        return "novo"

    def _get_subdir(self):
        return "novo/dados"

    @retry(max_attempts=3, delay=2.0)
    def _download_period(self, period: int, work_dir: Path) -> Path | None:
        url = f"https://api.exemplo.com/dados/{period}"
        response = requests.get(url, timeout=120)
        response.raise_for_status()
        output_path = work_dir / f"novo_{period}.csv"
        output_path.write_bytes(response.content)
        return output_path

    def _process_to_parquet(self, csv_path: Path, period: int) -> pd.DataFrame | None:
        df = pd.read_csv(csv_path)
        # Normalizacoes
        return df
```

### 2. Criar Explorer

```python
# providers/novo/explorer.py
from ifdata_bcb.providers.base_explorer import BaseExplorer
from ifdata_bcb.infra.sql import join_conditions

class NovoExplorer(BaseExplorer):
    _COLUMN_MAP = {
        "data_original": "DATA",
        "valor_original": "VALOR",
    }

    def _get_subdir(self):
        return "novo/dados"

    def _get_file_prefix(self):
        return "novo"

    def read(self, instituicao, start, end=None, **kwargs):
        self._validate_required_params(instituicao, start)

        conditions = [
            self._build_cnpj_condition(instituicao),
            self._build_date_condition(start, end),
        ]

        df = self._qe.read_glob(
            pattern=f"{self._get_file_prefix()}_*.parquet",
            subdir=self._get_subdir(),
            where=join_conditions(conditions),
        )

        return self._finalize_read(df)

    def collect(self, start, end, force=False):
        from .collector import NovoCollector
        collector = NovoCollector()
        return collector.collect(start, end, force=force)
```

### 3. Registrar em Constants

```python
# core/constants.py
DATA_SOURCES["novo"] = {
    "subdir": "novo/dados",
    "prefix": "novo",
}
```

### 4. Registrar em __init__.py

```python
# ifdata_bcb/__init__.py
_novo = None

def __getattr__(name):
    global _novo
    if name == "novo":
        if _novo is None:
            from ifdata_bcb.providers.novo.explorer import NovoExplorer
            _novo = NovoExplorer()
        return _novo
    raise AttributeError(f"module has no attribute '{name}'")
```

---

## Exports Publicos

```python
# providers/__init__.py
from ifdata_bcb.domain.exceptions import PeriodUnavailableError
from ifdata_bcb.providers.base_collector import BaseCollector
from ifdata_bcb.providers.base_explorer import BaseExplorer
from ifdata_bcb.providers.collector_models import CollectStatus
from ifdata_bcb.providers.cosif.collector import COSIFCollector
from ifdata_bcb.providers.cosif.explorer import COSIFExplorer
from ifdata_bcb.providers.ifdata.collector import (
    IFDATACadastroCollector,
    IFDATAValoresCollector,
)
from ifdata_bcb.providers.ifdata.valores_explorer import IFDATAExplorer
from ifdata_bcb.providers.ifdata.cadastro_explorer import CadastroExplorer

__all__ = [
    # Base
    "BaseCollector",
    "BaseExplorer",
    "CollectStatus",
    "PeriodUnavailableError",
    # COSIF
    "COSIFCollector",
    "COSIFExplorer",
    # IFDATA
    "IFDATAValoresCollector",
    "IFDATACadastroCollector",
    "IFDATAExplorer",
    "CadastroExplorer",
]
```
