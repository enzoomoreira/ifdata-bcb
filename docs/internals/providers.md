# Providers

A camada de providers implementa coleta e leitura para cada fonte de dados do BCB.

## Localizacao

```
src/ifdata_bcb/providers/
|-- __init__.py              # Exports publicos
|-- base_explorer.py        # Classe base abstrata para explorers
|-- base_collector.py       # Template para coleta + CollectStatus enum
|-- enrichment.py           # Enriquecimento cadastral inline
|-- cosif/                   # COSIF (mensal)
|   |-- __init__.py
|   |-- collector.py        # COSIFCollector
|   +-- explorer.py         # COSIFExplorer
+-- ifdata/                  # IFDATA (trimestral)
    |-- __init__.py
    |-- cadastro/            # Dados cadastrais
    |   |-- __init__.py
    |   |-- collector.py    # IFDATACadastroCollector
    |   |-- explorer.py     # CadastroExplorer
    |   +-- search.py       # CadastroSearch (busca com filtros fonte/escopo)
    +-- valores/             # Dados financeiros (valores)
        |-- __init__.py
        |-- collector.py    # IFDATAValoresCollector
        |-- explorer.py     # IFDATAExplorer
        +-- temporal.py     # TemporalResolver (resolucao temporal por periodo)
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
_MAX_WORKERS: int = 4  # Workers para coleta paralela
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
        |       |   |   +-- self._http.get(url)
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
# base_collector.py
class CollectStatus(Enum):
    SUCCESS = auto()  # Arquivo salvo
    UNAVAILABLE = auto()  # Periodo nao disponivel no BCB
    FAILED = auto()  # Erro no download/processamento
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
- **Mapeamento de colunas** (apresentacao em lowercase):
  - `DATA_BASE` -> `data`
  - `CNPJ_8` -> `cnpj_8`
  - `NOME_INSTITUICAO` -> `instituicao`
  - `NOME_CONTA` -> `conta`
  - `SALDO` -> `valor`

### Implementacao

```python
class COSIFExplorer(BaseExplorer):
    _COLUMN_MAP = {
        "DATA_BASE": "data",
        "CNPJ_8": "cnpj_8",
        "NOME_INSTITUICAO": "instituicao",
        "NOME_CONTA": "conta",
        "CONTA": "cod_conta",
        "DOCUMENTO": "documento",
        "SALDO": "valor",
    }

    _DERIVED_COLUMNS: set[str] = {"escopo"}
    _DATE_COLUMN = "DATA_BASE"

    # Analise de era (ver core/eras.py)
    _ERA_BOUNDARY = COSIF_ERA_BOUNDARY  # 202501
    _ERA_GROUP_COLUMN = "documento"
    _ERA_SOURCE_NAME = "COSIF"

    # list_values()
    _LIST_COLUMNS = {"data": "DATA_BASE", "escopo": "ESCOPO", "documento": "DOCUMENTO"}

    _ESCOPOS = {
        "individual": {"subdir": "cosif/individual", "prefix": "cosif_ind"},
        "prudencial": {"subdir": "cosif/prudencial", "prefix": "cosif_prud"},
    }

    def _get_sources(self):
        return self._ESCOPOS

    def _periodos_por_escopo(self) -> dict[str, list[int]]:
        """Escopos coincidem com as fontes de armazenamento."""

    def _apply_canonical_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """Substitui aliases do COSIF por nomes canônicos do cadastro."""

    def read(
        self,
        start: DateScalar,
        end: DateScalar | None = None,
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
        Devolve DataFrame com DatetimeIndex 'date' e colunas lowercase.
        """

    def fetch(self, start, end=None, *, instituicao=None, escopo=None, ...):
        """
        Baixa do BCB e devolve o DataFrame sem tocar o cache local.
        Coleta para diretorio temporario (infra.paths.temp_dir) com
        DataManager(base_path=tmp) injetado no collector e delega ao
        read() de um COSIFExplorer com QueryEngine(base_path=tmp).
        Mesmos filtros de read(), exceto cadastro=.
        """

    def list_values(self, columns, *, start=None, end=None, escopo=None, documento=None, limit=100):
        """Lista valores distintos (data, escopo, documento)."""

    def list_contas(self, termo=None, *, escopo=None, start=None, end=None, limit=100):
        """Lista contas. Filtros keyword-only apos termo."""

    def collect(
        self,
        start: DateScalar,
        end: DateScalar | None = None,
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

`data` sai das colunas: e o DatetimeIndex `date` do resultado de `read()`.

| Coluna | Descricao |
|--------|-----------|
| cnpj_8 | CNPJ de 8 digitos |
| instituicao | Nome da instituicao |
| escopo | "individual" ou "prudencial" |
| cod_conta | Codigo da conta COSIF |
| conta | Nome da conta |
| documento | Tipo de documento |
| valor | Saldo em reais |

---

## ifdata/valores/collector.py

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

## ifdata/cadastro/collector.py

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

## ifdata/valores/explorer.py (IFDATAExplorer)

### Especificidades

- **Resolucao de escopo**: Usa EntityLookup para resolver CNPJ -> codigo IFDATA
- **Nomes canônicos**: Usa `get_canonical_names_for_cnpjs()` do cadastro
- **Mapeamento de reporters**: Resolve chaves de reporte para entidades analiticas
- **Mapeamento de colunas** (apresentacao em lowercase):
  - `AnoMes` -> `data`
  - `CodInst` -> `cod_inst`
  - `NomeColuna` -> `conta`
  - `Saldo` -> `valor`

### Implementacao

```python
class IFDATAExplorer(BaseExplorer):
    _COLUMN_MAP = {
        "AnoMes": "data",
        "CodInst": "cod_inst",
        "NomeColuna": "conta",
        "Conta": "cod_conta",
        "Saldo": "valor",
        "NomeRelatorio": "relatorio",
        "Grupo": "grupo",
    }

    _DERIVED_COLUMNS: set[str] = {"cnpj_8", "instituicao", "escopo"}

    # Analise de era (ver core/eras.py)
    _ERA_BOUNDARY = IFDATA_ERA_BOUNDARY  # 202503
    _ERA_GROUP_COLUMN = "relatorio"
    _ERA_SOURCE_NAME = "IFDATA"
    _TRIMESTRAL = True

    def _periodos_por_escopo(self) -> dict[str, list[int]]:
        """Escopo e coluna dos dados (TipoInstituicao), nao fonte:
        resolve via query DISTINCT sobre o parquet."""

    def read(
        self,
        start: DateScalar,
        end: DateScalar | None = None,
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

    def fetch(self, start, end=None, *, instituicao=None, escopo=None, ...):
        """
        Baixa do BCB e devolve o DataFrame sem tocar o cache local.
        Coleta para diretorio temporario com DataManager(base_path=tmp)
        e delega ao read() de um IFDATAExplorer com QueryEngine(base_path=tmp).
        O TemporalResolver do explorer temporario e trocado por um apontado
        para o cache real -- a resolucao de conglomerados consulta o
        cadastro, que vive no cache local.
        """

    def list_values(self, columns, *, start=None, end=None, escopo=None, relatorio=None, grupo=None, limit=100):
        """Lista valores distintos (data, escopo, relatorio, grupo)."""

    def list_contas(self, termo=None, *, escopo=None, relatorio=None, start=None, end=None, limit=100):
        """Lista contas. Filtros keyword-only apos termo."""
```

### Resolucao de Escopo

A resolucao de escopo e feita internamente pelo `TemporalResolver` (em `valores/temporal.py`), que resolve CNPJs para codigos IFDATA por periodo:

- **individual**: CNPJ direto, TipoInstituicao=3
- **prudencial**: CodConglomeradoPrudencial, TipoInstituicao=1
- **financeiro**: CodConglomeradoFinanceiro ou CNPJ direto, TipoInstituicao=2

### Mapeamento de Reporters

O `TemporalResolver` cruza dados do IFDATA com o cadastro para mapear
chaves de reporte (cod_inst) para entidades analiticas (cnpj_8):

- **Individual**: cod_inst = cnpj_8 direto
- **Prudencial**: cod_inst pode ser CodConglomeradoPrudencial ou CNPJ direto
- **Financeiro**: cod_inst pode ser CodConglomeradoFinanceiro ou CNPJ direto

### mapeamento()

Tabela de mapeamento cod_inst <-> cnpj_8 por escopo (apenas IFDATA):

| Coluna | Descricao |
|--------|-----------|
| cod_inst | Codigo de reporte no IFDATA |
| tipo_inst | 1, 2 ou 3 |
| escopo | individual, prudencial, financeiro |
| report_key_type | "cnpj" ou nome do escopo |
| cnpj_8 | CNPJ da entidade associada |
| instituicao | Nome canonico |

### Colunas Disponiveis (read)

`data` sai das colunas: e o DatetimeIndex `date` do resultado de `read()`.

| Coluna | Descricao |
|--------|-----------|
| cnpj_8 | CNPJ de 8 digitos |
| instituicao | Nome da instituicao (canônico do cadastro) |
| escopo | "individual", "prudencial", "financeiro" |
| cod_inst | Codigo no IFDATA |
| cod_conta | Codigo numerico da conta |
| conta | Nome da conta |
| valor | Saldo em reais |
| relatorio | Ativo, Passivo, DRE, Resumo |
| grupo | Grupo da conta |

---

## ifdata/cadastro/ (CadastroExplorer + CadastroSearch)

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
        "Data": "data",
        "CNPJ_8": "cnpj_8",
        "NomeInstituicao": "instituicao",
        "SegmentoTb": "segmento",
        "CodConglomeradoPrudencial": "cod_congl_prud",
        "CodConglomeradoFinanceiro": "cod_congl_fin",
        "CNPJ_LIDER_8": "cnpj_lider_8",
        "Situacao": "situacao",
        "Atividade": "atividade",
        # ...
    }

    def read(
        self,
        start: DateScalar,
        end: DateScalar | None = None,
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
            atividade, tcb, td, tc, sr, municipio: Filtros (case/accent insensitive)
        """

    def fetch(self, start, end=None, *, instituicao=None, ...):
        """Baixa do BCB e devolve o DataFrame sem tocar o cache local.
        Coleta para diretorio temporario e delega ao read() de um
        CadastroExplorer com QueryEngine(base_path=tmp)."""

    def list_values(self, columns: list[str], *, ...) -> pd.DataFrame:
        """Lista valores distintos para colunas solicitadas."""

    def search(self, termo: str | None = None, *, fonte=None, escopo=None, ...) -> pd.DataFrame:
        """Delega para CadastroSearch (cadastro/search.py)."""
```

A logica de busca (fuzzy matching, filtros fonte/escopo, verificacao de disponibilidade por escopo)
vive em `CadastroSearch` (`cadastro/search.py`), que por sua vez usa `EntitySearch` para o fuzzy
matching basico e `EntityLookup` para resolucao de metadados.

### Colunas Disponiveis

`data` sai das colunas: e o DatetimeIndex `date` do resultado de `read()`.

| Coluna | Descricao |
|--------|-----------|
| cnpj_8 | CNPJ de 8 digitos |
| instituicao | Nome |
| segmento | Segmento regulatorio |
| cod_congl_prud | Codigo conglomerado prudencial |
| cod_congl_fin | Codigo conglomerado financeiro |
| cnpj_lider_8 | CNPJ do lider do conglomerado |
| situacao | A (Ativa) ou I (Inativa) |
| atividade | Tipo de atividade |
| uf | Estado |
| municipio | Municipio |

---

## providers/enrichment.py

### Responsabilidades

Modulo de enriquecimento cadastral inline. Permite adicionar colunas cadastrais ao resultado de `cosif.read()` e `ifdata.read()` via parametro `cadastro=`.

### validate_cadastro_columns()

```python
VALID_CADASTRO_COLUMNS = {
    "segmento",
    "cod_congl_prud",
    "cod_congl_fin",
    "cnpj_lider_8",
    "situacao",
    "atividade",
    "tcb",
    "td",
    "tc",
    "uf",
    "municipio",
    "sr",
    "data_inicio_atividade",
    "nome_congl_prud",
}


def validate_cadastro_columns(columns: list[str] | None) -> None:
    """Valida nomes de colunas cadastrais. Raises InvalidColumnError."""
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

    Consome o df de apresentacao (colunas lowercase, data ainda como
    coluna -- roda antes de _to_datetime_index no read()). O retorno de
    cadastro_explorer.read() vem com DatetimeIndex 'date' e volta a ser
    coluna via reset_index(names="data") para o JOIN.

    Usa ASOF LEFT JOIN no DuckDB para alinhamento temporal backward-looking:
    cada linha financeira recebe os atributos cadastrais do trimestre
    mais recente <= sua data.

    Para data unica: LEFT JOIN com ROW_NUMBER para pegar registro mais recente.
    Para time-series: ASOF LEFT JOIN via DuckDB SQL.

    Suporta coluna derivada nome_congl_prud: nome oficial do conglomerado
    prudencial, resolvido a partir das alias rows do cadastro.
    """
```

---

## ifdata/valores/temporal.py

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
        response = self._http.get(url)
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
        "data_original": "data",
        "valor_original": "valor",
    }

    def _get_subdir(self):
        return "novo/dados"

    def _get_file_prefix(self):
        return "novo"

    def read(self, start, end=None, *, instituicao=None, **kwargs):
        self._validate_required_params(start)

        conditions = [
            self._build_cnpj_condition(instituicao),
            self._build_date_condition(start, end),
        ]

        df = self._qe.read_glob(
            pattern=f"{self._get_file_prefix()}_*.parquet",
            subdir=self._get_subdir(),
            where=join_conditions(conditions),
        )

        # data vira DatetimeIndex 'date' no ultimo passo
        return self._to_datetime_index(self._finalize_read(df))

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
from ifdata_bcb.providers.base_collector import BaseCollector, CollectStatus
from ifdata_bcb.providers.base_explorer import BaseExplorer
from ifdata_bcb.providers.cosif.collector import COSIFCollector
from ifdata_bcb.providers.cosif.explorer import COSIFExplorer
from ifdata_bcb.providers.ifdata.valores.collector import IFDATAValoresCollector
from ifdata_bcb.providers.ifdata.cadastro.collector import IFDATACadastroCollector
from ifdata_bcb.providers.ifdata.valores.explorer import IFDATAExplorer
from ifdata_bcb.providers.ifdata.cadastro.explorer import CadastroExplorer

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
