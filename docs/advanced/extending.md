# Estendendo a Biblioteca

Guia para criar novos providers, customizar comportamentos e contribuir para o projeto.

## Arquitetura de Providers

### Componentes

Cada provider e composto por dois componentes principais:

| Componente | Responsabilidade | Classe Base |
|------------|------------------|-------------|
| **Explorer** | Interface de consulta (read, list_*) | `BaseExplorer` |
| **Collector** | Coleta de dados (download, processamento) | `BaseCollector` |

### Estrutura de Diretorio

```
src/ifdata_bcb/providers/
  novo_provider/
    __init__.py
    explorer.py    # NovoExplorer (herda BaseExplorer)
    collector.py   # NovoCollector (herda BaseCollector)
```

## Criando um Novo Provider

### Passo 1: Criar o Collector

O Collector e responsavel por baixar e processar dados.

```python
# src/ifdata_bcb/providers/novo/collector.py

from pathlib import Path
from typing import Optional
import pandas as pd
import requests

from ifdata_bcb.infra.resilience import retry, DEFAULT_REQUEST_TIMEOUT
from ifdata_bcb.services.base_collector import BaseCollector


class NovoCollector(BaseCollector):
    """
    Collector para dados do Novo Provider.

    Baixa dados de [fonte] e processa para formato Parquet.
    """

    # Periodicidade: 'monthly' ou 'quarterly'
    _PERIOD_TYPE = "monthly"

    # Numero de workers paralelos (ajustar conforme API)
    _MAX_WORKERS = 4

    def _get_file_prefix(self) -> str:
        """Prefixo dos arquivos (ex: novo_202412.parquet)."""
        return "novo"

    def _get_subdir(self) -> str:
        """Subdiretorio dentro de cache/."""
        return "novo"

    @retry(delay=2.0)  # Retry com backoff para APIs externas
    def _download_period(self, period: int) -> Optional[Path]:
        """
        Baixa dados de um periodo especifico.

        Args:
            period: Periodo no formato YYYYMM.

        Returns:
            Path do arquivo CSV baixado ou None se falhar.
        """
        import tempfile

        url = f"https://api.exemplo.com/dados/{period}.csv"
        temp_dir = Path(tempfile.mkdtemp(prefix=f"novo_{period}_"))
        output_path = temp_dir / f"novo_{period}.csv"

        try:
            response = requests.get(url, timeout=DEFAULT_REQUEST_TIMEOUT)
            response.raise_for_status()
            output_path.write_bytes(response.content)
            return output_path
        except requests.RequestException as e:
            self.logger.warning(f"Falha no download: {period} - {e}")
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
        import duckdb

        try:
            # Usar DuckDB para processamento eficiente
            query = f"""
                SELECT
                    {period} as DATA,
                    coluna1 as COLUNA_NORMALIZADA,
                    TRY_CAST(valor AS DOUBLE) as VALOR
                FROM read_csv('{csv_path}', header=true)
            """

            conn = duckdb.connect()
            try:
                df = conn.sql(query).df()
            finally:
                conn.close()

            if df.empty:
                return None

            # Normalizar colunas (ex: CNPJ)
            # df["CNPJ_8"] = df["CNPJ"].apply(standardize_cnpj_base8)

            # Reordenar colunas
            cols = ["DATA", "COLUNA_NORMALIZADA", "VALOR"]
            df = df[[c for c in cols if c in df.columns]]

            return df

        except Exception as e:
            self.logger.error(f"Erro processando {csv_path}: {e}")
            return None
```

### Passo 2: Criar o Explorer

O Explorer fornece a interface de consulta.

```python
# src/ifdata_bcb/providers/novo/explorer.py

from typing import Optional
import pandas as pd

from ifdata_bcb.domain.explorers import BaseExplorer, AccountInput, DateInput
from ifdata_bcb.infra.query import QueryEngine
from ifdata_bcb.services.entity_resolver import EntityResolver
from ifdata_bcb.providers.novo.collector import NovoCollector


class NovoExplorer(BaseExplorer):
    """
    Explorer para dados do Novo Provider.

    Exemplo:
        explorer = NovoExplorer()
        explorer.collect('2024-01', '2024-12')
        df = explorer.read('60872504', datas=202412)
    """

    def __init__(
        self,
        query_engine: Optional[QueryEngine] = None,
        entity_resolver: Optional[EntityResolver] = None,
    ):
        super().__init__(query_engine, entity_resolver)
        self._collector: Optional[NovoCollector] = None

    def _get_subdir(self) -> str:
        return "novo"

    def _get_file_prefix(self) -> str:
        return "novo"

    def _get_collector(self) -> NovoCollector:
        """Lazy initialization do collector."""
        if self._collector is None:
            self._collector = NovoCollector()
        return self._collector

    def collect(
        self,
        start: str,
        end: str,
        force: bool = False,
    ) -> None:
        """
        Coleta dados do Novo Provider.

        Args:
            start: Data inicial (YYYY-MM).
            end: Data final (YYYY-MM).
            force: Se True, recoleta dados existentes.
        """
        collector = self._get_collector()
        collector.collect(start, end, force=force)

    def read(
        self,
        instituicao: str,
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
        self._validate_required_params(instituicao, start, end)
        where_parts = []

        cnpj = self._resolve_entity(instituicao)
        where_parts.append(f"CNPJ_8 = '{cnpj}'")

        if conta:
            conta_list = self._normalize_accounts(conta)
            if len(conta_list) == 1:
                where_parts.append(f"CONTA = '{conta_list[0]}'")
            else:
                conta_str = ", ".join(f"'{c}'" for c in conta_list)
                where_parts.append(f"CONTA IN ({conta_str})")

        if datas:
            datas_list = self._normalize_dates(datas)
            if len(datas_list) == 1:
                where_parts.append(f"DATA = {datas_list[0]}")
            else:
                datas_str = ", ".join(str(d) for d in datas_list)
                where_parts.append(f"DATA IN ({datas_str})")

        where_clause = " AND ".join(where_parts) if where_parts else None

        pattern = f"{self._get_file_prefix()}_*.parquet"

        df = self._qe.read_glob(
            pattern=pattern,
            subdir=self._get_subdir(),
            columns=columns,
            where=where_clause,
        )

        return self._finalize_read(df)

    def list_items(self, limit: int = 100) -> pd.DataFrame:
        """Lista itens disponiveis."""
        pattern = f"{self._get_file_prefix()}_*.parquet"
        path = self._qe.cache_path / self._get_subdir() / pattern

        query = f"""
            SELECT DISTINCT COLUNA_NORMALIZADA
            FROM '{path}'
            ORDER BY COLUNA_NORMALIZADA
            LIMIT {limit}
        """

        return self._qe.sql(query)
```

### Passo 3: Criar __init__.py

```python
# src/ifdata_bcb/providers/novo/__init__.py

from ifdata_bcb.providers.novo.explorer import NovoExplorer
from ifdata_bcb.providers.novo.collector import NovoCollector

__all__ = ["NovoExplorer", "NovoCollector"]
```

### Passo 4: Registrar no Modulo Principal

Adicionar lazy loading no `__init__.py` raiz:

```python
# src/ifdata_bcb/__init__.py

_novo = None

def __getattr__(name):
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

### EntityResolver Customizado

```python
from ifdata_bcb.services import EntityResolver
from ifdata_bcb.providers.ifdata.explorer import IFDATAExplorer

# Resolver com thresholds ajustados
resolver = EntityResolver(
    fuzzy_threshold_auto=90,    # Mais restritivo
    fuzzy_threshold_suggest=80
)

explorer = IFDATAExplorer(entity_resolver=resolver)
```

### DataManager Customizado

```python
from ifdata_bcb.infra import DataManager
from ifdata_bcb.providers.cosif.collector import COSIFCollector

dm = DataManager(base_path="/dados/bcb")
collector = COSIFCollector("individual", data_manager=dm)
```

## Testando Providers

### Estrutura de Testes

```
tests/
  providers/
    novo/
      test_collector.py
      test_explorer.py
      conftest.py       # Fixtures compartilhadas
```

### Fixtures

```python
# tests/providers/novo/conftest.py

import pytest
import tempfile
from pathlib import Path

from ifdata_bcb.infra import QueryEngine, DataManager


@pytest.fixture
def temp_cache():
    """Diretorio temporario para testes."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def query_engine(temp_cache):
    """QueryEngine apontando para cache temporario."""
    return QueryEngine(base_path=temp_cache)


@pytest.fixture
def data_manager(temp_cache):
    """DataManager apontando para cache temporario."""
    return DataManager(base_path=temp_cache)
```

### Testando Collector

```python
# tests/providers/novo/test_collector.py

import pytest
from unittest.mock import patch, MagicMock
import pandas as pd

from ifdata_bcb.providers.novo.collector import NovoCollector


class TestNovoCollector:

    def test_get_file_prefix(self):
        collector = NovoCollector()
        assert collector._get_file_prefix() == "novo"

    def test_get_subdir(self):
        collector = NovoCollector()
        assert collector._get_subdir() == "novo"

    @patch("requests.get")
    def test_download_period_success(self, mock_get, temp_cache):
        mock_response = MagicMock()
        mock_response.content = b"col1,col2\nval1,val2"
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        collector = NovoCollector()
        result = collector._download_period(202412)

        assert result is not None
        assert result.exists()

    def test_process_to_parquet(self, temp_cache):
        # Criar CSV de teste
        csv_path = temp_cache / "test.csv"
        csv_path.write_text("col1,valor\nA,100\nB,200")

        collector = NovoCollector()
        df = collector._process_to_parquet(csv_path, 202412)

        assert df is not None
        assert len(df) == 2
```

### Testando Explorer

```python
# tests/providers/novo/test_explorer.py

import pytest
import pandas as pd

from ifdata_bcb.providers.novo.explorer import NovoExplorer


class TestNovoExplorer:

    def test_get_subdir(self, query_engine):
        explorer = NovoExplorer(query_engine=query_engine)
        assert explorer._get_subdir() == "novo"

    def test_read_empty(self, query_engine):
        explorer = NovoExplorer(query_engine=query_engine)
        df = explorer.read()
        assert df.empty

    def test_normalize_dates(self, query_engine):
        explorer = NovoExplorer(query_engine=query_engine)

        # Inteiro
        assert explorer._normalize_dates(202412) == [202412]

        # String
        assert explorer._normalize_dates("2024-12") == [202412]

        # Lista
        assert explorer._normalize_dates([202411, 202412]) == [202411, 202412]
```

### Mocking de APIs Externas

```python
@pytest.fixture
def mock_bcb_api():
    """Mock da API do BCB."""
    with patch("requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.content = b"DATA,VALOR\n202412,100"
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response
        yield mock_get
```

## Contribuindo

### Fluxo de Desenvolvimento

1. Fork do repositorio
2. Criar branch para feature: `git checkout -b feature/novo-provider`
3. Implementar com testes
4. Rodar linters: `uvx ruff check . && uvx ruff format .`
5. Rodar testes: `uv run pytest`
6. Criar Pull Request

### Code Style

- Usar `ruff` para linting e formatacao
- Type hints em todas as funcoes
- Docstrings no formato Google
- Sem emojis no codigo

### Checklist para PR

- [ ] Testes unitarios passando
- [ ] Documentacao atualizada
- [ ] Type hints completos
- [ ] Docstrings em metodos publicos
- [ ] Sem breaking changes (ou documentados)
- [ ] Codigo formatado com ruff

### Estrutura de Commit

```
feat(provider): adicionar NovoProvider para dados XYZ

- Implementar NovoCollector com download e processamento
- Implementar NovoExplorer com read e list_items
- Adicionar testes unitarios
- Documentar no docs/providers/novo.md
```

## Referencias

- [Arquitetura](architecture.md) - Visao geral da arquitetura
- [Camada de Dominio](domain.md) - BaseExplorer e excecoes
- [Infraestrutura](infra.md) - QueryEngine, DataManager, etc.
- [Servicos](services.md) - BaseCollector, EntityResolver, etc.
