# ifdata-bcb

Coleta e analise de dados contabeis e financeiros de instituicoes financeiras brasileiras. Dados publicos do Banco Central do Brasil.

| Fonte | Modulo | Dados | Periodicidade |
|-------|--------|-------|---------------|
| COSIF | `bcb.cosif` | Plano contabil (individual e prudencial) | Mensal |
| IFDATA | `bcb.ifdata` | Informacoes financeiras | Trimestral |
| Cadastro | `bcb.cadastro` | Metadados de instituicoes (segmento, conglomerado) | Trimestral |

## Instalacao

```bash
uv add ifdata-bcb
```

Requer Python 3.12+.

## Uso Rapido

```python
import ifdata_bcb as bcb

# 1. Coletar dados (primeira vez ou atualizar)
bcb.cadastro.collect('2024-01', '2024-12')
bcb.cosif.collect('2024-01', '2024-12')
bcb.ifdata.collect('2024-01', '2024-12')

# 2. Buscar instituicao por nome (fuzzy matching)
bcb.search('Itau')
bcb.search('Bradesco')
#    CNPJ_8                       INSTITUICAO  SITUACAO  FONTES  SCORE
# 0  60872504  ITAU UNIBANCO HOLDING S.A.           A    ...    100
# Quando possivel, prioriza resultados com dados disponiveis em FONTES.

# 3. Ler dados usando CNPJ de 8 digitos
# COSIF/IFDATA: instituicao e start sao OBRIGATORIOS
# start sozinho = data unica; start + end = range

# COSIF (escopo=None busca em todos os escopos)
df = bcb.cosif.read(
    instituicao='60872504',
    start='2024-12',
    conta='TOTAL GERAL DO ATIVO',
    escopo='prudencial'
)

# IFDATA
df = bcb.ifdata.read(
    instituicao='60872504',
    start='2024-01',
    end='2024-12',
    conta='Lucro Liquido'
)

# Cadastro
info = bcb.cadastro.info('60872504', start='2024-12')

# Cadastro tambem pode ser filtrado sem instituicao
df = bcb.cadastro.read(start='2024-12', segmento='Banco Multiplo')

# 4. Listar contas e instituicoes disponiveis
bcb.cosif.list_accounts(escopo='prudencial')
bcb.cosif.list_institutions(escopo='prudencial')

# 5. SQL direto com DuckDB (para analises avancadas)
from ifdata_bcb.infra import QueryEngine

qe = QueryEngine()
df = qe.sql("""
    SELECT CNPJ_8, NOME_INSTITUICAO, SALDO
    FROM '{cache}/cosif/prudencial/*.parquet'
    WHERE DATA_BASE = 202412 AND NOME_CONTA = 'TOTAL GERAL DO ATIVO'
    ORDER BY SALDO DESC
    LIMIT 10
""")
```

## Documentacao

### Guias de Uso

- **[getting-started.md](docs/getting-started.md)** - Instalacao e primeiro uso

### Fontes de Dados

- **[cosif.md](docs/providers/cosif.md)** - Plano contabil (individual/prudencial)
- **[ifdata.md](docs/providers/ifdata.md)** - Informacoes financeiras trimestrais
- **[cadastro.md](docs/providers/cadastro.md)** - Metadados de instituicoes

### Uso Avancado

- **[sql-queries.md](docs/advanced/sql-queries.md)** - Queries SQL com DuckDB
- **[extending.md](docs/advanced/extending.md)** - Como criar novos providers

### Arquitetura Interna

- **[architecture.md](docs/internals/architecture.md)** - Visao geral da arquitetura
- **[core.md](docs/internals/core.md)** - BaseExplorer, EntityLookup, Constants
- **[domain.md](docs/internals/domain.md)** - Exceptions, Models, Types, Validation
- **[infra.md](docs/internals/infra.md)** - Settings, QueryEngine, DataManager
- **[providers.md](docs/internals/providers.md)** - BaseCollector, Explorers

## Estrutura de Dados

```
{cache}/
  cosif/
    individual/       # cosif_ind_YYYYMM.parquet
    prudencial/       # cosif_prud_YYYYMM.parquet
  ifdata/
    valores/          # ifdata_val_YYYYMM.parquet
    cadastro/         # ifdata_cad_YYYYMM.parquet
```

O diretorio de cache varia por sistema:

| Sistema | Caminho |
|---------|---------|
| Windows | `%LOCALAPPDATA%\py-bacen\Cache\` |
| Linux | `~/.cache/py-bacen/` |
| macOS | `~/Library/Caches/py-bacen/` |

Customizavel via variavel de ambiente `BACEN_DATA_DIR`.

## API Publica

### Modulo Principal

```python
import ifdata_bcb as bcb

# Explorers (lazy loading)
bcb.cosif       # COSIFExplorer
bcb.ifdata      # IFDATAExplorer
bcb.cadastro    # CadastroExplorer

# Funcoes
bcb.search(termo, limit=10)  # Busca instituicoes por nome

# Exceptions
bcb.BacenAnalysisError       # Classe base para todos os erros
bcb.DataUnavailableError     # Dados nao disponiveis
```

### Metodos dos Explorers

Todos os explorers possuem:

| Metodo | Descricao |
|--------|-----------|
| `collect(start, end, ...)` | Coleta dados do BCB |
| `read(instituicao, start, ...)` | Le dados com filtros |
| `list_periods()` | Periodos disponiveis |
| `has_data()` | Verifica se tem dados |

Metodos especificos:

| Explorer | Metodos Adicionais |
|----------|-------------------|
| `cosif` | `list_accounts()`, `list_institutions()` |
| `ifdata` | `list_accounts()`, `list_institutions()`, `list_reporters()`, `list_reports()` |
| `cadastro` | `info()`, `list_segmentos()`, `list_ufs()`, `get_conglomerate_members()` |
