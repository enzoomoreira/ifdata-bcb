# ifdata-bcb

Coleta e analise de dados contabeis e financeiros de instituicoes financeiras brasileiras. Dados publicos do Banco Central do Brasil.

| Fonte | Modulo | Dados |
|-------|--------|-------|
| COSIF | `bcb.cosif` | Plano contabil mensal (individual e prudencial) |
| IFDATA | `bcb.ifdata` | Informacoes financeiras trimestrais |
| Cadastro | `bcb.cadastro` | Metadados de instituicoes (segmento, conglomerado) |

## Instalacao

```bash
uv sync
```

## Uso Rapido

```python
import ifdata_bcb as bcb

# Coleta de dados
bcb.cadastro.collect('2024-01', '2024-12')
bcb.cosif.collect('2024-01', '2024-12')
bcb.ifdata.collect('2024-01', '2024-12')

# Buscar instituicao por nome
bcb.search('Itau')
bcb.search('Bradesco')

# Leitura de dados COSIF (instituicao e start sao obrigatorios)
# start sozinho = data unica; start + end = range
# escopo=None busca em todos os escopos
df = bcb.cosif.read(instituicao='60872504', start='2024-12', conta=['TOTAL GERAL DO ATIVO'], escopo='prudencial')

# Leitura de dados IFDATA com range de datas
df = bcb.ifdata.read(instituicao='60872504', start='2024-01', end='2024-12', conta=['Lucro Liquido'])

# Informacoes cadastrais (start opcional - retorna mais recente se omitido)
info = bcb.cadastro.info('60872504')

# Listar contas e instituicoes
bcb.cosif.list_accounts(escopo='prudencial')
bcb.cosif.list_institutions(escopo='prudencial')

# SQL direto com DuckDB
df = bcb.sql("""
    SELECT CNPJ_8, INSTITUICAO, VALOR
    FROM '{cache}/cosif/prudencial/*.parquet'
    WHERE DATA = 202412 AND CONTA = 'TOTAL GERAL DO ATIVO'
    ORDER BY VALOR DESC
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
- **[domain.md](docs/internals/domain.md)** - BaseExplorer, Exceptions
- **[infra.md](docs/internals/infra.md)** - Config, QueryEngine, DataManager
- **[services.md](docs/internals/services.md)** - BaseCollector, EntityResolver

## Estrutura de Dados

```
{cache}/
  cosif/
    individual/       # cosif_ind_202401.parquet...
    prudencial/       # cosif_prud_202401.parquet...
  ifdata/
    valores/          # ifdata_val_202403.parquet...
    cadastro/         # ifdata_cad_202403.parquet...
```

O diretorio de cache varia por sistema:

| Sistema | Caminho |
|---------|---------|
| Windows | `%LOCALAPPDATA%\py-bacen\Cache\` |
| Linux | `~/.cache/py-bacen/` |
| macOS | `~/Library/Caches/py-bacen/` |

Customizavel via variavel de ambiente `BACEN_DATA_DIR`.
