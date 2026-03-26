# Consultas SQL Avancadas

A biblioteca utiliza DuckDB para executar queries SQL diretamente nos arquivos Parquet, possibilitando analises complexas alem da API padrao.

## Visao Geral

### Por que usar SQL direto?

- **Agregacoes complexas**: GROUP BY, HAVING, window functions
- **Joins**: Combinar dados de multiplas fontes (COSIF + Cadastro, etc.)
- **Performance**: DuckDB otimiza queries com predicate pushdown e column pruning
- **Flexibilidade**: Qualquer consulta SQL compativel com DuckDB

### DuckDB

O [DuckDB](https://duckdb.org/) e um banco de dados analitico embutido, otimizado para:
- Leitura de arquivos Parquet
- Consultas OLAP (agregacoes, joins)
- Processamento vetorizado

## QueryEngine

O acesso a queries SQL e feito via `QueryEngine`:

```python
from ifdata_bcb.infra import QueryEngine

qe = QueryEngine()
```

### Metodos Principais

#### sql(query: str) -> pd.DataFrame

Executa SQL puro com substituicao de variaveis:

```python
df = qe.sql("""
    SELECT CNPJ_8, NOME_INSTITUICAO, SUM(SALDO) as TOTAL
    FROM '{cache}/cosif/prudencial/cosif_prud_202412.parquet'
    WHERE NOME_CONTA = 'TOTAL GERAL DO ATIVO'
    GROUP BY CNPJ_8, NOME_INSTITUICAO
    ORDER BY TOTAL DESC
    LIMIT 10
""")
```

#### read_glob(pattern, subdir, columns=None, where=None) -> pd.DataFrame

Le multiplos arquivos Parquet como dataset unico:

```python
df = qe.read_glob(
    pattern='cosif_prud_2024*.parquet',
    subdir='cosif/prudencial',
    columns=['CNPJ_8', 'NOME_CONTA', 'SALDO'],
    where="CNPJ_8 = '60872504'"
)
```

### Variaveis Disponiveis no SQL

| Variavel | Descricao | Exemplo |
|----------|-----------|---------|
| `{cache}` | Diretorio de cache dos dados | `C:/Users/.../py-bacen/Cache` |

## Estrutura dos Arquivos Parquet

### Diretorios

```
py-bacen/Cache/
  cosif/
    individual/       # cosif_ind_YYYYMM.parquet
    prudencial/       # cosif_prud_YYYYMM.parquet
  ifdata/
    valores/          # ifdata_val_YYYYMM.parquet
    cadastro/         # ifdata_cad_YYYYMM.parquet
```

### Naming Convention

| Fonte | Prefixo | Exemplo |
|-------|---------|---------|
| COSIF Individual | `cosif_ind_` | `cosif_ind_202412.parquet` |
| COSIF Prudencial | `cosif_prud_` | `cosif_prud_202412.parquet` |
| IFDATA Valores | `ifdata_val_` | `ifdata_val_202412.parquet` |
| IFDATA Cadastro | `ifdata_cad_` | `ifdata_cad_202412.parquet` |

### Schema dos Arquivos

#### COSIF (individual/prudencial)

```sql
-- Verificar schema
DESCRIBE SELECT * FROM '{cache}/cosif/prudencial/cosif_prud_202412.parquet';

-- Colunas:
-- DATA_BASE (BIGINT): Periodo YYYYMM
-- CNPJ_8 (VARCHAR): CNPJ de 8 digitos
-- NOME_INSTITUICAO (VARCHAR): Nome
-- DOCUMENTO (BIGINT): Tipo de documento
-- CONTA (VARCHAR): Codigo da conta
-- NOME_CONTA (VARCHAR): Nome da conta
-- SALDO (DOUBLE): Valor em reais
```

#### IFDATA Valores

```sql
-- Colunas:
-- AnoMes (BIGINT): Periodo YYYYMM
-- CodInst (VARCHAR): Codigo da instituicao (CNPJ ou codigo conglomerado)
-- TipoInstituicao (BIGINT): Tipo (1=prudencial, 2=financeiro, 3=individual)
-- Conta (VARCHAR): Codigo da conta
-- NomeColuna (VARCHAR): Nome da conta
-- Saldo (DOUBLE): Valor em reais
-- NomeRelatorio (VARCHAR): Relatorio de origem
-- Grupo (VARCHAR): Grupo da conta
```

#### IFDATA Cadastro

```sql
-- Colunas:
-- CNPJ_8 (VARCHAR): CNPJ de 8 digitos
-- NomeInstituicao (VARCHAR): Nome
-- CodConglomeradoPrudencial (VARCHAR): Conglomerado prudencial
-- CodConglomeradoFinanceiro (VARCHAR): Conglomerado financeiro
-- CNPJ_LIDER_8 (VARCHAR): CNPJ da lideranca
-- Situacao (VARCHAR): A=Ativa, I=Inativa
-- Data (VARCHAR): Data do registro
-- Segmento, UF, Municipio, etc.
```

## Exemplos de Queries

### Agregacoes Basicas

```python
from ifdata_bcb.infra import QueryEngine

qe = QueryEngine()

# Total de ativos por instituicao
df = qe.sql("""
    SELECT
        CNPJ_8,
        NOME_INSTITUICAO,
        SUM(SALDO) as TOTAL
    FROM '{cache}/cosif/prudencial/*.parquet'
    WHERE DATA_BASE = 202412
      AND NOME_CONTA = 'TOTAL GERAL DO ATIVO'
    GROUP BY CNPJ_8, NOME_INSTITUICAO
    ORDER BY TOTAL DESC
    LIMIT 20
""")
```

### Joins entre Fontes

```python
# Join COSIF com Cadastro para obter segmento
df = qe.sql("""
    SELECT
        c.CNPJ_8,
        c.NOME_INSTITUICAO,
        c.SALDO / 1e9 as ATIVO_BILHOES,
        cad.SegmentoTb as SEGMENTO,
        cad.UF
    FROM '{cache}/cosif/prudencial/*.parquet' c
    JOIN '{cache}/ifdata/cadastro/*.parquet' cad
        ON c.CNPJ_8 = cad.CNPJ_8
    WHERE c.DATA_BASE = 202412
      AND c.NOME_CONTA = 'TOTAL GERAL DO ATIVO'
    ORDER BY c.SALDO DESC
    LIMIT 20
""")
```

### Window Functions

```python
# Ranking de instituicoes por ativo
df = qe.sql("""
    SELECT
        CNPJ_8,
        NOME_INSTITUICAO,
        SALDO / 1e12 as ATIVO_TRILHOES,
        ROW_NUMBER() OVER (ORDER BY SALDO DESC) as RANKING,
        SALDO / SUM(SALDO) OVER () * 100 as MARKET_SHARE
    FROM '{cache}/cosif/prudencial/*.parquet'
    WHERE DATA_BASE = 202412
      AND NOME_CONTA = 'TOTAL GERAL DO ATIVO'
      AND DOCUMENTO = 4060
    ORDER BY RANKING
    LIMIT 10
""")
```

### CTEs (Common Table Expressions)

```python
# Evolucao do sistema financeiro
df = qe.sql("""
    WITH mensal AS (
        SELECT
            DATA_BASE,
            COUNT(DISTINCT CNPJ_8) as N_INSTITUICOES,
            SUM(CASE WHEN NOME_CONTA = 'TOTAL GERAL DO ATIVO'
                     THEN SALDO ELSE 0 END) / 1e12 as ATIVO_TRILHOES
        FROM '{cache}/cosif/prudencial/*.parquet'
        WHERE DOCUMENTO = 4060
        GROUP BY DATA_BASE
    )
    SELECT
        DATA_BASE,
        N_INSTITUICOES,
        ATIVO_TRILHOES,
        ATIVO_TRILHOES - LAG(ATIVO_TRILHOES) OVER (ORDER BY DATA_BASE) as VARIACAO
    FROM mensal
    ORDER BY DATA_BASE
""")
```

### Subqueries

```python
# Instituicoes acima da media
df = qe.sql("""
    SELECT CNPJ_8, NOME_INSTITUICAO, SALDO / 1e9 as ATIVO_BILHOES
    FROM '{cache}/cosif/prudencial/*.parquet'
    WHERE DATA_BASE = 202412
      AND NOME_CONTA = 'TOTAL GERAL DO ATIVO'
      AND SALDO > (
          SELECT AVG(SALDO)
          FROM '{cache}/cosif/prudencial/*.parquet'
          WHERE DATA_BASE = 202412
            AND NOME_CONTA = 'TOTAL GERAL DO ATIVO'
      )
    ORDER BY SALDO DESC
""")
```

### CASE WHEN

```python
# Classificar instituicoes por porte
df = qe.sql("""
    SELECT
        CNPJ_8,
        NOME_INSTITUICAO,
        SALDO / 1e9 as ATIVO_BILHOES,
        CASE
            WHEN SALDO >= 1e12 THEN 'Grande (> 1 tri)'
            WHEN SALDO >= 100e9 THEN 'Medio (100bi - 1tri)'
            WHEN SALDO >= 10e9 THEN 'Pequeno (10bi - 100bi)'
            ELSE 'Micro (< 10bi)'
        END as PORTE
    FROM '{cache}/cosif/prudencial/*.parquet'
    WHERE DATA_BASE = 202412
      AND NOME_CONTA = 'TOTAL GERAL DO ATIVO'
    ORDER BY SALDO DESC
""")
```

### Analise por Segmento

```python
# Agregacao por segmento usando cadastro
df = qe.sql("""
    SELECT
        cad.SegmentoTb as SEGMENTO,
        COUNT(DISTINCT c.CNPJ_8) as N_INSTITUICOES,
        SUM(c.SALDO) / 1e12 as ATIVO_TRILHOES,
        AVG(c.SALDO) / 1e9 as MEDIA_BILHOES
    FROM '{cache}/cosif/prudencial/*.parquet' c
    JOIN '{cache}/ifdata/cadastro/*.parquet' cad
        ON c.CNPJ_8 = cad.CNPJ_8
    WHERE c.DATA_BASE = 202412
      AND c.NOME_CONTA = 'TOTAL GERAL DO ATIVO'
      AND c.DOCUMENTO = 4060
    GROUP BY cad.SegmentoTb
    HAVING COUNT(DISTINCT c.CNPJ_8) >= 5
    ORDER BY ATIVO_TRILHOES DESC
""")
```

### Serie Temporal

```python
# Evolucao trimestral de uma instituicao
df = qe.sql("""
    SELECT
        DATA_BASE,
        CNPJ_8,
        NOME_INSTITUICAO,
        NOME_CONTA,
        SALDO / 1e9 as VALOR_BILHOES
    FROM '{cache}/cosif/prudencial/*.parquet'
    WHERE CNPJ_8 = '60872504'
      AND NOME_CONTA IN ('TOTAL GERAL DO ATIVO', 'PATRIMONIO LIQUIDO')
      AND DATA_BASE >= 202401
    ORDER BY DATA_BASE, NOME_CONTA
""")
```

## Performance

### Predicate Pushdown

O DuckDB automaticamente empurra filtros para a leitura do Parquet:

```python
# Filtros no WHERE sao aplicados durante a leitura, nao apos
df = qe.sql("""
    SELECT *
    FROM '{cache}/cosif/prudencial/*.parquet'
    WHERE DATA_BASE = 202412       -- Pushdown: filtra durante leitura
      AND CNPJ_8 = '60872504'      -- Pushdown: filtra durante leitura
""")
```

### Column Pruning

Especificar colunas evita carregar dados desnecessarios:

```python
# BOM: apenas colunas necessarias
df = qe.read_glob(
    pattern='cosif_prud_*.parquet',
    subdir='cosif/prudencial',
    columns=['CNPJ_8', 'SALDO'],  # So carrega estas
    where="DATA_BASE = 202412"
)

# EVITAR: SELECT * carrega tudo
df = qe.sql("""
    SELECT *
    FROM '{cache}/cosif/prudencial/*.parquet'
    WHERE DATA_BASE = 202412
""")
```

### Glob Patterns

Use patterns especificos quando possivel:

```python
# BOM: arquivo especifico
df = qe.sql("""
    SELECT * FROM '{cache}/cosif/prudencial/cosif_prud_202412.parquet'
""")

# BOM: ano especifico
df = qe.sql("""
    SELECT * FROM '{cache}/cosif/prudencial/cosif_prud_2024*.parquet'
""")

# MAIS LENTO: todos os arquivos
df = qe.sql("""
    SELECT * FROM '{cache}/cosif/prudencial/*.parquet'
""")
```

### LIMIT

Use LIMIT para explorar dados:

```python
# Explorar estrutura
df = qe.sql("""
    SELECT * FROM '{cache}/cosif/prudencial/*.parquet' LIMIT 100
""")

# Ver valores unicos
df = qe.sql("""
    SELECT DISTINCT NOME_CONTA
    FROM '{cache}/cosif/prudencial/*.parquet'
    WHERE DATA_BASE = 202412
    LIMIT 100
""")
```

## Utilitarios Adicionais

### Listar Arquivos Disponiveis

```python
from ifdata_bcb.infra import list_parquet_files

# Lista arquivos em um subdiretorio
arquivos = list_parquet_files('cosif/prudencial')
# ['cosif_prud_202401', 'cosif_prud_202402', ...]
```

### Verificar Metadados

```python
from ifdata_bcb.infra import get_parquet_metadata

meta = get_parquet_metadata('cosif_prud_202412', 'cosif/prudencial')
# {
#     'arquivo': 'cosif_prud_202412.parquet',
#     'subdir': 'cosif/prudencial',
#     'registros': 150000,
#     'colunas': ['DATA_BASE', 'CNPJ_8', ...],
#     'status': 'ok'
# }
```

### DataManager

Para operacoes de escrita (uso avancado):

```python
from ifdata_bcb.infra import DataManager

dm = DataManager()

# Salvar DataFrame como Parquet
path = dm.save(df, 'meu_arquivo', 'minha_pasta')

# Periodos disponiveis
periodos = dm.get_periodos_disponiveis('cosif_prud', 'cosif/prudencial')
# [(2024, 1), (2024, 2), ...]
```

## Tratamento de Erros

```python
from ifdata_bcb.infra import QueryEngine

qe = QueryEngine()

# Erro de sintaxe SQL
try:
    df = qe.sql("SELECT * FORM tabela")  # Erro: FORM ao inves de FROM
except Exception as e:
    print(f"Erro SQL: {e}")

# Arquivo nao encontrado (retorna DataFrame vazio)
df = qe.sql("SELECT * FROM '{cache}/nao_existe/*.parquet'")
if df.empty:
    print("Nenhum dado encontrado")
```

## API vs SQL Direto

| Cenario | Recomendado |
|---------|-------------|
| Consultas simples por instituicao | API (`bcb.cosif.read()`) |
| Filtros por conta/data/escopo | API (`bcb.cosif.read()`) |
| Agregacoes complexas | SQL direto (`qe.sql()`) |
| Joins entre fontes | SQL direto (`qe.sql()`) |
| Window functions | SQL direto (`qe.sql()`) |
| Analises exploratórias | SQL direto (`qe.sql()`) |

## Referencias

- [Documentacao DuckDB](https://duckdb.org/docs/)
- [DuckDB SQL Reference](https://duckdb.org/docs/sql/introduction)
- [Parquet com DuckDB](https://duckdb.org/docs/data/parquet/overview)
