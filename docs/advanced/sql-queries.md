# Consultas SQL Avancadas

A biblioteca permite executar SQL diretamente nos dados coletados usando DuckDB, possibilitando analises complexas que seriam dificeis ou ineficientes com a API padrao.

## Visao Geral

### Por que usar SQL?

- **Agregacoes complexas**: GROUP BY, HAVING, window functions
- **Joins**: Combinar dados de multiplas fontes
- **Performance**: DuckDB otimiza queries com predicate pushdown e column pruning
- **Flexibilidade**: Qualquer consulta SQL compativel com DuckDB

### DuckDB

O [DuckDB](https://duckdb.org/) e um banco de dados analitico embutido, otimizado para:
- Leitura de arquivos Parquet
- Consultas OLAP
- Processamento vetorizado

## Funcao bcb.sql()

### Sintaxe

```python
bcb.sql(query: str) -> pd.DataFrame
```

### Variaveis Disponiveis

| Variavel | Descricao | Exemplo |
|----------|-----------|---------|
| `{cache}` | Diretorio de cache dos dados | `C:/Users/.../py-bacen/Cache` |
| `{raw}` | Alias para `{cache}` (compatibilidade) | - |

### Exemplo Basico

```python
import ifdata_bcb as bcb

df = bcb.sql("""
    SELECT CNPJ_8, INSTITUICAO, VALOR
    FROM '{cache}/cosif/prudencial/*.parquet'
    WHERE DATA = 202412
      AND CONTA = 'TOTAL GERAL DO ATIVO'
    ORDER BY VALOR DESC
    LIMIT 10
""")
```

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
-- DATA (BIGINT): Periodo YYYYMM
-- CNPJ_8 (VARCHAR): CNPJ de 8 digitos
-- INSTITUICAO (VARCHAR): Nome
-- DOCUMENTO (BIGINT): Tipo de documento
-- COD_CONTA (VARCHAR): Codigo da conta
-- CONTA (VARCHAR): Nome da conta
-- VALOR (DOUBLE): Valor em reais
```

#### IFDATA Valores

```sql
-- Colunas:
-- DATA (BIGINT): Periodo YYYYMM
-- COD_INST (VARCHAR): Codigo da instituicao
-- TIPO_INST (BIGINT): Tipo (1, 2, 3)
-- COD_CONTA (VARCHAR): Codigo da conta
-- CONTA (VARCHAR): Nome da conta
-- VALOR (DOUBLE): Valor em reais
-- RELATORIO (VARCHAR): Relatorio de origem
-- GRUPO (VARCHAR): Grupo da conta
```

#### IFDATA Cadastro

```sql
-- Colunas:
-- DATA (BIGINT): Periodo YYYYMM
-- CNPJ_8 (VARCHAR): CNPJ de 8 digitos
-- INSTITUICAO (VARCHAR): Nome
-- SEGMENTO (VARCHAR): Segmento
-- COD_CONGL_PRUD (VARCHAR): Conglomerado prudencial
-- COD_CONGL_FIN (VARCHAR): Conglomerado financeiro
-- CNPJ_LIDER_8 (VARCHAR): CNPJ da lider
-- SITUACAO (VARCHAR): Situacao
-- UF (VARCHAR): Estado
-- MUNICIPIO (VARCHAR): Cidade
-- ... outras colunas
```

## Exemplos de Queries

### Agregacoes Basicas

```python
# Total de ativos por instituicao
df = bcb.sql("""
    SELECT
        CNPJ_8,
        INSTITUICAO,
        SUM(VALOR) as TOTAL
    FROM '{cache}/cosif/prudencial/*.parquet'
    WHERE DATA = 202412
      AND CONTA = 'TOTAL GERAL DO ATIVO'
    GROUP BY CNPJ_8, INSTITUICAO
    ORDER BY TOTAL DESC
    LIMIT 20
""")
```

### Joins entre Fontes

```python
# Join COSIF com Cadastro para obter segmento
df = bcb.sql("""
    SELECT
        c.CNPJ_8,
        c.INSTITUICAO,
        c.VALOR / 1e9 as ATIVO_BILHOES,
        cad.SEGMENTO,
        cad.UF
    FROM '{cache}/cosif/prudencial/*.parquet' c
    JOIN '{cache}/ifdata/cadastro/*.parquet' cad
        ON c.CNPJ_8 = cad.CNPJ_8
        AND c.DATA = cad.DATA
    WHERE c.DATA = 202412
      AND c.CONTA = 'TOTAL GERAL DO ATIVO'
    ORDER BY c.VALOR DESC
    LIMIT 20
""")
```

### Window Functions

```python
# Ranking de instituicoes por ativo
df = bcb.sql("""
    SELECT
        CNPJ_8,
        INSTITUICAO,
        VALOR / 1e12 as ATIVO_TRILHOES,
        ROW_NUMBER() OVER (ORDER BY VALOR DESC) as RANKING,
        VALOR / SUM(VALOR) OVER () * 100 as MARKET_SHARE
    FROM '{cache}/cosif/prudencial/*.parquet'
    WHERE DATA = 202412
      AND CONTA = 'TOTAL GERAL DO ATIVO'
      AND DOCUMENTO = 4060
    ORDER BY RANKING
    LIMIT 10
""")
```

### CTEs (Common Table Expressions)

```python
# Evolucao do sistema financeiro
df = bcb.sql("""
    WITH mensal AS (
        SELECT
            DATA,
            COUNT(DISTINCT CNPJ_8) as N_INSTITUICOES,
            SUM(CASE WHEN CONTA = 'TOTAL GERAL DO ATIVO'
                     THEN VALOR ELSE 0 END) / 1e12 as ATIVO_TRILHOES
        FROM '{cache}/cosif/prudencial/*.parquet'
        WHERE DOCUMENTO = 4060
        GROUP BY DATA
    )
    SELECT
        DATA,
        N_INSTITUICOES,
        ATIVO_TRILHOES,
        ATIVO_TRILHOES - LAG(ATIVO_TRILHOES) OVER (ORDER BY DATA) as VARIACAO
    FROM mensal
    ORDER BY DATA
""")
```

### Subqueries

```python
# Instituicoes acima da media
df = bcb.sql("""
    SELECT CNPJ_8, INSTITUICAO, VALOR / 1e9 as ATIVO_BILHOES
    FROM '{cache}/cosif/prudencial/*.parquet'
    WHERE DATA = 202412
      AND CONTA = 'TOTAL GERAL DO ATIVO'
      AND VALOR > (
          SELECT AVG(VALOR)
          FROM '{cache}/cosif/prudencial/*.parquet'
          WHERE DATA = 202412
            AND CONTA = 'TOTAL GERAL DO ATIVO'
      )
    ORDER BY VALOR DESC
""")
```

### CASE WHEN

```python
# Classificar instituicoes por porte
df = bcb.sql("""
    SELECT
        CNPJ_8,
        INSTITUICAO,
        VALOR / 1e9 as ATIVO_BILHOES,
        CASE
            WHEN VALOR >= 1e12 THEN 'Grande (> 1 tri)'
            WHEN VALOR >= 100e9 THEN 'Medio (100bi - 1tri)'
            WHEN VALOR >= 10e9 THEN 'Pequeno (10bi - 100bi)'
            ELSE 'Micro (< 10bi)'
        END as PORTE
    FROM '{cache}/cosif/prudencial/*.parquet'
    WHERE DATA = 202412
      AND CONTA = 'TOTAL GERAL DO ATIVO'
    ORDER BY VALOR DESC
""")
```

### Analise por Segmento

```python
# Agregacao por segmento usando cadastro
df = bcb.sql("""
    SELECT
        cad.SEGMENTO,
        COUNT(DISTINCT c.CNPJ_8) as N_INSTITUICOES,
        SUM(c.VALOR) / 1e12 as ATIVO_TRILHOES,
        AVG(c.VALOR) / 1e9 as MEDIA_BILHOES
    FROM '{cache}/cosif/prudencial/*.parquet' c
    JOIN '{cache}/ifdata/cadastro/*.parquet' cad
        ON c.CNPJ_8 = cad.CNPJ_8
    WHERE c.DATA = 202412
      AND cad.DATA = 202412
      AND c.CONTA = 'TOTAL GERAL DO ATIVO'
      AND c.DOCUMENTO = 4060
    GROUP BY cad.SEGMENTO
    HAVING COUNT(DISTINCT c.CNPJ_8) >= 5
    ORDER BY ATIVO_TRILHOES DESC
""")
```

### Serie Temporal

```python
# Evolucao trimestral de uma instituicao
df = bcb.sql("""
    SELECT
        DATA,
        CNPJ_8,
        INSTITUICAO,
        CONTA,
        VALOR / 1e9 as VALOR_BILHOES
    FROM '{cache}/cosif/prudencial/*.parquet'
    WHERE CNPJ_8 = '60872504'
      AND CONTA IN ('TOTAL GERAL DO ATIVO', 'PATRIMONIO LIQUIDO')
      AND DATA >= 202401
    ORDER BY DATA, CONTA
""")
```

## Performance

### Predicate Pushdown

O DuckDB automaticamente empurra filtros para a leitura do Parquet:

```python
# Filtros no WHERE sao aplicados durante a leitura, nao apos
df = bcb.sql("""
    SELECT *
    FROM '{cache}/cosif/prudencial/*.parquet'
    WHERE DATA = 202412           -- Pushdown: le apenas arquivos de 202412
      AND CNPJ_8 = '60872504'     -- Pushdown: filtra durante leitura
""")
```

### Column Pruning

Especificar colunas evita carregar dados desnecessarios:

```python
# BOM: apenas colunas necessarias
df = bcb.sql("""
    SELECT CNPJ_8, VALOR
    FROM '{cache}/cosif/prudencial/*.parquet'
    WHERE DATA = 202412
""")

# EVITAR: SELECT * carrega tudo
df = bcb.sql("""
    SELECT *
    FROM '{cache}/cosif/prudencial/*.parquet'
    WHERE DATA = 202412
""")
```

### Glob Patterns

Use patterns especificos quando possivel:

```python
# BOM: arquivo especifico
df = bcb.sql("""
    SELECT * FROM '{cache}/cosif/prudencial/cosif_prud_202412.parquet'
""")

# BOM: ano especifico
df = bcb.sql("""
    SELECT * FROM '{cache}/cosif/prudencial/cosif_prud_2024*.parquet'
""")

# MAIS LENTO: todos os arquivos
df = bcb.sql("""
    SELECT * FROM '{cache}/cosif/prudencial/*.parquet'
""")
```

### LIMIT

Sempre use LIMIT para explorar dados:

```python
# Explorar estrutura
df = bcb.sql("""
    SELECT * FROM '{cache}/cosif/prudencial/*.parquet' LIMIT 100
""")

# Ver valores unicos
df = bcb.sql("""
    SELECT DISTINCT CONTA
    FROM '{cache}/cosif/prudencial/*.parquet'
    WHERE DATA = 202412
    LIMIT 100
""")
```

## QueryEngine Direto

Para uso mais avancado, acesse o QueryEngine diretamente:

```python
from ifdata_bcb.infra import QueryEngine

qe = QueryEngine()

# Listar arquivos
arquivos = qe.list_files('cosif/prudencial')

# Verificar schema
schema = qe.describe('cosif_prud_202412', 'cosif/prudencial')

# Ler com filtros
df = qe.read_glob(
    pattern='cosif_prud_2024*.parquet',
    subdir='cosif/prudencial',
    columns=['CNPJ_8', 'CONTA', 'VALOR'],
    where="CNPJ_8 = '60872504'"
)

# SQL direto
df = qe.sql("SELECT COUNT(*) FROM '{cache}/cosif/prudencial/*.parquet'")
```

## Tratamento de Erros

```python
# Erro de sintaxe SQL
try:
    df = bcb.sql("SELECT * FORM tabela")  # Erro: FORM ao inves de FROM
except Exception as e:
    print(f"Erro SQL: {e}")

# Arquivo nao encontrado (retorna DataFrame vazio)
df = bcb.sql("SELECT * FROM '{cache}/nao_existe/*.parquet'")
if df.empty:
    print("Nenhum dado encontrado")
```

## Referencias

- [Documentacao DuckDB](https://duckdb.org/docs/)
- [DuckDB SQL Reference](https://duckdb.org/docs/sql/introduction)
- [Parquet com DuckDB](https://duckdb.org/docs/data/parquet/overview)
