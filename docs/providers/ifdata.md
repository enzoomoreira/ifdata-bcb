# Provider IFDATA

O IFDATA (Informacoes Financeiras Trimestrais) contem dados financeiros trimestrais das instituicoes financeiras brasileiras.

## Visao Geral

### Origem dos Dados

Os dados IFDATA sao disponibilizados pelo Banco Central do Brasil via API OData:
- **URL Base**: `https://olinda.bcb.gov.br/olinda/servico/IFDATA/versao/v1/odata`
- **Formato**: CSV via parametro `$format=text/csv`
- **Encoding**: UTF-8

### Periodicidade

- **Frequencia**: Trimestral
- **Meses de Fechamento**: Marco (03), Junho (06), Setembro (09), Dezembro (12)
- **Formato**: YYYYMM (ex: 202412 para dezembro de 2024)
- **Disponibilidade**: Geralmente com 1-2 meses de atraso apos o fechamento

## Escopos IFDATA

O IFDATA suporta tres escopos que determinam a visao dos dados:

| Escopo | TIPO_INST | Descricao |
|--------|-----------|-----------|
| `individual` | 3 | Dados da instituicao especifica |
| `prudencial` | 1 | Dados do conglomerado prudencial |
| `financeiro` | 2 | Dados do conglomerado financeiro |

```python
# Filtrar por escopo (resolve CNPJ automaticamente)
df = bcb.ifdata.read(instituicao='60872504', start='2024-12', escopo='prudencial')

# Buscar em todos os escopos (escopo=None)
df = bcb.ifdata.read(instituicao='60872504', start='2024-12')

# Verificar tipos disponiveis
inst = bcb.ifdata.list_institutions(start='2024-12')
print(inst['TIPO_INST'].unique())  # [1, 2, 3]
```

## API Reference

### collect()

Coleta dados IFDATA Valores do BCB.

```python
bcb.ifdata.collect(
    start: str,           # Data inicial (YYYY-MM)
    end: str,             # Data final (YYYY-MM)
    force: bool = False   # Se True, recoleta dados existentes
)
```

**Nota**: Apenas trimestres (03, 06, 09, 12) serao coletados.

**Exemplos**:

```python
# Coletar dados de 2024
bcb.ifdata.collect('2024-01', '2024-12')
# Coleta apenas: 202403, 202406, 202409, 202412

# Forcar recoleta
bcb.ifdata.collect('2024-12', '2024-12', force=True)
```

### read()

Le dados IFDATA Valores com filtros.

```python
bcb.ifdata.read(
    instituicao: str | list,      # CNPJ(s) de 8 digitos. OBRIGATORIO
    start: str,                   # Data inicial ou unica (YYYY-MM). OBRIGATORIO
    end: str = None,              # Data final para range (YYYY-MM)
    conta: str | list = None,     # Nome(s) da(s) conta(s). Filtro case-insensitive
    columns: list = None,         # Colunas especificas
    escopo: str = None,           # 'individual', 'prudencial', 'financeiro', ou None (TODOS)
    relatorio: str = None         # Nome do relatorio para filtrar
) -> pd.DataFrame
```

**Importante**: Os parametros `instituicao` e `start` sao **obrigatorios**. O parametro `escopo=None` busca em **todos** os escopos (inclui coluna `ESCOPO` no resultado).

**API de datas**:
- `start` sozinho: filtra data unica (ex: `start='2024-12'`)
- `start` + `end`: gera range trimestral automatico (ex: `start='2024-01', end='2024-12'` -> 202403, 202406, 202409, 202412)

**Raises**:
- `MissingRequiredParameterError`: Se `instituicao` ou `start` nao fornecidos.
- `InvalidDateRangeError`: Se `start > end`.

**Exemplos**:

```python
# Data unica em um escopo especifico
df = bcb.ifdata.read(instituicao='60872504', start='2024-12', escopo='prudencial')

# Buscar em TODOS os escopos (escopo=None)
df = bcb.ifdata.read(instituicao='60872504', start='2024-12')

# Conta especifica (filtro case-insensitive)
df = bcb.ifdata.read(instituicao='60872504', start='2024-12', conta='lucro liquido')

# Multiplas contas com range de datas
df = bcb.ifdata.read(
    instituicao='60872504',
    start='2024-09',
    end='2024-12',
    conta=['Lucro Liquido', 'Ativo Total']
)
```

### read_by_account_code()

Le dados por codigo de conta IFDATA.

```python
bcb.ifdata.read_by_account_code(
    cod_conta: str,               # Codigo da conta
    instituicao: str | list,      # CNPJ(s) de 8 digitos. OBRIGATORIO
    start: str,                   # Data inicial ou unica (YYYY-MM). OBRIGATORIO
    end: str = None               # Data final para range (YYYY-MM)
) -> pd.DataFrame
```

**Raises**:
- `MissingRequiredParameterError`: Se `instituicao` ou `start` nao fornecidos.
- `InvalidDateRangeError`: Se `start > end`.

**Exemplo**:

```python
# Buscar por codigo de conta
df = bcb.ifdata.read_by_account_code('78187', instituicao='60872504', start='2024-12')
```

### list_accounts()

Lista contas disponiveis nos dados.

```python
bcb.ifdata.list_accounts(
    limit: int = 100  # Numero maximo de contas
) -> pd.DataFrame
```

**Retorna**: DataFrame com colunas `COD_CONTA` e `CONTA`.

**Exemplo**:

```python
contas = bcb.ifdata.list_accounts(limit=50)
print(contas)
```

### list_institutions()

Lista instituicoes disponiveis nos dados.

```python
bcb.ifdata.list_institutions(
    start: str = None,  # Data inicial ou unica (YYYY-MM)
    end: str = None     # Data final para range (YYYY-MM)
) -> pd.DataFrame
```

**Retorna**: DataFrame com colunas `COD_INST`, `TIPO_INST` e `INSTITUICAO` (para tipo_inst=3).

**Exemplo**:

```python
# Listar instituicoes de dezembro/2024
inst = bcb.ifdata.list_institutions(start='2024-12')

# Listar de um range de periodos
inst = bcb.ifdata.list_institutions(start='2024-01', end='2024-12')

# Contar por tipo
print(inst.groupby('TIPO_INST').size())
```

### list_periods()

Lista periodos disponiveis.

```python
periodos = bcb.ifdata.list_periods()  # Retorna [202403, 202406, ...]
```

### describe()

Retorna informacoes sobre os dados disponiveis.

```python
info = bcb.ifdata.describe()
```

## Colunas Disponiveis

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| `DATA` | datetime | Periodo de referencia (trimestral) |
| `CNPJ_8` | str | CNPJ original da consulta |
| `INSTITUICAO` | str | Nome da instituicao |
| `ESCOPO` | str | Escopo dos dados (individual, prudencial, financeiro) |
| `COD_INST` | str | Codigo da instituicao (CNPJ ou codigo interno) |
| `TIPO_INST` | int | Tipo de instituicao (1=prudencial, 2=financeiro, 3=individual) |
| `COD_CONTA` | str | Codigo da conta |
| `CONTA` | str | Nome/descricao da conta |
| `VALOR` | float | Valor em reais |
| `RELATORIO` | str | Nome do relatorio de origem |
| `GRUPO` | str | Grupo da conta |

### Sobre RELATORIO

Indica a origem dos dados:
- **Resumo**: Indicadores principais
- **Ativo**: Composicao do ativo
- **Passivo**: Composicao do passivo
- **DRE**: Demonstracao do Resultado

### Sobre GRUPO

Agrupamento logico das contas para navegacao hierarquica.

## Exemplos Avancados

### Filtrar por Escopo

```python
# Apenas escopo prudencial (conglomerados)
df_prud = bcb.ifdata.read(instituicao='60872504', start='2024-12', escopo='prudencial')
print(f"Escopo: {df_prud['ESCOPO'].iloc[0]}")

# Todos os escopos disponiveis
df_todos = bcb.ifdata.read(instituicao='60872504', start='2024-12')
print(f"Escopos: {df_todos['ESCOPO'].unique()}")
```

### Analisar Grupos de Contas

```python
# Listar grupos disponiveis
df = bcb.ifdata.read(instituicao='60872504', start='2024-12', escopo='prudencial')
grupos = df['GRUPO'].unique()
print(f"Grupos: {grupos}")

# Filtrar por grupo especifico usando SQL
df = bcb.sql("""
    SELECT COD_INST, CONTA, VALOR
    FROM '{cache}/ifdata/valores/*.parquet'
    WHERE DATA = 202412
      AND GRUPO = 'Indicadores'
      AND COD_INST = '60872504'
""")
```

### Serie Temporal de Lucro

```python
# Evolucao trimestral do Lucro Liquido
df = bcb.ifdata.read(
    instituicao='60872504',
    conta=['Lucro Liquido'],
    start='2023-01',
    end='2024-12'
)

# Ordenar e plotar
df_sorted = df.sort_values('DATA')
print(df_sorted[['DATA', 'VALOR']])
```

### Comparar Instituicoes

```python
# CNPJs dos maiores bancos
bancos = ['60872504', '60746948', '90400888', '00000000']

# Buscar Lucro Liquido de cada um
resultados = []
for cnpj in bancos:
    df = bcb.ifdata.read(instituicao=cnpj, start='2024-12', conta=['Lucro Liquido'], escopo='prudencial')
    if not df.empty:
        resultados.append({
            'CNPJ': cnpj,
            'Nome': df['INSTITUICAO'].iloc[0],
            'Lucro': df['VALOR'].iloc[0]
        })

import pandas as pd
ranking = pd.DataFrame(resultados).sort_values('Lucro', ascending=False)
print(ranking)
```

### Ranking por Ativo Total

```python
# Usando SQL para ranking
df = bcb.sql("""
    SELECT
        COD_INST,
        CONTA,
        VALOR / 1e9 as VALOR_BILHOES
    FROM '{cache}/ifdata/valores/*.parquet'
    WHERE DATA = 202412
      AND CONTA = 'Ativo Total'
      AND TIPO_INST = 1
    ORDER BY VALOR DESC
    LIMIT 20
""")
```

## URLs e Formato de Origem

### Estrutura das URLs

```
# Valores (3 tipos de instituicao)
https://olinda.bcb.gov.br/olinda/servico/IFDATA/versao/v1/odata/
  IfDataValores(AnoMes=@AnoMes,TipoInstituicao=@TipoInstituicao,Relatorio=@Relatorio)
  ?@AnoMes={YYYYMM}&@TipoInstituicao={1|2|3}&@Relatorio='T'&$format=text/csv
```

### Colunas do CSV Original

```
AnoMes,CodInst,TipoInstituicao,Conta,NomeColuna,Saldo,NomeRelatorio,Grupo
```

A biblioteca normaliza para o formato padronizado (ver secao Colunas Disponiveis).

## Diferenca Entre COSIF e IFDATA

| Aspecto | COSIF | IFDATA |
|---------|-------|--------|
| Periodicidade | Mensal | Trimestral |
| Plano de Contas | COSIF completo | Resumido |
| Escopos | Individual e Prudencial | Tipos 1, 2, 3 |
| Formato Original | CSV com metadata | CSV limpo |
| Detalhamento | Maior (milhares de contas) | Menor (centenas de contas) |

## Tratamento de Erros

```python
from ifdata_bcb import (
    MissingRequiredParameterError,
    InvalidDateRangeError,
    InvalidIdentifierError,
)

# Erro: parametro obrigatorio ausente
try:
    df = bcb.ifdata.read(instituicao='60872504')  # Falta start!
except MissingRequiredParameterError as e:
    print(f"Erro: {e}")
    # Forneca o parametro start (YYYY-MM)

# Erro: range de datas invalido
try:
    df = bcb.ifdata.read(instituicao='60872504', start='2024-12', end='2024-01')  # start > end!
except InvalidDateRangeError as e:
    print(f"Erro: {e}")

# Erro: identificador invalido
try:
    df = bcb.ifdata.read(instituicao='Itau', start='2024-12')  # Nome nao permitido!
except InvalidIdentifierError as e:
    print(f"Erro: {e}")
    # Use bcb.search('Itau') para encontrar o CNPJ

# Sem erro: retorna DataFrame vazio se nao encontrar dados
df = bcb.ifdata.read(instituicao='99999999', start='2024-12')  # CNPJ inexistente
if df.empty:
    print("Instituicao nao encontrada nos dados IFDATA")
```
