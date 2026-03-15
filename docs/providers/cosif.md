# Provider COSIF

O COSIF (Plano Contabil das Instituicoes do Sistema Financeiro Nacional) contem dados contabeis mensais das instituicoes financeiras brasileiras.

## Visao Geral

### Origem dos Dados

Os dados COSIF sao publicados pelo Banco Central do Brasil e disponibilizados em:
- **URL Base**: `https://www.bcb.gov.br/content/estabilidadefinanceira/cosif/`
- **Formato Original**: CSV compactado em ZIP
- **Encoding**: `cp1252` (individual) ou `latin-1` (prudencial)

### Periodicidade

- **Frequencia**: Mensal
- **Formato**: YYYYMM (ex: 202412 para dezembro de 2024)
- **Disponibilidade**: Geralmente com 1-2 meses de atraso

## Escopos

O COSIF possui dois escopos que representam visoes diferentes dos dados contabeis:

### Individual

Dados de cada instituicao financeira separadamente.

```python
# Coletar apenas escopo individual
bcb.cosif.collect('2024-01', '2024-12', escopo='individual')

# Consultar dados individuais (instituicao e start sao obrigatorios)
df = bcb.cosif.read(instituicao='60872504', start='2024-12', escopo='individual')
```

**Quando usar**:
- Analise de instituicoes especificas
- Comparacao entre entidades do mesmo grupo
- Estudos de concentracao por instituicao

### Prudencial

Dados consolidados do conglomerado prudencial (grupo de empresas sob mesma gestao).

```python
# Coletar apenas escopo prudencial
bcb.cosif.collect('2024-01', '2024-12', escopo='prudencial')

# Consultar dados prudenciais (instituicao e start sao obrigatorios)
df = bcb.cosif.read(instituicao='60872504', start='2024-12', escopo='prudencial')
```

**Quando usar**:
- Analise de grupos financeiros
- Comparacao entre conglomerados
- Visao consolidada de ativos e passivos

### Diferenca Entre Escopos

```python
# Comparar Ativo Total nos dois escopos
ativo_ind = bcb.cosif.read(instituicao='60872504', conta=['TOTAL GERAL DO ATIVO'],
                           start='2024-12', escopo='individual')
ativo_prud = bcb.cosif.read(instituicao='60872504', conta=['TOTAL GERAL DO ATIVO'],
                            start='2024-12', escopo='prudencial')

print(f"Individual: R$ {ativo_ind['VALOR'].iloc[0]:,.2f}")
print(f"Prudencial: R$ {ativo_prud['VALOR'].iloc[0]:,.2f}")
```

O valor prudencial geralmente e maior pois inclui todas as empresas do conglomerado.

## API Reference

### collect()

Coleta dados COSIF do BCB.

```python
bcb.cosif.collect(
    start: str,           # Data inicial (YYYY-MM)
    end: str,             # Data final (YYYY-MM)
    escopo: str = None,   # 'individual', 'prudencial', ou None (ambos)
    force: bool = False,  # Se True, recoleta dados existentes
    verbose: bool = True  # Se True, exibe progresso
)
```

**Exemplos**:

```python
# Coletar ambos os escopos (padrao)
bcb.cosif.collect('2024-01', '2024-12')

# Coletar apenas prudencial
bcb.cosif.collect('2024-01', '2024-12', escopo='prudencial')

# Forcar recoleta
bcb.cosif.collect('2024-12', '2024-12', force=True)
```

### read()

Le dados COSIF com filtros.

```python
bcb.cosif.read(
    instituicao: str | list,      # CNPJ(s) de 8 digitos. OBRIGATORIO
    start: str,                   # Data inicial ou unica (YYYY-MM). OBRIGATORIO
    end: str = None,              # Data final para range (YYYY-MM)
    conta: str | list = None,     # Nome(s) da(s) conta(s). Filtro case-insensitive
    escopo: str = None,           # 'individual', 'prudencial', ou None (TODOS)
    columns: list = None          # Colunas especificas
) -> pd.DataFrame
```

**Importante**: Os parametros `instituicao` e `start` sao **obrigatorios**. O parametro `escopo=None` busca em **todos** os escopos (inclui coluna `ESCOPO` no resultado).

**API de datas**:
- `start` sozinho: filtra data unica (ex: `start='2024-12'`)
- `start` + `end`: gera range mensal automatico (ex: `start='2024-01', end='2024-12'`)

**Raises**:
- `MissingRequiredParameterError`: Se `instituicao` ou `start` nao fornecidos.
- `InvalidDateRangeError`: Se `start > end`.

**Exemplos**:

```python
# Data unica em um escopo especifico
df = bcb.cosif.read(instituicao='60872504', start='2024-12', escopo='prudencial')

# Buscar em TODOS os escopos (escopo=None)
df = bcb.cosif.read(instituicao='60872504', start='2024-12')

# Conta especifica (filtro case-insensitive)
df = bcb.cosif.read(instituicao='60872504', start='2024-12',
                    conta='total geral do ativo', escopo='prudencial')

# Multiplas contas e range de datas
df = bcb.cosif.read(
    instituicao='60872504',
    start='2024-09',
    end='2024-12',
    conta=['TOTAL GERAL DO ATIVO', 'PATRIMONIO LIQUIDO'],
    escopo='prudencial'
)

# Apenas colunas especificas
df = bcb.cosif.read(instituicao='60872504', start='2024-12', escopo='prudencial',
                    columns=['CNPJ_8', 'CONTA', 'VALOR', 'DATA'])
```

### read_by_account_code()

Le dados por codigo de conta COSIF.

```python
bcb.cosif.read_by_account_code(
    cod_conta: str,               # Codigo da conta (ex: '10000007')
    instituicao: str | list,      # CNPJ(s) de 8 digitos. OBRIGATORIO
    start: str,                   # Data inicial ou unica (YYYY-MM). OBRIGATORIO
    end: str = None,              # Data final para range (YYYY-MM)
    escopo: str = None            # 'individual', 'prudencial', ou None (TODOS)
) -> pd.DataFrame
```

**Raises**:
- `MissingRequiredParameterError`: Se `instituicao` ou `start` nao fornecidos.
- `InvalidDateRangeError`: Se `start > end`.

**Exemplo**:

```python
# Buscar por codigo de conta
df = bcb.cosif.read_by_account_code('10000007', instituicao='60872504', start='2024-12', escopo='prudencial')
```

### list_accounts()

Lista contas disponiveis nos dados.

```python
bcb.cosif.list_accounts(
    escopo: str = None,  # 'individual', 'prudencial', ou None (ambos)
    limit: int = 100     # Numero maximo de contas
) -> pd.DataFrame
```

**Retorna**: DataFrame com colunas `COD_CONTA`, `CONTA` e `ESCOPO` (quando escopo=None).

**Nota**: A coluna foi renomeada de `NOME_CONTA` para `CONTA`.

**Exemplo**:

```python
# Listar contas de ambos escopos
contas = bcb.cosif.list_accounts()

# Listar contas do prudencial apenas
contas = bcb.cosif.list_accounts(escopo='prudencial', limit=50)
```

### list_institutions()

Lista instituicoes disponiveis nos dados.

```python
bcb.cosif.list_institutions(
    start: str = None,         # Data inicial ou unica (YYYY-MM)
    end: str = None,           # Data final para range (YYYY-MM)
    escopo: str = None         # 'individual', 'prudencial', ou None (ambos)
) -> pd.DataFrame
```

**Retorna**: DataFrame com colunas `CNPJ_8`, `INSTITUICAO` e `ESCOPO` (quando escopo=None).

**Exemplo**:

```python
# Listar instituicoes de dezembro/2024
inst = bcb.cosif.list_institutions(start='2024-12')

# Listar apenas do prudencial
inst = bcb.cosif.list_institutions(start='2024-12', escopo='prudencial')

# Listar de um range de periodos
inst = bcb.cosif.list_institutions(start='2024-01', end='2024-12')
```

### list_periods()

Lista periodos disponiveis.

```python
periodos = bcb.cosif.list_periods()  # Retorna [202401, 202402, ...]
```

### describe()

Retorna informacoes sobre os dados disponiveis.

```python
info = bcb.cosif.describe()
# {
#     'subdir': 'cosif/individual',
#     'prefix': 'cosif_ind',
#     'periods': [202401, 202402, ...],
#     'period_count': 12,
#     'has_data': True,
#     'first_period': 202401,
#     'last_period': 202412
# }
```

## Colunas Disponiveis

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| `DATA` | datetime | Periodo de referencia |
| `CNPJ_8` | str | CNPJ de 8 digitos |
| `INSTITUICAO` | str | Nome da instituicao |
| `ESCOPO` | str | Escopo dos dados (individual, prudencial) - incluso quando escopo=None |
| `DOCUMENTO` | int | Tipo de documento (4060, 4066, etc.) |
| `COD_CONTA` | str | Codigo da conta COSIF |
| `CONTA` | str | Nome/descricao da conta |
| `VALOR` | float | Valor em reais |

### Sobre a Coluna DOCUMENTO

O campo DOCUMENTO indica o tipo de demonstracao:
- **4060**: Balancete (Ativo, Passivo, PL)
- **4066**: DRE (Receitas, Despesas)

## Exemplos Avancados

### Multiplas Contas

```python
# Principais contas de balanco
contas_balanco = [
    'TOTAL GERAL DO ATIVO',
    'PATRIMONIO LIQUIDO',
    'DISPONIBILIDADES',
    'OPERACOES DE CREDITO'
]

df = bcb.cosif.read(
    instituicao='60746948',
    conta=contas_balanco,
    start='2024-12',
    escopo='prudencial'
)

# Pivotar para visualizacao
pivot = df.pivot_table(
    values='VALOR',
    index='INSTITUICAO',
    columns='CONTA',
    aggfunc='sum'
)
```

### Comparar Escopos

```python
import pandas as pd

# Coletar dados de ambos escopos
df_ind = bcb.cosif.read(instituicao='60872504', conta=['TOTAL GERAL DO ATIVO'],
                        start='2024-12', escopo='individual')
df_prud = bcb.cosif.read(instituicao='60872504', conta=['TOTAL GERAL DO ATIVO'],
                         start='2024-12', escopo='prudencial')

# Comparar
df_ind['ESCOPO'] = 'Individual'
df_prud['ESCOPO'] = 'Prudencial'

comparacao = pd.concat([df_ind, df_prud])
print(comparacao[['ESCOPO', 'INSTITUICAO', 'VALOR']])
```

### Evolucao Temporal

```python
# Serie temporal do Patrimonio Liquido
df = bcb.cosif.read(
    instituicao='60746948',
    conta=['PATRIMONIO LIQUIDO'],
    start='2024-01',
    end='2024-12',
    escopo='prudencial'
)

# Ordenar por data e plotar
df_sorted = df.sort_values('DATA')
df_sorted.plot(x='DATA', y='VALOR', kind='line', title='Patrimonio Liquido - Bradesco 2024')
```

### Top Instituicoes por Ativo

```python
# Usando SQL para ranking
df = bcb.sql("""
    SELECT
        CNPJ_8,
        INSTITUICAO,
        VALOR / 1e12 as ATIVO_TRILHOES
    FROM '{cache}/cosif/prudencial/*.parquet'
    WHERE DATA = 202412
      AND CONTA = 'TOTAL GERAL DO ATIVO'
      AND DOCUMENTO = 4060
    ORDER BY VALOR DESC
    LIMIT 10
""")
```

## URLs e Formato de Origem

### Estrutura das URLs

```
# Individual
https://www.bcb.gov.br/content/estabilidadefinanceira/cosif/Bancos/{YYYYMM}BANCOS.csv.zip

# Prudencial
https://www.bcb.gov.br/content/estabilidadefinanceira/cosif/Conglomerados-prudenciais/{YYYYMM}BLOPRUDENCIAL.csv.zip
```

### Estrutura do CSV Original

O CSV original tem:
- 3 linhas de metadata (ignoradas)
- Header na linha 4 comecando com `#DATA_BASE`
- Separador: `;` (ponto-e-virgula)
- Encoding: `cp1252` (individual) ou `latin-1` (prudencial)

Colunas originais:
```
#DATA_BASE;DOCUMENTO;CNPJ;AGENCIA;INSTITUICAO;COD_CONGL;NOME_CONGL;TAXONOMIA;CONTA;CONTA;SALDO
```

A biblioteca normaliza para o formato padronizado (ver secao Colunas Disponiveis).

## Tratamento de Erros

```python
from ifdata_bcb import (
    MissingRequiredParameterError,
    InvalidDateRangeError,
    InvalidIdentifierError,
)

# Erro: parametro obrigatorio ausente
try:
    df = bcb.cosif.read(instituicao='60872504')  # Falta start!
except MissingRequiredParameterError as e:
    print(f"Erro: {e}")
    # Forneca o parametro start (YYYY-MM)

# Erro: range de datas invalido
try:
    df = bcb.cosif.read(instituicao='60872504', start='2024-12', end='2024-01')  # start > end!
except InvalidDateRangeError as e:
    print(f"Erro: {e}")

# Erro: identificador invalido
try:
    df = bcb.cosif.read(instituicao='Itau', start='2024-12')  # Nome nao permitido!
except InvalidIdentifierError as e:
    print(f"Erro: {e}")
    # Use bcb.search('Itau') para encontrar o CNPJ
```
