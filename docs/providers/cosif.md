# Provider COSIF

O COSIF (Plano Contabil das Instituicoes do Sistema Financeiro Nacional) contem dados contabeis mensais das instituicoes financeiras brasileiras.

## Visao Geral

### Origem dos Dados

Os dados COSIF sao publicados pelo Banco Central do Brasil:

- **URL Base**: `https://www.bcb.gov.br/content/estabilidadefinanceira/cosif/`
- **Formato Original**: CSV compactado (ZIP)
- **Encoding**: `cp1252` (individual) ou `latin-1` (prudencial)

### Periodicidade

- **Frequencia**: Mensal
- **Formato**: YYYYMM (ex: 202412 para dezembro de 2024)
- **Disponibilidade**: Geralmente 1-2 meses de atraso

## Escopos

O COSIF possui dois escopos que representam visoes diferentes dos dados contabeis:

### Individual

Dados de cada instituicao financeira separadamente.

```python
# Coletar apenas escopo individual
bcb.cosif.collect('2024-01', '2024-12', escopo='individual')

# Consultar dados individuais
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

# Consultar dados prudenciais
df = bcb.cosif.read(instituicao='60872504', start='2024-12', escopo='prudencial')
```

**Quando usar**:
- Analise de grupos financeiros
- Comparacao entre conglomerados
- Visao consolidada de ativos e passivos

### Buscando em Todos os Escopos

Quando `escopo=None` (padrao), a busca e feita em ambos os escopos e uma coluna `ESCOPO` e incluida no resultado:

```python
# Buscar em TODOS os escopos
df = bcb.cosif.read(instituicao='60872504', start='2024-12')
# Resultado inclui coluna ESCOPO com valores 'individual' ou 'prudencial'
```

## API Reference

### collect()

Coleta dados COSIF do BCB.

```python
bcb.cosif.collect(
    start: str,                    # Data inicial (YYYY-MM)
    end: str,                      # Data final (YYYY-MM)
    escopo: str | None = None,     # 'individual', 'prudencial', ou None (ambos)
    force: bool = False,           # Se True, recoleta dados existentes
    verbose: bool = True           # Se True, exibe progresso
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
    instituicao: str | list[str],           # CNPJ(s) de 8 digitos. OBRIGATORIO
    start: str,                             # Data inicial ou unica. OBRIGATORIO
    end: str | None = None,                 # Data final para range
    conta: str | list[str] | None = None,   # Nome ou codigo da conta (case-insensitive)
    escopo: str | None = None,              # 'individual', 'prudencial', ou None (TODOS)
    columns: list[str] | None = None,       # Colunas especificas
    documento: str | list[str] | None = None,  # Tipo de documento (ex: balancete, semestral)
    cadastro: list[str] | None = None       # Colunas cadastrais para enriquecer o resultado
) -> pd.DataFrame
```

**Parametros Obrigatorios**: `instituicao` e `start`.

**API de Datas**:
- `start` sozinho: filtra data unica (ex: `start='2024-12'`)
- `start` + `end`: gera range mensal automatico

**Raises**:
- `MissingRequiredParameterError`: Se `instituicao` ou `start` nao fornecidos.
- `InvalidDateRangeError`: Se `start > end`.

**Exemplos**:

```python
# Data unica em um escopo especifico
df = bcb.cosif.read(instituicao='60872504', start='2024-12', escopo='prudencial')

# Buscar em TODOS os escopos
df = bcb.cosif.read(instituicao='60872504', start='2024-12')

# Conta especifica (filtro case-insensitive)
df = bcb.cosif.read(
    instituicao='60872504',
    start='2024-12',
    conta='total geral do ativo',
    escopo='prudencial'
)

# Multiplas contas e range de datas
df = bcb.cosif.read(
    instituicao='60872504',
    start='2024-01',
    end='2024-12',
    conta=['TOTAL GERAL DO ATIVO', 'PATRIMONIO LIQUIDO'],
    escopo='prudencial'
)

# Filtrar por codigo de conta (numerico)
df = bcb.cosif.read(
    instituicao='60872504',
    start='2024-12',
    conta='10100',
    escopo='prudencial'
)

# Filtrar por tipo de documento
df = bcb.cosif.read(
    instituicao='60872504',
    start='2024-12',
    escopo='prudencial',
    documento='4060'
)

# Apenas colunas especificas
df = bcb.cosif.read(
    instituicao='60872504',
    start='2024-12',
    escopo='prudencial',
    columns=['CNPJ_8', 'CONTA', 'VALOR', 'DATA']
)
```

### list_contas()

Lista contas disponiveis nos dados.

```python
bcb.cosif.list_contas(
    termo: str | None = None,      # Filtro por nome (case-insensitive)
    escopo: str | None = None,     # 'individual', 'prudencial', ou None (ambos)
    start: str | None = None,      # Periodo inicial (filtra contas que existem no periodo)
    end: str | None = None,        # Periodo final. Se None com start, filtra data unica
    limit: int = 100               # Numero maximo de contas
) -> pd.DataFrame
```

**Retorna**: DataFrame com colunas `COD_CONTA`, `CONTA` e `ESCOPOS` (quando escopo=None, string com escopos separados por virgula).

**Exemplos**:

```python
# Listar todas as contas (ambos escopos)
contas = bcb.cosif.list_contas()

# Buscar contas que contenham "deposito"
contas = bcb.cosif.list_contas(termo='deposito')

# Listar contas do prudencial apenas
contas = bcb.cosif.list_contas(escopo='prudencial', limit=50)
```

### list_instituicoes()

Lista instituicoes disponiveis nos dados.

```python
bcb.cosif.list_instituicoes(
    start: str | None = None,      # Data inicial ou unica
    end: str | None = None,        # Data final para range
    escopo: str | None = None      # 'individual', 'prudencial', ou None (ambos)
) -> pd.DataFrame
```

**Retorna**: Quando escopo=None, DataFrame com colunas `CNPJ_8`, `INSTITUICAO`, `TEM_INDIVIDUAL` (bool) e `TEM_PRUDENCIAL` (bool). Quando escopo especificado, retorna `CNPJ_8` e `INSTITUICAO`.

**Exemplos**:

```python
# Listar instituicoes de dezembro/2024
inst = bcb.cosif.list_instituicoes(start='2024-12')

# Listar apenas do prudencial
inst = bcb.cosif.list_instituicoes(start='2024-12', escopo='prudencial')

# Listar de um range de periodos
inst = bcb.cosif.list_instituicoes(start='2024-01', end='2024-12')
```

### list_periodos()

Lista periodos disponiveis (herdado de BaseExplorer).

```python
periodos = bcb.cosif.list_periodos()  # Retorna [202401, 202402, ...]
```

### describe()

Retorna informacoes sobre os dados disponiveis (herdado de BaseExplorer).

```python
info = bcb.cosif.describe()
# {
#     'sources': ['individual', 'prudencial'],
#     'periods': [202401, 202402, ...],
#     'period_count': 12,
#     'has_data': True,
#     'first_period': 202401,
#     'last_period': 202412,
#     'by_source': {
#         'individual': {'subdir': 'cosif/individual', 'prefix': 'cosif_ind', ...},
#         'prudencial': {'subdir': 'cosif/prudencial', 'prefix': 'cosif_prud', ...},
#     }
# }
```

## Colunas Disponiveis

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| `DATA` | datetime | Periodo de referencia |
| `CNPJ_8` | str | CNPJ de 8 digitos |
| `INSTITUICAO` | str | Nome da instituicao (canônico do cadastro) |
| `ESCOPO` | str | Escopo dos dados (individual, prudencial) |
| `COD_CONTA` | str | Codigo numerico da conta COSIF |
| `CONTA` | str | Nome/descricao da conta |
| `DOCUMENTO` | int | Tipo de documento (ex: 4060 = balancete) |
| `VALOR` | float | Valor em reais |

### Enriquecimento Cadastral

O parametro `cadastro` permite adicionar colunas do cadastro diretamente no resultado, sem precisar fazer merge manual:

```python
# Adicionar segmento e UF a cada linha
df = bcb.cosif.read(
    instituicao=['60872504', '60746948'],
    start='2024-01',
    end='2024-12',
    escopo='prudencial',
    cadastro=['SEGMENTO', 'UF', 'TCB']
)
# Resultado inclui colunas SEGMENTO, UF e TCB
```

Colunas cadastrais disponiveis: `SEGMENTO`, `COD_CONGL_PRUD`, `COD_CONGL_FIN`, `SITUACAO`, `ATIVIDADE`, `TCB`, `TD`, `TC`, `UF`, `MUNICIPIO`, `SR`, `DATA_INICIO_ATIVIDADE`.

Para dados mensais (COSIF), o alinhamento temporal e automatico: cada mes recebe os atributos cadastrais do trimestre mais recente.

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
# Coletar dados de ambos escopos de uma vez
df = bcb.cosif.read(
    instituicao='60872504',
    conta=['TOTAL GERAL DO ATIVO'],
    start='2024-12'
)
# escopo=None retorna ambos com coluna ESCOPO

# Comparar
print(df.pivot_table(values='VALOR', index='INSTITUICAO', columns='ESCOPO'))
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
df_sorted.plot(x='DATA', y='VALOR', kind='line', title='Patrimonio Liquido 2024')
```

### Top Instituicoes por Ativo (SQL)

```python
from ifdata_bcb.infra import QueryEngine

qe = QueryEngine()

# Usando SQL para ranking
df = qe.sql("""
    SELECT
        CNPJ_8,
        NOME_INSTITUICAO as INSTITUICAO,
        SALDO / 1e12 as ATIVO_TRILHOES
    FROM '{cache}/cosif/prudencial/*.parquet'
    WHERE DATA_BASE = 202412
      AND NOME_CONTA = 'TOTAL GERAL DO ATIVO'
      AND DOCUMENTO = 4060
    ORDER BY SALDO DESC
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

### Eras de Formato CSV

O BCB mudou o formato dos CSVs COSIF ao longo do tempo. O collector detecta e normaliza automaticamente:

| Era | Periodo | Header | Colunas |
|-----|---------|--------|---------|
| 1 | 199501-201009 | `DATA;CNPJ;NOME INSTITUICAO;...` | 8 |
| 2 | 201010-202412 | `#DATA_BASE;DOCUMENTO;CNPJ;...` | 11 |
| 3 | 202501+ | `#DATA_BASE;DOCUMENTO;CNPJ;...` | 11 (COSIF 1.5) |

Todos os CSVs tem 3 linhas de metadata antes do header. Separador: `;`. Encoding: `cp1252` (individual) ou `latin-1` (prudencial).

A Era 3 introduziu o novo plano contabil COSIF 1.5 (Resolucao CMN 4.966) com codigos de conta renumerados e incompativeis com as eras anteriores.

### Normalizacao

O collector normaliza todas as eras para um schema uniforme antes de salvar em Parquet:
- Era 1: Colunas renomeadas (`DATA` -> `DATA_BASE`, `NOME INSTITUICAO` -> `NOME_INSTITUICAO`), `CONTA` com leading zeros removidos via `CAST(BIGINT)`
- Todas as eras: `NOME_CONTA` normalizado para UPPER (Era 3 usa Title Case)

Colunas armazenadas em Parquet (uniformes para todas as eras):

| Coluna Original | Coluna Mapeada |
|-----------------|----------------|
| DATA_BASE | DATA |
| CNPJ_8 | CNPJ_8 |
| NOME_INSTITUICAO | INSTITUICAO |
| CONTA | COD_CONTA |
| NOME_CONTA | CONTA |
| DOCUMENTO | DOCUMENTO |
| SALDO | VALOR |

### Warning de Compatibilidade

Ao consultar periodos que cruzam a fronteira Era 2/Era 3 (202501), um `IncompatibleEraWarning` e emitido automaticamente:

```python
# Emite IncompatibleEraWarning: codigos de conta foram renumerados
df = bcb.cosif.read(instituicao='60872504', start='2024-12', end='2025-01')
```

O warning nao bloqueia a query -- apenas alerta que os codigos de conta podem ser incompativeis entre os periodos.

## Tratamento de Erros

```python
from ifdata_bcb.domain.exceptions import (
    MissingRequiredParameterError,
    InvalidDateRangeError,
    InvalidScopeError,
)

# Erro: parametro obrigatorio ausente
try:
    df = bcb.cosif.read(instituicao='60872504')  # Falta start!
except MissingRequiredParameterError as e:
    print(f"Erro: {e}")

# Erro: range de datas invalido
try:
    df = bcb.cosif.read(
        instituicao='60872504',
        start='2024-12',
        end='2024-01'  # start > end!
    )
except InvalidDateRangeError as e:
    print(f"Erro: {e}")

# Erro: escopo invalido
try:
    df = bcb.cosif.read(
        instituicao='60872504',
        start='2024-12',
        escopo='invalido'
    )
except InvalidScopeError as e:
    print(f"Erro: {e}")

# Sem erro: retorna DataFrame vazio se nao encontrar dados
df = bcb.cosif.read(instituicao='99999999', start='2024-12')
if df.empty:
    print("Instituicao nao encontrada nos dados COSIF")
```
