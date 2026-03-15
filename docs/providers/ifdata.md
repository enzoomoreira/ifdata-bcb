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
- **Disponibilidade**: Geralmente 1-2 meses de atraso apos o fechamento

## Escopos IFDATA

O IFDATA suporta tres escopos que determinam a visao dos dados:

| Escopo | TIPO_INST | Descricao |
|--------|-----------|-----------|
| `individual` | 3 | Dados da instituicao especifica |
| `prudencial` | 1 | Dados do conglomerado prudencial |
| `financeiro` | 2 | Dados do conglomerado financeiro |

```python
# Filtrar por escopo
df = bcb.ifdata.read(instituicao='60872504', start='2024-12', escopo='prudencial')

# Buscar em todos os escopos (escopo=None)
df = bcb.ifdata.read(instituicao='60872504', start='2024-12')
# Resultado inclui coluna ESCOPO
```

## API Reference

### collect()

Coleta dados IFDATA Valores do BCB.

```python
bcb.ifdata.collect(
    start: str,           # Data inicial (YYYY-MM)
    end: str,             # Data final (YYYY-MM)
    force: bool = False,  # Se True, recoleta dados existentes
    verbose: bool = True  # Se True, exibe progresso
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
    instituicao: str | list[str],           # CNPJ(s) de 8 digitos. OBRIGATORIO
    start: str,                             # Data inicial ou unica. OBRIGATORIO
    end: str | None = None,                 # Data final para range
    conta: str | list[str] | None = None,   # Nome ou codigo da conta (case-insensitive)
    columns: list[str] | None = None,       # Colunas especificas
    escopo: str | None = None,              # 'individual', 'prudencial', 'financeiro', ou None
    relatorio: str | None = None,           # Nome do relatorio para filtrar
    cadastro: list[str] | None = None       # Colunas cadastrais para enriquecer o resultado
) -> pd.DataFrame
```

**Parametros Obrigatorios**: `instituicao` e `start`.

**API de Datas**:
- `start` sozinho: filtra data unica (ex: `start='2024-12'`)
- `start` + `end`: gera range trimestral automatico

**Raises**:
- `TypeError`: Se `instituicao` ou `start` nao fornecidos (argumentos posicionais obrigatorios).
- `InvalidDateRangeError`: Se `start > end`.

**Exemplos**:

```python
# Data unica em um escopo especifico
df = bcb.ifdata.read(instituicao='60872504', start='2024-12', escopo='prudencial')

# Buscar em TODOS os escopos
df = bcb.ifdata.read(instituicao='60872504', start='2024-12')

# Conta especifica (filtro case-insensitive)
df = bcb.ifdata.read(instituicao='60872504', start='2024-12', conta='lucro liquido')

# Multiplas contas com range de datas
df = bcb.ifdata.read(
    instituicao='60872504',
    start='2024-03',
    end='2024-12',
    conta=['Lucro Liquido', 'Ativo Total']
)

# Filtrar por relatorio
df = bcb.ifdata.read(
    instituicao='60872504',
    start='2024-12',
    relatorio='Resumo'
)
```

### list_accounts()

Lista contas disponiveis nos dados.

```python
bcb.ifdata.list_accounts(
    termo: str | None = None,      # Filtro por nome (case-insensitive)
    escopo: str | None = None,     # 'individual', 'prudencial', 'financeiro'
    relatorio: str | None = None,  # Filtro por relatorio (case/accent-insensitive)
    limit: int = 100               # Numero maximo de contas
) -> pd.DataFrame
```

**Retorna**: DataFrame com colunas `COD_CONTA`, `CONTA`, `RELATORIO` e `GRUPO`, ordenado por RELATORIO, GRUPO, CONTA.

**Exemplos**:

```python
# Listar todas as contas
contas = bcb.ifdata.list_accounts()

# Buscar contas que contenham "lucro"
contas = bcb.ifdata.list_accounts(termo='lucro')

# Listar contas do escopo individual
contas = bcb.ifdata.list_accounts(escopo='individual', limit=50)

# Filtrar contas por relatorio
contas = bcb.ifdata.list_accounts(relatorio='Resumo')
```

### list_institutions()

Lista entidades analiticas com disponibilidade por escopo.

```python
bcb.ifdata.list_institutions(
    start: str | None = None,      # Data inicial ou unica
    end: str | None = None         # Data final para range
) -> pd.DataFrame
```

**Retorna**: DataFrame com colunas:
- `CNPJ_8`: CNPJ de 8 digitos da entidade
- `INSTITUICAO`: Nome canônico do cadastro
- `TEM_INDIVIDUAL`: bool - se ha dados no escopo individual
- `TEM_PRUDENCIAL`: bool - se ha dados no escopo prudencial
- `TEM_FINANCEIRO`: bool - se ha dados no escopo financeiro
- `COD_INST_INDIVIDUAL`: Codigo(s) de reporte individual
- `COD_INST_PRUDENCIAL`: Codigo(s) de reporte prudencial
- `COD_INST_FINANCEIRO`: Codigo(s) de reporte financeiro

**Exemplos**:

```python
# Listar entidades de dezembro/2024
inst = bcb.ifdata.list_institutions(start='2024-12')

# Filtrar entidades com dados prudenciais
prud = inst[inst['TEM_PRUDENCIAL']]

# Ver codigos de reporte de uma entidade
row = inst[inst['CNPJ_8'] == '60872504'].iloc[0]
print(f"Individual: {row['COD_INST_INDIVIDUAL']}")
print(f"Prudencial: {row['COD_INST_PRUDENCIAL']}")
```

### list_reporters()

Lista chaves operacionais de reporte do IFDATA por entidade e escopo.

```python
bcb.ifdata.list_reporters(
    start: str | None = None,      # Data inicial ou unica
    end: str | None = None         # Data final para range
) -> pd.DataFrame
```

**Retorna**: DataFrame com colunas:
- `COD_INST`: Codigo de reporte no IFDATA
- `TIPO_INST`: Codigo do tipo de instituicao (1, 2, 3)
- `ESCOPO`: "individual", "prudencial" ou "financeiro"
- `REPORT_KEY_TYPE`: "cnpj" ou nome do escopo (indica se COD_INST e CNPJ direto ou codigo de conglomerado)
- `CNPJ_8`: CNPJ da entidade associada
- `INSTITUICAO`: Nome canônico

**Exemplo**:

```python
# Ver mapeamento completo de reporters
reporters = bcb.ifdata.list_reporters(start='2024-12')
print(reporters[reporters['CNPJ_8'] == '60872504'])
```

### list_reports()

Lista relatorios disponiveis nos dados.

```python
bcb.ifdata.list_reports(
    start: str | None = None,      # Data inicial ou unica
    end: str | None = None         # Data final para range
) -> list[str]
```

**Retorna**: Lista de nomes de relatorios.

**Exemplos**:

```python
# Listar relatorios disponiveis
relatorios = bcb.ifdata.list_reports()
# ['Ativo', 'Passivo', 'DRE', 'Resumo', ...]

# Relatorios de um periodo especifico
relatorios = bcb.ifdata.list_reports(start='2024-12')
```

### list_periods()

Lista periodos disponiveis (herdado de BaseExplorer).

```python
periodos = bcb.ifdata.list_periods()  # Retorna [202403, 202406, ...]
```

### describe()

Retorna informacoes sobre os dados disponiveis (herdado de BaseExplorer).

```python
info = bcb.ifdata.describe()
```

## Colunas Disponiveis

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| `DATA` | datetime | Periodo de referencia (trimestral) |
| `CNPJ_8` | str | CNPJ de 8 digitos da consulta original |
| `INSTITUICAO` | str | Nome da instituicao (canônico do cadastro) |
| `ESCOPO` | str | Escopo dos dados (individual, prudencial, financeiro) |
| `COD_INST` | str | Codigo da instituicao no BCB |
| `COD_CONTA` | str | Codigo numerico da conta |
| `CONTA` | str | Nome/descricao da conta |
| `VALOR` | float | Valor em reais |
| `RELATORIO` | str | Nome do relatorio de origem |
| `GRUPO` | str | Grupo da conta |

### Sobre COD_INST vs CNPJ_8

- `COD_INST`: Codigo interno do BCB para a instituicao/conglomerado
- `CNPJ_8`: CNPJ de 8 digitos que voce passou na consulta

Para escopo `individual`, `COD_INST` e igual ao `CNPJ_8`.
Para escopos `prudencial` e `financeiro`, `COD_INST` pode ser o codigo do conglomerado
ou o proprio CNPJ, dependendo de como a entidade reporta ao BCB.

### Sobre RELATORIO

Indica a origem dos dados:
- **Resumo**: Indicadores principais
- **Ativo**: Composicao do ativo
- **Passivo**: Composicao do passivo
- **DRE**: Demonstracao do Resultado

### Sobre GRUPO

Agrupamento logico das contas para navegacao hierarquica.

### Enriquecimento Cadastral

O parametro `cadastro` permite adicionar colunas do cadastro diretamente no resultado, sem precisar fazer merge manual:

```python
# Adicionar tipo de banco e segmento
df = bcb.ifdata.read(
    instituicao=['60872504', '60746948'],
    start='2024-03',
    end='2024-12',
    escopo='prudencial',
    cadastro=['TCB', 'TC', 'SEGMENTO']
)
# Resultado inclui colunas TCB, TC e SEGMENTO
```

Colunas cadastrais disponiveis: `SEGMENTO`, `COD_CONGL_PRUD`, `COD_CONGL_FIN`, `SITUACAO`, `ATIVIDADE`, `TCB`, `TD`, `TC`, `UF`, `MUNICIPIO`, `SR`, `DATA_INICIO_ATIVIDADE`.

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

### Ranking por Ativo Total (SQL)

```python
from ifdata_bcb.infra import QueryEngine

qe = QueryEngine()

# Usando SQL para ranking
df = qe.sql("""
    SELECT
        CodInst as COD_INST,
        NomeColuna as CONTA,
        Saldo / 1e9 as VALOR_BILHOES
    FROM '{cache}/ifdata/valores/*.parquet'
    WHERE AnoMes = 202412
      AND NomeColuna = 'Ativo Total'
      AND TipoInstituicao = 1
    ORDER BY Saldo DESC
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

Mapeamento para colunas de apresentacao:

| Coluna Original | Coluna Mapeada |
|-----------------|----------------|
| AnoMes | DATA |
| CodInst | COD_INST |
| Conta | COD_CONTA |
| NomeColuna | CONTA |
| Saldo | VALOR |
| NomeRelatorio | RELATORIO |
| Grupo | GRUPO |

### Warning de Compatibilidade

A partir de 202503 (marco/2025), o BCB renumerou os codigos de conta no IFDATA. Ao consultar periodos que cruzam essa fronteira, um `IncompatibleEraWarning` e emitido automaticamente:

```python
# Emite IncompatibleEraWarning: codigos de conta foram renumerados
df = bcb.ifdata.read(instituicao='60872504', start='2024-12', end='2025-03')
```

O warning nao bloqueia a query -- apenas alerta que os codigos de conta podem ser incompativeis entre os periodos.

## Diferenca Entre COSIF e IFDATA

| Aspecto | COSIF | IFDATA |
|---------|-------|--------|
| Periodicidade | Mensal | Trimestral |
| Plano de Contas | COSIF completo | Resumido |
| Escopos | Individual, Prudencial | Individual, Prudencial, Financeiro |
| Formato Original | CSV com metadata | CSV limpo via API |
| Detalhamento | Maior (milhares de contas) | Menor (centenas de contas) |

## Tratamento de Erros

```python
from ifdata_bcb.domain.exceptions import (
    MissingRequiredParameterError,
    InvalidDateRangeError,
)

# Erro: parametro obrigatorio ausente (start e argumento posicional)
try:
    df = bcb.ifdata.read(instituicao='60872504')  # Falta start!
except TypeError as e:
    print(f"Erro: {e}")

# Erro: range de datas invalido
try:
    df = bcb.ifdata.read(
        instituicao='60872504',
        start='2024-12',
        end='2024-01'  # start > end!
    )
except InvalidDateRangeError as e:
    print(f"Erro: {e}")

# Sem erro: retorna DataFrame vazio se nao encontrar dados
df = bcb.ifdata.read(instituicao='99999999', start='2024-12')
if df.empty:
    print("Instituicao nao encontrada nos dados IFDATA")
```
