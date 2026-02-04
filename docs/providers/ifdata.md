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
    conta: str | list[str] | None = None,   # Nome(s) da(s) conta(s) (case-insensitive)
    columns: list[str] | None = None,       # Colunas especificas
    escopo: str | None = None,              # 'individual', 'prudencial', 'financeiro', ou None
    relatorio: str | None = None            # Nome do relatorio para filtrar
) -> pd.DataFrame
```

**Parametros Obrigatorios**: `instituicao` e `start`.

**API de Datas**:
- `start` sozinho: filtra data unica (ex: `start='2024-12'`)
- `start` + `end`: gera range trimestral automatico

**Raises**:
- `MissingRequiredParameterError`: Se `instituicao` ou `start` nao fornecidos.
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
    limit: int = 100               # Numero maximo de contas
) -> pd.DataFrame
```

**Retorna**: DataFrame com colunas `COD_CONTA` e `CONTA`.

**Exemplos**:

```python
# Listar todas as contas
contas = bcb.ifdata.list_accounts()

# Buscar contas que contenham "lucro"
contas = bcb.ifdata.list_accounts(termo='lucro')

# Listar contas do escopo individual
contas = bcb.ifdata.list_accounts(escopo='individual', limit=50)
```

### list_institutions()

Lista instituicoes disponiveis nos dados.

```python
bcb.ifdata.list_institutions(
    start: str | None = None,      # Data inicial ou unica
    end: str | None = None         # Data final para range
) -> pd.DataFrame
```

**Retorna**: DataFrame com colunas `COD_INST`, `TIPO_INST` e `INSTITUICAO` (para tipo_inst=3).

**Exemplos**:

```python
# Listar instituicoes de dezembro/2024
inst = bcb.ifdata.list_institutions(start='2024-12')

# Listar de um range de periodos
inst = bcb.ifdata.list_institutions(start='2024-01', end='2024-12')

# Contar por tipo
print(inst.groupby('TIPO_INST').size())
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
| `INSTITUICAO` | str | Nome da instituicao |
| `ESCOPO` | str | Escopo dos dados (individual, prudencial, financeiro) |
| `COD_INST` | str | Codigo da instituicao no BCB |
| `CONTA` | str | Nome/descricao da conta |
| `VALOR` | float | Valor em reais |
| `RELATORIO` | str | Nome do relatorio de origem |
| `GRUPO` | str | Grupo da conta |

### Sobre COD_INST vs CNPJ_8

- `COD_INST`: Codigo interno do BCB para a instituicao/conglomerado
- `CNPJ_8`: CNPJ de 8 digitos que voce passou na consulta

Para escopo `individual`, `COD_INST` e igual ao `CNPJ_8`.
Para escopos `prudencial` e `financeiro`, `COD_INST` e o codigo do conglomerado.

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
# Usando SQL para ranking
df = bcb.sql("""
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
| NomeColuna | CONTA |
| Saldo | VALOR |
| NomeRelatorio | RELATORIO |
| Grupo | GRUPO |

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
from ifdata_bcb import (
    MissingRequiredParameterError,
    InvalidDateRangeError,
)

# Erro: parametro obrigatorio ausente
try:
    df = bcb.ifdata.read(instituicao='60872504')  # Falta start!
except MissingRequiredParameterError as e:
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
