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
df = bcb.ifdata.read('2024-12', instituicao='60872504', escopo='prudencial')

# Buscar em todos os escopos (escopo=None)
df = bcb.ifdata.read('2024-12', instituicao='60872504')
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
    start: str,                             # Data inicial ou unica. OBRIGATORIO (posicional)
    end: str | None = None,                 # Data final para range (posicional)
    *,                                      # --- keyword-only a partir daqui ---
    instituicao: str | list[str] | None = None,  # CNPJ(s) de 8 digitos. Se None, retorna todas (bulk)
    escopo: str | None = None,              # 'individual', 'prudencial', 'financeiro', ou None
    conta: str | list[str] | None = None,   # Nome ou codigo da conta (case-insensitive)
    relatorio: str | None = None,           # Nome do relatorio para filtrar
    grupo: str | None = None,               # Grupo de conta para filtrar
    columns: list[str] | None = None,       # Colunas especificas
    cadastro: list[str] | None = None       # Colunas cadastrais para enriquecer o resultado
) -> pd.DataFrame
```

**Parametro Obrigatorio**: `start`.

**API de Datas**:
- `start` sozinho: filtra data unica (ex: `start='2024-12'`)
- `start` + `end`: gera range trimestral automatico

**Bulk read**: Quando `instituicao=None` (padrao), retorna dados de todas as instituicoes do periodo, sem necessidade de resolver entidade. Util para rankings e analises agregadas.

**Raises**:
- `MissingRequiredParameterError`: Se `start` nao fornecido.
- `InvalidDateRangeError`: Se `start > end`.

**Exemplos**:

```python
# Data unica em um escopo especifico
df = bcb.ifdata.read('2024-12', instituicao='60872504', escopo='prudencial')

# Buscar em TODOS os escopos
df = bcb.ifdata.read('2024-12', instituicao='60872504')

# Bulk read: todas as instituicoes
df = bcb.ifdata.read('2024-12', escopo='prudencial')

# Conta especifica (filtro case-insensitive)
df = bcb.ifdata.read('2024-12', instituicao='60872504', conta='lucro liquido')

# Multiplas contas com range de datas
df = bcb.ifdata.read(
    '2024-03',
    '2024-12',
    instituicao='60872504',
    conta=['Lucro Liquido', 'Ativo Total']
)

# Filtrar por relatorio
df = bcb.ifdata.read(
    '2024-12',
    instituicao='60872504',
    relatorio='Resumo'
)

# Filtrar por grupo de conta
df = bcb.ifdata.read(
    '2024-12',
    instituicao='60872504',
    grupo='Resumo'
)
```

### list_contas()

Lista contas disponiveis nos dados.

```python
bcb.ifdata.list_contas(
    termo: str | None = None,      # Filtro por nome (case-insensitive)
    escopo: str | None = None,     # 'individual', 'prudencial', 'financeiro'
    relatorio: str | None = None,  # Filtro por relatorio (case/accent-insensitive)
    start: str | None = None,      # Periodo inicial (filtra contas que existem no periodo)
    end: str | None = None,        # Periodo final. Se None com start, filtra data unica
    limit: int = 100               # Numero maximo de contas. Deve ser > 0
) -> pd.DataFrame
```

**Raises**: `ValueError` se `limit <= 0`.

**Retorna**: DataFrame com colunas `COD_CONTA`, `CONTA`, `RELATORIO` e `GRUPO`, ordenado por RELATORIO, GRUPO, CONTA.

**Exemplos**:

```python
# Listar todas as contas
contas = bcb.ifdata.list_contas()

# Buscar contas que contenham "lucro"
contas = bcb.ifdata.list_contas(termo='lucro')

# Listar contas do escopo individual
contas = bcb.ifdata.list_contas(escopo='individual', limit=50)

# Filtrar contas por relatorio
contas = bcb.ifdata.list_contas(relatorio='Resumo')
```

### list()

Lista valores distintos para colunas solicitadas (SELECT DISTINCT via DuckDB).

```python
bcb.ifdata.list(
    columns: list[str],            # Colunas a listar: DATA, ESCOPO, RELATORIO, GRUPO
    *,
    start: str | None = None,      # Periodo inicial
    end: str | None = None,        # Periodo final
    escopo: str | None = None,     # Filtro por escopo
    relatorio: str | None = None,  # Filtro por relatorio (case/accent insensitive)
    grupo: str | None = None,      # Filtro por grupo (case/accent insensitive)
    limit: int = 100               # Maximo de resultados
) -> pd.DataFrame
```

**Colunas bloqueadas** (emitem warning e retornam DataFrame vazio):
- `CONTA`, `COD_CONTA`: use `list_contas()` para buscar contas
- `COD_INST`: use `cadastro.search(fonte='ifdata')` para listar instituicoes
- `VALOR`: metrica continua, nao listavel

**Raises**: `InvalidColumnError` se coluna invalida. `TruncatedResultWarning` quando `len(resultado) == limit`.

**Exemplos**:

```python
# Listar relatorios disponiveis
bcb.ifdata.list(["RELATORIO"])

# Listar combinacoes relatorio + escopo
bcb.ifdata.list(["RELATORIO", "ESCOPO"])

# Listar grupos de um relatorio especifico
bcb.ifdata.list(["GRUPO"], relatorio="Ativo")

# Listar periodos como datetime64
bcb.ifdata.list(["DATA"])
```

### mapeamento()

Tabela de mapeamento COD_INST <-> CNPJ_8 por escopo.

```python
bcb.ifdata.mapeamento(
    start: str | None = None,      # Data inicial ou unica
    end: str | None = None         # Data final para range
) -> pd.DataFrame
```

**Retorna**: DataFrame com colunas:
- `COD_INST`: Codigo de reporte no IFDATA
- `TIPO_INST`: Codigo do tipo de instituicao (1, 2, 3)
- `ESCOPO`: "individual", "prudencial" ou "financeiro"
- `REPORT_KEY_TYPE`: "cnpj" ou nome do escopo
- `CNPJ_8`: CNPJ da entidade associada
- `INSTITUICAO`: Nome canonico

**Exemplos**:

```python
# Ver mapeamento completo
reporters = bcb.ifdata.mapeamento(start='2024-12')
print(reporters[reporters['CNPJ_8'] == '60872504'])

# Descobrir COD_INST de um banco por escopo
df = bcb.ifdata.mapeamento(start='2024-12')
df[df['CNPJ_8'] == '60746948']  # Bradesco: individual=60746948, prudencial=C0080075

# Listar membros de um conglomerado
df[df['COD_INST'] == 'C0080075']
```

### list_periodos()

Lista periodos disponiveis (herdado de BaseExplorer).

```python
periodos = bcb.ifdata.list_periodos()  # Retorna [202403, 202406, ...]
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
| `CNPJ_8` | str | CNPJ de 8 digitos (resolvido automaticamente para conglomerados em bulk reads) |
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
    '2024-03',
    '2024-12',
    instituicao=['60872504', '60746948'],
    escopo='prudencial',
    cadastro=['TCB', 'TC', 'SEGMENTO']
)
# Resultado inclui colunas TCB, TC e SEGMENTO
```

Colunas cadastrais disponiveis: `SEGMENTO`, `COD_CONGL_PRUD`, `COD_CONGL_FIN`, `CNPJ_LIDER_8`, `SITUACAO`, `ATIVIDADE`, `TCB`, `TD`, `TC`, `UF`, `MUNICIPIO`, `SR`, `DATA_INICIO_ATIVIDADE`, `NOME_CONGL_PRUD`.

## Exemplos Avancados

### Filtrar por Escopo

```python
# Apenas escopo prudencial (conglomerados)
df_prud = bcb.ifdata.read('2024-12', instituicao='60872504', escopo='prudencial')
print(f"Escopo: {df_prud['ESCOPO'].iloc[0]}")

# Todos os escopos disponiveis
df_todos = bcb.ifdata.read('2024-12', instituicao='60872504')
print(f"Escopos: {df_todos['ESCOPO'].unique()}")
```

### Analisar Grupos de Contas

```python
# Listar grupos disponiveis
df = bcb.ifdata.read('2024-12', instituicao='60872504', escopo='prudencial')
grupos = df['GRUPO'].unique()
print(f"Grupos: {grupos}")
```

### Serie Temporal de Lucro

```python
# Evolucao trimestral do Lucro Liquido
df = bcb.ifdata.read(
    '2023-01',
    '2024-12',
    instituicao='60872504',
    conta=['Lucro Liquido'],
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

### Warnings de Compatibilidade entre Eras

A partir de 202503 (marco/2025), o BCB mudou a estrutura dos dados IFDATA. A biblioteca detecta automaticamente cenarios problematicos e emite warnings especificos:

**IncompatibleEraWarning**: Codigos de conta renumerados em relatorios contabeis (Resumo, Ativo, Passivo, DRE):

```python
# Emite IncompatibleEraWarning: codigos de conta foram renumerados
df = bcb.ifdata.read('2024-12', '2025-03', instituicao='60872504', relatorio='Resumo')
```

**ScopeMigrationWarning**: Relatorios de credito migraram de escopo `financeiro` para `prudencial` a partir de 202503:

```python
# Emite ScopeMigrationWarning: periodos < 202503 nao tem dados no escopo prudencial
df = bcb.ifdata.read('2024-12', '2025-03', instituicao='60872504', escopo='prudencial',
                     relatorio='Carteira de credito ativa')
```

**DroppedReportWarning**: Relatorio descontinuado (ex: "por nivel de risco da operacao" apos 202412):

```python
# Emite DroppedReportWarning: relatorio descontinuado
df = bcb.ifdata.read('2025-03', relatorio='Carteira de credito ativa - por nivel de risco da operacao')
```

Relatorios com contas estaveis entre eras (credit reports, "Informacoes de Capital") **nao** emitem `IncompatibleEraWarning`.

Nenhum warning bloqueia a query -- apenas alertam sobre potenciais incompatibilidades nos resultados.

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

# Erro: parametro obrigatorio ausente
try:
    df = bcb.ifdata.read(start=None)  # Falta start!
except MissingRequiredParameterError as e:
    print(f"Erro: {e}")

# Erro: range de datas invalido
try:
    df = bcb.ifdata.read(
        '2024-12',
        '2024-01',  # start > end!
        instituicao='60872504',
    )
except InvalidDateRangeError as e:
    print(f"Erro: {e}")

# Sem erro: retorna DataFrame vazio se nao encontrar dados
df = bcb.ifdata.read('2024-12', instituicao='99999999')
if df.empty:
    print("Instituicao nao encontrada nos dados IFDATA")
```
