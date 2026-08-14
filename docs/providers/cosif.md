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
bcb.cosif.collect("2024-01", "2024-12", escopo="individual")

# Consultar dados individuais
df = bcb.cosif.read("2024-12", instituicao="60872504", escopo="individual")
```

**Quando usar**:
- Analise de instituicoes especificas
- Comparacao entre entidades do mesmo grupo
- Estudos de concentracao por instituicao

### Prudencial

Dados consolidados do conglomerado prudencial (grupo de empresas sob mesma gestao).

```python
# Coletar apenas escopo prudencial
bcb.cosif.collect("2024-01", "2024-12", escopo="prudencial")

# Consultar dados prudenciais
df = bcb.cosif.read("2024-12", instituicao="60872504", escopo="prudencial")
```

**Quando usar**:
- Analise de grupos financeiros
- Comparacao entre conglomerados
- Visao consolidada de ativos e passivos

### Buscando em Todos os Escopos

Quando `escopo=None` (padrao), a busca e feita em ambos os escopos e uma coluna `escopo` e incluida no resultado:

```python
# Buscar em TODOS os escopos
df = bcb.cosif.read("2024-12", instituicao="60872504")
# Resultado inclui coluna escopo com valores 'individual' ou 'prudencial'
```

## API Reference

### collect()

Coleta dados COSIF do BCB.

```python
bcb.cosif.collect(
    start: DateScalar,                 # Data inicial
    end: DateScalar | None = None,     # Data final. None = apenas start
    escopo: str | None = None,         # 'individual', 'prudencial', ou None (ambos)
    force: bool = False,               # Se True, recoleta dados existentes
    verbose: bool = True               # Se True, exibe progresso
)
```

**Exemplos**:

```python
# Coletar ambos os escopos (padrao)
bcb.cosif.collect("2024-01", "2024-12")

# Periodo unico: end e opcional, como em read()
bcb.cosif.collect("2024-12")

# Coletar apenas prudencial
bcb.cosif.collect("2024-01", "2024-12", escopo="prudencial")

# Forcar recoleta
bcb.cosif.collect("2024-12", force=True)
```

### fetch()

Baixa dados do BCB e devolve o DataFrame no formato de `read()`, **sem tocar o cache local**. Os arquivos baixados vivem num diretorio temporario descartado ao final; nada persiste.

```python
bcb.cosif.fetch(
    start: DateScalar,                             # Data inicial ou unica. OBRIGATORIO (posicional)
    end: DateScalar | None = None,                 # Data final para range (posicional)
    *,                                      # --- keyword-only a partir daqui ---
    instituicao: str | list[str] | None = None,
    escopo: str | None = None,
    conta: str | list[str] | None = None,
    documento: str | list[str] | None = None,
    columns: list[str] | None = None,
    verbose: bool = True
) -> pd.DataFrame
```

Mesmos filtros de `read()`, exceto `cadastro=` -- o enriquecimento exige o cadastro no cache local. Nomes canonicos vem do cadastro do cache local, quando coletado.

```python
# Consulta pontual sem popular o cache
df = bcb.cosif.fetch("2024-12", instituicao="60872504", escopo="prudencial")
```

### read()

Le dados COSIF com filtros.

```python
bcb.cosif.read(
    start: DateScalar,                             # Data inicial ou unica. OBRIGATORIO (posicional)
    end: DateScalar | None = None,                 # Data final para range (posicional)
    *,                                      # --- keyword-only a partir daqui ---
    instituicao: str | list[str] | None = None,  # CNPJ(s) de 8 digitos. Se None, retorna todas (bulk)
    escopo: str | None = None,              # 'individual', 'prudencial', ou None (TODOS)
    conta: str | list[str] | None = None,   # Nome ou codigo da conta (case-insensitive)
    documento: str | list[str] | None = None,  # Codigo numerico do documento (ex: 4010, 4016, 4060)
    columns: list[str] | None = None,       # Colunas especificas
    cadastro: list[str] | None = None       # Colunas cadastrais para enriquecer o resultado
) -> pd.DataFrame
```

**Parametro Obrigatorio**: `start`.

**API de Datas**:
- `start` sozinho: filtra data unica (ex: `start='2024-12'`)
- `start` + `end`: gera range mensal automatico

**Retorno**: DataFrame indexado por um `DatetimeIndex` nomeado `date` -- a data do periodo sai das colunas e vira o indice. As demais colunas usam nomes lowercase (`cnpj_8`, `conta`, `valor`, ...).

**Bulk read**: Quando `instituicao=None` (padrao), retorna dados de todas as instituicoes do periodo, sem necessidade de resolver entidade. Util para rankings e analises agregadas.

**Raises**:
- `MissingRequiredParameterError`: Se `start` nao fornecido.
- `InvalidDateRangeError`: Se `start > end`.
- `InvalidScopeError`: Se `documento` nao for numerico. A mensagem aponta para `cosif.list_values(['documento'])` para ver os disponiveis.

**Exemplos**:

```python
# Data unica em um escopo especifico
df = bcb.cosif.read("2024-12", instituicao="60872504", escopo="prudencial")

# Buscar em TODOS os escopos
df = bcb.cosif.read("2024-12", instituicao="60872504")

# Bulk read: todas as instituicoes
df = bcb.cosif.read("2024-12", escopo="prudencial")

# Conta especifica (filtro case-insensitive)
df = bcb.cosif.read(
    "2024-12", instituicao="60872504", conta="total geral do ativo", escopo="prudencial"
)

# Multiplas contas e range de datas
df = bcb.cosif.read(
    "2024-01",
    "2024-12",
    instituicao="60872504",
    conta=["TOTAL GERAL DO ATIVO", "PATRIMONIO LIQUIDO"],
    escopo="prudencial",
)

# Filtrar por codigo de conta (numerico)
df = bcb.cosif.read(
    "2024-12", instituicao="60872504", conta="10100", escopo="prudencial"
)

# Filtrar por tipo de documento (deve ser numerico)
df = bcb.cosif.read(
    "2024-12", instituicao="60872504", escopo="prudencial", documento="4060"
)

# Apenas colunas especificas (a data continua vindo como indice)
df = bcb.cosif.read(
    "2024-12",
    instituicao="60872504",
    escopo="prudencial",
    columns=["cnpj_8", "conta", "valor"],
)
```

### list_contas()

Lista contas disponiveis nos dados.

```python
bcb.cosif.list_contas(
    termo: str | None = None,      # Filtro por nome (case-insensitive)
    *,                             # --- keyword-only a partir daqui ---
    escopo: str | None = None,     # 'individual', 'prudencial', ou None (ambos)
    start: DateScalar | None = None,      # Periodo inicial (filtra contas que existem no periodo)
    end: DateScalar | None = None,        # Periodo final. Se None com start, filtra data unica
    limit: int = 100               # Numero maximo de contas. Deve ser > 0
) -> pd.DataFrame
```

**Raises**: `ValueError` se `limit <= 0`.

**Retorna**: DataFrame flat com colunas `cod_conta`, `conta` e `escopos` (quando escopo=None, string com escopos separados por virgula).

**Exemplos**:

```python
# Listar todas as contas (ambos escopos)
contas = bcb.cosif.list_contas()

# Buscar contas que contenham "deposito"
contas = bcb.cosif.list_contas(termo="deposito")

# Listar contas do prudencial apenas
contas = bcb.cosif.list_contas(escopo="prudencial", limit=50)
```

### list_values()

Lista valores distintos para colunas solicitadas (SELECT DISTINCT via DuckDB). Retorna DataFrame flat.

```python
bcb.cosif.list_values(
    columns: list[str],            # Colunas a listar: data, escopo, documento
    *,
    start: DateScalar | None = None,      # Periodo inicial
    end: DateScalar | None = None,        # Periodo final
    escopo: str | None = None,     # Filtro por escopo
    documento: str | list[str] | None = None,  # Filtro por documento
    limit: int = 100               # Maximo de resultados
) -> pd.DataFrame
```

**Colunas bloqueadas** (emitem warning e retornam DataFrame vazio):
- `conta`, `cod_conta`: use `list_contas()` para buscar contas
- `cnpj_8`, `instituicao`: use `cadastro.search()` para buscar instituicoes
- `valor`, `saldo`: metrica continua, nao listavel

**Raises**: `InvalidColumnError` se coluna invalida. `TruncatedResultWarning` quando `len(resultado) == limit`.

**Exemplos**:

```python
# Listar periodos disponiveis como datetime64
bcb.cosif.list_values(["data"])

# Listar documentos por escopo
bcb.cosif.list_values(["documento", "escopo"])

# Listar periodos de um escopo especifico
bcb.cosif.list_values(["data"], escopo="prudencial")
```

### list_periodos()

Lista periodos disponiveis (herdado de BaseExplorer). Aceita `escopo` para restringir a um escopo.

```python
periodos = bcb.cosif.list_periodos()                     # Uniao dos escopos: [202401, 202402, ...]
periodos = bcb.cosif.list_periodos(escopo="prudencial")  # Apenas periodos do prudencial
```

**Raises**: `InvalidScopeError` se o escopo nao for valido para o explorer.

### describe()

Retorna o que o explorer aceita e o que ha coletado (herdado de BaseExplorer).
As chaves de capacidade descrevem a superficie de chamada, o suficiente para
montar um `read()` sem consultar esta pagina.

```python
info = bcb.cosif.describe()
# {
#     # o que o explorer aceita
#     'escopos': ['individual', 'prudencial'],
#     'columns': ['data', 'documento', 'escopo'],       # listaveis por list_values()
#     'read_columns': ['cnpj_8', 'instituicao', 'escopo',
#                      'cod_conta', 'conta', 'documento', 'valor'],  # devolvidas por read()
#     'read_index': 'date',                             # indice do DataFrame de read()
#     'filtros': ['conta', 'documento', 'escopo', 'instituicao'],
#     'cadastro_columns': ['atividade', 'cnpj_lider_8', ...],  # validas em cadastro=
#
#     # o que ha em disco
#     'by_escopo': {
#         'individual': {'period_count': 12, 'has_data': True},
#         'prudencial': {'period_count': 12, 'has_data': True},
#     },
#     'periods': [202401, 202402, ...],
#     'period_count': 12,
#     'has_data': True,
#     'first_period': 202401,
#     'last_period': 202412,
# }
```

`filtros` e derivado da assinatura real de `read()`, entao nao envelhece.
`read_columns` nao inclui a data: ela vira o `DatetimeIndex` nomeado em
`read_index`. `describe(escopo)` devolve as mesmas chaves de capacidade,
restringe os periodos ao escopo pedido e troca `by_escopo` pela chave
`escopo`; escopo desconhecido levanta `InvalidScopeError`.

### check_era()

Diagnostica se a serie sobrevive a transicao Era 2/Era 3 (202501), sem trazer os
valores (herdado de BaseExplorer). Agrupa por `documento`. Ver
[Warning de Compatibilidade](#warning-de-compatibilidade).

```python
diag = bcb.cosif.check_era("2024-12", "2025-01", escopo="individual")
diag["grupos"]["4010"]["status"]  # 'renumerado'
```

## Colunas Disponiveis

`read()` devolve o periodo de referencia como indice do DataFrame: um `DatetimeIndex` nomeado `date`. As colunas sao:

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| `cnpj_8` | str | CNPJ de 8 digitos |
| `instituicao` | str | Nome da instituicao (canônico do cadastro) |
| `escopo` | str | Escopo dos dados (individual, prudencial) |
| `cod_conta` | str | Codigo numerico da conta COSIF |
| `conta` | str | Nome/descricao da conta |
| `documento` | int | Tipo de documento (ex: 4060 = balancete) |
| `valor` | float | Valor em reais |

### Enriquecimento Cadastral

O parametro `cadastro` permite adicionar colunas do cadastro diretamente no resultado, sem precisar fazer merge manual:

```python
# Adicionar segmento e UF a cada linha
df = bcb.cosif.read(
    "2024-01",
    "2024-12",
    instituicao=["60872504", "60746948"],
    escopo="prudencial",
    cadastro=["segmento", "uf", "tcb"],
)
# Resultado inclui colunas segmento, uf e tcb
```

Colunas cadastrais disponiveis: `atividade`, `cnpj_lider_8`, `cod_congl_fin`, `cod_congl_prud`, `data_inicio_atividade`, `municipio`, `nome_congl_prud`, `segmento`, `situacao`, `sr`, `tc`, `tcb`, `td`, `uf`.

Para dados mensais (COSIF), o alinhamento temporal e automatico: cada mes recebe os atributos cadastrais do trimestre mais recente.

## Exemplos Avancados

### Multiplas Contas

```python
# Principais contas de balanco
contas_balanco = [
    "TOTAL GERAL DO ATIVO",
    "PATRIMONIO LIQUIDO",
    "DISPONIBILIDADES",
    "OPERACOES DE CREDITO",
]

df = bcb.cosif.read(
    "2024-12", instituicao="60746948", conta=contas_balanco, escopo="prudencial"
)

# Pivotar para visualizacao
pivot = df.pivot_table(
    values="valor", index="instituicao", columns="conta", aggfunc="sum"
)
```

### Comparar Escopos

```python
# Coletar dados de ambos escopos de uma vez
df = bcb.cosif.read(
    "2024-12",
    instituicao="60872504",
    conta=["TOTAL GERAL DO ATIVO"],
)
# escopo=None retorna ambos com coluna escopo

# Comparar
print(df.pivot_table(values="valor", index="instituicao", columns="escopo"))
```

### Evolucao Temporal

```python
# Serie temporal do Patrimonio Liquido
df = bcb.cosif.read(
    "2024-01",
    "2024-12",
    instituicao="60746948",
    conta=["PATRIMONIO LIQUIDO"],
    escopo="prudencial",
)

# O indice ja e um DatetimeIndex ordenado por data
df["valor"].plot(kind="line", title="Patrimonio Liquido 2024")
```

### Top Instituicoes por Ativo (SQL)

```python
from ifdata_bcb.infra import QueryEngine

qe = QueryEngine()

# Usando SQL para ranking (nomes de STORAGE, nao de apresentacao)
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

Colunas armazenadas em Parquet (uniformes para todas as eras) e o nome de apresentacao correspondente na API:

| Coluna de Storage | Coluna de Apresentacao |
|-------------------|------------------------|
| DATA_BASE | data (em `read()` vira o indice `date`) |
| CNPJ_8 | cnpj_8 |
| NOME_INSTITUICAO | instituicao |
| CONTA | cod_conta |
| NOME_CONTA | conta |
| DOCUMENTO | documento |
| SALDO | valor |

Os nomes de storage nao mudaram: SQL direto via `QueryEngine` continua usando `DATA_BASE`, `NOME_CONTA`, `SALDO`, etc.

### Warning de Compatibilidade

Ao consultar periodos que cruzam a fronteira Era 2/Era 3 (202501), a biblioteca compara os codigos de conta dos dois lados no proprio resultado e emite `IncompatibleEraWarning`:

```python
# Emite IncompatibleEraWarning: 0% dos codigos em comum entre as eras
df = bcb.cosif.read("2024-12", "2025-01", instituicao="60872504")
```

A renumeracao do COSIF e total: os codigos da Era 2 tem 8 digitos e os da Era 3 tem 10, entao **nenhum** codigo sobrevive a transicao. Mesmo pelo nome da conta a serie nao e continua -- so uma minoria dos nomes se mantem. Trate os periodos antes e depois de 202501 como duas series distintas.

O warning nao bloqueia a query. O diagnostico completo fica em `df.attrs['era']` e pode ser consultado antes com `bcb.cosif.check_era('2024-12', '2025-01')` -- o agrupamento no COSIF e por `documento`. Ver [ifdata.md](ifdata.md#diagnostico-de-era-programatico) para o formato da estrutura.

## Tratamento de Erros

```python
from ifdata_bcb import (
    MissingRequiredParameterError,
    InvalidDateRangeError,
    InvalidScopeError,
)

# Erro: parametro obrigatorio ausente
try:
    df = bcb.cosif.read(start=None)  # Falta start!
except MissingRequiredParameterError as e:
    print(f"Erro: {e}")

# Erro: range de datas invalido
try:
    df = bcb.cosif.read(
        "2024-12",
        "2024-01",  # start > end!
        instituicao="60872504",
    )
except InvalidDateRangeError as e:
    print(f"Erro: {e}")

# Erro: escopo invalido
try:
    df = bcb.cosif.read("2024-12", instituicao="60872504", escopo="invalido")
except InvalidScopeError as e:
    print(f"Erro: {e}")

# Erro: documento nao numerico
try:
    df = bcb.cosif.read("2024-12", documento="balancete")
except InvalidScopeError as e:
    print(f"Erro: {e}")
    # Esperado codigo numerico (ex: 4010, 4016).
    # Use cosif.list_values(['documento']) para ver os disponiveis.

# Sem erro: retorna DataFrame vazio se nao encontrar dados
df = bcb.cosif.read("2024-12", instituicao="99999999")
if df.empty:
    print("Instituicao nao encontrada nos dados COSIF")
```
