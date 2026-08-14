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

| Escopo | tipo_inst | Descricao |
|--------|-----------|-----------|
| `individual` | 3 | Dados da instituicao especifica |
| `prudencial` | 1 | Dados do conglomerado prudencial |
| `financeiro` | 2 | Dados do conglomerado financeiro |

```python
# Filtrar por escopo
df = bcb.ifdata.read("2024-12", instituicao="60872504", escopo="prudencial")

# Buscar em todos os escopos (escopo=None)
df = bcb.ifdata.read("2024-12", instituicao="60872504")
# Resultado inclui coluna escopo
```

## API Reference

### collect()

Coleta dados IFDATA Valores do BCB.

```python
bcb.ifdata.collect(
    start: DateScalar,              # Data inicial
    end: DateScalar | None = None,  # Data final. None = apenas o trimestre de start
    force: bool = False,            # Se True, recoleta dados existentes
    verbose: bool = True            # Se True, exibe progresso
)
```

**Nota**: Apenas trimestres (03, 06, 09, 12) serao coletados.

**Exemplos**:

```python
# Coletar dados de 2024
bcb.ifdata.collect("2024-01", "2024-12")
# Coleta apenas: 202403, 202406, 202409, 202412

# Trimestre unico: end e opcional, e start e alinhado para o fim do trimestre
bcb.ifdata.collect("2024-07")  # coleta 202409

# Forcar recoleta
bcb.ifdata.collect("2024-12", force=True)
```

**Nao use `collect(x, x)` para um periodo unico.** Numa fonte trimestral o
range alinha o inicio para o fim do trimestre, que fica maior que o `end`, e a
chamada coleta zero periodos sem erro nenhum. Passe apenas `start`.

### fetch()

Baixa dados do BCB e devolve o DataFrame no formato de `read()`, **sem tocar o cache local**. Os arquivos baixados vivem num diretorio temporario descartado ao final; nada persiste.

```python
bcb.ifdata.fetch(
    start: DateScalar,                             # Data inicial ou unica. OBRIGATORIO (posicional)
    end: DateScalar | None = None,                 # Data final para range (posicional)
    *,                                      # --- keyword-only a partir daqui ---
    instituicao: str | list[str] | None = None,
    escopo: str | None = None,
    conta: str | list[str] | None = None,
    relatorio: str | None = None,
    grupo: str | None = None,
    columns: list[str] | None = None,
    verbose: bool = True
) -> pd.DataFrame
```

Mesmos filtros de `read()`, exceto `cadastro=` -- o enriquecimento exige o cadastro no cache local. Resolucao de conglomerados e nomes canonicos usam o cadastro do cache local, quando coletado.

```python
# Consulta pontual sem popular o cache
df = bcb.ifdata.fetch("2024-12", instituicao="60872504", escopo="prudencial")
```

### read()

Le dados IFDATA Valores com filtros.

```python
bcb.ifdata.read(
    start: DateScalar,                             # Data inicial ou unica. OBRIGATORIO (posicional)
    end: DateScalar | None = None,                 # Data final para range (posicional)
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

**Retorno**: DataFrame indexado por um `DatetimeIndex` nomeado `date` -- a data do periodo sai das colunas e vira o indice. As demais colunas usam nomes lowercase (`cnpj_8`, `conta`, `valor`, ...).

**Bulk read**: Quando `instituicao=None` (padrao), retorna dados de todas as instituicoes do periodo, sem necessidade de resolver entidade. Util para rankings e analises agregadas.

**Raises**:
- `MissingRequiredParameterError`: Se `start` nao fornecido.
- `InvalidDateRangeError`: Se `start > end`.

**Exemplos**:

```python
# Data unica em um escopo especifico
df = bcb.ifdata.read("2024-12", instituicao="60872504", escopo="prudencial")

# Buscar em TODOS os escopos
df = bcb.ifdata.read("2024-12", instituicao="60872504")

# Bulk read: todas as instituicoes
df = bcb.ifdata.read("2024-12", escopo="prudencial")

# Conta especifica (filtro case-insensitive)
df = bcb.ifdata.read("2024-12", instituicao="60872504", conta="lucro liquido")

# Multiplas contas com range de datas
df = bcb.ifdata.read(
    "2024-03", "2024-12", instituicao="60872504", conta=["Lucro Liquido", "Ativo Total"]
)

# Filtrar por relatorio
df = bcb.ifdata.read("2024-12", instituicao="60872504", relatorio="Resumo")

# Filtrar por grupo de conta
df = bcb.ifdata.read("2024-12", instituicao="60872504", grupo="Resumo")
```

### list_contas()

Lista contas disponiveis nos dados.

```python
bcb.ifdata.list_contas(
    termo: str | None = None,      # Filtro por nome (case-insensitive)
    *,                             # --- keyword-only a partir daqui ---
    escopo: str | None = None,     # 'individual', 'prudencial', 'financeiro'
    relatorio: str | None = None,  # Filtro por relatorio (case/accent-insensitive)
    start: DateScalar | None = None,      # Periodo inicial (filtra contas que existem no periodo)
    end: DateScalar | None = None,        # Periodo final. Se None com start, filtra data unica
    limit: int = 100               # Numero maximo de contas. Deve ser > 0
) -> pd.DataFrame
```

**Raises**: `ValueError` se `limit <= 0`.

**Retorna**: DataFrame flat com colunas `cod_conta`, `conta`, `relatorio` e `grupo`, ordenado por relatorio, grupo, conta.

**Exemplos**:

```python
# Listar todas as contas
contas = bcb.ifdata.list_contas()

# Buscar contas que contenham "lucro"
contas = bcb.ifdata.list_contas(termo="lucro")

# Listar contas do escopo individual
contas = bcb.ifdata.list_contas(escopo="individual", limit=50)

# Filtrar contas por relatorio
contas = bcb.ifdata.list_contas(relatorio="Resumo")
```

### list_values()

Lista valores distintos para colunas solicitadas (SELECT DISTINCT via DuckDB). Retorna DataFrame flat.

```python
bcb.ifdata.list_values(
    columns: list[str],            # Colunas a listar: data, escopo, relatorio, grupo
    *,
    start: DateScalar | None = None,      # Periodo inicial
    end: DateScalar | None = None,        # Periodo final
    escopo: str | None = None,     # Filtro por escopo
    relatorio: str | None = None,  # Filtro por relatorio (case/accent insensitive)
    grupo: str | None = None,      # Filtro por grupo (case/accent insensitive)
    limit: int = 100               # Maximo de resultados
) -> pd.DataFrame
```

**Colunas bloqueadas** (emitem warning e retornam DataFrame vazio):
- `conta`, `cod_conta`: use `list_contas()` para buscar contas
- `cod_inst`: use `cadastro.search(fonte='ifdata')` para listar instituicoes
- `valor`: metrica continua, nao listavel

**Raises**: `InvalidColumnError` se coluna invalida. `TruncatedResultWarning` quando `len(resultado) == limit`.

**Exemplos**:

```python
# Listar relatorios disponiveis
bcb.ifdata.list_values(["relatorio"])

# Listar combinacoes relatorio + escopo
bcb.ifdata.list_values(["relatorio", "escopo"])

# Listar grupos de um relatorio especifico
bcb.ifdata.list_values(["grupo"], relatorio="Ativo")

# Listar periodos como datetime64
bcb.ifdata.list_values(["data"])
```

### mapeamento()

Tabela de mapeamento cod_inst <-> cnpj_8 por escopo. Retorna DataFrame flat.

```python
bcb.ifdata.mapeamento(
    start: DateScalar | None = None,      # Data inicial ou unica
    end: DateScalar | None = None         # Data final para range
) -> pd.DataFrame
```

**Retorna**: DataFrame com colunas:
- `cod_inst`: Codigo de reporte no IFDATA
- `tipo_inst`: Codigo do tipo de instituicao (1, 2, 3)
- `escopo`: "individual", "prudencial" ou "financeiro"
- `report_key_type`: "cnpj" ou nome do escopo
- `cnpj_8`: CNPJ da entidade associada
- `instituicao`: Nome canonico

**Exemplos**:

```python
# Ver mapeamento completo
reporters = bcb.ifdata.mapeamento(start="2024-12")
print(reporters[reporters["cnpj_8"] == "60872504"])

# Descobrir cod_inst de um banco por escopo
df = bcb.ifdata.mapeamento(start="2024-12")
df[df["cnpj_8"] == "60746948"]  # Bradesco: individual=60746948, prudencial=C0080075

# Listar membros de um conglomerado
df[df["cod_inst"] == "C0080075"]
```

### list_periodos()

Lista periodos disponiveis (herdado de BaseExplorer). Aceita `escopo` para restringir a um escopo.

```python
periodos = bcb.ifdata.list_periodos()                     # Retorna [202403, 202406, ...]
periodos = bcb.ifdata.list_periodos(escopo="financeiro")  # Apenas periodos com linhas desse escopo
```

No IFDATA o escopo e coluna dos dados (`TipoInstituicao`), nao diretorio. A
resposta por escopo vem dos proprios dados: um periodo so aparece para um
escopo se ha linhas dele no parquet -- `financeiro`, por exemplo, nao existe
em todos os periodos.

**Raises**: `InvalidScopeError` se o escopo nao for valido para o explorer.

### describe()

Retorna o que o explorer aceita e o que ha coletado (herdado de BaseExplorer).
Ver [cosif.md](cosif.md#describe) para o formato completo.

```python
info = bcb.ifdata.describe()
info["escopos"]  # ['individual', 'prudencial', 'financeiro']
info["filtros"]  # ['conta', 'escopo', 'grupo', 'instituicao', 'relatorio']
info["read_index"]  # 'date' -- read() devolve a data como DatetimeIndex
info["by_escopo"]["financeiro"]  # {'period_count': ..., 'has_data': ...}
```

Como em `list_periodos()`, o resumo `by_escopo` (e `describe(escopo)`) e
resolvido pelos dados: um periodo so conta para o escopo que tem linhas dele.

### check_era()

Diagnostica se a serie sobrevive a transicao de era de 202503, sem trazer os
valores (herdado de BaseExplorer). Ver
[Diagnostico de era programatico](#diagnostico-de-era-programatico).

```python
diag = bcb.ifdata.check_era("2024-12", "2025-03", escopo="prudencial")
```

## Colunas Disponiveis

`read()` devolve o periodo de referencia como indice do DataFrame: um `DatetimeIndex` nomeado `date`. As colunas sao:

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| `cnpj_8` | str | CNPJ de 8 digitos (resolvido automaticamente para conglomerados em bulk reads) |
| `instituicao` | str | Nome da instituicao (canônico do cadastro) |
| `escopo` | str | Escopo dos dados (individual, prudencial, financeiro) |
| `cod_inst` | str | Codigo da instituicao no BCB |
| `cod_conta` | str | Codigo numerico da conta |
| `conta` | str | Nome/descricao da conta |
| `valor` | float | Valor em reais |
| `relatorio` | str | Nome do relatorio de origem |
| `grupo` | str | Grupo da conta |

### Sobre cod_inst vs cnpj_8

- `cod_inst`: Codigo interno do BCB para a instituicao/conglomerado
- `cnpj_8`: CNPJ de 8 digitos que voce passou na consulta

Para escopo `individual`, `cod_inst` e igual ao `cnpj_8`.
Para escopos `prudencial` e `financeiro`, `cod_inst` pode ser o codigo do conglomerado
ou o proprio CNPJ, dependendo de como a entidade reporta ao BCB.

### Sobre relatorio

Indica a origem dos dados:
- **Resumo**: Indicadores principais
- **Ativo**: Composicao do ativo
- **Passivo**: Composicao do passivo
- **DRE**: Demonstracao do Resultado

### Sobre grupo

Agrupamento logico das contas para navegacao hierarquica.

### Enriquecimento Cadastral

O parametro `cadastro` permite adicionar colunas do cadastro diretamente no resultado, sem precisar fazer merge manual:

```python
# Adicionar tipo de banco e segmento
df = bcb.ifdata.read(
    "2024-03",
    "2024-12",
    instituicao=["60872504", "60746948"],
    escopo="prudencial",
    cadastro=["tcb", "tc", "segmento"],
)
# Resultado inclui colunas tcb, tc e segmento
```

Colunas cadastrais disponiveis: `atividade`, `cnpj_lider_8`, `cod_congl_fin`, `cod_congl_prud`, `data_inicio_atividade`, `municipio`, `nome_congl_prud`, `segmento`, `situacao`, `sr`, `tc`, `tcb`, `td`, `uf`.

## Exemplos Avancados

### Filtrar por Escopo

```python
# Apenas escopo prudencial (conglomerados)
df_prud = bcb.ifdata.read("2024-12", instituicao="60872504", escopo="prudencial")
print(f"Escopo: {df_prud['escopo'].iloc[0]}")

# Todos os escopos disponiveis
df_todos = bcb.ifdata.read("2024-12", instituicao="60872504")
print(f"Escopos: {df_todos['escopo'].unique()}")
```

### Analisar Grupos de Contas

```python
# Listar grupos disponiveis
df = bcb.ifdata.read("2024-12", instituicao="60872504", escopo="prudencial")
grupos = df["grupo"].unique()
print(f"Grupos: {grupos}")
```

### Serie Temporal de Lucro

```python
# Evolucao trimestral do Lucro Liquido
df = bcb.ifdata.read(
    "2023-01",
    "2024-12",
    instituicao="60872504",
    conta=["Lucro Liquido"],
)

# O indice ja e um DatetimeIndex ordenado por data
print(df["valor"])
```

### Ranking por Ativo Total (SQL)

```python
from ifdata_bcb.infra import QueryEngine

qe = QueryEngine()

# Usando SQL para ranking (nomes de STORAGE, nao de apresentacao)
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

| Coluna Original | Coluna de Apresentacao |
|-----------------|------------------------|
| AnoMes | data (em `read()` vira o indice `date`) |
| CodInst | cod_inst |
| Conta | cod_conta |
| NomeColuna | conta |
| Saldo | valor |
| NomeRelatorio | relatorio |
| Grupo | grupo |

Os nomes de storage nao mudaram: SQL direto via `QueryEngine` continua usando `AnoMes`, `CodInst`, `Saldo`, etc.

### Warnings de Compatibilidade entre Eras

A partir de 202503 (marco/2025), o BCB mudou a estrutura dos dados IFDATA. Quando o periodo solicitado cobre os dois lados dessa transicao, a biblioteca compara os codigos de conta antes e depois no proprio resultado e avisa conforme o que mediu. Relatorios cujas contas continuam as mesmas nao geram warning nenhum.

**IncompatibleEraWarning**: Codigos de conta renumerados (Resumo, Ativo, Passivo, DRE, Segmentacao):

```python
# Emite IncompatibleEraWarning: 30% dos codigos em comum entre as eras
df = bcb.ifdata.read("2024-12", "2025-03", instituicao="60872504", relatorio="Resumo")
```

**ScopeMigrationWarning**: Relatorios de credito migraram de escopo `financeiro` para `prudencial` a partir de 202503:

```python
# Emite ScopeMigrationWarning: periodos < 202503 nao tem dados no escopo prudencial
df = bcb.ifdata.read("2024-12", "2025-03", instituicao="60872504", escopo="prudencial")
```

**DroppedReportWarning**: Relatorio descontinuado (ex: "por nivel de risco da operacao" apos 202412):

```python
# Emite DroppedReportWarning: relatorio descontinuado
df = bcb.ifdata.read(
    "2025-03", relatorio="Carteira de credito ativa - por nivel de risco da operacao"
)
```

**PartialDataWarning** (`reason='era_coverage_gap'`): Parte do resultado cobre so um lado da transicao, sem causa conhecida -- inclui relatorios que o BCB **introduziu** em 202503 e cache incompleto:

```python
# Este relatorio so existe a partir de 202503: metade da serie pedida nao existe
df = bcb.ifdata.read(
    "2024-12",
    "2025-03",
    relatorio="Carteira de credito ativa - por carteiras de instrumentos financeiros",
)
```

Nenhum warning bloqueia a query -- apenas alertam sobre potenciais incompatibilidades nos resultados.

### Diagnostico de era programatico

Os warnings sao deduplicados pelo Python (a segunda chamada identica nao reavisa) e nao viajam com o DataFrame. Para consumo programatico, o mesmo diagnostico esta disponivel como estrutura:

```python
df = bcb.ifdata.read("2024-12", "2025-03", relatorio="Resumo")
df.attrs["era"]["grupos"]["Resumo"]
# {'status': 'renumerado', 'n_pre': 9, 'n_post': 10,
#  'n_comum': 3, 'pct_overlap': 30.0, 'motivo': None}
```

E consultavel antes de puxar os dados, com `check_era()` -- que le apenas as colunas de dimensao e serializa direto para JSON:

```python
diag = bcb.ifdata.check_era("2024-12", "2025-03")
diag["cruza_boundary"]  # True
diag["periodos_ausentes"]  # []
[nome for nome, g in diag["grupos"].items() if g["status"] == "estavel"]
# relatorios cuja serie atravessa a transicao sem quebra
```

Valores de `status`: `estavel` (contas praticamente iguais dos dois lados), `renumerado` (menos de 90% dos codigos em comum), `so_pre` e `so_post` (o grupo so tem dados de um lado). O campo `motivo` traz a causa quando conhecida: `descontinuado` ou `migracao_escopo`.

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
    df = bcb.ifdata.read(start=None)  # Falta start!
except MissingRequiredParameterError as e:
    print(f"Erro: {e}")

# Erro: range de datas invalido
try:
    df = bcb.ifdata.read(
        "2024-12",
        "2024-01",  # start > end!
        instituicao="60872504",
    )
except InvalidDateRangeError as e:
    print(f"Erro: {e}")

# Sem erro: retorna DataFrame vazio se nao encontrar dados
df = bcb.ifdata.read("2024-12", instituicao="99999999")
if df.empty:
    print("Instituicao nao encontrada nos dados IFDATA")
```
