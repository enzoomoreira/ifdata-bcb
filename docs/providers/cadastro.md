# Provider Cadastro

O Cadastro contem metadados das instituicoes financeiras brasileiras, incluindo segmento, localizacao e informacoes de conglomerado.

## Visao Geral

### Origem dos Dados

Os dados cadastrais sao disponibilizados pelo Banco Central do Brasil via API OData:

- **URL Base**: `https://olinda.bcb.gov.br/olinda/servico/IFDATA/versao/v1/odata`
- **Endpoint**: `IfDataCadastro`
- **Formato**: CSV via parametro `$format=text/csv`
- **Encoding**: UTF-8

### Periodicidade

- **Frequencia**: Trimestral (mesma do IFDATA)
- **Meses de Fechamento**: Marco (03), Junho (06), Setembro (09), Dezembro (12)

### Relacao com Outras Fontes

O Cadastro e a fonte primaria para:
- Informacoes de conglomerado (cod_congl_prud, cod_congl_fin)
- Identificacao da instituicao lider
- Segmentacao e classificacoes regulatorias
- Resolucao de entidades via EntityLookup

## API Reference

### collect()

Coleta dados cadastrais do BCB.

```python
bcb.cadastro.collect(
    start: str,           # Data inicial (YYYY-MM)
    end: str,             # Data final (YYYY-MM)
    force: bool = False,  # Se True, recoleta dados existentes
    verbose: bool = True  # Se True, exibe progresso
)
```

**Exemplo**:

```python
# Coletar cadastro de 2024
bcb.cadastro.collect('2024-01', '2024-12')
```

### read()

Le dados cadastrais com filtros.

```python
bcb.cadastro.read(
    start: str,                             # Data inicial ou unica. OBRIGATORIO (posicional)
    end: str | None = None,                 # Data final para range (posicional)
    *,                                      # --- keyword-only a partir daqui ---
    instituicao: str | list[str] | None = None,  # CNPJ(s) de 8 digitos (opcional)
    segmento: str | None = None,            # Segmento para filtrar (accent-insensitive)
    uf: str | None = None,                  # UF para filtrar
    situacao: str | None = None,            # Situacao para filtrar ('A'=Ativo, 'I'=Inativo)
    atividade: str | None = None,           # Atividade para filtrar
    tcb: str | None = None,                 # TCB para filtrar
    td: str | None = None,                  # TD para filtrar
    tc: str | int | None = None,            # TC para filtrar (aceita str ou int)
    sr: str | None = None,                  # SR para filtrar
    municipio: str | None = None,           # Municipio para filtrar
    columns: list[str] | None = None        # Colunas especificas (aceita nomes de apresentacao)
) -> pd.DataFrame
```

**Parametro Obrigatorio**: `start`.

**API de Datas**:
- `start` sozinho: filtra data unica (ex: `start='2024-12'`)
- `start` + `end`: gera range trimestral automatico

**Filtros de texto** (`segmento`, `uf`, `situacao`, `atividade`, `tcb`, `td`, `tc`, `sr`, `municipio`): case e accent-insensitive. `'Banco Multiplo'` funciona igual a `'Banco Multiplo'` com acento.

**Raises**:
- `MissingRequiredParameterError`: Se `start` nao fornecido.
- `InvalidDateRangeError`: Se `start > end`.

**Exemplos**:

```python
# Dados de uma instituicao em um periodo especifico
df = bcb.cadastro.read('2024-12', instituicao='60872504')

# Filtrar por segmento (accent-insensitive)
df = bcb.cadastro.read('2024-12', segmento='Banco Multiplo')

# Filtrar por UF
df = bcb.cadastro.read('2024-12', instituicao='60872504', uf='SP')

# Filtrar apenas instituicoes ativas
df = bcb.cadastro.read('2024-12', situacao='A')

# Novos filtros: atividade, tcb, td, tc, sr, municipio
df = bcb.cadastro.read('2024-12', tcb='B1', sr='S1')
df = bcb.cadastro.read('2024-12', municipio='Sao Paulo', uf='SP')

# Combinar filtros
df = bcb.cadastro.read(
    '2024-12',
    instituicao='60872504',
    segmento='Banco Multiplo',
    uf='SP',
    situacao='A'
)
```

### list()

Lista valores distintos para colunas solicitadas (SELECT DISTINCT via DuckDB).

```python
bcb.cadastro.list(
    columns: list[str],            # Colunas a listar (ver tabela abaixo)
    *,
    start: str | None = None,      # Periodo inicial
    end: str | None = None,        # Periodo final
    segmento: str | None = None,   # Filtro por segmento (case/accent insensitive)
    uf: str | None = None,         # Filtro por UF
    situacao: str | None = None,   # Filtro por situacao
    atividade: str | None = None,  # Filtro por atividade
    tcb: str | None = None,        # Filtro por TCB
    td: str | None = None,         # Filtro por TD
    tc: str | int | None = None,   # Filtro por TC
    sr: str | None = None,         # Filtro por SR
    municipio: str | None = None,  # Filtro por municipio
    limit: int = 100               # Maximo de resultados
) -> pd.DataFrame
```

**Colunas aceitas**: DATA, SEGMENTO, UF, SITUACAO, ATIVIDADE, TCB, TD, TC, SR, MUNICIPIO.

**Colunas bloqueadas** (emitem warning e retornam DataFrame vazio):
- `CNPJ_8`, `INSTITUICAO`: use `cadastro.search()` para buscar instituicoes

**Raises**: `InvalidColumnError` se coluna invalida. `TruncatedResultWarning` quando `len(resultado) == limit`.

**Exemplos**:

```python
# Listar segmentos disponiveis
bcb.cadastro.list(["SEGMENTO"])

# Listar UFs
bcb.cadastro.list(["UF"])

# Listar municipios de SP (sem filtro, trunca em 100)
bcb.cadastro.list(["MUNICIPIO"], uf='SP')

# Combinacao de colunas
bcb.cadastro.list(["SEGMENTO", "UF"], situacao='A')
```

### search()

Busca instituicoes por nome ou lista todas com dados disponiveis.

```python
bcb.cadastro.search(
    termo: str | None = None,       # Termo de busca (fuzzy). Se None, lista todas
    *,
    fonte: str | None = None,       # "ifdata", "cosif", ou None (todas)
    escopo: str | None = None,      # Filtra por escopo disponivel na fonte
    start: str | None = None,       # Periodo inicial
    end: str | None = None,         # Periodo final
    limit: int = 100                # Maximo de resultados
) -> pd.DataFrame
```

**Com `termo`**: fuzzy matching, retorna matches ordenados por SCORE. Coluna SCORE presente.

**Sem `termo`**: lista todas as instituicoes com dados nos providers solicitados. Sem SCORE.

**Filtro `fonte=`**:
- `None`: instituicoes com dados em qualquer provider
- `"ifdata"`: so com dados no IFDATA
- `"cosif"`: so com dados no COSIF

**Filtro `escopo=`**: filtra por escopo disponivel na fonte (ex: `fonte="cosif", escopo="prudencial"`).

**Filtro `start=`/`end=`**: restringe a verificacao de disponibilidade a um intervalo de periodos. Aceita formatos `'YYYY-MM'` ou `YYYYMM`. Quando `start` e fornecido sem `end`, filtra periodo unico. Apenas instituicoes com dados (COSIF e/ou IFDATA) no intervalo solicitado aparecem no resultado, e a coluna `FONTES` reflete a disponibilidade naquele periodo.

**Retorna**: DataFrame com colunas `CNPJ_8`, `INSTITUICAO`, `SITUACAO`, `FONTES`, e `SCORE` (quando `termo` fornecido).

**Raises**: `InvalidScopeError` se fonte ou escopo invalidos.

**Exemplos**:

```python
# Buscar por nome
bcb.cadastro.search('Itau')

# Listar todas com dados no IFDATA
bcb.cadastro.search(fonte='ifdata')

# Listar instituicoes no COSIF prudencial
bcb.cadastro.search(fonte='cosif', escopo='prudencial')

# Buscar + filtrar por fonte
bcb.cadastro.search('Bradesco', fonte='cosif')

# Buscar com filtro temporal -- so instituicoes com dados em 2025
bcb.cadastro.search('Itau', start='2025-03', end='2025-12')

# Listar todas com dados no IFDATA em Q2 2025
bcb.cadastro.search(fonte='ifdata', start='2025-06')
```

## Colunas Disponiveis

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| `DATA` | datetime | Periodo de referencia |
| `CNPJ_8` | str | CNPJ de 8 digitos |
| `INSTITUICAO` | str | Nome completo |
| `SEGMENTO` | str | Segmento de atuacao |
| `COD_CONGL_PRUD` | str | Codigo do conglomerado prudencial |
| `COD_CONGL_FIN` | str | Codigo do conglomerado financeiro |
| `CNPJ_LIDER_8` | str | CNPJ do lider do conglomerado |
| `SITUACAO` | str | Situacao (A=Ativo) |
| `ATIVIDADE` | str | Atividade principal |
| `TCB` | str | Tipo de Consolidacao Bancaria |
| `TD` | str | Tipo de Documento |
| `TC` | str | Tipo de Controle |
| `UF` | str | Unidade Federativa |
| `MUNICIPIO` | str | Municipio da sede |
| `SR` | str | Segmento Regulatorio |
| `DATA_INICIO_ATIVIDADE` | str | Data de inicio das atividades (YYYYMM) |

## Segmentos e Classificacoes

### Principais Segmentos

| Segmento | Descricao |
|----------|-----------|
| Banco Multiplo | Bancos com multiplas carteiras |
| Banco Comercial | Bancos com foco em varejo |
| Banco de Investimento | Bancos com foco em mercado de capitais |
| Cooperativa de Credito | Cooperativas de economia e credito |
| Instituicao de Pagamento | Fintechs de pagamento |
| Sociedade de Credito Direto | Fintechs de credito |

### Classificacoes Regulatorias

#### TCB (Tipo de Consolidacao Bancaria)

| Codigo | Descricao |
|--------|-----------|
| B1 | Segmento 1 (maiores bancos) |
| B2 | Segmento 2 |
| B3 | Segmento 3 |
| B4 | Segmento 4 |
| B5 | Segmento 5 (menores) |

#### SR (Segmento Regulatorio)

| Codigo | Descricao |
|--------|-----------|
| S1 | Sistemicamente importante |
| S2 | Grande porte |
| S3 | Medio porte |
| S4 | Pequeno porte |
| S5 | Simplificado |

### Filtrar por Classificacao

```python
# Bancos do Segmento S1 (maiores)
df = bcb.cadastro.read('2024-12', instituicao='60872504')
if df['SR'].iloc[0] == 'S1':
    print("Banco sistemicamente importante")
```

## Conglomerados

### Prudencial vs Financeiro

| Tipo | Campo | Descricao |
|------|-------|-----------|
| Prudencial | `COD_CONGL_PRUD` | Consolidacao para fins de supervisao |
| Financeiro | `COD_CONGL_FIN` | Consolidacao para fins contabeis |

### Listar Membros do Conglomerado

```python
# Usar mapeamento IFDATA para ver membros
df = bcb.ifdata.mapeamento(start='2024-12')

# Descobrir cod_inst de um banco
df[df['CNPJ_8'] == '60872504']

# Listar todos os membros do conglomerado
df[df['COD_INST'] == 'C0080075']
```

## Exemplos Avancados

Nos exemplos SQL abaixo, assuma `qe = QueryEngine()` ja inicializado.

### Estatisticas por Segmento (SQL)

```python
from ifdata_bcb.infra import QueryEngine

qe = QueryEngine()

# Contar instituicoes por segmento
df = qe.sql("""
    SELECT SegmentoTb as SEGMENTO, COUNT(*) as TOTAL
    FROM '{cache}/ifdata/cadastro/*.parquet'
    WHERE Data = 202412
    GROUP BY SegmentoTb
    ORDER BY TOTAL DESC
""")
print(df)
```

### Instituicoes por UF (SQL)

```python
# Distribuicao geografica
df = qe.sql("""
    SELECT Uf as UF, COUNT(*) as TOTAL
    FROM '{cache}/ifdata/cadastro/*.parquet'
    WHERE Data = 202412
    GROUP BY Uf
    ORDER BY TOTAL DESC
    LIMIT 10
""")
print(df)
```

### Fintechs Ativas (SQL)

```python
df = qe.sql("""
    SELECT CNPJ_8, NomeInstituicao as INSTITUICAO, SegmentoTb as SEGMENTO, Uf as UF
    FROM '{cache}/ifdata/cadastro/*.parquet'
    WHERE Data = 202412
      AND Situacao = 'A'
      AND (SegmentoTb = 'Instituicao de Pagamento'
           OR SegmentoTb = 'Sociedade de Credito Direto')
    ORDER BY NomeInstituicao
""")
print(df)
```

### Mapeamento de Conglomerados (SQL)

```python
# Contar membros por conglomerado
df = qe.sql("""
    SELECT
        CodConglomeradoPrudencial as COD_CONGL,
        COUNT(*) as MEMBROS
    FROM '{cache}/ifdata/cadastro/*.parquet'
    WHERE Data = 202412
      AND CodConglomeradoPrudencial IS NOT NULL
    GROUP BY CodConglomeradoPrudencial
    ORDER BY MEMBROS DESC
    LIMIT 10
""")
print(df)
```

## URLs e Formato de Origem

### Estrutura da URL

```
https://olinda.bcb.gov.br/olinda/servico/IFDATA/versao/v1/odata/
  IfDataCadastro(AnoMes=@AnoMes)?@AnoMes={YYYYMM}&$format=text/csv
```

### Colunas do CSV Original

```
Data,CodInst,NomeInstituicao,SegmentoTb,CodConglomeradoPrudencial,
CodConglomeradoFinanceiro,CnpjInstituicaoLider,Situacao,Atividade,
Tcb,Td,Tc,Uf,Municipio,Sr,DataInicioAtividade
```

Mapeamento para colunas de apresentacao:

| Coluna Original | Coluna Mapeada |
|-----------------|----------------|
| Data | DATA |
| NomeInstituicao | INSTITUICAO |
| SegmentoTb | SEGMENTO |
| CodConglomeradoPrudencial | COD_CONGL_PRUD |
| CodConglomeradoFinanceiro | COD_CONGL_FIN |
| Situacao | SITUACAO |
| Atividade | ATIVIDADE |
| Tcb | TCB |
| Td | TD |
| Tc | TC |
| Uf | UF |
| Municipio | MUNICIPIO |
| Sr | SR |
| CnpjInstituicaoLider | CNPJ_LIDER_8 (normalizado durante coleta) |
| DataInicioAtividade | DATA_INICIO_ATIVIDADE |

## Tratamento de Erros

```python
# start e obrigatorio em read()
df = bcb.cadastro.read('2024-12', instituicao='60872504')  # OK!

# search() sem resultados retorna DataFrame vazio
df = bcb.cadastro.search('XYZNONEXISTENT')
assert df.empty

# Coluna invalida em list()
from ifdata_bcb.domain.exceptions import InvalidColumnError
try:
    bcb.cadastro.list(["FOO"])
except InvalidColumnError as e:
    print(f"Erro: {e}")
    # Coluna 'FOO' invalida. Disponiveis: ATIVIDADE, DATA, MUNICIPIO, ...

# Fonte/escopo invalido em search()
from ifdata_bcb.domain.exceptions import InvalidScopeError
try:
    bcb.cadastro.search(fonte='cosif', escopo='financeiro')  # COSIF nao tem financeiro
except InvalidScopeError as e:
    print(f"Erro: {e}")
```
