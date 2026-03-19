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

### info()

Retorna informacoes detalhadas de uma instituicao como dicionario.

```python
bcb.cadastro.info(
    instituicao: str,                # CNPJ de 8 digitos
    start: str | None = None         # Periodo (YYYY-MM). Se None, usa ultimo periodo
) -> dict | None
```

**Retorna**: Dicionario com todas as colunas da instituicao ou `None` se nao encontrar.
Valores "null" sao convertidos para `None`.

**Exemplo**:

```python
# Dados do ultimo periodo disponivel
info = bcb.cadastro.info('60872504')

# Dados de um periodo especifico
info = bcb.cadastro.info('60872504', start='2024-12')

if info:
    print(f"Nome: {info['INSTITUICAO']}")
    print(f"Segmento: {info['SEGMENTO']}")
    print(f"UF: {info['UF']}")
    print(f"Conglomerado: {info['COD_CONGL_PRUD']}")
```

### list_segmentos()

Lista segmentos disponiveis.

```python
bcb.cadastro.list_segmentos() -> list[str]
```

**Exemplo**:

```python
segmentos = bcb.cadastro.list_segmentos()
print(segmentos)
# ['Administradora de Consorcio', 'Agencia de Fomento', 'Banco Comercial',
#  'Banco Multiplo', 'Cooperativa de Credito', ...]
```

### list_ufs()

Lista UFs disponiveis.

```python
bcb.cadastro.list_ufs() -> list[str]
```

**Exemplo**:

```python
ufs = bcb.cadastro.list_ufs()
print(ufs)  # ['AC', 'AL', 'AM', ..., 'SP', 'TO']
```

### get_conglomerate_members()

Retorna membros de um conglomerado prudencial.

```python
bcb.cadastro.get_conglomerate_members(
    cod_congl: str,                  # Codigo do conglomerado
    start: str | None = None         # Periodo (YYYY-MM). Se None, usa ultimo periodo
) -> pd.DataFrame
```

**Exemplo**:

```python
# Membros do conglomerado do Itau (ultimo periodo)
membros = bcb.cadastro.get_conglomerate_members('C0080099')
print(membros[['CNPJ_8', 'INSTITUICAO', 'SEGMENTO']])

# Membros em um periodo especifico
membros = bcb.cadastro.get_conglomerate_members('C0080099', start='2024-12')
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
# Obter codigo do conglomerado (ultimo periodo)
info = bcb.cadastro.info('60872504')
cod_congl = info['COD_CONGL_PRUD']

# Listar todos os membros
membros = bcb.cadastro.get_conglomerate_members(cod_congl)
print(f"Conglomerado {cod_congl} tem {len(membros)} membros:")
print(membros[['CNPJ_8', 'INSTITUICAO', 'SEGMENTO']])
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
# Instituicao nao encontrada
info = bcb.cadastro.info('99999999')
if info is None:
    print("Instituicao nao encontrada no cadastro")

# start e obrigatorio em read()
df = bcb.cadastro.read('2024-12', instituicao='60872504')  # OK!

# info() e get_conglomerate_members(): start=None usa ultimo periodo disponivel
info = bcb.cadastro.info('60872504')             # OK!
membros = bcb.cadastro.get_conglomerate_members('C0080099')  # OK!
```
