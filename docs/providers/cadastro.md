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
- Mapeamento nome -> CNPJ (funcao `bcb.search()`)
- Informacoes de conglomerado (cod_congl_prud, cod_congl_fin)
- Identificacao da instituicao lider
- Segmentacao e classificacoes regulatorias

## API Reference

### collect()

Coleta dados cadastrais do BCB.

```python
bcb.cadastro.collect(
    start: str,           # Data inicial (YYYY-MM)
    end: str,             # Data final (YYYY-MM)
    force: bool = False   # Se True, recoleta dados existentes
)
```

**Exemplo**:

```python
# Coletar cadastro de 2024
bcb.cadastro.collect('2024-01', '2024-12')
```

### read()

Le dados cadastrais com filtros opcionais.

```python
bcb.cadastro.read(
    instituicao: str | list = None,  # CNPJ(s) de 8 digitos
    start: str = None,               # Data inicial ou unica (YYYY-MM)
    end: str = None,                 # Data final para range (YYYY-MM)
    segmento: str = None,            # Segmento para filtrar
    uf: str = None,                  # UF para filtrar
    columns: list = None             # Colunas especificas
) -> pd.DataFrame
```

**API de datas**:
- `start` sozinho: filtra data unica (ex: `start='2024-12'`)
- `start` + `end`: gera range trimestral automatico

**Exemplos**:

```python
# Dados de uma instituicao
df = bcb.cadastro.read(instituicao='60872504')

# Filtrar por segmento
df = bcb.cadastro.read(segmento='Banco Multiplo', start='2024-12')

# Filtrar por UF
df = bcb.cadastro.read(uf='SP', start='2024-12')

# Combinar filtros
df = bcb.cadastro.read(segmento='Cooperativa de Credito', uf='MG', start='2024-12')
```

### info()

Retorna informacoes detalhadas de uma instituicao.

```python
bcb.cadastro.info(
    instituicao: str,         # CNPJ de 8 digitos
    start: str = None         # Periodo (YYYY-MM). Se None, retorna mais recente
) -> dict | None
```

**Retorna**: Dicionario com todas as colunas da instituicao ou `None` se nao encontrar.
Valores "null" sao convertidos para `None`.

**Exemplo**:

```python
# Dados mais recentes
info = bcb.cadastro.info('60872504')

# Periodo especifico
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
    cod_congl: str,           # Codigo do conglomerado
    start: str                # Periodo (YYYY-MM). OBRIGATORIO
) -> pd.DataFrame
```

**Raises**:
- `MissingRequiredParameterError`: Se `start` nao fornecido.

**Exemplo**:

```python
# Membros do conglomerado do Itau
membros = bcb.cadastro.get_conglomerate_members('C0080099', start='2024-12')
print(membros[['CNPJ_8', 'INSTITUICAO', 'SEGMENTO']])
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
| `CNPJ_LIDER_8` | str | CNPJ da instituicao lider |
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
# Bancos do Segmento 1 (maiores)
df = bcb.cadastro.read(start='2024-12')
bancos_s1 = df[df['SR'] == 'S1']
print(bancos_s1[['CNPJ_8', 'INSTITUICAO']])

# Bancos multiplos ativos
df_multiplos = bcb.cadastro.read(segmento='Banco Multiplo', start='2024-12')
ativos = df_multiplos[df_multiplos['SITUACAO'] == 'A']
```

## Conglomerados

### Prudencial vs Financeiro

| Tipo | Campo | Descricao |
|------|-------|-----------|
| Prudencial | `COD_CONGL_PRUD` | Consolidacao para fins de supervisao |
| Financeiro | `COD_CONGL_FIN` | Consolidacao para fins contabeis |

### Identificar Lider do Conglomerado

```python
# Obter info da instituicao
info = bcb.cadastro.info(instituicao='60872504', start='2024-12')

# Verificar se e lider
if info['CNPJ_8'] == info['CNPJ_LIDER_8']:
    print("Esta instituicao e a lider do conglomerado")
else:
    print(f"A lider do conglomerado e: {info['CNPJ_LIDER_8']}")
```

### Listar Membros do Conglomerado

```python
# Obter codigo do conglomerado
info = bcb.cadastro.info(instituicao='60872504', start='2024-12')
cod_congl = info['COD_CONGL_PRUD']

# Listar todos os membros
membros = bcb.cadastro.get_conglomerate_members(cod_congl, start='2024-12')
print(f"Conglomerado {cod_congl} tem {len(membros)} membros:")
print(membros[['CNPJ_8', 'INSTITUICAO', 'SEGMENTO']])
```

## Exemplos Avancados

### Estatisticas por Segmento

```python
# Contar instituicoes por segmento
df = bcb.cadastro.read(start='2024-12')
contagem = df.groupby('SEGMENTO').size().sort_values(ascending=False)
print(contagem)
```

### Instituicoes por UF

```python
# Distribuicao geografica
df = bcb.cadastro.read(start='2024-12')
por_uf = df.groupby('UF').size().sort_values(ascending=False)
print(por_uf.head(10))
```

### Cooperativas de um Estado

```python
# Cooperativas de Santa Catarina
coops_sc = bcb.cadastro.read(
    segmento='Cooperativa de Credito',
    uf='SC',
    start='2024-12'
)
print(f"Cooperativas em SC: {len(coops_sc)}")
```

### Mapeamento de Conglomerados

```python
# Criar mapeamento CNPJ -> Conglomerado
df = bcb.cadastro.read(start='2024-12')

# Filtrar apenas instituicoes com conglomerado
com_congl = df[df['COD_CONGL_PRUD'].notna()]

# Contar membros por conglomerado
congl_count = com_congl.groupby('COD_CONGL_PRUD').size()
print(f"Maiores conglomerados:")
print(congl_count.sort_values(ascending=False).head(10))
```

### SQL Personalizado

```python
# Fintechs ativas
df = bcb.sql("""
    SELECT CNPJ_8, INSTITUICAO, SEGMENTO, UF
    FROM '{cache}/ifdata/cadastro/*.parquet'
    WHERE DATA = 202412
      AND SITUACAO = 'A'
      AND (SEGMENTO = 'Instituicao de Pagamento'
           OR SEGMENTO = 'Sociedade de Credito Direto')
    ORDER BY INSTITUICAO
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

A biblioteca normaliza para o formato padronizado (ver secao Colunas Disponiveis).

## Tratamento de Erros

```python
from ifdata_bcb import InvalidIdentifierError

# Erro: identificador invalido
try:
    df = bcb.cadastro.read(instituicao='Itau')  # Nome nao permitido!
except InvalidIdentifierError as e:
    print(f"Erro: {e}")
    # Use bcb.search('Itau') para encontrar o CNPJ

# Instituicao nao encontrada
info = bcb.cadastro.info('99999999')
if info is None:
    print("Instituicao nao encontrada no cadastro")
```
