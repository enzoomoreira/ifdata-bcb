# Inicio Rapido

Guia para comecar a usar o `ifdata-bcb` em analises de dados financeiros do Banco Central do Brasil.

## O que e o ifdata-bcb

O `ifdata-bcb` e uma biblioteca Python para coleta e exploracao de dados bancarios do Brasil, disponibilizados pelo Banco Central. A biblioteca fornece acesso a:

- **COSIF**: Plano Contabil das Instituicoes do Sistema Financeiro Nacional (dados mensais)
- **IFDATA**: Informacoes Financeiras Trimestrais (dados trimestrais)
- **Cadastro**: Metadados das instituicoes financeiras (segmento, conglomerado, situacao)

### Casos de Uso

- Analise de balancos de bancos e instituicoes financeiras
- Comparacao de indicadores entre instituicoes
- Acompanhamento de evolucao temporal de contas contabeis
- Pesquisa academica em financas bancarias
- Due diligence e analise de credito

## Instalacao

### Requisitos

- Python 3.12 ou superior
- Conexao com internet (para coleta de dados do BCB)

### Instalacao via pip

```bash
pip install ifdata-bcb
```

### Instalacao via uv

```bash
uv add ifdata-bcb
```

### Verificacao

```python
import ifdata_bcb as bcb

# Verificar componentes disponiveis
print(f"bcb.cosif: {type(bcb.cosif).__name__}")  # COSIFExplorer
print(f"bcb.ifdata: {type(bcb.ifdata).__name__}")  # IFDATAExplorer
print(f"bcb.cadastro: {type(bcb.cadastro).__name__}")  # CadastroExplorer
print(f"bcb.cadastro.search: {type(bcb.cadastro.search).__name__}")  # method
```

## Primeiro Uso

### 1. Importar a biblioteca

```python
import ifdata_bcb as bcb
```

### 2. Coletar dados

Antes de consultar, e necessario coletar os dados do site do BCB:

```python
# Definir periodo
START = "2024-01"
END = "2024-12"

# Coletar cadastro (necessario para busca por nome)
bcb.cadastro.collect(START, END)

# Coletar COSIF (ambos escopos por padrao: individual e prudencial)
bcb.cosif.collect(START, END)

# Coletar IFDATA
bcb.ifdata.collect(START, END)
```

A coleta baixa os dados do site do BCB e armazena localmente em formato Parquet. Esse processo so precisa ser feito uma vez por periodo (a menos que use `force=True`).

Para espiar dados sem persistir nada no cache local, use `fetch()`: ele baixa do BCB para um diretorio temporario, devolve o DataFrame no mesmo formato de `read()` e descarta os arquivos ao final. Aceita os mesmos filtros de `read()`, exceto `cadastro=`:

```python
# Download temporario, cache local intacto
df = bcb.cosif.fetch("2024-12", instituicao="60872504", escopo="prudencial")
df = bcb.ifdata.fetch("2024-12", conta="Lucro Liquido", verbose=False)
```

### 3. Buscar instituicao

A biblioteca usa o padrao "search + select" para identificar instituicoes:

```python
# Buscar instituicao por nome (fuzzy matching)
bcb.cadastro.search("Itau")
#    cnpj_8                 instituicao  situacao        fontes  score
# 0  60872504  ITAU UNIBANCO HOLDING S.A.        A  cosif,ifdata    100

bcb.cadastro.search("Bradesco")
bcb.cadastro.search("Santander")

# Listar todas as instituicoes com dados no COSIF
bcb.cadastro.search(fonte="cosif")

# Filtrar por fonte e escopo
bcb.cadastro.search(fonte="ifdata", escopo="prudencial")
```

O resultado retorna:

| Coluna | Descricao |
|--------|-----------|
| `cnpj_8` | CNPJ de 8 digitos (usar este valor nas consultas) |
| `instituicao` | Nome completo da instituicao |
| `situacao` | Status: A (Ativa) ou I (Inativa) |
| `fontes` | Fontes onde ha dados disponiveis para consulta (`cosif`, `ifdata`) |
| `score` | Score de similaridade (0-100, presente apenas com `termo`) |

Quando houver matches com e sem dados disponiveis, o `search()` prioriza os que possuem `fontes`.

### 4. Consultar dados

Use o CNPJ de 8 digitos nas consultas:

```python
# COSIF: start e OBRIGATORIO (posicional). instituicao e keyword-only e opcional
# start sozinho = data unica; start + end = range de datas
df = bcb.cosif.read(
    "2024-12", instituicao="60872504", conta="TOTAL GERAL DO ATIVO", escopo="prudencial"
)

# Bulk read: sem instituicao, retorna todas
df = bcb.cosif.read("2024-12", escopo="prudencial")

# IFDATA: start e OBRIGATORIO. instituicao e keyword-only e opcional
df = bcb.ifdata.read("2024-12", instituicao="60872504", conta="Lucro Liquido")

# Cadastro: start obrigatorio em read(), instituicao opcional
df = bcb.cadastro.read("2024-12", segmento="Banco Multiplo")
```

## Conceitos Fundamentais

### CNPJ de 8 Digitos

A biblioteca usa CNPJ de 8 digitos (base do CNPJ, sem filial e digito verificador) como identificador unico de instituicoes. Este formato evita ambiguidades entre filiais e garante consistencia entre as fontes.

Voce nao precisa normalizar antes de consultar. Sao aceitos a base de 8 digitos
e o CNPJ completo de 14, com ou sem formatacao -- os quatro valores abaixo
consultam a mesma instituicao:

```python
bcb.cosif.read("2024-12", instituicao="60872504")
bcb.cosif.read("2024-12", instituicao="60.872.504")
bcb.cosif.read("2024-12", instituicao="60872504000123")
bcb.cosif.read("2024-12", instituicao="60.872.504/0001-23")
```

No CNPJ completo os digitos verificadores sao conferidos antes do truncamento,
entao um numero digitado errado levanta `InvalidIdentifierError` em vez de
consultar uma instituicao que nao existe. Valores com menos de 8 digitos sao
rejeitados: sem zero a esquerda, `"1234567"` viraria um CNPJ diferente e
plausivel.

```python
# Correto: start posicional, instituicao keyword-only
bcb.cosif.read("2024-12", instituicao="60872504", escopo="prudencial")

# Correto: lista de instituicoes
bcb.cosif.read("2024-12", instituicao=["60872504", "60746948"])

# Correto: bulk read (sem instituicao)
bcb.cosif.read("2024-12", escopo="prudencial")

# ERRO: nome direto gera InvalidIdentifierError
bcb.cosif.read("2024-12", instituicao="Itau")  # Erro!

# ERRO: sem start gera MissingRequiredParameterError
bcb.cosif.read(start=None)  # Erro!
```

**Sempre use `bcb.cadastro.search()` para encontrar o CNPJ correto antes de fazer consultas.**

### Escopos COSIF

O COSIF tem dois escopos que representam visoes diferentes dos dados:

| Escopo | Descricao | Quando Usar |
|--------|-----------|-------------|
| `individual` | Dados de cada instituicao separadamente | Analise de instituicoes especificas |
| `prudencial` | Dados consolidados do conglomerado | Analise de grupos financeiros |

```python
# Escopo prudencial (conglomerado)
df = bcb.cosif.read("2024-12", instituicao="60872504", escopo="prudencial")

# Escopo individual
df = bcb.cosif.read("2024-12", instituicao="60872504", escopo="individual")

# Buscar em AMBOS os escopos (escopo=None, padrao)
# Retorna coluna escopo indicando a origem
df = bcb.cosif.read("2024-12", instituicao="60872504")
```

### Escopos IFDATA

O IFDATA tem tres escopos:

| Escopo | Tipo | Descricao |
|--------|------|-----------|
| `individual` | 3 | Instituicao individual |
| `prudencial` | 1 | Conglomerado prudencial |
| `financeiro` | 2 | Conglomerado financeiro |

```python
# Escopo individual
df = bcb.ifdata.read("2024-12", instituicao="60872504", escopo="individual")

# Buscar em TODOS os escopos (escopo=None, padrao)
df = bcb.ifdata.read("2024-12", instituicao="60872504")
```

### Periodicidade

| Fonte | Periodicidade | Formato |
|-------|---------------|---------|
| COSIF | Mensal | YYYYMM (ex: 202412) |
| IFDATA | Trimestral | YYYYMM (03, 06, 09, 12) |
| Cadastro | Trimestral | YYYYMM (03, 06, 09, 12) |

### Formato de Datas

A biblioteca aceita datas nos formatos:

```python
from datetime import date, datetime
import pandas as pd

# Formatos aceitos para start/end
start = "2024-12"  # String YYYY-MM
start = "202412"  # String YYYYMM
start = 202412  # Inteiro YYYYMM
start = date(2024, 12, 1)  # date do Python
start = datetime(2024, 12, 1)  # datetime do Python
start = pd.Timestamp("2024-12-01")  # pd.Timestamp
```

Comportamento:

- **start sozinho**: Data unica
- **start + end**: Range de datas (a biblioteca gera automaticamente os periodos)

`read()` devolve a data como `DatetimeIndex` nomeado `date` (nao existe coluna `data` no resultado) -- `df.loc["2024"]` e `df.resample()` funcionam direto. Metodos flat como `list_values()`, `list_contas()`, `search()` e `mapeamento()` devolvem a coluna `data` (tipo `datetime64`) quando solicitada.

## Exemplos Praticos

### Ativo Total de uma Instituicao

```python
import ifdata_bcb as bcb

# Buscar CNPJ
bcb.cadastro.search("Bradesco")
# CNPJ do Bradesco: 60746948

# Consultar Ativo Total
df = bcb.cosif.read(
    "2024-12", instituicao="60746948", conta="TOTAL GERAL DO ATIVO", escopo="prudencial"
)
print(f"Ativo Total: R$ {df['valor'].iloc[0]:,.2f}")
```

### Comparar Bancos

```python
import pandas as pd
import ifdata_bcb as bcb

# CNPJs dos maiores bancos
bancos = {
    "Itau": "60872504",
    "Bradesco": "60746948",
    "Santander": "90400888",
    "BB": "00000000",
}

# Coletar Ativo Total de cada banco
resultados = []
for nome, cnpj in bancos.items():
    df = bcb.cosif.read(
        "2024-12", instituicao=cnpj, conta="TOTAL GERAL DO ATIVO", escopo="prudencial"
    )
    if not df.empty:
        resultados.append({"Banco": nome, "Ativo": df["valor"].iloc[0]})

pd.DataFrame(resultados).sort_values("Ativo", ascending=False)
```

### Serie Temporal

```python
import matplotlib.pyplot as plt
import ifdata_bcb as bcb

# Evolucao do Patrimonio Liquido do Itau em 2024
df = bcb.cosif.read(
    "2024-01",
    "2024-12",
    instituicao="60872504",
    conta="PATRIMONIO LIQUIDO",
    escopo="prudencial",
)

# Plotar (o DatetimeIndex 'date' vira o eixo x automaticamente)
df["valor"].plot(kind="line")
plt.title("Patrimonio Liquido - Itau Unibanco")
plt.ylabel("R$")
plt.show()
```

### Explorar Dados Disponiveis

```python
import ifdata_bcb as bcb

# Listar relatorios IFDATA disponiveis
bcb.ifdata.list_values(["relatorio"])

# Listar segmentos do cadastro
bcb.cadastro.list_values(["segmento"])

# Listar UFs com filtro
bcb.cadastro.list_values(["uf"], situacao="A")

# Listar combinacoes de data + escopo no COSIF
bcb.cosif.list_values(["data", "escopo"])

# Listar municipios de SP
bcb.cadastro.list_values(["municipio"], uf="SP")

# Buscar contas por termo (keyword-only apos termo)
bcb.cosif.list_contas("ativo", escopo="prudencial")
```

### Enriquecimento Cadastral Inline

Em vez de consultar cadastro separadamente e fazer merge manual, use o parametro `cadastro` em `cosif.read()` ou `ifdata.read()`:

```python
# Sem cadastro inline (3 passos)
df = bcb.ifdata.read("2024-12", instituicao="60872504", escopo="prudencial")
df_cad = bcb.cadastro.read("2024-12", instituicao="60872504")
df = df.merge(df_cad[["cnpj_8", "tcb", "segmento"]], on="cnpj_8", how="left")

# Com cadastro inline (1 passo)
df = bcb.ifdata.read(
    "2024-12", instituicao="60872504", escopo="prudencial", cadastro=["tcb", "segmento"]
)
```

O alinhamento temporal e automatico: para dados mensais (COSIF), cada mes recebe os atributos do trimestre mais recente.

### Consultas SQL com DuckDB

Para analises mais complexas, use o `QueryEngine` diretamente:

```python
from ifdata_bcb.infra import QueryEngine

qe = QueryEngine()

# {cache} e substituido automaticamente pelo diretorio de cache
df = qe.sql("""
    SELECT
        CNPJ_8,
        NOME_INSTITUICAO as INSTITUICAO,
        NOME_CONTA as CONTA,
        SALDO as VALOR
    FROM '{cache}/cosif/prudencial/*.parquet'
    WHERE DATA_BASE = 202412
      AND NOME_CONTA = 'TOTAL GERAL DO ATIVO'
    ORDER BY SALDO DESC
    LIMIT 10
""")
```

**Nota:** As colunas no Parquet usam os nomes originais do BCB (DATA_BASE, NOME_INSTITUICAO, SALDO, etc). O mapeamento para os nomes canonicos lowercase (data, instituicao, valor) e feito apenas pelos explorers.

## Armazenamento de Dados

### Localizacao do Cache

Os dados coletados sao armazenados localmente em formato Parquet:

| Sistema | Caminho Padrao |
|---------|----------------|
| Windows | `%LOCALAPPDATA%\py-bacen\Cache\` |
| Linux | `~/.cache/py-bacen/` |
| macOS | `~/Library/Caches/py-bacen/` |

### Estrutura de Diretorios

```
py-bacen/
  Cache/
    cosif/
      individual/     # cosif_ind_YYYYMM.parquet
      prudencial/     # cosif_prud_YYYYMM.parquet
    ifdata/
      valores/        # ifdata_val_YYYYMM.parquet
      cadastro/       # ifdata_cad_YYYYMM.parquet
  Logs/
    ifdata_YYYY-MM-DD.log
```

### Customizar Diretorio

Use a variavel de ambiente `BACEN_DATA_DIR` para mudar o diretorio de cache:

```powershell
# Windows PowerShell
$env:BACEN_DATA_DIR = "C:\dados\bcb"
```

```bash
# Linux/macOS
export BACEN_DATA_DIR="/dados/bcb"
```

### Verificar Periodos Disponiveis

```python
# Via explorers
bcb.cosif.list_periodos()  # Todos os periodos (ambos escopos)
bcb.cosif.list_periodos("individual")  # Apenas individual
bcb.cosif.has_data()  # True se tem dados
bcb.ifdata.describe("prudencial")  # Capacidades + periodos do escopo

# Via DataManager (mais baixo nivel)
from ifdata_bcb.infra import DataManager

dm = DataManager()
periodos = dm.get_periodos_disponiveis("cosif_prud", "cosif/prudencial")
print(f"Periodos: {periodos}")  # [(2024, 1), (2024, 2), ...]
```

### Limpar Cache

Para limpar dados coletados, delete os arquivos `.parquet` no diretorio de cache manualmente ou via script:

```python
from pathlib import Path
from ifdata_bcb.infra import get_settings

cache = get_settings().cache_path
# Deletar arquivo especifico
(cache / "cosif" / "prudencial" / "cosif_prud_202412.parquet").unlink()
```

## Tratamento de Erros

A biblioteca usa excecoes especificas para diferentes situacoes:

```python
from ifdata_bcb import (
    BacenAnalysisError,
    InvalidIdentifierError,
    MissingRequiredParameterError,
    InvalidScopeError,
    InvalidDateRangeError,
)

try:
    df = bcb.cosif.read("2024-12", instituicao="60872504")
except InvalidIdentifierError as e:
    print(f"CNPJ invalido: {e}")
except MissingRequiredParameterError as e:
    print(f"Parametro obrigatorio: {e}")
except BacenAnalysisError as e:
    # Captura qualquer erro da biblioteca
    print(f"Erro: {e}")
```

Tudo -- excecoes e warnings -- e importavel direto de `ifdata_bcb`; nao e
preciso conhecer o layout interno do pacote.

| Excecao | Quando ocorre |
|---------|---------------|
| `InvalidIdentifierError` | CNPJ invalido (nem base de 8 nem completo de 14 com DV valido) |
| `MissingRequiredParameterError` | Parametro obrigatorio nao fornecido (ex: `start`) |
| `InvalidScopeError` | Valor invalido em `escopo`, `fonte` ou `documento` |
| `InvalidColumnError` | Coluna invalida em `columns=`, `list_values()` ou `cadastro=` |
| `InvalidDateRangeError` | start > end |
| `InvalidDateFormatError` | Formato de data invalido |
| `PeriodUnavailableError` | Periodo nao disponivel na fonte (404) |
| `DataProcessingError` | Falha no processamento (parquet corrompido, erro de query) |
| `BacenAnalysisError` | Classe base para todos os erros |

Dados indisponiveis nao levantam excecao: `read()` devolve DataFrame vazio e
emite um warning explicando o motivo, para que um resultado parcial nao seja
perdido inteiro. Todos os warnings herdam de `BacenWarning`, entao um filtro
so silencia a biblioteca:

```python
import warnings
from ifdata_bcb import BacenWarning

warnings.simplefilter("ignore", BacenWarning)
```

| Warning | Quando ocorre |
|---------|---------------|
| `PartialDataWarning` | Resultado vazio ou incompleto, com o diagnostico do motivo |
| `IncompatibleEraWarning` | A query cruza uma renumeracao de plano contabil do BCB |
| `ScopeUnavailableWarning` | Escopo indisponivel para a entidade em parte do periodo |
| `ScopeMigrationWarning` | Relatorio mudou de escopo entre eras |
| `DroppedReportWarning` | Relatorio descontinuado a partir de uma era |
| `NullValuesWarning` | Entidade presente, mas com todos os valores NULL |
| `EmptyFilterWarning` | Filtro vazio (ex: `columns=[]`) |
| `TruncatedResultWarning` | Resultado cortado pelo `limit` |
| `BacenWarning` | Classe base para todos os warnings |

## Proximos Passos

- [Provider COSIF](providers/cosif.md) - API completa do COSIF
- [Provider IFDATA](providers/ifdata.md) - API completa do IFDATA
- [Provider Cadastro](providers/cadastro.md) - API completa do Cadastro
- [Consultas SQL](advanced/sql-queries.md) - SQL avancado com DuckDB
