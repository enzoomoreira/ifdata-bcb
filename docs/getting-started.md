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
print(f"bcb.cosif: {type(bcb.cosif).__name__}")      # COSIFExplorer
print(f"bcb.ifdata: {type(bcb.ifdata).__name__}")    # IFDATAExplorer
print(f"bcb.cadastro: {type(bcb.cadastro).__name__}")  # CadastroExplorer
print(f"bcb.search: {type(bcb.search).__name__}")    # function
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
START = '2024-01'
END = '2024-12'

# Coletar cadastro (necessario para busca por nome)
bcb.cadastro.collect(START, END)

# Coletar COSIF (ambos escopos por padrao: individual e prudencial)
bcb.cosif.collect(START, END)

# Coletar IFDATA
bcb.ifdata.collect(START, END)
```

A coleta baixa os dados do site do BCB e armazena localmente em formato Parquet. Esse processo so precisa ser feito uma vez por periodo (a menos que use `force=True`).

### 3. Buscar instituicao

A biblioteca usa o padrao "search + select" para identificar instituicoes:

```python
# Buscar instituicao por nome (fuzzy matching)
bcb.search('Itau')
#    CNPJ_8                       INSTITUICAO  SITUACAO                           FONTES  SCORE
# 0  60872504  ITAU UNIBANCO HOLDING S.A.           A                   cosif,ifdata    100

bcb.search('Bradesco')
bcb.search('Santander')
```

O resultado retorna:

| Coluna | Descricao |
|--------|-----------|
| `CNPJ_8` | CNPJ de 8 digitos (usar este valor nas consultas) |
| `INSTITUICAO` | Nome completo da instituicao |
| `SITUACAO` | Status: A (Ativa) ou I (Inativa) |
| `FONTES` | Fontes onde ha dados disponiveis para consulta (`cosif`, `ifdata`) |
| `SCORE` | Score de similaridade (0-100) |

Quando houver matches com e sem dados disponiveis, o `search()` prioriza os que possuem `FONTES`.

### 4. Consultar dados

Use o CNPJ de 8 digitos nas consultas:

```python
# COSIF: instituicao e start sao OBRIGATORIOS
# start sozinho = data unica; start + end = range de datas
df = bcb.cosif.read(
    instituicao='60872504',
    start='2024-12',
    conta='TOTAL GERAL DO ATIVO',
    escopo='prudencial'
)

# IFDATA: instituicao e start sao OBRIGATORIOS
df = bcb.ifdata.read(
    instituicao='60872504',
    start='2024-12',
    conta='Lucro Liquido'
)

# Cadastro: consultar info basica (start=None usa ultimo periodo)
info = bcb.cadastro.info('60872504')

# Cadastro: instituicao e start sao opcionais
df = bcb.cadastro.read(segmento='Banco Multiplo')
```

## Conceitos Fundamentais

### CNPJ de 8 Digitos

A biblioteca usa CNPJ de 8 digitos (base do CNPJ, sem filial e digito verificador) como identificador unico de instituicoes. Este formato evita ambiguidades entre filiais e garante consistencia entre as fontes.

```python
# Correto: CNPJ de 8 digitos com start obrigatorio
bcb.cosif.read(instituicao='60872504', start='2024-12', escopo='prudencial')

# Correto: lista de instituicoes
bcb.cosif.read(instituicao=['60872504', '60746948'], start='2024-12')

# ERRO: nome direto gera InvalidIdentifierError
bcb.cosif.read(instituicao='Itau', start='2024-12')  # Erro!

# ERRO: sem start gera MissingRequiredParameterError
bcb.cosif.read(instituicao='60872504', escopo='prudencial')  # Erro!
```

**Sempre use `bcb.search()` para encontrar o CNPJ correto antes de fazer consultas.**

### Escopos COSIF

O COSIF tem dois escopos que representam visoes diferentes dos dados:

| Escopo | Descricao | Quando Usar |
|--------|-----------|-------------|
| `individual` | Dados de cada instituicao separadamente | Analise de instituicoes especificas |
| `prudencial` | Dados consolidados do conglomerado | Analise de grupos financeiros |

```python
# Escopo prudencial (conglomerado)
df = bcb.cosif.read(instituicao='60872504', start='2024-12', escopo='prudencial')

# Escopo individual
df = bcb.cosif.read(instituicao='60872504', start='2024-12', escopo='individual')

# Buscar em AMBOS os escopos (escopo=None, padrao)
# Retorna coluna ESCOPO indicando a origem
df = bcb.cosif.read(instituicao='60872504', start='2024-12')
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
df = bcb.ifdata.read(instituicao='60872504', start='2024-12', escopo='individual')

# Buscar em TODOS os escopos (escopo=None, padrao)
df = bcb.ifdata.read(instituicao='60872504', start='2024-12')
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
# Formatos aceitos para start/end
start = '2024-12'      # String YYYY-MM
start = '202412'       # String YYYYMM
start = 202412         # Inteiro YYYYMM
```

Comportamento:

- **start sozinho**: Data unica
- **start + end**: Range de datas (a biblioteca gera automaticamente os periodos)

A coluna `DATA` retornada e sempre do tipo `datetime64[ns]`.

## Exemplos Praticos

### Ativo Total de uma Instituicao

```python
import ifdata_bcb as bcb

# Buscar CNPJ
bcb.search('Bradesco')
# CNPJ do Bradesco: 60746948

# Consultar Ativo Total
df = bcb.cosif.read(
    instituicao='60746948',
    conta='TOTAL GERAL DO ATIVO',
    start='2024-12',
    escopo='prudencial'
)
print(f"Ativo Total: R$ {df['VALOR'].iloc[0]:,.2f}")
```

### Comparar Bancos

```python
import pandas as pd
import ifdata_bcb as bcb

# CNPJs dos maiores bancos
bancos = {
    'Itau': '60872504',
    'Bradesco': '60746948',
    'Santander': '90400888',
    'BB': '00000000'
}

# Coletar Ativo Total de cada banco
resultados = []
for nome, cnpj in bancos.items():
    df = bcb.cosif.read(
        instituicao=cnpj,
        conta='TOTAL GERAL DO ATIVO',
        start='2024-12',
        escopo='prudencial'
    )
    if not df.empty:
        resultados.append({
            'Banco': nome,
            'Ativo': df['VALOR'].iloc[0]
        })

pd.DataFrame(resultados).sort_values('Ativo', ascending=False)
```

### Serie Temporal

```python
import matplotlib.pyplot as plt
import ifdata_bcb as bcb

# Evolucao do Patrimonio Liquido do Itau em 2024
df = bcb.cosif.read(
    instituicao='60872504',
    conta='PATRIMONIO LIQUIDO',
    start='2024-01',
    end='2024-12',
    escopo='prudencial'
)

# Plotar
df.plot(x='DATA', y='VALOR', kind='line')
plt.title('Patrimonio Liquido - Itau Unibanco')
plt.ylabel('R$')
plt.show()
```

### Informacoes Cadastrais

```python
import ifdata_bcb as bcb

# Info completa de uma instituicao (ultimo periodo)
info = bcb.cadastro.info('60872504')
print(f"Nome: {info['INSTITUICAO']}")
print(f"Segmento: {info['SEGMENTO']}")
print(f"UF: {info['UF']}")
print(f"Situacao: {info['SITUACAO']}")

# Listar segmentos disponiveis
bcb.cadastro.list_segmentos()

# Listar UFs
bcb.cadastro.list_ufs()
```

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

**Nota:** As colunas no Parquet usam os nomes originais do BCB (DATA_BASE, NOME_INSTITUICAO, SALDO, etc). O mapeamento de nomes (DATA, INSTITUICAO, VALOR) e feito apenas pelos explorers.

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
bcb.cosif.list_periods()                    # Todos os periodos (ambos escopos)
bcb.cosif.list_periods(source='individual')  # Apenas individual
bcb.cosif.has_data()                         # True se tem dados

# Via DataManager (mais baixo nivel)
from ifdata_bcb.infra import DataManager

dm = DataManager()
periodos = dm.get_available_periods('cosif_prud', 'cosif/prudencial')
print(f"Periodos: {periodos}")  # [(2024, 1), (2024, 2), ...]
```

### Limpar Cache

Para limpar dados coletados, delete os arquivos `.parquet` no diretorio de cache manualmente ou via script:

```python
from pathlib import Path
from ifdata_bcb.infra import get_settings

cache = get_settings().cache_path
# Deletar arquivo especifico
(cache / 'cosif' / 'prudencial' / 'cosif_prud_202412.parquet').unlink()
```

## Tratamento de Erros

A biblioteca usa excecoes especificas para diferentes situacoes:

```python
from ifdata_bcb import BacenAnalysisError, DataUnavailableError
from ifdata_bcb.domain.exceptions import (
    InvalidIdentifierError,
    MissingRequiredParameterError,
    InvalidScopeError,
    InvalidDateRangeError,
)

try:
    df = bcb.cosif.read(instituicao='60872504', start='2024-12')
except InvalidIdentifierError as e:
    print(f"CNPJ invalido: {e}")
except MissingRequiredParameterError as e:
    print(f"Parametro obrigatorio: {e}")
except BacenAnalysisError as e:
    # Captura qualquer erro da biblioteca
    print(f"Erro: {e}")
```

| Excecao | Quando ocorre |
|---------|---------------|
| `InvalidIdentifierError` | CNPJ invalido ou nome ao inves de CNPJ |
| `MissingRequiredParameterError` | Parametro obrigatorio nao fornecido (`instituicao` e `start` para COSIF/IFDATA; cadastro aceita ambos opcionais) |
| `InvalidScopeError` | Escopo invalido (ex: 'xyz') |
| `InvalidDateRangeError` | start > end |
| `DataUnavailableError` | Dados nao disponiveis para o CNPJ/escopo |
| `BacenAnalysisError` | Classe base para todos os erros |

## Proximos Passos

- [Provider COSIF](providers/cosif.md) - API completa do COSIF
- [Provider IFDATA](providers/ifdata.md) - API completa do IFDATA
- [Provider Cadastro](providers/cadastro.md) - API completa do Cadastro
- [Consultas SQL](advanced/sql-queries.md) - SQL avancado com DuckDB
