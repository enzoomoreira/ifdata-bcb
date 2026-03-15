# Inicio Rapido

Guia para comecar a usar o `ifdata-bcb` em analises de dados financeiros do Banco Central do Brasil.

## O que e o ifdata-bcb

O `ifdata-bcb` e uma biblioteca Python para coleta e exploracao de dados bancarios do Brasil, disponibilizados pelo Banco Central. A biblioteca fornece acesso a:

- **COSIF**: Plano Contabil das Instituicoes do Sistema Financeiro Nacional (dados mensais)
- **IFDATA**: Informacoes Financeiras Trimestrais (dados trimestrais)
- **Cadastro**: Metadados das instituicoes financeiras

### Casos de Uso

- Analise de balancos de bancos e instituicoes financeiras
- Comparacao de indicadores entre instituicoes
- Acompanhamento de evolucao temporal de contas contabeis
- Pesquisa academica em financas bancarias
- Due diligence e analise de credito

## Instalacao

### Requisitos

- Python 3.10 ou superior
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
print(f"bcb.cosif: {type(bcb.cosif).__name__}")
print(f"bcb.ifdata: {type(bcb.ifdata).__name__}")
print(f"bcb.cadastro: {type(bcb.cadastro).__name__}")
print(f"bcb.search: {type(bcb.search).__name__}")
print(f"bcb.sql: {type(bcb.sql).__name__}")
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

# Coletar COSIF (ambos escopos por padrao)
bcb.cosif.collect(START, END)

# Coletar IFDATA
bcb.ifdata.collect(START, END)
```

### 3. Buscar instituicao (search + select)

A biblioteca usa o padrao "search + select" para identificar instituicoes:

```python
# Buscar instituicao por nome
bcb.search('Itau')
#    CNPJ_8               INSTITUICAO                          FONTES  SCORE
# 0  60872504  ITAU UNIBANCO HOLDING S.A.  cadastro,cosif_ind,cosif_prud    100
```

O resultado retorna:
- **CNPJ_8**: CNPJ de 8 digitos (usar este valor nas consultas)
- **INSTITUICAO**: Nome completo da instituicao
- **FONTES**: Fontes onde a instituicao aparece
- **SCORE**: Score de similaridade (0-100)

### 4. Consultar dados

Use o CNPJ de 8 digitos nas consultas:

```python
# Consultar COSIF (instituicao e start sao OBRIGATORIOS)
# start sozinho = data unica; start + end = range
# escopo=None busca em todos os escopos
df = bcb.cosif.read(instituicao='60872504', start='2024-12', conta='TOTAL GERAL DO ATIVO', escopo='prudencial')

# Consultar IFDATA (instituicao e start sao OBRIGATORIOS)
df = bcb.ifdata.read(instituicao='60872504', start='2024-12', conta='Lucro Liquido')
```

## Conceitos Fundamentais

### CNPJ de 8 Digitos

A biblioteca usa CNPJ de 8 digitos (base do CNPJ, sem filial e digito verificador) como identificador unico de instituicoes. Este formato evita ambiguidades entre filiais e garante consistencia entre as fontes.

```python
# Correto: CNPJ de 8 digitos com start obrigatorio
bcb.cosif.read(instituicao='60872504', start='2024-12', escopo='prudencial')

# Errado: nome direto gera InvalidIdentifierError
bcb.cosif.read(instituicao='Itau', start='2024-12')  # Erro!

# Errado: sem start gera MissingRequiredParameterError
bcb.cosif.read(instituicao='60872504', escopo='prudencial')  # Erro!

# Aceita lista de instituicoes
bcb.cosif.read(instituicao=['60872504', '60746948'], start='2024-12')
```

Sempre use `bcb.search()` para encontrar o CNPJ correto antes de fazer consultas.

### Escopos COSIF

O COSIF tem dois escopos que representam visoes diferentes dos dados:

| Escopo | Descricao | Quando Usar |
|--------|-----------|-------------|
| `individual` | Dados de cada instituicao separadamente | Analise de instituicoes especificas |
| `prudencial` | Dados consolidados do conglomerado | Analise de grupos financeiros |

**Importante**: Os parametros `instituicao` e `start` sao **obrigatorios** em `bcb.cosif.read()`. O parametro `escopo=None` busca em **todos** os escopos.

```python
# Escopo prudencial (conglomerado)
df = bcb.cosif.read(instituicao='60872504', start='2024-12', escopo='prudencial')

# Escopo individual
df = bcb.cosif.read(instituicao='60872504', start='2024-12', escopo='individual')

# Buscar em todos os escopos (escopo=None)
df = bcb.cosif.read(instituicao='60872504', start='2024-12')
```

### Periodicidade

| Fonte | Periodicidade | Formato |
|-------|---------------|---------|
| COSIF | Mensal | YYYYMM (ex: 202412) |
| IFDATA | Trimestral | YYYYMM (03, 06, 09, 12) |
| Cadastro | Trimestral | YYYYMM (03, 06, 09, 12) |

### Formato de Datas

A biblioteca usa os parametros `start` e `end` para filtrar datas:

```python
# Data unica (start sozinho)
start='2024-12'

# Range de datas (start + end)
start='2024-01', end='2024-12'
```

A biblioteca gera automaticamente o range de datas apropriado:
- **COSIF**: Range mensal (202401, 202402, ..., 202412)
- **IFDATA/Cadastro**: Range trimestral (202403, 202406, 202409, 202412)

A coluna `DATA` retornada e do tipo `datetime64[ns]`, nao inteiro.

## Exemplos Praticos

### Ativo Total de uma Instituicao

```python
# Buscar CNPJ
bcb.search('Bradesco')
# CNPJ do Bradesco: 60746948

# Consultar Ativo Total
df = bcb.cosif.read(
    instituicao='60746948',
    conta=['TOTAL GERAL DO ATIVO'],
    start='2024-12',
    escopo='prudencial'
)
print(f"Ativo Total: R$ {df['VALOR'].iloc[0]:,.2f}")
```

### Comparar Bancos

```python
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
        conta=['TOTAL GERAL DO ATIVO'],
        start='2024-12',
        escopo='prudencial'
    )
    if not df.empty:
        resultados.append({
            'Banco': nome,
            'Ativo': df['VALOR'].iloc[0]
        })

import pandas as pd
pd.DataFrame(resultados).sort_values('Ativo', ascending=False)
```

### Serie Temporal

```python
# Evolucao do Patrimonio Liquido do Itau em 2024
df = bcb.cosif.read(
    instituicao='60872504',
    conta=['PATRIMONIO LIQUIDO'],
    start='2024-01',
    end='2024-12',
    escopo='prudencial'
)

# Plotar (se tiver matplotlib)
import matplotlib.pyplot as plt
df.plot(x='DATA', y='VALOR', kind='line')
plt.title('Patrimonio Liquido - Itau Unibanco')
plt.ylabel('R$')
plt.show()
```

### SQL Personalizado

Para analises mais complexas, use SQL diretamente:

```python
df = bcb.sql("""
    SELECT
        CNPJ_8,
        INSTITUICAO,
        CONTA,
        VALOR
    FROM '{cache}/cosif/prudencial/*.parquet'
    WHERE DATA = 202412
      AND CONTA = 'TOTAL GERAL DO ATIVO'
    ORDER BY VALOR DESC
    LIMIT 10
""")
```

## Armazenamento de Dados

### Localizacao do Cache

Os dados coletados sao armazenados localmente em formato Parquet:

| Sistema | Caminho |
|---------|---------|
| Windows | `%LOCALAPPDATA%\py-bacen\Cache\` |
| Linux | `~/.cache/py-bacen/` |
| macOS | `~/Library/Caches/py-bacen/` |

### Estrutura de Diretorios

```
py-bacen/
  Cache/
    cosif/
      individual/     # Arquivos cosif_ind_YYYYMM.parquet
      prudencial/     # Arquivos cosif_prud_YYYYMM.parquet
    ifdata/
      valores/        # Arquivos ifdata_val_YYYYMM.parquet
      cadastro/       # Arquivos ifdata_cad_YYYYMM.parquet
  Logs/
    ifdata_YYYY-MM-DD.log
```

### Customizar Diretorio

Use a variavel de ambiente `BACEN_DATA_DIR` para mudar o diretorio de cache:

```bash
# Windows PowerShell
$env:BACEN_DATA_DIR = "C:\dados\bcb"

# Linux/macOS
export BACEN_DATA_DIR="/dados/bcb"
```

### Limpar Cache

Para limpar dados coletados, delete os arquivos `.parquet` no diretorio de cache:

```python
from ifdata_bcb.infra import DataManager

dm = DataManager()
# Deletar arquivo especifico
dm.delete('cosif_prud_202412', 'cosif/prudencial')
```

Ou delete o diretorio inteiro manualmente.

### Verificar Periodos Disponiveis

```python
from ifdata_bcb.infra import DataManager

dm = DataManager()

# Ultimo periodo disponivel
ultimo = dm.get_last_period('cosif_prud', 'cosif/prudencial')
print(f"Ultimo periodo: {ultimo}")  # (2024, 12)

# Todos os periodos
periodos = dm.get_available_periods('cosif_prud', 'cosif/prudencial')
print(f"Periodos: {periodos}")
```

## Proximos Passos

- [Provider COSIF](providers/cosif.md) - Detalhes do COSIF
- [Provider IFDATA](providers/ifdata.md) - Detalhes do IFDATA
- [Provider Cadastro](providers/cadastro.md) - Metadados das instituicoes
- [Consultas SQL](advanced/sql-queries.md) - SQL avancado com DuckDB
