# Changelog

## [1.0.0] - 2026-08-14

Primeiro major. Todas as quebras de contrato planejadas saem juntas, sem
camada de transicao: a API antiga deixa de existir e este guia e a referencia
de migracao. Quem fixou `ifdata-bcb<1` nao e afetado.

### Guia de migracao

| Antes (0.6.0) | Agora (1.0.0) |
|---|---|
| `df["DATA"]` | `df.index` (DatetimeIndex `date`) |
| `df["VALOR"]`, `df["CNPJ_8"]`, ... | `df["valor"]`, `df["cnpj_8"]`, ... (lowercase) |
| `explorer.list([...])` | `explorer.list_values([...])` |
| `list_contas('x', 'individual')` | `list_contas('x', escopo='individual')` |
| `list_periodos(source)` / `describe(source)` | `list_periodos(escopo)` / `describe(escopo)` |
| `describe()["by_source"]` | `describe()["by_escopo"]` |
| `cadastro=["SEGMENTO"]` | `cadastro=["segmento"]` |

### Alterado

**Colunas canonicas em lowercase.** Todo DataFrame devolvido pela API publica
(`read()`, `list_values()`, `list_contas()`, `search()`, `mapeamento()`) usa
nomes minusculos: `data`, `cnpj_8`, `instituicao`, `escopo`, `cod_conta`,
`conta`, `valor`, `fontes`, `score`, etc. Os inputs acompanham: `columns=` e
`cadastro=` aceitam os nomes novos (alem dos nomes de storage originais do
BCB, que nao mudam nos parquets).

**`read()` devolve `DatetimeIndex`.** A data sai das colunas e vira o index
datetime nomeado `date`, inclusive no DataFrame vazio -- o formato nao depende
de haver resultado. `df.loc["2024"]`, `resample()` e `plot()` funcionam
direto. `describe()` ganha `read_index` e `read_columns` deixa de listar a
data.

**`list()` virou `list_values()`.** O nome sombreava o builtin e nao dizia o
que listava; o novo descreve o `SELECT DISTINCT` real e se alinha a
`list_contas()`/`list_periodos()`. Assinatura e retorno identicos (com as
colunas agora em lowercase).

**`list_contas()` e keyword-only apos `termo`.** As ordens posicionais
divergiam entre explorers (`relatorio` na 3a posicao do IFDATA), entao
`list_contas('x', 'individual', '2024-01')` mudava de significado conforme o
explorer. Agora so `termo` e posicional, como `read()` e `search()`.

**`source` unificado em `escopo` na introspeccao.** `list_periodos()`,
`has_data()` e `describe()` falavam a lingua do storage (`source`, com
`'default'` fora do COSIF); agora falam a do dominio. No IFDATA a resposta vem
dos dados: um periodo so aparece para o escopo que tem linhas nele.
`describe()` troca `source`/`subdir`/`prefix`/`sources`/`by_source` por
`escopo`/`by_escopo` e para de vazar detalhes de armazenamento.

### Removido

**`DataUnavailableError`.** Nunca foi levantada por nenhum caminho da
biblioteca -- quem escrevia `except DataUnavailableError` tinha um handler
morto. Saiu do contrato publico na 0.6.0 com a promessa de remocao no major;
indisponibilidade continua sinalizada com `ScopeUnavailableWarning` mais
DataFrame vazio.

### Adicionado

**`fetch()` stateless nos tres explorers.** Baixa do BCB e devolve o
DataFrame no formato de `read()` sem tocar o cache local -- os arquivos vivem
num diretorio temporario descartado ao final. Aceita os mesmos filtros de
`read()`, exceto `cadastro=` (o enriquecimento exige o cadastro persistido).
Util para espiar um periodo sem se comprometer com uma coleta.

## [0.6.0] - 2026-08-14

Release de DX. Nada aqui muda o dado que a biblioteca devolve; muda o que ela
aceita, o que ela diz quando algo da errado e o que ela consegue contar sobre
si mesma. Alvo declarado: agentes que montam a chamada seguinte a partir do que
a lib responde.

### Adicionado

**`describe()` descreve a superficie de chamada, nao so o cache.** Alem dos
periodos coletados, passa a devolver `escopos` validos, `filtros` aceitos por
`read()`, `read_columns` (as colunas que `read()` devolve) e `cadastro_columns`
(as validas em `cadastro=`). E o suficiente para montar uma chamada sem
consultar a documentacao. `filtros` e derivado de `inspect.signature(read)`, e
nao de uma lista escrita a mao que envelheceria no primeiro parametro novo. O
retorno passa a ser tipado (`ExplorerInfo`, `TypedDict`) em vez de `dict` cru.

**`BacenWarning`, base de todos os warnings da lib.** Silenciar o que a
biblioteca emite passa a ser um filtro so:

```python
warnings.simplefilter("ignore", BacenWarning)
```

Antes era preciso listar as oito classes, e filtrar por `UserWarning` pegava
tambem o que vinha de outras bibliotecas. Todas continuam herdando de
`UserWarning`, entao quem ja filtrava assim nao muda.

**Excecoes e warnings no top-level.** As nove excecoes e os nove warnings ficam
importaveis direto de `ifdata_bcb`. Tratar erro ou filtrar warning deixa de
exigir conhecer `ifdata_bcb.domain.exceptions`. O import continua barato: o
modulo de excecoes nao importa nada, entao o lazy loading dos explorers segue
intacto (ha teste em subprocesso fixando isso).

**`collect()` aceita `end=None`**, coletando so o periodo de `start`, como
`read()` ja fazia. Nas fontes trimestrais isso corrige um caminho que falhava
em silencio: `collect('2024-07', '2024-07')` -- o workaround obvio, e o que os
docs sugeriam -- alinhava o inicio para 202409, que fica maior que o `end`, e
coletava zero periodos sem erro nenhum.

**CNPJ aceito como o usuario cola.** `instituicao=` passa a aceitar a base de 8
digitos e o CNPJ completo de 14, com ou sem formatacao: `'60872504'`,
`'60.872.504'`, `'60872504000123'` e `'60.872.504/0001-23'` consultam a mesma
instituicao. Nos 14 digitos os verificadores sao conferidos antes do
truncamento, entao um numero digitado errado levanta `InvalidIdentifierError`
em vez de consultar uma instituicao que nao existe. Valores com menos de 8
digitos continuam rejeitados: completar com zero a esquerda produziria um CNPJ
diferente e plausivel.

**Autocomplete no ponto de entrada.** `bcb.cosif`, `bcb.ifdata` e
`bcb.cadastro` sao resolvidos por `__getattr__`, entao Pyright e mypy os viam
como `Any` -- zero autocomplete de `read()`/`collect()`/`search()`. Declarados
sob `TYPE_CHECKING`, sem mudar o runtime.

**Datas tipadas como o runtime sempre aceitou.** As assinaturas diziam
`start: str`, mas `int`, `date`, `datetime` e `pd.Timestamp` ja funcionavam.
`bcb.cosif.read(202412)` deixa de ser erro no type checker do usuario.

**A promessa do `py.typed` virou verificavel.** `pyright` roda zerado sobre
`src/` e passa a ser gate de CI, junto com a suite rodando tambem em
`windows-latest`, um cron semanal dos testes de contrato contra a API real do
BCB (o maior risco sistemico da lib e o BCB mudar schema em silencio) e a
publicacao no PyPI condicionada a lint, type check e testes.

### Corrigido

**Escritas concorrentes na mesma cache nao colidem mais.** Dois `collect()` do
mesmo periodo -- threads ou dois processos -- disputavam o mesmo arquivo
temporario da escrita atomica e, no Windows, falhavam com `WinError 32`; o
vencedor da corrida de rename tambem podia derrubar o outro com `WinError 5`
transitorio. O temporario agora e unico por escritor e o rename do destino tem
retry curto: vale last-writer-wins, nunca um arquivo hibrido ou truncado.

**Mensagem corrompida em `documento` invalido.** `cosif.read(documento='abc')`
respondia `"Escopo 'abc' invalido. Validos: 'v', 'a', 'l', 'o', 'r', ..."` --
uma `str` chegava onde se esperava `list[str]` e era iterada caractere a
caractere. Agora explica o formato e aponta `cosif.list(['DOCUMENTO'])`, e
nomeia o elemento culpado quando a entrada e uma lista.

**`source` invalido levantava `KeyError` cru**, fora de `BacenAnalysisError`,
em `list_periodos()` e `describe()`. Passa a levantar `InvalidScopeError`, e
quando o valor passado e um escopo a mensagem diz isso -- `ifdata.list_periodos('individual')`
agora responde que `individual` e escopo e sugere `escopo='individual'`.

**`cadastro.read()` sem resultado nao dizia nada.** COSIF e IFDATA ja
diagnosticavam (falta `collect()`? filtro nao casou?); o cadastro devolvia
vazio em silencio. Aplicada a mesma cascata.

**`dir(bcb)` vazava internos** -- `Any`, `logger`, `TYPE_CHECKING`, `_cosif`,
`_ifdata`, `_cadastro` apareciam junto da API publica.

**Mensagens de `InvalidScopeError` nomeiam o parametro.** A classe atende
`escopo`, `fonte`, `source` e `documento`, mas o texto comecava com "Escopo"
fixo -- errado em tres dos quatro casos.

### Alterado

**Coluna invalida levanta `InvalidColumnError`, nao `InvalidScopeError`.**
Vale para `read(columns=...)` e para `cadastro=`; `list()` ja usava a excecao
certa, entao a mesma falha levantava tipos diferentes conforme o metodo. Quem
capturava `InvalidScopeError` nesses dois casos precisa trocar para
`InvalidColumnError`. Ambas herdam de `BacenAnalysisError`, entao quem captura
a base nao e afetado.

**`domain/validation.py` expoe funcoes, nao modelos Pydantic.**
`NormalizedDates`, `ValidatedCnpj8`, `InstitutionList` e `AccountList` viraram
`normalize_dates()`, `validate_cnpj8()`, `normalize_institutions()` e
`normalize_accounts()`. O `__init__` gerado pelo Pydantic e tipado com a
anotacao pos-validacao, entao todo uso dos validators `mode="before"` era um
erro de type check por construcao; os modelos ja se comportavam como funcoes
(todo call site desembrulhava `.values` na hora) e nenhuma `ValidationError`
era capturada. Modulo interno -- quem importava os modelos diretamente troca
pela funcao equivalente.

### Removido

**`DataUnavailableError` sai do contrato publico.** Estava exportada e
documentada, mas nenhum caminho da biblioteca a levantava -- quem escreveu
`except DataUnavailableError` tem um handler que nunca executou. Escopo
indisponivel para uma entidade continua sinalizado com
`ScopeUnavailableWarning` mais DataFrame vazio, para que um resultado parcial
nao se perca inteiro. A classe ainda existe em `ifdata_bcb.domain.exceptions` e
sera removida na v1.0.0. Ha teste garantindo que toda excecao exportada tem
`raise` em algum lugar do `src/`.

## [0.5.0] - 2026-08-04

A v0.4.2 nunca chegou ao PyPI. O conteudo dela esta aqui: o ultimo release
publicado e a v0.4.1, e quem atualiza a partir dela recebe tudo o que segue.

### Seguranca

**Queries DuckDB passam a ser parametrizadas.** Os valores de filtro nao entram
mais no texto da query: cada um vira um parametro nomeado (`$p0`, `$p1`, ...) e
viaja separado, ate o bind do DuckDB. Nao ha mais escape de aspas em lugar
nenhum -- e essa a garantia, porque sem escape nao ha ordem de escape para
errar. Elimina a classe inteira do bug de injecao, e nao apenas o vetor NFKD
corrigido logo abaixo.

As funcoes de `infra/sql.py` devolvem `SqlCondition`, subclasse de `str` que
carrega o fragmento com placeholders e, em `.params`, os valores. Interpolar o
fragmento por f-string descarta os params, entao quem monta SQL a mao precisa
passar `params=merge_params(...)` -- esquecer nao devolve resultado errado em
silencio, o DuckDB recusa a query com placeholder sem valor.

Um placeholder por valor, e nao `IN (SELECT unnest($lista))`: medido contra 12
parquets de 200 mil linhas, a forma com subquery perde o filter pushdown
(15 ms contra 11 ms) e degrada com listas grandes, enquanto um placeholder por
valor mantem o pushdown e o tempo dos literais mesmo com 5 mil valores.

Quem escreve explorers proprios seguindo `docs/advanced/extending.md` continua
com `where=join_conditions(conditions)` funcionando sem mudanca. Codigo que
interpolava fragmentos em SQL proprio precisa passar os params.

**Injecao SQL via normalizacao Unicode** (`infra/sql.py`): o escape de aspas era
aplicado antes da normalizacao NFKD. Como NFKD decompoe compatibilidade e nao
apenas acentos, `U+FF07` (FULLWIDTH APOSTROPHE) virava uma aspa simples ASCII
depois do escape, fechando o literal SQL. Alcancavel por `read(conta=...)`,
`list_contas()` e `search()`.

### Alterado

**Deteccao de era passa a medir o dado retornado.** A logica anterior decidia a
priori, pelo range solicitado e por duas tabelas hardcoded de relatorios
("estaveis" e "descontinuados"). `diagnose_eras()` agora compara os conjuntos de
codigos de conta dos dois lados do boundary no proprio resultado, por relatorio
(IFDATA) ou documento (COSIF). As tabelas param de decidir se ha warning e
passam apenas a explicar a causa quando ela e conhecida.

Casos que a versao anterior errava, verificados contra os parquets de 202412 e
202503:

- **Relatorio introduzido pelo BCB nao era sinalizado.** `Carteira de credito
  ativa - por carteiras de instrumentos financeiros` so existe a partir de
  202503; como o nome casava o prefixo de credito, era classificado como estavel
  e a query cruzando o boundary retornava metade da serie sem warning nenhum.
- **Bulk com escopo filtrado nao avisava sobre a migracao.** Sem `relatorio=`,
  o `ScopeMigrationWarning` nunca era emitido -- `read(escopo='financeiro')`
  cruzando o boundary perdia todo o credito pos-202503 em silencio.
- **Falso positivo quando o resultado nao cruzava o boundary.** O warning saia
  pelo range pedido, mesmo que o cache ou os filtros restringissem o resultado a
  uma unica era.

A classificacao dos 15 relatorios permanece identica a anterior nos casos em que
a anterior acertava. O limiar de estabilidade e 90% de codigos em comum, medido
por grupo e disponivel no diagnostico.

**Mensagem do `IncompatibleEraWarning` reescrita.** Nao ha colisao de codigos
entre as eras do COSIF (8 digitos contra 10), entao o risco nao e mistura e sim
descontinuidade: a serie de cada conta termina no boundary e recomeca com outro
codigo. A mensagem agora diz isso e inclui o overlap medido.

**Novo `PartialDataWarning` com `reason="era_coverage_gap"`** para lacunas sem
causa conhecida -- relatorios introduzidos pelo BCB e cache incompleto. Warnings
de mesma causa saem agregados: um resultado bulk cruzando o boundary emite um
warning por causa, nao um por relatorio.

### Adicionado

**`df.attrs["era"]` no retorno de `read()`** com o diagnostico estruturado
(`EraDiagnostic`, um `TypedDict` serializavel para JSON). Os warnings do Python
sao deduplicados e nao viajam com o DataFrame; o diagnostico resolve os dois
problemas para consumo programatico.

**`explorer.check_era(start, end, *, escopo=None)`** nos explorers COSIF e
IFDATA: retorna o mesmo `EraDiagnostic` lendo apenas as colunas de dimensao,
sem trazer valores. Util para decidir como montar a query -- ou para recuperar
o diagnostico quando `attrs` se perde no caminho.

`EraDiagnostic` e `GrupoEra` exportados no top-level.

`ClassVar` nos class attributes de configuracao dos explorers e collectors, e
`strict=True` em todos os `zip()` -- todos iteram colunas do mesmo DataFrame,
entao o comprimento e igual por construcao e o `strict` passa a verificar esse
invariante em vez de truncar em silencio.

### Build

**Ruff pinado e rule set declarado no `pyproject.toml`.** O CI rodava
`uvx ruff check .` sem versao, e o projeto nao tinha secao `[tool.ruff]`:
herdava o rule set default de qualquer ruff que o uvx resolvesse naquele dia.
O default do ruff 0.16 e muito mais amplo que o de quando o CI foi escrito, e o
job passou a falhar sozinho, sem nenhuma mudanca de codigo. O ruff entra no
dependency group dev com pin exato e o CI usa `uv run --frozen ruff`, entao dev
e CI compartilham o mesmo binario e subir versao vira um PR deliberado.

### BREAKING CHANGES

**`configure_logging()` e `set_log_level()` substituidas por `enable_logging()` e
`disable_logging()`.** A biblioteca nao emite mais logs por padrao.

`configure_logging()` chamava `logger.remove()` no logger global do loguru, que
pertence a aplicacao consumidora, e era disparada implicitamente por
`get_logger()` -- presente no `__init__` de `QueryEngine`, `EntityLookup` e
`DataManager`. Bastava instanciar a API publica para a aplicacao perder os
proprios sinks de logging. Tambem criava arquivo de log em disco sem ser pedido.

```python
# Antes (v0.4.1) -- logging ativo por padrao, destruindo os sinks da app
from ifdata_bcb.infra.log import configure_logging, set_log_level

set_log_level("DEBUG")

# Agora (v0.5.0) -- opt-in explicito
import ifdata_bcb as bcb

bcb.enable_logging(level="DEBUG", to_file=True)
bcb.disable_logging()
```

**Coleta IFDATA falha o periodo inteiro se qualquer tipo de instituicao falhar.**
Antes, o periodo era gravado se ao menos 1 dos 3 tipos tivesse sucesso, marcado
como SUCCESS. Como a deteccao de "periodo ja coletado" e feita por nome de
arquivo, o dado incompleto nunca era reparado sem `force=True`. Coletas que antes
"passavam" parcialmente agora falham de forma explicita.

### Corrigido

- **Escrita de Parquet nao-atomica** (`infra/storage.py`): interrupcao no meio da
  escrita deixava arquivo truncado com o nome definitivo, tratado como periodo
  coletado para sempre. Passa a escrever em `.tmp` e mover com `os.replace`.
  `collect()` limpa `.tmp` orfaos de execucoes anteriores.
- **Retry em respostas 4xx** (`infra/resilience.py`): `httpx.HTTPError` na lista
  de excecoes transientes fazia com que um 404 de periodo inexistente fosse
  retentado 3x com backoff contra a API do BCB. `ValueError` generico tambem
  estava na lista, mascarando bugs de logica. Substituidos pelo predicado
  `is_retryable` (transporte, 5xx e 429).

### Removido

`check_era_boundary()` e `check_ifdata_era()`, substituidas por
`diagnose_eras()` + `emit_era_warnings()`. `_STABLE_REPORTS_NORMALIZED` e
`_is_stable_report()` deixam de existir -- o overlap medido os substitui.

`escape_sql_string()`, sem uso apos a parametrizacao. `build_in_clause()` perde
o parametro `escape`, que deixou de ter sentido.

Dependencia `ipywidgets`, declarada mas sem nenhum import no codigo.
`ui/display.py` usa Rich com `force_jupyter=False` deliberadamente.

## [0.4.0] - 2026-03-26

Refatoracao arquitetural com mudancas de API, migracao de HTTP client, novos metodos de consulta e otimizacoes de performance.

### BREAKING CHANGES

**Assinatura de `read()` alterada em todos os providers:**
- `start` agora e o primeiro argumento posicional (antes vinha depois de `instituicao`)
- `instituicao` agora e keyword-only e opcional (antes era posicional e obrigatorio em IFDATA/COSIF)
- Permite bulk reads sem filtro de instituicao (`instituicao=None` retorna todas)

```python
# Antes (v0.3.0)
df = bcb.ifdata.read("60872504", "2024-12")
df = bcb.cosif.read("60872504", "2024-12")

# Agora (v0.4.0)
df = bcb.ifdata.read("2024-12", instituicao="60872504")
df = bcb.cosif.read("2024-12", instituicao="60872504")
df = bcb.ifdata.read("2024-12")  # bulk: todas as instituicoes
```

**`cadastro.read()`: `start` agora e obrigatorio** (antes era opcional com fallback para ultimo periodo).

**Metodos renomeados (ingles -> portugues):**

| v0.3.0 | v0.4.0 |
|--------|--------|
| `list_periods()` | `list_periodos()` |
| `list_accounts()` | `list_contas()` |
| `list_mapeamento()` | `mapeamento()` |

**Metodos removidos:**

| Removido | Substituto |
|----------|------------|
| `bcb.search()` | `bcb.cadastro.search()` |
| `cadastro.info()` | `cadastro.read()` |
| `cadastro.list_segmentos()` | `cadastro.list(["SEGMENTO"])` |
| `cadastro.list_ufs()` | `cadastro.list(["UF"])` |
| `cadastro.get_conglomerate_members()` | `ifdata.mapeamento()` |
| `ifdata.list_institutions()` | `cadastro.search(fonte='ifdata')` |
| `ifdata.list_reporters()` | removido sem substituto |
| `ifdata.list_reports()` | `ifdata.list(["RELATORIO"])` |

**Excecoes removidas:** `EntityNotFoundError`, `AmbiguousIdentifierError`

**Dependencia HTTP:** `requests` + `urllib3` substituidos por `httpx>=0.28.0`

**Removidos:** dataclass `ScopeResolution`, modulo `domain/models.py`, funcao `yyyymm_to_datetime()`

### Added

- `cadastro.search(termo, *, fonte, escopo, start, end, limit)`: busca centralizada de instituicoes com fuzzy matching, filtros por fonte de dados (ifdata/cosif), escopo, e filtragem por disponibilidade de dados no periodo
- `list()` generico em todos os providers (IFDATA, COSIF, Cadastro): retorna valores distintos para colunas solicitadas via `SELECT DISTINCT` no DuckDB, com filtros categoricos e truncation warning
- `ifdata.mapeamento(start, end)`: acesso direto a tabela de mapeamento COD_INST <-> CNPJ_8 por escopo e periodo
- Bulk read (`instituicao=None`) em `cosif.read()` e `ifdata.read()`: retorna dados de todas as instituicoes sem resolver entidade
- Parametro `grupo` em `ifdata.read()` para filtrar por grupo de conta
- 6 novos filtros em `cadastro.read()`: `atividade`, `tcb`, `td`, `tc`, `sr`, `municipio`
- `check_ifdata_era()` em `core/eras.py`: verificacao de era especifica para IFDATA com logica por tipo de relatorio -- detecta relatorios descontinuados (`DroppedReportWarning`) e migracoes de escopo (`ScopeMigrationWarning`)
- 8 novas classes de warning estruturadas com atributos semanticos: `PartialDataWarning`, `ScopeUnavailableWarning`, `NullValuesWarning`, `ScopeMigrationWarning`, `DroppedReportWarning`, `EmptyFilterWarning`, `TruncatedResultWarning`
- `InvalidColumnError` para colunas invalidas em `list()`
- `DateScalar` type alias: `read()`, `collect()` e demais metodos aceitam `date`, `datetime` e `pd.Timestamp` nativos alem de int/str
- `stem_ptbr()` em `utils/text.py`: stemming PT-BR para busca singular/plural -- `list_contas()` usa para matching ("operacao" encontra "Operacoes")
- `infra/sql.py` com 7 funcoes de construcao SQL: `build_string_condition`, `build_int_condition`, `build_account_condition`, `build_like_condition`, `join_conditions`, `escape_sql_string`, `build_in_clause`
- `utils/nulls.py` com `is_valid()`: check escalar de nulidade sem pandas, compativel com None, NaN, `pd.NA` e `pd.NaT`
- `format_entity_labels()` em `utils/text.py` para formatacao de CNPJs com nomes em mensagens de warning
- `NOME_CONGL_PRUD` como coluna derivada no enrichment cadastral: nome da instituicao lider do conglomerado prudencial, resolvida via lookup SQL
- `describe()` agora inclui key `"columns"` com nomes aceitos pelo `list()`
- `providers/enrichment.py` com `enrich_with_cadastro()` e `validate_cadastro_columns()`: enriquecimento cadastral extraido como modulo independente
- `TemporalResolver` e `TemporalGroup` em `valores/temporal.py`: resolucao de CNPJs para codigos IFDATA por periodo com suporte a backfill/forward-fill
- `CadastroSearch` em `cadastro/search.py`: logica de busca extraida do explorer
- `EntitySearch` em `core/entity/search.py`: busca fuzzy isolada do lookup de metadados
- Novos metodos no BaseExplorer: `_validate_escopo()`, `_validate_columns()`, `_filter_columns()`, `_storage_columns_for_query()`, `_apply_canonical_names()`, `_check_null_value_instituicoes()`, `_diagnose_empty_result()`, `_ensure_data_exists()`

### Changed

**Arquitetura:**
- Provider IFDATA decomposto em sub-packages: `ifdata/cadastro/` (collector, explorer, search) e `ifdata/valores/` (collector, explorer, temporal)
- `EntityLookup` decomposto em pacote `core/entity/`: `lookup.py` (resolucao de metadados) e `search.py` (busca fuzzy via `EntitySearch`) -- responsabilidades separadas com dependencia unidirecional
- `CadastroExplorer.search()` extraido para `CadastroSearch` em `cadastro/search.py` -- explorer reduzido de ~570 para ~290 linhas
- `BaseExplorer` movido de `core/` para `providers/` (pertence a hierarquia de providers)
- `CollectStatus` movido de `collector_models.py` para `base_collector.py`
- HTTP client migrado de `requests` para `httpx` com connection pooling via `httpx.Client`, eliminando overhead de conexao TCP por request
- `TRANSIENT_EXCEPTIONS` simplificada: `requests.*` + `urllib3.*` consolidados em `httpx.HTTPError`
- `IFDATA_API_BASE` URL centralizada em `core/constants.py` (antes duplicada nos collectors)

**Performance:**
- Pipeline de finalizacao (`_finalize_read`) simplificado: dedup, conversao datetime e exclusao de colunas movidos para o DuckDB; pipeline pos-query reduzido a rename + sort + reorder
- Queries DuckDB consolidadas por batch de periodos em reads prudencial/financeiro -- antes cada conglomerado gerava um `read_glob` separado, agora grupos com mesmos periodos sao lidos em uma unica query (~2x mais rapido)
- Cache de nomes canonicos em `EntityLookup`: queries subsequentes com mesmos CNPJs retornam do cache sem hit no DuckDB
- `_search_without_termo()` usa arrays `.values` em vez de `.iterrows()` (~18x mais rapido)
- Null checks escalares (`pd.notna`/`pd.isna`) substituidos por `is_valid()` em pure Python (~2x mais rapido para valores DuckDB)
- Enrichment cadastral migrado de `pd.merge_asof` para ASOF LEFT JOIN via DuckDB SQL
- `TemporalResolver.resolve_mapeamento()` consolidado em uma unica query SQL (antes 3 metodos + manipulacao pandas)
- `_check_null_value_instituicoes()` usa operacoes vetorizadas com sets em vez de `groupby().apply()`
- Conversao redundante `pd.to_datetime()` em `_base_list()` removida (DuckDB ja retorna `datetime64[us]`)

**Logging:**
- Filosofia redefinida: log interno reduzido ~78%, removidos logs de parsing de datas e SQL de rotina
- Reads promovidos de DEBUG para INFO com output estruturado (ex: `COSIF read: escopo=prudencial -> 301 rows`)
- `emit_user_warning()` log level rebaixado de WARNING para DEBUG (reduz ruido para warnings que ja sao emitidos via `warnings.warn()`)
- Fix: log de enrichment usava printf-style (`%d/%d`) com loguru -- corrigido para f-string

**Outros:**
- CNPJ regex corrigido de `^\d+$` para `^\d{8}$` -- codigos de conglomerado numericos curtos nao sao mais tratados como CNPJ
- `FIRST_AVAILABLE_PERIOD` ampliado: `cosif_individual` recuado para 198807, `ifdata_valores` para 200003
- `EntityLookup.real_entity_condition()` tornado `@staticmethod`
- `_apply_canonical_names()` so atua quando INSTITUICAO nao existe no DataFrame
- `list_contas()` COSIF aplica dedup via `ROW_NUMBER() OVER (PARTITION BY CONTA ORDER BY DATA_BASE DESC)` para eliminar variantes de nome entre eras
- `InstitutionList.normalize_and_validate()` reutiliza `ValidatedCnpj8` em vez de duplicar regex
- Metodos publicos padronizados para portugues: `list_periods` -> `list_periodos`, `_normalize_dates` -> `_normalize_datas`, etc.
- Documentacao atualizada em 15 arquivos refletindo nova arquitetura

### Removed

**Arquivos deletados:**
- `core/api.py` (funcionalidade absorvida por `cadastro.search()`)
- `core/base_explorer.py` (movido para `providers/base_explorer.py`)
- `core/entity_lookup.py` (decomposto em `core/entity/`)
- `domain/models.py` (`ScopeResolution` removida)
- `providers/collector_models.py` (`CollectStatus` movido para `base_collector.py`)
- `providers/ifdata/collector.py` (dividido em `cadastro/collector.py` e `valores/collector.py`)
- `providers/ifdata/explorer.py` (movido para `valores/explorer.py`)
- `providers/ifdata/cadastro_explorer.py` (movido para `cadastro/explorer.py`)

**API removida:**
- `bcb.search()` do namespace top-level
- `cadastro.info()`, `cadastro.get_conglomerate_members()`, `cadastro.list_segmentos()`, `cadastro.list_ufs()`
- `ifdata.list_institutions()`, `ifdata.list_reporters()`, `ifdata.list_reports()`, `ifdata.list_mapeamento()`
- Excecoes `EntityNotFoundError` e `AmbiguousIdentifierError`
- `ScopeResolution` dataclass e `resolve_ifdata_escopo()`
- `yyyymm_to_datetime()` em `utils/date.py`
- Dependencias `requests` e `urllib3`

---

## [0.3.0] - 2026-03-15

Release inicial com suporte a COSIF multi-era, validacao Pydantic, enrichment cadastral e suite de testes.

### Added
- Modulo `core/eras.py` para deteccao e tratamento de multiplas eras de formato do BCB (Era 1: 1995-2010/09, Era 2: 2010/10-2024/12, Era 3: 2025/01+)
- `IncompatibleEraWarning` para alertar sobre combinacao de periodos com planos COSIF diferentes
- COSIF collector com suporte a todas as eras (antes crashava em CSVs Era 1)
- `union_by_name=true` no `QueryEngine.read_glob` para leitura defensiva de parquets heterogeneos
- Validacao de cutoff dates por fonte: periodos anteriores ao primeiro disponivel no BCB sao filtrados automaticamente
- `FIRST_AVAILABLE_PERIOD` com datas empiricas por fonte
- Colunas `COD_CONTA` e `DOCUMENTO` expostas no COSIF; `COD_CONTA` no IFDATA
- Filtro por codigo de conta numerico em `cosif.read(conta=)` e `ifdata.read(conta=)`
- Parametros `documento` (COSIF), `situacao` (Cadastro), `relatorio` (IFDATA `list_accounts()`)
- Parametro `cadastro` em `cosif.read()` e `ifdata.read()` para enriquecimento inline com atributos cadastrais
- `domain/validation.py` com validators Pydantic: `NormalizedDates`, `ValidatedCnpj8`, `InstitutionList`, `AccountList`
- `infra/config.py` com `Settings` via `pydantic-settings`
- `IFDATAExplorer`: metodos de introspeccao (`list_accounts`, `list_institutions`, `list_reporters`, `list_reports`)
- `EntityLookup`: resolucao canonica de entidades com suporte a CodInst
- Suite de testes QA (64 cenarios), unitarios (151), integracao (111) -- 326 total
- Testes de contrato BCB com health check dos endpoints reais

### Changed
- NOME_CONTA normalizado para UPPER em todas as eras
- `cadastro.read()`: `start` agora opcional com fallback para ultimo periodo
- Codebase migrada de `Optional[X]`/`Union[X, Y]` para sintaxe Python 3.12+
- Suite de testes reorganizada em `tests/unit/`, `tests/integration/`, `tests/qa/`
- `BaseExplorer`: validacao delegada para Pydantic
- `BaseCollector`: downloads isolados em `temp_dir`

### Fixed
- Filtros accent-insensitive: `'Lucro Liquido'` funciona igual a versao acentuada
- DataFrames vazios retornam colunas de apresentacao
- `cosif.read()` nao retorna mais duplicatas por DOCUMENTO
- `columns=` aceita nomes de apresentacao alem de storage
- `NormalizedDates` valida range de mes (1-12)
- String de ano puro (`"2024"`) levanta erro em vez de ser interpretada como YYYYMM
- Validacao de `cadastro` movida para inicio de `read()`
- CNPJ usa `[0-9]{8}` rejeitando digitos unicode fullwidth
- `import ifdata_bcb` reduzido de ~0.65s para ~0.017s via lazy loading
