# Camada de Dominio

A camada de dominio contem modelos de dados, tipos e excecoes da biblioteca.

## Localizacao

```
src/ifdata_bcb/domain/
|-- __init__.py           # Exports publicos
|-- exceptions.py        # Hierarquia de excecoes
|-- types.py             # Type aliases
+-- validation.py        # Pydantic models (NormalizedDates, ValidatedCnpj8, etc)
```

---

## exceptions.py

### Hierarquia

```
Exception
    +-- BacenAnalysisError (base)
    |       +-- InvalidScopeError
    |       +-- DataUnavailableError      (nao levantada; sai na v1.0.0)
    |       +-- InvalidIdentifierError
    |       +-- MissingRequiredParameterError
    |       +-- InvalidDateRangeError
    |       +-- InvalidDateFormatError
    |       +-- PeriodUnavailableError
    |       +-- DataProcessingError
    |       +-- InvalidColumnError
    +-- UserWarning
            +-- BacenWarning (base dos warnings da lib)
                    +-- IncompatibleEraWarning
                    +-- PartialDataWarning
                    +-- ScopeUnavailableWarning
                    +-- NullValuesWarning
                    +-- ScopeMigrationWarning
                    +-- DroppedReportWarning
                    +-- EmptyFilterWarning
                    +-- TruncatedResultWarning
            +-- TruncatedResultWarning
```

> **Nota:** `EntityNotFoundError` e `AmbiguousIdentifierError` foram removidas da hierarquia por nao terem call sites restantes.

### BacenAnalysisError

Excecao base para todos os erros da biblioteca:

```python
class BacenAnalysisError(Exception):
    """
    Permite capturar qualquer erro da biblioteca:

        try:
            df = bcb.cosif.read(...)
        except BacenAnalysisError as e:
            print(f"Erro: {e}")
    """

    pass
```

### InvalidScopeError

Valor invalido para um parametro de dominio fechado. Usada para `escopo`,
`fonte`, `source` e `documento` -- por isso a mensagem nomeia o parametro em
vez de dizer sempre "Escopo".

```python
class InvalidScopeError(BacenAnalysisError):
    def __init__(
        self,
        scope: str,
        value: str,
        valid_values: list[str],
        hint: str = "",
    ):
        self.scope = scope
        self.value = value
        self.valid_values = list(valid_values)
        self.hint = hint


# Uso
raise InvalidScopeError(
    scope="escopo",
    value="invalido",
    valid_values=["individual", "prudencial", "financeiro"],
)
# "Valor invalido para 'escopo': 'invalido'. Validos: 'individual', 'prudencial', 'financeiro'."
```

`valid_values` vazio omite a clausula "Validos:". Serve para parametros cujo
dominio nao e enumeravel a priori, onde `hint` explica o formato:

```python
raise InvalidScopeError(
    scope="documento",
    value="abc",
    valid_values=[],
    hint="Esperado codigo numerico (ex: 4010, 4016).",
)
# "Valor invalido para 'documento': 'abc'. Esperado codigo numerico (ex: 4010, 4016)."
```

### DataUnavailableError

Nao levantada por nenhum caminho da biblioteca. Escopo indisponivel para uma
entidade e sinalizado com `ScopeUnavailableWarning` mais DataFrame vazio, e
nao com excecao: o consumidor recebe os dados parciais junto do diagnostico em
vez de perder o resultado inteiro.

Saiu do contrato publico na v0.6.0 e a classe sera removida na v1.0.0.

### BacenWarning

Base dos warnings da biblioteca. Existe para que um unico filtro cubra tudo
que a lib emite:

```python
import warnings
from ifdata_bcb import BacenWarning

warnings.simplefilter("ignore", BacenWarning)
```

Continua herdando de `UserWarning`, entao quem ja filtrava por `UserWarning`
nao e afetado. Warnings de providers novos devem derivar dela.

### InvalidIdentifierError

Formato de identificador invalido:

```python
class InvalidIdentifierError(BacenAnalysisError):
    def __init__(self, identificador: str):
        self.identificador = identificador


# Uso
raise InvalidIdentifierError(identificador="Itau")
# Mensagem: "Identificador 'Itau' invalido. Esperado CNPJ de 8 digitos."
```

### MissingRequiredParameterError

Parametro obrigatorio ausente:

```python
class MissingRequiredParameterError(BacenAnalysisError):
    def __init__(self, param_name: str):
        self.param_name = param_name


# Uso
raise MissingRequiredParameterError(param_name="start")
# Mensagem: "Parametro obrigatorio ausente: 'start'."
```

### InvalidDateRangeError

Range de datas invalido (start > end):

```python
class InvalidDateRangeError(BacenAnalysisError):
    def __init__(self, start: str, end: str):
        self.start = start
        self.end = end


# Uso
raise InvalidDateRangeError(start="2024-12", end="2024-01")
# Mensagem: "Data inicial (2024-12) maior que data final (2024-01)."
```

### InvalidDateFormatError

Formato de data nao reconhecido:

```python
class InvalidDateFormatError(BacenAnalysisError):
    def __init__(self, value: str, detail: str = ""):
        self.value = value
        self.detail = detail


# Uso
raise InvalidDateFormatError(value="2024/12/01")
# Mensagem: "Formato de data invalido: '2024/12/01'."
```

### PeriodUnavailableError

Periodo nao disponivel no BCB (usado internamente na coleta):

```python
class PeriodUnavailableError(BacenAnalysisError):
    def __init__(self, period: int):
        self.period = period


# Uso interno
raise PeriodUnavailableError(period=202501)
# Mensagem: "Periodo 202501 indisponivel na fonte."
```

### DataProcessingError

Falha no processamento de dados de uma fonte (usado internamente nos collectors):

```python
class DataProcessingError(BacenAnalysisError):
    def __init__(self, source: str, detail: str = ""):
        self.source = source
        self.detail = detail


# Uso interno
raise DataProcessingError("cosif:prudencial", "Erro na leitura do CSV")
```

### IncompatibleEraWarning

Warning emitido quando uma query abrange periodos com codigos de conta incompativeis (pre/pos COSIF 1.5):

```python
class IncompatibleEraWarning(UserWarning):
    """Emitido quando uma query abrange periodos com codigos de conta incompativeis."""

    def __init__(self, message: str, boundary: int, source: str):
        self.boundary = boundary  # Periodo fronteira (ex: 202501)
        self.source = source  # Fonte (ex: "COSIF")


# Emitido automaticamente por emit_era_warnings() em core/eras.py, a partir do
# overlap de codigos de conta medido no proprio resultado.
# Exemplo: cosif.read('2024-12', '2025-01') emite este warning
```

Nao herda de `BacenAnalysisError` -- e um `UserWarning` capturavel via `warnings.catch_warnings()`:

```python
import warnings
from ifdata_bcb.domain.exceptions import IncompatibleEraWarning

with warnings.catch_warnings(record=True) as w:
    warnings.simplefilter("always")
    df = bcb.cosif.read("2024-12", "2025-01", instituicao="60872504")
    if w and issubclass(w[0].category, IncompatibleEraWarning):
        era_warning = w[0].message
        print(f"Boundary: {era_warning.boundary}, Source: {era_warning.source}")
```

### PartialDataWarning

Warning emitido quando o resultado pode estar incompleto -- por exemplo, quando alguns periodos ou entidades nao retornaram dados, ou quando uma query de leitura falha por incompatibilidade de schema:

```python
class PartialDataWarning(UserWarning):
    """Resultado incompleto - alguns periodos/entidades sem dados."""

    def __init__(self, message: str, reason: str = "", detail: dict | None = None):
        self.reason = reason  # Ex: "query_failed", "no_cnpj_for_enrichment"
        self.detail = detail
```

### ScopeUnavailableWarning

Warning emitido quando um escopo nao esta disponivel para uma entidade em parte do range temporal solicitado:

```python
class ScopeUnavailableWarning(UserWarning):
    """Escopo indisponivel para entidade em parte do range temporal."""

    def __init__(
        self, message: str, entities: list[str], escopo: str, periodos: list[int]
    ):
        self.entities = entities  # CNPJs afetados
        self.escopo = escopo  # Escopo indisponivel
        self.periodos = periodos  # Periodos afetados
```

### NullValuesWarning

Warning emitido quando uma entidade esta presente nos dados mas com todos os valores financeiros (VALOR) NULL. Ocorre quando o BCB registra a entidade no periodo mas nao fornece valores:

```python
class NullValuesWarning(UserWarning):
    """Entidade presente nos dados mas com todos os valores financeiros NULL."""

    def __init__(self, message: str, entities: list[str]):
        self.entities = entities  # CNPJs com valores NULL
```

### ScopeMigrationWarning

Warning emitido quando um relatorio migrou de escopo entre eras (ex: relatorios de credito migraram de `financeiro` para `prudencial` a partir de 202503):

```python
class ScopeMigrationWarning(UserWarning):
    """Relatorio migrou de escopo entre eras."""

    def __init__(
        self,
        message: str,
        relatorio: str,
        escopo_pre: str,
        escopo_post: str,
        boundary: int,
    ):
        self.relatorio = relatorio  # Nome do relatorio afetado
        self.escopo_pre = escopo_pre  # Escopo antes do boundary (ex: "financeiro")
        self.escopo_post = escopo_post  # Escopo apos o boundary (ex: "prudencial")
        self.boundary = boundary  # Periodo boundary (ex: 202503)
```

### DroppedReportWarning

Warning emitido quando um relatorio foi descontinuado a partir de determinada era:

```python
class DroppedReportWarning(UserWarning):
    """Relatorio descontinuado a partir de determinada era."""

    def __init__(self, message: str, relatorio: str, last_period: int):
        self.relatorio = relatorio  # Nome do relatorio descontinuado
        self.last_period = last_period  # Ultimo periodo disponivel (ex: 202412)
```

### EmptyFilterWarning

Warning emitido quando um filtro vazio e passado a um parametro (ex: `columns=[]`):

```python
class EmptyFilterWarning(UserWarning):
    """Filtro vazio passado a um parametro (ex: columns=[], conta=[])."""

    def __init__(self, message: str, parameter: str):
        self.parameter = parameter  # Nome do parametro vazio
```

---

## validation.py

Funcoes de normalizacao e validacao de inputs. Usadas internamente pelo
`BaseExplorer`. Eram modelos Pydantic ate a 0.6.x; viraram funcoes porque o
`__init__` gerado pelo Pydantic e tipado com a anotacao pos-validacao, o que
tornava todo call site com `mode="before"` um erro de type check.

### normalize_dates

Normaliza `DateInput` para `list[int]` no formato YYYYMM:

```python
normalize_dates("2024-12")  # [202412]
normalize_dates([202401, "2024-02"])  # [202401, 202402]
```

### validate_cnpj8

Valida e normaliza CNPJ para a base de 8 digitos. Aceita base-8 e CNPJ completo
de 14 digitos, com ou sem formatacao; no caso de 14, confere os digitos
verificadores antes de truncar:

```python
validate_cnpj8("60872504")  # "60872504"
validate_cnpj8("60.872.504/0001-23")  # "60872504"
validate_cnpj8("abc")  # Raises InvalidIdentifierError
```

### normalize_institutions

Normaliza `InstitutionInput` para lista de CNPJs validados:

```python
normalize_institutions("60872504")  # ["60872504"]
normalize_institutions(["60872504", "60746948"])  # ["60872504", "60746948"]
```

### normalize_accounts

Normaliza `AccountInput` para lista de strings:

```python
normalize_accounts("TOTAL DO ATIVO")  # ["TOTAL DO ATIVO"]
```

---

## types.py

### DateScalar

Tipo unitario para um valor de data:

```python
DateScalar = int | str | date | datetime | pd.Timestamp
```

### DateInput

Tipo flexivel para parametros de data:

```python
DateInput = DateScalar | list[DateScalar]
```

Aceita:
- `int`: 202412
- `str`: '202412', '2024-12', '2024-12-01'
- `date`: date(2024, 12, 1)
- `datetime`: datetime(2024, 12, 1)
- `pd.Timestamp`: pd.Timestamp('2024-12-01')
- `list` de qualquer combinacao dos tipos acima

### AccountInput

Tipo flexivel para parametros de conta:

```python
AccountInput = str | list[str]
```

Aceita:
- `str`: 'TOTAL GERAL DO ATIVO'
- `list[str]`: ['TOTAL GERAL DO ATIVO', 'PATRIMONIO LIQUIDO']

### InstitutionInput

Tipo flexivel para parametros de instituicao:

```python
InstitutionInput = str | list[str]
```

Aceita:
- `str`: '60872504'
- `list[str]`: ['60872504', '00000000']

---

## Tratamento de Erros

### Capturar Qualquer Erro

```python
from ifdata_bcb import BacenAnalysisError

try:
    df = bcb.cosif.read("2024-12", instituicao="60872504")
except BacenAnalysisError as e:
    print(f"Erro: {e}")
```

### Capturar Erros Especificos

```python
from ifdata_bcb import (
    InvalidIdentifierError,
    MissingRequiredParameterError,
    InvalidDateRangeError,
    InvalidColumnError,
)

try:
    df = bcb.ifdata.read("2024-12", instituicao="Itau", escopo="prudencial")
except InvalidIdentifierError as e:
    print(f"CNPJ invalido: {e.identificador}")
except MissingRequiredParameterError as e:
    print(f"Faltou: {e.param_name}")
except InvalidDateRangeError as e:
    print(f"Datas invertidas: {e.start} > {e.end}")
except InvalidColumnError as e:
    print(f"Coluna invalida: {e.column}. Disponiveis: {e.valid_columns}")
```

### Padroes de Validacao em Explorers

```python
def read(self, start, end=None, *, instituicao=None, escopo=None):
    # 1. Parametro obrigatorio (apenas start)
    if start is None:
        raise MissingRequiredParameterError("start")

    # 2. Validar range de datas
    if end is not None:
        start_int = self._normalize_datas(start)[0]
        end_int = self._normalize_datas(end)[0]
        if start_int > end_int:
            raise InvalidDateRangeError(start, end)

    # 3. Validar CNPJ (se fornecido)
    if instituicao is not None:
        cnpj = self._resolve_entidade(instituicao)  # Levanta InvalidIdentifierError

    # 4. Validar escopo (em IFDATAExplorer)
    if escopo not in [None, "individual", "prudencial", "financeiro"]:
        raise InvalidScopeError(
            scope="escopo",
            value=escopo,
            valid_values=["individual", "prudencial", "financeiro"],
        )
```

---

## Imports

O `domain/__init__.py` e um namespace leve (sem re-exports). Importe diretamente dos submodulos:

```python
# Imports diretos (nao passam pelo __init__.py do domain)
from ifdata_bcb.domain.exceptions import BacenAnalysisError, InvalidScopeError
from ifdata_bcb.domain.types import DateInput, AccountInput, InstitutionInput
from ifdata_bcb.domain.validation import validate_cnpj8, normalize_dates
```

Re-export no `__init__.py` raiz. O conjunto publico inteiro -- as nove
excecoes levantadas e os nove warnings -- fica disponivel em `ifdata_bcb`,
para que tratar erro ou filtrar warning nao exija conhecer o layout interno:

```python
# ifdata_bcb/__init__.py
from ifdata_bcb.domain.exceptions import (
    BacenAnalysisError,
    BacenWarning,
    # ... o restante do conjunto publico
)
```

Os imports sao eager, e nao lazy como os explorers: `domain/exceptions.py` nao
importa nada, entao expo-los nao carrega pandas nem duckdb. Ha teste em
subprocesso fixando isso -- e a condicao que sustenta o lazy loading.
