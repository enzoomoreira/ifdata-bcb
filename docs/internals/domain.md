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
    |       +-- DataUnavailableError
    |       +-- InvalidIdentifierError
    |       +-- MissingRequiredParameterError
    |       +-- InvalidDateRangeError
    |       +-- InvalidDateFormatError
    |       +-- PeriodUnavailableError
    |       +-- DataProcessingError
    |       +-- InvalidColumnError
    +-- UserWarning
            +-- IncompatibleEraWarning
            +-- PartialDataWarning
            +-- ScopeUnavailableWarning
            +-- NullValuesWarning
            +-- ScopeMigrationWarning
            +-- DroppedReportWarning
            +-- EmptyFilterWarning
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

Escopo ou tipo invalido:

```python
class InvalidScopeError(BacenAnalysisError):
    def __init__(self, scope: str, value: str, valid_values: list[str]):
        self.scope = scope
        self.value = value
        self.valid_values = valid_values

# Uso
raise InvalidScopeError(
    scope="escopo",
    value="invalido",
    valid_values=["individual", "prudencial", "financeiro"],
)
# Mensagem: "Escopo 'invalido' invalido. Validos: 'individual', 'prudencial', 'financeiro'."
```

### DataUnavailableError

Dados nao disponiveis para a consulta:

```python
class DataUnavailableError(BacenAnalysisError):
    def __init__(self, entity: str, scope_type: str, reason: str = ""):
        self.entity = entity
        self.scope_type = scope_type
        self.reason = reason

# Uso
raise DataUnavailableError(
    entity="60872504",
    scope_type="financeiro",
    reason="Instituicao nao possui dados de conglomerado financeiro",
)
# Mensagem: "Dados indisponiveis para '60872504' no escopo 'financeiro'. ..."
```

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
raise MissingRequiredParameterError(param_name="instituicao")
# Mensagem: "Parametro obrigatorio ausente: 'instituicao'."
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
# Mensagem: "Range de datas invalido: start='2024-12' > end='2024-01'."
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
        self.source = source      # Fonte (ex: "COSIF")

# Emitido automaticamente por check_era_boundary() em core/eras.py
# Exemplo: cosif.read('2024-12', '2025-01') emite este warning
```

Nao herda de `BacenAnalysisError` -- e um `UserWarning` capturavel via `warnings.catch_warnings()`:

```python
import warnings
from ifdata_bcb.domain.exceptions import IncompatibleEraWarning

with warnings.catch_warnings(record=True) as w:
    warnings.simplefilter("always")
    df = bcb.cosif.read('2024-12', '2025-01', instituicao='60872504')
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
        self.reason = reason    # Ex: "query_failed", "no_cnpj_for_enrichment"
        self.detail = detail
```

### ScopeUnavailableWarning

Warning emitido quando um escopo nao esta disponivel para uma entidade em parte do range temporal solicitado:

```python
class ScopeUnavailableWarning(UserWarning):
    """Escopo indisponivel para entidade em parte do range temporal."""
    def __init__(self, message: str, entities: list[str], escopo: str, periodos: list[int]):
        self.entities = entities  # CNPJs afetados
        self.escopo = escopo      # Escopo indisponivel
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
        self.relatorio = relatorio      # Nome do relatorio afetado
        self.escopo_pre = escopo_pre    # Escopo antes do boundary (ex: "financeiro")
        self.escopo_post = escopo_post  # Escopo apos o boundary (ex: "prudencial")
        self.boundary = boundary        # Periodo boundary (ex: 202503)
```

### DroppedReportWarning

Warning emitido quando um relatorio foi descontinuado a partir de determinada era:

```python
class DroppedReportWarning(UserWarning):
    """Relatorio descontinuado a partir de determinada era."""
    def __init__(self, message: str, relatorio: str, last_period: int):
        self.relatorio = relatorio      # Nome do relatorio descontinuado
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

Modelos Pydantic para normalizacao e validacao de inputs. Usados internamente pelo `BaseExplorer`.

### NormalizedDates

Normaliza `DateInput` para `list[int]` no formato YYYYMM:

```python
class NormalizedDates(BaseModel):
    values: list[int]

    @field_validator("values", mode="before")
    def normalize(cls, v: DateInput) -> list[int]: ...

# Uso
NormalizedDates(values="2024-12").values  # [202412]
NormalizedDates(values=[202401, "2024-02"]).values  # [202401, 202402]
```

### ValidatedCnpj8

Valida CNPJ de exatamente 8 digitos:

```python
class ValidatedCnpj8(BaseModel):
    value: str

# Uso
ValidatedCnpj8(value="60872504").value  # "60872504"
ValidatedCnpj8(value="abc")  # Raises InvalidIdentifierError
```

### InstitutionList

Normaliza `InstitutionInput` para lista de CNPJs validados:

```python
class InstitutionList(BaseModel):
    values: list[str]

# Uso
InstitutionList(values="60872504").values  # ["60872504"]
InstitutionList(values=["60872504", "60746948"]).values  # ["60872504", "60746948"]
```

### AccountList

Normaliza `AccountInput` para lista de strings:

```python
class AccountList(BaseModel):
    values: list[str]

# Uso
AccountList(values="TOTAL DO ATIVO").values  # ["TOTAL DO ATIVO"]
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
    df = bcb.cosif.read('2024-12', instituicao='60872504')
except BacenAnalysisError as e:
    print(f"Erro: {e}")
```

### Capturar Erros Especificos

```python
from ifdata_bcb.domain.exceptions import (
    InvalidIdentifierError,
    MissingRequiredParameterError,
    InvalidDateRangeError,
    DataUnavailableError,
)

try:
    df = bcb.ifdata.read('2024-12', instituicao='Itau', escopo='prudencial')
except InvalidIdentifierError as e:
    print(f"CNPJ invalido: {e.identificador}")
except MissingRequiredParameterError as e:
    print(f"Faltou: {e.param_name}")
except InvalidDateRangeError as e:
    print(f"Datas invertidas: {e.start} > {e.end}")
except DataUnavailableError as e:
    print(f"Sem dados para {e.entity} em {e.scope_type}")
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
from ifdata_bcb.domain.validation import ValidatedCnpj8, NormalizedDates
```

Re-export no `__init__.py` raiz (lazy, apenas as mais comuns):

```python
# ifdata_bcb/__init__.py
from ifdata_bcb.domain.exceptions import (
    BacenAnalysisError,
    DataUnavailableError,
)

__all__ = [
    "cosif",
    "ifdata",
    "cadastro",
    "search",
    "BacenAnalysisError",
    "DataUnavailableError",
]
```
