# Camada de Dominio

A camada de dominio contem modelos de dados, tipos e excecoes da biblioteca.

## Localizacao

```
src/ifdata_bcb/domain/
|-- __init__.py           # Exports publicos
|-- exceptions.py        # Hierarquia de excecoes
|-- models.py            # Dataclasses (ScopeResolution)
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
    |       +-- EntityNotFoundError
    |       +-- AmbiguousIdentifierError
    |       +-- InvalidIdentifierError
    |       +-- MissingRequiredParameterError
    |       +-- InvalidDateRangeError
    |       +-- InvalidDateFormatError
    |       +-- PeriodUnavailableError
    |       +-- DataProcessingError
    +-- UserWarning
            +-- IncompatibleEraWarning
            +-- PartialDataWarning
            +-- ScopeUnavailableWarning
            +-- NullValuesWarning
            +-- EmptyFilterWarning
```

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

### EntityNotFoundError

Entidade nao encontrada:

```python
class EntityNotFoundError(BacenAnalysisError):
    def __init__(self, identifier: str):
        self.identifier = identifier

# Uso
raise EntityNotFoundError(identifier="Banco Inexistente")
# Mensagem: "Entidade nao encontrada: 'Banco Inexistente'."
```

### AmbiguousIdentifierError

Identificador com multiplas correspondencias:

```python
class AmbiguousIdentifierError(BacenAnalysisError):
    def __init__(self, identifier: str, matches: list[str]):
        self.identifier = identifier
        self.matches = matches

# Uso
raise AmbiguousIdentifierError(
    identifier="Itau",
    matches=["ITAU UNIBANCO S.A.", "BANCO ITAU BBA S.A."],
)
# Mensagem: "Identificador 'Itau' ambiguo. Encontrados: 'ITAU UNIBANCO S.A.', 'BANCO ITAU BBA S.A.'."
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

# Emitido automaticamente por check_era_boundary() em core/eras.py
# Exemplo: cosif.read(start='2024-12', end='2025-01') emite este warning
```

Nao herda de `BacenAnalysisError` -- e um `UserWarning` capturavel via `warnings.catch_warnings()`:

```python
import warnings
from ifdata_bcb.domain.exceptions import IncompatibleEraWarning

with warnings.catch_warnings(record=True) as w:
    warnings.simplefilter("always")
    df = bcb.cosif.read(instituicao='60872504', start='2024-12', end='2025-01')
    if w and issubclass(w[0].category, IncompatibleEraWarning):
        print("Cuidado: codigos de conta podem ser incompativeis")
```

### PartialDataWarning

Warning emitido quando o resultado pode estar incompleto -- por exemplo, quando alguns periodos ou entidades nao retornaram dados, ou quando uma query de leitura falha por incompatibilidade de schema:

```python
class PartialDataWarning(UserWarning):
    """Resultado incompleto - alguns periodos/entidades sem dados."""
```

### ScopeUnavailableWarning

Warning emitido quando um escopo nao esta disponivel para uma entidade em parte do range temporal solicitado:

```python
class ScopeUnavailableWarning(UserWarning):
    """Escopo indisponivel para entidade em parte do range temporal."""
```

### NullValuesWarning

Warning emitido quando uma entidade esta presente nos dados mas com todos os valores financeiros (VALOR) NULL. Ocorre quando o BCB registra a entidade no periodo mas nao fornece valores:

```python
class NullValuesWarning(UserWarning):
    """Entidade presente nos dados mas com todos os valores financeiros NULL."""
```

### EmptyFilterWarning

Warning emitido quando um filtro vazio e passado a um parametro (ex: `columns=[]`):

```python
class EmptyFilterWarning(UserWarning):
    """Filtro vazio passado a um parametro (ex: columns=[], conta=[])."""
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

## models.py

### ScopeResolution

Dataclass imutavel para resultado de resolucao de escopo IFDATA:

```python
@dataclass(frozen=True)
class ScopeResolution:
    cod_inst: str         # Codigo para filtrar IFDATA (CNPJ ou CodConglomerado)
    tipo_inst: int        # TipoInstituicao: 1=prudencial, 2=financeiro, 3=individual
    cnpj_original: str    # CNPJ de 8 digitos original
    escopo: str           # "individual", "prudencial", ou "financeiro"
```

Uso:

```python
from ifdata_bcb.core import EntityLookup
from ifdata_bcb.providers.ifdata.scope import resolve_ifdata_escopo

lookup = EntityLookup()
resolution = resolve_ifdata_escopo(lookup, "60872504", "prudencial")

print(resolution.cod_inst)      # "C0080099" (codigo conglomerado)
print(resolution.tipo_inst)     # 1
print(resolution.escopo)        # "prudencial"
print(resolution.cnpj_original) # "60872504"
```

---

## types.py

### DateInput

Tipo flexivel para parametros de data:

```python
DateInput = int | str | list[int] | list[str]
```

Aceita:
- `int`: 202412
- `str`: '202412', '2024-12', '2024-12-01'
- `list[int]`: [202401, 202402, 202403]
- `list[str]`: ['2024-01', '2024-02', '2024-03']

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
    df = bcb.cosif.read(instituicao='60872504', start='2024-12')
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
    df = bcb.ifdata.read(instituicao='Itau', start='2024-12', escopo='prudencial')
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
def read(self, instituicao, start, end=None, escopo=None):
    # 1. Parametros obrigatorios
    if instituicao is None:
        raise MissingRequiredParameterError("instituicao")
    if start is None:
        raise MissingRequiredParameterError("start")

    # 2. Validar range de datas
    if end is not None:
        start_int = self._normalize_datas(start)[0]
        end_int = self._normalize_datas(end)[0]
        if start_int > end_int:
            raise InvalidDateRangeError(start, end)

    # 3. Validar CNPJ
    cnpj = self._resolve_entidade(instituicao)  # Levanta InvalidIdentifierError

    # 4. Validar escopo (em IFDATAExplorer)
    if escopo not in ["individual", "prudencial", "financeiro"]:
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
from ifdata_bcb.domain.models import ScopeResolution
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
