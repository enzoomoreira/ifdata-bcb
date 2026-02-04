# Camada de Dominio

A camada de dominio contem modelos de dados, tipos e excecoes da biblioteca.

## Localizacao

```
src/ifdata_bcb/domain/
|-- __init__.py           # Exports publicos
|-- exceptions.py        # Hierarquia de excecoes
|-- models.py            # Dataclasses (ScopeResolution)
+-- types.py             # Type aliases
```

---

## exceptions.py

### Hierarquia

```
Exception
    +-- BacenAnalysisError (base)
            +-- InvalidScopeError
            +-- DataUnavailableError
            +-- EntityNotFoundError
            +-- AmbiguousIdentifierError
            +-- InvalidIdentifierError
            +-- MissingRequiredParameterError
            +-- InvalidDateRangeError
            +-- InvalidDateFormatError
            +-- PeriodUnavailableError
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
    def __init__(
        self,
        scope_name: str,
        value: Optional[str] = None,
        valid_values: Optional[list[str]] = None,
        context: Optional[str] = None,
    ):
        self.scope_name = scope_name
        self.value = value
        self.valid_values = valid_values
        self.context = context

# Uso
raise InvalidScopeError(
    scope_name="escopo",
    value="invalido",
    valid_values=["individual", "prudencial", "financeiro"],
    context="Instituicao pertence a conglomerado"
)
# Mensagem: "O parametro 'escopo' tem valor invalido: 'invalido'.
#            Valores validos: individual, prudencial, financeiro.
#            Contexto: Instituicao pertence a conglomerado"
```

### DataUnavailableError

Dados nao disponiveis para a consulta:

```python
class DataUnavailableError(BacenAnalysisError):
    def __init__(
        self,
        entity: str,
        scope_type: str,
        reason: Optional[str] = None,
        suggestions: Optional[list[str]] = None,
    ):
        self.entity = entity
        self.scope_type = scope_type
        self.reason = reason
        self.suggestions = suggestions

# Uso
raise DataUnavailableError(
    entity="60872504",
    scope_type="financeiro",
    reason="Instituicao nao possui dados de conglomerado financeiro",
    suggestions=["Tente escopo='prudencial'", "Verifique bcb.cadastro.info()"]
)
```

### EntityNotFoundError

Entidade nao encontrada:

```python
class EntityNotFoundError(BacenAnalysisError):
    def __init__(
        self,
        identifier: str,
        suggestions: Optional[list[str]] = None,
    ):
        self.identifier = identifier
        self.suggestions = suggestions

# Uso
raise EntityNotFoundError(
    identifier="Banco Inexistente",
    suggestions=["Verifique o nome", "Use bcb.search('termo')"]
)
```

### AmbiguousIdentifierError

Identificador com multiplas correspondencias:

```python
class AmbiguousIdentifierError(BacenAnalysisError):
    def __init__(
        self,
        identifier: str,
        matches: Optional[list[str]] = None,
        suggestion: Optional[str] = None,
    ):
        self.identifier = identifier
        self.matches = matches
        self.suggestion = suggestion

# Uso
raise AmbiguousIdentifierError(
    identifier="Itau",
    matches=["ITAU UNIBANCO S.A.", "BANCO ITAU BBA S.A."],
    suggestion="Use CNPJ de 8 digitos ou nome mais completo"
)
```

### InvalidIdentifierError

Formato de identificador invalido:

```python
class InvalidIdentifierError(BacenAnalysisError):
    def __init__(
        self,
        identificador: str,
        suggestion: Optional[str] = None,
    ):
        self.identificador = identificador
        self.suggestion = suggestion

# Uso
raise InvalidIdentifierError(
    identificador="Itau",
    suggestion="Use bcb.search('Itau') para encontrar o CNPJ de 8 digitos."
)
# Mensagem: "Identificador 'Itau' em formato invalido. Esperado CNPJ de 8 digitos.
#            Use bcb.search('Itau') para encontrar o CNPJ de 8 digitos."
```

### MissingRequiredParameterError

Parametro obrigatorio ausente:

```python
class MissingRequiredParameterError(BacenAnalysisError):
    def __init__(
        self,
        param_name: str,
        context: Optional[str] = None,
    ):
        self.param_name = param_name
        self.context = context

# Uso
raise MissingRequiredParameterError(
    param_name="instituicao",
    context="Forneca CNPJ de 8 digitos (use bcb.search() para encontrar)"
)
# Mensagem: "O parametro 'instituicao' e obrigatorio.
#            Forneca CNPJ de 8 digitos (use bcb.search() para encontrar)"
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
    def __init__(self, value: str, expected_formats: Optional[list[str]] = None):
        self.value = value
        self.expected_formats = expected_formats or ["YYYYMM", "YYYY-MM"]

# Uso
raise InvalidDateFormatError(
    value="2024/12/01",
    expected_formats=["YYYYMM", "YYYY-MM", "YYYY-MM-DD"]
)
```

### PeriodUnavailableError

Periodo nao disponivel no BCB (usado internamente na coleta):

```python
class PeriodUnavailableError(BacenAnalysisError):
    def __init__(self, period: int, source: str):
        self.period = period
        self.source = source

# Uso interno
raise PeriodUnavailableError(period=202501, source="COSIF Individual")
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

lookup = EntityLookup()
resolution = lookup.resolve_ifdata_scope("60872504", "prudencial")

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
DateInput = Union[int, str, list[int], list[str]]
```

Aceita:
- `int`: 202412
- `str`: '202412', '2024-12', '2024-12-01'
- `list[int]`: [202401, 202402, 202403]
- `list[str]`: ['2024-01', '2024-02', '2024-03']

### AccountInput

Tipo flexivel para parametros de conta:

```python
AccountInput = Union[str, list[str]]
```

Aceita:
- `str`: 'TOTAL GERAL DO ATIVO'
- `list[str]`: ['TOTAL GERAL DO ATIVO', 'PATRIMONIO LIQUIDO']

### InstitutionInput

Tipo flexivel para parametros de instituicao:

```python
InstitutionInput = Union[str, list[str]]
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
from ifdata_bcb import (
    InvalidIdentifierError,
    MissingRequiredParameterError,
    InvalidDateRangeError,
    DataUnavailableError,
)

try:
    df = bcb.ifdata.read(instituicao='Itau', start='2024-12', escopo='prudencial')
except InvalidIdentifierError as e:
    print(f"CNPJ invalido: {e.identificador}")
    print(f"Sugestao: {e.suggestion}")
except MissingRequiredParameterError as e:
    print(f"Faltou: {e.param_name}")
except InvalidDateRangeError as e:
    print(f"Datas invertidas: {e.start} > {e.end}")
except DataUnavailableError as e:
    print(f"Sem dados para {e.entity} em {e.scope_type}")
    for s in e.suggestions or []:
        print(f"  - {s}")
```

### Padroes de Validacao em Explorers

```python
def read(self, instituicao, start, end=None, escopo="individual"):
    # 1. Parametros obrigatorios
    if instituicao is None:
        raise MissingRequiredParameterError(
            param_name="instituicao",
            context="Forneca CNPJ de 8 digitos"
        )
    if start is None:
        raise MissingRequiredParameterError(
            param_name="start",
            context="Formato: YYYY-MM"
        )

    # 2. Validar range de datas
    if end is not None:
        start_int = self._normalize_dates(start)[0]
        end_int = self._normalize_dates(end)[0]
        if start_int > end_int:
            raise InvalidDateRangeError(start, end)

    # 3. Validar CNPJ
    cnpj = self._resolve_entity(instituicao)  # Levanta InvalidIdentifierError

    # 4. Validar escopo (em IFDATAExplorer)
    if escopo not in ["individual", "prudencial", "financeiro"]:
        raise InvalidScopeError(
            scope_name="escopo",
            value=escopo,
            valid_values=["individual", "prudencial", "financeiro"]
        )
```

---

## Exports Publicos

```python
# domain/__init__.py
from ifdata_bcb.domain.exceptions import (
    BacenAnalysisError,
    InvalidScopeError,
    DataUnavailableError,
    EntityNotFoundError,
    AmbiguousIdentifierError,
    InvalidIdentifierError,
    MissingRequiredParameterError,
    InvalidDateRangeError,
    InvalidDateFormatError,
    PeriodUnavailableError,
)
from ifdata_bcb.domain.models import ScopeResolution
from ifdata_bcb.domain.types import DateInput, AccountInput, InstitutionInput

__all__ = [
    "BacenAnalysisError",
    "InvalidScopeError",
    "DataUnavailableError",
    "EntityNotFoundError",
    "AmbiguousIdentifierError",
    "InvalidIdentifierError",
    "MissingRequiredParameterError",
    "InvalidDateRangeError",
    "InvalidDateFormatError",
    "PeriodUnavailableError",
    "ScopeResolution",
    "DateInput",
    "AccountInput",
    "InstitutionInput",
]
```

Re-export no `__init__.py` raiz:

```python
# ifdata_bcb/__init__.py
from ifdata_bcb.domain.exceptions import (
    BacenAnalysisError,
    InvalidScopeError,
    # ... todas as excecoes
)

__all__ = [
    # Explorers
    "cosif",
    "ifdata",
    "cadastro",
    # Funcoes
    "search",
    # Excecoes (para import direto: from ifdata_bcb import BacenAnalysisError)
    "BacenAnalysisError",
    "InvalidScopeError",
    # ...
]
```
