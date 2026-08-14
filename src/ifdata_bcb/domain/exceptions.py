class BacenAnalysisError(Exception):
    """Excecao base da biblioteca. Use `except BacenAnalysisError` para capturar todos os erros."""


class InvalidScopeError(BacenAnalysisError):
    """Valor invalido para um parametro de dominio fechado (escopo, fonte, documento).

    A mensagem nomeia o parametro em vez de dizer sempre "Escopo": a classe e
    usada para escopo, fonte e documento, e o texto fixo produzia frases
    erradas nos dois ultimos.

    `valid_values` vazio omite a clausula "Validos:" -- serve para parametros
    cujo dominio nao e enumeravel (documento), onde `hint` explica o formato.
    """

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
        msg = f"Valor invalido para '{scope}': '{value}'."
        if self.valid_values:
            valid_str = ", ".join(repr(v) for v in self.valid_values)
            msg += f" Validos: {valid_str}."
        if hint:
            msg += f" {hint}"
        super().__init__(msg)


class DataUnavailableError(BacenAnalysisError):
    """Nao levantada por nenhum caminho da biblioteca.

    Escopo indisponivel para uma entidade e sinalizado com
    ScopeUnavailableWarning mais DataFrame vazio, e nao com excecao: o
    consumidor recebe os dados parciais junto do diagnostico em vez de perder
    o resultado inteiro. Saiu do contrato publico na v0.6.0; a classe some na
    v1.0.0, onde remover nome publico e permitido.
    """

    def __init__(self, entity: str, scope_type: str, reason: str = ""):
        self.entity = entity
        self.scope_type = scope_type
        self.reason = reason
        msg = f"Dados indisponiveis para '{entity}' no escopo '{scope_type}'."
        if reason:
            msg += f" {reason}"
        super().__init__(msg)


class InvalidIdentifierError(BacenAnalysisError):
    def __init__(self, identificador: str):
        self.identificador = identificador
        super().__init__(
            f"Identificador '{identificador}' invalido. Esperado CNPJ de 8 digitos."
        )


class MissingRequiredParameterError(BacenAnalysisError):
    def __init__(self, param_name: str):
        self.param_name = param_name
        super().__init__(f"Parametro obrigatorio ausente: '{param_name}'.")


class InvalidDateRangeError(BacenAnalysisError):
    def __init__(self, start: str, end: str):
        self.start = start
        self.end = end
        super().__init__(f"Data inicial ({start}) maior que data final ({end}).")


class InvalidDateFormatError(BacenAnalysisError):
    def __init__(self, value: str, detail: str = ""):
        self.value = value
        self.detail = detail
        msg = f"Formato de data invalido: '{value}'."
        if detail:
            msg += f" {detail}"
        super().__init__(msg)


class PeriodUnavailableError(BacenAnalysisError):
    def __init__(self, period: int):
        self.period = period
        super().__init__(f"Periodo {period} indisponivel na fonte.")


class DataProcessingError(BacenAnalysisError):
    def __init__(self, source: str, detail: str = ""):
        self.source = source
        self.detail = detail
        msg = f"Falha no processamento da fonte '{source}'."
        if detail:
            msg += f" {detail}"
        super().__init__(msg)


class InvalidColumnError(BacenAnalysisError):
    """Coluna invalida em read(), list() ou no parametro cadastro=."""

    def __init__(self, column: str, valid_columns: list[str], extras: str = ""):
        self.column = column
        self.valid_columns = valid_columns
        valid_str = ", ".join(valid_columns)
        msg = f"Coluna '{column}' invalida. Disponiveis: {valid_str}."
        if extras:
            msg += f" {extras}"
        super().__init__(msg)


class BacenWarning(UserWarning):
    """Base dos warnings da biblioteca.

    Existe para que `warnings.simplefilter("ignore", BacenWarning)` silencie
    tudo que a lib emite -- antes era preciso listar as oito classes uma a uma.
    Continua sendo UserWarning, entao quem ja filtrava por UserWarning nao muda.
    """


class IncompatibleEraWarning(BacenWarning):
    """Emitido quando uma query abrange periodos com codigos de conta incompativeis."""

    def __init__(self, message: str, boundary: int, source: str):
        self.boundary = boundary
        self.source = source
        super().__init__(message)


class PartialDataWarning(BacenWarning):
    """Resultado incompleto - alguns periodos/entidades sem dados."""

    def __init__(self, message: str, reason: str = "", detail: dict | None = None):
        self.reason = reason
        self.detail = detail
        super().__init__(message)


class ScopeUnavailableWarning(BacenWarning):
    """Escopo indisponivel para entidade em parte do range temporal."""

    def __init__(
        self,
        message: str,
        entities: list[str],
        escopo: str,
        periodos: list[int],
    ):
        self.entities = entities
        self.escopo = escopo
        self.periodos = periodos
        super().__init__(message)


class NullValuesWarning(BacenWarning):
    """Entidade presente nos dados mas com todos os valores financeiros NULL."""

    def __init__(self, message: str, entities: list[str]):
        self.entities = entities
        super().__init__(message)


class ScopeMigrationWarning(BacenWarning):
    """Relatorio migrou de escopo entre eras (ex: credito de financeiro para prudencial)."""

    def __init__(
        self,
        message: str,
        relatorio: str,
        escopo_pre: str,
        escopo_post: str,
        boundary: int,
    ):
        self.relatorio = relatorio
        self.escopo_pre = escopo_pre
        self.escopo_post = escopo_post
        self.boundary = boundary
        super().__init__(message)


class DroppedReportWarning(BacenWarning):
    """Relatorio descontinuado a partir de determinada era."""

    def __init__(self, message: str, relatorio: str, last_period: int):
        self.relatorio = relatorio
        self.last_period = last_period
        super().__init__(message)


class EmptyFilterWarning(BacenWarning):
    """Filtro vazio passado a um parametro (ex: columns=[], conta=[])."""

    def __init__(self, message: str, parameter: str):
        self.parameter = parameter
        super().__init__(message)


class TruncatedResultWarning(BacenWarning):
    """Resultado truncado pelo limit."""

    def __init__(self, message: str, limit: int):
        self.limit = limit
        super().__init__(message)
