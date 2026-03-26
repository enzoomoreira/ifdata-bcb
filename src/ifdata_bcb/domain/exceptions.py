class BacenAnalysisError(Exception):
    """Excecao base da biblioteca. Use `except BacenAnalysisError` para capturar todos os erros."""

    pass


class InvalidScopeError(BacenAnalysisError):
    def __init__(self, scope: str, value: str, valid_values: list[str]):
        self.scope = scope
        self.value = value
        self.valid_values = valid_values
        valid_str = ", ".join(repr(v) for v in valid_values)
        super().__init__(f"Escopo '{value}' invalido. Validos: {valid_str}.")


class DataUnavailableError(BacenAnalysisError):
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


class IncompatibleEraWarning(UserWarning):
    """Emitido quando uma query abrange periodos com codigos de conta incompativeis."""

    def __init__(self, message: str, boundary: int, source: str):
        self.boundary = boundary
        self.source = source
        super().__init__(message)


class PartialDataWarning(UserWarning):
    """Resultado incompleto - alguns periodos/entidades sem dados."""

    def __init__(self, message: str, reason: str = "", detail: dict | None = None):
        self.reason = reason
        self.detail = detail
        super().__init__(message)


class ScopeUnavailableWarning(UserWarning):
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


class NullValuesWarning(UserWarning):
    """Entidade presente nos dados mas com todos os valores financeiros NULL."""

    def __init__(self, message: str, entities: list[str]):
        self.entities = entities
        super().__init__(message)


class ScopeMigrationWarning(UserWarning):
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


class DroppedReportWarning(UserWarning):
    """Relatorio descontinuado a partir de determinada era."""

    def __init__(self, message: str, relatorio: str, last_period: int):
        self.relatorio = relatorio
        self.last_period = last_period
        super().__init__(message)


class EmptyFilterWarning(UserWarning):
    """Filtro vazio passado a um parametro (ex: columns=[], conta=[])."""

    def __init__(self, message: str, parameter: str):
        self.parameter = parameter
        super().__init__(message)


class InvalidColumnError(BacenAnalysisError):
    """Coluna invalida para list()."""

    def __init__(self, column: str, valid_columns: list[str], extras: str = ""):
        self.column = column
        self.valid_columns = valid_columns
        valid_str = ", ".join(valid_columns)
        msg = f"Coluna '{column}' invalida. Disponiveis: {valid_str}."
        if extras:
            msg += f" {extras}"
        super().__init__(msg)


class TruncatedResultWarning(UserWarning):
    """Resultado truncado pelo limit."""

    def __init__(self, message: str, limit: int):
        self.limit = limit
        super().__init__(message)
