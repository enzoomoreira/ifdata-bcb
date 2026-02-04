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


class EntityNotFoundError(BacenAnalysisError):
    def __init__(self, identifier: str):
        self.identifier = identifier
        super().__init__(f"Entidade nao encontrada: '{identifier}'.")


class AmbiguousIdentifierError(BacenAnalysisError):
    def __init__(self, identifier: str, matches: list[str]):
        self.identifier = identifier
        self.matches = matches
        matches_str = ", ".join(repr(m) for m in matches[:5])
        if len(matches) > 5:
            matches_str += f" (e mais {len(matches) - 5})"
        super().__init__(
            f"Identificador '{identifier}' ambiguo. Encontrados: {matches_str}."
        )


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
