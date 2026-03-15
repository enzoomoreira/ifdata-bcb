"""
Excecoes customizadas para a biblioteca Bacen Data Analysis
"""


class BacenAnalysisError(Exception):
    """
    Excecao base para todos os erros na biblioteca Bacen Data Analysis.

    Esta classe serve como base para todas as excecoes especificas,
    permitindo que usuarios capturem qualquer erro da biblioteca
    usando `except BacenAnalysisError:`.
    """
    pass


class InvalidScopeError(BacenAnalysisError):
    """
    Excecao levantada quando um escopo ou tipo e invalido ou nao especificado.

    Args:
        scope_name: Nome do parametro de escopo (ex: 'tipo', 'escopo')
        value: Valor fornecido (ou None se nao fornecido)
        valid_values: Lista de valores validos
        context: Contexto adicional para ajudar na mensagem de erro
    """
    def __init__(self, scope_name: str, value=None, valid_values=None, context=None):
        self.scope_name = scope_name
        self.value = value
        self.valid_values = valid_values or []

        if value is None:
            msg = f"O parametro '{scope_name}' e obrigatorio e deve ser especificado."
        elif valid_values and value not in valid_values:
            msg = (
                f"O valor '{value}' e invalido para o parametro '{scope_name}'. "
                f"Valores validos: {', '.join(repr(v) for v in valid_values)}."
            )
        else:
            msg = f"O parametro '{scope_name}' tem um valor invalido: {value}."

        if context:
            msg += f" Contexto: {context}"

        super().__init__(msg)


class DataUnavailableError(BacenAnalysisError):
    """
    Excecao levantada quando os dados solicitados nao estao disponiveis
    para o contexto especificado.

    Args:
        entity: Identificador da entidade (nome ou CNPJ)
        scope_type: Tipo de escopo que foi solicitado
        reason: Razao pela qual os dados nao estao disponiveis
        suggestions: Lista de sugestoes para o usuario
    """
    def __init__(self, entity: str, scope_type: str, reason: str = None, suggestions=None):
        self.entity = entity
        self.scope_type = scope_type
        self.reason = reason
        self.suggestions = suggestions or []

        msg = (
            f"Dados nao disponiveis para a entidade '{entity}' "
            f"com escopo/tipo '{scope_type}'."
        )

        if reason:
            msg += f" Razao: {reason}"

        if self.suggestions:
            msg += f" Sugestoes: {', '.join(self.suggestions)}"

        super().__init__(msg)


class EntityNotFoundError(BacenAnalysisError):
    """
    Excecao levantada quando uma entidade nao e encontrada no mapeamento.

    Args:
        identifier: Identificador usado na busca
        suggestions: Lista de sugestoes para o usuario
    """
    def __init__(self, identifier: str, suggestions=None):
        self.identifier = identifier
        self.suggestions = suggestions or []

        msg = f"Entidade nao encontrada para o identificador '{identifier}'."

        if self.suggestions:
            msg += f" Sugestoes: {', '.join(self.suggestions)}"

        super().__init__(msg)


class AmbiguousIdentifierError(BacenAnalysisError):
    """
    Excecao levantada quando um identificador e ambiguo,
    resultando em multiplas correspondencias.

    Args:
        identifier: Identificador usado na busca
        matches: Lista de correspondencias encontradas
        suggestion: Sugestao para resolver a ambiguidade
    """
    def __init__(self, identifier: str, matches=None, suggestion=None):
        self.identifier = identifier
        self.matches = matches or []
        self.suggestion = suggestion

        msg = (
            f"O identificador '{identifier}' e ambiguo. "
            f"Encontradas {len(self.matches)} correspondencias."
        )

        if self.matches:
            matches_str = ', '.join(repr(m) for m in self.matches[:5])
            if len(self.matches) > 5:
                matches_str += f" (e mais {len(self.matches) - 5})"
            msg += f" Correspondencias: {matches_str}"

        if self.suggestion:
            msg += f" Sugestao: {self.suggestion}"

        super().__init__(msg)


class InvalidIdentifierError(BacenAnalysisError):
    """
    Excecao levantada quando o formato do identificador e invalido.

    O sistema requer CNPJ de 8 digitos para evitar ambiguidades.
    Use bcb.search() para encontrar o CNPJ correto.

    Args:
        identificador: O identificador invalido fornecido.
        suggestion: Sugestao para o usuario resolver o problema.
    """
    def __init__(self, identificador: str, suggestion: str = None):
        self.identificador = identificador
        self.suggestion = suggestion

        msg = f"Identificador '{identificador}' em formato invalido. "
        msg += "Esperado CNPJ de 8 digitos."
        if suggestion:
            msg += f" {suggestion}"

        super().__init__(msg)


class MissingRequiredParameterError(BacenAnalysisError):
    """
    Excecao levantada quando um parametro obrigatorio nao foi fornecido.

    Args:
        param_name: Nome do parametro ausente.
        context: Contexto adicional para ajudar o usuario.
    """
    def __init__(self, param_name: str, context: str = None):
        self.param_name = param_name
        self.context = context

        msg = f"O parametro '{param_name}' e obrigatorio."
        if context:
            msg += f" {context}"

        super().__init__(msg)


class InvalidDateRangeError(BacenAnalysisError):
    """
    Excecao levantada quando o range de datas e invalido (start > end).

    Args:
        start: Data inicial fornecida.
        end: Data final fornecida.
    """
    def __init__(self, start: str, end: str):
        self.start = start
        self.end = end

        msg = f"Data inicial ({start}) deve ser menor ou igual a data final ({end})."

        super().__init__(msg)
