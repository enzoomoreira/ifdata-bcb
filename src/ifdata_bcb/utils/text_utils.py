"""Utilitarios para normalizacao de texto."""

import unicodedata


def normalize_accents(text: str) -> str:
    """
    Remove acentos de uma string, mantendo os caracteres base.

    Usa decomposicao NFKD do Unicode para separar caracteres base de
    seus modificadores (acentos), e depois remove os modificadores.

    Args:
        text: Texto com possíveis acentos.

    Returns:
        Texto sem acentos.

    Exemplo:
        >>> normalize_accents("ITAÚ UNIBANCO")
        'ITAU UNIBANCO'
        >>> normalize_accents("São Paulo")
        'Sao Paulo'
    """
    if not isinstance(text, str):
        return text
    nfkd = unicodedata.normalize("NFKD", text)
    return "".join(c for c in nfkd if not unicodedata.combining(c))


def normalize_text(text: str) -> str:
    """
    Normaliza texto removendo newlines e espacos multiplos.

    Substitui quebras de linha e multiplos espacos por um unico espaco,
    e remove espacos no inicio/fim do texto.

    Args:
        text: Texto a normalizar.

    Returns:
        Texto normalizado com espacos unicos.

    Exemplo:
        >>> normalize_text("Depositos a Prazo \\n(a4)")
        'Depositos a Prazo (a4)'
        >>> normalize_text("  Nome   com   espacos  ")
        'Nome com espacos'
    """
    if not isinstance(text, str):
        return text
    # split() sem argumentos divide em qualquer whitespace (espacos, tabs, newlines)
    # e automaticamente ignora vazios, entao join com espaco normaliza tudo
    return " ".join(text.split())
