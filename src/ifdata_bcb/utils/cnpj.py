import re

_DV1_PESOS = (5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2)
_DV2_PESOS = (6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2)


def only_digits(value: str) -> str:
    """Extrai apenas digitos ASCII.

    Deliberadamente `[0-9]` e nao `\\d`: `\\d` casa digitos fullwidth (U+FF10..),
    e `int()` os converte silenciosamente -- e por ai que passava o vetor de
    normalizacao unicode.
    """
    return re.sub(r"[^0-9]", "", str(value).strip())


def _digito_verificador(base: str, pesos: tuple[int, ...]) -> int:
    soma = sum(int(d) * p for d, p in zip(base, pesos, strict=True))
    resto = soma % 11
    return 0 if resto < 2 else 11 - resto


def is_valid_cnpj14(digitos: str) -> bool:
    """Confere os dois digitos verificadores de um CNPJ completo (14 digitos)."""
    if len(digitos) != 14 or not digitos.isascii() or not digitos.isdigit():
        return False
    dv1 = _digito_verificador(digitos[:12], _DV1_PESOS)
    dv2 = _digito_verificador(digitos[:13], _DV2_PESOS)
    return digitos[12:] == f"{dv1}{dv2}"


def standardize_cnpj_base8(cnpj: str) -> str | None:
    """Padroniza CNPJ para string de 8 digitos (remove formatacao, zfill, trunca).

    O zfill existe porque a fonte do BCB entrega CNPJ sem zero a esquerda. Nao
    use em input de usuario: la um valor curto e erro de digitacao, nao zero
    perdido (ver validate_cnpj8).
    """
    if cnpj is None:
        return None
    cleaned = only_digits(cnpj)
    if not cleaned:
        return None
    return cleaned.zfill(8)[:8]
