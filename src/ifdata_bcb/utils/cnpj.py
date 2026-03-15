import re


def standardize_cnpj_base8(cnpj: str) -> str | None:
    """Padroniza CNPJ para string de 8 digitos (remove formatacao, zfill, trunca)."""
    if cnpj is None:
        return None
    cleaned = re.sub(r"[^0-9]", "", str(cnpj).strip())
    if not cleaned:
        return None
    return cleaned.zfill(8)[:8]
