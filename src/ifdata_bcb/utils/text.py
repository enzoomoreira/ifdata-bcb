import unicodedata


def normalize_accents(text: str) -> str:
    if not isinstance(text, str):
        return text
    # NFKD separa caracteres base de modificadores (acentos)
    nfkd = unicodedata.normalize("NFKD", text)
    return "".join(c for c in nfkd if not unicodedata.combining(c))


def normalize_text(text: str) -> str:
    if not isinstance(text, str):
        return text
    # split() sem args divide em qualquer whitespace e ignora vazios
    return " ".join(text.split())
