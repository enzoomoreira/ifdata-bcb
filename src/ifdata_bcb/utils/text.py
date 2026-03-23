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


def stem_ptbr(term: str) -> str:
    """Stem simples PT-BR: remove sufixos comuns para busca singular/plural.

    Usa pares atomicos (singular, plural) com raiz minima de 4 chars
    para evitar falsos positivos.
    """
    t = normalize_accents(term.lower())
    pairs = [
        ("icao", "icoes"),  # aplicacao/aplicacoes
        ("ucao", "ucoes"),  # reducao/reducoes
        ("cao", "coes"),  # operacao/operacoes, captacao/captacoes
        ("sao", "soes"),  # provisao/provisoes
        ("ao", "oes"),  # restante
        ("al", "ais"),  # capital/capitais
        ("el", "eis"),  # papel/papeis
    ]
    for sing, plur in pairs:
        for suf in (plur, sing):
            if t.endswith(suf) and len(t) - len(suf) >= 4:
                return t[: -len(suf)]
    return t


def format_entity_labels(
    cnpjs: list[str],
    nomes: dict[str, str],
    limit: int = 5,
) -> str:
    """Formata lista de CNPJs com nomes canonicos para mensagens de warning.

    Se count <= limit, retorna labels separados por virgula.
    Caso contrario, retorna '{count} entidades'.
    """
    labels = []
    for cnpj in cnpjs:
        nome = nomes.get(cnpj, "")
        label = f"{cnpj} ({nome})" if nome else cnpj
        labels.append(label)
    if len(labels) <= limit:
        return ", ".join(labels)
    return f"{len(labels)} entidades"
