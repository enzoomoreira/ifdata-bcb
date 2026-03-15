"""Constantes centralizadas para fontes de dados."""

# Mapeamento escopo -> TipoInstituicao (IFDATA)
TIPO_INST_MAP: dict[str, int] = {
    "individual": 3,
    "prudencial": 1,
    "financeiro": 2,
}

# Configuracao das fontes de dados
DATA_SOURCES: dict[str, dict[str, str]] = {
    "cadastro": {
        "subdir": "ifdata/cadastro",
        "prefix": "ifdata_cad",
    },
    "ifdata_valores": {
        "subdir": "ifdata/valores",
        "prefix": "ifdata_val",
    },
    "cosif_individual": {
        "subdir": "cosif/individual",
        "prefix": "cosif_ind",
    },
    "cosif_prudencial": {
        "subdir": "cosif/prudencial",
        "prefix": "cosif_prud",
    },
}


# Primeiro periodo disponivel por fonte (YYYYMM).
# Valores conservadores baseados em testes empiricos (2026-03).
# Periodos anteriores retornam 404 no BCB.
FIRST_AVAILABLE_PERIOD: dict[str, int] = {
    "cosif_individual": 199501,
    "cosif_prudencial": 201407,
    "ifdata_valores": 200303,
    "cadastro": 200503,
}

# Reverse lookup: prefix -> source key
_PREFIX_TO_SOURCE: dict[str, str] = {
    cfg["prefix"]: key for key, cfg in DATA_SOURCES.items()
}


def get_source_key(prefix: str) -> str | None:
    """Retorna source key a partir do prefix do arquivo."""
    return _PREFIX_TO_SOURCE.get(prefix)


def get_first_available(prefix: str) -> int | None:
    """Retorna primeiro periodo disponivel para um prefix, ou None."""
    source = get_source_key(prefix)
    if source is None:
        return None
    return FIRST_AVAILABLE_PERIOD.get(source)


def get_pattern(source: str) -> str:
    """Retorna pattern glob para fonte de dados."""
    return f"{DATA_SOURCES[source]['prefix']}_*.parquet"


def get_subdir(source: str) -> str:
    """Retorna subdiretorio para fonte de dados."""
    return DATA_SOURCES[source]["subdir"]
