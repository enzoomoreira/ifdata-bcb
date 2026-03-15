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


def get_pattern(source: str) -> str:
    """Retorna pattern glob para fonte de dados."""
    return f"{DATA_SOURCES[source]['prefix']}_*.parquet"


def get_subdir(source: str) -> str:
    """Retorna subdiretorio para fonte de dados."""
    return DATA_SOURCES[source]["subdir"]
