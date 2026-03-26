"""Funcoes de construcao de condicoes SQL para DuckDB."""

from ifdata_bcb.utils.text import normalize_accents


def build_string_condition(
    column: str,
    values: list[str],
    case_insensitive: bool = False,
    accent_insensitive: bool = False,
) -> str:
    """Constroi condicao para valores string com escape de aspas."""
    if not values:
        raise ValueError("values must not be empty")
    escaped = [v.strip().replace("'", "''") for v in values]
    col_expr = column

    if accent_insensitive:
        col_expr = f"strip_accents({col_expr})"
        escaped = [normalize_accents(v) for v in escaped]

    if case_insensitive:
        col_expr = f"UPPER({col_expr})"
        escaped = [v.upper() for v in escaped]

    if len(escaped) == 1:
        return f"{col_expr} = '{escaped[0]}'"
    values_str = ", ".join(f"'{v}'" for v in escaped)
    return f"{col_expr} IN ({values_str})"


def build_int_condition(column: str, values: list[int]) -> str:
    """Constroi condicao para valores inteiros."""
    if not values:
        raise ValueError("values must not be empty")
    if len(values) == 1:
        return f"{column} = {values[0]}"
    values_str = ", ".join(str(v) for v in values)
    return f"{column} IN ({values_str})"


def build_account_condition(
    name_col: str,
    code_col: str,
    values: list[str],
) -> str:
    """Match por nome (accent/case insensitive) OU por codigo."""
    name_cond = build_string_condition(
        name_col,
        values,
        case_insensitive=True,
        accent_insensitive=True,
    )
    code_cond = build_string_condition(
        f"CAST({code_col} AS VARCHAR)",
        values,
        case_insensitive=True,
    )
    return f"({name_cond} OR {code_cond})"


def _escape_like_meta(term: str, esc: str = "$") -> str:
    """Escapa metacaracteres LIKE (%, _) com o caractere de escape."""
    return term.replace(esc, esc + esc).replace("%", esc + "%").replace("_", esc + "_")


def build_like_condition(
    column: str,
    term: str,
    case_insensitive: bool = True,
    accent_insensitive: bool = True,
) -> str:
    """Constroi condicao LIKE para busca textual parcial."""
    term_clean = term.strip().replace("'", "''")
    col_expr = column

    if accent_insensitive:
        col_expr = f"strip_accents({col_expr})"
        term_clean = normalize_accents(term_clean)

    if case_insensitive:
        col_expr = f"UPPER({col_expr})"
        term_clean = term_clean.upper()

    term_clean = _escape_like_meta(term_clean)
    return f"{col_expr} LIKE '%{term_clean}%' ESCAPE '$'"


def join_conditions(conditions: list[str | None]) -> str | None:
    """Junta condicoes com AND, ignorando None e strings vazias."""
    valid = [c for c in conditions if c]
    return " AND ".join(valid) if valid else None


def escape_sql_string(value: str) -> str:
    """Escapa aspas simples para SQL."""
    return value.replace("'", "''")


def build_in_clause(values: list[str], escape: bool = True) -> str:
    """Constroi lista SQL IN: 'a', 'b', 'c'."""
    if not values:
        raise ValueError("values must not be empty")
    if escape:
        values = [escape_sql_string(v) for v in values]
    return ", ".join(f"'{v}'" for v in values)
