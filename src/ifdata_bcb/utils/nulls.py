def is_valid(val: object) -> bool:
    """Checa se valor escalar e nao-nulo/nao-NaN.

    Substitui pd.notna() para valores individuais extraidos de
    DataFrames DuckDB. Funciona com None, float('nan'), numpy.nan,
    pd.NA (StringDtype) e pd.NaT.

    Explora auto-desigualdade IEEE 754: NaN != NaN, NaT != NaT.
    pd.NA == pd.NA retorna pd.NA (ambiguo), capturado via try/except.
    """
    if val is None:
        return False
    try:
        return bool(val == val)
    except (TypeError, ValueError):
        return False
