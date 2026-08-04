def is_valid(val: object) -> bool:
    """Checa se valor ESCALAR e nao-nulo/nao-NaN.

    Substitui pd.notna() para valores individuais extraidos de
    DataFrames DuckDB. Funciona com None, float('nan'), numpy.nan,
    pd.NA (StringDtype) e pd.NaT.

    Explora auto-desigualdade IEEE 754: NaN != NaN, NaT != NaT.
    pd.NA == pd.NA retorna pd.NA (ambiguo), capturado via try/except.

    Somente escalares: com array ou Series, `val == val` produz um array e
    bool() levanta ValueError, que e capturado e retorna False. Para esses
    casos use pd.notna() diretamente.
    """
    if val is None:
        return False
    try:
        return bool(val == val)
    except (TypeError, ValueError):
        return False
