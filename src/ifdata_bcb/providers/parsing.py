"""
Deteccao de perda silenciosa de dados no parsing de CSVs do BCB.

Os collectors usam `ignore_errors=true` (descarta linhas malformadas) e
`TRY_CAST` (converte valor nao-parseavel em NULL). Ambos sao necessarios --
os CSVs do BCB tem linhas sujas -- mas sem contagem uma mudanca de formato
do BCB (separador decimal, coluna renomeada) zeraria os dados sem nenhum
sinal para o usuario.

Este modulo compara o que foi lido com o que era esperado e emite
PartialDataWarning quando a perda passa do residual.
"""

from pathlib import Path

import duckdb
import pandas as pd

from ifdata_bcb.domain.exceptions import PartialDataWarning
from ifdata_bcb.infra.log import emit_user_warning

# Abaixo disso a perda e considerada ruido normal dos CSVs do BCB.
DROP_RATIO_THRESHOLD = 0.01

_NULL_TOKENS = {"", "null", "none", "nan"}


def count_parseable_rows(
    cursor: duckdb.DuckDBPyConnection,
    csv_path: Path,
    delim: str,
    skip: int = 0,
    encoding: str | None = None,
) -> int | None:
    """
    Conta linhas do CSV sem conversao de tipo (all_varchar).

    Serve de referencia para o total real: a query tipada do collector pode
    descartar linhas por erro de conversao, esta nao. Retorna None se a
    contagem falhar -- a verificacao e diagnostica e nunca deve quebrar a
    coleta.
    """
    path_str = str(csv_path).replace("\\", "/")
    opts = [f"'{path_str}'", f"delim='{delim}'", "header=true", "all_varchar=true"]
    if skip:
        opts.append(f"skip={skip}")
    if encoding:
        opts.append(f"encoding='{encoding}'")

    try:
        result = cursor.sql(
            f"SELECT COUNT(*) FROM read_csv({', '.join(opts)})"
        ).fetchone()
    except duckdb.Error:
        return None
    return result[0] if result else None


def warn_if_rows_dropped(
    source: str, rows_read: int, rows_expected: int | None
) -> None:
    """Avisa se o parser tipado descartou uma fracao relevante das linhas."""
    if not rows_expected or rows_expected <= 0:
        return

    dropped = rows_expected - rows_read
    if dropped <= 0:
        return

    ratio = dropped / rows_expected
    if ratio < DROP_RATIO_THRESHOLD:
        return

    emit_user_warning(
        PartialDataWarning(
            f"{source}: {dropped:,} de {rows_expected:,} linhas ({ratio:.1%}) foram "
            f"descartadas por erro de formato durante a leitura do CSV. Isso pode "
            f"indicar mudanca de layout na fonte do BCB.",
            reason="rows_dropped",
            detail={"dropped": dropped, "expected": rows_expected},
        ),
        stacklevel=3,
    )


def warn_if_values_nulled(
    source: str,
    column: str,
    raw: pd.Series,
    parsed: pd.Series,
) -> None:
    """
    Avisa se TRY_CAST anulou valores que existiam no CSV.

    Um separador decimal diferente no arquivo do BCB zeraria todos os saldos
    sem levantar erro nenhum -- este e o sinal que denuncia isso.
    """
    if raw.empty:
        return

    raw_str = raw.astype("string").str.strip()
    had_value = raw_str.notna() & ~raw_str.str.lower().isin(_NULL_TOKENS)
    lost = int((parsed.isna() & had_value).sum())
    if lost == 0:
        return

    total = int(had_value.sum())
    if total <= 0:
        return

    ratio = lost / total
    if ratio < DROP_RATIO_THRESHOLD:
        return

    emit_user_warning(
        PartialDataWarning(
            f"{source}: {lost:,} de {total:,} valores ({ratio:.1%}) da coluna "
            f"{column} nao puderam ser convertidos para numero e viraram NULL. "
            f"Isso pode indicar mudanca no formato numerico da fonte do BCB.",
            reason="values_nulled",
            detail={"column": column, "nulled": lost, "total": total},
        ),
        stacklevel=3,
    )
