"""Construcao de condicoes SQL parametrizadas para DuckDB.

Valores nunca entram no texto da query. Cada um vira um parametro nomeado
(`$p0`, `$p1`, ...) e viaja separado, em `SqlCondition.params`, ate o bind do
DuckDB. Isso elimina a classe inteira de bug de injecao: nao ha escape cuja
ordem de aplicacao possa estar errada -- como esteve, quando a normalizacao
NFKD rodava depois do escape de aspas e reintroduzia uma aspa ASCII a partir
de U+FF07.

Um placeholder por valor, e nao `IN (SELECT unnest($lista))`: a forma com
subquery faz o DuckDB perder o filter pushdown para o parquet.

Quem executa a query precisa repassar os params. `QueryEngine.read_glob()` faz
isso sozinho quando recebe um `SqlCondition` em `where`; queries montadas a mao
e enviadas por `QueryEngine.sql()` devem passar `params=merge_params(...)`.
"""

from collections.abc import Iterable, Mapping
from itertools import count

from ifdata_bcb.utils.text import normalize_accents

# Monotonico por processo: dois fragmentos nunca disputam o mesmo nome, entao
# mesclar params de fragmentos independentes e sempre seguro.
_param_counter = count()


class SqlCondition(str):
    """Fragmento SQL junto dos valores que seus placeholders esperam.

    E uma subclasse de `str` para que a interpolacao textual continue
    funcionando onde ja funcionava. O contrapeso: qualquer operacao de string
    (`+`, f-string, `"".join`) devolve `str` puro e descarta os params. Para
    compor fragmentos preservando os valores, use `join_conditions()`; para
    coletar params antes de executar, use `merge_params()`.

    Perder os params nao produz resultado errado em silencio -- o DuckDB
    recusa a query com placeholder sem valor.
    """

    params: Mapping[str, object]

    def __new__(cls, sql: str, params: Mapping[str, object]) -> "SqlCondition":
        obj = super().__new__(cls, sql)
        obj.params = dict(params)
        return obj


def merge_params(*fragments: object) -> dict[str, object]:
    """Junta os params de fragmentos `SqlCondition`, ignorando `str` puro e None."""
    merged: dict[str, object] = {}
    for fragment in fragments:
        merged.update(getattr(fragment, "params", {}))
    return merged


def _bind(values: Iterable[object]) -> tuple[list[str], dict[str, object]]:
    """Gera um placeholder nomeado por valor."""
    holders: list[str] = []
    params: dict[str, object] = {}
    for value in values:
        name = f"p{next(_param_counter)}"
        holders.append(f"${name}")
        params[name] = value
    return holders, params


def _equality(col_expr: str, holders: list[str]) -> str:
    if len(holders) == 1:
        return f"{col_expr} = {holders[0]}"
    return f"{col_expr} IN ({', '.join(holders)})"


def build_string_condition(
    column: str,
    values: list[str],
    case_insensitive: bool = False,
    accent_insensitive: bool = False,
) -> SqlCondition:
    """Constroi condicao de igualdade/IN para valores string."""
    if not values:
        raise ValueError("values must not be empty")
    normalized = [v.strip() for v in values]
    col_expr = column

    if accent_insensitive:
        col_expr = f"strip_accents({col_expr})"
        normalized = [normalize_accents(v) for v in normalized]

    if case_insensitive:
        col_expr = f"UPPER({col_expr})"
        normalized = [v.upper() for v in normalized]

    holders, params = _bind(normalized)
    return SqlCondition(_equality(col_expr, holders), params)


def build_int_condition(column: str, values: list[int]) -> SqlCondition:
    """Constroi condicao de igualdade/IN para valores inteiros.

    Raises:
        ValueError: Se values for vazio ou contiver algo que nao seja inteiro.
    """
    if not values:
        raise ValueError("values must not be empty")
    holders, params = _bind(int(v) for v in values)
    return SqlCondition(_equality(column, holders), params)


def build_between_condition(column: str, low: int, high: int) -> SqlCondition:
    """Constroi condicao BETWEEN para um intervalo de periodos YYYYMM."""
    holders, params = _bind([int(low), int(high)])
    return SqlCondition(f"{column} BETWEEN {holders[0]} AND {holders[1]}", params)


def build_account_condition(
    name_col: str,
    code_col: str,
    values: list[str],
) -> SqlCondition:
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
    return SqlCondition(
        f"({name_cond} OR {code_cond})",
        merge_params(name_cond, code_cond),
    )


def _escape_like_meta(term: str, esc: str = "$") -> str:
    """Escapa metacaracteres LIKE (%, _) com o caractere de escape."""
    return term.replace(esc, esc + esc).replace("%", esc + "%").replace("_", esc + "_")


def build_like_condition(
    column: str,
    term: str,
    case_insensitive: bool = True,
    accent_insensitive: bool = True,
) -> SqlCondition:
    """Constroi condicao LIKE para busca textual parcial."""
    term_clean = term.strip()
    col_expr = column

    if accent_insensitive:
        col_expr = f"strip_accents({col_expr})"
        term_clean = normalize_accents(term_clean)

    if case_insensitive:
        col_expr = f"UPPER({col_expr})"
        term_clean = term_clean.upper()

    # O ESCAPE '$' e literal na query; '$' seguido de aspa nao forma nome de
    # parametro, entao nao colide com a sintaxe $nome do bind.
    holders, params = _bind([f"%{_escape_like_meta(term_clean)}%"])
    return SqlCondition(f"{col_expr} LIKE {holders[0]} ESCAPE '$'", params)


def join_conditions(conditions: list[str | None]) -> SqlCondition | None:
    """Junta condicoes com AND, ignorando None e strings vazias.

    Os params dos fragmentos descartados saem junto: so viaja o valor cujo
    placeholder sobreviveu ao filtro.
    """
    valid = [c for c in conditions if c]
    if not valid:
        return None
    return SqlCondition(" AND ".join(valid), merge_params(*valid))


def build_in_clause(values: list[str]) -> SqlCondition:
    """Constroi a lista de uma clausula IN: `$p0, $p1, $p2`."""
    if not values:
        raise ValueError("values must not be empty")
    holders, params = _bind(values)
    return SqlCondition(", ".join(holders), params)
