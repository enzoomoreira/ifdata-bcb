"""Testes unitarios para infra/sql.py."""

import re

import pytest

from ifdata_bcb.infra.sql import (
    SqlCondition,
    build_account_condition,
    build_between_condition,
    build_in_clause,
    build_int_condition,
    build_like_condition,
    build_string_condition,
    join_conditions,
    merge_params,
)

# Os nomes de parametro vem de um contador global ao processo, entao o numero
# exato depende da ordem de execucao. As asserts casam a forma, nao o valor.
PH = r"\$p\d+"


def valores(cond: SqlCondition) -> list[object]:
    """Valores vinculados, na ordem em que aparecem no SQL."""
    return [cond.params[name] for name in re.findall(r"\$(p\d+)", cond)]


class TestBuildStringCondition:
    def test_single_value(self) -> None:
        cond = build_string_condition("col", ["abc"])
        assert re.fullmatch(rf"col = {PH}", cond)
        assert valores(cond) == ["abc"]

    def test_multiple_values(self) -> None:
        cond = build_string_condition("col", ["a", "b"])
        assert re.fullmatch(rf"col IN \({PH}, {PH}\)", cond)
        assert valores(cond) == ["a", "b"]

    def test_quotes_travel_as_value_not_sql(self) -> None:
        cond = build_string_condition("col", ["it's"])
        assert "it" not in cond  # o valor nao esta no texto da query
        assert valores(cond) == ["it's"]

    def test_case_insensitive(self) -> None:
        cond = build_string_condition("col", ["abc"], case_insensitive=True)
        assert "UPPER(" in cond
        assert valores(cond) == ["ABC"]

    def test_accent_insensitive(self) -> None:
        cond = build_string_condition("col", ["cafe"], accent_insensitive=True)
        assert "strip_accents(" in cond

    def test_both_insensitive(self) -> None:
        cond = build_string_condition(
            "col", ["cafe"], case_insensitive=True, accent_insensitive=True
        )
        assert "strip_accents(" in cond
        assert "UPPER(" in cond
        assert valores(cond) == ["CAFE"]

    def test_strips_whitespace(self) -> None:
        cond = build_string_condition("col", ["  abc  "])
        assert valores(cond) == ["abc"]


class TestBuildIntCondition:
    def test_single_value(self) -> None:
        cond = build_int_condition("col", [42])
        assert re.fullmatch(rf"col = {PH}", cond)
        assert valores(cond) == [42]

    def test_multiple_values(self) -> None:
        cond = build_int_condition("col", [1, 2])
        assert re.fullmatch(rf"col IN \({PH}, {PH}\)", cond)
        assert valores(cond) == [1, 2]

    def test_rejects_non_integer(self) -> None:
        """A anotacao list[int] nao e checada em runtime; o int() checa."""
        with pytest.raises(ValueError):
            build_int_condition("col", ["1 OR 1=1"])  # type: ignore[list-item]


class TestBuildBetweenCondition:
    def test_shape_and_values(self) -> None:
        cond = build_between_condition("AnoMes", 202301, 202312)
        assert re.fullmatch(rf"AnoMes BETWEEN {PH} AND {PH}", cond)
        assert valores(cond) == [202301, 202312]


class TestBuildAccountCondition:
    def test_or_structure(self) -> None:
        cond = build_account_condition("nome", "cod", ["abc"])
        assert "OR" in cond
        assert "strip_accents(" in cond
        assert "CAST(cod AS VARCHAR)" in cond

    def test_carries_params_of_both_sides(self) -> None:
        cond = build_account_condition("nome", "cod", ["abc"])
        assert len(cond.params) == 2
        assert valores(cond) == ["ABC", "ABC"]


class TestBuildLikeCondition:
    def test_basic(self) -> None:
        cond = build_like_condition("col", "abc")
        assert re.search(rf"LIKE {PH} ESCAPE '\$'", cond)
        assert "UPPER(strip_accents(col))" in cond
        assert valores(cond) == ["%ABC%"]

    def test_quotes_travel_as_value(self) -> None:
        cond = build_like_condition("col", "it's")
        assert valores(cond) == ["%IT'S%"]

    def test_no_accent(self) -> None:
        cond = build_like_condition("col", "abc", accent_insensitive=False)
        assert "strip_accents" not in cond
        assert "UPPER(col)" in cond

    def test_no_case(self) -> None:
        cond = build_like_condition("col", "ABC", case_insensitive=False)
        assert "UPPER" not in cond
        assert "strip_accents(col)" in cond

    def test_escapes_percent(self) -> None:
        cond = build_like_condition("col", "100%")
        assert valores(cond) == ["%100$%%"]
        assert "ESCAPE '$'" in cond

    def test_escapes_underscore(self) -> None:
        cond = build_like_condition("col", "conta_x")
        assert valores(cond) == ["%CONTA$_X%"]

    def test_escapes_dollar_sign(self) -> None:
        cond = build_like_condition("col", "R$100")
        assert valores(cond) == ["%R$$100%"]


class TestJoinConditions:
    def test_filters_none(self) -> None:
        assert join_conditions(["a = 1", None, "b = 2"]) == "a = 1 AND b = 2"

    def test_all_none(self) -> None:
        assert join_conditions([None, None]) is None

    def test_single(self) -> None:
        assert join_conditions(["a = 1"]) == "a = 1"

    def test_empty_list(self) -> None:
        assert join_conditions([]) is None

    def test_filters_empty_strings(self) -> None:
        assert join_conditions(["a = 1", "", None, "b = 2"]) == "a = 1 AND b = 2"

    def test_all_empty_returns_none(self) -> None:
        assert join_conditions(["", ""]) is None

    def test_merges_params_of_all_fragments(self) -> None:
        a = build_string_condition("x", ["um"])
        b = build_int_condition("y", [2])
        joined = join_conditions([a, None, b])
        assert joined is not None
        assert joined.params == {**a.params, **b.params}
        assert valores(joined) == ["um", 2]

    def test_drops_params_of_discarded_fragments(self) -> None:
        """Fragmento filtrado nao pode deixar param orfao -- o DuckDB recusa."""
        a = build_string_condition("x", ["um"])
        joined = join_conditions([a, None, ""])
        assert joined is not None
        assert joined.params == a.params


class TestMergeParams:
    def test_ignores_plain_str_and_none(self) -> None:
        cond = build_string_condition("x", ["a"])
        assert merge_params(cond, "rn = 1", None) == cond.params

    def test_empty(self) -> None:
        assert merge_params() == {}


class TestBuildInClause:
    def test_basic(self) -> None:
        cond = build_in_clause(["a", "b"])
        assert re.fullmatch(rf"{PH}, {PH}", cond)
        assert valores(cond) == ["a", "b"]

    def test_quotes_travel_as_value(self) -> None:
        cond = build_in_clause(["it's"])
        assert valores(cond) == ["it's"]


# =========================================================================
# Testes adversariais -- edge cases e inputs hostis
# =========================================================================


class TestBuildStringConditionAdversarial:
    def test_empty_list_raises(self) -> None:
        with pytest.raises(ValueError, match="must not be empty"):
            build_string_condition("col", [])

    def test_whitespace_only_values(self) -> None:
        cond = build_string_condition("col", ["  "])
        assert valores(cond) == [""]

    def test_sql_injection_attempt(self) -> None:
        payload = "'; DROP TABLE users; --"
        cond = build_string_condition("col", [payload])
        assert "DROP TABLE" not in cond
        assert valores(cond) == [payload]

    def test_unicode_multibyte(self) -> None:
        cond = build_string_condition("col", ["ção"])
        assert valores(cond) == ["ção"]


class TestBuildIntConditionAdversarial:
    def test_empty_list_raises(self) -> None:
        with pytest.raises(ValueError, match="must not be empty"):
            build_int_condition("col", [])

    def test_negative_and_zero(self) -> None:
        cond = build_int_condition("col", [-1, 0])
        assert valores(cond) == [-1, 0]


class TestBuildInClauseAdversarial:
    def test_empty_list_raises(self) -> None:
        with pytest.raises(ValueError, match="must not be empty"):
            build_in_clause([])

    def test_single_value_no_comma(self) -> None:
        assert "," not in build_in_clause(["abc"])


class TestBuildLikeConditionAdversarial:
    def test_empty_term_matches_all(self) -> None:
        cond = build_like_condition("col", "")
        assert valores(cond) == ["%%"]

    def test_all_metacharacters_combined(self) -> None:
        """%, _, e $ sao todos escapados corretamente na mesma string."""
        cond = build_like_condition("col", "%_$")
        assert valores(cond) == ["%$%$_$$%"]

    def test_unicode_accent_stripping(self) -> None:
        cond = build_like_condition("col", "café")
        assert valores(cond) == ["%CAFE%"]
        assert "strip_accents(" in cond


class TestBuildAccountConditionAdversarial:
    def test_empty_values_raises(self) -> None:
        with pytest.raises(ValueError, match="must not be empty"):
            build_account_condition("nome", "cod", [])


# =========================================================================
# Injecao SQL
#
# Com os valores vinculados como parametros, nao ha literal para fechar: o
# DuckDB recebe o payload como dado, nunca como texto de query. Os testes
# abaixo executam de verdade, contra uma tabela real.
#
# O vetor historico era a normalizacao Unicode: NFKD decompoe compatibilidade,
# nao apenas acentos, e U+FF07 (FULLWIDTH APOSTROPHE) vira uma aspa simples
# ASCII. Com escape textual, normalizar depois de escapar deixava essa aspa
# sem escape. U+FF07 e o unico codepoint que decompoe para "'" (varredura
# completa de U+0020 a U+11000).
# =========================================================================

FULLWIDTH_APOSTROPHE = "＇"  # noqa: RUF001 -- a ambiguidade e o objeto do teste

# Fecha o literal, injeta OR 1=1 e reabre para manter o SQL sintaticamente valido
INJECTION_PAYLOAD = f"{FULLWIDTH_APOSTROPHE} OR 1=1 OR {FULLWIDTH_APOSTROPHE}"


@pytest.fixture
def conn():
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE t (NOME_CONTA VARCHAR, CODIGO INT, SALDO INT)")
    con.execute("INSERT INTO t VALUES ('CAIXA', 10, 100), ('TITULOS', 20, 200)")
    yield con
    con.close()


def _select(conn, cond: SqlCondition) -> list[tuple]:
    return conn.execute(f"SELECT * FROM t WHERE {cond}", cond.params).fetchall()


class TestUnicodeNormalizationInjection:
    """O payload nao pode escapar do literal SQL e virar predicado."""

    def test_string_condition_does_not_escape_literal(self, conn) -> None:
        cond = build_string_condition(
            "NOME_CONTA",
            [INJECTION_PAYLOAD],
            case_insensitive=True,
            accent_insensitive=True,
        )
        assert _select(conn, cond) == [], f"filtro burlado: {cond}"

    def test_like_condition_does_not_escape_literal(self, conn) -> None:
        cond = build_like_condition("NOME_CONTA", INJECTION_PAYLOAD)
        assert _select(conn, cond) == [], f"filtro burlado: {cond}"

    def test_account_condition_does_not_escape_literal(self, conn) -> None:
        cond = build_account_condition("NOME_CONTA", "CODIGO", [INJECTION_PAYLOAD])
        assert _select(conn, cond) == [], f"filtro burlado: {cond}"

    def test_in_clause_with_multiple_values(self, conn) -> None:
        """Caminho IN (>1 valor) monta os placeholders separadamente do '='."""
        cond = build_string_condition(
            "NOME_CONTA",
            [INJECTION_PAYLOAD, "OUTRA"],
            case_insensitive=True,
            accent_insensitive=True,
        )
        assert _select(conn, cond) == [], f"filtro burlado: {cond}"

    def test_payload_never_reaches_the_query_text(self) -> None:
        """A garantia estrutural: nenhum caractere do payload entra no SQL."""
        cond = build_string_condition(
            "col", [INJECTION_PAYLOAD], accent_insensitive=True
        )
        assert "OR 1=1" not in cond
        assert "'" not in cond
        assert re.fullmatch(rf"strip_accents\(col\) = {PH}", cond)

    def test_subquery_exfiltration_blocked(self, conn) -> None:
        payload = (
            f"{FULLWIDTH_APOSTROPHE} OR (SELECT MAX(SALDO) FROM t) = 200 OR "
            f"{FULLWIDTH_APOSTROPHE}"
        )
        cond = build_string_condition(
            "NOME_CONTA", [payload], case_insensitive=True, accent_insensitive=True
        )
        assert _select(conn, cond) == [], f"subquery executada: {cond}"

    def test_placeholder_lookalike_in_value_is_inert(self, conn) -> None:
        """Um valor que parece placeholder e dado, nao referencia de param."""
        cond = build_string_condition("NOME_CONTA", ["$p0"])
        assert _select(conn, cond) == []

    def test_legitimate_accented_value_still_matches(self, conn) -> None:
        """A parametrizacao nao pode quebrar o proposito da normalizacao."""
        conn.execute("INSERT INTO t VALUES ('OPERACOES', 30, 300)")
        cond = build_string_condition(
            "NOME_CONTA",
            ["operações"],
            case_insensitive=True,
            accent_insensitive=True,
        )
        rows = _select(conn, cond)
        assert len(rows) == 1
        assert rows[0][0] == "OPERACOES"
