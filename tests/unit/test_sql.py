"""Testes unitarios para infra/sql.py."""

import pytest

from ifdata_bcb.infra.sql import (
    build_account_condition,
    build_in_clause,
    build_int_condition,
    build_like_condition,
    build_string_condition,
    escape_sql_string,
    join_conditions,
)


class TestBuildStringCondition:
    def test_single_value(self) -> None:
        assert build_string_condition("col", ["abc"]) == "col = 'abc'"

    def test_multiple_values(self) -> None:
        result = build_string_condition("col", ["a", "b"])
        assert result == "col IN ('a', 'b')"

    def test_escape_quotes(self) -> None:
        result = build_string_condition("col", ["it's"])
        assert "it''s" in result

    def test_case_insensitive(self) -> None:
        result = build_string_condition("col", ["abc"], case_insensitive=True)
        assert "UPPER(" in result
        assert "ABC" in result

    def test_accent_insensitive(self) -> None:
        result = build_string_condition("col", ["cafe"], accent_insensitive=True)
        assert "strip_accents(" in result

    def test_both_insensitive(self) -> None:
        result = build_string_condition(
            "col", ["cafe"], case_insensitive=True, accent_insensitive=True
        )
        assert "strip_accents(" in result
        assert "UPPER(" in result
        assert "CAFE" in result

    def test_strips_whitespace(self) -> None:
        result = build_string_condition("col", ["  abc  "])
        assert result == "col = 'abc'"


class TestBuildIntCondition:
    def test_single_value(self) -> None:
        assert build_int_condition("col", [42]) == "col = 42"

    def test_multiple_values(self) -> None:
        assert build_int_condition("col", [1, 2]) == "col IN (1, 2)"


class TestBuildAccountCondition:
    def test_or_structure(self) -> None:
        result = build_account_condition("nome", "cod", ["abc"])
        assert "OR" in result
        assert "strip_accents(" in result
        assert "CAST(cod AS VARCHAR)" in result


class TestBuildLikeCondition:
    def test_basic(self) -> None:
        result = build_like_condition("col", "abc")
        assert "LIKE '%ABC%' ESCAPE '$'" in result
        assert "UPPER(strip_accents(col))" in result

    def test_escapes_quotes(self) -> None:
        result = build_like_condition("col", "it's")
        assert "it''s" in result.upper() or "IT''S" in result

    def test_no_accent(self) -> None:
        result = build_like_condition("col", "abc", accent_insensitive=False)
        assert "strip_accents" not in result
        assert "UPPER(col)" in result

    def test_no_case(self) -> None:
        result = build_like_condition("col", "ABC", case_insensitive=False)
        assert "UPPER" not in result
        assert "strip_accents(col)" in result

    def test_escapes_percent(self) -> None:
        result = build_like_condition("col", "100%")
        assert "100$%" in result
        assert "ESCAPE '$'" in result

    def test_escapes_underscore(self) -> None:
        result = build_like_condition("col", "conta_x")
        assert "CONTA$_X" in result

    def test_escapes_dollar_sign(self) -> None:
        result = build_like_condition("col", "R$100")
        assert "R$$100" in result


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


class TestEscapeSqlString:
    def test_escapes_single_quote(self) -> None:
        assert escape_sql_string("it's") == "it''s"

    def test_no_quotes(self) -> None:
        assert escape_sql_string("abc") == "abc"


class TestBuildInClause:
    def test_basic(self) -> None:
        assert build_in_clause(["a", "b"]) == "'a', 'b'"

    def test_escapes_by_default(self) -> None:
        assert build_in_clause(["it's"]) == "'it''s'"

    def test_no_escape(self) -> None:
        assert build_in_clause(["it's"], escape=False) == "'it's'"


# =========================================================================
# Testes adversariais -- edge cases e inputs hostis
# =========================================================================


class TestBuildStringConditionAdversarial:
    def test_empty_list_raises(self) -> None:
        with pytest.raises(ValueError, match="must not be empty"):
            build_string_condition("col", [])

    def test_whitespace_only_values(self) -> None:
        """Strip produz strings vazias -- SQL valido com valor vazio."""
        result = build_string_condition("col", ["  "])
        assert result == "col = ''"

    def test_sql_injection_attempt(self) -> None:
        """Aspas escapadas impedem quebra da string SQL."""
        result = build_string_condition("col", ["'; DROP TABLE users; --"])
        assert "DROP TABLE" in result
        assert "''; DROP TABLE users; --'" in result
        # A aspa e escapada: '' nao fecha a string SQL

    def test_unicode_multibyte(self) -> None:
        result = build_string_condition("col", ["\u00e7\u00e3o"])
        assert "\u00e7\u00e3o" in result


class TestBuildIntConditionAdversarial:
    def test_empty_list_raises(self) -> None:
        with pytest.raises(ValueError, match="must not be empty"):
            build_int_condition("col", [])

    def test_negative_and_zero(self) -> None:
        result = build_int_condition("col", [-1, 0])
        assert result == "col IN (-1, 0)"


class TestBuildInClauseAdversarial:
    def test_empty_list_raises(self) -> None:
        with pytest.raises(ValueError, match="must not be empty"):
            build_in_clause([])

    def test_single_value_no_comma(self) -> None:
        assert build_in_clause(["abc"]) == "'abc'"
        assert "," not in build_in_clause(["abc"])


class TestBuildLikeConditionAdversarial:
    def test_empty_term_matches_all(self) -> None:
        result = build_like_condition("col", "")
        assert "LIKE '%%'" in result

    def test_all_metacharacters_combined(self) -> None:
        """%, _, e $ sao todos escapados corretamente na mesma string."""
        result = build_like_condition("col", "%_$")
        assert "$%" in result
        assert "$_" in result
        assert "$$" in result

    def test_unicode_accent_stripping(self) -> None:
        result = build_like_condition("col", "caf\u00e9")
        assert "CAFE" in result
        assert "strip_accents(" in result


class TestBuildAccountConditionAdversarial:
    def test_empty_values_raises(self) -> None:
        with pytest.raises(ValueError, match="must not be empty"):
            build_account_condition("nome", "cod", [])
