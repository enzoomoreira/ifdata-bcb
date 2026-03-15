"""Testes para ifdata_bcb.utils.text."""

from ifdata_bcb.utils.text import normalize_accents, normalize_text


class TestNormalizeAccents:
    """normalize_accents: remove acentos preservando letras base."""

    def test_removes_acute(self) -> None:
        assert normalize_accents("cafe") == "cafe"
        assert normalize_accents("acucar") == "acucar"

    def test_removes_tilde(self) -> None:
        assert normalize_accents("organizacao") == "organizacao"

    def test_removes_cedilla(self) -> None:
        assert normalize_accents("acai") == "acai"

    def test_common_bank_names(self) -> None:
        assert normalize_accents("BANCO DO BRASIL S.A.") == "BANCO DO BRASIL S.A."

    def test_accented_uppercase(self) -> None:
        # "INSTITUICAO" com acentos deve virar sem acentos
        result = normalize_accents("\u00cdNDICE")
        assert result == "INDICE"

    def test_empty_string(self) -> None:
        assert normalize_accents("") == ""

    def test_no_accents_unchanged(self) -> None:
        assert normalize_accents("BANCO ITAU") == "BANCO ITAU"

    def test_non_string_passthrough(self) -> None:
        assert normalize_accents(123) == 123
        assert normalize_accents(None) is None

    def test_mixed_accents(self) -> None:
        # e com agudo, a com til, c com cedilha
        result = normalize_accents("\u00e9 \u00e3 \u00e7")
        assert result == "e a c"


class TestNormalizeText:
    """normalize_text: colapsa whitespace em espaco unico."""

    def test_multiple_spaces(self) -> None:
        assert normalize_text("hello   world") == "hello world"

    def test_tabs_and_newlines(self) -> None:
        assert normalize_text("hello\t\nworld") == "hello world"

    def test_leading_trailing_whitespace(self) -> None:
        assert normalize_text("  hello  ") == "hello"

    def test_empty_string(self) -> None:
        assert normalize_text("") == ""

    def test_single_word(self) -> None:
        assert normalize_text("word") == "word"

    def test_non_string_passthrough(self) -> None:
        assert normalize_text(42) == 42
        assert normalize_text(None) is None

    def test_already_normalized(self) -> None:
        assert normalize_text("hello world") == "hello world"

    def test_only_whitespace(self) -> None:
        assert normalize_text("   \t\n  ") == ""
