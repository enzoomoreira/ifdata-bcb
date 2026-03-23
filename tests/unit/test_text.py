"""Testes para ifdata_bcb.utils.text."""

import pytest

from ifdata_bcb.utils.text import (
    format_entity_labels,
    normalize_accents,
    normalize_text,
    stem_ptbr,
)


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


class TestFormatEntityLabels:
    """format_entity_labels: formata CNPJs com nomes para mensagens de warning."""

    def test_single_cnpj_with_nome(self) -> None:
        result = format_entity_labels(["60872504"], {"60872504": "ITAU UNIBANCO S.A."})
        assert result == "60872504 (ITAU UNIBANCO S.A.)"

    def test_single_cnpj_without_nome(self) -> None:
        result = format_entity_labels(["60872504"], {})
        assert result == "60872504"

    def test_single_cnpj_empty_nome(self) -> None:
        result = format_entity_labels(["60872504"], {"60872504": ""})
        assert result == "60872504"

    def test_multiple_cnpjs_within_limit(self) -> None:
        result = format_entity_labels(
            ["60872504", "90400888"],
            {"60872504": "ITAU", "90400888": "SANTANDER"},
        )
        assert "60872504 (ITAU)" in result
        assert "90400888 (SANTANDER)" in result
        assert ", " in result

    def test_exceeds_limit_returns_count(self) -> None:
        cnpjs = [f"{i:08d}" for i in range(6)]
        result = format_entity_labels(cnpjs, {}, limit=5)
        assert result == "6 entidades"

    def test_exactly_at_limit_returns_labels(self) -> None:
        cnpjs = [f"{i:08d}" for i in range(5)]
        result = format_entity_labels(cnpjs, {}, limit=5)
        assert "entidades" not in result
        assert "00000004" in result

    def test_empty_list(self) -> None:
        result = format_entity_labels([], {})
        assert result == ""

    def test_mixed_with_and_without_nome(self) -> None:
        result = format_entity_labels(
            ["60872504", "99999999"],
            {"60872504": "ITAU"},
        )
        assert "60872504 (ITAU)" in result
        assert "99999999" in result
        assert "()" not in result


class TestStemPtbr:
    """stem_ptbr: stemming PT-BR para busca singular/plural."""

    @pytest.mark.parametrize(
        "singular, plural, expected_stem",
        [
            ("operacao", "operacoes", "opera"),
            ("captacao", "captacoes", "capta"),
            ("aplicacao", "aplicacoes", "aplica"),
            ("provisao", "provisoes", "provi"),
            ("reducao", "reducoes", "redu"),
            ("informacao", "informacoes", "informa"),
            ("capital", "capitais", "capit"),
        ],
    )
    def test_singular_plural_produce_same_stem(
        self, singular: str, plural: str, expected_stem: str
    ) -> None:
        assert stem_ptbr(singular) == expected_stem
        assert stem_ptbr(plural) == expected_stem

    @pytest.mark.parametrize(
        "term",
        ["credito", "ativo", "lucro", "deposito", "patrimonio", "basileia", "rwa"],
    )
    def test_no_inflection_passthrough(self, term: str) -> None:
        assert stem_ptbr(term) == term

    def test_accent_insensitive(self) -> None:
        # Both accented and non-accented forms produce the same stem
        assert stem_ptbr("operacao") == stem_ptbr("operac\u00e3o")
        assert stem_ptbr("operacao") == "opera"

    def test_minimum_root_length_4(self) -> None:
        # A short word where stripping the suffix would leave < 4 chars
        # should pass through unchanged. "mao" -> root "m" (1 char) -> no strip.
        assert len(stem_ptbr("mao")) >= 4 or stem_ptbr("mao") == "mao"
        # "cao" has root "" -> passthrough
        assert stem_ptbr("cao") == "cao"
        # "leao" -> root "le" (2 chars) -> no strip
        assert stem_ptbr("leao") == "leao"
        # "uniao" -> root "uni" (3 chars) -> no strip
        assert stem_ptbr("uniao") == "uniao"

    def test_case_insensitive(self) -> None:
        assert stem_ptbr("OPERACAO") == stem_ptbr("operacao")
        assert stem_ptbr("Captacao") == stem_ptbr("captacao")

    def test_el_eis_pair(self) -> None:
        # "imovel/imoveis" -> root "imov" (4 chars, meets minimum)
        assert stem_ptbr("imovel") == stem_ptbr("imoveis")
        assert stem_ptbr("imovel") == "imov"

    def test_el_eis_pair_short_root_no_strip(self) -> None:
        # "papel" root "pap" (3 chars) < minimum 4 -> passthrough
        assert stem_ptbr("papel") == "papel"
        assert stem_ptbr("papeis") == "papeis"

    def test_al_ais_pair(self) -> None:
        assert stem_ptbr("capital") == stem_ptbr("capitais")
        assert stem_ptbr("capital") == "capit"
