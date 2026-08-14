"""Testes para ifdata_bcb.utils.cnpj."""

import pytest

from ifdata_bcb.utils.cnpj import is_valid_cnpj14, only_digits, standardize_cnpj_base8


class TestOnlyDigits:
    def test_remove_formatacao(self) -> None:
        assert only_digits("60.872.504/0001-23") == "60872504000123"

    def test_ignora_digitos_fullwidth(self) -> None:
        """`\\d` casaria U+FF10..U+FF19 e int() os converteria em silencio."""
        assert only_digits("".join(chr(0xFF10 + i) for i in range(1, 9))) == ""

    def test_aceita_int(self) -> None:
        assert only_digits(12345678) == "12345678"


class TestIsValidCnpj14:
    @pytest.mark.parametrize(
        "cnpj",
        [
            "60872504000123",  # Itau Unibanco Holding
            "60746948000112",  # Bradesco
            "00000000000191",  # Banco do Brasil
        ],
    )
    def test_cnpjs_reais_passam(self, cnpj: str) -> None:
        assert is_valid_cnpj14(cnpj) is True

    @pytest.mark.parametrize(
        "cnpj",
        [
            "60872504000124",  # DV trocado
            "99999999999999",
            "11111111111111",
            "6087250400012",  # 13 digitos
            "608725040001234",  # 15 digitos
            "6087250400012a",
            "",
        ],
    )
    def test_invalidos_falham(self, cnpj: str) -> None:
        assert is_valid_cnpj14(cnpj) is False


class TestStandardizeCnpjBase8:
    """standardize_cnpj_base8: padroniza CNPJ para 8 digitos."""

    def test_none_returns_none(self) -> None:
        assert standardize_cnpj_base8(None) is None

    def test_empty_string_returns_none(self) -> None:
        assert standardize_cnpj_base8("") is None

    def test_whitespace_only_returns_none(self) -> None:
        assert standardize_cnpj_base8("   ") is None

    def test_non_numeric_returns_none(self) -> None:
        assert standardize_cnpj_base8("abcdefgh") is None

    def test_already_8_digits(self) -> None:
        assert standardize_cnpj_base8("12345678") == "12345678"

    def test_short_input_gets_zero_padded(self) -> None:
        # "123" -> zfill(8) -> "00000123" -> [:8] -> "00000123"
        assert standardize_cnpj_base8("123") == "00000123"

    def test_single_digit(self) -> None:
        assert standardize_cnpj_base8("5") == "00000005"

    def test_long_cnpj_truncated_to_8(self) -> None:
        # CNPJ completo de 14 digitos: deve pegar apenas os 8 primeiros
        assert standardize_cnpj_base8("12345678000195") == "12345678"

    def test_formatted_cnpj_cleaned(self) -> None:
        # "12.345.678/0001-95" -> "12345678000195" -> zfill(8) -> "12345678"
        assert standardize_cnpj_base8("12.345.678/0001-95") == "12345678"

    def test_cnpj_with_dashes_only(self) -> None:
        assert standardize_cnpj_base8("1234-5678") == "12345678"

    def test_numeric_int_input(self) -> None:
        # int 12345678 -> str -> cleaned "12345678"
        assert standardize_cnpj_base8(12345678) == "12345678"

    def test_numeric_int_small(self) -> None:
        # int 1 -> str "1" -> zfill(8) -> "00000001"
        assert standardize_cnpj_base8(1) == "00000001"

    def test_leading_zeros_preserved(self) -> None:
        assert standardize_cnpj_base8("00000001") == "00000001"

    def test_whitespace_stripped(self) -> None:
        assert standardize_cnpj_base8("  12345678  ") == "12345678"

    def test_mixed_chars_and_digits(self) -> None:
        # "abc123def456" -> cleaned "123456" -> zfill(8) -> "00123456"
        assert standardize_cnpj_base8("abc123def456") == "00123456"
