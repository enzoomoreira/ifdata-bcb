"""Testes para ifdata_bcb.utils.cnpj."""

from ifdata_bcb.utils.cnpj import standardize_cnpj_base8


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
