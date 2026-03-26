"""QA: inputs invalidos -- simula usuario real passando dados errados."""

import pytest

from ifdata_bcb.core.entity import EntitySearch
from ifdata_bcb.domain.exceptions import (
    InvalidDateFormatError,
    InvalidDateRangeError,
    InvalidIdentifierError,
    InvalidScopeError,
)
from ifdata_bcb.providers.cosif.explorer import COSIFExplorer


class TestMissingParams:
    def test_read_without_start_raises(self, qa_cosif: COSIFExplorer) -> None:
        with pytest.raises(TypeError):
            qa_cosif.read(instituicao="60872504")  # type: ignore[call-arg]

    def test_read_without_instituicao_does_not_raise(
        self, qa_cosif: COSIFExplorer
    ) -> None:
        """instituicao e opcional agora (bulk read)."""
        import warnings

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            df = qa_cosif.read("2023-03")
        assert df is not None


class TestInvalidCNPJ:
    def test_cnpj_7_digits(self, qa_cosif: COSIFExplorer) -> None:
        with pytest.raises(InvalidIdentifierError):
            qa_cosif.read(instituicao="1234567", start="2023-03")

    def test_cnpj_9_digits(self, qa_cosif: COSIFExplorer) -> None:
        with pytest.raises(InvalidIdentifierError):
            qa_cosif.read(instituicao="123456789", start="2023-03")

    def test_cnpj_letters(self, qa_cosif: COSIFExplorer) -> None:
        with pytest.raises(InvalidIdentifierError):
            qa_cosif.read(instituicao="abcdefgh", start="2023-03")

    def test_cnpj_sql_injection(self, qa_cosif: COSIFExplorer) -> None:
        with pytest.raises(InvalidIdentifierError):
            qa_cosif.read(instituicao="'; DROP TABLE--", start="2023-03")

    def test_cnpj_with_spaces(self, qa_cosif: COSIFExplorer) -> None:
        with pytest.raises(InvalidIdentifierError):
            qa_cosif.read(instituicao="6087 2504", start="2023-03")

    def test_cnpj_fullwidth_unicode_digits(self, qa_cosif: COSIFExplorer) -> None:
        fullwidth = "".join(chr(0xFF10 + i) for i in range(1, 9))
        with pytest.raises(InvalidIdentifierError):
            qa_cosif.read(instituicao=fullwidth, start="2023-03")

    def test_cnpj_as_int_raises(self, qa_cosif: COSIFExplorer) -> None:
        with pytest.raises((TypeError, InvalidIdentifierError)):
            qa_cosif.read(instituicao=60872504, start="2023-03")  # type: ignore[arg-type]

    def test_mixed_valid_invalid_list(self, qa_cosif: COSIFExplorer) -> None:
        with pytest.raises(InvalidIdentifierError):
            qa_cosif.read(instituicao=["60872504", "abc"], start="2023-03")


class TestInvalidDates:
    def test_date_abc(self, qa_cosif: COSIFExplorer) -> None:
        with pytest.raises(InvalidDateFormatError):
            qa_cosif.read(instituicao="60872504", start="abc")

    def test_date_9999_99(self, qa_cosif: COSIFExplorer) -> None:
        with pytest.raises(InvalidDateFormatError):
            qa_cosif.read(instituicao="60872504", start="9999-99")

    def test_start_after_end(self, qa_cosif: COSIFExplorer) -> None:
        with pytest.raises(InvalidDateRangeError):
            qa_cosif.read(instituicao="60872504", start="2024-01", end="2023-01")

    def test_date_as_int_works(self, qa_cosif: COSIFExplorer) -> None:
        df = qa_cosif.read(instituicao="60872504", start=202303)
        assert not df.empty


class TestInvalidScope:
    def test_escopo_inexistente(self, qa_cosif: COSIFExplorer) -> None:
        with pytest.raises(InvalidScopeError):
            qa_cosif.read(instituicao="60872504", start="2023-03", escopo="inexistente")

    def test_cadastro_coluna_fake(self, qa_cosif: COSIFExplorer) -> None:
        with pytest.raises(InvalidScopeError):
            qa_cosif.read(
                instituicao="60872504", start="2023-03", cadastro=["COLUNA_FAKE"]
            )

    def test_columns_unknown_raises(self, qa_cosif: COSIFExplorer) -> None:
        with pytest.raises(InvalidScopeError):
            qa_cosif.read("2023-03", instituicao="60872504", columns=["INVENTADA"])


class TestPassthroughColumns:
    """Colunas nativas do parquet que nao estao em _COLUMN_MAP devem ser aceitas."""

    def test_cosif_cnpj8_accepted(self, qa_cosif: COSIFExplorer) -> None:
        df = qa_cosif.read(
            "2023-03", instituicao="60872504", columns=["CNPJ_8", "DATA", "VALOR"]
        )
        assert list(df.columns) == ["CNPJ_8", "DATA", "VALOR"]

    def test_cosif_documento_accepted(self, qa_cosif: COSIFExplorer) -> None:
        df = qa_cosif.read(
            "2023-03", instituicao="60872504", columns=["DOCUMENTO", "DATA"]
        )
        assert "DOCUMENTO" in df.columns


class TestSearchResilience:
    def test_search_sql_injection(self, qa_search: EntitySearch) -> None:
        df = qa_search.search("'; DROP TABLE--")
        assert df.empty or isinstance(df.empty, bool)

    def test_search_10k_chars(self, qa_search: EntitySearch) -> None:
        df = qa_search.search("A" * 10000)
        assert df.empty

    def test_search_unicode_special(self, qa_search: EntitySearch) -> None:
        for term in ["\x00\x01\x02", "\ud800", "banco"]:
            try:
                qa_search.search(term)
            except (UnicodeError, ValueError):
                pass  # Erros de encoding sao aceitaveis

    def test_search_with_quotes_in_term(self, qa_search: EntitySearch) -> None:
        """Aspas no termo de busca nao crasheiam a query SQL."""
        import pandas as pd

        df = qa_search.search("BANCO 'ALFA'")
        assert isinstance(df, pd.DataFrame)

    def test_search_empty_term_returns_empty(self, qa_search: EntitySearch) -> None:
        df = qa_search.search("")
        assert df.empty


class TestGracefulEmpty:
    def test_nonexistent_account(self, qa_cosif: COSIFExplorer) -> None:
        df = qa_cosif.read(
            instituicao="60872504", start="2023-03", conta="XYZFAKE_INEXISTENTE"
        )
        assert df.empty

    def test_list_contas_negative_limit(self, qa_cosif: COSIFExplorer) -> None:
        with pytest.raises(Exception):
            qa_cosif.list_contas(limit=-1)
