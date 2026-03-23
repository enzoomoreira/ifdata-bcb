"""Testes de integracao -- COSIF read() e list methods."""

import pandas as pd
import pytest

from ifdata_bcb.domain.exceptions import InvalidScopeError
from ifdata_bcb.providers.cosif.explorer import COSIFExplorer
from ifdata_bcb.providers.ifdata.cadastro.explorer import CadastroExplorer
from ifdata_bcb.providers.ifdata.valores.explorer import IFDATAExplorer
from tests.conftest import BANCO_A_CNPJ


class TestCOSIFRead:
    def test_read_returns_dataframe_with_correct_columns(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        cosif = explorers[0]
        df = cosif.read(instituicao=BANCO_A_CNPJ, start="2023-03")

        assert not df.empty
        for col in ("DATA", "CNPJ_8", "INSTITUICAO", "ESCOPO", "CONTA", "VALOR"):
            assert col in df.columns

    def test_read_converts_data_to_datetime(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        df = explorers[0].read(instituicao=BANCO_A_CNPJ, start="2023-03")
        assert pd.api.types.is_datetime64_any_dtype(df["DATA"])

    def test_read_filters_by_institution(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        df = explorers[0].read(instituicao=BANCO_A_CNPJ, start="2023-03")
        assert all(df["CNPJ_8"] == BANCO_A_CNPJ)

    def test_read_both_scopes_returns_escopo_column(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        df = explorers[0].read(instituicao=BANCO_A_CNPJ, start="2023-03")
        assert "ESCOPO" in df.columns
        assert "individual" in df["ESCOPO"].unique()

    def test_read_single_escopo(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        df = explorers[0].read(
            instituicao=BANCO_A_CNPJ, start="2023-03", escopo="individual"
        )
        assert not df.empty
        assert all(df["ESCOPO"] == "individual")

    def test_read_filters_by_account(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        df = explorers[0].read(
            instituicao=BANCO_A_CNPJ, start="2023-03", conta="ATIVO TOTAL"
        )
        assert not df.empty
        assert all(df["CONTA"] == "ATIVO TOTAL")

    def test_read_no_data_returns_empty_with_columns(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        df = explorers[0].read(instituicao="99999999", start="2023-03")
        assert df.empty
        assert "DATA" in df.columns

    def test_read_applies_canonical_names(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        df = explorers[0].read(instituicao=BANCO_A_CNPJ, start="2023-03")
        if not df.empty:
            assert "BANCO ALFA S.A." in df["INSTITUICAO"].unique()

    def test_read_with_cadastro_enrichment(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        df = explorers[0].read(
            instituicao=BANCO_A_CNPJ, start="2023-03", cadastro=["SEGMENTO", "UF"]
        )
        assert "SEGMENTO" in df.columns
        assert "UF" in df.columns

    def test_read_includes_cod_conta_and_documento(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        df = explorers[0].read(instituicao=BANCO_A_CNPJ, start="2023-03")

        assert "COD_CONTA" in df.columns
        assert "DOCUMENTO" in df.columns
        assert "10100" in df["COD_CONTA"].astype(str).values
        assert "20200" in df["COD_CONTA"].astype(str).values
        assert df["DOCUMENTO"].notna().all()

    def test_read_filters_by_account_code(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        df = explorers[0].read(instituicao=BANCO_A_CNPJ, start="2023-03", conta="10100")
        assert not df.empty
        assert all(df["COD_CONTA"].astype(str) == "10100")


class TestCOSIFListMethods:
    def test_list_periodos(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        assert 202303 in explorers[0].list_periodos()

    def test_has_data(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        assert explorers[0].has_data() is True

    def test_describe(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        info = explorers[0].describe()
        assert info["has_data"] is True
        assert info["period_count"] >= 1
        assert "by_source" in info

    def test_list_contas(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        df = explorers[0].list_contas()
        assert not df.empty
        assert "COD_CONTA" in df.columns
        assert "ESCOPOS" in df.columns

    def test_list_contas_with_filter(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        df = explorers[0].list_contas(termo="ATIVO")
        assert not df.empty
        assert all("ATIVO" in c.upper() for c in df["CONTA"])

    def test_list_contas_limit_applies_to_total(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        df = explorers[0].list_contas(limit=2)
        assert len(df) <= 2

    def test_list_contas_with_escopo_no_escopos_column(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        df = explorers[0].list_contas(escopo="individual")
        assert not df.empty
        assert "ESCOPOS" not in df.columns
        assert list(df.columns) == ["COD_CONTA", "CONTA"]

    def test_list_instituicoes(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        df = explorers[0].list_instituicoes()
        assert not df.empty
        assert "CNPJ_8" in df.columns
        assert "TEM_INDIVIDUAL" in df.columns
        assert "TEM_PRUDENCIAL" in df.columns
        assert df["TEM_INDIVIDUAL"].dtype == bool

    def test_list_instituicoes_with_escopo_no_tem_columns(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        df = explorers[0].list_instituicoes(escopo="individual")
        assert not df.empty
        assert list(df.columns) == ["CNPJ_8", "INSTITUICAO"]


class TestCOSIFDocumentoValidation:
    """Validacao do parametro documento em cosif.read()."""

    def test_documento_non_numeric_raises(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        """documento com valor nao-numerico deve levantar InvalidScopeError."""
        with pytest.raises(InvalidScopeError) as exc_info:
            explorers[0].read(
                "2023-03", instituicao=BANCO_A_CNPJ, documento="balancete"
            )
        assert exc_info.value.scope == "documento"

    def test_documento_numeric_string_accepted(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        """documento com string numerica deve ser aceito sem erro."""
        df = explorers[0].read("2023-03", instituicao=BANCO_A_CNPJ, documento="4010")
        assert isinstance(df, pd.DataFrame)

    def test_documento_list_with_non_numeric_raises(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        """Lista com elemento nao-numerico deve levantar InvalidScopeError."""
        with pytest.raises(InvalidScopeError) as exc_info:
            explorers[0].read(
                "2023-03", instituicao=BANCO_A_CNPJ, documento=["4010", "abc"]
            )
        assert exc_info.value.scope == "documento"
