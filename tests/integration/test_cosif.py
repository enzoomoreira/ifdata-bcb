"""Testes de integracao -- COSIF read() e list methods."""

import pandas as pd

from ifdata_bcb.providers.cosif.explorer import COSIFExplorer
from ifdata_bcb.providers.ifdata.cadastro_explorer import CadastroExplorer
from ifdata_bcb.providers.ifdata.explorer import IFDATAExplorer
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

    def test_read_single_scope(
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


class TestCOSIFListMethods:
    def test_list_periods(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        assert 202303 in explorers[0].list_periods()

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

    def test_list_accounts(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        df = explorers[0].list_accounts()
        assert not df.empty
        assert "COD_CONTA" in df.columns

    def test_list_accounts_with_filter(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        df = explorers[0].list_accounts(termo="ATIVO")
        assert not df.empty
        assert all("ATIVO" in c.upper() for c in df["CONTA"])

    def test_list_institutions(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        df = explorers[0].list_institutions()
        assert not df.empty
        assert "CNPJ_8" in df.columns
