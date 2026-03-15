"""Testes de integracao -- IFDATA read() e list methods."""

from ifdata_bcb.providers.cosif.explorer import COSIFExplorer
from ifdata_bcb.providers.ifdata.cadastro_explorer import CadastroExplorer
from ifdata_bcb.providers.ifdata.explorer import IFDATAExplorer
from tests.conftest import BANCO_A_CNPJ


class TestIFDATARead:
    def test_read_individual_returns_data(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        df = explorers[1].read(
            instituicao=BANCO_A_CNPJ, start="2023-03", escopo="individual"
        )
        assert not df.empty
        for col in ("DATA", "CNPJ_8", "VALOR"):
            assert col in df.columns

    def test_read_individual_filters_by_institution(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        df = explorers[1].read(
            instituicao=BANCO_A_CNPJ, start="2023-03", escopo="individual"
        )
        assert all(df["CNPJ_8"] == BANCO_A_CNPJ)

    def test_read_prudencial_resolves_conglomerate(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        df = explorers[1].read(
            instituicao=BANCO_A_CNPJ, start="2023-03", escopo="prudencial"
        )
        assert not df.empty
        assert BANCO_A_CNPJ in df["CNPJ_8"].values

    def test_read_no_data_returns_empty(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        df = explorers[1].read(
            instituicao="99999999", start="2023-03", escopo="individual"
        )
        assert df.empty
        assert "DATA" in df.columns

    def test_read_includes_cod_conta(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        df = explorers[1].read(
            instituicao=BANCO_A_CNPJ, start="2023-03", escopo="individual"
        )
        assert "COD_CONTA" in df.columns

    def test_read_filters_by_account_code(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        df = explorers[1].read(
            instituicao=BANCO_A_CNPJ,
            start="2023-03",
            conta="10100",
            escopo="individual",
        )
        assert not df.empty
        assert all(df["COD_CONTA"] == "10100")


class TestIFDATAListMethods:
    def test_list_periods(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        assert 202303 in explorers[1].list_periods()

    def test_has_data(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        assert explorers[1].has_data() is True

    def test_list_accounts_includes_relatorio_and_grupo(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        df = explorers[1].list_accounts()
        assert "RELATORIO" in df.columns
        assert "GRUPO" in df.columns

    def test_list_accounts_filters_by_relatorio(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        df = explorers[1].list_accounts(relatorio="Resumo")
        assert not df.empty
        assert all(df["RELATORIO"].str.upper().str.contains("RESUMO"))
