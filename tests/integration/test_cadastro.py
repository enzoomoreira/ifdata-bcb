"""Testes de integracao -- Cadastro read(), info() e list methods."""

from ifdata_bcb.providers.cosif.explorer import COSIFExplorer
from ifdata_bcb.providers.ifdata.cadastro_explorer import CadastroExplorer
from ifdata_bcb.providers.ifdata.explorer import IFDATAExplorer
from tests.conftest import BANCO_A_CNPJ


class TestCadastroRead:
    def test_read_returns_correct_columns(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        df = explorers[2].read(instituicao=BANCO_A_CNPJ, start="2023-03")
        assert not df.empty
        for col in ("DATA", "CNPJ_8", "INSTITUICAO", "SEGMENTO", "SITUACAO"):
            assert col in df.columns

    def test_read_filters_by_institution(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        df = explorers[2].read(instituicao=BANCO_A_CNPJ, start="2023-03")
        assert all(df["CNPJ_8"] == BANCO_A_CNPJ)

    def test_read_filters_by_uf(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        df = explorers[2].read(start="2023-03", uf="SP")
        assert not df.empty
        assert all(df["UF"].str.upper() == "SP")

    def test_info_returns_dict(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        info = explorers[2].info(BANCO_A_CNPJ, start="2023-03")
        assert info is not None
        assert info["CNPJ_8"] == BANCO_A_CNPJ
        assert info["INSTITUICAO"] == "BANCO ALFA S.A."

    def test_info_unknown_entity_returns_none(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        assert explorers[2].info("99999999", start="2023-03") is None

    def test_list_segmentos(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        assert "S1" in explorers[2].list_segmentos()

    def test_list_ufs(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        ufs = explorers[2].list_ufs()
        assert "SP" in ufs
        assert "RJ" in ufs
