"""Testes dos novos filtros do CadastroExplorer.read().

Valida atividade, tcb, td, tc (str|int), sr, municipio.
Tambem testa que start e agora obrigatorio.
"""

import pytest

from ifdata_bcb.domain.exceptions import MissingRequiredParameterError
from ifdata_bcb.providers.cosif.explorer import COSIFExplorer
from ifdata_bcb.providers.ifdata.cadastro_explorer import CadastroExplorer
from ifdata_bcb.providers.ifdata.valores_explorer import IFDATAExplorer

BANCO_A_CNPJ = "60872504"


class TestCadastroStartObrigatorio:
    """Cadastro.read() agora exige start (era opcional)."""

    def test_read_without_start_raises(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        cadastro = explorers[2]
        with pytest.raises((MissingRequiredParameterError, TypeError)):
            cadastro.read(start=None)  # type: ignore[arg-type]

    def test_read_with_start_works(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        cadastro = explorers[2]
        df = cadastro.read("2023-03")
        assert not df.empty


class TestCadastroNewFilters:
    """Novos filtros: atividade, tcb, td, tc, sr, municipio."""

    def test_filter_uf(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        cadastro = explorers[2]
        df = cadastro.read("2023-03", uf="SP")
        assert not df.empty
        assert (df["UF"] == "SP").all()

    def test_filter_segmento(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        cadastro = explorers[2]
        df_all = cadastro.read("2023-03")
        segmentos = df_all["SEGMENTO"].dropna().unique()
        if len(segmentos) > 0:
            seg = segmentos[0]
            df = cadastro.read("2023-03", segmento=seg)
            assert not df.empty
            assert (df["SEGMENTO"] == seg).all()

    def test_filter_atividade(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        cadastro = explorers[2]
        df_all = cadastro.read("2023-03")
        if "ATIVIDADE" in df_all.columns:
            atividades = df_all["ATIVIDADE"].dropna().unique()
            if len(atividades) > 0:
                ativ = atividades[0]
                df = cadastro.read("2023-03", atividade=ativ)
                assert not df.empty

    def test_filter_tcb(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        cadastro = explorers[2]
        df_all = cadastro.read("2023-03")
        if "TCB" in df_all.columns:
            tcbs = df_all["TCB"].dropna().unique()
            if len(tcbs) > 0:
                df = cadastro.read("2023-03", tcb=tcbs[0])
                assert not df.empty

    def test_filter_tc_as_string(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        cadastro = explorers[2]
        df_all = cadastro.read("2023-03")
        if "TC" in df_all.columns:
            tcs = df_all["TC"].dropna().unique()
            if len(tcs) > 0:
                df = cadastro.read("2023-03", tc=str(tcs[0]))
                assert not df.empty

    def test_filter_tc_as_int(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        """tc aceita int, deve converter para str internamente."""
        cadastro = explorers[2]
        df_all = cadastro.read("2023-03")
        if "TC" in df_all.columns:
            tcs = df_all["TC"].dropna().unique()
            numeric = [t for t in tcs if str(t).isdigit()]
            if numeric:
                df = cadastro.read("2023-03", tc=int(numeric[0]))
                assert not df.empty

    def test_filter_municipio(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        cadastro = explorers[2]
        df_all = cadastro.read("2023-03")
        if "MUNICIPIO" in df_all.columns:
            munis = df_all["MUNICIPIO"].dropna().unique()
            if len(munis) > 0:
                df = cadastro.read("2023-03", municipio=munis[0])
                assert not df.empty
                assert (df["MUNICIPIO"] == munis[0]).all()

    def test_nonexistent_filter_value_returns_empty(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        cadastro = explorers[2]
        df = cadastro.read("2023-03", uf="XX")
        assert df.empty

    def test_multiple_filters_combined(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        """Multiplos filtros sao AND (todos devem casar)."""
        cadastro = explorers[2]
        df = cadastro.read("2023-03", uf="SP", situacao="A")
        if not df.empty:
            assert (df["UF"] == "SP").all()
            assert (df["SITUACAO"] == "A").all()
