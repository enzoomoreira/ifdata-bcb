"""describe() como contrato de introspeccao (item 3.13).

O alvo da lib sao agentes que montam a chamada seguinte a partir daqui, entao
o que se testa e que o retorno descreve a superficie real -- e nao uma lista
paralela que envelheceu.
"""

import inspect

import pytest

from ifdata_bcb.providers.base_explorer import BaseExplorer
from ifdata_bcb.providers.cosif.explorer import COSIFExplorer
from ifdata_bcb.providers.ifdata.cadastro.explorer import CadastroExplorer
from ifdata_bcb.providers.ifdata.valores.explorer import IFDATAExplorer

CAPACIDADES = (
    "escopos",
    "columns",
    "read_columns",
    "filtros",
    "cadastro_columns",
)


@pytest.fixture
def todos(
    explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer],
) -> tuple[BaseExplorer, ...]:
    return explorers


class TestChavesDeCapacidade:
    @pytest.mark.parametrize("chave", CAPACIDADES)
    def test_presente_em_todos_os_explorers(
        self, todos: tuple[BaseExplorer, ...], chave: str
    ) -> None:
        for explorer in todos:
            assert chave in explorer.describe(), type(explorer).__name__

    @pytest.mark.parametrize("chave", CAPACIDADES)
    def test_presente_tambem_no_modo_escopo(
        self, todos: tuple[BaseExplorer, ...], chave: str
    ) -> None:
        """describe('individual') nao pode devolver menos que describe()."""
        assert chave in todos[0].describe("individual")


class TestFiltrosRefletemAAssinatura:
    """A lista de filtros vem de inspect: nao pode divergir do read() real."""

    def test_filtros_sao_os_keyword_only_de_read(
        self, todos: tuple[BaseExplorer, ...]
    ) -> None:
        for explorer in todos:
            esperado = sorted(
                nome
                for nome, p in inspect.signature(type(explorer).read).parameters.items()
                if p.kind is inspect.Parameter.KEYWORD_ONLY
                and nome not in ("columns", "cadastro")
            )
            assert explorer.describe()["filtros"] == esperado

    def test_cada_filtro_e_aceito_por_read(
        self, todos: tuple[BaseExplorer, ...]
    ) -> None:
        """A promessa util: passar o que describe() lista nao pode dar TypeError."""
        for explorer in todos:
            aceitos = inspect.signature(type(explorer).read).parameters
            for filtro in explorer.describe()["filtros"]:
                assert filtro in aceitos, f"{type(explorer).__name__}.{filtro}"

    def test_cosif_lista_documento_e_ifdata_nao(
        self, todos: tuple[BaseExplorer, ...]
    ) -> None:
        assert "documento" in todos[0].describe()["filtros"]
        assert "documento" not in todos[1].describe()["filtros"]
        assert "relatorio" in todos[1].describe()["filtros"]


class TestEscoposEColunas:
    def test_escopos_do_cosif(self, todos: tuple[BaseExplorer, ...]) -> None:
        assert todos[0].describe()["escopos"] == ["individual", "prudencial"]

    def test_cadastro_nao_tem_escopo(self, todos: tuple[BaseExplorer, ...]) -> None:
        assert todos[2].describe()["escopos"] == []

    def test_read_columns_bate_com_o_read_real(
        self, todos: tuple[BaseExplorer, ...]
    ) -> None:
        cosif = todos[0]
        df = cosif.read("2023-03")
        assert list(df.columns) == cosif.describe()["read_columns"]

    def test_cadastro_columns_so_onde_read_aceita_cadastro(
        self, todos: tuple[BaseExplorer, ...]
    ) -> None:
        assert "SEGMENTO" in todos[0].describe()["cadastro_columns"]
        assert todos[2].describe()["cadastro_columns"] == []

    def test_columns_sao_aceitas_por_list_values(
        self, todos: tuple[BaseExplorer, ...]
    ) -> None:
        for explorer in todos:
            for coluna in explorer.describe()["columns"]:
                explorer.list_values([coluna], limit=1)


class TestDirNaoVazaInternos:
    """3.14: dir(bcb) listava Any, logger, _cosif, TYPE_CHECKING."""

    def test_nenhum_nome_privado_ou_de_import(self) -> None:
        import ifdata_bcb as bcb

        nomes = dir(bcb)
        vazados = [
            n
            for n in nomes
            if n.startswith("_") and not (n.startswith("__") and n.endswith("__"))
        ]
        assert vazados == []
        assert "Any" not in nomes
        assert "logger" not in nomes
        assert "TYPE_CHECKING" not in nomes

    def test_tudo_de_all_continua_visivel(self) -> None:
        import ifdata_bcb as bcb

        assert set(bcb.__all__) <= set(dir(bcb))


class TestCadastroDiagnosticaVazio:
    """3.10: COSIF e IFDATA diagnosticavam; o cadastro calava."""

    def test_filtro_sem_resultado_emite_warning(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        from ifdata_bcb.domain.exceptions import PartialDataWarning

        with pytest.warns(PartialDataWarning) as rec:
            df = explorers[2].read("2023-03", uf="ZZ")
        assert df.empty
        assert "Cadastro" in str(rec[0].message)

    def test_mensagem_cita_os_filtros_do_cadastro(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        """A mensagem generica falava de 'escopo, conta', que o cadastro nao tem."""
        from ifdata_bcb.domain.exceptions import PartialDataWarning

        with pytest.warns(PartialDataWarning) as rec:
            explorers[2].read("2023-03", uf="ZZ")
        msg = str(rec[0].message)
        assert "segmento" in msg
        assert "conta" not in msg

    def test_resultado_com_dados_nao_emite_warning(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        import warnings

        with warnings.catch_warnings():
            warnings.simplefilter("error")
            df = explorers[2].read("2023-03")
        assert not df.empty
