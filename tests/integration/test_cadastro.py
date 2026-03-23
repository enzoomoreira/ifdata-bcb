"""Testes de integracao -- Cadastro read(), list, search methods."""

from pathlib import Path

import pytest

from ifdata_bcb.core.entity_lookup import EntityLookup
from ifdata_bcb.domain.exceptions import InvalidScopeError
from ifdata_bcb.infra.query import QueryEngine
from ifdata_bcb.providers.cosif.explorer import COSIFExplorer
from ifdata_bcb.providers.ifdata.cadastro.explorer import CadastroExplorer
from ifdata_bcb.providers.ifdata.valores.explorer import IFDATAExplorer
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

    def test_read_filters_by_situacao(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        df = explorers[2].read(start="2023-06", situacao="A")
        assert not df.empty
        assert "I" not in df["SITUACAO"].values

    def test_read_all_institutions_without_filter(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        """read() sem instituicao= retorna todas as entidades."""
        df = explorers[2].read(start="2023-03")
        assert not df.empty
        assert len(df["CNPJ_8"].unique()) > 1


# =========================================================================
# search()
# =========================================================================


def _make_cadastro(cache_dir: Path) -> CadastroExplorer:
    qe = QueryEngine(base_path=cache_dir)
    el = EntityLookup(query_engine=qe)
    return CadastroExplorer(query_engine=qe, entity_lookup=el)


class TestSearchWithTermo:
    """search() com termo: fuzzy matching + filtros."""

    def test_finds_by_name(self, populated_cache: Path) -> None:
        cad = _make_cadastro(populated_cache)
        df = cad.search("BANCO ALFA")
        assert not df.empty
        assert BANCO_A_CNPJ in df["CNPJ_8"].values

    def test_score_present_with_termo(self, populated_cache: Path) -> None:
        cad = _make_cadastro(populated_cache)
        df = cad.search("ALFA")
        assert "SCORE" in df.columns
        assert all(df["SCORE"] > 0)

    def test_correct_columns_with_termo(self, populated_cache: Path) -> None:
        cad = _make_cadastro(populated_cache)
        df = cad.search("BANCO ALFA")
        expected = ["CNPJ_8", "INSTITUICAO", "SITUACAO", "FONTES", "SCORE"]
        assert list(df.columns) == expected

    def test_no_match_returns_empty(self, populated_cache: Path) -> None:
        cad = _make_cadastro(populated_cache)
        df = cad.search("XYZNONEXISTENT")
        assert df.empty
        assert "SCORE" in df.columns

    def test_search_with_fonte_ifdata(self, populated_cache: Path) -> None:
        cad = _make_cadastro(populated_cache)
        df = cad.search("BANCO ALFA", fonte="ifdata")
        assert not df.empty
        assert all(df["FONTES"].str.contains("ifdata"))

    def test_search_with_fonte_cosif(self, populated_cache: Path) -> None:
        cad = _make_cadastro(populated_cache)
        df = cad.search("BANCO ALFA", fonte="cosif")
        if not df.empty:
            assert all(df["FONTES"].str.contains("cosif"))

    def test_respects_limit(self, populated_cache: Path) -> None:
        cad = _make_cadastro(populated_cache)
        df = cad.search("BANCO", limit=1)
        assert len(df) <= 1


class TestSearchWithoutTermo:
    """search() sem termo: lista todas as instituicoes com dados."""

    def test_lists_all_with_data(self, populated_cache: Path) -> None:
        cad = _make_cadastro(populated_cache)
        df = cad.search()
        assert not df.empty
        # Must have at least BANCO_A_CNPJ (has IFDATA + COSIF data)
        assert BANCO_A_CNPJ in df["CNPJ_8"].values

    def test_no_score_without_termo(self, populated_cache: Path) -> None:
        cad = _make_cadastro(populated_cache)
        df = cad.search()
        assert "SCORE" not in df.columns

    def test_correct_columns_without_termo(self, populated_cache: Path) -> None:
        cad = _make_cadastro(populated_cache)
        df = cad.search()
        expected = ["CNPJ_8", "INSTITUICAO", "SITUACAO", "FONTES"]
        assert list(df.columns) == expected

    def test_fontes_column_populated(self, populated_cache: Path) -> None:
        cad = _make_cadastro(populated_cache)
        df = cad.search()
        assert all(df["FONTES"] != "")

    def test_fonte_ifdata_filter(self, populated_cache: Path) -> None:
        cad = _make_cadastro(populated_cache)
        df = cad.search(fonte="ifdata")
        assert not df.empty
        assert all(df["FONTES"].str.contains("ifdata"))

    def test_fonte_cosif_filter(self, populated_cache: Path) -> None:
        cad = _make_cadastro(populated_cache)
        df = cad.search(fonte="cosif")
        if not df.empty:
            assert all(df["FONTES"].str.contains("cosif"))

    def test_sorted_by_situacao_then_name(self, populated_cache: Path) -> None:
        cad = _make_cadastro(populated_cache)
        df = cad.search()
        if len(df) > 1:
            # Ativas (A) before Inativas (I)
            situacoes = df["SITUACAO"].tolist()
            assert situacoes == sorted(situacoes)

    def test_respects_limit(self, populated_cache: Path) -> None:
        cad = _make_cadastro(populated_cache)
        df = cad.search(limit=1)
        assert len(df) <= 1


class TestSearchEscopoFilter:
    """search() com filtro de escopo."""

    def test_escopo_individual_ifdata(self, populated_cache: Path) -> None:
        cad = _make_cadastro(populated_cache)
        df = cad.search(fonte="ifdata", escopo="individual")
        # BANCO_A has individual data (CodInst == CNPJ_8 with TipoInstituicao=3)
        assert not df.empty
        assert BANCO_A_CNPJ in df["CNPJ_8"].values

    def test_escopo_prudencial_ifdata(self, populated_cache: Path) -> None:
        cad = _make_cadastro(populated_cache)
        df = cad.search(fonte="ifdata", escopo="prudencial")
        # BANCO_A has conglomerado prudencial (COD_CONGL_PRUD="40")
        assert not df.empty
        assert BANCO_A_CNPJ in df["CNPJ_8"].values

    def test_escopo_financeiro_ifdata(self, populated_cache: Path) -> None:
        cad = _make_cadastro(populated_cache)
        df = cad.search(fonte="ifdata", escopo="financeiro")
        # BANCO_A has conglomerado financeiro (COD_CONGL_FIN="50")
        assert not df.empty
        assert BANCO_A_CNPJ in df["CNPJ_8"].values

    def test_escopo_cosif_prudencial(self, populated_cache: Path) -> None:
        cad = _make_cadastro(populated_cache)
        df = cad.search(fonte="cosif", escopo="prudencial")
        # LIDER_CNPJ == BANCO_A_CNPJ has prudencial COSIF data
        if not df.empty:
            assert BANCO_A_CNPJ in df["CNPJ_8"].values

    def test_escopo_cosif_individual(self, populated_cache: Path) -> None:
        cad = _make_cadastro(populated_cache)
        df = cad.search(fonte="cosif", escopo="individual")
        if not df.empty:
            assert all(df["FONTES"].str.contains("cosif"))

    def test_escopo_without_fonte_uses_ifdata(self, populated_cache: Path) -> None:
        """escopo= sem fonte= filtra por escopo IFDATA."""
        cad = _make_cadastro(populated_cache)
        df = cad.search(escopo="individual")
        assert not df.empty

    def test_escopo_with_termo(self, populated_cache: Path) -> None:
        cad = _make_cadastro(populated_cache)
        df = cad.search("BANCO ALFA", fonte="ifdata", escopo="individual")
        assert not df.empty
        assert BANCO_A_CNPJ in df["CNPJ_8"].values
        assert "SCORE" in df.columns


class TestSearchValidation:
    """search() validacao de parametros."""

    def test_invalid_fonte_raises(self, populated_cache: Path) -> None:
        cad = _make_cadastro(populated_cache)
        with pytest.raises(InvalidScopeError, match="invalid"):
            cad.search(fonte="invalid")

    def test_invalid_escopo_cosif_raises(self, populated_cache: Path) -> None:
        """COSIF nao tem escopo 'financeiro'."""
        cad = _make_cadastro(populated_cache)
        with pytest.raises(InvalidScopeError, match="financeiro"):
            cad.search(fonte="cosif", escopo="financeiro")

    def test_invalid_escopo_ifdata_raises(self, populated_cache: Path) -> None:
        cad = _make_cadastro(populated_cache)
        with pytest.raises(InvalidScopeError, match="invalid"):
            cad.search(fonte="ifdata", escopo="invalid")

    def test_invalid_limit_raises(self, populated_cache: Path) -> None:
        cad = _make_cadastro(populated_cache)
        with pytest.raises(ValueError, match="limit"):
            cad.search(limit=0)

    def test_invalid_limit_negative_raises(self, populated_cache: Path) -> None:
        cad = _make_cadastro(populated_cache)
        with pytest.raises(ValueError, match="limit"):
            cad.search(limit=-1)


class TestSearchEmptyCache:
    """search() com cache vazio ou parcial."""

    def test_search_empty_cache_returns_empty(self, tmp_cache_dir: Path) -> None:
        cad = _make_cadastro(tmp_cache_dir)
        df = cad.search()
        assert df.empty

    def test_search_with_termo_empty_cache(self, tmp_cache_dir: Path) -> None:
        cad = _make_cadastro(tmp_cache_dir)
        df = cad.search("BANCO")
        assert df.empty
