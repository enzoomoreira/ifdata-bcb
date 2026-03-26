"""Testes para filtragem temporal em search() e _get_data_sources_for_cnpjs().

Valida que date_range restringe verificacao de disponibilidade
de dados (COSIF/IFDATA) ao intervalo solicitado.

Fixture base: populated_cache com dados em 202303 (cosif ind/prud, ifdata)
e cadastro em 202303 + 202306.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from ifdata_bcb.core.entity import EntityLookup, EntitySearch
from ifdata_bcb.core.entity.lookup import EntityLookup as EntityLookupDirect
from ifdata_bcb.infra.query import QueryEngine
from ifdata_bcb.providers.ifdata.cadastro.explorer import CadastroExplorer
from ifdata_bcb.providers.ifdata.cadastro.search import CadastroSearch

from tests.conftest import BANCO_A_CNPJ, BANCO_B_CNPJ


def _make_lookup(cache_dir: Path) -> EntityLookup:
    qe = QueryEngine(base_path=cache_dir)
    return EntityLookup(query_engine=qe)


def _make_search(cache_dir: Path) -> EntitySearch:
    lookup = _make_lookup(cache_dir)
    return EntitySearch(lookup)


def _make_cadastro(cache_dir: Path) -> CadastroExplorer:
    qe = QueryEngine(base_path=cache_dir)
    el = EntityLookup(query_engine=qe)
    return CadastroExplorer(query_engine=qe, entity_lookup=el)


# =========================================================================
# EntityLookup._date_filter -- logica pura
# =========================================================================


class TestDateFilter:
    """Testa a geracao de clausula SQL BETWEEN."""

    def test_none_returns_empty(self) -> None:
        assert EntityLookupDirect._date_filter("DATA_BASE", None) == ""

    def test_single_period(self) -> None:
        result = EntityLookupDirect._date_filter("AnoMes", (202303, 202303))
        assert result == " AND AnoMes BETWEEN 202303 AND 202303"

    def test_range(self) -> None:
        result = EntityLookupDirect._date_filter("DATA_BASE", (202301, 202312))
        assert result == " AND DATA_BASE BETWEEN 202301 AND 202312"

    def test_different_columns(self) -> None:
        for col in ("DATA_BASE", "AnoMes", "Data"):
            result = EntityLookupDirect._date_filter(col, (202406, 202412))
            assert col in result
            assert "BETWEEN" in result


# =========================================================================
# CadastroSearch._resolve_date_range -- parsing de datas
# =========================================================================


class TestResolveDateRange:
    """Testa conversao de start/end para tupla (min, max)."""

    def test_none_returns_none(self) -> None:
        assert CadastroSearch._resolve_date_range(None, None) is None

    def test_start_only(self) -> None:
        result = CadastroSearch._resolve_date_range("2024-06", None)
        assert result == (202406, 202406)

    def test_start_and_end(self) -> None:
        result = CadastroSearch._resolve_date_range("2024-01", "2024-12")
        assert result == (202401, 202412)

    def test_int_format(self) -> None:
        result = CadastroSearch._resolve_date_range("202406", None)
        assert result == (202406, 202406)

    def test_start_none_end_ignored(self) -> None:
        """Se start=None, end e irrelevante."""
        assert CadastroSearch._resolve_date_range(None, "2024-12") is None


# =========================================================================
# EntityLookup._get_data_sources_for_cnpjs com date_range
# =========================================================================


class TestDataSourcesDateRange:
    """Testa que date_range filtra verificacao de disponibilidade."""

    def test_without_date_range_finds_sources(
        self, populated_cache: Path
    ) -> None:
        """Sem date_range, encontra todas as fontes disponiveis."""
        lookup = _make_lookup(populated_cache)
        sources = lookup._get_data_sources_for_cnpjs([BANCO_A_CNPJ])
        assert "cosif" in sources[BANCO_A_CNPJ]
        assert "ifdata" in sources[BANCO_A_CNPJ]

    def test_matching_period_finds_sources(
        self, populated_cache: Path
    ) -> None:
        """date_range que inclui 202303 encontra dados."""
        lookup = _make_lookup(populated_cache)
        sources = lookup._get_data_sources_for_cnpjs(
            [BANCO_A_CNPJ], date_range=(202303, 202303)
        )
        assert "cosif" in sources[BANCO_A_CNPJ]
        assert "ifdata" in sources[BANCO_A_CNPJ]

    def test_future_period_finds_nothing(
        self, populated_cache: Path
    ) -> None:
        """date_range futuro nao encontra nenhuma fonte."""
        lookup = _make_lookup(populated_cache)
        sources = lookup._get_data_sources_for_cnpjs(
            [BANCO_A_CNPJ], date_range=(203012, 203012)
        )
        assert sources[BANCO_A_CNPJ] == set()

    def test_past_period_finds_nothing(
        self, populated_cache: Path
    ) -> None:
        """date_range anterior aos dados nao encontra nenhuma fonte."""
        lookup = _make_lookup(populated_cache)
        sources = lookup._get_data_sources_for_cnpjs(
            [BANCO_A_CNPJ], date_range=(200101, 200112)
        )
        assert sources[BANCO_A_CNPJ] == set()

    def test_range_spanning_data_finds_sources(
        self, populated_cache: Path
    ) -> None:
        """Range amplo que inclui 202303 encontra dados."""
        lookup = _make_lookup(populated_cache)
        sources = lookup._get_data_sources_for_cnpjs(
            [BANCO_A_CNPJ], date_range=(202301, 202312)
        )
        assert "cosif" in sources[BANCO_A_CNPJ]

    def test_multiple_cnpjs_filtered_independently(
        self, populated_cache: Path
    ) -> None:
        """Cada CNPJ e filtrado independentemente."""
        lookup = _make_lookup(populated_cache)
        sources = lookup._get_data_sources_for_cnpjs(
            [BANCO_A_CNPJ, BANCO_B_CNPJ], date_range=(202303, 202303)
        )
        # Ambos tem dados em 202303
        assert sources[BANCO_A_CNPJ] != set()
        assert sources[BANCO_B_CNPJ] != set()


# =========================================================================
# EntitySearch.search com date_range
# =========================================================================


class TestEntitySearchDateRange:
    """Testa propagacao de date_range no fuzzy search."""

    def test_search_without_date_range(
        self, populated_cache: Path
    ) -> None:
        """Busca sem date_range retorna entidades com dados."""
        search = _make_search(populated_cache)
        df = search.search("ALFA")
        assert not df.empty
        assert "FONTES" in df.columns

    def test_search_with_matching_date_range(
        self, populated_cache: Path
    ) -> None:
        """Busca com date_range que inclui dados retorna resultados."""
        search = _make_search(populated_cache)
        df = search.search("ALFA", date_range=(202303, 202303))
        assert not df.empty
        assert df.iloc[0]["FONTES"] != ""

    def test_search_with_future_date_range_returns_empty(
        self, populated_cache: Path
    ) -> None:
        """Busca com date_range futuro retorna vazio (nao entidades sem dados)."""
        search = _make_search(populated_cache)
        df = search.search("ALFA", date_range=(203012, 203012))
        assert df.empty

    def test_exact_cnpj_with_future_date_range_returns_empty(
        self, populated_cache: Path
    ) -> None:
        """Match exato por CNPJ com date_range futuro retorna vazio."""
        search = _make_search(populated_cache)
        df = search.search(BANCO_A_CNPJ, date_range=(203012, 203012))
        assert df.empty


# =========================================================================
# CadastroExplorer.search com start/end (integracao ponta-a-ponta)
# =========================================================================


class TestCadastroSearchDateFilter:
    """Testa search() via CadastroExplorer com parametros start/end."""

    def test_search_without_dates_returns_results(
        self, populated_cache: Path
    ) -> None:
        """search() sem start/end funciona como antes."""
        cadastro = _make_cadastro(populated_cache)
        df = cadastro.search("ALFA")
        assert not df.empty

    def test_search_with_matching_start(
        self, populated_cache: Path
    ) -> None:
        """search(start=) com periodo valido retorna resultados."""
        cadastro = _make_cadastro(populated_cache)
        df = cadastro.search("ALFA", start="2023-03")
        assert not df.empty
        assert "SCORE" in df.columns

    def test_search_with_future_start_returns_empty(
        self, populated_cache: Path
    ) -> None:
        """search(start=) com periodo futuro retorna vazio."""
        cadastro = _make_cadastro(populated_cache)
        df = cadastro.search("ALFA", start="2030-12")
        assert df.empty

    def test_search_without_termo_with_dates(
        self, populated_cache: Path
    ) -> None:
        """search(start=) sem termo lista entidades com dados no periodo."""
        cadastro = _make_cadastro(populated_cache)
        df_all = cadastro.search()
        df_dated = cadastro.search(start="2023-03")
        # Com date_range, deve retornar <= sem date_range
        assert len(df_dated) <= len(df_all)
        # Coluna SCORE nao presente sem termo
        assert "SCORE" not in df_dated.columns

    def test_search_without_termo_future_returns_empty(
        self, populated_cache: Path
    ) -> None:
        """search() sem termo com periodo futuro retorna vazio."""
        cadastro = _make_cadastro(populated_cache)
        df = cadastro.search(start="2030-12")
        assert df.empty

    def test_search_with_start_and_end(
        self, populated_cache: Path
    ) -> None:
        """search(start=, end=) com range valido retorna resultados."""
        cadastro = _make_cadastro(populated_cache)
        df = cadastro.search("ALFA", start="2023-01", end="2023-12")
        assert not df.empty

    def test_search_with_fonte_and_dates(
        self, populated_cache: Path
    ) -> None:
        """search(fonte=, start=) combina filtros corretamente."""
        cadastro = _make_cadastro(populated_cache)
        df = cadastro.search("ALFA", fonte="cosif", start="2023-03")
        assert not df.empty
        # FONTES deve conter cosif
        assert all(df["FONTES"].str.contains("cosif"))

    def test_search_with_fonte_and_future_dates(
        self, populated_cache: Path
    ) -> None:
        """search(fonte=, start=) com periodo futuro retorna vazio."""
        cadastro = _make_cadastro(populated_cache)
        df = cadastro.search("ALFA", fonte="cosif", start="2030-12")
        assert df.empty

    def test_search_with_escopo_and_dates(
        self, populated_cache: Path
    ) -> None:
        """search(escopo=, start=) combina filtros corretamente."""
        cadastro = _make_cadastro(populated_cache)
        df = cadastro.search(
            "ALFA", fonte="ifdata", escopo="individual", start="2023-03"
        )
        assert not df.empty

    def test_start_end_single_date_equivalent(
        self, populated_cache: Path
    ) -> None:
        """start='X' sem end == start='X', end='X'."""
        cadastro = _make_cadastro(populated_cache)
        df_single = cadastro.search("ALFA", start="2023-03")
        df_range = cadastro.search("ALFA", start="2023-03", end="2023-03")
        assert len(df_single) == len(df_range)
