"""Testes de contrato para o pacote core/entity/ e cadastro/search.

Valida que a decomposicao entity_lookup.py -> entity/ preserva a API
publica e que as novas classes (EntitySearch, CadastroSearch) compoem
corretamente.
"""

from pathlib import Path

import pandas as pd
import pytest


# =========================================================================
# Re-export contract: core.entity expoe EntityLookup + EntitySearch
# =========================================================================


class TestEntityPackageExports:
    def test_entity_lookup_importable_from_package(self) -> None:
        from ifdata_bcb.core.entity import EntityLookup

        assert EntityLookup is not None

    def test_entity_search_importable_from_package(self) -> None:
        from ifdata_bcb.core.entity import EntitySearch

        assert EntitySearch is not None

    def test_importable_from_core_init(self) -> None:
        from ifdata_bcb.core import EntityLookup, EntitySearch

        assert EntityLookup is not None
        assert EntitySearch is not None

    def test_old_import_path_removed(self) -> None:
        with pytest.raises(ImportError):
            __import__("ifdata_bcb.core.entity_lookup", fromlist=["EntityLookup"])

    def test_submodule_imports_match_package(self) -> None:
        from ifdata_bcb.core.entity import EntityLookup as EL1, EntitySearch as ES1
        from ifdata_bcb.core.entity.lookup import EntityLookup as EL2
        from ifdata_bcb.core.entity.search import EntitySearch as ES2

        assert EL1 is EL2
        assert ES1 is ES2


# =========================================================================
# EntitySearch composicao com EntityLookup
# =========================================================================


class TestEntitySearchComposition:
    def test_search_receives_lookup_query_engine(self, populated_cache: Path) -> None:
        from ifdata_bcb.core.entity import EntityLookup, EntitySearch
        from ifdata_bcb.infra.query import QueryEngine

        qe = QueryEngine(base_path=populated_cache)
        lookup = EntityLookup(query_engine=qe)
        search = EntitySearch(lookup)

        assert search._qe is qe
        assert search._lookup is lookup

    def test_search_with_custom_threshold(self, populated_cache: Path) -> None:
        from ifdata_bcb.core.entity import EntityLookup, EntitySearch
        from ifdata_bcb.infra.query import QueryEngine

        qe = QueryEngine(base_path=populated_cache)
        lookup = EntityLookup(query_engine=qe)
        search = EntitySearch(lookup, fuzzy_threshold_suggest=95)

        assert search._fuzzy.threshold_suggest == 95

    def test_search_returns_dataframe(self, populated_cache: Path) -> None:
        from ifdata_bcb.core.entity import EntityLookup, EntitySearch
        from ifdata_bcb.infra.query import QueryEngine

        qe = QueryEngine(base_path=populated_cache)
        lookup = EntityLookup(query_engine=qe)
        search = EntitySearch(lookup)

        df = search.search("ALFA")
        assert isinstance(df, pd.DataFrame)
        assert not df.empty

    def test_lookup_has_no_search_method(self) -> None:
        from ifdata_bcb.core.entity import EntityLookup

        assert not hasattr(EntityLookup, "search")


# =========================================================================
# CadastroSearch composicao
# =========================================================================


class TestCadastroSearchExports:
    def test_importable_from_cadastro_package(self) -> None:
        from ifdata_bcb.providers.ifdata.cadastro import CadastroSearch

        assert CadastroSearch is not None

    def test_importable_from_search_module(self) -> None:
        from ifdata_bcb.providers.ifdata.cadastro.search import CadastroSearch

        assert CadastroSearch is not None


class TestCadastroSearchComposition:
    def test_explorer_delegates_to_cadastro_search(self, populated_cache: Path) -> None:
        from ifdata_bcb.core.entity import EntityLookup
        from ifdata_bcb.infra.query import QueryEngine
        from ifdata_bcb.providers.ifdata.cadastro.explorer import CadastroExplorer
        from ifdata_bcb.providers.ifdata.cadastro.search import CadastroSearch

        qe = QueryEngine(base_path=populated_cache)
        el = EntityLookup(query_engine=qe)
        cad = CadastroExplorer(query_engine=qe, entity_lookup=el)

        assert hasattr(cad, "_cadastro_search")
        assert isinstance(cad._cadastro_search, CadastroSearch)

    def test_cadastro_search_with_termo_matches_explorer(
        self, populated_cache: Path
    ) -> None:
        """CadastroSearch.search() retorna mesmo resultado que CadastroExplorer.search()."""
        from ifdata_bcb.core.entity import EntityLookup
        from ifdata_bcb.infra.query import QueryEngine
        from ifdata_bcb.providers.ifdata.cadastro.explorer import CadastroExplorer

        qe = QueryEngine(base_path=populated_cache)
        el = EntityLookup(query_engine=qe)
        cad = CadastroExplorer(query_engine=qe, entity_lookup=el)

        df_explorer = cad.search("ALFA")
        df_direct = cad._cadastro_search.search("ALFA")

        pd.testing.assert_frame_equal(df_explorer, df_direct)

    def test_cadastro_search_without_termo_matches_explorer(
        self, populated_cache: Path
    ) -> None:
        from ifdata_bcb.core.entity import EntityLookup
        from ifdata_bcb.infra.query import QueryEngine
        from ifdata_bcb.providers.ifdata.cadastro.explorer import CadastroExplorer

        qe = QueryEngine(base_path=populated_cache)
        el = EntityLookup(query_engine=qe)
        cad = CadastroExplorer(query_engine=qe, entity_lookup=el)

        df_explorer = cad.search()
        df_direct = cad._cadastro_search.search()

        pd.testing.assert_frame_equal(df_explorer, df_direct)


# =========================================================================
# Bulk prudencial regex: ^\d{8}$ distingue CNPJ de conglomerate codes
# =========================================================================


class TestBulkPrudencialCnpjRegex:
    r"""Testa que o regex ^\d{8}$ separa corretamente CNPJ de codigos curtos."""

    def test_8_digit_cnpj_assigned_directly(self, explorers: tuple) -> None:
        """CodInst com 8 digitos no prudencial e tratado como CNPJ direto."""
        ifdata = explorers[1]
        df = ifdata.read("2023-03", escopo="individual")
        if "CNPJ_8" in df.columns:
            assert df["CNPJ_8"].str.match(r"^\d{8}$").all()

    def test_short_numeric_code_not_treated_as_cnpj(self, explorers: tuple) -> None:
        """Codigos numericos curtos (ex: "40") nao viram CNPJ_8 direto."""
        ifdata = explorers[1]
        df = ifdata.read("2023-03", escopo="prudencial")
        if "CNPJ_8" in df.columns and "COD_INST" in df.columns:
            # Nenhum CNPJ_8 deve ter menos de 8 digitos
            resolved = df["CNPJ_8"].dropna()
            if not resolved.empty:
                assert resolved.str.match(r"^\d{8}$").all(), (
                    f"CNPJ_8 contem valores que nao sao 8 digitos: "
                    f"{resolved[~resolved.str.match(r'^\\d{8}$')].unique()}"
                )
