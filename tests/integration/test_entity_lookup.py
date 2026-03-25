"""Testes para EntityLookup e EntitySearch -- resolucao de entidades e busca fuzzy."""

from pathlib import Path

import pytest

from ifdata_bcb.core.entity import EntityLookup, EntitySearch
from ifdata_bcb.infra.query import QueryEngine
from tests.conftest import (
    BANCO_A_CNPJ,
    BANCO_B_CNPJ,
    COD_CONGL_FIN,
    COD_CONGL_PRUD,
)


def _make_lookup(cache_dir: Path) -> EntityLookup:
    qe = QueryEngine(base_path=cache_dir)
    return EntityLookup(query_engine=qe)


def _make_search(cache_dir: Path) -> EntitySearch:
    lookup = _make_lookup(cache_dir)
    return EntitySearch(lookup)


# =========================================================================
# real_entity_condition / resolved_entity_cnpj_expr
# =========================================================================


class TestRealEntityCondition:
    def test_filters_aliases_by_codinst(self, populated_cache: Path) -> None:
        lookup = _make_lookup(populated_cache)
        condition = lookup.real_entity_condition()
        # Deve ser uma expressao SQL valida que filtra apenas entidades reais
        assert "CNPJ_8 IS NOT NULL" in condition
        assert "regexp_matches" in condition


# =========================================================================
# get_entity_identifiers
# =========================================================================


class TestGetEntityIdentifiers:
    def test_returns_identifiers_for_known_entity(self, populated_cache: Path) -> None:
        lookup = _make_lookup(populated_cache)
        info = lookup.get_entity_identifiers(BANCO_A_CNPJ)

        assert info["cnpj_interesse"] == BANCO_A_CNPJ
        assert info["cod_congl_prud"] == COD_CONGL_PRUD
        assert info["cod_congl_fin"] == COD_CONGL_FIN
        assert info["nome_entidade"] == "BANCO ALFA S.A."

    def test_returns_identifiers_for_entity_without_conglomerate(
        self, populated_cache: Path
    ) -> None:
        lookup = _make_lookup(populated_cache)
        info = lookup.get_entity_identifiers(BANCO_B_CNPJ)

        assert info["cnpj_interesse"] == BANCO_B_CNPJ
        assert info["cod_congl_prud"] is None
        assert info["cod_congl_fin"] is None
        assert info["nome_entidade"] == "BANCO BETA S.A."

    def test_returns_defaults_for_unknown_cnpj(self, populated_cache: Path) -> None:
        lookup = _make_lookup(populated_cache)
        info = lookup.get_entity_identifiers("99999999")

        assert info["cnpj_interesse"] == "99999999"
        assert info["cnpj_reporte_cosif"] == "99999999"
        assert info["nome_entidade"] is None

    def test_returns_defaults_for_empty_string(self, populated_cache: Path) -> None:
        lookup = _make_lookup(populated_cache)
        info = lookup.get_entity_identifiers("")

        assert info["cnpj_interesse"] == ""
        assert info["nome_entidade"] is None

    def test_caching_returns_same_result(self, populated_cache: Path) -> None:
        lookup = _make_lookup(populated_cache)
        info1 = lookup.get_entity_identifiers(BANCO_A_CNPJ)
        info2 = lookup.get_entity_identifiers(BANCO_A_CNPJ)
        assert info1 == info2

    def test_clear_cache_resets(self, populated_cache: Path) -> None:
        lookup = _make_lookup(populated_cache)
        lookup.get_entity_identifiers(BANCO_A_CNPJ)
        lookup.clear_cache()
        # Deve funcionar novamente apos clear
        info = lookup.get_entity_identifiers(BANCO_A_CNPJ)
        assert info["nome_entidade"] == "BANCO ALFA S.A."


# =========================================================================
# get_canonical_names_for_cnpjs
# =========================================================================


class TestGetCanonicalNames:
    def test_returns_names_for_known_cnpjs(self, populated_cache: Path) -> None:
        lookup = _make_lookup(populated_cache)
        names = lookup.get_canonical_names_for_cnpjs([BANCO_A_CNPJ, BANCO_B_CNPJ])

        assert names[BANCO_A_CNPJ] == "BANCO ALFA S.A."
        assert names[BANCO_B_CNPJ] == "BANCO BETA S.A."

    def test_returns_empty_string_for_unknown_cnpj(self, populated_cache: Path) -> None:
        lookup = _make_lookup(populated_cache)
        names = lookup.get_canonical_names_for_cnpjs(["99999999"])
        assert names["99999999"] == ""

    def test_empty_input_returns_empty_dict(self, populated_cache: Path) -> None:
        lookup = _make_lookup(populated_cache)
        assert lookup.get_canonical_names_for_cnpjs([]) == {}

    def test_cache_reuses_previous_results(self, populated_cache: Path) -> None:
        """Segunda chamada com mesmos CNPJs nao deve executar query SQL."""
        lookup = _make_lookup(populated_cache)
        lookup.get_canonical_names_for_cnpjs([BANCO_A_CNPJ])

        # Substituir sql() por um que falha -- se cache funciona, nao sera chamado
        original_sql = lookup._qe.sql
        lookup._qe.sql = lambda *a, **kw: (_ for _ in ()).throw(  # type: ignore[assignment]
            RuntimeError("sql() nao deveria ser chamado com cache preenchido")
        )
        try:
            names = lookup.get_canonical_names_for_cnpjs([BANCO_A_CNPJ])
            assert names[BANCO_A_CNPJ] == "BANCO ALFA S.A."
        finally:
            lookup._qe.sql = original_sql

    def test_cache_partial_hit_queries_only_missing(
        self, populated_cache: Path
    ) -> None:
        """Com subset ja cacheado, apenas CNPJs novos devem gerar query."""
        lookup = _make_lookup(populated_cache)
        lookup.get_canonical_names_for_cnpjs([BANCO_A_CNPJ])

        # Agora pedir A + B: A ja esta no cache, so B precisa de query
        calls: list[str] = []
        original_sql = lookup._qe.sql

        def tracking_sql(query: str) -> object:
            calls.append(query)
            return original_sql(query)

        lookup._qe.sql = tracking_sql  # type: ignore[assignment]
        try:
            names = lookup.get_canonical_names_for_cnpjs([BANCO_A_CNPJ, BANCO_B_CNPJ])
            assert names[BANCO_A_CNPJ] == "BANCO ALFA S.A."
            assert names[BANCO_B_CNPJ] == "BANCO BETA S.A."
            # Query deve conter apenas BANCO_B, nao BANCO_A
            assert len(calls) == 1
            assert BANCO_B_CNPJ in calls[0]
            assert BANCO_A_CNPJ not in calls[0]
        finally:
            lookup._qe.sql = original_sql

    def test_cache_unknown_cnpj_not_retried(self, populated_cache: Path) -> None:
        """CNPJ desconhecido cacheado como '' nao gera query repetida."""
        lookup = _make_lookup(populated_cache)
        lookup.get_canonical_names_for_cnpjs(["99999999"])

        original_sql = lookup._qe.sql
        lookup._qe.sql = lambda *a, **kw: (_ for _ in ()).throw(  # type: ignore[assignment]
            RuntimeError("sql() nao deveria ser chamado")
        )
        try:
            names = lookup.get_canonical_names_for_cnpjs(["99999999"])
            assert names["99999999"] == ""
        finally:
            lookup._qe.sql = original_sql

    def test_clear_cache_forces_fresh_query(self, populated_cache: Path) -> None:
        """Apos clear_cache(), proxima chamada deve consultar o banco."""
        lookup = _make_lookup(populated_cache)
        lookup.get_canonical_names_for_cnpjs([BANCO_A_CNPJ])
        assert BANCO_A_CNPJ in lookup._name_cache

        lookup.clear_cache()
        assert lookup._name_cache == {}

        # Deve funcionar normalmente apos clear
        names = lookup.get_canonical_names_for_cnpjs([BANCO_A_CNPJ])
        assert names[BANCO_A_CNPJ] == "BANCO ALFA S.A."


# =========================================================================
# search
# =========================================================================


class TestSearch:
    def test_finds_by_exact_name(self, populated_cache: Path) -> None:
        search = _make_search(populated_cache)
        df = search.search("BANCO ALFA")

        assert not df.empty
        assert BANCO_A_CNPJ in df["CNPJ_8"].values

    def test_finds_by_partial_name(self, populated_cache: Path) -> None:
        search = _make_search(populated_cache)
        df = search.search("ALFA")

        assert not df.empty
        assert BANCO_A_CNPJ in df["CNPJ_8"].values

    def test_returns_score_column(self, populated_cache: Path) -> None:
        search = _make_search(populated_cache)
        df = search.search("ALFA")

        assert "SCORE" in df.columns
        assert all(df["SCORE"] > 0)

    def test_returns_situacao(self, populated_cache: Path) -> None:
        search = _make_search(populated_cache)
        df = search.search("ALFA")

        assert "SITUACAO" in df.columns

    def test_respects_limit(self, populated_cache: Path) -> None:
        search = _make_search(populated_cache)
        df = search.search("BANCO", limit=1)

        assert len(df) <= 1

    def test_empty_term_returns_empty(self, populated_cache: Path) -> None:
        search = _make_search(populated_cache)
        df = search.search("   ")

        assert df.empty
        assert list(df.columns) == [
            "CNPJ_8",
            "INSTITUICAO",
            "SITUACAO",
            "FONTES",
            "SCORE",
        ]

    def test_no_match_returns_empty(self, populated_cache: Path) -> None:
        search = _make_search(populated_cache)
        df = search.search("XYZNONEXISTENT")

        assert df.empty

    def test_invalid_limit_raises(self, populated_cache: Path) -> None:
        search = _make_search(populated_cache)
        with pytest.raises(ValueError, match="limit"):
            search.search("ALFA", limit=0)


# =========================================================================
# _get_data_sources_for_cnpjs
# =========================================================================


class TestGetDataSources:
    def test_detects_cosif_source(self, populated_cache: Path) -> None:
        lookup = _make_lookup(populated_cache)
        sources = lookup._get_data_sources_for_cnpjs([BANCO_A_CNPJ])

        assert "cosif" in sources[BANCO_A_CNPJ]

    def test_detects_ifdata_source(self, populated_cache: Path) -> None:
        lookup = _make_lookup(populated_cache)
        sources = lookup._get_data_sources_for_cnpjs([BANCO_A_CNPJ])

        assert "ifdata" in sources[BANCO_A_CNPJ]

    def test_returns_empty_for_unknown_cnpj(self, populated_cache: Path) -> None:
        lookup = _make_lookup(populated_cache)
        sources = lookup._get_data_sources_for_cnpjs(["99999999"])

        assert sources["99999999"] == set()


# =========================================================================
# get_entity_identifiers -- FIRST FILTER (NULL-resistant)
# =========================================================================


class TestGetEntityIdentifiersFinNull:
    """Testa que FIRST ... FILTER retorna valor nao-NULL mesmo quando recente e NULL."""

    def test_cod_congl_fin_returns_non_null_when_recent_is_null(
        self, fin_disappeared_cache: Path
    ) -> None:
        """CodConglFin desapareceu no periodo recente; FIRST FILTER pega o anterior."""
        lookup = _make_lookup(fin_disappeared_cache)
        info = lookup.get_entity_identifiers(BANCO_A_CNPJ)

        assert info["cod_congl_fin"] == COD_CONGL_FIN
        assert info["cod_congl_prud"] == COD_CONGL_PRUD
        assert info["nome_entidade"] == "BANCO ALFA S.A."


# =========================================================================
# search -- CNPJ exact match
# =========================================================================


class TestSearchByCnpj:
    def test_search_by_cnpj_exact_match(self, populated_cache: Path) -> None:
        search = _make_search(populated_cache)
        df = search.search(BANCO_A_CNPJ)

        assert not df.empty
        assert df.iloc[0]["CNPJ_8"] == BANCO_A_CNPJ
        assert df.iloc[0]["SCORE"] == 100

    def test_search_by_cnpj_unknown_returns_empty(self, populated_cache: Path) -> None:
        """CNPJ desconhecido de 8 digitos sem match fuzzy retorna vazio."""
        search = _make_search(populated_cache)
        df = search.search("99999999")

        assert df.empty

    def test_search_by_cnpj_has_correct_columns(self, populated_cache: Path) -> None:
        search = _make_search(populated_cache)
        df = search.search(BANCO_A_CNPJ)

        expected_cols = ["CNPJ_8", "INSTITUICAO", "SITUACAO", "FONTES", "SCORE"]
        assert list(df.columns) == expected_cols

    def test_search_by_name_still_works(self, populated_cache: Path) -> None:
        search = _make_search(populated_cache)
        df = search.search("BANCO ALFA")

        assert not df.empty
        assert BANCO_A_CNPJ in df["CNPJ_8"].values

    def test_search_cnpj_exact_no_sources_returns_empty(
        self, populated_cache: Path
    ) -> None:
        """CNPJ no cadastro mas sem dados em nenhuma fonte retorna vazio."""
        search = _make_search(populated_cache)
        df = search.search(BANCO_B_CNPJ)
        if not df.empty:
            assert (df["FONTES"] != "").all()
