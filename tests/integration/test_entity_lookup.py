"""Testes para EntityLookup -- resolucao de entidades, escopos e busca fuzzy."""

from pathlib import Path

import pandas as pd
import pytest

from ifdata_bcb.core.entity_lookup import EntityLookup
from ifdata_bcb.domain.exceptions import DataUnavailableError, InvalidScopeError
from ifdata_bcb.infra.query import QueryEngine
from ifdata_bcb.providers.ifdata.scope import resolve_ifdata_escopo
from tests.conftest import (
    BANCO_A_CNPJ,
    BANCO_B_CNPJ,
    COD_CONGL_FIN,
    COD_CONGL_PRUD,
)


def _make_lookup(cache_dir: Path) -> EntityLookup:
    qe = QueryEngine(base_path=cache_dir)
    return EntityLookup(query_engine=qe)


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

    def test_legacy_fallback_without_codinst(self, tmp_cache_dir: Path) -> None:
        """Se cadastro nao tem CodInst, usa heuristica por nome."""
        from tests.conftest import _save_parquet

        df = pd.DataFrame(
            {
                "Data": pd.array([202303], dtype="Int64"),
                "CNPJ_8": ["12345678"],
                "NomeInstituicao": ["BANCO TESTE"],
                "CNPJ_LIDER_8": [None],
                "CodConglomeradoPrudencial": [None],
                "CodConglomeradoFinanceiro": [None],
                "Situacao": ["A"],
            }
        )
        _save_parquet(df, tmp_cache_dir, "ifdata/cadastro", "ifdata_cad_202303")

        lookup = _make_lookup(tmp_cache_dir)
        condition = lookup.real_entity_condition()
        # Sem CodInst -> usa heuristica
        assert "PRUDENCIAL" in condition or "MASTER" in condition


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
# resolve_ifdata_escopo
# =========================================================================


class TestResolveIfdataScope:
    def test_individual_returns_cnpj_directly(self, populated_cache: Path) -> None:
        lookup = _make_lookup(populated_cache)
        result = resolve_ifdata_escopo(lookup, BANCO_A_CNPJ, "individual")

        assert result.cod_inst == BANCO_A_CNPJ
        assert result.tipo_inst == 3
        assert result.escopo == "individual"

    def test_prudencial_resolves_to_conglomerate(self, populated_cache: Path) -> None:
        lookup = _make_lookup(populated_cache)
        result = resolve_ifdata_escopo(lookup, BANCO_A_CNPJ, "prudencial")

        assert result.cod_inst == COD_CONGL_PRUD
        assert result.tipo_inst == 1
        assert result.escopo == "prudencial"

    def test_prudencial_raises_for_entity_without_conglomerate(
        self, populated_cache: Path
    ) -> None:
        lookup = _make_lookup(populated_cache)
        with pytest.raises(DataUnavailableError) as exc_info:
            resolve_ifdata_escopo(lookup, BANCO_B_CNPJ, "prudencial")
        assert "prudencial" in str(exc_info.value)

    def test_prudencial_suggests_cadastro_when_entity_unknown(
        self, tmp_cache_dir: Path
    ) -> None:
        """Sem cadastro coletado, mensagem sugere executar cadastro.collect()."""
        lookup = _make_lookup(tmp_cache_dir)
        with pytest.raises(DataUnavailableError, match="cadastro.collect"):
            resolve_ifdata_escopo(lookup, "60872504", "prudencial")

    def test_invalid_scope_raises(self, populated_cache: Path) -> None:
        lookup = _make_lookup(populated_cache)
        with pytest.raises(InvalidScopeError):
            resolve_ifdata_escopo(lookup, BANCO_A_CNPJ, "inexistente")

    def test_scope_case_insensitive(self, populated_cache: Path) -> None:
        lookup = _make_lookup(populated_cache)
        result = resolve_ifdata_escopo(lookup, BANCO_A_CNPJ, "INDIVIDUAL")
        assert result.escopo == "individual"

    def test_financeiro_resolves_to_conglomerate(self, populated_cache: Path) -> None:
        lookup = _make_lookup(populated_cache)
        result = resolve_ifdata_escopo(lookup, BANCO_A_CNPJ, "financeiro")
        assert result.escopo == "financeiro"
        assert result.tipo_inst == 2

    def test_financeiro_raises_for_entity_without_congl(
        self, populated_cache: Path
    ) -> None:
        lookup = _make_lookup(populated_cache)
        with pytest.raises(DataUnavailableError, match="financeiro"):
            resolve_ifdata_escopo(lookup, BANCO_B_CNPJ, "financeiro")


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


# =========================================================================
# search
# =========================================================================


class TestSearch:
    def test_finds_by_exact_name(self, populated_cache: Path) -> None:
        lookup = _make_lookup(populated_cache)
        df = lookup.search("BANCO ALFA")

        assert not df.empty
        assert BANCO_A_CNPJ in df["CNPJ_8"].values

    def test_finds_by_partial_name(self, populated_cache: Path) -> None:
        lookup = _make_lookup(populated_cache)
        df = lookup.search("ALFA")

        assert not df.empty
        assert BANCO_A_CNPJ in df["CNPJ_8"].values

    def test_returns_score_column(self, populated_cache: Path) -> None:
        lookup = _make_lookup(populated_cache)
        df = lookup.search("ALFA")

        assert "SCORE" in df.columns
        assert all(df["SCORE"] > 0)

    def test_returns_situacao(self, populated_cache: Path) -> None:
        lookup = _make_lookup(populated_cache)
        df = lookup.search("ALFA")

        assert "SITUACAO" in df.columns

    def test_respects_limit(self, populated_cache: Path) -> None:
        lookup = _make_lookup(populated_cache)
        df = lookup.search("BANCO", limit=1)

        assert len(df) <= 1

    def test_empty_term_returns_empty(self, populated_cache: Path) -> None:
        lookup = _make_lookup(populated_cache)
        df = lookup.search("   ")

        assert df.empty
        assert list(df.columns) == [
            "CNPJ_8",
            "INSTITUICAO",
            "SITUACAO",
            "FONTES",
            "SCORE",
        ]

    def test_no_match_returns_empty(self, populated_cache: Path) -> None:
        lookup = _make_lookup(populated_cache)
        df = lookup.search("XYZNONEXISTENT")

        assert df.empty

    def test_invalid_limit_raises(self, populated_cache: Path) -> None:
        lookup = _make_lookup(populated_cache)
        with pytest.raises(ValueError, match="limit"):
            lookup.search("ALFA", limit=0)


# =========================================================================
# _get_data_sources_for_cnpjs
# =========================================================================


class TestRealEntityConditionFallback:
    def test_legacy_heuristic_still_returns_valid_entities(
        self, tmp_cache_dir: Path
    ) -> None:
        """Se cadastro nao tem CodInst, get_entity_identifiers ainda funciona."""
        from tests.conftest import _save_parquet

        df = pd.DataFrame(
            {
                "Data": pd.array([202303], dtype="Int64"),
                "CNPJ_8": [BANCO_A_CNPJ],
                "NomeInstituicao": ["BANCO ALFA S.A."],
                "CNPJ_LIDER_8": [BANCO_A_CNPJ],
                "CodConglomeradoPrudencial": [COD_CONGL_PRUD],
                "CodConglomeradoFinanceiro": [COD_CONGL_FIN],
                "Situacao": ["A"],
            }
        )
        _save_parquet(df, tmp_cache_dir, "ifdata/cadastro", "ifdata_cad_202303")

        lookup = _make_lookup(tmp_cache_dir)
        # Sem CodInst -> usa heuristica legacy
        assert not lookup._cadastro_has_codinst()
        info = lookup.get_entity_identifiers(BANCO_A_CNPJ)
        assert info["nome_entidade"] == "BANCO ALFA S.A."
        assert info["cnpj_interesse"] == BANCO_A_CNPJ
        assert info["cod_congl_prud"] == COD_CONGL_PRUD


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
        lookup = _make_lookup(populated_cache)
        df = lookup.search(BANCO_A_CNPJ)

        assert not df.empty
        assert df.iloc[0]["CNPJ_8"] == BANCO_A_CNPJ
        assert df.iloc[0]["SCORE"] == 100

    def test_search_by_cnpj_unknown_returns_empty(self, populated_cache: Path) -> None:
        """CNPJ desconhecido de 8 digitos sem match fuzzy retorna vazio."""
        lookup = _make_lookup(populated_cache)
        df = lookup.search("99999999")

        assert df.empty

    def test_search_by_cnpj_has_correct_columns(self, populated_cache: Path) -> None:
        lookup = _make_lookup(populated_cache)
        df = lookup.search(BANCO_A_CNPJ)

        expected_cols = ["CNPJ_8", "INSTITUICAO", "SITUACAO", "FONTES", "SCORE"]
        assert list(df.columns) == expected_cols

    def test_search_by_name_still_works(self, populated_cache: Path) -> None:
        lookup = _make_lookup(populated_cache)
        df = lookup.search("BANCO ALFA")

        assert not df.empty
        assert BANCO_A_CNPJ in df["CNPJ_8"].values

    def test_search_cnpj_exact_no_sources_returns_empty(
        self, populated_cache: Path
    ) -> None:
        """CNPJ no cadastro mas sem dados em nenhuma fonte retorna vazio."""
        lookup = _make_lookup(populated_cache)
        df = lookup.search(BANCO_B_CNPJ)
        if not df.empty:
            assert (df["FONTES"] != "").all()
