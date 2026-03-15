"""Testes para ifdata_bcb.core.constants."""

import pytest

from ifdata_bcb.core.constants import (
    DATA_SOURCES,
    FIRST_AVAILABLE_PERIOD,
    TIPO_INST_MAP,
    get_first_available,
    get_pattern,
    get_source_key,
    get_subdir,
)


class TestTipoInstMap:
    """Mapeamento de escopos para codigos IFDATA."""

    def test_individual(self) -> None:
        assert TIPO_INST_MAP["individual"] == 3

    def test_prudencial(self) -> None:
        assert TIPO_INST_MAP["prudencial"] == 1

    def test_financeiro(self) -> None:
        assert TIPO_INST_MAP["financeiro"] == 2

    def test_only_three_scopes(self) -> None:
        assert set(TIPO_INST_MAP.keys()) == {"individual", "prudencial", "financeiro"}


class TestGetPattern:
    def test_known_source(self) -> None:
        pattern = get_pattern("cadastro")
        assert pattern == "ifdata_cad_*.parquet"

    def test_unknown_source_raises(self) -> None:
        with pytest.raises(KeyError):
            get_pattern("nonexistent")


class TestGetSubdir:
    def test_known_source(self) -> None:
        assert get_subdir("cadastro") == "ifdata/cadastro"

    def test_cosif_individual(self) -> None:
        assert get_subdir("cosif_individual") == "cosif/individual"

    def test_unknown_source_raises(self) -> None:
        with pytest.raises(KeyError):
            get_subdir("nonexistent")


class TestDataSourcesConsistency:
    """Todas as fontes devem ter subdir e prefix."""

    @pytest.mark.parametrize("source_name", list(DATA_SOURCES.keys()))
    def test_has_required_keys(self, source_name: str) -> None:
        source = DATA_SOURCES[source_name]
        assert "subdir" in source
        assert "prefix" in source

    @pytest.mark.parametrize("source_name", list(DATA_SOURCES.keys()))
    def test_get_pattern_matches(self, source_name: str) -> None:
        pattern = get_pattern(source_name)
        assert pattern.endswith("_*.parquet")
        assert DATA_SOURCES[source_name]["prefix"] in pattern


class TestGetSourceKey:
    """Reverse lookup: prefix -> source key."""

    @pytest.mark.parametrize(
        "prefix,expected",
        [
            ("cosif_ind", "cosif_individual"),
            ("cosif_prud", "cosif_prudencial"),
            ("ifdata_val", "ifdata_valores"),
            ("ifdata_cad", "cadastro"),
        ],
    )
    def test_known_prefixes(self, prefix: str, expected: str) -> None:
        assert get_source_key(prefix) == expected

    def test_unknown_prefix_returns_none(self) -> None:
        assert get_source_key("unknown_prefix") is None

    def test_all_data_sources_have_reverse_mapping(self) -> None:
        for key, cfg in DATA_SOURCES.items():
            assert get_source_key(cfg["prefix"]) == key


class TestFirstAvailablePeriod:
    """Registry de cutoff dates."""

    def test_all_data_sources_have_cutoff(self) -> None:
        for key in DATA_SOURCES:
            assert key in FIRST_AVAILABLE_PERIOD, (
                f"Fonte '{key}' sem cutoff em FIRST_AVAILABLE_PERIOD"
            )

    def test_cutoff_values_are_valid_yyyymm(self) -> None:
        for key, period in FIRST_AVAILABLE_PERIOD.items():
            month = period % 100
            year = period // 100
            assert 1 <= month <= 12, f"{key}: mes invalido {month}"
            assert 1990 <= year <= 2030, f"{key}: ano fora do range {year}"

    def test_cosif_prudencial_after_individual(self) -> None:
        assert (
            FIRST_AVAILABLE_PERIOD["cosif_prudencial"]
            > FIRST_AVAILABLE_PERIOD["cosif_individual"]
        )


class TestGetFirstAvailable:
    """Lookup de cutoff por prefix."""

    def test_cosif_individual(self) -> None:
        assert get_first_available("cosif_ind") == 199501

    def test_cosif_prudencial(self) -> None:
        assert get_first_available("cosif_prud") == 201407

    def test_ifdata_valores(self) -> None:
        assert get_first_available("ifdata_val") == 200303

    def test_ifdata_cadastro(self) -> None:
        assert get_first_available("ifdata_cad") == 200503

    def test_unknown_prefix_returns_none(self) -> None:
        assert get_first_available("unknown") is None
