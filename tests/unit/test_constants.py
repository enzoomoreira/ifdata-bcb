"""Testes para ifdata_bcb.core.constants."""

import pytest

from ifdata_bcb.core.constants import (
    DATA_SOURCES,
    TIPO_INST_MAP,
    get_pattern,
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
