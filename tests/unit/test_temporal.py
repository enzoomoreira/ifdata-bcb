"""Testes unitarios para TemporalResolver.add_cnpj_mapping."""

import pandas as pd

from ifdata_bcb.providers.ifdata.valores.temporal import TemporalResolver


class TestAddCnpjMapping:
    def test_empty_map_uses_cod_inst_directly(self) -> None:
        df = pd.DataFrame({"CodInst": ["A", "B"], "val": [1, 2]})
        result = TemporalResolver.add_cnpj_mapping(df, {})
        assert "CNPJ_8" in result.columns
        assert list(result["CNPJ_8"]) == ["A", "B"]

    def test_map_expands_to_multiple_cnpjs(self) -> None:
        df = pd.DataFrame({"CodInst": ["COD1", "COD1"], "val": [1, 2]})
        cnpj_map = {"COD1": ["CNPJ_A", "CNPJ_B"]}
        result = TemporalResolver.add_cnpj_mapping(df, cnpj_map)
        assert set(result["CNPJ_8"]) == {"CNPJ_A", "CNPJ_B"}
        assert len(result) == 4  # 2 rows x 2 cnpjs

    def test_empty_df_returns_empty(self) -> None:
        df = pd.DataFrame(
            {"CodInst": pd.Series([], dtype=str), "val": pd.Series([], dtype=float)}
        )
        result = TemporalResolver.add_cnpj_mapping(df, {"X": ["Y"]})
        assert result.empty
