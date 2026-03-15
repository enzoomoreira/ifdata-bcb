"""Testes para ifdata_bcb.core.eras -- deteccao de eras e warnings."""

import warnings
from pathlib import Path


from ifdata_bcb.core.eras import (
    COSIF_ERA_BOUNDARY,
    IFDATA_ERA_BOUNDARY,
    build_cosif_select,
    check_era_boundary,
    detect_cosif_csv_era,
)
from ifdata_bcb.domain.exceptions import IncompatibleEraWarning


# =========================================================================
# Helpers
# =========================================================================

ERA_1_HEADER = "DATA;CNPJ;NOME INSTITUICAO;ATRIBUTO;DOCUMENTO;CONTA;NOME CONTA;SALDO"
ERA_2_HEADER = "#DATA_BASE;DOCUMENTO;CNPJ;AGENCIA;NOME_INSTITUICAO;COD_CONGL;NOME_CONGL;TAXONOMIA;CONTA;NOME_CONTA;SALDO"

METADATA_LINES = [
    "Balancete Patrimonial (Codigo documento 4010)",
    "Data de geracao dos dados: 14/12/2009",
    "Fonte: Instituicoes financeiras",
]


def _write_csv(path: Path, header: str, rows: list[str] | None = None) -> Path:
    lines = METADATA_LINES + [header]
    if rows:
        lines.extend(rows)
    path.write_text("\n".join(lines), encoding="utf-8")
    return path


# =========================================================================
# detect_cosif_csv_era
# =========================================================================


class TestDetectCosifCsvEra:
    def test_era_1_header_returns_1(self, workspace_tmp_dir: Path) -> None:
        csv = _write_csv(workspace_tmp_dir / "e1.csv", ERA_1_HEADER)
        assert detect_cosif_csv_era(csv, "utf-8") == 1

    def test_era_2_header_returns_2(self, workspace_tmp_dir: Path) -> None:
        csv = _write_csv(workspace_tmp_dir / "e2.csv", ERA_2_HEADER)
        assert detect_cosif_csv_era(csv, "utf-8") == 2

    def test_era_3_same_as_era_2(self, workspace_tmp_dir: Path) -> None:
        """Era 3 tem mesmas colunas que Era 2 -- deve retornar 2."""
        csv = _write_csv(workspace_tmp_dir / "e3.csv", ERA_2_HEADER)
        assert detect_cosif_csv_era(csv, "utf-8") == 2

    def test_cp1252_encoding(self, workspace_tmp_dir: Path) -> None:
        """Headers reais usam CP1252 com acentos."""
        path = workspace_tmp_dir / "cp.csv"
        lines = METADATA_LINES + [ERA_2_HEADER]
        path.write_text("\n".join(lines), encoding="CP1252")
        assert detect_cosif_csv_era(path, "CP1252") == 2

    def test_corrupted_encoding_still_detects(self, workspace_tmp_dir: Path) -> None:
        """errors='replace' deve permitir deteccao mesmo com encoding errado."""
        path = workspace_tmp_dir / "bad.csv"
        lines = METADATA_LINES + [ERA_2_HEADER]
        path.write_bytes("\n".join(lines).encode("CP1252"))
        # Ler com utf-8 (errado) -- errors=replace nao deve crashar
        assert detect_cosif_csv_era(path, "utf-8") == 2

    def test_file_with_only_3_lines_returns_era_1(
        self, workspace_tmp_dir: Path
    ) -> None:
        """CSV truncado sem header real -- readline retorna '' e nao contem #DATA_BASE."""
        path = workspace_tmp_dir / "short.csv"
        path.write_text("\n".join(METADATA_LINES), encoding="utf-8")
        # Sem header, readline() retorna string vazia -> nao tem #DATA_BASE -> era 1
        assert detect_cosif_csv_era(path, "utf-8") == 1

    def test_header_with_extra_whitespace(self, workspace_tmp_dir: Path) -> None:
        """BCB por vezes inclui espacos extras nos headers."""
        csv = _write_csv(
            workspace_tmp_dir / "ws.csv",
            "  #DATA_BASE ;DOCUMENTO;CNPJ;AGENCIA;NOME_INSTITUICAO;COD_CONGL;NOME_CONGL;TAXONOMIA;CONTA;NOME_CONTA;SALDO  ",
        )
        assert detect_cosif_csv_era(csv, "utf-8") == 2


# =========================================================================
# build_cosif_select
# =========================================================================


class TestBuildCosifSelect:
    def test_era_1_sql_has_upper_nome_conta(self, workspace_tmp_dir: Path) -> None:
        sql = build_cosif_select(1, workspace_tmp_dir / "f.csv", "utf-8")
        assert 'UPPER("NOME CONTA")' in sql

    def test_era_2_sql_has_upper_nome_conta(self, workspace_tmp_dir: Path) -> None:
        sql = build_cosif_select(2, workspace_tmp_dir / "f.csv", "utf-8")
        assert "UPPER(NOME_CONTA)" in sql

    def test_era_1_casts_conta_to_bigint(self, workspace_tmp_dir: Path) -> None:
        sql = build_cosif_select(1, workspace_tmp_dir / "f.csv", "utf-8")
        assert "CAST(CONTA AS BIGINT)" in sql

    def test_era_2_does_not_cast_conta(self, workspace_tmp_dir: Path) -> None:
        sql = build_cosif_select(2, workspace_tmp_dir / "f.csv", "utf-8")
        assert "CAST(CONTA" not in sql

    def test_era_1_maps_old_column_names(self, workspace_tmp_dir: Path) -> None:
        sql = build_cosif_select(1, workspace_tmp_dir / "f.csv", "utf-8")
        assert '"DATA" as DATA_BASE' in sql
        assert '"NOME INSTITUICAO" as NOME_INSTITUICAO' in sql

    def test_era_2_uses_data_base_directly(self, workspace_tmp_dir: Path) -> None:
        sql = build_cosif_select(2, workspace_tmp_dir / "f.csv", "utf-8")
        assert '"#DATA_BASE" as DATA_BASE' in sql

    def test_windows_backslashes_converted(self, workspace_tmp_dir: Path) -> None:
        """Paths Windows com backslash devem ser convertidos para forward slash."""
        fake_path = Path("C:\\Users\\test\\data.csv")
        sql = build_cosif_select(1, fake_path, "utf-8")
        assert "\\" not in sql
        assert "C:/Users/test/data.csv" in sql

    def test_both_eras_produce_same_output_columns(
        self, workspace_tmp_dir: Path
    ) -> None:
        """Ambas as queries devem selecionar as mesmas 7 colunas de output."""
        expected = {
            "DATA_BASE",
            "CNPJ",
            "NOME_INSTITUICAO",
            "DOCUMENTO",
            "CONTA",
            "NOME_CONTA",
            "SALDO",
        }
        for era in [1, 2]:
            sql = build_cosif_select(era, workspace_tmp_dir / "f.csv", "utf-8")
            # Extrair aliases (as X) e nomes diretos do SELECT
            for col in expected:
                assert col in sql, f"Era {era}: coluna {col} ausente no SQL"


# =========================================================================
# check_era_boundary
# =========================================================================


class TestCheckEraBoundary:
    def test_crossing_boundary_emits_warning(self) -> None:
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            check_era_boundary([202412, 202501], COSIF_ERA_BOUNDARY, "COSIF")
            assert len(w) == 1
            assert issubclass(w[0].category, IncompatibleEraWarning)

    def test_all_before_boundary_no_warning(self) -> None:
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            check_era_boundary([202401, 202412], COSIF_ERA_BOUNDARY, "COSIF")
            assert len(w) == 0

    def test_all_after_boundary_no_warning(self) -> None:
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            check_era_boundary([202501, 202506], COSIF_ERA_BOUNDARY, "COSIF")
            assert len(w) == 0

    def test_single_date_no_warning(self) -> None:
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            check_era_boundary([202501], COSIF_ERA_BOUNDARY, "COSIF")
            assert len(w) == 0

    def test_none_dates_no_warning(self) -> None:
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            check_era_boundary(None, COSIF_ERA_BOUNDARY, "COSIF")
            assert len(w) == 0

    def test_empty_list_no_warning(self) -> None:
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            check_era_boundary([], COSIF_ERA_BOUNDARY, "COSIF")
            assert len(w) == 0

    def test_boundary_exact_on_max_triggers(self) -> None:
        """max == boundary: min < boundary <= max deve ser True."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            check_era_boundary([202412, 202501], COSIF_ERA_BOUNDARY, "X")
            assert len(w) == 1

    def test_boundary_exact_on_min_no_warning(self) -> None:
        """min == boundary: min < boundary is False."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            check_era_boundary([202501, 202506], COSIF_ERA_BOUNDARY, "X")
            assert len(w) == 0

    def test_ifdata_boundary_works(self) -> None:
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            check_era_boundary([202412, 202503], IFDATA_ERA_BOUNDARY, "IFDATA")
            assert len(w) == 1

    def test_unsorted_dates_still_detects(self) -> None:
        """Datas fora de ordem devem funcionar (usa min/max)."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            check_era_boundary([202506, 202401, 202501], COSIF_ERA_BOUNDARY, "X")
            assert len(w) == 1

    def test_warning_message_includes_source_name(self) -> None:
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            check_era_boundary([202412, 202501], COSIF_ERA_BOUNDARY, "MeuFonte")
            assert "MeuFonte" in str(w[0].message)

    def test_warning_message_includes_boundary(self) -> None:
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            check_era_boundary([202412, 202501], COSIF_ERA_BOUNDARY, "X")
            assert "202501" in str(w[0].message)
