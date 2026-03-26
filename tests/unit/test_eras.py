"""Testes para ifdata_bcb.core.eras -- deteccao de eras e warnings."""

import warnings
from pathlib import Path

from ifdata_bcb.core.eras import (
    COSIF_ERA_BOUNDARY,
    IFDATA_ERA_BOUNDARY,
    _is_credit_report,
    _is_stable_report,
    _match_dropped_report,
    _normalize_report_name,
    build_cosif_select,
    check_era_boundary,
    check_ifdata_era,
    detect_cosif_csv_era,
)
from ifdata_bcb.domain.exceptions import (
    DroppedReportWarning,
    IncompatibleEraWarning,
    ScopeMigrationWarning,
)


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

    def test_era_boundary_message_is_source_aware(self) -> None:
        """Warning message deve usar o source_name fornecido, nao hardcoded."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            check_era_boundary([202412, 202503], IFDATA_ERA_BOUNDARY, "IFDATA")
            msg = str(w[0].message)
            assert "IFDATA" in msg
            # Nao deve mencionar COSIF quando o source e IFDATA
            assert "COSIF" not in msg


# =========================================================================
# _normalize_report_name
# =========================================================================


class TestNormalizeReportName:
    def test_removes_accents(self) -> None:
        assert (
            _normalize_report_name("Informacoes de Capital") == "informacoes de capital"
        )

    def test_removes_cedilla_and_tilde(self) -> None:
        assert (
            _normalize_report_name("Informacoes de Capital") == "informacoes de capital"
        )

    def test_lowercase(self) -> None:
        assert _normalize_report_name("RESUMO") == "resumo"

    def test_strips_whitespace(self) -> None:
        assert _normalize_report_name("  Ativo  ") == "ativo"

    def test_mixed_accents_and_case(self) -> None:
        result = _normalize_report_name("Carteira de Credito Ativa")
        assert result == "carteira de credito ativa"

    def test_empty_string(self) -> None:
        assert _normalize_report_name("") == ""


# =========================================================================
# _is_credit_report
# =========================================================================


class TestIsCreditReport:
    def test_exact_prefix_match(self) -> None:
        assert _is_credit_report("Carteira de credito ativa") is True

    def test_prefix_with_suffix(self) -> None:
        assert (
            _is_credit_report(
                "Carteira de credito ativa - por nivel de risco da operacao"
            )
            is True
        )

    def test_accented_input(self) -> None:
        assert _is_credit_report("Carteira de credito ativa") is True

    def test_uppercase_input(self) -> None:
        assert _is_credit_report("CARTEIRA DE CREDITO ATIVA") is True

    def test_non_credit_report(self) -> None:
        assert _is_credit_report("Resumo") is False

    def test_none_returns_false(self) -> None:
        assert _is_credit_report(None) is False


# =========================================================================
# _is_stable_report
# =========================================================================


class TestIsStableReport:
    def test_credit_report_is_stable(self) -> None:
        assert _is_stable_report("Carteira de credito ativa") is True

    def test_informacoes_capital_is_stable(self) -> None:
        assert _is_stable_report("Informacoes de Capital") is True

    def test_informacoes_capital_with_accents(self) -> None:
        assert _is_stable_report("Informacoes de Capital") is True

    def test_resumo_is_not_stable(self) -> None:
        assert _is_stable_report("Resumo") is False

    def test_ativo_is_not_stable(self) -> None:
        assert _is_stable_report("Ativo") is False

    def test_none_is_not_stable(self) -> None:
        assert _is_stable_report(None) is False


# =========================================================================
# _match_dropped_report
# =========================================================================


class TestMatchDroppedReport:
    def test_matches_dropped_report(self) -> None:
        result = _match_dropped_report(
            "Carteira de credito ativa - por nivel de risco da operacao"
        )
        assert result == 202412

    def test_accented_input_matches(self) -> None:
        result = _match_dropped_report(
            "Carteira de credito ativa - por nivel de risco da operacao"
        )
        assert result == 202412

    def test_non_dropped_returns_none(self) -> None:
        assert _match_dropped_report("Resumo") is None

    def test_credit_prefix_alone_not_dropped(self) -> None:
        assert _match_dropped_report("Carteira de credito ativa") is None

    def test_none_returns_none(self) -> None:
        assert _match_dropped_report(None) is None


# =========================================================================
# check_ifdata_era
# =========================================================================


class TestCheckIfdataEra:
    """Testes para check_ifdata_era -- verificacoes de era IFDATA Valores."""

    # --- Sem warning ---

    def test_none_dates_no_warning(self) -> None:
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            check_ifdata_era(None)
            assert len(w) == 0

    def test_empty_dates_no_warning(self) -> None:
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            check_ifdata_era([])
            assert len(w) == 0

    def test_all_before_boundary_no_warning(self) -> None:
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            check_ifdata_era([202406, 202412], relatorio="Resumo")
            assert len(w) == 0

    def test_all_after_boundary_no_warning(self) -> None:
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            check_ifdata_era([202503, 202506], relatorio="Resumo")
            assert len(w) == 0

    def test_single_date_no_era_warning(self) -> None:
        """Single date nao cruza boundary -- sem IncompatibleEra."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            check_ifdata_era([202412], relatorio="Resumo")
            assert len(w) == 0

    # --- IncompatibleEraWarning ---

    def test_accounting_report_crossing_boundary_emits_incompatible(self) -> None:
        """Resumo/Ativo/Passivo/DRE cruzando boundary -> IncompatibleEraWarning."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            check_ifdata_era([202412, 202503], relatorio="Resumo")
            assert len(w) == 1
            assert issubclass(w[0].category, IncompatibleEraWarning)

    def test_none_relatorio_crossing_boundary_emits_incompatible(self) -> None:
        """relatorio=None nao e estavel -> emite IncompatibleEraWarning."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            check_ifdata_era([202412, 202503], relatorio=None)
            assert len(w) == 1
            assert issubclass(w[0].category, IncompatibleEraWarning)

    def test_incompatible_warning_has_correct_attributes(self) -> None:
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            check_ifdata_era([202412, 202503], relatorio="Ativo")
            msg = w[0].message
            assert msg.boundary == IFDATA_ERA_BOUNDARY
            assert msg.source == "IFDATA"

    # --- Stable reports: NO IncompatibleEraWarning ---

    def test_credit_report_crossing_boundary_no_incompatible(self) -> None:
        """Credit report e estavel -- nao emite IncompatibleEraWarning."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            check_ifdata_era([202412, 202503], relatorio="Carteira de credito ativa")
            incompatible = [
                x for x in w if issubclass(x.category, IncompatibleEraWarning)
            ]
            assert len(incompatible) == 0

    def test_capital_info_crossing_boundary_no_incompatible(self) -> None:
        """Informacoes de Capital e estavel."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            check_ifdata_era([202412, 202503], relatorio="Informacoes de Capital")
            incompatible = [
                x for x in w if issubclass(x.category, IncompatibleEraWarning)
            ]
            assert len(incompatible) == 0

    # --- ScopeMigrationWarning ---

    def test_credit_financeiro_crossing_boundary_emits_scope_migration(self) -> None:
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            check_ifdata_era(
                [202412, 202503],
                relatorio="Carteira de credito ativa",
                escopo="financeiro",
            )
            migration = [x for x in w if issubclass(x.category, ScopeMigrationWarning)]
            assert len(migration) == 1

    def test_credit_prudencial_crossing_boundary_emits_scope_migration(self) -> None:
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            check_ifdata_era(
                [202412, 202503],
                relatorio="Carteira de credito ativa",
                escopo="prudencial",
            )
            migration = [x for x in w if issubclass(x.category, ScopeMigrationWarning)]
            assert len(migration) == 1

    def test_scope_migration_attributes_financeiro(self) -> None:
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            check_ifdata_era(
                [202412, 202503],
                relatorio="Carteira de credito ativa",
                escopo="financeiro",
            )
            migration = [x for x in w if issubclass(x.category, ScopeMigrationWarning)]
            msg = migration[0].message
            assert msg.escopo_pre == "financeiro"
            assert msg.escopo_post == "prudencial"
            assert msg.boundary == IFDATA_ERA_BOUNDARY

    def test_credit_no_escopo_crossing_boundary_no_scope_migration(self) -> None:
        """escopo=None nao dispara ScopeMigrationWarning."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            check_ifdata_era(
                [202412, 202503],
                relatorio="Carteira de credito ativa",
                escopo=None,
            )
            migration = [x for x in w if issubclass(x.category, ScopeMigrationWarning)]
            assert len(migration) == 0

    def test_non_credit_report_no_scope_migration(self) -> None:
        """Resumo com escopo=financeiro nao dispara ScopeMigrationWarning."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            check_ifdata_era([202412, 202503], relatorio="Resumo", escopo="financeiro")
            migration = [x for x in w if issubclass(x.category, ScopeMigrationWarning)]
            assert len(migration) == 0

    # --- DroppedReportWarning ---

    def test_dropped_report_after_last_period_emits_warning(self) -> None:
        """Periodo apos last_period -> DroppedReportWarning, mesmo sem cruzar boundary."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            check_ifdata_era(
                [202503],
                relatorio="Carteira de credito ativa - por nivel de risco da operacao",
            )
            dropped = [x for x in w if issubclass(x.category, DroppedReportWarning)]
            assert len(dropped) == 1
            assert dropped[0].message.last_period == 202412

    def test_dropped_report_within_last_period_no_warning(self) -> None:
        """Periodo <= last_period -> sem DroppedReportWarning."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            check_ifdata_era(
                [202412],
                relatorio="Carteira de credito ativa - por nivel de risco da operacao",
            )
            dropped = [x for x in w if issubclass(x.category, DroppedReportWarning)]
            assert len(dropped) == 0

    def test_non_dropped_report_no_dropped_warning(self) -> None:
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            check_ifdata_era([202503], relatorio="Resumo")
            dropped = [x for x in w if issubclass(x.category, DroppedReportWarning)]
            assert len(dropped) == 0

    # --- Combinacao de warnings ---

    def test_dropped_credit_financeiro_crossing_emits_two_warnings(self) -> None:
        """Dropped credit + escopo financeiro crossing boundary:
        DroppedReportWarning + ScopeMigrationWarning. Nao IncompatibleEraWarning."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            check_ifdata_era(
                [202412, 202503],
                relatorio="Carteira de credito ativa - por nivel de risco da operacao",
                escopo="financeiro",
            )
            dropped = [x for x in w if issubclass(x.category, DroppedReportWarning)]
            migration = [x for x in w if issubclass(x.category, ScopeMigrationWarning)]
            incompatible = [
                x for x in w if issubclass(x.category, IncompatibleEraWarning)
            ]
            assert len(dropped) == 1
            assert len(migration) == 1
            assert len(incompatible) == 0

    def test_accounting_report_no_escopo_crossing_emits_only_incompatible(self) -> None:
        """Resumo sem escopo cruzando boundary: apenas IncompatibleEraWarning."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            check_ifdata_era([202412, 202503], relatorio="Resumo", escopo=None)
            assert len(w) == 1
            assert issubclass(w[0].category, IncompatibleEraWarning)
