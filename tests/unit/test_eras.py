"""Testes para ifdata_bcb.core.eras -- deteccao de eras e warnings."""

import warnings
from pathlib import Path

import pandas as pd
import pytest

from ifdata_bcb.core.eras import (
    COSIF_ERA_BOUNDARY,
    IFDATA_ERA_BOUNDARY,
    _is_credit_report,
    _match_dropped_report,
    _normalize_report_name,
    build_cosif_select,
    detect_cosif_csv_era,
    diagnose_eras,
    emit_era_warnings,
)
from ifdata_bcb.domain.exceptions import (
    DataProcessingError,
    DroppedReportWarning,
    IncompatibleEraWarning,
    PartialDataWarning,
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

CREDITO_DROPPED = "Carteira de credito ativa - por nivel de risco da operacao"
CREDITO_NOVO = "Carteira de credito ativa - por carteiras de instrumentos financeiros"


def _write_csv(path: Path, header: str, rows: list[str] | None = None) -> Path:
    lines = [*METADATA_LINES, header]
    if rows:
        lines.extend(rows)
    path.write_text("\n".join(lines), encoding="utf-8")
    return path


def _make_df(
    rows: list[tuple[int, str, str]], date_as_int: bool = False
) -> pd.DataFrame:
    """Constroi DataFrame de (periodo YYYYMM, grupo, codigo de conta)."""
    df = pd.DataFrame(rows, columns=["_p", "relatorio", "cod_conta"])
    if date_as_int:
        df["data"] = df["_p"]
    else:
        df["data"] = pd.to_datetime(df["_p"].astype(str) + "01", format="%Y%m%d")
    return df.drop(columns="_p")


def _diagnose(rows, solicitados, **kwargs):
    """diagnose_eras com os defaults do IFDATA."""
    kwargs.setdefault("group_col", "relatorio")
    return diagnose_eras(
        _make_df(rows, date_as_int=kwargs.pop("date_as_int", False)),
        boundary=IFDATA_ERA_BOUNDARY,
        source="IFDATA",
        periodos_solicitados=solicitados,
        **kwargs,
    )


def _emit(diag) -> list[warnings.WarningMessage]:
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        emit_era_warnings(diag)
    return list(w)


CRUZA = [202412, 202503]


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
        lines = [*METADATA_LINES, ERA_2_HEADER]
        path.write_text("\n".join(lines), encoding="CP1252")
        assert detect_cosif_csv_era(path, "CP1252") == 2

    def test_corrupted_encoding_still_detects(self, workspace_tmp_dir: Path) -> None:
        """errors='replace' deve permitir deteccao mesmo com encoding errado."""
        path = workspace_tmp_dir / "bad.csv"
        lines = [*METADATA_LINES, ERA_2_HEADER]
        path.write_bytes("\n".join(lines).encode("CP1252"))
        # Ler com utf-8 (errado) -- errors=replace nao deve crashar
        assert detect_cosif_csv_era(path, "utf-8") == 2

    def test_truncated_file_raises(self, workspace_tmp_dir: Path) -> None:
        """
        CSV truncado sem header nao pode ser assumido como era 1.

        O fallback silencioso levava o arquivo ao SELECT da era 1, que falhava
        depois com um Binder Error criptico do DuckDB.
        """
        path = workspace_tmp_dir / "short.csv"
        path.write_text("\n".join(METADATA_LINES), encoding="utf-8")

        with pytest.raises(DataProcessingError, match="Formato de CSV desconhecido"):
            detect_cosif_csv_era(path, "utf-8")

    def test_unknown_header_raises_with_headers_in_message(
        self, workspace_tmp_dir: Path
    ) -> None:
        """Formato novo do BCB deve dizer o que encontrou."""
        path = workspace_tmp_dir / "futuro.csv"
        novo_header = "COL_A;COL_B;COL_C"
        path.write_text(
            "\n".join([*METADATA_LINES, novo_header]),
            encoding="utf-8",
        )

        with pytest.raises(DataProcessingError) as exc_info:
            detect_cosif_csv_era(path, "utf-8")

        assert "COL_A" in str(exc_info.value)

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
# diagnose_eras -- cobertura de periodos e gatilho da analise
# =========================================================================


class TestDiagnoseErasCobertura:
    def test_df_vazio_nao_cruza(self) -> None:
        diag = diagnose_eras(
            pd.DataFrame(),
            boundary=IFDATA_ERA_BOUNDARY,
            source="IFDATA",
            periodos_solicitados=CRUZA,
        )
        assert diag["cruza_boundary"] is False
        assert diag["grupos"] == {}

    def test_solicitados_none_nao_cruza(self) -> None:
        diag = _diagnose([(202412, "Ativo", "1"), (202503, "Ativo", "2")], None)
        assert diag["cruza_boundary"] is False

    def test_range_so_antes_do_boundary_nao_cruza(self) -> None:
        diag = _diagnose(
            [(202409, "Ativo", "1"), (202412, "Ativo", "1")], [202409, 202412]
        )
        assert diag["cruza_boundary"] is False
        assert diag["grupos"] == {}

    def test_range_so_depois_do_boundary_nao_cruza(self) -> None:
        diag = _diagnose(
            [(202503, "Ativo", "1"), (202506, "Ativo", "1")], [202503, 202506]
        )
        assert diag["cruza_boundary"] is False

    def test_boundary_exato_no_inicio_nao_cruza(self) -> None:
        """Range comecando no boundary esta inteiro na era nova."""
        diag = _diagnose([(202503, "Ativo", "1")], [202503, 202506])
        assert diag["cruza_boundary"] is False

    def test_range_cruzando_ativa_analise(self) -> None:
        diag = _diagnose([(202412, "Ativo", "1"), (202503, "Ativo", "2")], CRUZA)
        assert diag["cruza_boundary"] is True

    def test_periodos_presentes_e_ausentes(self) -> None:
        diag = _diagnose([(202503, "Ativo", "1")], CRUZA)
        assert diag["periodos_presentes"] == [202503]
        assert diag["periodos_ausentes"] == [202412]

    def test_data_como_int_yyyymm(self) -> None:
        """Aceita coluna de data ja em int, nao so datetime."""
        diag = _diagnose(
            [(202412, "Ativo", "1"), (202503, "Ativo", "2")], CRUZA, date_as_int=True
        )
        assert diag["cruza_boundary"] is True
        assert diag["grupos"]["Ativo"]["status"] == "renumerado"

    def test_sem_coluna_de_conta_so_cobertura(self) -> None:
        """columns= pode remover cod_conta: degrada para cobertura de periodo."""
        df = _make_df([(202412, "Ativo", "1"), (202503, "Ativo", "2")]).drop(
            columns="cod_conta"
        )
        diag = diagnose_eras(
            df,
            boundary=IFDATA_ERA_BOUNDARY,
            source="IFDATA",
            periodos_solicitados=CRUZA,
            group_col="relatorio",
        )
        assert diag["cruza_boundary"] is True
        assert diag["grupos"] == {}

    def test_sem_group_col_analisa_global(self) -> None:
        diag = _diagnose(
            [(202412, "Ativo", "1"), (202503, "Passivo", "2")], CRUZA, group_col=None
        )
        assert list(diag["grupos"]) == ["IFDATA"]
        assert diag["grupos"]["IFDATA"]["status"] == "renumerado"

    def test_group_col_ausente_do_df_analisa_global(self) -> None:
        diag = _diagnose(
            [(202412, "Ativo", "1"), (202503, "Ativo", "2")],
            CRUZA,
            group_col="NAO_EXISTE",
        )
        assert list(diag["grupos"]) == ["IFDATA"]


# =========================================================================
# diagnose_eras -- classificacao por overlap de contas
# =========================================================================


class TestDiagnoseErasClassificacao:
    def test_contas_identicas_sao_estaveis(self) -> None:
        diag = _diagnose(
            [
                (202412, "Credito", "1"),
                (202412, "Credito", "2"),
                (202503, "Credito", "1"),
                (202503, "Credito", "2"),
            ],
            CRUZA,
        )
        grupo = diag["grupos"]["Credito"]
        assert grupo["status"] == "estavel"
        assert grupo["pct_overlap"] == 100.0
        assert grupo["n_comum"] == 2

    def test_contas_disjuntas_sao_renumeradas(self) -> None:
        diag = _diagnose([(202412, "Ativo", "1"), (202503, "Ativo", "9")], CRUZA)
        grupo = diag["grupos"]["Ativo"]
        assert grupo["status"] == "renumerado"
        assert grupo["pct_overlap"] == 0.0

    def test_threshold_no_limite_e_estavel(self) -> None:
        """9 de 10 contas em comum = 90% = exatamente o threshold."""
        pre = [(202412, "R", str(i)) for i in range(10)]
        post = [(202503, "R", str(i)) for i in range(1, 11)]
        grupo = _diagnose(pre + post, CRUZA)["grupos"]["R"]
        assert grupo["pct_overlap"] == 90.0
        assert grupo["status"] == "estavel"

    def test_abaixo_do_threshold_e_renumerado(self) -> None:
        """8 de 10 contas em comum = 80% < 90%."""
        pre = [(202412, "R", str(i)) for i in range(10)]
        post = [(202503, "R", str(i)) for i in range(2, 12)]
        grupo = _diagnose(pre + post, CRUZA)["grupos"]["R"]
        assert grupo["pct_overlap"] == 80.0
        assert grupo["status"] == "renumerado"

    def test_pct_usa_o_maior_lado_como_denominador(self) -> None:
        """Lado novo com mais contas nao pode inflar o overlap."""
        pre = [(202412, "R", "1")]
        post = [(202503, "R", str(i)) for i in range(1, 5)]
        grupo = _diagnose(pre + post, CRUZA)["grupos"]["R"]
        assert grupo["n_comum"] == 1
        assert grupo["pct_overlap"] == 25.0

    def test_grupo_so_antes_do_boundary(self) -> None:
        diag = _diagnose(
            [(202412, "Ativo", "1"), (202412, "Sumiu", "5"), (202503, "Ativo", "1")],
            CRUZA,
        )
        assert diag["grupos"]["Sumiu"]["status"] == "so_pre"
        assert diag["grupos"]["Sumiu"]["n_post"] == 0

    def test_grupo_so_depois_do_boundary(self) -> None:
        """O gap que a deteccao por tabela nao pegava: relatorio introduzido."""
        diag = _diagnose([(202503, CREDITO_NOVO, "1")], CRUZA)
        assert diag["grupos"][CREDITO_NOVO]["status"] == "so_post"
        assert diag["grupos"][CREDITO_NOVO]["n_pre"] == 0

    def test_grupos_independentes_na_mesma_query(self) -> None:
        diag = _diagnose(
            [
                (202412, "Ativo", "1"),
                (202503, "Ativo", "9"),
                (202412, "Credito", "7"),
                (202503, "Credito", "7"),
            ],
            CRUZA,
        )
        assert diag["grupos"]["Ativo"]["status"] == "renumerado"
        assert diag["grupos"]["Credito"]["status"] == "estavel"


# =========================================================================
# diagnose_eras -- motivo (as tabelas explicam, nao detectam)
# =========================================================================


class TestDiagnoseErasMotivo:
    def test_relatorio_descontinuado_ganha_motivo(self) -> None:
        diag = _diagnose([(202412, CREDITO_DROPPED, "1")], CRUZA)
        assert diag["grupos"][CREDITO_DROPPED]["motivo"] == "descontinuado"

    def test_credito_com_escopo_filtrado_ganha_motivo(self) -> None:
        diag = _diagnose(
            [(202412, "Carteira de credito ativa - por indexador", "1")],
            CRUZA,
            escopo="financeiro",
        )
        grupo = diag["grupos"]["Carteira de credito ativa - por indexador"]
        assert grupo["motivo"] == "migracao_escopo"

    def test_credito_sem_escopo_filtrado_nao_ganha_motivo(self) -> None:
        """Sem filtro de escopo os dois lados vem juntos -- nao ha migracao."""
        diag = _diagnose(
            [(202412, "Carteira de credito ativa - por indexador", "1")], CRUZA
        )
        grupo = diag["grupos"]["Carteira de credito ativa - por indexador"]
        assert grupo["motivo"] is None

    def test_relatorio_desconhecido_fica_sem_motivo(self) -> None:
        """Mudanca futura do BCB e detectada mesmo sem entrada na tabela."""
        diag = _diagnose([(202503, "Relatorio Que Ainda Nao Existe", "1")], CRUZA)
        grupo = diag["grupos"]["Relatorio Que Ainda Nao Existe"]
        assert grupo["status"] == "so_post"
        assert grupo["motivo"] is None


# =========================================================================
# emit_era_warnings
# =========================================================================


class TestEmitEraWarnings:
    def test_nao_cruzando_nao_emite(self) -> None:
        diag = _diagnose([(202503, "Ativo", "1")], [202503, 202506])
        assert _emit(diag) == []

    def test_estavel_nao_emite(self) -> None:
        diag = _diagnose([(202412, "Credito", "1"), (202503, "Credito", "1")], CRUZA)
        assert _emit(diag) == []

    def test_renumerado_emite_incompatible_era(self) -> None:
        diag = _diagnose([(202412, "Ativo", "1"), (202503, "Ativo", "9")], CRUZA)
        w = _emit(diag)
        assert len(w) == 1
        assert issubclass(w[0].category, IncompatibleEraWarning)

    def test_mensagem_cita_descontinuidade_e_overlap(self) -> None:
        pre = [(202412, "R", str(i)) for i in range(10)]
        post = [(202503, "R", str(i)) for i in range(2, 12)]
        msg = str(_emit(_diagnose(pre + post, CRUZA))[0].message)
        assert "80.0%" in msg
        assert "duas series" in msg

    def test_varios_renumerados_agregam_em_um_warning(self) -> None:
        diag = _diagnose(
            [
                (202412, "Ativo", "1"),
                (202503, "Ativo", "9"),
                (202412, "Passivo", "2"),
                (202503, "Passivo", "8"),
            ],
            CRUZA,
        )
        w = _emit(diag)
        assert len(w) == 1
        assert "Ativo" in str(w[0].message)
        assert "Passivo" in str(w[0].message)

    def test_dropped_emite_dropped_report_warning(self) -> None:
        diag = _diagnose([(202412, CREDITO_DROPPED, "1")], CRUZA)
        w = _emit(diag)
        assert len(w) == 1
        assert issubclass(w[0].category, DroppedReportWarning)
        assert w[0].message.last_period == 202412

    def test_migracao_de_escopo_emite_scope_migration(self) -> None:
        diag = _diagnose(
            [(202412, "Carteira de credito ativa - por indexador", "1")],
            CRUZA,
            escopo="financeiro",
        )
        w = _emit(diag)
        assert len(w) == 1
        assert issubclass(w[0].category, ScopeMigrationWarning)
        assert "prudencial" in str(w[0].message)

    def test_lacuna_sem_explicacao_emite_partial_data(self) -> None:
        diag = _diagnose([(202503, "Relatorio Novo", "1")], CRUZA)
        w = _emit(diag)
        assert len(w) == 1
        assert issubclass(w[0].category, PartialDataWarning)
        assert w[0].message.reason == "era_coverage_gap"

    def test_lacunas_sem_explicacao_agregam_em_um_warning(self) -> None:
        diag = _diagnose(
            [(202503, "Novo A", "1"), (202503, "Novo B", "2"), (202412, "Velho", "3")],
            CRUZA,
        )
        partial = [x for x in _emit(diag) if issubclass(x.category, PartialDataWarning)]
        assert len(partial) == 1
        assert partial[0].message.detail["so_post"] == ["Novo A", "Novo B"]
        assert partial[0].message.detail["so_pre"] == ["Velho"]

    def test_dropped_e_migracao_saem_juntos(self) -> None:
        diag = _diagnose([(202412, CREDITO_DROPPED, "1")], CRUZA, escopo="financeiro")
        cats = {x.category for x in _emit(diag)}
        assert DroppedReportWarning in cats
        assert IncompatibleEraWarning not in cats

    def test_source_aparece_na_mensagem(self) -> None:
        df = _make_df([(202412, "4010", "1"), (202501, "4010", "9")])
        diag = diagnose_eras(
            df.rename(columns={"relatorio": "documento"}),
            boundary=COSIF_ERA_BOUNDARY,
            source="COSIF",
            periodos_solicitados=[202412, 202501],
            group_col="documento",
        )
        msg = str(_emit(diag)[0].message)
        assert "COSIF" in msg
        assert "IFDATA" not in msg


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
