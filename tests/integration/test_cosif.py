"""Testes de integracao -- COSIF read() e list methods."""

from pathlib import Path

import pandas as pd
import pytest

from ifdata_bcb.core.entity import EntityLookup
from ifdata_bcb.domain.exceptions import InvalidScopeError
from ifdata_bcb.infra.query import QueryEngine
from ifdata_bcb.providers.cosif.explorer import COSIFExplorer
from ifdata_bcb.providers.ifdata.cadastro.explorer import CadastroExplorer
from ifdata_bcb.providers.ifdata.valores.explorer import IFDATAExplorer
from tests.conftest import BANCO_A_CNPJ, _save_parquet


class TestCOSIFRead:
    def test_read_returns_dataframe_with_correct_columns(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        cosif = explorers[0]
        df = cosif.read(instituicao=BANCO_A_CNPJ, start="2023-03")

        assert not df.empty
        for col in ("cnpj_8", "instituicao", "escopo", "conta", "valor"):
            assert col in df.columns

    def test_read_devolve_datetimeindex_date(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        df = explorers[0].read(instituicao=BANCO_A_CNPJ, start="2023-03")
        assert isinstance(df.index, pd.DatetimeIndex)
        assert df.index.name == "date"
        assert "data" not in df.columns

    def test_read_filters_by_institution(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        df = explorers[0].read(instituicao=BANCO_A_CNPJ, start="2023-03")
        assert all(df["cnpj_8"] == BANCO_A_CNPJ)

    def test_read_both_scopes_returns_escopo_column(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        df = explorers[0].read(instituicao=BANCO_A_CNPJ, start="2023-03")
        assert "escopo" in df.columns
        assert "individual" in df["escopo"].unique()

    def test_read_single_escopo(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        df = explorers[0].read(
            instituicao=BANCO_A_CNPJ, start="2023-03", escopo="individual"
        )
        assert not df.empty
        assert all(df["escopo"] == "individual")

    def test_read_filters_by_account(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        df = explorers[0].read(
            instituicao=BANCO_A_CNPJ, start="2023-03", conta="ATIVO TOTAL"
        )
        assert not df.empty
        assert all(df["conta"] == "ATIVO TOTAL")

    def test_read_no_data_returns_empty_with_columns(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        df = explorers[0].read(instituicao="99999999", start="2023-03")
        assert df.empty
        assert isinstance(df.index, pd.DatetimeIndex)
        assert df.index.name == "date"
        assert "cnpj_8" in df.columns

    def test_read_applies_canonical_names(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        df = explorers[0].read(instituicao=BANCO_A_CNPJ, start="2023-03")
        if not df.empty:
            assert "BANCO ALFA S.A." in df["instituicao"].unique()

    def test_read_with_cadastro_enrichment(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        df = explorers[0].read(
            instituicao=BANCO_A_CNPJ, start="2023-03", cadastro=["segmento", "uf"]
        )
        assert "segmento" in df.columns
        assert "uf" in df.columns

    def test_read_includes_cod_conta_and_documento(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        df = explorers[0].read(instituicao=BANCO_A_CNPJ, start="2023-03")

        assert "cod_conta" in df.columns
        assert "documento" in df.columns
        assert "10100" in df["cod_conta"].astype(str).values
        assert "20200" in df["cod_conta"].astype(str).values
        assert df["documento"].notna().all()

    def test_read_filters_by_account_code(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        df = explorers[0].read(instituicao=BANCO_A_CNPJ, start="2023-03", conta="10100")
        assert not df.empty
        assert all(df["cod_conta"].astype(str) == "10100")


class TestCOSIFListMethods:
    def test_list_periodos(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        assert 202303 in explorers[0].list_periodos()

    def test_has_data(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        assert explorers[0].has_data() is True

    def test_describe(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        info = explorers[0].describe()
        assert info["has_data"] is True
        assert info["period_count"] >= 1
        assert set(info["by_escopo"]) == {"individual", "prudencial"}
        assert info["by_escopo"]["individual"]["has_data"] is True

    def test_list_contas(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        df = explorers[0].list_contas()
        assert not df.empty
        assert "cod_conta" in df.columns
        assert "escopos" in df.columns

    def test_list_contas_with_filter(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        df = explorers[0].list_contas(termo="ATIVO")
        assert not df.empty
        assert all("ATIVO" in c.upper() for c in df["conta"])

    def test_list_contas_limit_applies_to_total(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        df = explorers[0].list_contas(limit=2)
        assert len(df) <= 2

    def test_list_contas_with_escopo_no_escopos_column(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        df = explorers[0].list_contas(escopo="individual")
        assert not df.empty
        assert "escopos" not in df.columns
        assert list(df.columns) == ["cod_conta", "conta"]


class TestCOSIFDocumentoValidation:
    """Validacao do parametro documento em cosif.read()."""

    def test_documento_non_numeric_raises(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        """documento com valor nao-numerico deve levantar InvalidScopeError."""
        with pytest.raises(InvalidScopeError) as exc_info:
            explorers[0].read(
                "2023-03", instituicao=BANCO_A_CNPJ, documento="balancete"
            )
        assert exc_info.value.scope == "documento"
        # Regressao: valid_values recebia str e a mensagem saia caractere a
        # caractere ("Validos: 'v', 'a', 'l', 'o', 'r', ...").
        msg = str(exc_info.value)
        assert "'v', 'a', 'l'" not in msg
        assert "Validos" not in msg
        assert "list_values(['documento'])" in msg

    def test_documento_numeric_string_accepted(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        """documento com string numerica deve ser aceito sem erro."""
        df = explorers[0].read("2023-03", instituicao=BANCO_A_CNPJ, documento="4010")
        assert isinstance(df, pd.DataFrame)

    def test_documento_list_with_non_numeric_raises(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        """Lista com elemento nao-numerico deve levantar InvalidScopeError."""
        with pytest.raises(InvalidScopeError) as exc_info:
            explorers[0].read(
                "2023-03", instituicao=BANCO_A_CNPJ, documento=["4010", "abc"]
            )
        assert exc_info.value.scope == "documento"
        # Aponta o elemento culpado, nao a lista inteira
        assert exc_info.value.value == "abc"

    def test_documento_filter_works_with_non_numeric_values_in_column(
        self, workspace_tmp_dir: Path
    ) -> None:
        """
        Coluna DOCUMENTO mista nao pode derrubar o filtro.

        DOCUMENTO e VARCHAR: comparar com INT fazia o DuckDB converter a coluna
        inteira e falhar por causa de um unico valor nao-numerico, descartando
        tambem as linhas que casariam.
        """
        _save_parquet(
            pd.DataFrame(
                {
                    "DATA_BASE": pd.array([202303] * 3, dtype="Int64"),
                    "CNPJ_8": [BANCO_A_CNPJ] * 3,
                    "NOME_INSTITUICAO": ["BANCO ALFA S.A."] * 3,
                    "DOCUMENTO": ["4010", "BALANCETE", "4016"],
                    "CONTA": ["10100", "20200", "30300"],
                    "NOME_CONTA": ["ATIVO", "PASSIVO", "PL"],
                    "SALDO": [100.0, 200.0, 300.0],
                }
            ),
            workspace_tmp_dir,
            "cosif/individual",
            "cosif_ind_202303",
        )
        qe = QueryEngine(base_path=workspace_tmp_dir)
        cosif = COSIFExplorer(
            query_engine=qe, entity_lookup=EntityLookup(query_engine=qe)
        )

        df = cosif.read("2023-03", instituicao=BANCO_A_CNPJ, documento="4010")

        assert len(df) == 1
        assert df.iloc[0]["documento"] == "4010"
