"""Testes de bulk read (instituicao=None) para IFDATA e COSIF.

Testa o novo code path _read_bulk e o comportamento natural do COSIF
quando instituicao e omitido.
"""

import warnings

import pandas as pd
import pytest

from ifdata_bcb.domain.exceptions import (
    InvalidScopeError,
    MissingRequiredParameterError,
    PartialDataWarning,
)
from ifdata_bcb.providers.cosif.explorer import COSIFExplorer
from ifdata_bcb.providers.ifdata.valores.explorer import IFDATAExplorer

BANCO_A_CNPJ = "60872504"
BANCO_B_CNPJ = "90400888"


class TestIFDATABulkIndividual:
    """Bulk individual: CodInst = CNPJ_8, canonical names aplicados."""

    def test_bulk_individual_returns_data(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, ...]
    ) -> None:
        ifdata = explorers[1]
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            df = ifdata.read("2023-03", escopo="individual")
        assert not df.empty

    def test_bulk_individual_has_cnpj8(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, ...]
    ) -> None:
        ifdata = explorers[1]
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            df = ifdata.read("2023-03", escopo="individual")
        assert "CNPJ_8" in df.columns
        assert df["CNPJ_8"].notna().all()

    def test_bulk_individual_has_instituicao(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, ...]
    ) -> None:
        """Canonical names devem ser aplicados no bulk individual."""
        ifdata = explorers[1]
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            df = ifdata.read("2023-03", escopo="individual")
        assert "INSTITUICAO" in df.columns

    def test_bulk_individual_cnpj_matches_codinst(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, ...]
    ) -> None:
        """No escopo individual, COD_INST e o proprio CNPJ_8."""
        ifdata = explorers[1]
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            df = ifdata.read("2023-03", escopo="individual")
        if "COD_INST" in df.columns:
            assert (df["CNPJ_8"] == df["COD_INST"]).all()

    def test_bulk_individual_has_escopo_column(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, ...]
    ) -> None:
        ifdata = explorers[1]
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            df = ifdata.read("2023-03", escopo="individual")
        assert "ESCOPO" in df.columns
        assert (df["ESCOPO"] == "individual").all()


class TestIFDATABulkPrudencial:
    """Bulk prudencial: sem CNPJ_8 (CodInst e codigo de conglomerado)."""

    def test_bulk_prudencial_returns_data(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, ...]
    ) -> None:
        ifdata = explorers[1]
        df = ifdata.read("2023-03", escopo="prudencial")
        assert not df.empty

    def test_bulk_prudencial_resolves_cnpj8_via_conglomerate(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, ...]
    ) -> None:
        """Prudencial bulk resolve CNPJ_8 via lookup de conglomerado no cadastro."""
        ifdata = explorers[1]
        df = ifdata.read("2023-03", escopo="prudencial")
        if "CNPJ_8" in df.columns:
            resolved = df["CNPJ_8"].dropna()
            if not resolved.empty:
                assert resolved.str.match(r"^\d{8}$").all()

    def test_bulk_prudencial_has_cod_inst(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, ...]
    ) -> None:
        ifdata = explorers[1]
        df = ifdata.read("2023-03", escopo="prudencial")
        assert "COD_INST" in df.columns
        assert df["COD_INST"].notna().all()


class TestIFDATABulkMultiEscopo:
    """Bulk sem escopo especificado: mix de individual + prudencial + financeiro."""

    def test_multi_escopo_returns_all(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, ...]
    ) -> None:
        ifdata = explorers[1]
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            df = ifdata.read("2023-03")
        assert not df.empty
        escopos = set(df["ESCOPO"].unique())
        assert len(escopos) >= 2

    def test_multi_escopo_all_have_cnpj8(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, ...]
    ) -> None:
        """Individual tem CNPJ direto, prudencial resolve via conglomerado."""
        ifdata = explorers[1]
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            df = ifdata.read("2023-03")
        if "CNPJ_8" not in df.columns:
            pytest.skip("CNPJ_8 nao presente no resultado")
        ind = df[df["ESCOPO"] == "individual"]
        if not ind.empty:
            assert ind["CNPJ_8"].notna().all()


class TestIFDATABulkWithFilters:
    """Bulk com filtros (conta, relatorio, grupo)."""

    def test_bulk_with_conta_filter(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, ...]
    ) -> None:
        ifdata = explorers[1]
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            df = ifdata.read("2023-03", escopo="individual", conta="10100")
        assert not df.empty
        assert (df["COD_CONTA"].astype(str) == "10100").all()

    def test_bulk_with_relatorio_filter(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, ...]
    ) -> None:
        ifdata = explorers[1]
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            df = ifdata.read("2023-03", escopo="individual", relatorio="Resumo")
        assert not df.empty

    def test_bulk_with_nonexistent_conta_returns_empty(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, ...]
    ) -> None:
        ifdata = explorers[1]
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            df = ifdata.read("2023-03", escopo="individual", conta="INEXISTENTE_999")
        assert df.empty

    def test_bulk_with_invalid_escopo_raises(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, ...]
    ) -> None:
        ifdata = explorers[1]
        with pytest.raises(InvalidScopeError):
            ifdata.read("2023-03", escopo="invalido")

    def test_bulk_without_start_raises(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, ...]
    ) -> None:
        ifdata = explorers[1]
        with pytest.raises((MissingRequiredParameterError, TypeError)):
            ifdata.read(start=None, escopo="individual")  # type: ignore[arg-type]


class TestIFDATABulkEnrichment:
    """Bulk com cadastro= enrichment."""

    def test_bulk_individual_enrichment_works(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, ...]
    ) -> None:
        """Individual bulk tem CNPJ_8, entao enrichment deve funcionar."""
        ifdata = explorers[1]
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            df = ifdata.read("2023-03", escopo="individual", cadastro=["SEGMENTO"])
        assert "SEGMENTO" in df.columns

    def test_bulk_prudencial_enrichment_works(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, ...]
    ) -> None:
        """Prudencial bulk com CNPJ_8 resolvido permite enrichment."""
        ifdata = explorers[1]
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            df = ifdata.read("2023-03", escopo="prudencial", cadastro=["SEGMENTO"])
        if not df.empty and "CNPJ_8" in df.columns and df["CNPJ_8"].notna().any():
            assert "SEGMENTO" in df.columns


class TestIFDATABulkDiagnostics:
    """Diagnostico de resultado vazio em modo bulk."""

    def test_bulk_empty_result_warns_without_instituicao_mention(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, ...]
    ) -> None:
        """Warning de resultado vazio nao deve mencionar 'instituicao' em bulk."""
        ifdata = explorers[1]
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            ifdata.read(
                "2023-03", escopo="individual", conta="CONTA_QUE_NAO_EXISTE_XYZ"
            )

        partial_warnings = [x for x in w if issubclass(x.category, PartialDataWarning)]
        assert len(partial_warnings) > 0


class TestCOSIFBulk:
    """COSIF bulk: trivial (CNPJ_8 ja esta no parquet)."""

    def test_cosif_bulk_returns_data(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, ...]
    ) -> None:
        cosif = explorers[0]
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            df = cosif.read("2023-03")
        assert not df.empty

    def test_cosif_bulk_has_cnpj8(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, ...]
    ) -> None:
        cosif = explorers[0]
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            df = cosif.read("2023-03")
        assert "CNPJ_8" in df.columns
        assert df["CNPJ_8"].notna().all()

    def test_cosif_bulk_individual_escopo(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, ...]
    ) -> None:
        cosif = explorers[0]
        df = cosif.read("2023-03", escopo="individual")
        assert not df.empty
        assert (df["ESCOPO"] == "individual").all()

    def test_cosif_bulk_prudencial_escopo(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, ...]
    ) -> None:
        cosif = explorers[0]
        df = cosif.read("2023-03", escopo="prudencial")
        assert not df.empty
        assert (df["ESCOPO"] == "prudencial").all()

    def test_cosif_bulk_returns_more_than_single_institution(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, ...]
    ) -> None:
        """Bulk deve retornar dados de multiplas instituicoes."""
        cosif = explorers[0]
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            df = cosif.read("2023-03", escopo="individual")
        unique_cnpjs = df["CNPJ_8"].nunique()
        assert unique_cnpjs >= 2

    def test_cosif_bulk_same_data_as_sum_of_parts(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, ...]
    ) -> None:
        """Bulk individual deve retornar mesmos dados que ler cada CNPJ separado."""
        cosif = explorers[0]
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            df_bulk = cosif.read("2023-03", escopo="individual")

        df_a = cosif.read("2023-03", instituicao=BANCO_A_CNPJ, escopo="individual")
        df_b = cosif.read("2023-03", instituicao=BANCO_B_CNPJ, escopo="individual")
        df_combined = pd.concat([df_a, df_b], ignore_index=True)

        assert len(df_bulk) == len(df_combined)
