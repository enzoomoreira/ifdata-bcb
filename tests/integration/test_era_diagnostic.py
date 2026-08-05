"""Integracao: diagnostico de era end-to-end via read() e check_era().

Usa um cache sintetico que cruza o boundary de 202503 -- as fixtures gerais
so tem 202303, dentro de uma unica era.
"""

import warnings
from pathlib import Path

import pandas as pd
import pytest

from ifdata_bcb.core.entity import EntityLookup
from ifdata_bcb.domain.exceptions import DroppedReportWarning, IncompatibleEraWarning
from ifdata_bcb.infra.query import QueryEngine
from ifdata_bcb.providers.ifdata.cadastro.explorer import CadastroExplorer
from ifdata_bcb.providers.ifdata.valores.explorer import IFDATAExplorer
from tests.conftest import BANCO_A_CNPJ

# Resumo renumera as contas entre eras; Credito mantem as mesmas.
_LINHAS = [
    (202412, "10100", "Resumo"),
    (202412, "20200", "Resumo"),
    (202503, "90100", "Resumo"),
    (202503, "90200", "Resumo"),
    (202412, "C1", "Carteira de credito ativa - por indexador"),
    (202503, "C1", "Carteira de credito ativa - por indexador"),
]


def _valores_cross_era() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "AnoMes": pd.array([p for p, _, _ in _LINHAS], dtype="Int64"),
            "CodInst": [BANCO_A_CNPJ] * len(_LINHAS),
            "TipoInstituicao": pd.array([3] * len(_LINHAS), dtype="Int64"),
            "Conta": [c for _, c, _ in _LINHAS],
            "NomeColuna": [f"CONTA {c}" for _, c, _ in _LINHAS],
            "Saldo": [100.0 * i for i in range(len(_LINHAS))],
            "NomeRelatorio": [r for _, _, r in _LINHAS],
            "Grupo": ["Balanco"] * len(_LINHAS),
        }
    )


@pytest.fixture
def ifdata_cross_era(tmp_cache_dir: Path) -> IFDATAExplorer:
    df = _valores_cross_era()
    target = tmp_cache_dir / "ifdata" / "valores"
    target.mkdir(parents=True, exist_ok=True)
    for periodo in (202412, 202503):
        df[df["AnoMes"] == periodo].to_parquet(
            target / f"ifdata_val_{periodo}.parquet", engine="pyarrow", index=False
        )
    qe = QueryEngine(base_path=tmp_cache_dir)
    return IFDATAExplorer(query_engine=qe, entity_lookup=EntityLookup(query_engine=qe))


class TestAttrsNoRead:
    def test_read_grava_diagnostico_em_attrs(
        self, ifdata_cross_era: IFDATAExplorer
    ) -> None:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            df = ifdata_cross_era.read("2024-12", "2025-03", escopo="individual")

        diag = df.attrs.get("era")
        assert diag is not None
        assert diag["cruza_boundary"] is True
        assert diag["grupos"]["Resumo"]["status"] == "renumerado"

    def test_attrs_presente_mesmo_sem_cruzar_boundary(
        self, ifdata_cross_era: IFDATAExplorer
    ) -> None:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            df = ifdata_cross_era.read("2024-12", escopo="individual")

        assert df.attrs["era"]["cruza_boundary"] is False

    def test_attrs_sobrevive_ao_filtro_de_colunas(
        self, ifdata_cross_era: IFDATAExplorer
    ) -> None:
        """columns= remove COD_CONTA do output, mas a analise ja rodou antes."""
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            df = ifdata_cross_era.read(
                "2024-12", "2025-03", escopo="individual", columns=["DATA", "VALOR"]
            )

        assert list(df.columns) == ["DATA", "VALOR"]
        assert df.attrs["era"]["grupos"]["Resumo"]["status"] == "renumerado"

    def test_read_cruzando_boundary_emite_warning(
        self, ifdata_cross_era: IFDATAExplorer
    ) -> None:
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            ifdata_cross_era.read("2024-12", "2025-03", escopo="individual")

        assert any(issubclass(x.category, IncompatibleEraWarning) for x in w)


class TestRelatorioDescontinuado:
    def test_periodo_posterior_explica_o_vazio(
        self, ifdata_cross_era: IFDATAExplorer
    ) -> None:
        """Resultado vazio nao tem dado para medir, mas o nome do relatorio basta."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            df = ifdata_cross_era.read(
                "2025-03",
                relatorio="Carteira de credito ativa - por nivel de risco da operacao",
            )

        assert df.empty
        dropped = [x for x in w if issubclass(x.category, DroppedReportWarning)]
        assert len(dropped) == 1
        assert dropped[0].message.last_period == 202412

    def test_periodo_anterior_nao_avisa(self, ifdata_cross_era: IFDATAExplorer) -> None:
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            ifdata_cross_era.read(
                "2024-12",
                relatorio="Carteira de credito ativa - por nivel de risco da operacao",
            )

        assert not [x for x in w if issubclass(x.category, DroppedReportWarning)]


class TestCheckEra:
    def test_retorna_diagnostico_estruturado(
        self, ifdata_cross_era: IFDATAExplorer
    ) -> None:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            diag = ifdata_cross_era.check_era("2024-12", "2025-03")

        assert diag["source"] == "IFDATA"
        assert diag["boundary"] == 202503
        assert diag["periodos_presentes"] == [202412, 202503]
        assert diag["grupos"]["Resumo"]["status"] == "renumerado"
        assert (
            diag["grupos"]["Carteira de credito ativa - por indexador"]["status"]
            == "estavel"
        )

    def test_diagnostico_e_serializavel(self, ifdata_cross_era: IFDATAExplorer) -> None:
        """Consumo agentico: o diagnostico precisa virar JSON sem tratamento."""
        import json

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            diag = ifdata_cross_era.check_era("2024-12", "2025-03")

        assert json.loads(json.dumps(diag)) == diag

    def test_periodo_sem_dados_retorna_estrutura_vazia(
        self, ifdata_cross_era: IFDATAExplorer
    ) -> None:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            diag = ifdata_cross_era.check_era("2020-03", "2020-06")

        assert diag["grupos"] == {}
        assert diag["cruza_boundary"] is False

    def test_explorer_sem_transicao_de_era_levanta(self, tmp_cache_dir: Path) -> None:
        qe = QueryEngine(base_path=tmp_cache_dir)
        cadastro = CadastroExplorer(
            query_engine=qe, entity_lookup=EntityLookup(query_engine=qe)
        )
        with pytest.raises(NotImplementedError, match="transicao de era"):
            cadastro.check_era("2024-12")
