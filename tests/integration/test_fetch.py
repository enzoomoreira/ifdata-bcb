"""fetch() stateless (item 4.3): baixa, devolve no formato de read() e nao persiste.

O download real e coberto pelos testes de contrato dos collectors; aqui o
collector e substituido por um fake que escreve parquet no DataManager
injetado -- o que se testa e o wiring: cache temporario, delegacao a read()
e a garantia de que o cache local do explorer fica intocado.
"""

from pathlib import Path

import pandas as pd
import pytest

import ifdata_bcb.providers.cosif.explorer as cosif_mod
import ifdata_bcb.providers.ifdata.cadastro.explorer as cad_mod
import ifdata_bcb.providers.ifdata.valores.explorer as val_mod
from ifdata_bcb.core.entity import EntityLookup
from ifdata_bcb.infra.query import QueryEngine
from ifdata_bcb.infra.storage import DataManager
from ifdata_bcb.providers.cosif.explorer import COSIFExplorer
from ifdata_bcb.providers.ifdata.cadastro.explorer import CadastroExplorer
from ifdata_bcb.providers.ifdata.valores.explorer import IFDATAExplorer

_CNPJ = "60872504"


def _cosif_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "DATA_BASE": pd.array([202303] * 2, dtype="Int64"),
            "CNPJ_8": [_CNPJ] * 2,
            "NOME_INSTITUICAO": ["BANCO ALFA S.A."] * 2,
            "DOCUMENTO": ["D1", "D2"],
            "CONTA": ["10100", "20200"],
            "NOME_CONTA": ["ATIVO TOTAL", "PASSIVO TOTAL"],
            "SALDO": [1000.0, 800.0],
        }
    )


class FakeCOSIFCollector:
    _PREFIX = {"individual": "cosif_ind", "prudencial": "cosif_prud"}

    def __init__(self, escopo: str, data_manager: DataManager):
        self._escopo = escopo
        self._dm = data_manager

    def collect(self, start, end=None, **kw) -> None:
        self._dm.save(
            _cosif_df(), f"{self._PREFIX[self._escopo]}_202303", f"cosif/{self._escopo}"
        )


class FakeValoresCollector:
    def __init__(self, data_manager: DataManager):
        self._dm = data_manager

    def collect(self, start, end=None, **kw) -> None:
        df = pd.DataFrame(
            {
                "AnoMes": pd.array([202303] * 2, dtype="Int64"),
                "CodInst": [_CNPJ] * 2,
                "TipoInstituicao": pd.array([3, 3], dtype="Int64"),
                "Conta": ["10100", "20200"],
                "NomeColuna": ["ATIVO TOTAL", "PASSIVO TOTAL"],
                "Saldo": [1000.0, 800.0],
                "NomeRelatorio": ["Resumo"] * 2,
                "Grupo": ["Balanco"] * 2,
            }
        )
        self._dm.save(df, "ifdata_val_202303", "ifdata/valores")


class FakeCadastroCollector:
    def __init__(self, data_manager: DataManager):
        self._dm = data_manager

    def collect(self, start, end=None, **kw) -> None:
        df = pd.DataFrame(
            {
                "Data": pd.array([202303], dtype="Int64"),
                "CodInst": [_CNPJ],
                "CNPJ_8": [_CNPJ],
                "NomeInstituicao": ["BANCO ALFA S.A."],
                "SegmentoTb": ["S1"],
                "CodConglomeradoPrudencial": [None],
                "CodConglomeradoFinanceiro": [None],
                "CNPJ_LIDER_8": [None],
                "Situacao": ["A"],
                "Atividade": ["001"],
                "Tcb": ["0001"],
                "Td": ["01"],
                "Tc": ["1"],
                "Uf": ["SP"],
                "Municipio": ["Sao Paulo"],
                "Sr": ["01"],
                "DataInicioAtividade": ["19900101"],
            }
        )
        self._dm.save(df, "ifdata_cad_202303", "ifdata/cadastro")


@pytest.fixture
def cache_vazio(workspace_tmp_dir: Path) -> Path:
    return workspace_tmp_dir


def _sem_parquet(base: Path) -> bool:
    return not list(base.rglob("*.parquet"))


class TestFetchCOSIF:
    def test_formato_de_read_e_cache_intocado(
        self, cache_vazio: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(cosif_mod, "COSIFCollector", FakeCOSIFCollector)
        qe = QueryEngine(base_path=cache_vazio)
        cosif = COSIFExplorer(query_engine=qe, entity_lookup=EntityLookup(qe))

        df = cosif.fetch("2023-03", instituicao=_CNPJ, verbose=False)

        assert not df.empty
        assert df.index.name == "date"
        assert isinstance(df.index, pd.DatetimeIndex)
        assert set(df["cnpj_8"]) == {_CNPJ}
        assert set(df["escopo"]) == {"individual", "prudencial"}
        assert _sem_parquet(cache_vazio)  # nada persistiu no cache real

    def test_filtros_passam_para_read(
        self, cache_vazio: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(cosif_mod, "COSIFCollector", FakeCOSIFCollector)
        qe = QueryEngine(base_path=cache_vazio)
        cosif = COSIFExplorer(query_engine=qe, entity_lookup=EntityLookup(qe))

        df = cosif.fetch(
            "2023-03",
            escopo="individual",
            conta="ATIVO TOTAL",
            columns=["cnpj_8", "valor"],
            verbose=False,
        )
        assert list(df.columns) == ["cnpj_8", "valor"]
        assert len(df) == 1


class TestFetchIFDATA:
    def test_bulk_individual(
        self, cache_vazio: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(val_mod, "IFDATAValoresCollector", FakeValoresCollector)
        qe = QueryEngine(base_path=cache_vazio)
        ifdata = IFDATAExplorer(query_engine=qe, entity_lookup=EntityLookup(qe))

        df = ifdata.fetch("2023-03", escopo="individual", verbose=False)

        assert not df.empty
        assert df.index.name == "date"
        assert set(df["cnpj_8"]) == {_CNPJ}
        assert set(df["escopo"]) == {"individual"}
        assert _sem_parquet(cache_vazio)


class TestFetchCadastro:
    def test_formato_e_filtro(
        self, cache_vazio: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(cad_mod, "IFDATACadastroCollector", FakeCadastroCollector)
        qe = QueryEngine(base_path=cache_vazio)
        cadastro = CadastroExplorer(query_engine=qe, entity_lookup=EntityLookup(qe))

        df = cadastro.fetch("2023-03", uf="SP", verbose=False)

        assert not df.empty
        assert df.index.name == "date"
        assert set(df["cnpj_8"]) == {_CNPJ}
        assert set(df["uf"]) == {"SP"}
        assert _sem_parquet(cache_vazio)
