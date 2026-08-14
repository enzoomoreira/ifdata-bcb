"""QA: escritas concorrentes na mesma cache -- cenario dos itens 1.3 e 2.11.

test_concurrency.py cobre leituras simultaneas; aqui e o outro lado: varios
collect() em paralelo (threads) e dois processos gravando o mesmo periodo na
mesma cache. O contrato sob teste e o da escrita atomica: nunca existe um
.parquet truncado com nome definitivo, e nenhum .tmp orfao sobra.
"""

import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd

from ifdata_bcb.infra.storage import DataManager, get_parquet_metadata

_SUBDIR = "escrita_concorrente"


def _df(valor: int, rows: int = 50) -> pd.DataFrame:
    return pd.DataFrame({"DATA": [202401] * rows, "VALOR": [valor] * rows})


def _tmp_orfaos(base: Path) -> list[Path]:
    return list((base / _SUBDIR).glob("*.tmp"))


class TestEscritasEmThreads:
    def test_escritas_paralelas_em_arquivos_distintos(self, tmp_path: Path) -> None:
        def save(i: int) -> Path:
            # Um DataManager por thread, espelhando collect(): cada collector
            # possui o seu.
            return DataManager(base_path=tmp_path).save(_df(i), f"periodo_{i}", _SUBDIR)

        with ThreadPoolExecutor(max_workers=8) as ex:
            futures = [ex.submit(save, i) for i in range(16)]
            paths = [f.result() for f in as_completed(futures)]

        assert len(paths) == 16
        for i in range(16):
            df = pd.read_parquet(tmp_path / _SUBDIR / f"periodo_{i}.parquet")
            assert df["VALOR"].eq(i).all()
        assert _tmp_orfaos(tmp_path) == []

    def test_escritas_paralelas_no_mesmo_arquivo(self, tmp_path: Path) -> None:
        """Dois collect() do mesmo periodo: vence o ultimo, nunca um hibrido."""

        def save(i: int) -> Path:
            return DataManager(base_path=tmp_path).save(
                _df(i), "mesmo_periodo", _SUBDIR
            )

        with ThreadPoolExecutor(max_workers=8) as ex:
            futures = [ex.submit(save, i) for i in range(16)]
            for f in as_completed(futures):
                f.result()

        df = pd.read_parquet(tmp_path / _SUBDIR / "mesmo_periodo.parquet")
        assert len(df) == 50
        assert df["VALOR"].nunique() == 1  # conteudo integro de UM escritor
        assert _tmp_orfaos(tmp_path) == []

    def test_metadata_durante_escritas(self, tmp_path: Path) -> None:
        """get_parquet_metadata compartilha uma conexao DuckDB global (2.11)."""
        DataManager(base_path=tmp_path).save(_df(0), "periodo_fixo", _SUBDIR)

        def write(i: int) -> str:
            DataManager(base_path=tmp_path).save(_df(i), f"periodo_{i}", _SUBDIR)
            return "write"

        def read_meta() -> str:
            meta = get_parquet_metadata("periodo_fixo", _SUBDIR, base_path=tmp_path)
            assert meta is not None
            assert meta["status"] == "OK"
            assert meta["registros"] == 50
            return "meta"

        with ThreadPoolExecutor(max_workers=8) as ex:
            futures = []
            for i in range(10):
                futures.append(ex.submit(write, i))
                futures.append(ex.submit(read_meta))
            results = [f.result() for f in as_completed(futures)]

        assert results.count("write") == 10
        assert results.count("meta") == 10


_SCRIPT_DOIS_PROCESSOS = """
import sys
from pathlib import Path
import pandas as pd
from ifdata_bcb.infra.storage import DataManager

base, valor = Path(sys.argv[1]), int(sys.argv[2])
dm = DataManager(base_path=base)
df = pd.DataFrame({"DATA": [202401] * 50, "VALOR": [valor] * 50})
for _ in range(20):
    dm.save(df, "mesmo_periodo", "escrita_concorrente")
"""


class TestEscritasEmProcessos:
    def test_dois_processos_gravando_o_mesmo_periodo(self, tmp_path: Path) -> None:
        procs = [
            subprocess.Popen(
                [sys.executable, "-c", _SCRIPT_DOIS_PROCESSOS, str(tmp_path), str(v)],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            for v in (1, 2)
        ]
        for p in procs:
            _, stderr = p.communicate(timeout=60)
            assert p.returncode == 0, stderr.decode(errors="replace")

        df = pd.read_parquet(tmp_path / _SUBDIR / "mesmo_periodo.parquet")
        assert len(df) == 50
        assert df["VALOR"].nunique() == 1
        assert _tmp_orfaos(tmp_path) == []
