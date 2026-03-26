from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd

from ifdata_bcb.core.constants import (
    DATA_SOURCES,
    IFDATA_API_BASE,
    TIPO_INST_MAP,
    get_subdir,
)
from ifdata_bcb.domain.exceptions import DataProcessingError
from ifdata_bcb.infra.storage import DataManager
from ifdata_bcb.providers.base_collector import BaseCollector


class IFDATAValoresCollector(BaseCollector):
    """Collector para IFDATA Valores (trimestral). Baixa 3 tipos de instituicao."""

    _PERIOD_TYPE = "quarterly"

    def __init__(self, data_manager: DataManager | None = None):
        super().__init__(data_manager)

    def _get_file_prefix(self) -> str:
        return DATA_SOURCES["ifdata_valores"]["prefix"]

    def _get_subdir(self) -> str:
        return get_subdir("ifdata_valores")

    def _download_period(self, period: int, work_dir: Path) -> Path | None:
        """Baixa 3 tipos de instituicao em paralelo."""
        tipos_inst = list(TIPO_INST_MAP.values())
        downloaded = []

        def download_tipo(tipo: int) -> Path | None:
            url = (
                f"{IFDATA_API_BASE}/IfDataValores"
                f"(AnoMes=@AnoMes,TipoInstituicao=@TipoInstituicao,Relatorio=@Relatorio)"
                f"?@AnoMes={period}&@TipoInstituicao={tipo}&@Relatorio='T'&$format=text/csv"
            )
            output_path = work_dir / f"ifdata_val_{period}_{tipo}.csv"
            try:
                self._download_single(url, output_path)
                return output_path
            except Exception:
                return None

        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = [executor.submit(download_tipo, t) for t in tipos_inst]
            for future in as_completed(futures):
                path = future.result()
                if path:
                    downloaded.append(path)

        return work_dir if downloaded else None

    def _process_to_parquet(self, csv_dir: Path, period: int) -> pd.DataFrame | None:
        """Processa CSVs do diretorio em um unico DataFrame."""
        try:
            csv_files = list(csv_dir.glob("*.csv"))
            if not csv_files:
                return None

            cursor = self._get_cursor()
            dfs = []

            for csv_path in csv_files:
                if csv_path.stat().st_size <= 100:
                    continue

                query = f"""
                    SELECT
                        AnoMes, CodInst, TipoInstituicao, Conta, NomeColuna,
                        TRY_CAST(REPLACE(CAST(Saldo AS VARCHAR), ',', '.')
                            AS DOUBLE) as Saldo,
                        NomeRelatorio, Grupo
                    FROM read_csv('{csv_path}', delim=',', header=true,
                        ignore_errors=true)
                """

                df = cursor.sql(query).df()
                if not df.empty:
                    dfs.append(df)

            if not dfs:
                return None

            df = pd.concat(dfs, ignore_index=True)
            df = df.replace("null", None)
            df["AnoMes"] = pd.to_numeric(df["AnoMes"], errors="coerce").astype("Int64")

            cols = [
                "AnoMes",
                "CodInst",
                "TipoInstituicao",
                "Conta",
                "NomeColuna",
                "Saldo",
                "NomeRelatorio",
                "Grupo",
            ]
            return df[[c for c in cols if c in df.columns]]

        except Exception as e:
            self.logger.error(f"Erro processando {csv_dir}: {e}")
            raise DataProcessingError("ifdata_valores", str(e)) from e
