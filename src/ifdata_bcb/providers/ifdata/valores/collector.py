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
from ifdata_bcb.providers.parsing import (
    count_parseable_rows,
    warn_if_rows_dropped,
    warn_if_values_nulled,
)


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
        """
        Baixa os 3 tipos de instituicao em paralelo.

        Exige que todos tenham sucesso. Gravar o periodo com apenas parte dos
        tipos o marcaria como coletado (a deteccao e por nome de arquivo), e o
        tipo faltante nunca seria rebaixado sem force=True.
        """
        tipos_inst = list(TIPO_INST_MAP.values())

        def download_tipo(tipo: int) -> Path:
            url = (
                f"{IFDATA_API_BASE}/IfDataValores"
                f"(AnoMes=@AnoMes,TipoInstituicao=@TipoInstituicao,Relatorio=@Relatorio)"
                f"?@AnoMes={period}&@TipoInstituicao={tipo}&@Relatorio='T'&$format=text/csv"
            )
            output_path = work_dir / f"ifdata_val_{period}_{tipo}.csv"
            self._download_single(url, output_path, period)
            return output_path

        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = {executor.submit(download_tipo, t): t for t in tipos_inst}
            for future in as_completed(futures):
                tipo = futures[future]
                try:
                    future.result()
                except Exception as e:
                    self.logger.warning(
                        f"Periodo {period}: falha no tipo de instituicao {tipo}: {e}"
                    )
                    raise

        return work_dir

    def _process_to_parquet(self, data_path: Path, period: int) -> pd.DataFrame | None:
        """Processa CSVs do diretorio em um unico DataFrame."""
        try:
            csv_files = list(data_path.glob("*.csv"))
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
                        CAST(Saldo AS VARCHAR) as _saldo_raw,
                        NomeRelatorio, Grupo
                    FROM read_csv('{csv_path}', delim=',', header=true,
                        ignore_errors=true)
                """

                df = cursor.sql(query).df()
                if df.empty:
                    continue

                source = f"ifdata_valores {csv_path.stem}"
                warn_if_rows_dropped(
                    source,
                    len(df),
                    count_parseable_rows(cursor, csv_path, delim=","),
                )
                warn_if_values_nulled(source, "Saldo", df["_saldo_raw"], df["Saldo"])
                dfs.append(df.drop(columns=["_saldo_raw"]))

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
            self.logger.error(f"Erro processando {data_path}: {e}")
            raise DataProcessingError("ifdata_valores", str(e)) from e
