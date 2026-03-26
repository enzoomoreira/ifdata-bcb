from pathlib import Path

import pandas as pd

from ifdata_bcb.core.constants import DATA_SOURCES, IFDATA_API_BASE, get_subdir
from ifdata_bcb.domain.exceptions import DataProcessingError
from ifdata_bcb.infra.storage import DataManager
from ifdata_bcb.providers.base_collector import BaseCollector
from ifdata_bcb.utils.cnpj import standardize_cnpj_base8
from ifdata_bcb.utils.nulls import is_valid


class IFDATACadastroCollector(BaseCollector):
    """Collector para dados IFDATA Cadastro (trimestral)."""

    _PERIOD_TYPE = "quarterly"

    def __init__(self, data_manager: DataManager | None = None):
        super().__init__(data_manager)

    def _get_file_prefix(self) -> str:
        return DATA_SOURCES["cadastro"]["prefix"]

    def _get_subdir(self) -> str:
        return get_subdir("cadastro")

    def _download_period(self, period: int, work_dir: Path) -> Path | None:
        url = (
            f"{IFDATA_API_BASE}/IfDataCadastro(AnoMes=@AnoMes)"
            f"?@AnoMes={period}&$format=text/csv"
        )

        output_path = work_dir / f"ifdata_cad_{period}.csv"

        try:
            self._download_single(url, output_path)
            if output_path.stat().st_size > 100:
                return output_path
        except Exception as e:
            self.logger.debug(f"Periodo {period} falhou: {e}")

        return None

    def _process_to_parquet(self, csv_path: Path, period: int) -> pd.DataFrame | None:
        """Processa CSV de cadastro."""
        try:
            query = f"""
                SELECT
                    Data,
                    CodInst,
                    NomeInstituicao,
                    SegmentoTb,
                    CodConglomeradoPrudencial,
                    CodConglomeradoFinanceiro,
                    CnpjInstituicaoLider,
                    Situacao,
                    Atividade,
                    Tcb,
                    Td,
                    Tc,
                    Uf,
                    Municipio,
                    Sr,
                    DataInicioAtividade
                FROM read_csv(
                    '{csv_path}',
                    delim=',',
                    header=true,
                    ignore_errors=true,
                    types={{'CodInst': 'VARCHAR', 'CnpjInstituicaoLider': 'VARCHAR'}}
                )
            """

            cursor = self._get_cursor()
            df = cursor.sql(query).df()

            if df.empty:
                return None

            df = df.replace("null", None)
            df["CNPJ_LIDER_8"] = df["CnpjInstituicaoLider"].apply(
                lambda x: standardize_cnpj_base8(x) if is_valid(x) else None
            )
            cod_inst = df["CodInst"].astype(str).str.strip()
            is_numeric_cod_inst = cod_inst.str.fullmatch(r"\d+").fillna(False)
            df["CNPJ_8"] = None
            df.loc[is_numeric_cod_inst, "CNPJ_8"] = df.loc[
                is_numeric_cod_inst, "CodInst"
            ].apply(standardize_cnpj_base8)
            df["Data"] = pd.to_numeric(df["Data"], errors="coerce").astype("Int64")
            df = df.drop(columns=["CnpjInstituicaoLider"])

            cols = [
                "Data",
                "CodInst",
                "CNPJ_8",
                "NomeInstituicao",
                "SegmentoTb",
                "CodConglomeradoPrudencial",
                "CodConglomeradoFinanceiro",
                "CNPJ_LIDER_8",
                "Situacao",
                "Atividade",
                "Tcb",
                "Td",
                "Tc",
                "Uf",
                "Municipio",
                "Sr",
                "DataInicioAtividade",
            ]
            return df[[c for c in cols if c in df.columns]]

        except Exception as e:
            self.logger.error(f"Erro processando {csv_path}: {e}")
            raise DataProcessingError("ifdata_cadastro", str(e)) from e
