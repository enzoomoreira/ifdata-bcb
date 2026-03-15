from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd
import requests

from ifdata_bcb.core.constants import DATA_SOURCES, TIPO_INST_MAP, get_subdir
from ifdata_bcb.domain.exceptions import DataProcessingError
from ifdata_bcb.infra.resilience import DEFAULT_REQUEST_TIMEOUT, retry
from ifdata_bcb.infra.storage import DataManager
from ifdata_bcb.providers.base_collector import BaseCollector
from ifdata_bcb.utils.cnpj import standardize_cnpj_base8


class IFDATAValoresCollector(BaseCollector):
    """Collector para IFDATA Valores (trimestral). Baixa 3 tipos de instituicao."""

    _PERIOD_TYPE = "quarterly"
    _BASE_URL = "https://olinda.bcb.gov.br/olinda/servico/IFDATA/versao/v1/odata"

    def __init__(self, data_manager: DataManager | None = None):
        super().__init__(data_manager)

    def _get_file_prefix(self) -> str:
        return DATA_SOURCES["ifdata_valores"]["prefix"]

    def _get_subdir(self) -> str:
        return get_subdir("ifdata_valores")

    @retry(delay=2.0)
    def _download_single(self, url: str, output_path: Path) -> bool:
        response = requests.get(url, timeout=DEFAULT_REQUEST_TIMEOUT)
        response.raise_for_status()
        output_path.write_bytes(response.content)
        return True

    def _download_period(self, period: int, work_dir: Path) -> Path | None:
        """Baixa 3 tipos de instituicao em paralelo."""
        tipos_inst = list(TIPO_INST_MAP.values())
        downloaded = []

        def download_tipo(tipo: int) -> Path | None:
            url = (
                f"{self._BASE_URL}/IfDataValores"
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


class IFDATACadastroCollector(BaseCollector):
    """Collector para dados IFDATA Cadastro (trimestral)."""

    _PERIOD_TYPE = "quarterly"
    _BASE_URL = "https://olinda.bcb.gov.br/olinda/servico/IFDATA/versao/v1/odata"

    def __init__(self, data_manager: DataManager | None = None):
        super().__init__(data_manager)

    def _get_file_prefix(self) -> str:
        return DATA_SOURCES["cadastro"]["prefix"]

    def _get_subdir(self) -> str:
        return get_subdir("cadastro")

    @retry(delay=2.0)
    def _download_single(self, url: str, output_path: Path) -> bool:
        response = requests.get(url, timeout=DEFAULT_REQUEST_TIMEOUT)
        response.raise_for_status()
        output_path.write_bytes(response.content)
        return True

    def _download_period(self, period: int, work_dir: Path) -> Path | None:
        url = (
            f"{self._BASE_URL}/IfDataCadastro(AnoMes=@AnoMes)"
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
                lambda x: standardize_cnpj_base8(x) if pd.notna(x) else None
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
