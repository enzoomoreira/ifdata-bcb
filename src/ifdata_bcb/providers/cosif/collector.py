import zipfile
from pathlib import Path
from typing import TypedDict

import httpx
import pandas as pd

from ifdata_bcb.core.constants import DATA_SOURCES, get_subdir
from ifdata_bcb.domain.exceptions import (
    DataProcessingError,
    InvalidScopeError,
    PeriodUnavailableError,
)
from ifdata_bcb.infra.resilience import retry
from ifdata_bcb.infra.storage import DataManager
from ifdata_bcb.providers.base_collector import BaseCollector
from ifdata_bcb.utils.cnpj import standardize_cnpj_base8


class _EscopoConfig(TypedDict):
    url_segment: str
    file_pattern: str
    suffixes: list[str]
    prefix: str
    subdir: str
    encoding: str


class COSIFCollector(BaseCollector):
    """Collector para dados COSIF (mensal). Escopos: 'individual' ou 'prudencial'."""

    _PERIOD_TYPE = "monthly"

    # Configuracao por escopo
    # Nota: BCB usa encodings diferentes por escopo
    _CONFIG: dict[str, _EscopoConfig] = {
        "individual": {
            "url_segment": "Bancos",
            "file_pattern": "BANCOS",
            "suffixes": ["BANCOS.csv.zip", "BANCOS.zip", "BANCOS.csv"],
            "prefix": DATA_SOURCES["cosif_individual"]["prefix"],
            "subdir": get_subdir("cosif_individual"),
            "encoding": "CP1252",
        },
        "prudencial": {
            "url_segment": "Conglomerados-prudenciais",
            "file_pattern": "BLOPRUDENCIAL",
            "suffixes": [
                "BLOPRUDENCIAL.csv.zip",
                "BLOPRUDENCIAL.zip",
                "BLOPRUDENCIAL.csv",
            ],
            "prefix": DATA_SOURCES["cosif_prudencial"]["prefix"],
            "subdir": get_subdir("cosif_prudencial"),
            "encoding": "latin-1",
        },
    }

    def __init__(
        self,
        escopo: str,
        data_manager: DataManager | None = None,
    ):
        escopo = escopo.lower()
        if escopo not in self._CONFIG:
            raise InvalidScopeError("escopo", escopo, list(self._CONFIG.keys()))

        super().__init__(data_manager)
        self.escopo = escopo
        self._config = self._CONFIG[escopo]

    def _get_file_prefix(self) -> str:
        return self._config["prefix"]

    def _get_subdir(self) -> str:
        return self._config["subdir"]

    @retry(delay=2.0)
    def _download_single(self, url: str, output_path: Path, period: int = 0) -> bool:
        """
        Excecoes:
            PeriodUnavailableError: Se 404 (sem retry).
            httpx.HTTPError: Se falhas apos todas tentativas.
        """
        response = self._http.get(url)

        # 404 = periodo indisponivel, propaga PeriodUnavailableError (sem retry)
        if response.status_code == 404:
            raise PeriodUnavailableError(period)

        response.raise_for_status()
        output_path.write_bytes(response.content)
        return True

    def _download_period(self, period: int, work_dir: Path) -> Path | None:
        """
        Tenta diferentes sufixos ate encontrar um que funcione.

        Excecoes:
            PeriodUnavailableError: Se todos os sufixos retornarem 404.
        """
        url_segment = self._config["url_segment"]
        suffixes = self._config["suffixes"]
        file_pattern = self._config["file_pattern"]

        # Contadores para diferenciar 404 de outros erros
        not_found_count = 0
        other_errors = 0

        for suffix in suffixes:
            url = f"https://www.bcb.gov.br/content/estabilidadefinanceira/cosif/{url_segment}/{period}{suffix}"
            local_file = work_dir / f"{period}{suffix}"

            try:
                self._download_single(url, local_file, period)

                # Se for ZIP, extrair
                if "zip" in suffix.lower():
                    try:
                        with zipfile.ZipFile(local_file, "r") as zf:
                            # Encontrar CSV no ZIP
                            csv_files = [
                                m
                                for m in zf.namelist()
                                if m.lower().endswith(".csv")
                                and file_pattern.lower() in m.lower()
                            ]
                            if csv_files:
                                zf.extractall(work_dir, members=csv_files)
                                csv_path = work_dir / csv_files[0]
                                return csv_path
                    except zipfile.BadZipFile:
                        other_errors += 1
                        continue
                else:
                    # Arquivo CSV direto
                    return local_file

            except PeriodUnavailableError:
                # 404 - periodo nao disponivel, tentar proximo sufixo
                not_found_count += 1
                continue

            except httpx.HTTPError:
                # Outros erros de rede
                other_errors += 1
                continue

        # Se todos retornaram 404, periodo nao esta disponivel
        if not_found_count == len(suffixes):
            raise PeriodUnavailableError(period)

        return None

    def _process_to_parquet(self, csv_path: Path, period: int) -> pd.DataFrame | None:
        """Processa CSV COSIF para DataFrame. Suporta todas as eras de formato."""
        try:
            from ifdata_bcb.core.eras import build_cosif_select, detect_cosif_csv_era

            encoding = self._config["encoding"]
            era = detect_cosif_csv_era(csv_path, encoding)
            query = build_cosif_select(era, csv_path, encoding)

            cursor = self._get_cursor()
            df = cursor.sql(query).df()

            if df.empty:
                return None

            df["CNPJ_8"] = df["CNPJ"].apply(standardize_cnpj_base8)
            df["DATA_BASE"] = pd.to_numeric(df["DATA_BASE"], errors="coerce").astype(
                "Int64"
            )
            df = df.drop(columns=["CNPJ"])

            cols = [
                "DATA_BASE",
                "CNPJ_8",
                "NOME_INSTITUICAO",
                "DOCUMENTO",
                "CONTA",
                "NOME_CONTA",
                "SALDO",
            ]
            return df[[c for c in cols if c in df.columns]]

        except Exception as e:
            self.logger.error(f"Erro processando {csv_path}: {e}")
            raise DataProcessingError(f"cosif:{self.escopo}", str(e)) from e
