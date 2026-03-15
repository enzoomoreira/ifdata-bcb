"""
Collector para dados COSIF do BCB.

Faz download e processamento de dados COSIF (Individual e Prudencial).
"""

import tempfile
import zipfile
from pathlib import Path
from typing import Optional

import duckdb
import pandas as pd
import requests

from ifdata_bcb.infra.resilience import DEFAULT_REQUEST_TIMEOUT, retry
from ifdata_bcb.infra.storage import DataManager
from ifdata_bcb.services.base_collector import BaseCollector, PeriodUnavailableError
from ifdata_bcb.utils.cnpj import standardize_cnpj_base8


class COSIFCollector(BaseCollector):
    """
    Collector para dados COSIF.

    Faz download de dados COSIF do BCB e processa para formato Parquet
    otimizado, salvando um arquivo por periodo.

    Escopos suportados:
    - 'individual': Dados de instituicoes individuais
    - 'prudencial': Dados de conglomerados prudenciais

    Exemplo:
        collector = COSIFCollector('individual')
        collector.collect('2024-01', '2024-12')
    """

    _PERIOD_TYPE = "monthly"

    # Configuracao por escopo
    # Nota: BCB usa encodings diferentes por escopo
    _CONFIG = {
        "individual": {
            "url_segment": "Bancos",
            "file_pattern": "BANCOS",
            "suffixes": ["BANCOS.csv.zip", "BANCOS.zip", "BANCOS.csv"],
            "prefix": "cosif_ind",
            "subdir": "cosif/individual",
            "encoding": "cp1252",
        },
        "prudencial": {
            "url_segment": "Conglomerados-prudenciais",
            "file_pattern": "BLOPRUDENCIAL",
            "suffixes": [
                "BLOPRUDENCIAL.csv.zip",
                "BLOPRUDENCIAL.zip",
                "BLOPRUDENCIAL.csv",
            ],
            "prefix": "cosif_prud",
            "subdir": "cosif/prudencial",
            "encoding": "latin-1",
        },
    }

    def __init__(
        self,
        escopo: str,
        data_manager: Optional[DataManager] = None,
    ):
        """
        Inicializa o collector COSIF.

        Args:
            escopo: 'individual' ou 'prudencial'.
            data_manager: DataManager customizado. Se None, cria um novo.

        Raises:
            ValueError: Se escopo nao for valido.
        """
        escopo = escopo.lower()
        if escopo not in self._CONFIG:
            valid = ", ".join(self._CONFIG.keys())
            raise ValueError(f"Escopo '{escopo}' invalido. Validos: {valid}")

        super().__init__(data_manager)
        self.escopo = escopo
        self._config = self._CONFIG[escopo]

    def _get_file_prefix(self) -> str:
        return self._config["prefix"]

    def _get_subdir(self) -> str:
        return self._config["subdir"]

    @retry(delay=2.0)  # API BCB e lenta, usar delay maior que o padrao
    def _download_single(self, url: str, output_path: Path, period: int = 0) -> bool:
        """
        Faz download de um arquivo com retry automatico via decorator.

        Args:
            url: URL para download.
            output_path: Caminho para salvar o arquivo.
            period: Periodo sendo baixado (para erro mais informativo).

        Returns:
            True se sucesso.

        Raises:
            PeriodUnavailableError: Se 404 (periodo nao disponivel, sem retry).
            requests.RequestException: Se outras falhas apos todas tentativas.
        """
        response = requests.get(url, timeout=DEFAULT_REQUEST_TIMEOUT)

        # 404 indica periodo indisponivel - nao retentar
        # PeriodUnavailableError nao esta em TRANSIENT_EXCEPTIONS, entao propaga imediatamente
        if response.status_code == 404:
            raise PeriodUnavailableError(period)

        response.raise_for_status()
        output_path.write_bytes(response.content)
        return True

    def _download_period(self, period: int) -> Optional[Path]:
        """
        Baixa dados COSIF de um periodo especifico.

        Tenta diferentes sufixos ate encontrar um que funcione.

        Args:
            period: Periodo no formato YYYYMM.

        Returns:
            Path do arquivo CSV ou None se falhar.

        Raises:
            PeriodUnavailableError: Se todos os sufixos retornarem 404.
        """
        url_segment = self._config["url_segment"]
        suffixes = self._config["suffixes"]
        file_pattern = self._config["file_pattern"]

        # Diretorio temporario para download
        temp_dir = Path(tempfile.mkdtemp(prefix=f"cosif_{period}_"))

        # Contadores para diferenciar 404 de outros erros
        not_found_count = 0
        other_errors = 0

        for suffix in suffixes:
            url = f"https://www.bcb.gov.br/content/estabilidadefinanceira/cosif/{url_segment}/{period}{suffix}"
            local_file = temp_dir / f"{period}{suffix}"

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
                                zf.extractall(temp_dir, members=csv_files)
                                csv_path = temp_dir / csv_files[0]
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

            except requests.RequestException:
                # Outros erros de rede
                other_errors += 1
                continue

        # Se todos retornaram 404, periodo nao esta disponivel
        if not_found_count == len(suffixes):
            raise PeriodUnavailableError(period)

        return None

    def _process_to_parquet(
        self, csv_path: Path, period: int
    ) -> Optional[pd.DataFrame]:
        """
        Processa CSV COSIF para DataFrame normalizado (schema raw).

        Usa DuckDB para processamento eficiente. Mantem nomes de storage
        (raw) - o mapeamento para nomes de apresentacao e feito no Explorer.

        Schema de storage:
        - DATA_BASE (int64): Periodo YYYYMM
        - CNPJ_8 (string): CNPJ normalizado para 8 digitos
        - NOME_INSTITUICAO (string): Nome da instituicao
        - DOCUMENTO (string): Tipo de documento (7 ou 8)
        - CONTA (string): Codigo da conta COSIF
        - NOME_CONTA (string): Nome/descricao da conta
        - SALDO (double): Valor em reais

        Args:
            csv_path: Caminho do arquivo CSV.
            period: Periodo no formato YYYYMM.

        Returns:
            DataFrame processado ou None se falhar.
        """
        try:
            # Usar DuckDB para processamento eficiente
            # COSIF: separador ; e tem 3 linhas de metadata
            # Header real comeca com "#DATA_BASE" na linha 4
            # Colunas originais: #DATA_BASE;DOCUMENTO;CNPJ;AGENCIA;NOME_INSTITUICAO;
            #                    COD_CONGL;NOME_CONGL;TAXONOMIA;CONTA;NOME_CONTA;SALDO
            # Encoding varia por escopo (ver _CONFIG)
            # Thread-safe: conexao local por execucao (API global nao e thread-safe)
            encoding = self._config["encoding"]
            query = f"""
                SELECT
                    "#DATA_BASE" as DATA_BASE,
                    CNPJ,
                    NOME_INSTITUICAO,
                    DOCUMENTO,
                    CONTA,
                    NOME_CONTA,
                    TRY_CAST(REPLACE(SALDO, ',', '.') AS DOUBLE) as SALDO
                FROM read_csv(
                    '{csv_path}',
                    delim=';',
                    header=true,
                    skip=3,
                    encoding='{encoding}'
                )
            """

            conn = duckdb.connect()
            try:
                df = conn.sql(query).df()
            finally:
                conn.close()

            if df.empty:
                return None

            # Normalizar CNPJ para 8 digitos (transformacao necessaria)
            df["CNPJ_8"] = df["CNPJ"].apply(standardize_cnpj_base8)

            # Converter DATA_BASE para int (YYYYMM)
            df["DATA_BASE"] = pd.to_numeric(
                df["DATA_BASE"], errors="coerce"
            ).astype("Int64")

            # Remover CNPJ original (manter apenas CNPJ_8)
            df = df.drop(columns=["CNPJ"])

            # Ordenar colunas de storage
            cols = [
                "DATA_BASE",
                "CNPJ_8",
                "NOME_INSTITUICAO",
                "DOCUMENTO",
                "CONTA",
                "NOME_CONTA",
                "SALDO",
            ]
            df = df[[c for c in cols if c in df.columns]]

            return df

        except Exception as e:
            self.logger.error(f"Erro processando {csv_path}: {e}")
            return None
