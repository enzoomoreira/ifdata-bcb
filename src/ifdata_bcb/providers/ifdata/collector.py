"""
Collectors para dados IFDATA do BCB.

Faz download e processamento de dados IFDATA (Valores e Cadastro).
"""

import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Optional

import duckdb
import pandas as pd
import requests

from ifdata_bcb.infra.resilience import DEFAULT_REQUEST_TIMEOUT, retry
from ifdata_bcb.infra.storage import DataManager
from ifdata_bcb.services.base_collector import BaseCollector
from ifdata_bcb.utils.cnpj import standardize_cnpj_base8


class IFDATAValoresCollector(BaseCollector):
    """
    Collector para dados IFDATA Valores.

    Faz download de dados de valores financeiros do IFDATA e processa
    para formato Parquet otimizado, salvando um arquivo por periodo.

    IFDATA Valores tem 3 tipos de instituicao que sao baixados e
    consolidados em um unico arquivo por periodo.

    Exemplo:
        collector = IFDATAValoresCollector()
        collector.collect('2024-01', '2024-12')
    """

    _PERIOD_TYPE = "quarterly"
    _BASE_URL = "https://olinda.bcb.gov.br/olinda/servico/IFDATA/versao/v1/odata"

    def __init__(self, data_manager: Optional[DataManager] = None):
        """
        Inicializa o collector IFDATA Valores.

        Args:
            data_manager: DataManager customizado. Se None, cria um novo.
        """
        super().__init__(data_manager)

    def _get_file_prefix(self) -> str:
        return "ifdata_val"

    def _get_subdir(self) -> str:
        return "ifdata/valores"

    @retry(delay=2.0)  # API BCB e lenta, usar delay maior que o padrao
    def _download_single(self, url: str, output_path: Path) -> bool:
        """
        Faz download de um arquivo com retry automatico via decorator.

        Args:
            url: URL para download.
            output_path: Caminho para salvar o arquivo.

        Returns:
            True se sucesso.

        Raises:
            requests.RequestException: Se todas tentativas falharem.
        """
        response = requests.get(url, timeout=DEFAULT_REQUEST_TIMEOUT)
        response.raise_for_status()
        output_path.write_bytes(response.content)
        return True

    def _download_period(self, period: int) -> Optional[Path]:
        """
        Baixa dados IFDATA Valores de um periodo.

        IFDATA Valores requer baixar 3 tipos de instituicao separadamente
        e depois consolidar. Downloads sao feitos em paralelo para melhor
        performance (API do BCB e lenta).

        Args:
            period: Periodo no formato YYYYMM.

        Returns:
            Path do diretorio com os CSVs ou None se falhar.
        """
        temp_dir = Path(tempfile.mkdtemp(prefix=f"ifdata_val_{period}_"))

        tipos_inst = [1, 2, 3]
        downloaded = []
        errors = []

        def download_tipo(tipo: int) -> tuple[Optional[Path], Optional[str]]:
            """Download de um tipo de instituicao, retorna (path, error)."""
            url = (
                f"{self._BASE_URL}/IfDataValores"
                f"(AnoMes=@AnoMes,TipoInstituicao=@TipoInstituicao,Relatorio=@Relatorio)"
                f"?@AnoMes={period}&@TipoInstituicao={tipo}&@Relatorio='T'&$format=text/csv"
            )
            output_path = temp_dir / f"ifdata_val_{period}_{tipo}.csv"

            try:
                self._download_single(url, output_path)
                return (output_path, None)
            except Exception as e:
                return (None, f"tipo {tipo}: {e}")

        # Download paralelo dos 3 tipos (ganho de ~36% vs sequencial)
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = {executor.submit(download_tipo, t): t for t in tipos_inst}
            for future in as_completed(futures):
                path, error = future.result()
                if error:
                    errors.append(error)
                elif path:
                    downloaded.append(path)

        if errors:
            # Log tecnico; falha sera reportada visualmente pelo collect()
            self.logger.debug(f"Periodo {period}: {len(errors)} download(s) falharam")

        if not downloaded:
            return None

        return temp_dir

    def _process_to_parquet(self, csv_dir: Path, period: int) -> Optional[pd.DataFrame]:
        """
        Processa CSVs IFDATA Valores para DataFrame normalizado (schema raw).

        Consolida os 3 tipos de instituicao em um unico DataFrame.
        Mantem nomes de storage (raw) - o mapeamento para nomes de
        apresentacao e feito no Explorer.

        Schema de storage:
        - AnoMes (int64): Periodo YYYYMM
        - CodInst (string): Codigo da instituicao
        - TipoInstituicao (int64): Tipo (1=prudencial, 2=financeiro, 3=individual)
        - Conta (string): Codigo da conta
        - NomeColuna (string): Nome/descricao da conta
        - Saldo (double): Valor em reais
        - NomeRelatorio (string): Nome do relatorio
        - Grupo (string): Grupo da conta

        Args:
            csv_dir: Diretorio com os arquivos CSV.
            period: Periodo no formato YYYYMM.

        Returns:
            DataFrame processado ou None se falhar.
        """
        try:
            csv_files = list(csv_dir.glob("*.csv"))
            if not csv_files:
                return None

            dfs = []
            for csv_path in csv_files:
                # Verificar se arquivo nao esta vazio
                if csv_path.stat().st_size <= 100:
                    continue

                # Usar DuckDB para leitura eficiente
                # Thread-safe: conexao local por execucao (API global nao e thread-safe)
                # Mantem nomes originais da API (storage)
                query = f"""
                    SELECT
                        AnoMes,
                        CodInst,
                        TipoInstituicao,
                        Conta,
                        NomeColuna,
                        TRY_CAST(REPLACE(CAST(Saldo AS VARCHAR), ',', '.') AS DOUBLE) as Saldo,
                        NomeRelatorio,
                        Grupo
                    FROM read_csv(
                        '{csv_path}',
                        delim=',',
                        header=true,
                        ignore_errors=true
                    )
                """

                conn = duckdb.connect()
                try:
                    df = conn.sql(query).df()
                finally:
                    conn.close()
                if not df.empty:
                    dfs.append(df)

            if not dfs:
                return None

            # Concatenar todos os tipos
            df = pd.concat(dfs, ignore_index=True)

            # Substituir strings "null" por None
            df = df.replace("null", None)

            # Converter AnoMes para int
            df["AnoMes"] = pd.to_numeric(df["AnoMes"], errors="coerce").astype("Int64")

            # Ordenar colunas de storage
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
            df = df[[c for c in cols if c in df.columns]]

            return df

        except Exception as e:
            self.logger.error(f"Erro processando {csv_dir}: {e}")
            return None


class IFDATACadastroCollector(BaseCollector):
    """
    Collector para dados IFDATA Cadastro.

    Faz download de dados cadastrais do IFDATA e processa
    para formato Parquet otimizado, salvando um arquivo por periodo.

    Exemplo:
        collector = IFDATACadastroCollector()
        collector.collect('2024-01', '2024-12')
    """

    _PERIOD_TYPE = "quarterly"
    _BASE_URL = "https://olinda.bcb.gov.br/olinda/servico/IFDATA/versao/v1/odata"

    def __init__(self, data_manager: Optional[DataManager] = None):
        """
        Inicializa o collector IFDATA Cadastro.

        Args:
            data_manager: DataManager customizado. Se None, cria um novo.
        """
        super().__init__(data_manager)

    def _get_file_prefix(self) -> str:
        return "ifdata_cad"

    def _get_subdir(self) -> str:
        return "ifdata/cadastro"

    @retry(delay=2.0)  # API BCB e lenta, usar delay maior que o padrao
    def _download_single(self, url: str, output_path: Path) -> bool:
        """
        Faz download de um arquivo com retry automatico via decorator.

        Args:
            url: URL para download.
            output_path: Caminho para salvar o arquivo.

        Returns:
            True se sucesso.

        Raises:
            requests.RequestException: Se todas tentativas falharem.
        """
        response = requests.get(url, timeout=DEFAULT_REQUEST_TIMEOUT)
        response.raise_for_status()
        output_path.write_bytes(response.content)
        return True

    def _download_period(self, period: int) -> Optional[Path]:
        """
        Baixa dados IFDATA Cadastro de um periodo.

        Args:
            period: Periodo no formato YYYYMM.

        Returns:
            Path do arquivo CSV ou None se falhar.
        """
        url = (
            f"{self._BASE_URL}/IfDataCadastro(AnoMes=@AnoMes)"
            f"?@AnoMes={period}&$format=text/csv"
        )

        temp_dir = Path(tempfile.mkdtemp(prefix=f"ifdata_cad_{period}_"))
        output_path = temp_dir / f"ifdata_cad_{period}.csv"

        try:
            self._download_single(url, output_path)
            if output_path.stat().st_size > 100:
                return output_path
        except Exception as e:
            self.logger.debug(f"Periodo {period} falhou: {e}")

        return None

    def _process_to_parquet(
        self, csv_path: Path, period: int
    ) -> Optional[pd.DataFrame]:
        """
        Processa CSV IFDATA Cadastro para DataFrame normalizado (schema raw).

        Mantem nomes de storage (raw) - o mapeamento para nomes de
        apresentacao e feito no Explorer.

        Schema de storage:
        - Data (int64): Periodo YYYYMM
        - CNPJ_8 (string): CNPJ normalizado para 8 digitos
        - NomeInstituicao (string): Nome da instituicao
        - SegmentoTb (string): Segmento (B1, B2, S1, etc.)
        - CodConglomeradoPrudencial (string): Codigo do conglomerado prudencial
        - CodConglomeradoFinanceiro (string): Codigo do conglomerado financeiro
        - CNPJ_LIDER_8 (string): CNPJ do lider normalizado
        - Situacao (string): Situacao da instituicao
        - Atividade (string): Atividade principal
        - Tcb, Td, Tc (string): Classificacoes regulatorias
        - Uf, Municipio, Sr (string): Localizacao
        - DataInicioAtividade (string): Data de inicio

        Args:
            csv_path: Caminho do arquivo CSV.
            period: Periodo no formato YYYYMM.

        Returns:
            DataFrame processado ou None se falhar.
        """
        try:
            # Usar DuckDB para leitura eficiente
            # Thread-safe: conexao local por execucao (API global nao e thread-safe)
            # Mantem nomes originais da API (storage)
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

            conn = duckdb.connect()
            try:
                df = conn.sql(query).df()
            finally:
                conn.close()

            if df.empty:
                return None

            # Substituir strings "null" por None
            df = df.replace("null", None)

            # Normalizar CNPJs para 8 digitos (transformacao necessaria)
            df["CNPJ_8"] = df["CodInst"].apply(standardize_cnpj_base8)
            df["CNPJ_LIDER_8"] = df["CnpjInstituicaoLider"].apply(
                lambda x: standardize_cnpj_base8(x) if pd.notna(x) else None
            )

            # Converter Data para int (YYYYMM)
            df["Data"] = pd.to_numeric(df["Data"], errors="coerce").astype("Int64")

            # Remover colunas originais
            df = df.drop(columns=["CodInst", "CnpjInstituicaoLider"])

            # Ordenar colunas de storage
            cols = [
                "Data",
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
            df = df[[c for c in cols if c in df.columns]]

            return df

        except Exception as e:
            self.logger.error(f"Erro processando {csv_path}: {e}")
            return None
