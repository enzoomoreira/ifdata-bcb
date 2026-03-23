"""Testes de contrato: verificam que os endpoints do BCB respondem com o schema esperado.

Estes testes fazem requisicoes REAIS ao BCB. Rodar sob demanda:
    uv run python -m pytest tests/contract/ -m contract -v

NAO rodar no CI (dependem de rede e disponibilidade do BCB).
"""

import io
import zipfile

import pandas as pd
import pytest
import httpx

# Periodo recente que sabemos existir
_TEST_PERIOD = 202412
_TIMEOUT = 30


pytestmark = pytest.mark.contract


# =========================================================================
# COSIF endpoints
# =========================================================================


class TestCOSIFEndpoints:
    """Verifica que os endpoints COSIF respondem e tem o schema esperado."""

    _COSIF_BASE = "https://www.bcb.gov.br/content/estabilidadefinanceira/cosif"

    _EXPECTED_COLUMNS = {
        "#DATA_BASE",
        "CNPJ",
        "NOME_INSTITUICAO",
        "DOCUMENTO",
        "CONTA",
        "NOME_CONTA",
        "SALDO",
    }

    def _download_cosif_csv(self, segment: str, pattern: str) -> pd.DataFrame:
        """Baixa e extrai CSV COSIF de um ZIP."""
        suffixes = [f"{pattern}.csv.zip", f"{pattern}.zip"]

        for suffix in suffixes:
            url = f"{self._COSIF_BASE}/{segment}/{_TEST_PERIOD}{suffix}"
            r = httpx.get(url, timeout=_TIMEOUT, follow_redirects=True)
            if r.status_code != 200:
                continue

            with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
                csv_files = [
                    n
                    for n in zf.namelist()
                    if n.lower().endswith(".csv") and pattern.lower() in n.lower()
                ]
                if not csv_files:
                    continue

                with zf.open(csv_files[0]) as f:
                    return pd.read_csv(
                        f,
                        sep=";",
                        skiprows=3,
                        encoding="latin-1",
                        nrows=5,
                    )

        pytest.fail(f"Nenhum sufixo funcionou para {segment}/{_TEST_PERIOD}")

    def test_individual_returns_200(self) -> None:
        url = f"{self._COSIF_BASE}/Bancos/{_TEST_PERIOD}BANCOS.csv.zip"
        r = httpx.head(url, timeout=_TIMEOUT, follow_redirects=True)
        assert r.status_code == 200, f"COSIF Individual: status {r.status_code}"

    def test_prudencial_returns_200(self) -> None:
        url = f"{self._COSIF_BASE}/Conglomerados-prudenciais/{_TEST_PERIOD}BLOPRUDENCIAL.csv.zip"
        r = httpx.head(url, timeout=_TIMEOUT, follow_redirects=True)
        assert r.status_code == 200, f"COSIF Prudencial: status {r.status_code}"

    def test_individual_csv_has_expected_columns(self) -> None:
        df = self._download_cosif_csv("Bancos", "BANCOS")
        actual = set(df.columns)
        missing = self._EXPECTED_COLUMNS - actual
        assert not missing, f"Colunas ausentes no CSV COSIF Individual: {missing}"

    def test_prudencial_csv_has_expected_columns(self) -> None:
        df = self._download_cosif_csv("Conglomerados-prudenciais", "BLOPRUDENCIAL")
        actual = set(df.columns)
        missing = self._EXPECTED_COLUMNS - actual
        assert not missing, f"Colunas ausentes no CSV COSIF Prudencial: {missing}"


# =========================================================================
# IFDATA endpoints
# =========================================================================


class TestIFDATAEndpoints:
    """Verifica que a API IFDATA responde e tem o schema esperado."""

    _IFDATA_BASE = "https://olinda.bcb.gov.br/olinda/servico/IFDATA/versao/v1/odata"

    _VALORES_EXPECTED_COLUMNS = {
        "AnoMes",
        "CodInst",
        "TipoInstituicao",
        "Conta",
        "NomeColuna",
        "Saldo",
        "NomeRelatorio",
        "Grupo",
    }

    _CADASTRO_EXPECTED_COLUMNS = {
        "Data",
        "CodInst",
        "NomeInstituicao",
        "SegmentoTb",
        "CodConglomeradoPrudencial",
        "CodConglomeradoFinanceiro",
        "CnpjInstituicaoLider",
        "Situacao",
    }

    def _fetch_ifdata_csv(self, endpoint: str, params: str) -> pd.DataFrame:
        url = f"{self._IFDATA_BASE}/{endpoint}?{params}&$format=text/csv"
        r = httpx.get(url, timeout=_TIMEOUT, follow_redirects=True)
        assert r.status_code == 200, f"IFDATA {endpoint}: status {r.status_code}"
        return pd.read_csv(io.StringIO(r.text), nrows=5)

    def test_valores_api_returns_csv(self) -> None:
        url = (
            f"{self._IFDATA_BASE}/IfDataValores"
            f"(AnoMes=@AnoMes,TipoInstituicao=@TipoInstituicao,Relatorio=@Relatorio)"
            f"?@AnoMes={_TEST_PERIOD}&@TipoInstituicao=3&@Relatorio='T'&$format=text/csv"
        )
        r = httpx.get(url, timeout=_TIMEOUT, follow_redirects=True)
        assert r.status_code == 200
        assert len(r.text) > 100, "Resposta muito curta (possivelmente vazia)"

    def test_valores_csv_has_expected_columns(self) -> None:
        params = f"@AnoMes={_TEST_PERIOD}&@TipoInstituicao=3&@Relatorio='T'"
        df = self._fetch_ifdata_csv(
            "IfDataValores(AnoMes=@AnoMes,TipoInstituicao=@TipoInstituicao,Relatorio=@Relatorio)",
            params,
        )
        actual = set(df.columns)
        missing = self._VALORES_EXPECTED_COLUMNS - actual
        assert not missing, f"Colunas ausentes no IFDATA Valores: {missing}"

    def test_cadastro_api_returns_csv(self) -> None:
        url = (
            f"{self._IFDATA_BASE}/IfDataCadastro(AnoMes=@AnoMes)"
            f"?@AnoMes={_TEST_PERIOD}&$format=text/csv"
        )
        r = httpx.get(url, timeout=_TIMEOUT, follow_redirects=True)
        assert r.status_code == 200
        assert len(r.text) > 100

    def test_cadastro_csv_has_expected_columns(self) -> None:
        df = self._fetch_ifdata_csv(
            "IfDataCadastro(AnoMes=@AnoMes)",
            f"@AnoMes={_TEST_PERIOD}",
        )
        actual = set(df.columns)
        missing = self._CADASTRO_EXPECTED_COLUMNS - actual
        assert not missing, f"Colunas ausentes no IFDATA Cadastro: {missing}"
