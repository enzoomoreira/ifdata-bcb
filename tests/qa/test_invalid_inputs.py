"""QA: inputs invalidos -- simula usuario real passando dados errados."""

import contextlib
from pathlib import Path

import pandas as pd
import pytest

from ifdata_bcb.core.entity import EntitySearch
from ifdata_bcb.domain.exceptions import (
    InvalidColumnError,
    InvalidDateFormatError,
    InvalidDateRangeError,
    InvalidIdentifierError,
    InvalidScopeError,
)
from ifdata_bcb.providers.cosif.explorer import COSIFExplorer
from ifdata_bcb.providers.ifdata.cadastro.explorer import CadastroExplorer
from ifdata_bcb.providers.ifdata.valores.explorer import IFDATAExplorer


class TestMissingParams:
    def test_read_without_start_raises(self, qa_cosif: COSIFExplorer) -> None:
        with pytest.raises(TypeError):
            qa_cosif.read(instituicao="60872504")  # type: ignore[call-arg]

    def test_read_without_instituicao_does_not_raise(
        self, qa_cosif: COSIFExplorer
    ) -> None:
        """instituicao e opcional agora (bulk read)."""
        import warnings

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            df = qa_cosif.read("2023-03")
        assert df is not None


class TestInvalidCNPJ:
    def test_cnpj_7_digits(self, qa_cosif: COSIFExplorer) -> None:
        with pytest.raises(InvalidIdentifierError):
            qa_cosif.read(instituicao="1234567", start="2023-03")

    def test_cnpj_9_digits(self, qa_cosif: COSIFExplorer) -> None:
        with pytest.raises(InvalidIdentifierError):
            qa_cosif.read(instituicao="123456789", start="2023-03")

    def test_cnpj_letters(self, qa_cosif: COSIFExplorer) -> None:
        with pytest.raises(InvalidIdentifierError):
            qa_cosif.read(instituicao="abcdefgh", start="2023-03")

    def test_cnpj_sql_injection(self, qa_cosif: COSIFExplorer) -> None:
        with pytest.raises(InvalidIdentifierError):
            qa_cosif.read(instituicao="'; DROP TABLE--", start="2023-03")

    def test_cnpj_with_spaces_is_accepted(self, qa_cosif: COSIFExplorer) -> None:
        """Espaco no meio e artefato de copiar/colar, nao input invalido."""
        df = qa_cosif.read(instituicao="6087 2504", start="2023-03")
        assert not df.empty

    def test_cnpj_completo_14_digitos(self, qa_cosif: COSIFExplorer) -> None:
        df = qa_cosif.read(instituicao="60.872.504/0001-23", start="2023-03")
        assert not df.empty
        assert set(df["cnpj_8"].unique()) == {"60872504"}

    def test_cnpj_14_digitos_com_dv_errado(self, qa_cosif: COSIFExplorer) -> None:
        with pytest.raises(InvalidIdentifierError):
            qa_cosif.read(instituicao="99999999999999", start="2023-03")

    def test_cnpj_fullwidth_unicode_digits(self, qa_cosif: COSIFExplorer) -> None:
        fullwidth = "".join(chr(0xFF10 + i) for i in range(1, 9))
        with pytest.raises(InvalidIdentifierError):
            qa_cosif.read(instituicao=fullwidth, start="2023-03")

    def test_cnpj_as_int_raises(self, qa_cosif: COSIFExplorer) -> None:
        with pytest.raises((TypeError, InvalidIdentifierError)):
            qa_cosif.read(instituicao=60872504, start="2023-03")  # type: ignore[arg-type]

    def test_mixed_valid_invalid_list(self, qa_cosif: COSIFExplorer) -> None:
        with pytest.raises(InvalidIdentifierError):
            qa_cosif.read(instituicao=["60872504", "abc"], start="2023-03")


class TestInvalidDates:
    def test_date_abc(self, qa_cosif: COSIFExplorer) -> None:
        with pytest.raises(InvalidDateFormatError):
            qa_cosif.read(instituicao="60872504", start="abc")

    def test_date_9999_99(self, qa_cosif: COSIFExplorer) -> None:
        with pytest.raises(InvalidDateFormatError):
            qa_cosif.read(instituicao="60872504", start="9999-99")

    def test_start_after_end(self, qa_cosif: COSIFExplorer) -> None:
        with pytest.raises(InvalidDateRangeError):
            qa_cosif.read(instituicao="60872504", start="2024-01", end="2023-01")

    def test_date_as_int_works(self, qa_cosif: COSIFExplorer) -> None:
        df = qa_cosif.read(instituicao="60872504", start=202303)
        assert not df.empty


class TestInvalidScope:
    def test_escopo_inexistente(self, qa_cosif: COSIFExplorer) -> None:
        with pytest.raises(InvalidScopeError):
            qa_cosif.read(instituicao="60872504", start="2023-03", escopo="inexistente")

    def test_cadastro_coluna_fake(self, qa_cosif: COSIFExplorer) -> None:
        with pytest.raises(InvalidColumnError):
            qa_cosif.read(
                instituicao="60872504", start="2023-03", cadastro=["COLUNA_FAKE"]
            )

    def test_columns_unknown_raises(self, qa_cosif: COSIFExplorer) -> None:
        with pytest.raises(InvalidColumnError):
            qa_cosif.read("2023-03", instituicao="60872504", columns=["INVENTADA"])


class TestEscopoEmIntrospeccao:
    """4.5: o parametro source foi unificado em escopo."""

    def test_escopo_inexistente_no_cosif(self, qa_cosif: COSIFExplorer) -> None:
        with pytest.raises(InvalidScopeError, match="individual"):
            qa_cosif.list_periodos("inexistente")

    def test_describe_com_escopo_inexistente(self, qa_cosif: COSIFExplorer) -> None:
        with pytest.raises(InvalidScopeError):
            qa_cosif.describe("inexistente")

    def test_ifdata_aceita_escopo_e_responde_pelos_dados(
        self, qa_ifdata: IFDATAExplorer
    ) -> None:
        """Antes o palpite natural list_periodos('individual') dava erro."""
        assert qa_ifdata.list_periodos("individual") == [202303]
        assert qa_ifdata.list_periodos("financeiro") == [202303]

    def test_ifdata_periodo_sem_o_escopo_fica_fora(
        self, qa_ifdata: IFDATAExplorer, qa_cache: Path
    ) -> None:
        pd.DataFrame(
            {
                "AnoMes": pd.array([202306] * 2, dtype="Int64"),
                "CodInst": ["60872504"] * 2,
                "TipoInstituicao": pd.array([3, 3], dtype="Int64"),
                "Conta": ["10100", "20200"],
                "NomeColuna": ["ATIVO TOTAL", "PASSIVO TOTAL"],
                "Saldo": [1e6, 8e5],
                "NomeRelatorio": ["Resumo"] * 2,
                "Grupo": ["Balanco"] * 2,
            }
        ).to_parquet(qa_cache / "ifdata/valores/ifdata_val_202306.parquet", index=False)
        assert qa_ifdata.list_periodos("individual") == [202303, 202306]
        assert qa_ifdata.list_periodos("financeiro") == [202303]
        assert qa_ifdata.list_periodos() == [202303, 202306]

    def test_explorer_sem_escopos_rejeita_escopo(
        self, qa_cadastro: CadastroExplorer
    ) -> None:
        with pytest.raises(InvalidScopeError, match="nao tem escopos"):
            qa_cadastro.list_periodos("individual")


class TestPassthroughColumns:
    """Colunas canonicas em columns= sao aceitas e retornadas lowercase."""

    def test_cosif_cnpj8_accepted(self, qa_cosif: COSIFExplorer) -> None:
        df = qa_cosif.read(
            "2023-03", instituicao="60872504", columns=["cnpj_8", "data", "valor"]
        )
        # data pedida em columns= vira o DatetimeIndex, nao uma coluna
        assert list(df.columns) == ["cnpj_8", "valor"]
        assert df.index.name == "date"

    def test_cosif_documento_accepted(self, qa_cosif: COSIFExplorer) -> None:
        df = qa_cosif.read(
            "2023-03", instituicao="60872504", columns=["documento", "data"]
        )
        assert "documento" in df.columns


class TestValoresNaoEntramNoSQL:
    """Garantia estrutural: nenhum valor de filtro chega ao texto da query.

    Nao basta o resultado vir vazio -- vazio tambem seria o resultado de uma
    injecao que nao casou nada. O que se verifica aqui e que o payload nunca
    entra no SQL enviado ao DuckDB, e sim no dict de parametros.
    """

    PAYLOADS = [
        "'; DROP TABLE t; --",
        # U+FF07 decompoe para aspa ASCII sob NFKD: o vetor original
        "＇ OR 1=1 OR ＇",  # noqa: RUF001 -- a ambiguidade e o objeto do teste
        "banco' OR '1'='1",
    ]

    def _capturar_queries(
        self, explorer: COSIFExplorer
    ) -> tuple[list[str], list[dict]]:
        queries: list[str] = []
        params_capturados: list[dict] = []
        qe = explorer._qe
        sql_original = qe.sql
        glob_original = qe.read_glob

        def spy_sql(query, params=None):
            queries.append(query)
            params_capturados.append(dict(params or {}))
            return sql_original(query, params)

        def spy_glob(*a, **kw):
            where = kw.get("where")
            queries.append(str(where or ""))
            params_capturados.append(dict(getattr(where, "params", {})))
            return glob_original(*a, **kw)

        qe.sql = spy_sql  # type: ignore[method-assign]
        qe.read_glob = spy_glob  # type: ignore[method-assign]
        return queries, params_capturados

    def test_conta_hostil_nao_entra_no_texto_da_query(
        self, qa_cosif: COSIFExplorer
    ) -> None:
        queries, params = self._capturar_queries(qa_cosif)

        for payload in self.PAYLOADS:
            df = qa_cosif.read("2023-03", instituicao="60872504", conta=payload)
            assert df.empty

        assert queries, "nenhuma query capturada -- o spy nao pegou o caminho"
        sql_completo = "\n".join(queries)
        assert "DROP TABLE" not in sql_completo
        assert "1=1" not in sql_completo
        assert "'1'='1" not in sql_completo

        # E o payload realmente viajou -- como dado, no bind
        vinculados = [str(v) for p in params for v in p.values()]
        assert any("DROP TABLE" in v for v in vinculados)

    def test_termo_hostil_em_list_contas_nao_entra_no_sql(
        self, qa_cosif: COSIFExplorer
    ) -> None:
        queries, params = self._capturar_queries(qa_cosif)

        qa_cosif.list_contas(termo="'; DROP TABLE t; --")

        sql_completo = "\n".join(queries)
        assert "DROP TABLE" not in sql_completo
        vinculados = [str(v) for p in params for v in p.values()]
        assert any("DROP TABLE" in v for v in vinculados)


class TestSearchResilience:
    def test_search_sql_injection(self, qa_search: EntitySearch) -> None:
        df = qa_search.search("'; DROP TABLE--")
        assert df.empty or isinstance(df.empty, bool)

    def test_search_10k_chars(self, qa_search: EntitySearch) -> None:
        df = qa_search.search("A" * 10000)
        assert df.empty

    def test_search_unicode_special(self, qa_search: EntitySearch) -> None:
        for term in ["\x00\x01\x02", "\ud800", "banco"]:
            # Erros de encoding sao aceitaveis; crash de outro tipo nao.
            with contextlib.suppress(UnicodeError, ValueError):
                qa_search.search(term)

    def test_search_with_quotes_in_term(self, qa_search: EntitySearch) -> None:
        """Aspas no termo de busca nao crasheiam a query SQL."""
        import pandas as pd

        df = qa_search.search("BANCO 'ALFA'")
        assert isinstance(df, pd.DataFrame)

    def test_search_empty_term_returns_empty(self, qa_search: EntitySearch) -> None:
        df = qa_search.search("")
        assert df.empty


class TestGracefulEmpty:
    def test_nonexistent_account(self, qa_cosif: COSIFExplorer) -> None:
        df = qa_cosif.read(
            instituicao="60872504", start="2023-03", conta="XYZFAKE_INEXISTENTE"
        )
        assert df.empty

    def test_list_contas_negative_limit(self, qa_cosif: COSIFExplorer) -> None:
        with pytest.raises(ValueError, match="limit deve ser > 0"):
            qa_cosif.list_contas(limit=-1)
