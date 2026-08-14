"""Unit: ranking, dedup e threshold de EntitySearch, sem dados reais.

A logica de montagem de resultado (dedup por CNPJ, ordenacao ativa-primeiro,
corte por threshold, filtro de fontes) so tinha cobertura via integracao, que
depende do corpus da fixture e nao consegue fixar os casos de borda um a um.
Aqui o corpus e injetado por fakes e cada regra vira um teste.
"""

import pandas as pd
import pytest

from ifdata_bcb.core.entity.search import EntitySearch

_ENTITIES_SQL = "__ENTITIES__"

_ATIVA = "ATIVA"
_CANCELADA = "CANCELADA"


class FakeQueryEngine:
    def __init__(self, entities: pd.DataFrame, aliases: pd.DataFrame):
        self._entities = entities
        self._aliases = aliases

    def sql(self, query: str) -> pd.DataFrame:
        return self._entities if query == _ENTITIES_SQL else self._aliases


class FakeLookup:
    """Espelha a superficie de EntityLookup que EntitySearch consome."""

    def __init__(
        self,
        entities: pd.DataFrame,
        aliases: pd.DataFrame,
        sources: dict[str, set[str]],
        situacao: dict[str, str],
    ):
        self.query_engine = FakeQueryEngine(entities, aliases)
        self._sources = sources
        self._situacao = situacao

    def _latest_cadastro_sql(
        self, inner_cols: str, outer_cols: str, extra_where: str
    ) -> str:
        return _ENTITIES_SQL

    def _source_path(self, name: str) -> str:
        return f"/fake/{name}/*.parquet"

    def resolved_entity_cnpj_expr(self) -> str:
        return "CNPJ_8"

    def _get_data_sources_for_cnpjs(
        self,
        cnpjs: list[str],
        date_range: tuple[int, int] | None = None,
    ) -> dict[str, set[str]]:
        return {c: self._sources[c] for c in cnpjs if c in self._sources}

    def _get_latest_situacao(self, cnpjs: list[str]) -> dict[str, str]:
        return {c: self._situacao[c] for c in cnpjs if c in self._situacao}


def make_search(
    entities: list[tuple[str, str, str]],
    aliases: list[tuple[str, str]] | None = None,
    sources: dict[str, set[str]] | None = None,
    threshold: int = 78,
) -> EntitySearch:
    """entities: (cnpj, nome, situacao). aliases default = os proprios nomes.

    Se `sources` e None, toda entidade ganha {'cosif'} para nao cair no filtro
    de fontes -- os testes de ranking nao querem esse ruido.
    """
    ents = pd.DataFrame(
        [{"CNPJ_8": c, "NOME": n, "NOME_NORM": n} for c, n, _ in entities],
        columns=["CNPJ_8", "NOME", "NOME_NORM"],
    )
    alias_pairs = aliases if aliases is not None else [(c, n) for c, n, _ in entities]
    alias_df = pd.DataFrame(
        [{"CNPJ_8": c, "NOME": n, "NOME_NORM": n} for c, n in alias_pairs],
        columns=["CNPJ_8", "NOME", "NOME_NORM"],
    )
    if sources is None:
        sources = {c: {"cosif"} for c, _, _ in entities}
    situacao = {c: s for c, _, s in entities}
    lookup = FakeLookup(ents, alias_df, sources, situacao)
    return EntitySearch(lookup, fuzzy_threshold_suggest=threshold)  # type: ignore[arg-type]


class TestDedup:
    def test_dois_aliases_do_mesmo_cnpj_viram_uma_linha(self) -> None:
        search = make_search(
            [("11111111", "BANCO ALFA", _ATIVA)],
            aliases=[("11111111", "BANCO ALFA"), ("11111111", "BANCO ALFA SA")],
        )
        df = search.search("BANCO ALFA")
        assert len(df) == 1
        assert df.loc[0, "cnpj_8"] == "11111111"
        assert df.loc[0, "score"] == 100


class TestRanking:
    def test_ativa_vem_antes_de_cancelada(self) -> None:
        search = make_search(
            [
                ("11111111", "BANCO TESTE A", _CANCELADA),
                ("22222222", "BANCO TESTE B", _ATIVA),
                ("33333333", "BANCO TESTE C", _ATIVA),
            ]
        )
        df = search.search("BANCO TESTE")
        # Os tres empatam no score (mesma distancia do termo); decide situacao
        # (ATIVA < CANCELADA) e, dentro do empate, nome em ordem alfabetica.
        assert list(df["cnpj_8"]) == ["22222222", "33333333", "11111111"]

    def test_score_maior_vence_dentro_da_mesma_situacao(self) -> None:
        search = make_search(
            [
                ("11111111", "BANCO TESTE", _ATIVA),
                ("22222222", "BANCO TESTA", _ATIVA),
            ]
        )
        df = search.search("BANCO TESTE")
        # Empate de nome favoreceria TESTA (alfabetico); o match exato (100)
        # tem que vir primeiro, provando que score desc domina o tiebreak.
        assert list(df["cnpj_8"]) == ["11111111", "22222222"]
        assert df.loc[0, "score"] > df.loc[1, "score"]

    def test_limit_corta_depois_da_ordenacao(self) -> None:
        search = make_search(
            [
                ("11111111", "BANCO TESTE A", _CANCELADA),
                ("22222222", "BANCO TESTE B", _ATIVA),
                ("33333333", "BANCO TESTE C", _ATIVA),
            ]
        )
        df = search.search("BANCO TESTE", limit=2)
        assert list(df["cnpj_8"]) == ["22222222", "33333333"]


class TestThreshold:
    def test_match_fraco_fica_fora(self) -> None:
        search = make_search([("11111111", "BANCO COMPLETAMENTE DIFERENTE", _ATIVA)])
        assert search.search("XYZ QWERTY").empty

    def test_threshold_baixo_deixa_o_mesmo_match_entrar(self) -> None:
        entities = [("11111111", "BANCO COMPLETAMENTE DIFERENTE", _ATIVA)]
        assert make_search(entities, threshold=1).search("XYZ QWERTY").empty is False

    def test_resultado_vazio_preserva_colunas(self) -> None:
        search = make_search([("11111111", "BANCO ALFA", _ATIVA)])
        df = search.search("XYZ QWERTY")
        assert list(df.columns) == [
            "cnpj_8",
            "instituicao",
            "situacao",
            "fontes",
            "score",
        ]


class TestExactCnpj:
    def test_cnpj_presente_retorna_score_100(self) -> None:
        search = make_search(
            [("60872504", "BANCO ALFA", _ATIVA)],
            sources={"60872504": {"ifdata", "cosif"}},
        )
        df = search.search("60872504")
        assert len(df) == 1
        assert df.loc[0, "score"] == 100
        assert df.loc[0, "fontes"] == "cosif,ifdata"

    def test_cnpj_presente_sem_fontes_retorna_vazio(self) -> None:
        search = make_search([("60872504", "BANCO ALFA", _ATIVA)], sources={})
        assert search.search("60872504").empty

    def test_cnpj_ausente_cai_no_fuzzy_e_nao_acha_nada(self) -> None:
        search = make_search([("11111111", "BANCO ALFA", _ATIVA)])
        assert search.search("99999999").empty


class TestFilaFontes:
    def test_com_date_range_entidade_sem_fonte_sai(self) -> None:
        search = make_search(
            [
                ("11111111", "BANCO TESTE A", _ATIVA),
                ("22222222", "BANCO TESTE B", _ATIVA),
            ],
            sources={"22222222": {"cosif"}},
        )
        df = search.search("BANCO TESTE", date_range=(202401, 202412))
        assert list(df["cnpj_8"]) == ["22222222"]

    def test_sem_date_range_e_sem_nenhuma_fonte_mantem_os_matches(self) -> None:
        """Cadastro coletado mas dados nao: filtrar esvaziaria tudo."""
        search = make_search(
            [
                ("11111111", "BANCO TESTE A", _ATIVA),
                ("22222222", "BANCO TESTE B", _ATIVA),
            ],
            sources={},
        )
        df = search.search("BANCO TESTE")
        assert len(df) == 2
        assert (df["fontes"] == "").all()


class TestInputs:
    def test_termo_vazio_retorna_vazio(self) -> None:
        search = make_search([("11111111", "BANCO ALFA", _ATIVA)])
        assert search.search("   ").empty

    def test_limit_invalido_levanta(self) -> None:
        search = make_search([("11111111", "BANCO ALFA", _ATIVA)])
        with pytest.raises(ValueError, match="limit"):
            search.search("BANCO", limit=0)

    def test_corpus_vazio_retorna_vazio(self) -> None:
        search = make_search([])
        assert search.search("BANCO").empty
