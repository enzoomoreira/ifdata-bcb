"""
Buscador de entidades (instituicoes financeiras) em todas as fontes de dados.

Este modulo implementa o padrao "search + select" para resolver ambiguidades
na identificacao de entidades, permitindo ao usuario:
1. Buscar por nome parcial
2. Visualizar opcoes com CNPJ, nome e fontes disponiveis
3. Selecionar o CNPJ correto para consultas subsequentes
"""

from typing import Optional

import pandas as pd

from ifdata_bcb.utils.fuzzy_matcher import FuzzyMatcher
from ifdata_bcb.utils.text_utils import normalize_accents
from ifdata_bcb.infra.log import get_logger
from ifdata_bcb.infra.query import QueryEngine


class EntitySearcher:
    """
    Busca entidades por nome em todas as fontes de dados do BCB.

    Agrega dados de:
    - Cadastro IFDATA (fonte primaria)
    - COSIF Individual
    - COSIF Prudencial

    Exemplo:
        searcher = EntitySearcher()
        results = searcher.search('Itau Unibanco')
        print(results)
        #    CNPJ_8              INSTITUICAO                          FONTES  SCORE
        # 0  60872504  ITAU UNIBANCO HOLDING S.A.  cadastro,cosif_ind,cosif_prud    100
    """

    # Subdiretorios das fontes de dados
    _CADASTRO_SUBDIR = "ifdata/cadastro"
    _COSIF_IND_SUBDIR = "cosif/individual"
    _COSIF_PRUD_SUBDIR = "cosif/prudencial"

    def __init__(
        self,
        query_engine: Optional[QueryEngine] = None,
        fuzzy_threshold: int = 50,
    ):
        """
        Inicializa o buscador de entidades.

        Args:
            query_engine: QueryEngine customizado. Se None, cria um novo.
            fuzzy_threshold: Score minimo para incluir resultado (padrao: 50).
        """
        self._qe = query_engine or QueryEngine()
        self._fuzzy_threshold = fuzzy_threshold
        self._logger = get_logger(__name__)

        self._fuzzy_matcher = FuzzyMatcher(
            threshold_auto=85,
            threshold_suggest=fuzzy_threshold,
        )

        # Cache do mapeamento completo (carregado sob demanda)
        self._entity_data: Optional[dict[str, dict]] = None

    def _load_entity_data(self) -> dict[str, dict]:
        """
        Carrega dados de entidades de todas as fontes usando DuckDB.

        Retorna dicionario onde chave e CNPJ_8 e valor contem:
        - nome: Nome da instituicao
        - fontes: Set de fontes onde aparece

        Nota: Usa nomes de storage nas queries e renomeia para uso interno.

        Returns:
            Dicionario {cnpj_8: {nome: str, fontes: set[str]}}
        """
        entities: dict[str, dict] = {}

        def add_from_df(df: pd.DataFrame, fonte: str, nome_col: str) -> None:
            """Adiciona entidades do DataFrame ao mapeamento."""
            if df.empty:
                return
            df_unique = df.drop_duplicates(subset=["CNPJ_8"])
            for i in range(len(df_unique)):
                nome = df_unique[nome_col].iloc[i]
                cnpj = df_unique["CNPJ_8"].iloc[i]
                if pd.notna(cnpj):
                    cnpj_str = str(cnpj)
                    if cnpj_str not in entities:
                        entities[cnpj_str] = {
                            "nome": str(nome) if pd.notna(nome) else "",
                            "fontes": set(),
                        }
                    entities[cnpj_str]["fontes"].add(fonte)
                    # Atualiza nome se vazio e temos um nome valido
                    if not entities[cnpj_str]["nome"] and pd.notna(nome):
                        entities[cnpj_str]["nome"] = str(nome)

        # 1. Cadastro IFDATA (fonte primaria)
        # Storage: NomeInstituicao
        df_cad = self._qe.read_glob(
            "ifdata_cad_*.parquet",
            self._CADASTRO_SUBDIR,
            columns=["CNPJ_8", "NomeInstituicao"],
        )
        add_from_df(df_cad, "cadastro", "NomeInstituicao")

        # 2. COSIF Individual
        # Storage: NOME_INSTITUICAO
        df_cosif_ind = self._qe.read_glob(
            "cosif_ind_*.parquet",
            self._COSIF_IND_SUBDIR,
            columns=["CNPJ_8", "NOME_INSTITUICAO"],
        )
        add_from_df(df_cosif_ind, "cosif_ind", "NOME_INSTITUICAO")

        # 3. COSIF Prudencial
        # Storage: NOME_INSTITUICAO
        df_cosif_prud = self._qe.read_glob(
            "cosif_prud_*.parquet",
            self._COSIF_PRUD_SUBDIR,
            columns=["CNPJ_8", "NOME_INSTITUICAO"],
        )
        add_from_df(df_cosif_prud, "cosif_prud", "NOME_INSTITUICAO")

        self._logger.debug(f"Entidades carregadas: {len(entities)}")
        return entities

    def _get_entity_data(self) -> dict[str, dict]:
        """Obtem dados de entidades (lazy loading)."""
        if self._entity_data is None:
            self._entity_data = self._load_entity_data()
        return self._entity_data

    def search(self, termo: str, limit: int = 10) -> pd.DataFrame:
        """
        Busca entidades por nome em todas as fontes de dados.

        Args:
            termo: Nome da instituicao (parcial ou completo).
            limit: Numero maximo de resultados (padrao: 10).

        Returns:
            DataFrame com colunas:
            - CNPJ_8: CNPJ de 8 digitos
            - INSTITUICAO: Nome da instituicao
            - FONTES: String com fontes separadas por virgula
            - SCORE: Score de similaridade (0-100)

        Exemplo:
            >>> searcher.search('Itau Unibanco')
               CNPJ_8              INSTITUICAO                          FONTES  SCORE
            0  60872504  ITAU UNIBANCO HOLDING S.A.  cadastro,cosif_ind,cosif_prud    100

            >>> searcher.search('Banco do Brasil')
               CNPJ_8          INSTITUICAO                          FONTES  SCORE
            0  00000000  BANCO DO BRASIL S.A.  cadastro,cosif_ind,cosif_prud    100
        """
        entities = self._get_entity_data()

        if not entities:
            return pd.DataFrame(columns=["CNPJ_8", "INSTITUICAO", "FONTES", "SCORE"])

        # Criar mapeamento nome -> cnpj para fuzzy matching
        # Se houver nomes duplicados, usa o primeiro CNPJ encontrado
        # Usamos versao normalizada (sem acentos) como chave para matching
        nome_to_cnpj: dict[str, str] = {}
        # Mapeamento para preservar nome original para exibicao
        nome_normalized_to_original: dict[str, str] = {}

        for cnpj, data in entities.items():
            nome_upper = data["nome"].upper()
            nome_normalized = normalize_accents(nome_upper)
            if nome_normalized and nome_normalized not in nome_to_cnpj:
                nome_to_cnpj[nome_normalized] = cnpj
                nome_normalized_to_original[nome_normalized] = nome_upper

        # Busca fuzzy - normalizar termo de busca tambem
        termo_upper = normalize_accents(termo.strip().upper())
        matches = self._fuzzy_matcher.search(
            query=termo_upper,
            choices=nome_to_cnpj,
            limit=limit,
            score_cutoff=self._fuzzy_threshold,
        )

        # Montar resultado
        results = []
        seen_cnpjs = set()  # Evitar duplicatas se houver

        for nome, score in matches:
            cnpj = nome_to_cnpj[nome]
            if cnpj in seen_cnpjs:
                continue
            seen_cnpjs.add(cnpj)

            entity_data = entities[cnpj]
            fontes_str = ",".join(sorted(entity_data["fontes"]))

            results.append({
                "CNPJ_8": cnpj,
                "INSTITUICAO": entity_data["nome"],
                "FONTES": fontes_str,
                "SCORE": score,
            })

        df = pd.DataFrame(results)

        # Garantir ordem das colunas mesmo se vazio
        if df.empty:
            return pd.DataFrame(columns=["CNPJ_8", "INSTITUICAO", "FONTES", "SCORE"])

        return df[["CNPJ_8", "INSTITUICAO", "FONTES", "SCORE"]]

    def list_all(self, limit: int = 100) -> pd.DataFrame:
        """
        Lista todas as entidades disponiveis.

        Args:
            limit: Numero maximo de resultados (padrao: 100).

        Returns:
            DataFrame com colunas CNPJ_8, INSTITUICAO, FONTES (sem SCORE).
        """
        entities = self._get_entity_data()

        results = []
        for cnpj, data in list(entities.items())[:limit]:
            fontes_str = ",".join(sorted(data["fontes"]))
            results.append({
                "CNPJ_8": cnpj,
                "INSTITUICAO": data["nome"],
                "FONTES": fontes_str,
            })

        df = pd.DataFrame(results)

        if df.empty:
            return pd.DataFrame(columns=["CNPJ_8", "INSTITUICAO", "FONTES"])

        return df[["CNPJ_8", "INSTITUICAO", "FONTES"]]

    def clear_cache(self) -> None:
        """Limpa o cache de entidades."""
        self._entity_data = None
