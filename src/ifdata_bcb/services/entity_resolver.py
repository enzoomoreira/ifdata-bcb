"""
Resolvedor de identificadores de entidades (CNPJ/nomes) usando DuckDB.

Este modulo fornece resolucao de nomes/CNPJs para identificadores canonicos
usando queries DuckDB sobre arquivos Parquet, sem carregar dados em memoria.
"""

import re
from dataclasses import dataclass
from typing import Optional

import pandas as pd

from ifdata_bcb.utils.fuzzy_matcher import FuzzyMatcher
from ifdata_bcb.domain.exceptions import (
    AmbiguousIdentifierError,
    DataUnavailableError,
    EntityNotFoundError,
    InvalidScopeError,
)
from ifdata_bcb.infra.cache import cached
from ifdata_bcb.infra.log import get_logger
from ifdata_bcb.infra.query import QueryEngine


@dataclass(frozen=True)
class ScopeResolution:
    """
    Resultado da resolucao de escopo para IFDATA.

    Attributes:
        cod_inst: Codigo a usar na query (CNPJ_8 ou COD_CONGL_*)
        tipo_inst: Tipo de instituicao para filtrar (1, 2, ou 3)
        cnpj_original: CNPJ_8 original do usuario (para merge)
        escopo: Escopo resolvido
    """

    cod_inst: str
    tipo_inst: int
    cnpj_original: str
    escopo: str


@dataclass(frozen=True)
class ResolvedEntity:
    """
    Objeto imutavel contendo todos os identificadores resolvidos de uma entidade.

    Uso de dataclass frozen=True permite hashability para @lru_cache e
    garante imutabilidade (thread-safe, sem side effects).

    Attributes:
        cnpj_interesse: CNPJ de 8 digitos da entidade de interesse
        cnpj_reporte_cosif: CNPJ a usar para busca COSIF (pode ser do lider)
        cod_congl_prud: Codigo do conglomerado prudencial
        nome_entidade: Nome canonico da entidade
        identificador_original: Identificador usado na busca original (para debug)
    """

    cnpj_interesse: Optional[str]
    cnpj_reporte_cosif: Optional[str]
    cod_congl_prud: Optional[str]
    nome_entidade: Optional[str]
    identificador_original: str


class EntityResolver:
    """
    Resolve identificadores (nomes/CNPJs) em CNPJs canonicos e metadados.

    Usa DuckDB para queries eficientes sobre arquivos Parquet, evitando
    carregar todos os dados em memoria.

    Fontes de dados:
    - IFDATA Cadastro: Dados cadastrais completos (conglomerado, lider, etc.)
    - COSIF Individual/Prudencial: Nomes de instituicoes adicionais

    Exemplo:
        resolver = EntityResolver()

        # Resolver nome para CNPJ
        cnpj = resolver.find_cnpj('Itau')

        # Resolver completamente
        entity = resolver.resolve_full('Bradesco')
        print(entity.cnpj_interesse)
        print(entity.nome_entidade)
    """

    # Subdiretorios dos dados
    _CADASTRO_SUBDIR = "ifdata/cadastro"
    _COSIF_IND_SUBDIR = "cosif/individual"
    _COSIF_PRUD_SUBDIR = "cosif/prudencial"

    def __init__(
        self,
        query_engine: Optional[QueryEngine] = None,
        enable_fuzzy: bool = True,
        fuzzy_threshold_auto: int = 85,
        fuzzy_threshold_suggest: int = 70,
    ):
        """
        Inicializa o resolvedor de entidades.

        Args:
            query_engine: QueryEngine customizado. Se None, cria um novo.
            enable_fuzzy: Habilita fuzzy matching como fallback (padrao: True).
            fuzzy_threshold_auto: Score minimo para aceitar automaticamente (padrao: 85).
            fuzzy_threshold_suggest: Score minimo para sugerir matches (padrao: 70).
        """
        self._qe = query_engine or QueryEngine()
        self._enable_fuzzy = enable_fuzzy
        self._logger = get_logger(__name__)

        self._fuzzy_matcher = FuzzyMatcher(
            threshold_auto=fuzzy_threshold_auto,
            threshold_suggest=fuzzy_threshold_suggest,
        )

        # Cache do mapeamento nome -> cnpj (carregado sob demanda)
        self._name_mapping: Optional[dict[str, str]] = None

    def _load_name_mapping(self) -> dict[str, str]:
        """
        Carrega mapeamento de nomes para CNPJs usando DuckDB.

        Extrai nomes de todas as fontes disponiveis (cadastro, COSIF).
        Usa nomes de storage nas queries.

        Returns:
            Dicionario {nome_upper: cnpj_8}
        """
        mapping: dict[str, str] = {}

        def add_from_df(df: pd.DataFrame, nome_col: str, overwrite: bool = True) -> None:
            """Adiciona nomes do DataFrame ao mapeamento."""
            if df.empty:
                return
            df_unique = df.drop_duplicates(subset=[nome_col])
            for i in range(len(df_unique)):
                nome = df_unique[nome_col].iloc[i]
                cnpj = df_unique["CNPJ_8"].iloc[i]
                if pd.notna(nome) and pd.notna(cnpj):
                    nome_upper = str(nome).strip().upper()
                    if overwrite or nome_upper not in mapping:
                        mapping[nome_upper] = str(cnpj)

        # 1. Cadastro IFDATA (fonte primaria)
        # Storage: NomeInstituicao
        df_cad = self._qe.read_glob(
            "ifdata_cad_*.parquet",
            self._CADASTRO_SUBDIR,
            columns=["CNPJ_8", "NomeInstituicao"],
        )
        add_from_df(df_cad, "NomeInstituicao", overwrite=True)

        # 2. COSIF Individual (nomes adicionais, nao sobrescreve)
        # Storage: NOME_INSTITUICAO
        df_cosif_ind = self._qe.read_glob(
            "cosif_ind_*.parquet",
            self._COSIF_IND_SUBDIR,
            columns=["CNPJ_8", "NOME_INSTITUICAO"],
        )
        add_from_df(df_cosif_ind, "NOME_INSTITUICAO", overwrite=False)

        # 3. COSIF Prudencial (nomes adicionais, nao sobrescreve)
        # Storage: NOME_INSTITUICAO
        df_cosif_prud = self._qe.read_glob(
            "cosif_prud_*.parquet",
            self._COSIF_PRUD_SUBDIR,
            columns=["CNPJ_8", "NOME_INSTITUICAO"],
        )
        add_from_df(df_cosif_prud, "NOME_INSTITUICAO", overwrite=False)

        self._logger.debug(f"Mapeamento carregado: {len(mapping)} entidades")
        return mapping

    def _get_name_mapping(self) -> dict[str, str]:
        """Obtem mapeamento de nomes (lazy loading)."""
        if self._name_mapping is None:
            self._name_mapping = self._load_name_mapping()
        return self._name_mapping

    @cached(maxsize=256)
    def find_cnpj(self, identificador: str) -> str:
        """
        Encontra o CNPJ_8 a partir de um nome ou de um CNPJ.

        Args:
            identificador: Nome da instituicao ou CNPJ de 8 digitos.

        Returns:
            CNPJ de 8 digitos.

        Raises:
            EntityNotFoundError: Se o identificador nao for encontrado.
            AmbiguousIdentifierError: Se o identificador for ambiguo.

        Exemplo:
            cnpj = resolver.find_cnpj('60872504')  # Retorna '60872504'
            cnpj = resolver.find_cnpj('00000000')  # Retorna '00000000' (Banco do Brasil)
        """
        identificador = identificador.strip()
        identificador_upper = identificador.upper()

        # Se ja e um CNPJ de 8 digitos, retorna diretamente
        if re.fullmatch(r"\d{8}", identificador):
            return identificador

        mapping = self._get_name_mapping()

        # Busca exata
        if identificador_upper in mapping:
            return mapping[identificador_upper]

        # Busca parcial (contains)
        matches_contains = [
            (nome, cnpj)
            for nome, cnpj in mapping.items()
            if identificador_upper in nome
        ]

        if len(matches_contains) == 1:
            return matches_contains[0][1]
        elif len(matches_contains) > 1:
            nomes_encontrados = [nome for nome, _ in matches_contains[:5]]
            raise AmbiguousIdentifierError(
                identifier=identificador,
                matches=nomes_encontrados,
                suggestion="Use um nome mais completo ou o CNPJ de 8 digitos",
            )

        # Fuzzy matching (se habilitado)
        if self._enable_fuzzy:
            # Formato para fuzzy matcher: {nome: cnpj}
            matches = self._fuzzy_matcher.search(
                query=identificador_upper,
                choices=mapping,
                limit=5,
            )

            if matches:
                best_match, best_score = matches[0]

                # Auto-aceita se score >= threshold_auto
                if best_score >= self._fuzzy_matcher.threshold_auto:
                    return mapping[best_match]

                # Sugere top matches se score >= threshold_suggest
                if best_score >= self._fuzzy_matcher.threshold_suggest:
                    suggestions_with_scores = [
                        f"{nome} (similaridade: {score}%)"
                        for nome, score in matches[:3]
                    ]
                    raise AmbiguousIdentifierError(
                        identifier=identificador,
                        matches=suggestions_with_scores,
                        suggestion="Foram encontradas correspondencias aproximadas. "
                        "Use um nome mais especifico ou o CNPJ de 8 digitos",
                    )

        # Nao encontrado
        suggestions = [
            "Verifique se o nome ou CNPJ esta correto",
            "Use o CNPJ de 8 digitos para maior precisao",
        ]

        # Adiciona sugestoes fuzzy se habilitado
        if self._enable_fuzzy:
            fuzzy_suggestions = self._fuzzy_matcher.search(
                query=identificador_upper,
                choices=mapping,
                limit=3,
                score_cutoff=50,
            )
            if fuzzy_suggestions:
                suggestions.append("\nSugestoes de nomes similares:")
                for nome, score in fuzzy_suggestions:
                    suggestions.append(f"  - {nome} (similaridade: {score}%)")

        raise EntityNotFoundError(identifier=identificador, suggestions=suggestions)

    def find_cnpj_fuzzy(
        self,
        identificador: str,
        limit: int = 5,
        score_cutoff: int = 70,
    ) -> list[tuple[str, str, int]]:
        """
        Busca fuzzy explicita retornando multiplas correspondencias.

        Util para interfaces que querem mostrar opcoes ao usuario.

        Args:
            identificador: Nome da instituicao ou CNPJ.
            limit: Numero maximo de resultados (padrao: 5).
            score_cutoff: Score minimo para incluir resultado (padrao: 70).

        Returns:
            Lista de tuplas (nome, cnpj, score) ordenada por score decrescente.

        Exemplo:
            matches = resolver.find_cnpj_fuzzy('Banco Brasil', limit=3)
            # [('BANCO DO BRASIL S.A.', '00000000', 95), ...]
        """
        identificador_upper = identificador.strip().upper()
        mapping = self._get_name_mapping()

        # Se ja e um CNPJ de 8 digitos, busca o nome correspondente
        if re.fullmatch(r"\d{8}", identificador):
            for nome, cnpj in mapping.items():
                if cnpj == identificador:
                    return [(nome, identificador, 100)]
            return []

        # Busca fuzzy
        matches = self._fuzzy_matcher.search(
            query=identificador_upper,
            choices=mapping,
            limit=limit,
            score_cutoff=score_cutoff,
        )

        return [(nome, mapping[nome], score) for nome, score in matches]

    def _get_cadastro_info(self, cnpj_8: str) -> dict[str, Optional[str]]:
        """
        Busca informacoes cadastrais de uma entidade via DuckDB.

        Usa nomes de storage nas queries.

        Args:
            cnpj_8: CNPJ de 8 digitos.

        Returns:
            Dicionario com informacoes cadastrais.
        """
        # Query mais recente para o CNPJ
        # Storage names: Data, NomeInstituicao, CodConglomeradoPrudencial,
        #               CodConglomeradoFinanceiro, CNPJ_LIDER_8
        df = self._qe.read_glob(
            "ifdata_cad_*.parquet",
            self._CADASTRO_SUBDIR,
            columns=[
                "Data",
                "CNPJ_8",
                "NomeInstituicao",
                "CodConglomeradoPrudencial",
                "CodConglomeradoFinanceiro",
                "CNPJ_LIDER_8",
            ],
            where=f"CNPJ_8 = '{cnpj_8}'",
        )

        if df.empty:
            return {
                "nome_entidade": None,
                "cod_congl_prud": None,
                "cod_congl_fin": None,
                "cnpj_reporte_cosif": cnpj_8,
            }

        # Pegar registro mais recente
        df_sorted = df.sort_values(by="Data", ascending=False)

        # Extrair valores do primeiro registro (usando nomes de storage)
        nome_val = df_sorted["NomeInstituicao"].iloc[0]
        cod_congl_prud_val = df_sorted["CodConglomeradoPrudencial"].iloc[0]
        cod_congl_fin_val = df_sorted["CodConglomeradoFinanceiro"].iloc[0]

        result: dict[str, Optional[str]] = {
            "nome_entidade": str(nome_val) if pd.notna(nome_val) else None,
            "cod_congl_prud": None,
            "cod_congl_fin": None,
            "cnpj_reporte_cosif": cnpj_8,
        }

        if pd.notna(cod_congl_prud_val):
            result["cod_congl_prud"] = str(cod_congl_prud_val)

            # Buscar lider do conglomerado (usando nomes de storage)
            df_congl = self._qe.read_glob(
                "ifdata_cad_*.parquet",
                self._CADASTRO_SUBDIR,
                columns=["Data", "CNPJ_8", "CodConglomeradoPrudencial", "CNPJ_LIDER_8"],
                where=f"CodConglomeradoPrudencial = '{cod_congl_prud_val}'",
            )

            if not df_congl.empty:
                # Pegar CNPJ lider mais recente (remover linhas sem lider)
                df_lideres = df_congl[df_congl["CNPJ_LIDER_8"].notna()]
                if not df_lideres.empty:
                    df_lideres_sorted = df_lideres.sort_values(
                        by="Data", ascending=False
                    )
                    cnpj_lider = df_lideres_sorted["CNPJ_LIDER_8"].iloc[0]
                    if pd.notna(cnpj_lider):
                        result["cnpj_reporte_cosif"] = str(cnpj_lider)

        if pd.notna(cod_congl_fin_val):
            result["cod_congl_fin"] = str(cod_congl_fin_val)

        return result

    @cached(maxsize=256)
    def get_entity_identifiers(self, cnpj_8: str) -> dict[str, Optional[str]]:
        """
        Obtem metadados completos da entidade a partir do CNPJ.

        Args:
            cnpj_8: CNPJ de 8 digitos.

        Returns:
            Dicionario com metadados:
            - 'cnpj_interesse': CNPJ da entidade de interesse
            - 'cnpj_reporte_cosif': CNPJ a usar para busca COSIF (pode ser lider)
            - 'cod_congl_prud': Codigo do conglomerado prudencial
            - 'cod_congl_fin': Codigo do conglomerado financeiro
            - 'nome_entidade': Nome canonico da entidade
        """
        if not cnpj_8:
            return {
                "cnpj_interesse": cnpj_8,
                "cnpj_reporte_cosif": cnpj_8,
                "cod_congl_prud": None,
                "cod_congl_fin": None,
                "nome_entidade": None,
            }

        info = self._get_cadastro_info(cnpj_8)

        return {
            "cnpj_interesse": cnpj_8,
            "cnpj_reporte_cosif": info.get("cnpj_reporte_cosif", cnpj_8),
            "cod_congl_prud": info.get("cod_congl_prud"),
            "cod_congl_fin": info.get("cod_congl_fin"),
            "nome_entidade": info.get("nome_entidade"),
        }

    def get_names_for_cnpjs(self, cnpjs: list[str]) -> dict[str, str]:
        """
        Obtem nomes de instituicoes para uma lista de CNPJs.

        Eficiente para lookup em lote, invertendo o mapeamento nome->cnpj.

        Args:
            cnpjs: Lista de CNPJs de 8 digitos.

        Returns:
            Dicionario {cnpj: nome_instituicao}. CNPJs nao encontrados
            terao string vazia como valor.

        Exemplo:
            nomes = resolver.get_names_for_cnpjs(['60872504', '00000000'])
            # {'60872504': 'ITAU UNIBANCO HOLDING S.A.', '00000000': 'BANCO DO BRASIL S.A.'}
        """
        mapping = self._get_name_mapping()

        # Inverter mapeamento: {nome_upper: cnpj} -> {cnpj: nome}
        # Como podem haver multiplos nomes para o mesmo CNPJ, pegamos o primeiro encontrado
        cnpj_to_name: dict[str, str] = {}
        for nome, cnpj in mapping.items():
            if cnpj not in cnpj_to_name:
                cnpj_to_name[cnpj] = nome

        return {cnpj: cnpj_to_name.get(cnpj, "") for cnpj in cnpjs}

    def resolve_ifdata_scope(self, cnpj_8: str, escopo: str) -> ScopeResolution:
        """
        Resolve CNPJ para codigo IFDATA baseado no escopo.

        Args:
            cnpj_8: CNPJ de 8 digitos da entidade.
            escopo: 'individual', 'prudencial', ou 'financeiro'.

        Returns:
            ScopeResolution com cod_inst e tipo_inst apropriados.

        Raises:
            DataUnavailableError: Se entidade nao tem dados para o escopo.
            InvalidScopeError: Se escopo invalido.
        """
        escopo_lower = escopo.lower()

        if escopo_lower == "individual":
            return ScopeResolution(
                cod_inst=cnpj_8,
                tipo_inst=3,
                cnpj_original=cnpj_8,
                escopo="individual",
            )

        # Buscar informacoes cadastrais (ja tem cache via get_entity_identifiers)
        info = self.get_entity_identifiers(cnpj_8)

        if escopo_lower == "prudencial":
            cod_congl = info.get("cod_congl_prud")
            if not cod_congl:
                raise DataUnavailableError(
                    entity=cnpj_8,
                    scope_type="prudencial",
                    reason="Entidade nao pertence a um conglomerado prudencial.",
                    suggestions=["Use escopo='individual' para dados individuais."],
                )
            return ScopeResolution(
                cod_inst=cod_congl,
                tipo_inst=1,
                cnpj_original=cnpj_8,
                escopo="prudencial",
            )

        if escopo_lower == "financeiro":
            cod_congl = info.get("cod_congl_fin")
            if not cod_congl:
                raise DataUnavailableError(
                    entity=cnpj_8,
                    scope_type="financeiro",
                    reason="Entidade nao pertence a um conglomerado financeiro.",
                    suggestions=["Use escopo='prudencial' ou 'individual'."],
                )
            return ScopeResolution(
                cod_inst=cod_congl,
                tipo_inst=2,
                cnpj_original=cnpj_8,
                escopo="financeiro",
            )

        raise InvalidScopeError(
            scope_name="escopo",
            value=escopo,
            valid_values=["individual", "prudencial", "financeiro"],
        )

    @cached(maxsize=256)
    def resolve_full(self, identificador: str) -> ResolvedEntity:
        """
        Resolve completamente um identificador em uma unica operacao.

        Este metodo combina find_cnpj() e get_entity_identifiers() para
        evitar chamadas duplicadas.

        Args:
            identificador: Nome da instituicao ou CNPJ de 8 digitos.

        Returns:
            ResolvedEntity com todos os identificadores e metadados.

        Raises:
            EntityNotFoundError: Se o identificador nao for encontrado.
            AmbiguousIdentifierError: Se o identificador for ambiguo.

        Exemplo:
            entity = resolver.resolve_full('60872504')
            print(entity.cnpj_interesse)  # '60872504'
            print(entity.nome_entidade)   # 'ITAÚ UNIBANCO HOLDING S.A.'
        """
        cnpj_8 = self.find_cnpj(identificador)
        info = self.get_entity_identifiers(cnpj_8)

        return ResolvedEntity(
            cnpj_interesse=cnpj_8,
            cnpj_reporte_cosif=info.get("cnpj_reporte_cosif", cnpj_8),
            cod_congl_prud=info.get("cod_congl_prud"),
            nome_entidade=info.get("nome_entidade"),
            identificador_original=identificador,
        )

    def clear_cache(self) -> None:
        """Limpa os caches LRU e o mapeamento em memoria."""
        self.find_cnpj.cache_clear()
        self.get_entity_identifiers.cache_clear()
        self.resolve_full.cache_clear()
        self._name_mapping = None

    def reload_mapping(self) -> None:
        """Forca recriacao do mapeamento de nomes."""
        self._name_mapping = None
        self.clear_cache()
        self._name_mapping = self._load_name_mapping()
