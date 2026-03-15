"""
Modulo para fuzzy matching de strings
"""

from typing import Dict, List, Tuple
from thefuzz import fuzz, process


class FuzzyMatcher:
    """
    Wrapper para logica de fuzzy matching usando thefuzz.

    Classe generica e reutilizavel para qualquer caso de uso que precise
    de correspondencia aproximada de strings.

    Single Responsibility: Logica de fuzzy matching.
    """

    def __init__(
        self,
        threshold_auto: int = 85,
        threshold_suggest: int = 70
    ):
        """
        Inicializa o fuzzy matcher.

        Args:
            threshold_auto: Score minimo para aceitar match automaticamente (padrao: 85)
            threshold_suggest: Score minimo para sugerir matches (padrao: 70)
        """
        self._threshold_auto = threshold_auto
        self._threshold_suggest = threshold_suggest

    @property
    def threshold_auto(self) -> int:
        """Score minimo para aceitar match automaticamente."""
        return self._threshold_auto

    @property
    def threshold_suggest(self) -> int:
        """Score minimo para sugerir matches."""
        return self._threshold_suggest

    def search(
        self,
        query: str,
        choices: Dict[str, str],
        limit: int = 5,
        score_cutoff: int = 0
    ) -> List[Tuple[str, int]]:
        """
        Realiza busca fuzzy e retorna matches ordenados por score.

        Args:
            query: String de busca
            choices: Dicionario {string_a_comparar: valor_associado}
            limit: Numero maximo de resultados (padrao: 5)
            score_cutoff: Score minimo para incluir resultado (padrao: 0)

        Returns:
            Lista de tuplas (chave, score) ordenada por score decrescente

        Example:
            >>> matcher = FuzzyMatcher()
            >>> choices = {"BANCO DO BRASIL": "00000000", "BANCO ITAU": "11111111"}
            >>> matcher.search("BANCO BRASIL", choices, limit=2)
            [("BANCO DO BRASIL", 95), ("BANCO ITAU", 72)]
        """
        matches = process.extract(
            query,
            choices.keys(),
            scorer=fuzz.token_set_ratio,
            limit=limit
        )
        return [(chave, score) for chave, score in matches if score >= score_cutoff]

    def get_best_match(
        self,
        query: str,
        choices: Dict[str, str]
    ) -> Tuple[str, int, bool]:
        """
        Retorna o melhor match com indicador de confianca.

        Args:
            query: String de busca
            choices: Dicionario {string_a_comparar: valor_associado}

        Returns:
            Tupla (melhor_chave, score, is_auto_accepted)
            - is_auto_accepted=True se score >= threshold_auto

        Example:
            >>> matcher = FuzzyMatcher(threshold_auto=85)
            >>> choices = {"BANCO DO BRASIL": "00000000"}
            >>> best, score, auto = matcher.get_best_match("BANCO BRASIL", choices)
            >>> print(best, score, auto)
            "BANCO DO BRASIL" 95 True
        """
        matches = self.search(query, choices, limit=1)

        if not matches:
            return ("", 0, False)

        best_match, best_score = matches[0]
        is_auto_accepted = best_score >= self._threshold_auto

        return (best_match, best_score, is_auto_accepted)

    def get_suggestions(
        self,
        query: str,
        choices: Dict[str, str],
        limit: int = 3
    ) -> List[Tuple[str, int]]:
        """
        Retorna sugestoes de matches acima do threshold_suggest.

        Args:
            query: String de busca
            choices: Dicionario {string_a_comparar: valor_associado}
            limit: Numero maximo de sugestoes (padrao: 3)

        Returns:
            Lista de tuplas (chave, score) com score >= threshold_suggest

        Example:
            >>> matcher = FuzzyMatcher(threshold_suggest=70)
            >>> choices = {"BANCO DO BRASIL": "00000000", "BANCO ITAU": "11111111"}
            >>> matcher.get_suggestions("BANCO BRASIL", choices)
            [("BANCO DO BRASIL", 95), ("BANCO ITAU", 72)]
        """
        return self.search(
            query,
            choices,
            limit=limit,
            score_cutoff=self._threshold_suggest
        )
