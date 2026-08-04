from collections.abc import Iterable

from thefuzz import fuzz, process

from ifdata_bcb.infra.config import get_settings


class FuzzyMatcher:
    def __init__(self, threshold_suggest: int | None = None):
        self.threshold_suggest = (
            threshold_suggest
            if threshold_suggest is not None
            else get_settings().fuzzy_threshold
        )

    def search(
        self,
        query: str,
        choices: Iterable[str],
        score_cutoff: int = 0,
    ) -> list[tuple[str, int]]:
        """Retorna [(escolha, score)] ordenado por score desc, depois alfabetico."""
        # Materializa como lista: com um dict, extractBests devolve triplas
        # (valor, score, chave) em vez de pares, quebrando quem consome.
        matches = process.extractBests(
            query,
            list(choices),
            scorer=fuzz.token_set_ratio,
            score_cutoff=score_cutoff,
            limit=None,
        )
        matches.sort(key=lambda x: (-x[1], x[0]))
        return matches
