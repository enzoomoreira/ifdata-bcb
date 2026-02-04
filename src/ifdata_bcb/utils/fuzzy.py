from thefuzz import fuzz, process


class FuzzyMatcher:
    def __init__(
        self,
        threshold_auto: int = 85,
        threshold_suggest: int = 70,
    ):
        self.threshold_auto = threshold_auto
        self.threshold_suggest = threshold_suggest

    def search(
        self,
        query: str,
        choices: dict[str, str],
        limit: int = 5,
        score_cutoff: int = 0,
    ) -> list[tuple[str, int]]:
        # Busca mais resultados para garantir que pegamos todos com mesmo score
        # antes de aplicar limit, garantindo resultado determinístico
        matches = process.extract(
            query,
            choices.keys(),
            scorer=fuzz.token_set_ratio,
            limit=None,  # Pega todos
        )
        # Filtra por score_cutoff e ordena: score desc, nome asc (determinístico)
        filtered = [(chave, score) for chave, score in matches if score >= score_cutoff]
        filtered.sort(key=lambda x: (-x[1], x[0]))
        return filtered[:limit]
