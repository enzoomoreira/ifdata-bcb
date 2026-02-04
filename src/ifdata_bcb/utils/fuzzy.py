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
        matches = process.extract(
            query,
            choices.keys(),
            scorer=fuzz.token_set_ratio,
            limit=limit,
        )
        return [(chave, score) for chave, score in matches if score >= score_cutoff]
