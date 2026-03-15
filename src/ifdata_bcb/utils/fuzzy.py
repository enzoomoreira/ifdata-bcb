from thefuzz import fuzz, process


class FuzzyMatcher:
    def __init__(self, threshold_suggest: int = 78):
        self.threshold_suggest = threshold_suggest

    def search(
        self,
        query: str,
        choices: dict[str, str],
        score_cutoff: int = 0,
    ) -> list[tuple[str, int]]:
        matches = process.extractBests(
            query,
            choices.keys(),
            scorer=fuzz.token_set_ratio,
            score_cutoff=score_cutoff,
            limit=None,
        )
        filtered = [(chave, score) for chave, score in matches]
        filtered.sort(key=lambda x: (-x[1], x[0]))
        return filtered
