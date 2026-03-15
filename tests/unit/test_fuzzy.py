"""Testes unitarios -- FuzzyMatcher."""

from ifdata_bcb.utils.fuzzy import FuzzyMatcher


class TestFuzzyMatcher:
    def test_does_not_match_nubank_to_andbank(self) -> None:
        matcher = FuzzyMatcher()
        choices = {"ANDBANK": "cnpj1", "BANCO ANDBANK (BRASIL) S.A.": "cnpj2"}
        results = matcher.search(
            "NUBANK", choices, score_cutoff=matcher.threshold_suggest
        )
        assert not results
