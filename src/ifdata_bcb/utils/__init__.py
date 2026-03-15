"""
Utilitarios genericos para ifdata-bcb.
"""

from ifdata_bcb.utils.cnpj import standardize_cnpj_base8
from ifdata_bcb.utils.fuzzy_matcher import FuzzyMatcher
from ifdata_bcb.utils.text_utils import normalize_text

__all__ = ["standardize_cnpj_base8", "FuzzyMatcher", "normalize_text"]
