"""Funcoes de alto nivel para a API publica."""

import pandas as pd

# Singleton para lazy loading
_lookup = None


def search(termo: str, limit: int = 10) -> pd.DataFrame:
    """
    Busca instituicoes por nome em todas as fontes de dados.

    Use esta funcao para encontrar o CNPJ de 8 digitos de uma instituicao
    antes de fazer consultas com bcb.cosif.read(), bcb.ifdata.read(), etc.

    Retorna DataFrame com: CNPJ_8, INSTITUICAO, FONTES, SCORE.
    """
    global _lookup
    if _lookup is None:
        from ifdata_bcb.core.entity_lookup import EntityLookup

        _lookup = EntityLookup()
    return _lookup.search(termo, limit=limit)
