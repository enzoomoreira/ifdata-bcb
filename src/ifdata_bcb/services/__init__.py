"""
Servicos de aplicacao para ifdata-bcb.

Contem componentes transversais de orquestracao:
- BaseCollector: Classe base para coleta de dados
- EntityResolver: Resolucao de nomes/CNPJs
- EntitySearcher: Busca fuzzy de entidades
"""

from ifdata_bcb.services.base_collector import BaseCollector
from ifdata_bcb.services.entity_resolver import EntityResolver, ResolvedEntity
from ifdata_bcb.services.entity_searcher import EntitySearcher

__all__ = [
    "BaseCollector",
    "EntityResolver",
    "ResolvedEntity",
    "EntitySearcher",
]
