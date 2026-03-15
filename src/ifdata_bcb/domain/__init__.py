"""
Modulo de dominio com classes base e excecoes.
"""

from ifdata_bcb.domain.exceptions import (
    AmbiguousIdentifierError,
    BacenAnalysisError,
    DataUnavailableError,
    EntityNotFoundError,
    InvalidIdentifierError,
    InvalidScopeError,
)
from ifdata_bcb.domain.explorers import BaseExplorer

__all__ = [
    "BaseExplorer",
    "AmbiguousIdentifierError",
    "BacenAnalysisError",
    "DataUnavailableError",
    "EntityNotFoundError",
    "InvalidIdentifierError",
    "InvalidScopeError",
]
