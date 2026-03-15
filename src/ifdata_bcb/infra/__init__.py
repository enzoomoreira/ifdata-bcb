"""
Camada de infraestrutura para ifdata-bcb.

Este modulo contem:
- cache: Sistema de cache centralizado com metricas
- config: Constantes e funcoes de configuracao de paths
- query: QueryEngine para queries DuckDB em arquivos Parquet
- storage: DataManager para persistencia de dados
- log: Configuracao de logging com loguru
- resilience: Retry com exponential backoff e jitter
"""

from ifdata_bcb.infra.cache import (
    CacheStats,
    cached,
    clear_all_caches,
    get_cache_info,
)
from ifdata_bcb.infra.config import get_cache_path
from ifdata_bcb.infra.log import configure_logging, get_log_path, get_logger, set_log_level
from ifdata_bcb.infra.query import QueryEngine
from ifdata_bcb.infra.resilience import (
    DEFAULT_BACKOFF_FACTOR,
    DEFAULT_REQUEST_TIMEOUT,
    DEFAULT_RETRY_ATTEMPTS,
    DEFAULT_RETRY_DELAY,
    TRANSIENT_EXCEPTIONS,
    retry,
)
from ifdata_bcb.infra.storage import DataManager

__all__ = [
    "CacheStats",
    "cached",
    "clear_all_caches",
    "get_cache_info",
    "get_cache_path",
    "QueryEngine",
    "DataManager",
    "configure_logging",
    "get_log_path",
    "get_logger",
    "set_log_level",
    "retry",
    "TRANSIENT_EXCEPTIONS",
    "DEFAULT_RETRY_ATTEMPTS",
    "DEFAULT_RETRY_DELAY",
    "DEFAULT_BACKOFF_FACTOR",
    "DEFAULT_REQUEST_TIMEOUT",
]
