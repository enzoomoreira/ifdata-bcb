"""
Sistema de cache centralizado para a biblioteca.

Fornece um decorator que wrapa lru_cache com:
- Registro global de caches para clear_all
- Metricas opcionais de hit/miss
- Configuracao centralizada
"""

from functools import lru_cache, wraps
from typing import Callable, Optional
import threading

# Registro global de caches (para clear_all)
_registered_caches: list[Callable] = []
_lock = threading.Lock()


class CacheStats:
    """Estatisticas de cache (opcional, para debug). Thread-safe."""

    _stats: dict[str, dict[str, int]] = {}
    _stats_lock = threading.Lock()

    @classmethod
    def record_hit(cls, name: str) -> None:
        """Registra um cache hit."""
        with cls._stats_lock:
            if name not in cls._stats:
                cls._stats[name] = {"hits": 0, "misses": 0}
            cls._stats[name]["hits"] += 1

    @classmethod
    def record_miss(cls, name: str) -> None:
        """Registra um cache miss."""
        with cls._stats_lock:
            if name not in cls._stats:
                cls._stats[name] = {"hits": 0, "misses": 0}
            cls._stats[name]["misses"] += 1

    @classmethod
    def get_stats(cls) -> dict[str, dict]:
        """
        Retorna estatisticas de todos os caches.

        Returns:
            Dicionario com {nome: {hits, misses, total, hit_rate}} para cada cache.
        """
        with cls._stats_lock:
            result = {}
            for name, stats in cls._stats.items():
                total = stats["hits"] + stats["misses"]
                hit_rate = (stats["hits"] / total * 100) if total > 0 else 0
                result[name] = {
                    **stats,
                    "total": total,
                    "hit_rate": f"{hit_rate:.1f}%",
                }
            return result

    @classmethod
    def clear(cls) -> None:
        """Limpa todas as estatisticas."""
        with cls._stats_lock:
            cls._stats.clear()


def cached(
    maxsize: int = 128,
    name: Optional[str] = None,
    track_stats: bool = False,
) -> Callable:
    """
    Decorator de cache com registro global e metricas opcionais.

    Wrapa functools.lru_cache com funcionalidades adicionais:
    - Registro global para clear_all_caches()
    - Metricas opcionais de hit/miss via CacheStats

    Args:
        maxsize: Tamanho maximo do cache (padrao: 128).
        name: Nome para identificacao nas metricas. Se None, usa __qualname__.
        track_stats: Se True, registra hits/misses (overhead minimo).

    Returns:
        Decorator que aplica lru_cache e registra o cache.

    Exemplo:
        @cached(maxsize=256, name="find_cnpj")
        def find_cnpj(self, identificador: str) -> str:
            ...
    """
    def decorator(func: Callable) -> Callable:
        cache_name = name or func.__qualname__

        # Aplicar lru_cache
        cached_func = lru_cache(maxsize=maxsize)(func)

        # Registrar para clear_all
        with _lock:
            _registered_caches.append(cached_func)

        if not track_stats:
            return cached_func

        # Wrapper com metricas (apenas se track_stats=True)
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Verificar se esta no cache (via cache_info)
            info_before = cached_func.cache_info()
            result = cached_func(*args, **kwargs)
            info_after = cached_func.cache_info()

            if info_after.hits > info_before.hits:
                CacheStats.record_hit(cache_name)
            else:
                CacheStats.record_miss(cache_name)

            return result

        # Preservar metodos do lru_cache
        wrapper.cache_info = cached_func.cache_info
        wrapper.cache_clear = cached_func.cache_clear

        return wrapper

    return decorator


def clear_all_caches() -> int:
    """
    Limpa todos os caches registrados.

    Returns:
        Numero de caches limpos.
    """
    with _lock:
        count = 0
        for cache in _registered_caches:
            cache.cache_clear()
            count += 1
        CacheStats.clear()
        return count


def get_cache_info() -> dict[str, dict]:
    """
    Retorna informacoes de todos os caches registrados.

    Util para debug e monitoramento do uso de cache.

    Returns:
        Dicionario com {nome: cache_info} para cada cache registrado.

    Exemplo:
        >>> get_cache_info()
        {'EntityResolver.find_cnpj': {'hits': 10, 'misses': 5, 'maxsize': 256, 'currsize': 5}}
    """
    result = {}
    with _lock:
        for cache in _registered_caches:
            name = cache.__wrapped__.__qualname__
            info = cache.cache_info()
            result[name] = {
                "hits": info.hits,
                "misses": info.misses,
                "maxsize": info.maxsize,
                "currsize": info.currsize,
            }
    return result
