import threading
from collections.abc import Callable
from functools import _lru_cache_wrapper, lru_cache

_registered_caches: list[_lru_cache_wrapper] = []
_lock = threading.Lock()


def cached(maxsize: int = 128) -> Callable:
    """Decorator de cache com registro global para permitir clear_all_caches()."""

    def decorator(func: Callable) -> Callable:
        cached_func = lru_cache(maxsize=maxsize)(func)

        with _lock:
            _registered_caches.append(cached_func)

        return cached_func

    return decorator


def clear_all_caches() -> int:
    """Limpa todos os caches registrados. Retorna quantos caches foram limpos."""
    with _lock:
        count = 0
        for cache in _registered_caches:
            cache.cache_clear()
            count += 1
        return count


def get_cache_info() -> dict[str, dict]:
    """Retorna {nome: {hits, misses, maxsize, currsize}} para cada cache."""
    result = {}
    with _lock:
        for cache in _registered_caches:
            # Qualificado com o modulo: so o __qualname__ colide em silencio
            # entre classes homonimas de modulos diferentes.
            wrapped = cache.__wrapped__
            name = f"{wrapped.__module__}.{wrapped.__qualname__}"
            info = cache.cache_info()
            result[name] = {
                "hits": info.hits,
                "misses": info.misses,
                "maxsize": info.maxsize,
                "currsize": info.currsize,
            }
    return result
