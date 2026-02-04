import threading
from functools import lru_cache
from typing import Callable

_registered_caches: list[Callable] = []
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
            name = cache.__wrapped__.__qualname__
            info = cache.cache_info()
            result[name] = {
                "hits": info.hits,
                "misses": info.misses,
                "maxsize": info.maxsize,
                "currsize": info.currsize,
            }
    return result
