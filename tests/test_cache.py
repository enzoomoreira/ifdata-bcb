"""Testes para ifdata_bcb.infra.cache."""


from ifdata_bcb.infra.cache import (
    _registered_caches,
    cached,
    clear_all_caches,
    get_cache_info,
)


class TestCachedDecorator:
    """cached: decorator LRU com registro global."""

    def test_caches_return_value(self) -> None:
        call_count = 0

        class Holder:
            @cached(maxsize=2)
            def compute(self, x: int) -> int:
                nonlocal call_count
                call_count += 1
                return x * 2

        h = Holder()
        assert h.compute(5) == 10
        assert h.compute(5) == 10
        assert call_count == 1

    def test_different_args_not_cached(self) -> None:
        call_count = 0

        class Holder:
            @cached(maxsize=10)
            def compute(self, x: int) -> int:
                nonlocal call_count
                call_count += 1
                return x + 1

        h = Holder()
        h.compute(1)
        h.compute(2)
        assert call_count == 2

    def test_registered_in_global_list(self) -> None:
        initial_count = len(_registered_caches)

        class Holder:
            @cached(maxsize=1)
            def dummy(self) -> str:
                return "ok"

        assert len(_registered_caches) >= initial_count + 1


class TestClearAllCaches:
    def test_clears_and_returns_count(self) -> None:
        count = clear_all_caches()
        assert isinstance(count, int)
        assert count >= 0


class TestGetCacheInfo:
    def test_returns_dict(self) -> None:
        info = get_cache_info()
        assert isinstance(info, dict)
        for name, stats in info.items():
            assert "hits" in stats
            assert "misses" in stats
            assert "maxsize" in stats
            assert "currsize" in stats
