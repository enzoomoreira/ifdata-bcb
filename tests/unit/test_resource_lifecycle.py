"""Testes de ciclo de vida de recursos -- caches por instancia e conexoes."""

from pathlib import Path

import duckdb
import pytest

from ifdata_bcb.core.entity import EntityLookup
from ifdata_bcb.infra.query import QueryEngine
from ifdata_bcb.infra.storage import DataManager


class TestEntityLookupCacheIsPerInstance:
    """
    O cache de identificadores nao pode ser global.

    Com lru_cache em metodo, `self` entrava na chave: cada EntityLookup (e sua
    conexao DuckDB) ficava viva no cache, e clear_cache() de uma instancia
    limpava o cache de todas.
    """

    def test_clear_cache_does_not_affect_other_instances(
        self, tmp_cache_dir: Path
    ) -> None:
        a = EntityLookup(query_engine=QueryEngine(base_path=tmp_cache_dir))
        b = EntityLookup(query_engine=QueryEngine(base_path=tmp_cache_dir))

        a.get_entity_identifiers("60872504")
        b.get_entity_identifiers("60872504")
        assert a._identifiers_cache and b._identifiers_cache

        a.clear_cache()

        assert a._identifiers_cache == {}
        assert b._identifiers_cache != {}

    def test_cache_returns_same_result(self, tmp_cache_dir: Path) -> None:
        el = EntityLookup(query_engine=QueryEngine(base_path=tmp_cache_dir))

        first = el.get_entity_identifiers("60872504")
        second = el.get_entity_identifiers("60872504")

        assert first == second

    def test_instance_is_not_retained_by_a_global_cache(
        self, tmp_cache_dir: Path
    ) -> None:
        """A instancia precisa ser coletavel depois de sair de escopo."""
        import gc
        import weakref

        el = EntityLookup(query_engine=QueryEngine(base_path=tmp_cache_dir))
        el.get_entity_identifiers("60872504")
        ref = weakref.ref(el)

        del el
        gc.collect()

        assert ref() is None


class TestConnectionLifecycle:
    def test_query_engine_closes(self, tmp_cache_dir: Path) -> None:
        qe = QueryEngine(base_path=tmp_cache_dir)
        qe.close()

        with pytest.raises(duckdb.Error):
            qe.sql("SELECT 1")

    def test_query_engine_context_manager(self, tmp_cache_dir: Path) -> None:
        with QueryEngine(base_path=tmp_cache_dir) as qe:
            assert not qe.sql("SELECT 1 AS x").empty

        with pytest.raises(duckdb.Error):
            qe.sql("SELECT 1")

    def test_data_manager_context_manager(self, tmp_path: Path) -> None:
        with DataManager(base_path=tmp_path) as dm:
            assert dm.cache_path == tmp_path

        with pytest.raises(duckdb.Error):
            dm._conn.execute("SELECT 1")

    def test_collector_context_manager_closes_http(self) -> None:
        from unittest.mock import MagicMock

        from ifdata_bcb.providers.cosif.collector import COSIFCollector

        with COSIFCollector("individual", data_manager=MagicMock()) as collector:
            assert not collector._http.is_closed

        assert collector._http.is_closed
