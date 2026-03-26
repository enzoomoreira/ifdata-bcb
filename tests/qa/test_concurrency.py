"""QA: testes de concorrencia -- multiplos reads/searches simultaneos."""

from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from ifdata_bcb.core.entity import EntityLookup, EntitySearch
from ifdata_bcb.infra.query import QueryEngine
from ifdata_bcb.providers.cosif.explorer import COSIFExplorer
from tests.conftest import BANCO_A_CNPJ


def _make_cosif(cache_dir: Path) -> COSIFExplorer:
    qe = QueryEngine(base_path=cache_dir)
    el = EntityLookup(query_engine=qe)
    return COSIFExplorer(query_engine=qe, entity_lookup=el)


class TestConcurrentReads:
    def test_20_simultaneous_cosif_reads(self, populated_cache: Path) -> None:
        def do_read() -> int:
            cosif = _make_cosif(populated_cache)
            df = cosif.read(instituicao=BANCO_A_CNPJ, start="2023-03")
            return len(df)

        with ThreadPoolExecutor(max_workers=8) as ex:
            futures = [ex.submit(do_read) for _ in range(20)]
            results = [f.result() for f in as_completed(futures)]

        assert len(results) == 20
        assert all(r > 0 for r in results)

    def test_parallel_search(self, populated_cache: Path) -> None:
        def do_search(term: str) -> int:
            qe = QueryEngine(base_path=populated_cache)
            el = EntityLookup(query_engine=qe)
            return len(EntitySearch(el).search(term))

        with ThreadPoolExecutor(max_workers=8) as ex:
            futures = [ex.submit(do_search, "ALFA") for _ in range(10)]
            results = [f.result() for f in as_completed(futures)]

        assert len(results) == 10

    def test_mixed_read_and_search(self, populated_cache: Path) -> None:
        def do_read() -> str:
            cosif = _make_cosif(populated_cache)
            cosif.read(instituicao=BANCO_A_CNPJ, start="2023-03")
            return "read"

        def do_search() -> str:
            qe = QueryEngine(base_path=populated_cache)
            el = EntityLookup(query_engine=qe)
            EntitySearch(el).search("ALFA")
            return "search"

        with ThreadPoolExecutor(max_workers=8) as ex:
            futures = []
            for _ in range(10):
                futures.append(ex.submit(do_read))
                futures.append(ex.submit(do_search))
            results = [f.result() for f in as_completed(futures)]

        assert len(results) == 20
        assert "read" in results
        assert "search" in results
