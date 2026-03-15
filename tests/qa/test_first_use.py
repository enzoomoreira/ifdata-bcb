"""QA: primeiro uso -- import, lazy loading, cache vazio, excecoes."""

import subprocess
import sys
from pathlib import Path

import pytest

from ifdata_bcb.core.entity_lookup import EntityLookup
from ifdata_bcb.infra.query import QueryEngine
from ifdata_bcb.providers.cosif.explorer import COSIFExplorer


class TestImport:
    def test_import_succeeds(self) -> None:
        import ifdata_bcb  # noqa: F401

    def test_lazy_loading_cosif_type(self) -> None:
        import ifdata_bcb as bcb

        assert type(bcb.cosif).__name__ == "COSIFExplorer"

    def test_dir_exposes_public_api(self) -> None:
        import ifdata_bcb

        d = dir(ifdata_bcb)
        for name in ("cosif", "ifdata", "cadastro", "search"):
            assert name in d

    def test_from_import_search(self) -> None:
        from ifdata_bcb import search  # noqa: F401

        assert callable(search)

    def test_import_nonexistent_raises(self) -> None:
        with pytest.raises(ImportError):
            from ifdata_bcb import nonexistent_attr  # type: ignore[attr-defined]  # noqa: F401


class TestLazyLoading:
    def test_pandas_not_loaded_on_import(self) -> None:
        result = subprocess.run(
            [
                sys.executable,
                "-c",
                "import sys; import ifdata_bcb; print('pandas' not in sys.modules)",
            ],
            capture_output=True,
            text=True,
        )
        assert "True" in result.stdout


class TestExceptions:
    def test_all_exceptions_importable(self) -> None:
        from ifdata_bcb.domain.exceptions import (  # noqa: F401
            AmbiguousIdentifierError,
            BacenAnalysisError,
            DataProcessingError,
            DataUnavailableError,
            EntityNotFoundError,
            InvalidDateFormatError,
            InvalidDateRangeError,
            InvalidIdentifierError,
            InvalidScopeError,
            MissingRequiredParameterError,
            PeriodUnavailableError,
        )

    def test_all_inherit_base(self) -> None:
        from ifdata_bcb.domain.exceptions import (
            BacenAnalysisError,
            DataProcessingError,
            DataUnavailableError,
            InvalidDateFormatError,
            InvalidDateRangeError,
            InvalidIdentifierError,
            InvalidScopeError,
            MissingRequiredParameterError,
            PeriodUnavailableError,
        )

        for exc in (
            InvalidScopeError,
            DataUnavailableError,
            InvalidIdentifierError,
            MissingRequiredParameterError,
            InvalidDateRangeError,
            InvalidDateFormatError,
            PeriodUnavailableError,
            DataProcessingError,
        ):
            assert issubclass(exc, BacenAnalysisError)

    def test_str_produces_readable_message(self) -> None:
        from ifdata_bcb.domain.exceptions import (
            InvalidDateFormatError,
            InvalidIdentifierError,
            InvalidScopeError,
            MissingRequiredParameterError,
        )

        msgs = [
            str(InvalidScopeError("escopo", "fake", ["a", "b"])),
            str(InvalidIdentifierError("abc")),
            str(MissingRequiredParameterError("instituicao")),
            str(InvalidDateFormatError("abc")),
        ]
        assert all(len(m) > 10 for m in msgs)

    def test_semantic_attributes(self) -> None:
        from ifdata_bcb.domain.exceptions import InvalidScopeError

        e = InvalidScopeError("escopo", "fake", ["a", "b"])
        assert e.valid_values == ["a", "b"]
        assert e.scope == "escopo"
        assert e.value == "fake"


class TestConfiguration:
    def test_settings_valid_paths(self) -> None:
        from ifdata_bcb.infra.config import get_settings

        s = get_settings()
        assert s.cache_path is not None
        assert s.logs_path is not None

    def test_logging_idempotent(self) -> None:
        from ifdata_bcb.infra.log import configure_logging, get_logger

        configure_logging()
        configure_logging()
        logger = get_logger("test")
        assert logger is not None


class TestEmptyCacheExperience:
    def test_list_periods_empty(self, tmp_cache_dir: Path) -> None:
        qe = QueryEngine(base_path=tmp_cache_dir)
        cosif = COSIFExplorer(
            query_engine=qe, entity_lookup=EntityLookup(query_engine=qe)
        )
        assert cosif.list_periods() == []

    def test_has_data_false(self, tmp_cache_dir: Path) -> None:
        qe = QueryEngine(base_path=tmp_cache_dir)
        cosif = COSIFExplorer(
            query_engine=qe, entity_lookup=EntityLookup(query_engine=qe)
        )
        assert cosif.has_data() is False

    def test_describe_empty(self, tmp_cache_dir: Path) -> None:
        qe = QueryEngine(base_path=tmp_cache_dir)
        cosif = COSIFExplorer(
            query_engine=qe, entity_lookup=EntityLookup(query_engine=qe)
        )
        desc = cosif.describe()
        assert desc["has_data"] is False

    def test_search_empty(self, tmp_cache_dir: Path) -> None:
        qe = QueryEngine(base_path=tmp_cache_dir)
        el = EntityLookup(query_engine=qe)
        df = el.search("Itau")
        assert df.empty


class TestPackageMetadata:
    def test_py_typed_exists(self) -> None:
        import ifdata_bcb

        py_typed = Path(ifdata_bcb.__file__).parent / "py.typed"
        assert py_typed.exists()

    def test_public_types_importable(self) -> None:
        from ifdata_bcb.domain.types import (  # noqa: F401
            AccountInput,
            DateInput,
            InstitutionInput,
        )
