"""Fixtures compartilhadas para a suite de testes."""

from collections.abc import Generator
from pathlib import Path

import pytest

from ifdata_bcb.infra.paths import temp_dir


@pytest.fixture
def workspace_tmp_dir() -> Generator[Path, None, None]:
    """Diretorio temporario em %TEMP% com cleanup automatico."""
    with temp_dir(prefix="test") as path:
        yield path


@pytest.fixture
def tmp_cache_dir(workspace_tmp_dir: Path) -> Path:
    """Diretorio temporario para simular cache de dados."""
    cache_dir = workspace_tmp_dir / "cache"
    cache_dir.mkdir()
    return cache_dir
