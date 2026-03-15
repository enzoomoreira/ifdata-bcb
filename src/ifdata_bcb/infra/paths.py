import shutil
import tempfile
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path


def ensure_dir(path: Path) -> Path:
    """Cria diretorio (e pais) se nao existir. Thread-safe."""
    path.mkdir(parents=True, exist_ok=True)
    return path


@contextmanager
def temp_dir(prefix: str) -> Generator[Path, None, None]:
    """
    Context manager para diretorio temporario com cleanup automatico.

    Thread-safe: cada chamada cria um diretorio independente via mkdtemp.
    """
    path = Path(tempfile.mkdtemp(prefix=f"ifdata_{prefix}_"))
    try:
        yield path
    finally:
        shutil.rmtree(path, ignore_errors=True)
