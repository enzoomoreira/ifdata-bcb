import os
from pathlib import Path
from platformdirs import user_cache_dir

# Nome da aplicacao para diretorio de cache
APP_NAME = "py-bacen"


def get_cache_path() -> Path:
    """
    Retorna caminho para cache de dados.

    Prioridade: BACEN_DATA_DIR env var > AppData/Local/py-bacen/Cache.
    """
    env_path = os.environ.get("BACEN_DATA_DIR")
    if env_path:
        path = Path(env_path)
    else:
        # user_cache_dir no Windows retorna AppData/Local/py-bacen/Cache
        path = Path(user_cache_dir(APP_NAME, appauthor=False))

    path.mkdir(parents=True, exist_ok=True)
    return path


def get_logs_path() -> Path:
    """Retorna caminho para logs: AppData/Local/py-bacen/Logs/."""
    # Pega o parent do cache (py-bacen) para criar Logs no mesmo nivel
    cache_path = Path(user_cache_dir(APP_NAME, appauthor=False))
    logs_path = cache_path.parent / "Logs"
    logs_path.mkdir(parents=True, exist_ok=True)
    return logs_path
